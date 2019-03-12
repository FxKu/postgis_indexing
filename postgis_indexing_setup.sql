-- drop all procedures
DO
$$
BEGIN

END;
$$
LANGUAGE plpgsql;

-- create table for experiments
DROP TABLE IF EXISTS experiments CASCADE;
CREATE TABLE experiments(
  id SERIAL PRIMARY KEY,
  table_name TEXT,
  geom_type TEXT,
  ngeoms BIGINT,
  test_area GEOMETRY
);

-- create table for query statistics
DROP TABLE IF EXISTS query_stats CASCADE;
CREATE TABLE query_stats
(
  id SERIAL PRIMARY KEY,
  query_type TEXT,
  index_type TEXT,
  index_params JSONB,
  index_size TEXT,
  is_set_table_stats BOOLEAN,
  is_clustered_table BOOLEAN,
  duration_in_ms DOUBLE PRECISION,
  experiment_id INTEGER,
  FOREIGN KEY (experiment_id) REFERENCES experiments(id)
);

-- procedure to create a point table without any index
CREATE OR REPLACE PROCEDURE create_point_test_wo_index(exp_id INTEGER) AS
$$
DECLARE
  start_time DOUBLE PRECISION;
  tablename TEXT;
  npoints BIGINT;
  test_box GEOMETRY;
  srid INTEGER;
  i INTEGER;
  index_size TEXT;
BEGIN
  SELECT
    table_name,
    ngeoms,
    test_area
  INTO
    tablename,
    npoints,
    test_box
  FROM
    experiments
  WHERE
    id = $1;

  srid := ST_SRID(test_box);
  start_time := extract(epoch FROM clock_timestamp());

  -- first create the table
  EXECUTE format(
    'CREATE TABLE %I (gid BIGINT, geom geometry(Point,%s))',
    tablename, srid
  );

  -- fill with random data
  FOR i IN 1..ceil(npoints::numeric/100000) LOOP
    EXECUTE format(
      'INSERT INTO %I
        SELECT
          (d).path[1]::bigint + $1 AS gid,
          (d).geom::geometry(Point,%s)
        FROM
          ST_Dump(
            ST_GeneratePoints(
              $2, $3
            )
          ) d',
      tablename, srid
    )
    USING
      i * 100000 - 100000,
      test_box,
      CASE WHEN npoints::numeric/100000/i > 1
        THEN 100000
        ELSE npoints - ((i-1) * 100000)
      END;

    COMMIT;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'CREATE TABLE',
    NULL,
    NULL,
    NULL,
    false,
    false,
    (extract(epoch FROM clock_timestamp()) - start_time) * 1000,
    $1
  );

  SELECT
    pg_size_pretty(pg_relation_size(oid))
  INTO
    index_size
  FROM
    pg_class c
  WHERE
    c.relkind = 'r'
    AND relname= tablename;

  EXECUTE format('SELECT count(*) FROM %I', tablename) INTO npoints;
  RAISE NOTICE 'table size: %, counting %', index_size, npoints;
END;
$$
LANGUAGE plpgsql;

-- procedure to create a linestring table without any index
CREATE OR REPLACE PROCEDURE create_line_test_wo_index(exp_id INTEGER) AS
$$
DECLARE
  start_time DOUBLE PRECISION;
  tablename TEXT;
  npoints BIGINT;
  test_box GEOMETRY;
  srid INTEGER;
  i INTEGER;
  inserted BOOLEAN;
  index_size TEXT;
BEGIN
  SELECT
    table_name,
    ngeoms,
    test_area
  INTO
    tablename,
    npoints,
    test_box
  FROM
    experiments
  WHERE
    id = $1;

  srid := ST_SRID(test_box);
  start_time := extract(epoch FROM clock_timestamp());

  -- first create the table
  EXECUTE format(
    'CREATE TABLE %I (gid BIGINT, geom geometry(LineString,%s))',
    tablename, srid
  );

  -- fill with random data
  FOR i IN 1..ceil(npoints::numeric/10000) LOOP
    inserted := FALSE;

    -- sometimes GEOSVoronoiDiagram Exceptions can occur
    -- try as long as data could be inserted
    WHILE NOT inserted LOOP
      BEGIN
        EXECUTE format(
          'INSERT INTO %I
             SELECT
               gid,
               geom
             FROM (
               SELECT
                 row_number() OVER () + $1 AS gid,
                 (d).geom::geometry(LineString,%s)
               FROM
                 ST_Dump(
                   ST_VoronoiLines(
                     ST_GeneratePoints(
                       $2, $3 
                     ),
                     round((random()/2)::numeric,4)
                   )
                 ) d
               WHERE
                 ST_Within(
                   (d).geom::geometry(LineString,%s),
                   $2
                 )
             ) t
             WHERE
               gid <= $4',
          tablename, srid, srid
        )
        USING
          i * 10000 - 10000,
          test_box,
          CASE WHEN npoints::numeric/10000/i > 1
            THEN 10000
            ELSE npoints - ((i-1) * 10000)
          END,
          i * 10000;

        inserted := TRUE;

        EXCEPTION
          WHEN others THEN
            RAISE NOTICE 'Voronoi troubles!';
            CONTINUE;
      END;
    END LOOP;

    -- commit inserted data
    COMMIT;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'CREATE TABLE',
    NULL,
    NULL,
    NULL,
    false,
    false,
    (extract(epoch FROM clock_timestamp()) - start_time) * 1000,
    $1
  );

  SELECT
    pg_size_pretty(pg_relation_size(oid))
  INTO
    index_size
  FROM
    pg_class c
  WHERE
    c.relkind = 'r'
    AND relname= tablename;

  EXECUTE format('SELECT count(*) FROM %I', tablename) INTO npoints;
  RAISE NOTICE 'table size: %, counting %', index_size, npoints;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE create_spatial_index(
  exp_id INTEGER,
  index_type TEXT,
  index_function TEXT DEFAULT NULL,
  index_op_class TEXT DEFAULT NULL,
  storage_param TEXT DEFAULT NULL
  ) AS
$$
DECLARE
  params JSONB := '{}'::jsonb;
  index_column TEXT;
  start_time DOUBLE PRECISION;
  execution_time_in_ms DOUBLE PRECISION;
  index_size TEXT;
  tablename TEXT;
BEGIN
  SELECT
    table_name
  INTO
    tablename
  FROM
    experiments
  WHERE
    id = $1;

  -- functional index
  IF $3 IS NOT NULL THEN
    params := params || jsonb_build_object('func', $3);
  END IF;

  -- op_class
  IF $4 IS NOT NULL THEN
    params := params || jsonb_build_object('op_class', $4);
  END IF;

  -- storage parameter
  IF $5 IS NOT NULL THEN
    params := params || jsonb_build_object('storage', $5);
  END IF;

  start_time := extract(epoch FROM clock_timestamp());

  EXECUTE format(
    'CREATE INDEX %I ON %I USING %s (%s %s) %s',
    tablename || '_' || index_type,
    tablename, 
    index_type,
    CASE WHEN $3 IS NOT NULL
      THEN $3 || '(geom)'
      ELSE 'geom'
    END,
    COALESCE($4, ''),
    CASE WHEN $5 IS NOT NULL
      THEN 'WITH (' || $5 || ')'
      ELSE ''
    END
  );

  execution_time_in_ms := (extract(epoch FROM clock_timestamp()) - start_time) * 1000;

  -- query index size
  SELECT
    pg_size_pretty(pg_relation_size(i.indexrelid))
  INTO
    index_size
  FROM
    pg_index i
  JOIN
    pg_class c
    ON c.oid = i.indexrelid
  WHERE
    c.relkind = 'i'
    AND relname= (tablename || '_' || index_type);

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'CREATE INDEX',
    $2,
    params,
    index_size,
    false,
    false,
    execution_time_in_ms,
    $1
  );
END;
$$
LANGUAGE plpgsql;

-- procedure to create a point table with a given index and then inserting points
CREATE OR REPLACE PROCEDURE create_point_test_w_index(
  exp_id INTEGER,
  index_type TEXT,
  index_function TEXT DEFAULT NULL,
  index_op_class TEXT DEFAULT NULL,
  storage_param TEXT DEFAULT NULL
  ) AS
$$
DECLARE
  tablename TEXT;
  npoints BIGINT;
  test_box GEOMETRY;
  srid INTEGER;
  params JSONB := '{}'::jsonb;
  start_time DOUBLE PRECISION;
  execution_time_in_ms DOUBLE PRECISION;
  index_size TEXT;
  i INTEGER;
BEGIN
  SELECT
    table_name,
    ngeoms,
    test_area
  INTO
    tablename,
    npoints,
    test_box
  FROM
    experiments
  WHERE
    id = $1;

  srid := ST_SRID(test_box);

  EXECUTE format(
    'CREATE TABLE %I (gid bigint PRIMARY KEY, geom geometry(Point,%s))',
    tablename, srid);

  -- functional index
  IF $3 IS NOT NULL THEN
    params := params || jsonb_build_object('func', $3);
  END IF;

  -- op_class
  IF $4 IS NOT NULL THEN
    params := params || jsonb_build_object('op_class', $4);
  END IF;

  -- storage parameter
  IF $5 IS NOT NULL THEN
    params := params || jsonb_build_object('storage', $5);
  END IF;

  EXECUTE format(
    'CREATE INDEX %I ON %I USING %s (%s %s) %s %s',
    tablename || '_' || index_type,
    tablename, 
    index_type,
    CASE WHEN $3 IS NOT NULL
      THEN $3 || '(geom)'
      ELSE 'geom'
    END,
    COALESCE($4, ''),
    CASE WHEN $5 IS NOT NULL
      THEN 'WITH (' || $5 || ')'
      ELSE ''
    END,
    ' TABLESPACE pgdata'
  );

  start_time := extract(epoch FROM clock_timestamp());
  
  -- fill with random data
  FOR i IN 1..ceil(npoints::numeric/100000) LOOP
    EXECUTE format(
      'INSERT INTO %I
        SELECT
          (d).path[1]::bigint + $1 AS gid,
          (d).geom::geometry(Point,%s)
        FROM
          ST_Dump(
            ST_GeneratePoints(
              $2, $3
            )
          ) d',
      tablename, srid)
    USING
      i * 100000 - 100000,
      test_box,
      CASE WHEN npoints::numeric/100000/i > 1
        THEN 100000
        ELSE npoints - ((i-1) * 100000)
      END;

    COMMIT;
  END LOOP;

  execution_time_in_ms := (extract(epoch FROM clock_timestamp()) - start_time) * 1000;

  -- query index size
  SELECT
    pg_size_pretty(pg_relation_size(i.indexrelid))
  INTO
    index_size
  FROM
    pg_index i
  JOIN
    pg_class c
    ON c.oid = i.indexrelid
  WHERE
    c.relkind = 'i'
    AND relname= (tablename || '_' || index_type);

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'INSERT',
    $2,
    params,
    index_size,
    false,
    false,
    execution_time_in_ms,
    $1
  );

  SELECT
    pg_size_pretty(pg_relation_size(oid))
  INTO
    index_size
  FROM
    pg_class c
  WHERE
    c.relkind = 'r'
    AND relname= tablename;

  EXECUTE format('SELECT count(*) FROM %I', tablename) INTO npoints;
  RAISE NOTICE 'table size: %, counting %', index_size, npoints;
END;
$$
LANGUAGE plpgsql;

-- procedure to create a linestring table with a given index and then inserting lines
CREATE OR REPLACE PROCEDURE create_line_test_w_index(
  exp_id INTEGER,
  index_type TEXT,
  index_function TEXT DEFAULT NULL,
  index_op_class TEXT DEFAULT NULL,
  storage_param TEXT DEFAULT NULL
  ) AS
$$
DECLARE
  tablename TEXT;
  npoints BIGINT;
  test_box GEOMETRY;
  srid INTEGER;
  params JSONB := '{}'::jsonb;
  start_time DOUBLE PRECISION;
  execution_time_in_ms DOUBLE PRECISION;
  index_size TEXT;
  i INTEGER;
  inserted BOOLEAN;
BEGIN
  SELECT
    table_name,
    ngeoms,
    test_area
  INTO
    tablename,
    npoints,
    test_box
  FROM
    experiments
  WHERE
    id = $1;

  srid := ST_SRID(test_box);

  EXECUTE format(
    'CREATE TABLE %I (gid bigint PRIMARY KEY, geom geometry(LineString,%s))',
    tablename, srid);

  -- functional index
  IF $3 IS NOT NULL THEN
    params := params || jsonb_build_object('func', $3);
  END IF;

  -- op_class
  IF $4 IS NOT NULL THEN
    params := params || jsonb_build_object('op_class', $4);
  END IF;

  -- storage parameter
  IF $5 IS NOT NULL THEN
    params := params || jsonb_build_object('storage', $5);
  END IF;

  EXECUTE format(
    'CREATE INDEX %I ON %I USING %s (%s %s) %s %s',
    tablename || '_' || index_type,
    tablename, 
    index_type,
    CASE WHEN $3 IS NOT NULL
      THEN $3 || '(geom)'
      ELSE 'geom'
    END,
    COALESCE($4, ''),
    CASE WHEN $5 IS NOT NULL
      THEN 'WITH (' || $5 || ')'
      ELSE ''
    END,
    ' TABLESPACE pgdata'
  );

  start_time := extract(epoch FROM clock_timestamp());
  
  -- fill with random data
  FOR i IN 1..ceil(npoints::numeric/10000) LOOP
    inserted := FALSE;

    -- sometimes GEOSVoronoiDiagram Exceptions can occur
    -- try as long as data could be inserted
    WHILE NOT inserted LOOP
      BEGIN
        EXECUTE format(
          'INSERT INTO %I
             SELECT
               gid,
               geom
             FROM (
               SELECT
                 row_number() OVER () + $1 AS gid,
                 (d).geom::geometry(LineString,%s)
               FROM
                 ST_Dump(
                   ST_VoronoiLines(
                     ST_GeneratePoints(
                       $2, $3 
                     ),
                     round((random()/2)::numeric,4)
                   )
                 ) d
               WHERE
                 ST_Within(
                   (d).geom::geometry(LineString,%s),
                   $2
                 )
             ) t
             WHERE
               gid <= $4',
          tablename, srid, srid
        )
        USING
          i * 10000 - 10000,
          test_box,
          CASE WHEN npoints::numeric/10000/i > 1
            THEN 10000
            ELSE npoints - ((i-1) * 10000)
          END,
          i * 10000;

        inserted := TRUE;

        EXCEPTION
          WHEN others THEN
            RAISE NOTICE 'Voronoi troubles!';
            CONTINUE;
      END;
    END LOOP;

    -- commit inserted data
    COMMIT;
  END LOOP;

  execution_time_in_ms := (extract(epoch FROM clock_timestamp()) - start_time) * 1000;

  -- query index size
  SELECT
    pg_size_pretty(pg_relation_size(i.indexrelid))
  INTO
    index_size
  FROM
    pg_index i
  JOIN
    pg_class c
    ON c.oid = i.indexrelid
  WHERE
    c.relkind = 'i'
    AND relname= (tablename || '_' || index_type);

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'INSERT',
    $2,
    params,
    index_size,
    false,
    false,
    execution_time_in_ms,
    $1
  );

  SELECT
    pg_size_pretty(pg_relation_size(oid))
  INTO
    index_size
  FROM
    pg_class c
  WHERE
    c.relkind = 'r'
    AND relname= tablename;

  EXECUTE format('SELECT count(*) FROM %I', tablename) INTO npoints;
  RAISE NOTICE 'table size: %, counting %', index_size, npoints;
END;
$$
LANGUAGE plpgsql;


-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_pts_100000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      pts_100000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;

-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_pts_1000000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      pts_1000000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;

-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_pts_10000000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      pts_10000000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;

-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_pts_100000000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      pts_100000000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;

-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_pts_1000000000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE,
  random_search BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
  search_area GEOMETRY;
BEGIN
  -- define a static search area
  search_area := ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326);

  FOR i IN 1..100 LOOP
    -- use random search areas
    IF $6 THEN
      search_area := ST_SetSRID(ST_Buffer(ST_GeometryN(ST_GeneratePoints(ST_MakeEnvelope(20.2619773, 43.618682, 30.0454257, 48.2653964), 1), 1), 0.1), 4326);
    END IF;

    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      pts_1000000000
    WHERE
      ST_Intersects(geom, search_area);

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;


-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_line_100000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      line_100000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;

-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_line_1000000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      line_1000000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;

-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_line_10000000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      line_10000000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;

-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_line_100000000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      line_100000000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;

-- function to test speed of st_intersects queries
CREATE OR REPLACE PROCEDURE st_intersects_line_1000000000(
  exp_id INTEGER,
  index_type TEXT,
  index_params JSONB DEFAULT '{}'::jsonb,
  is_set_table_stats BOOLEAN DEFAULT FALSE,
  is_clustered_table BOOLEAN DEFAULT FALSE
  ) AS
$$
DECLARE
  i INTEGER;
  start_time DOUBLE PRECISION;
  avg_ex_time DOUBLE PRECISION;
BEGIN
  FOR i IN 1..100 LOOP
    start_time := extract(epoch FROM clock_timestamp());

    PERFORM
      count(gid)
    FROM
      line_1000000000
    WHERE
      ST_Intersects(geom, ST_SetSRID(ST_Buffer(ST_MakePoint(26.096306, 44.439663), 0.1), 4326));

    IF i = 1 THEN
      avg_ex_time := extract(epoch FROM clock_timestamp()) - start_time;
    ELSE
      avg_ex_time := (avg_ex_time + (extract(epoch FROM clock_timestamp()) - start_time)) / 2;
    END IF;
  END LOOP;

  INSERT INTO
    query_stats
  VALUES (
    nextval('query_stats_id_seq'),
    'ST_Intersects',
    $2,
    $3,
    NULL,
    $4,
    $5,
    avg_ex_time * 1000,
    $1
  );  
END;
$$
LANGUAGE plpgsql;