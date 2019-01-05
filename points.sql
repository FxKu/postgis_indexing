-- create table name variable
SELECT 'pts_' || :'npoints' AS table_name
\gset

-- create a first experiment
INSERT INTO
  experiments(table_name, geom_type, ngeoms, test_area)
VALUES (
  :'table_name', 'Point', :npoints, ST_GeomFromWKB(:'test_area', 4326)
)
RETURNING id AS exp_id
\gset

-- check how fast the point generation is
CALL create_point_test_wo_index(:exp_id);

-- create matching procedure name
SELECT 'st_intersects_' || :'table_name' AS avg_st_intersects_100_runs
\gset

-- how fast are queries without an index
CALL :"avg_st_intersects_100_runs"(:exp_id, NULL);

-- do tests with GIST index
SELECT :'table_name' || '_gist' AS gist_index
\gset

-- check GIST index creation speed on filled table
CALL create_spatial_index(:exp_id, 'gist');

-- test performance of this GIST index
\i test_gist.sql

-- check GIST index creation speed when inserting data
CALL create_point_test_w_index(:exp_id, 'gist');

-- test performance of this GIST index
\i test_gist.sql

-- do tests with sp-GIST index
SELECT :'table_name' || '_spgist' AS spgist_index
\gset

-- create table again for sp-GIST tests
CALL create_point_test_wo_index(:exp_id);

-- check sp-GIST index creation speed on filled table
CALL create_spatial_index(:exp_id, 'spgist');

-- test performance of this sp-GIST index
\i test_spgist.sql

-- check sp-GIST index creation speed when inserting data
CALL create_point_test_w_index(:exp_id, 'spgist');

-- test performance of this sp-GIST index
\i test_spgist.sql

-- recreate the table once more
CALL create_point_test_wo_index(:exp_id);

-- now cluster the database on behalf of a GeoHash ordering
-- first create the index
CALL create_spatial_index(:exp_id, 'btree', 'ST_GeoHash');

-- set BTREE name
SELECT :'table_name' || '_btree' AS geohash_index
\gset

-- remember starting time for clustering
SELECT extract(epoch FROM clock_timestamp()) AS start_time
\gset

CLUSTER :"table_name" USING :"geohash_index";

INSERT INTO
  query_stats
VALUES (
  nextval('query_stats_id_seq'),
  'CLUSTER TABLE geohash',
  NULL,
  NULL,
  NULL,
  false,
  true,
  (extract(epoch FROM clock_timestamp()) - :start_time) * 1000,
  :exp_id
);

-- do one more tests with a GIST index
CALL create_spatial_index(:exp_id, 'gist');
VACUUM ANALYSE :"table_name" (geom);
CALL :"avg_st_intersects_100_runs"(:exp_id, 'gist', NULL, true, true);
DROP INDEX :"gist_index";

-- do one more tests with a sp-GIST index
CALL create_spatial_index(:exp_id, 'spgist');
VACUUM ANALYSE :"table_name" (geom);
CALL :"avg_st_intersects_100_runs"(:exp_id, 'spgist', NULL, true, true);
DROP INDEX :"spgist_index";

-- now, test the BRIN index
\i test_brin.sql

-- time for next test, so drop table
DROP TABLE :"table_name" CASCADE;
