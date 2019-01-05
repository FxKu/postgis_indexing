-- how fast are queries with a GIST index
CALL :"avg_st_intersects_100_runs"(:exp_id, 'gist');

-- remember starting time for vacuuming
SELECT extract(epoch FROM clock_timestamp()) AS start_time
\gset

-- update table statistics
VACUUM ANALYSE :"table_name" (geom);

INSERT INTO
  query_stats
VALUES (
  nextval('query_stats_id_seq'),
  'VACUUM gist',
  NULL,
  NULL,
  NULL,
  true,
  false,
  (extract(epoch FROM clock_timestamp()) - :start_time) * 1000,
  :exp_id
);

-- how fast are queries with a GIST index and updated stats
CALL :"avg_st_intersects_100_runs"(:exp_id, 'gist', NULL, true, false);

-- remember starting time for clustering
SELECT extract(epoch FROM clock_timestamp()) AS start_time
\gset

-- cluster table based on GIST index
CLUSTER :"table_name" USING :"gist_index";

INSERT INTO
  query_stats
VALUES (
  nextval('query_stats_id_seq'),
  'CLUSTER TABLE gist',
  NULL,
  NULL,
  NULL,
  false,
  true,
  (extract(epoch FROM clock_timestamp()) - :start_time) * 1000,
  :exp_id
);

-- how fast are queries after clustering the table based on GIST index
VACUUM ANALYSE :"table_name" (geom);
CALL :"avg_st_intersects_100_runs"(:exp_id, 'gist', NULL, true, true);

-- drop the GIST index
DROP INDEX :"gist_index";

-- time for next test, so drop table
DROP TABLE :"table_name" CASCADE;