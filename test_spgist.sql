-- how fast are queries with a sp-GIST index
CALL :"avg_st_intersects_100_runs"(:exp_id, 'spgist');

-- remember starting time for vacuuming
SELECT extract(epoch FROM clock_timestamp()) AS start_time
\gset

-- update table statistics
VACUUM ANALYSE :"table_name" (geom);

INSERT INTO
  query_stats
VALUES (
  nextval('query_stats_id_seq'),
  'VACUUM spgist',
  NULL,
  NULL,
  NULL,
  true,
  false,
  (extract(epoch FROM clock_timestamp()) - :start_time) * 1000,
  :exp_id
);

-- how fast are queries with a sp-GIST index and updated stats
CALL :"avg_st_intersects_100_runs"(:exp_id, 'spgist', NULL, true, false);

-- drop the sp-GIST index
DROP INDEX :"spgist_index";

-- time for next test, so drop table
DROP TABLE :"table_name" CASCADE;