-- set BRIN index name
SELECT :'table_name' || '_brin' AS brin_index
\gset

-- create BRIN index
CALL create_spatial_index(:exp_id, 'brin', NULL, NULL, 'pages_per_range=96, autosummarize=true');

-- remember starting time for vacuuming
SELECT extract(epoch FROM clock_timestamp()) AS start_time
\gset

VACUUM ANALYSE :"table_name" (geom);

INSERT INTO
  query_stats
VALUES (
  nextval('query_stats_id_seq'),
  'VACUUM brin',
  NULL,
  NULL,
  NULL,
  true,
  true,
  (extract(epoch FROM clock_timestamp()) - :start_time) * 1000,
  :exp_id
);

-- disable seq scan, as only then the BRIN index will be considered
SET enable_seqscan = false;
CALL :"avg_st_intersects_100_runs"(:exp_id, 'brin', '{"storage":"pages_per_range=96, autosummarize=true"}'::jsonb, true, true);
SET enable_seqscan = true;

-- drop the BRIN index
DROP INDEX :"brin_index";