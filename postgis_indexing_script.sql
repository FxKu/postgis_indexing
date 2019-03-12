\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\echo 'setting up experiment tables'
\i postgis_indexing_setup.sql


\echo
\echo 'Set variables for tests'
SELECT ST_AsBinary(ST_MakeEnvelope(20.2619773, 43.618682, 30.0454257, 48.2653964, 4326)) AS test_area
\gset

\echo
\echo '1st experiment: 100.000 points'
\set npoints 100000
\i points.sql

\echo
\echo '2nd experiment: 1.000.000 points'
\set npoints 1000000
\i points.sql

\echo
\echo '3rd experiment: 10.000.000 points'
\set npoints 10000000
\i points.sql

\echo
\echo '4th experiment: 100.000.000 points'
\set npoints 100000000
\i points.sql

\echo
\echo '5th experiment: 1.000.000.000 points'
\set npoints 1000000000
\i points.sql

\echo
\echo 'finished point experiments'


\echo
\echo '6th experiment: 100.000 lines'
\set npoints 100000
\i linestrings.sql

\echo
\echo '7th experiment: 1.000.000 lines'
\set npoints 1000000
\i linestrings.sql

\echo
\echo '8th experiment: 10.000.000 lines'
\set npoints 10000000
\i linestrings.sql

\echo
\echo '9th experiment: 100.000.000 lines'
\set npoints 100000000
\i linestrings.sql

\echo
\echo '10th experiment: 1.000.000.000 lines'
\set npoints 1000000000
\i linestrings.sql

\echo
\echo 'finished linestring experiments'
