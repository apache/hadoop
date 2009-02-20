set hive.partition.pruning=strict;

EXPLAIN
SELECT count(1) FROM srcpart;

SELECT count(1) FROM srcpart;
