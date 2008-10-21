CREATE TABLE dest1(c1 STRING);

FROM src INSERT OVERWRITE TABLE dest1 SELECT '  abc  ' WHERE src.key = 86;

EXPLAIN
SELECT '|', trim(dest1.c1), '|', rtrim(dest1.c1), '|', ltrim(dest1.c1), '|' FROM dest1;

SELECT '|', trim(dest1.c1), '|', rtrim(dest1.c1), '|', ltrim(dest1.c1), '|' FROM dest1;
