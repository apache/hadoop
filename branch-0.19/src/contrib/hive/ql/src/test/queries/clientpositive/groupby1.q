CREATE TABLE dest1(key INT, value DOUBLE);

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1 SELECT src.key, sum(substr(src.value,4)) GROUP BY src.key;

FROM src INSERT OVERWRITE TABLE dest1 SELECT src.key, sum(substr(src.value,4)) GROUP BY src.key;

SELECT dest1.* FROM dest1;
