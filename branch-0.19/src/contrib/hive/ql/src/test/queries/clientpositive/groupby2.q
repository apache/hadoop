CREATE TABLE dest1(key STRING, c1 INT, c2 STRING);

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,0,1), count(DISTINCT substr(src.value,4)), concat(substr(src.key,0,1),sum(substr(src.value,4))) GROUP BY substr(src.key,0,1);

FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,0,1), count(DISTINCT substr(src.value,4)), concat(substr(src.key,0,1),sum(substr(src.value,4))) GROUP BY substr(src.key,0,1);

SELECT dest1.* FROM dest1;
