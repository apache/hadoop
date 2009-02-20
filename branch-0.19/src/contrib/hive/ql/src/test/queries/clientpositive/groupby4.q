CREATE TABLE dest1(c1 STRING);

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,0,1) GROUP BY substr(src.key,0,1);

FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,0,1) GROUP BY substr(src.key,0,1);

SELECT dest1.* FROM dest1;

