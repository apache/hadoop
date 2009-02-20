CREATE TABLE dest1(c1 INT, c2 INT, c3 INT, c4 INT, c5 INT);

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT sum(substr(src.value,4)), avg(substr(src.value,4)), avg(DISTINCT substr(src.value,4)), max(substr(src.value,4)), min(substr(src.value,4));

FROM src
INSERT OVERWRITE TABLE dest1 SELECT sum(substr(src.value,4)), avg(substr(src.value,4)), avg(DISTINCT substr(src.value,4)), max(substr(src.value,4)), min(substr(src.value,4));

SELECT dest1.* FROM dest1;
