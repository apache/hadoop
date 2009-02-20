CREATE TABLE dest1(key INT, value STRING);

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value;

SELECT dest1.* FROM dest1;
