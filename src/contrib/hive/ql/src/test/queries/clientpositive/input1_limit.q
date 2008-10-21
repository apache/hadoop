CREATE TABLE dest1(key INT, value STRING);
CREATE TABLE dest2(key INT, value STRING);

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key < 100 LIMIT 5;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key < 100 LIMIT 5;

SELECT dest1.* FROM dest1;
SELECT dest2.* FROM dest2;

DROP TABLE dest1;
DROP TABLE dest2;

