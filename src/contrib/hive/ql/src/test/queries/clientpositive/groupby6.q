CREATE TABLE dest1(c1 STRING);

FROM src
INSERT OVERWRITE TABLE dest1 SELECT DISTINCT substr(src.value,4,1);

SELECT dest1.* FROM dest1;

