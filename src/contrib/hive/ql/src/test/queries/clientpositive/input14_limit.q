CREATE TABLE dest1(key INT, value STRING);

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) AS (tkey, tvalue) 
         USING '/bin/cat'
  CLUSTER BY tkey LIMIT 20
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) AS (tkey, tvalue) 
         USING '/bin/cat'
  CLUSTER BY tkey LIMIT 20
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

SELECT dest1.* FROM dest1;
