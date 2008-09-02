CREATE TABLE dest1(key INT, value STRING);

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) AS (tkey, tvalue) 
         USING '/bin/cat'
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

SELECT dest1.* FROM dest1;
