CREATE TABLE dest1(key INT, value STRING);

EXPLAIN
FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.lint, src_thrift.lintstring) AS (tkey, tvalue) 
         USING '/bin/cat'
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue;

FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.lint, src_thrift.lintstring) AS (tkey, tvalue) 
         USING '/bin/cat'
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue;

SELECT dest1.* FROM dest1;
