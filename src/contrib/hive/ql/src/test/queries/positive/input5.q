FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.lint, src_thrift.lintstring) AS (tkey, tvalue) 
         USING '/bin/cat'
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue
