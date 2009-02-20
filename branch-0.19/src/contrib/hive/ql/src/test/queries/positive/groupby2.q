FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,0,1), count(DISTINCT substr(src.value,4)), concat(substr(src.key,0,1),sum(substr(src.value,4))) GROUP BY substr(src.key,0,1)
