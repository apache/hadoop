FROM src
INSERT OVERWRITE TABLE dest1 SELECT sum(substr(src.value,4)), avg(substr(src.value,4)), avg(DISTINCT substr(src.value,4)), max(substr(src.value,4)), min(substr(src.value,4))
