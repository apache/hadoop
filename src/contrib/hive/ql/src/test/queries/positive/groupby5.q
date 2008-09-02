INSERT OVERWRITE TABLE dest1 
SELECT src.key, sum(substr(src.value,4)) 
FROM src
GROUP BY src.key
