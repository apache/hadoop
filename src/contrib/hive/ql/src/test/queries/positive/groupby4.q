FROM src
SELECT substr(src.key,0,1) GROUP BY substr(src.key,0,1)
