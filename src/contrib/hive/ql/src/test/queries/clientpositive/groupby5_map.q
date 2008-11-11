set hive.map.aggr=true;

CREATE TABLE dest1(key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1 SELECT sum(src.key);

FROM src INSERT OVERWRITE TABLE dest1 SELECT sum(src.key);

SELECT dest1.* FROM dest1;
