CREATE TABLE dest4_sequencefile(key INT, value STRING) STORED AS COMPRESSED;

FROM src
INSERT OVERWRITE TABLE dest4_sequencefile SELECT src.key, src.value;

SELECT dest4_sequencefile.* FROM dest4_sequencefile;
