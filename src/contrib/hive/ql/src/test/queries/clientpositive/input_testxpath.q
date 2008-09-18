CREATE TABLE dest1(key INT, value STRING);

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE dest1 SELECT src_thrift.lint[1], src_thrift.lintstring[0].mystring;

FROM src_thrift
INSERT OVERWRITE TABLE dest1 SELECT src_thrift.lint[1], src_thrift.lintstring[0].mystring;

SELECT dest1.* FROM dest1;
