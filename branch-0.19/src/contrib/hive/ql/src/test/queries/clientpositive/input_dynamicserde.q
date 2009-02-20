CREATE TABLE dest1(a array<int>, b array<string>, c map<string,string>, d int, e string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '1'
COLLECTION ITEMS TERMINATED BY '2'
MAP KEYS TERMINATED BY '3'
LINES TERMINATED BY '10';

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE dest1 SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring;

FROM src_thrift
INSERT OVERWRITE TABLE dest1 SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring;

SELECT dest1.* FROM dest1;
