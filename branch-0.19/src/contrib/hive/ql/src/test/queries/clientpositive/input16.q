-- TestSerDe is a user defined serde where the default delimiter is Ctrl-B
CREATE TABLE INPUT16(KEY STRING, VALUE STRING) ROW FORMAT SERIALIZER 'org.apache.hadoop.hive.serde2.TestSerDe';
LOAD DATA LOCAL INPATH '../data/files/kv1_cb.txt' INTO TABLE INPUT16;
SELECT INPUT16.VALUE, INPUT16.KEY FROM INPUT16;
DROP TABLE INPUT16;
