-- TestSerDe is a user defined serde where the default delimiter is Ctrl-B
-- the user is overwriting it with ctrlC
CREATE TABLE INPUT16_CC(KEY STRING, VALUE STRING) ROW FORMAT SERIALIZER 'org.apache.hadoop.hive.serde2.TestSerDe'  with serdeproperties ('testserde.default.serialization.format'='\003', 'dummy.prop.not.used'='dummyy.val');
LOAD DATA LOCAL INPATH '../data/files/kv1_cc.txt' INTO TABLE INPUT16_CC;
SELECT INPUT16_CC.VALUE, INPUT16_CC.KEY FROM INPUT16_CC;
DROP TABLE INPUT16_CC;
