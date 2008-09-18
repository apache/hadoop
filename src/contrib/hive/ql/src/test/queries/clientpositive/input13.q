CREATE TABLE dest1(key INT, value STRING);
CREATE TABLE dest2(key INT, value STRING);
CREATE TABLE dest3(key INT, value STRING) PARTITIONED BY(ds STRING, hr STRING);

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200
INSERT OVERWRITE TABLE dest3 PARTITION(ds='2008-04-08', hr='12') SELECT src.key WHERE src.key >= 200 and src.key < 300
INSERT OVERWRITE DIRECTORY '../../../../build/contrib/hive/ql/test/data/warehouse/dest4.out' SELECT src.value WHERE src.key >= 300;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200
INSERT OVERWRITE TABLE dest3 PARTITION(ds='2008-04-08', hr='12') SELECT src.key WHERE src.key >= 200 and src.key < 300
INSERT OVERWRITE DIRECTORY '../../../../build/contrib/hive/ql/test/data/warehouse/dest4.out' SELECT src.value WHERE src.key >= 300;

SELECT dest1.* FROM dest1;
SELECT dest2.* FROM dest2;
SELECT dest3.* FROM dest3;
!../../../../bin/hadoop dfs -cat ../../../../build/contrib/hive/ql/test/data/warehouse/dest4.out/*;
