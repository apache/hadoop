drop table alter1;
create table alter1(a int, b int);
describe extended alter1;
alter table alter1 set tblproperties ('a'='1', 'c'='3');
describe extended alter1;
alter table alter1 set tblproperties ('a'='1', 'c'='4', 'd'='3');
describe extended alter1;

alter table alter1 set serdeproperties('s1'='9');
describe extended alter1;
alter table alter1 set serdeproperties('s1'='10', 's2' ='20');
describe extended alter1;

alter table alter1 set serde 'org.apache.hadoop.hive.serde2.TestSerDe' with serdeproperties('s1'='9');
describe extended alter1;

alter table alter1 set serde 'org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe';
describe extended alter1;

drop table alter1;
