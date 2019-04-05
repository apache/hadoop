<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
# Secure Docker-compose with KMS, Yarn RM and NM
This docker compose allows to test Sample Map Reduce Jobs with OzoneFileSystem
It is a superset of ozonesecure docker-compose, which add Yarn NM/RM in addition
to Ozone OM/SCM/NM/DN and Kerberos KDC.

## Basic setup

```
cd $(git rev-parse --show-toplevel)/hadoop-ozone/dist/target/ozone-@project.version@/compose/ozonesecure-mr

docker-compose up -d
```

## Ozone Manager Setup

```
docker-compose exec om bash

kinit -kt /etc/security/keytabs/testuser.keytab testuser/om@EXAMPLE.COM

ozone sh volume create /vol1

ozone sh bucket create /vol1/bucket1

ozone sh key put /vol1/bucket1/key1 LICENSE.txt

ozone fs -ls o3fs://bucket1.vol1/
```

## Yarn Resource Manager Setup
```
docker-compose exec rm bash

kinit -kt /etc/security/keytabs/hadoop.keytab hadoop/rm@EXAMPLE.COM
export HADOOP_MAPRED_HOME=/opt/hadoop/share/hadoop/mapreduce

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/hadoop/share/hadoop/mapreduce/*:/opt/ozone/share/ozone/lib/hadoop-ozone-filesystem-lib-current-@project.version@.jar

hadoop fs -mkdir /user
hadoop fs -mkdir /user/hadoop
```

## Run Examples

### WordCount
```
yarn jar $HADOOP_MAPRED_HOME/hadoop-mapreduce-examples-*.jar wordcount o3fs://bucket1.vol1/key1 o3fs://bucket1.vol1/key1.count

hadoop fs -cat /key1.count/part-r-00000
```

### Pi
```
yarn jar $HADOOP_MAPRED_HOME/hadoop-mapreduce-examples-*.jar pi 10 100
```

### RandomWrite
```
yarn jar $HADOOP_MAPRED_HOME/hadoop-mapreduce-examples-*.jar randomwriter -Dtest.randomwrite.total_bytes=10000000  o3fs://bucket1.vol1/randomwrite.out
```
