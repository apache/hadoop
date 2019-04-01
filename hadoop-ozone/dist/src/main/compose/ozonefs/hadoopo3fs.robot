# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Test ozone fs usage from Hdfs and Spark
Library             OperatingSystem
Library             String
Resource            ../../smoketest/env-compose.robot
Resource            ../../smoketest/commonlib.robot

*** Variables ***
${DATANODE_HOST}        datanode

*** Keywords ***

Test hadoop dfs
    [arguments]        ${prefix}
    ${random} =        Generate Random String  5  [NUMBERS]
    ${result} =        Execute on host            ${prefix}       hdfs dfs -put /opt/hadoop/NOTICE.txt o3fs://bucket1.vol1/${prefix}-${random}
    ${result} =        Execute on host            ${prefix}       hdfs dfs -ls o3fs://bucket1.vol1/
                       Should contain             ${result}       ${prefix}-${random}

*** Test Cases ***

Create bucket and volume to test
   ${result} =         Run tests on host      scm            createbucketenv.robot

Test hadoop 3.1
   Test hadoop dfs     hadoop31

Test hadoop 3.2
   Test hadoop dfs     hadoop31

Test hadoop 2.9
   Test hadoop dfs     hadoop29

Test hadoop 2.7
   Test hadoop dfs     hadoop27

Test spark 2.3
   ${legacyjar} =      Execute on host        spark         bash -c 'find /opt/ozone/share/ozone/lib/ -name *legacy*.jar'
   ${postfix} =        Generate Random String  5  [NUMBERS]
   ${result} =         Execute on host        spark         /opt/spark/bin/spark-submit --jars ${legacyjar} --class org.apache.spark.examples.DFSReadWriteTest /opt/spark//examples/jars/spark-examples_2.11-2.3.0.jar /opt/spark/README.md o3fs://bucket1.vol1/spark-${postfix}

