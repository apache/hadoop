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
Documentation       Ozonefs test
Library             OperatingSystem
Suite Setup         Startup Ozone cluster with size          5
Suite Teardown      Teardown Ozone cluster
Resource            ../commonlib.robot

*** Variables ***
${COMPOSEFILE}          ${CURDIR}/docker-compose.yaml
${PROJECTDIR}           ${CURDIR}/../../../../../..


*** Test Cases ***
Create volume and bucket
    Execute on          datanode        ozone oz -createVolume http://ozoneManager/fstest -user bilbo -quota 100TB -root
    Execute on          datanode        ozone oz -createBucket http://ozoneManager/fstest/bucket1

Check volume from ozonefs
    ${result} =         Execute on          datanode          ozone fs -ls o3://bucket1.fstest/

Create directory from ozonefs
                        Execute on          datanode          ozone fs -mkdir -p o3://bucket1.fstest/testdir/deep
    ${result} =         Execute on          ozoneManager      ozone oz -listKey o3://ozoneManager/fstest/bucket1 | grep -v WARN | jq -r '.[].keyName'
                                            Should contain    ${result}         testdir/deep
