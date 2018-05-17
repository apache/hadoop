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
Documentation       Smoke test to start cluster with docker-compose environments.
Library             OperatingSystem
Suite Setup         Startup Ozone Cluster
Suite Teardown      Teardown Ozone Cluster

*** Variables ***
${basedir}
*** Test Cases ***

Daemons are running without error
    Is daemon running without error           ksm
    Is daemon running without error           scm
    Is daemon running without error           namenode
    Is daemon running without error           datanode

Check if datanode is connected to the scm
    Wait Until Keyword Succeeds     3min    5sec    Have healthy datanodes   1

Scale it up to 5 datanodes
    Scale datanodes up  5
    Wait Until Keyword Succeeds     3min    5sec    Have healthy datanodes   5

Test ozone shell (RestClient without http port)
                    Execute on          datanode        ozone oz -createVolume http://ksm/hive -user bilbo -quota 100TB -root
    ${result} =     Execute on          datanode        ozone oz -listVolume o3://ksm -user bilbo | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="hive")'
                    Should contain      ${result}       createdOn
                    Execute on          datanode        ozone oz -updateVolume http://ksm/hive -user bill -quota 10TB
    ${result} =     Execute on          datanode        ozone oz -infoVolume http://ksm/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .owner | .name'
                    Should Be Equal     ${result}       bill
    ${result} =     Execute on          datanode        ozone oz -infoVolume http://ksm/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .quota | .size'
                    Should Be Equal     ${result}       10
                    Execute on          datanode        ozone oz -createBucket http://ksm/hive/bb1
    ${result} =     Execute on          datanode        ozone oz -infoBucket http://ksm/hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute on          datanode        ozone oz -updateBucket http://ksm/hive/bb1 -addAcl user:frodo:rw,group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="samwise") | .type'
                    Should Be Equal     ${result}       GROUP
    ${result} =     Execute on          datanode        ozone oz -updateBucket http://ksm/hive/bb1 -removeAcl group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="frodo") | .type'
                    Should Be Equal     ${result}       USER
    ${result} =     Execute on          datanode        ozone oz -listBucket o3://ksm/hive/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       hive
                    Execute on          datanode        ozone oz -putKey http://ksm/hive/bb1/key1 -file NOTICE.txt
                    Execute on          datanode        rm -f NOTICE.txt.1
                    Execute on          datanode        ozone oz -getKey http://ksm/hive/bb1/key1 -file NOTICE.txt.1
                    Execute on          datanode        ls -l NOTICE.txt.1
    ${result} =     Execute on          datanode        ozone oz -infoKey http://ksm/hive/bb1/key1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.keyName=="key1")'
                    Should contain      ${result}       createdOn
    ${result} =     Execute on          datanode        ozone oz -listKey o3://ksm/hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.keyName=="key1") | .keyName'
                    Should Be Equal     ${result}       key1
                    Execute on          datanode        ozone oz -deleteKey http://ksm/hive/bb1/key1 -v
                    Execute on          datanode        ozone oz -deleteBucket http://ksm/hive/bb1
                    Execute on          datanode        ozone oz -deleteVolume http://ksm/hive -user bilbo

Test ozone shell (RestClient with http port)
                    Execute on          datanode        ozone oz -createVolume http://ksm:9874/hive -user bilbo -quota 100TB -root
    ${result} =     Execute on          datanode        ozone oz -listVolume o3://ksm:9862 -user bilbo | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="hive")'
                    Should contain      ${result}       createdOn
                    Execute on          datanode        ozone oz -updateVolume http://ksm:9874/hive -user bill -quota 10TB
    ${result} =     Execute on          datanode        ozone oz -infoVolume http://ksm:9874/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .owner | .name'
                    Should Be Equal     ${result}       bill
    ${result} =     Execute on          datanode        ozone oz -infoVolume http://ksm:9874/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .quota | .size'
                    Should Be Equal     ${result}       10
                    Execute on          datanode        ozone oz -createBucket http://ksm:9874/hive/bb1
    ${result} =     Execute on          datanode        ozone oz -infoBucket http://ksm:9874/hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute on          datanode        ozone oz -updateBucket http://ksm:9874/hive/bb1 -addAcl user:frodo:rw,group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="samwise") | .type'
                    Should Be Equal     ${result}       GROUP
    ${result} =     Execute on          datanode        ozone oz -updateBucket http://ksm:9874/hive/bb1 -removeAcl group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="frodo") | .type'
                    Should Be Equal     ${result}       USER
    ${result} =     Execute on          datanode        ozone oz -listBucket o3://ksm:9862/hive/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       hive
                    Execute on          datanode        ozone oz -putKey http://ksm:9874/hive/bb1/key1 -file NOTICE.txt
                    Execute on          datanode        rm -f NOTICE.txt.1
                    Execute on          datanode        ozone oz -getKey http://ksm:9874/hive/bb1/key1 -file NOTICE.txt.1
                    Execute on          datanode        ls -l NOTICE.txt.1
    ${result} =     Execute on          datanode        ozone oz -infoKey http://ksm:9874/hive/bb1/key1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.keyName=="key1")'
                    Should contain      ${result}       createdOn
    ${result} =     Execute on          datanode        ozone oz -listKey o3://ksm:9862/hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.keyName=="key1") | .keyName'
                    Should Be Equal     ${result}       key1
                    Execute on          datanode        ozone oz -deleteKey http://ksm:9874/hive/bb1/key1 -v
                    Execute on          datanode        ozone oz -deleteBucket http://ksm:9874/hive/bb1
                    Execute on          datanode        ozone oz -deleteVolume http://ksm:9874/hive -user bilbo

Test ozone shell (RestClient without hostname)
                    Execute on          datanode        ozone oz -createVolume http:///hive -user bilbo -quota 100TB -root
    ${result} =     Execute on          datanode        ozone oz -listVolume o3:/// -user bilbo | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="hive")'
                    Should contain      ${result}       createdOn
                    Execute on          datanode        ozone oz -updateVolume http:///hive -user bill -quota 10TB
    ${result} =     Execute on          datanode        ozone oz -infoVolume http:///hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .owner | .name'
                    Should Be Equal     ${result}       bill
    ${result} =     Execute on          datanode        ozone oz -infoVolume http:///hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .quota | .size'
                    Should Be Equal     ${result}       10
                    Execute on          datanode        ozone oz -createBucket http:///hive/bb1
    ${result} =     Execute on          datanode        ozone oz -infoBucket http:///hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute on          datanode        ozone oz -updateBucket http:///hive/bb1 -addAcl user:frodo:rw,group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="samwise") | .type'
                    Should Be Equal     ${result}       GROUP
    ${result} =     Execute on          datanode        ozone oz -updateBucket http:///hive/bb1 -removeAcl group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="frodo") | .type'
                    Should Be Equal     ${result}       USER
    ${result} =     Execute on          datanode        ozone oz -listBucket o3:///hive/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       hive
                    Execute on          datanode        ozone oz -putKey http:///hive/bb1/key1 -file NOTICE.txt
                    Execute on          datanode        rm -f NOTICE.txt.1
                    Execute on          datanode        ozone oz -getKey http:///hive/bb1/key1 -file NOTICE.txt.1
                    Execute on          datanode        ls -l NOTICE.txt.1
    ${result} =     Execute on          datanode        ozone oz -infoKey http:///hive/bb1/key1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.keyName=="key1")'
                    Should contain      ${result}       createdOn
    ${result} =     Execute on          datanode        ozone oz -listKey o3:///hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.keyName=="key1") | .keyName'
                    Should Be Equal     ${result}       key1
                    Execute on          datanode        ozone oz -deleteKey http:///hive/bb1/key1 -v
                    Execute on          datanode        ozone oz -deleteBucket http:///hive/bb1
                    Execute on          datanode        ozone oz -deleteVolume http:///hive -user bilbo

Test ozone shell (RpcClient without http port)
                    Execute on          datanode        ozone oz -createVolume o3://ksm/hive -user bilbo -quota 100TB -root
    ${result} =     Execute on          datanode        ozone oz -listVolume o3://ksm -user bilbo | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="hive")'
                    Should contain      ${result}       createdOn
                    Execute on          datanode        ozone oz -updateVolume o3://ksm/hive -user bill -quota 10TB
    ${result} =     Execute on          datanode        ozone oz -infoVolume o3://ksm/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .owner | .name'
                    Should Be Equal     ${result}       bill
    ${result} =     Execute on          datanode        ozone oz -infoVolume o3://ksm/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .quota | .size'
                    Should Be Equal     ${result}       10
                    Execute on          datanode        ozone oz -createBucket o3://ksm/hive/bb1
    ${result} =     Execute on          datanode        ozone oz -infoBucket o3://ksm/hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute on          datanode        ozone oz -updateBucket o3://ksm/hive/bb1 -addAcl user:frodo:rw,group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="samwise") | .type'
                    Should Be Equal     ${result}       GROUP
    ${result} =     Execute on          datanode        ozone oz -updateBucket o3://ksm/hive/bb1 -removeAcl group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="frodo") | .type'
                    Should Be Equal     ${result}       USER
    ${result} =     Execute on          datanode        ozone oz -listBucket o3://ksm/hive/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       hive
                    Execute on          datanode        ozone oz -deleteBucket o3://ksm/hive/bb1
                    Execute on          datanode        ozone oz -deleteVolume o3://ksm/hive -user bilbo

Test ozone shell (RpcClient with http port)
                    Execute on          datanode        ozone oz -createVolume o3://ksm:9862/hive -user bilbo -quota 100TB -root
    ${result} =     Execute on          datanode        ozone oz -listVolume o3://ksm:9862 -user bilbo | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="hive")'
                    Should contain      ${result}       createdOn
                    Execute on          datanode        ozone oz -updateVolume o3://ksm:9862/hive -user bill -quota 10TB
    ${result} =     Execute on          datanode        ozone oz -infoVolume o3://ksm:9862/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .owner | .name'
                    Should Be Equal     ${result}       bill
    ${result} =     Execute on          datanode        ozone oz -infoVolume o3://ksm:9862/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .quota | .size'
                    Should Be Equal     ${result}       10
                    Execute on          datanode        ozone oz -createBucket o3://ksm:9862/hive/bb1
    ${result} =     Execute on          datanode        ozone oz -infoBucket o3://ksm:9862/hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute on          datanode        ozone oz -updateBucket o3://ksm:9862/hive/bb1 -addAcl user:frodo:rw,group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="samwise") | .type'
                    Should Be Equal     ${result}       GROUP
    ${result} =     Execute on          datanode        ozone oz -updateBucket o3://ksm:9862/hive/bb1 -removeAcl group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="frodo") | .type'
                    Should Be Equal     ${result}       USER
    ${result} =     Execute on          datanode        ozone oz -listBucket o3://ksm:9862/hive/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       hive
                    Execute on          datanode        ozone oz -deleteBucket o3://ksm:9862/hive/bb1
                    Execute on          datanode        ozone oz -deleteVolume o3://ksm:9862/hive -user bilbo

Test ozone shell (RpcClient without hostname)
                    Execute on          datanode        ozone oz -createVolume o3:///hive -user bilbo -quota 100TB -root
    ${result} =     Execute on          datanode        ozone oz -listVolume o3:/// -user bilbo | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="hive")'
                    Should contain      ${result}       createdOn
                    Execute on          datanode        ozone oz -updateVolume o3:///hive -user bill -quota 10TB
    ${result} =     Execute on          datanode        ozone oz -infoVolume o3:///hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .owner | .name'
                    Should Be Equal     ${result}       bill
    ${result} =     Execute on          datanode        ozone oz -infoVolume o3:///hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .quota | .size'
                    Should Be Equal     ${result}       10
                    Execute on          datanode        ozone oz -createBucket o3:///hive/bb1
    ${result} =     Execute on          datanode        ozone oz -infoBucket o3:///hive/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute on          datanode        ozone oz -updateBucket o3:///hive/bb1 -addAcl user:frodo:rw,group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="samwise") | .type'
                    Should Be Equal     ${result}       GROUP
    ${result} =     Execute on          datanode        ozone oz -updateBucket o3:///hive/bb1 -removeAcl group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="frodo") | .type'
                    Should Be Equal     ${result}       USER
    ${result} =     Execute on          datanode        ozone oz -listBucket o3:///hive/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       hive
                    Execute on          datanode        ozone oz -deleteBucket o3:///hive/bb1
                    Execute on          datanode        ozone oz -deleteVolume o3:///hive -user bilbo

*** Keywords ***

Startup Ozone Cluster
    ${rc}       ${output} =                 Run docker compose          down
    ${rc}       ${output} =                 Run docker compose          up -d
    Should Be Equal As Integers             ${rc} 	                    0
    Wait Until Keyword Succeeds             1min    5sec    Is Daemon started   ksm     HTTP server of KSM is listening

Teardown Ozone Cluster
    Run docker compose      down

Is daemon running without error
    [arguments]             ${name}
    ${result} =             Run                     docker ps
    Should contain          ${result}               _${name}_1
    ${rc}                   ${result} =             Run docker compose      logs ${name}
    Should not contain      ${result}               ERROR

Is Daemon started
    [arguments]     ${name}             ${expression}
    ${rc}           ${result} =         Run docker compose      logs
    Should contain  ${result}           ${expression}

Have healthy datanodes
    [arguments]         ${requirednodes}
    ${result} =         Execute on          scm                 curl -s 'http://localhost:9876/jmx?qry=Hadoop:service=SCMNodeManager,name=SCMNodeManagerInfo' | jq -r '.beans[0].NodeCount[] | select(.key=="HEALTHY") | .value'
    Should Be Equal     ${result}           ${requirednodes}

Scale datanodes up
    [arguments]              ${requirednodes}
    Run docker compose       scale datanode=${requirednodes}

Execute on
    [arguments]     ${componentname}    ${command}
    ${rc}           ${return} =         Run docker compose          exec ${componentname} ${command}
    [return]        ${return}

Run docker compose
    [arguments]                     ${command}
                                    Set Environment Variable    OZONEDIR                               ${basedir}/hadoop-dist/target/ozone
    ${rc}                           ${output} =                 Run And Return Rc And Output           docker-compose -f ${basedir}/hadoop-ozone/acceptance-test/src/test/compose/docker-compose.yaml ${command}
    Log                             ${output}
    Should Be Equal As Integers     ${rc}                       0
    [return]                            ${rc}                       ${output}
