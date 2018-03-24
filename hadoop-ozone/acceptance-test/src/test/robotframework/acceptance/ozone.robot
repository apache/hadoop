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
${COMMON_REST_HEADER}   -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H  "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE root"
${version}

*** Test Cases ***

Daemons are running without error
    Is daemon running without error           ksm
    Is daemon running without error           scm
    Is daemon running without error           namenode
    Is daemon running without error           datanode

Check if datanode is connected to the scm
    Wait Until Keyword Succeeds     2min    5sec    Have healthy datanodes   1

Scale it up to 5 datanodes
    Scale datanodes up  5
    Wait Until Keyword Succeeds     3min    5sec    Have healthy datanodes   5

Test rest interface
    ${result} =     Execute on          datanode        curl -i -X POST ${COMMON_RESTHEADER} "http://localhost:9880/volume1"
                    Should contain      ${result}       201 Created
    ${result} =     Execute on          datanode        curl -i -X POST ${COMMON_RESTHEADER} "http://localhost:9880/volume1/bucket1"
                    Should contain      ${result}       201 Created
    ${result} =     Execute on          datanode        curl -i -X DELETE ${COMMON_RESTHEADER} "http://localhost:9880/volume1/bucket1"
                    Should contain      ${result}       200 OK
    ${result} =     Execute on          datanode        curl -i -X DELETE ${COMMON_RESTHEADER} "http://localhost:9880/volume1"
                    Should contain      ${result}       200 OK

Test oz cli
                    Execute on          datanode        oz oz -createVolume http://localhost:9880/hive -user bilbo -quota 100TB -root
    ${result} =     Execute on          datanode        oz oz -listVolume http://localhost:9880/ -user bilbo | grep -v Removed | jq '.[] | select(.volumeName=="hive")'
                    Should contain      ${result}       createdOn
                    Execute on          datanode        oz oz -createBucket http://localhost:9880/hive/bb1
    ${result}       Execute on          datanode        oz oz -listBucket http://localhost:9880/hive/ | grep -v Removed | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       hive
                    Execute on          datanode        oz oz -deleteBucket http://localhost:9880/hive/bb1
                    Execute on          datanode        oz oz -deleteVolume http://localhost:9880/hive -user bilbo



Check webui static resources
    ${result} =			Execute on		scm		curl -s -I http://localhost:9876/static/bootstrap-3.0.2/js/bootstrap.min.js
	 Should contain		${result}		200
    ${result} =			Execute on		ksm		curl -s -I http://localhost:9874/static/bootstrap-3.0.2/js/bootstrap.min.js
	 Should contain		${result}		200

Start freon testing
    ${result} =		Execute on		ksm		oz freon -numOfVolumes 5 -numOfBuckets 5 -numOfKeys 5 -numOfThreads 10
	 Wait Until Keyword Succeeds	3min	10sec		Should contain		${result}		Number of Keys added: 125
	 Should Not Contain		${result}		ERROR

*** Keywords ***

Startup Ozone Cluster
    ${rc}       ${output} =                 Run docker compose          down
    ${rc}       ${output} =                 Run docker compose          up -d
    Should Be Equal As Integers 	        ${rc} 	                    0
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
                                    Set Environment Variable    HADOOPDIR                              ${basedir}/../../hadoop-dist/target/hadoop-${version}
    ${rc}                           ${output} =                 Run And Return Rc And Output           docker-compose -f ${basedir}/target/compose/docker-compose.yaml ${command}
    Should Be Equal As Integers     ${rc}                       0
    [return]                            ${rc}                       ${output}
