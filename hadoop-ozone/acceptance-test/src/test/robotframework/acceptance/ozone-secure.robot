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

Daemons are running
    Is daemon running           om
    Is daemon running           scm
    Is daemon running           datanode
    Is daemon running           ozone.kdc

Check if datanode is connected to the scm
    Wait Until Keyword Succeeds     3min    5sec    Have healthy datanodes   1

Test rest interface
    ${result} =     Execute on      0   datanode        curl -i -X POST ${COMMON_RESTHEADER} "http://localhost:9880/volume1"
                    Should contain      ${result}       201 Created
    ${result} =     Execute on      0   datanode        curl -i -X POST ${COMMON_RESTHEADER} "http://localhost:9880/volume1/bucket1"
                    Should contain      ${result}       201 Created
    ${result} =     Execute on      0   datanode        curl -i -X DELETE ${COMMON_RESTHEADER} "http://localhost:9880/volume1/bucket1"
                    Should contain      ${result}       200 OK
    ${result} =     Execute on      0   datanode        curl -i -X DELETE ${COMMON_RESTHEADER} "http://localhost:9880/volume1"
                    Should contain      ${result}       200 OK

Test ozone cli
    ${result} =     Execute on      1   datanode        ozone oz -createVolume o3://om/hive -user bilbo -quota 100TB -root
                    Should contain      ${result}       Client cannot authenticate via
                    # Authenticate testuser
                    Execute on      0   datanode        kinit -k testuser/datanode@EXAMPLE.COM -t /etc/security/keytabs/testuser.keytab
                    Execute on      0   datanode        ozone oz -createVolume o3://om/hive -user bilbo -quota 100TB -root
    ${result} =     Execute on      0   datanode        ozone oz -listVolume o3://om/ -user bilbo | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="hive")'
                    Should contain      ${result}       createdOn
                    Execute on      0   datanode        ozone oz -updateVolume o3://om/hive -user bill -quota 10TB
    ${result} =     Execute on      0   datanode        ozone oz -infoVolume o3://om/hive | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="hive") | .owner | .name'
                    Should Be Equal     ${result}       bill

*** Keywords ***

Startup Ozone Cluster
    ${rc}       ${output} =                 Run docker compose        0     down
    ${rc}       ${output} =                 Run docker compose        0     up -d
    Should Be Equal As Integers 	          ${rc} 	                  0
    Wait Until Keyword Succeeds             3min    10sec    Is Daemon started   ksm     KSM is listening

Teardown Ozone Cluster
    Run docker compose      0           down

Is daemon running
    [arguments]             ${name}
    ${result} =             Run                     docker ps
    Should contain          ${result}               _${name}_1

Is Daemon started
    [arguments]     ${name}             ${expression}
    ${rc}           ${result} =         Run docker compose      0         logs
    Should contain  ${result}           ${expression}

Have healthy datanodes
    [arguments]         ${requirednodes}
    ${result} =         Execute on     0     scm                 curl -s 'http://localhost:9876/jmx?qry=Hadoop:service=SCMNodeManager,name=SCMNodeManagerInfo' | jq -r '.beans[0].NodeCount[] | select(.key=="HEALTHY") | .value'
    Should Be Equal     ${result}           ${requirednodes}

Execute on
    [arguments]     ${expected_rc}      ${componentname}      ${command}
    ${rc}           ${return} =         Run docker compose    ${expected_rc}     exec ${componentname} ${command}
    [return]        ${return}

Run docker compose
    [arguments]                     ${expected_rc}              ${command}
                                    Set Environment Variable    OZONEDIR                               ${basedir}/hadoop-dist/target/ozone
    ${rc}                           ${output} =                 Run And Return Rc And Output           docker-compose -f ${basedir}/hadoop-ozone/acceptance-test/src/test/compose/compose-secure/docker-compose.yaml ${command}
    Should Be Equal As Integers     ${rc}                       ${expected_rc}
    [return]                        ${rc}                       ${output}
