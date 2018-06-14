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

*** Keywords ***

Startup Ozone cluster with size
    [arguments]  ${datanodeno}
    ${rc}        ${output} =                 Run docker compose          down
                                             Run                         echo "Starting new docker-compose environment" >> docker-compose.log
    ${rc}        ${output} =                 Run docker compose          up -d
    Should Be Equal As Integers             ${rc}                       0
    Wait Until Keyword Succeeds             1min    5sec    Is Daemon started   ksm     HTTP server of KSM is listening
    Daemons are running without error
    Scale datanodes up                      5

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

Scale datanodes up
    [arguments]                     ${datanodeno}
    Run docker compose              scale datanode=${datanodeno}
    Wait Until Keyword Succeeds     3min    5sec    Have healthy datanodes   ${datanodeno}

Teardown Ozone cluster
    Run docker compose      down
    Run docker compose      logs >> docker-compose.log

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

Execute on
    [arguments]     ${componentname}    ${command}
    ${rc}           ${return} =         Run docker compose          exec -T ${componentname} ${command}
    [return]        ${return}

Run docker compose
    [arguments]                     ${command}
                                    Set Environment Variable    COMPOSE_INTERACTIVE_NO_CLI             1
                                    Set Environment Variable    OZONEDIR      ${PROJECTDIR}/hadoop-dist/target/ozone
    ${rc}                           ${output} =                 Run And Return Rc And Output           docker-compose -f ${COMPOSEFILE} ${command}
    Log                             ${output}
    Should Be Equal As Integers     ${rc}                       0
    [return]                            ${rc}                       ${output}
