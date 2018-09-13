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
Documentation       Test ozone shell CLI usage
Library             OperatingSystem
Suite Setup         Startup Ozone cluster with size          5
Suite Teardown      Teardown Ozone cluster
Resource            ../commonlib.robot
Test Timeout        2 minute

*** Variables ***
${COMPOSEFILE}          ${CURDIR}/docker-compose.yaml
${PROJECTDIR}           ${CURDIR}/../../../../../..

*** Test Cases ***
RestClient without http port
   Test ozone shell       http://          ozoneManager          restwoport

RestClient with http port
   Test ozone shell       http://          ozoneManager:9874     restwport

RestClient without host name
   Test ozone shell       http://          ${EMPTY}              restwohost

RpcClient with port
   Test ozone shell       o3://            ozoneManager:9862     rpcwoport

RpcClient without host
   Test ozone shell       o3://            ${EMPTY}              rpcwport

RpcClient without scheme
   Test ozone shell       ${EMPTY}         ${EMPTY}              rpcwoscheme


*** Keywords ***
Test ozone shell
    [arguments]     ${protocol}         ${server}       ${volume}
    ${result} =     Execute on          datanode        ozone oz volume create ${protocol}${server}/${volume} --user bilbo --quota 100TB --root
                    Should not contain  ${result}       Failed
                    Should contain      ${result}       Creating Volume: ${volume}
    ${result} =     Execute on          datanode        ozone oz volume list ${protocol}${server}/ --user bilbo | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="${volume}")'
                    Should contain      ${result}       createdOn
    ${result} =     Execute on          datanode        ozone oz volume list --user bilbo | grep -Ev 'Removed|DEBUG|ERROR|INFO|TRACE|WARN' | jq -r '.[] | select(.volumeName=="${volume}")'
                    Should contain      ${result}       createdOn
                    Execute on          datanode        ozone oz volume update ${protocol}${server}/${volume} --user bill --quota 10TB
    ${result} =     Execute on          datanode        ozone oz volume info ${protocol}${server}/${volume} | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="${volume}") | .owner | .name'
                    Should Be Equal     ${result}       bill
    ${result} =     Execute on          datanode        ozone oz volume info ${protocol}${server}/${volume} | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="${volume}") | .quota | .size'
                    Should Be Equal     ${result}       10
                    Execute on          datanode        ozone oz bucket create ${protocol}${server}/${volume}/bb1
    ${result} =     Execute on          datanode        ozone oz bucket info ${protocol}${server}/${volume}/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute on          datanode        ozone oz bucket update ${protocol}${server}/${volume}/bb1 --addAcl user:frodo:rw,group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="samwise") | .type'
                    Should Be Equal     ${result}       GROUP
    ${result} =     Execute on          datanode        ozone oz bucket update ${protocol}${server}/${volume}/bb1 --removeAcl group:samwise:r | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .acls | .[] | select(.name=="frodo") | .type'
                    Should Be Equal     ${result}       USER
    ${result} =     Execute on          datanode        ozone oz bucket list ${protocol}${server}/${volume}/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       ${volume}
                    Run Keyword         Test key handling       ${protocol}       ${server}       ${volume}
                    Execute on          datanode        ozone oz bucket delete ${protocol}${server}/${volume}/bb1
                    Execute on          datanode        ozone oz volume delete ${protocol}${server}/${volume} --user bilbo

Test key handling
    [arguments]     ${protocol}         ${server}       ${volume}
                    Execute on          datanode        ozone oz key put ${protocol}${server}/${volume}/bb1/key1 NOTICE.txt
                    Execute on          datanode        rm -f NOTICE.txt.1
                    Execute on          datanode        ozone oz key get ${protocol}${server}/${volume}/bb1/key1 NOTICE.txt.1
                    Execute on          datanode        ls -l NOTICE.txt.1
    ${result} =     Execute on          datanode        ozone oz key info ${protocol}${server}/${volume}/bb1/key1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.keyName=="key1")'
                    Should contain      ${result}       createdOn
    ${result} =     Execute on          datanode        ozone oz key list ${protocol}${server}/${volume}/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.keyName=="key1") | .keyName'
                    Should Be Equal     ${result}       key1
                    Execute on          datanode        ozone oz key delete ${protocol}${server}/${volume}/bb1/key1
