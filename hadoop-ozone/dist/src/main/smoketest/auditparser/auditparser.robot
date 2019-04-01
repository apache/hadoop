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
Documentation       Smoketest ozone cluster startup
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot

*** Variables ***
${user}        hadoop
${count}       4

*** Keywords ***
Set username
    ${hostname} =          Execute         hostname
    Set Suite Variable     ${user}         testuser/${hostname}@EXAMPLE.COM
    [return]               ${user}

*** Test Cases ***
Initiating freon to generate data
    ${result} =        Execute              ozone freon randomkeys --numOfVolumes 5 --numOfBuckets 5 --numOfKeys 5 --numOfThreads 1
                       Wait Until Keyword Succeeds      3min       10sec     Should contain   ${result}   Number of Keys added: 125
                       Should Not Contain               ${result}  ERROR

Testing audit parser
    ${logfile} =       Execute              ls -t /opt/hadoop/logs | grep om-audit | head -1
                       Execute              ozone auditparser /opt/hadoop/audit.db load "/opt/hadoop/logs/${logfile}"
    ${result} =        Execute              ozone auditparser /opt/hadoop/audit.db template top5cmds
                       Should Contain       ${result}  ALLOCATE_KEY
    ${result} =        Execute              ozone auditparser /opt/hadoop/audit.db template top5users
    Run Keyword If     '${SECURITY_ENABLED}' == 'true'      Set username
                       Should Contain       ${result}  ${user}
    ${result} =        Execute              ozone auditparser /opt/hadoop/audit.db query "select count(*) from audit where op='CREATE_VOLUME' and RESULT='SUCCESS'"
    ${result} =        Convert To Number     ${result}
                       Should be true       ${result}>${count}
    ${result} =        Execute              ozone auditparser /opt/hadoop/audit.db query "select count(*) from audit where op='CREATE_BUCKET' and RESULT='SUCCESS'"
    ${result} =        Convert To Number     ${result}
                       Should be true       ${result}>${count}
