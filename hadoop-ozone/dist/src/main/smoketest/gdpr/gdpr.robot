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
Documentation       Smoketest Ozone GDPR Feature
Library             OperatingSystem
Library             BuiltIn
Library             String
Resource            ../commonlib.robot
Suite Setup         Generate volume

*** Variables ***
${volume}    generated

*** Keywords ***
Generate volume
   ${random} =         Generate Random String  5  [LOWER]
   Set Suite Variable  ${volume}  ${random}

*** Test Cases ***
Test GDPR disabled
  Test GDPR(disabled) without explicit options      ${volume}

Test GDPR --enforcegdpr=true
  Test GDPR with --enforcegdpr=true                 ${volume}

Test GDPR -g=true
  Test GDPR with -g=true                            ${volume}

Test GDPR -g=false
  Test GDPR with -g=false                            ${volume}

*** Keywords ***
Test GDPR(disabled) without explicit options
    [arguments]     ${volume}
                    Execute             ozone sh volume create /${volume} --quota 100TB
                    Execute             ozone sh bucket create /${volume}/mybucket1
    ${result} =     Execute             ozone sh bucket info /${volume}/mybucket1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="mybucket1") | .metadata | .gdprEnabled'
                    Should Be Equal     ${result}       null
                    Execute             ozone sh key put /${volume}/mybucket1/mykey /opt/hadoop/NOTICE.txt
                    Execute             rm -f NOTICE.txt.1
    ${result} =     Execute             ozone sh key info /${volume}/mybucket1/mykey | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="mykey") | .metadata | .gdprEnabled'
                    Should Be Equal     ${result}       null
                    Execute             ozone sh key delete /${volume}/mybucket1/mykey

Test GDPR with --enforcegdpr=true
    [arguments]     ${volume}
                    Execute             ozone sh bucket create --enforcegdpr=true /${volume}/mybucket2
    ${result} =     Execute             ozone sh bucket info /${volume}/mybucket2 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="mybucket2") | .metadata | .gdprEnabled'
                    Should Be Equal     ${result}       true
                    Execute             ozone sh key put /${volume}/mybucket2/mykey /opt/hadoop/NOTICE.txt
                    Execute             rm -f NOTICE.txt.1
    ${result} =     Execute             ozone sh key info /${volume}/mybucket2/mykey | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="mykey") | .metadata | .gdprEnabled'
                    Should Be Equal     ${result}       true
                    Execute             ozone sh key delete /${volume}/mybucket2/mykey

Test GDPR with -g=true
    [arguments]     ${volume}
                    Execute             ozone sh bucket create -g=true /${volume}/mybucket3
    ${result} =     Execute             ozone sh bucket info /${volume}/mybucket3 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="mybucket3") | .metadata | .gdprEnabled'
                    Should Be Equal     ${result}       true
                    Execute             ozone sh key put /${volume}/mybucket3/mykey /opt/hadoop/NOTICE.txt
                    Execute             rm -f NOTICE.txt.1
    ${result} =     Execute             ozone sh key info /${volume}/mybucket3/mykey | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="mykey") | .metadata | .gdprEnabled'
                    Should Be Equal     ${result}       true
                    Execute             ozone sh key delete /${volume}/mybucket3/mykey

Test GDPR with -g=false
    [arguments]     ${volume}
                    Execute             ozone sh bucket create /${volume}/mybucket4
    ${result} =     Execute             ozone sh bucket info /${volume}/mybucket4 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="mybucket4") | .metadata | .gdprEnabled'
                    Should Be Equal     ${result}       null
                    Execute             ozone sh key put /${volume}/mybucket4/mykey /opt/hadoop/NOTICE.txt
                    Execute             rm -f NOTICE.txt.1
    ${result} =     Execute             ozone sh key info /${volume}/mybucket4/mykey | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="mykey") | .metadata | .gdprEnabled'
                    Should Be Equal     ${result}       null
                    Execute             ozone sh key delete /${volume}/mybucket4/mykey
