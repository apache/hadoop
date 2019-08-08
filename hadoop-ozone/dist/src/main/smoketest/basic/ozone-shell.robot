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
Resource            ../commonlib.robot
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
Test Timeout        2 minute

*** Variables ***

*** Test Cases ***
RpcClient with port
   Test ozone shell       o3://            om:9862     rpcwoport

RpcClient volume acls
   Test Volume Acls       o3://            om:9862     rpcwoport2

RpcClient bucket acls
    Test Bucket Acls      o3://            om:9862     rpcwoport2

RpcClient key acls
    Test Key Acls         o3://            om:9862     rpcwoport2

RpcClient without host
    Test ozone shell      o3://            ${EMPTY}    rpcwport

RpcClient without scheme
    Test ozone shell      ${EMPTY}         ${EMPTY}    rpcwoscheme


*** Keywords ***
Test ozone shell
    [arguments]     ${protocol}         ${server}       ${volume}
    ${result} =     Execute             ozone sh volume create ${protocol}${server}/${volume} --quota 100TB
                    Should not contain  ${result}       Failed
                    Should contain      ${result}       Creating Volume: ${volume}
    ${result} =     Execute             ozone sh volume list ${protocol}${server}/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.volumeName=="${volume}")'
                    Should contain      ${result}       createdOn
    ${result} =     Execute             ozone sh volume list | grep -Ev 'Removed|DEBUG|ERROR|INFO|TRACE|WARN' | jq -r '.[] | select(.volumeName=="${volume}")'
                    Should contain      ${result}       createdOn
# TODO: Disable updating the owner, acls should be used to give access to other user.        
                    Execute             ozone sh volume update ${protocol}${server}/${volume} --quota 10TB
#    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="${volume}") | .owner | .name'
#                    Should Be Equal     ${result}       bill
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.volumeName=="${volume}") | .quota | .size'
                    Should Be Equal     ${result}       10
                    Execute             ozone sh bucket create ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.bucketName=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute             ozone sh bucket list ${protocol}${server}/${volume}/ | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.bucketName=="bb1") | .volumeName'
                    Should Be Equal     ${result}       ${volume}
                    Run Keyword         Test key handling       ${protocol}       ${server}       ${volume}
                    Execute             ozone sh bucket delete ${protocol}${server}/${volume}/bb1
                    Execute             ozone sh volume delete ${protocol}${server}/${volume}

Test Volume Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute         ozone sh volume create ${protocol}${server}/${volume}
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh volume addacl ${protocol}${server}/${volume} -a user:superuser1:rwxy[DEFAULT]
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\" .
    ${result} =     Execute             ozone sh volume removeacl ${protocol}${server}/${volume} -a user:superuser1:xy
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\" .
    ${result} =     Execute             ozone sh volume setacl ${protocol}${server}/${volume} -al user:superuser1:rwxy,group:superuser1:a,user:testuser/scm@EXAMPLE.COM:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\" .
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"ALL\" .

Test Bucket Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute             ozone sh bucket create ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh bucket addacl ${protocol}${server}/${volume}/bb1 -a user:superuser1:rwxy
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    ${result} =     Execute             ozone sh bucket removeacl ${protocol}${server}/${volume}/bb1 -a user:superuser1:xy
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\"
    ${result} =     Execute             ozone sh bucket setacl ${protocol}${server}/${volume}/bb1 -al user:superuser1:rwxy,group:superuser1:a,user:testuser/scm@EXAMPLE.COM:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"ALL\" .


Test key handling
    [arguments]     ${protocol}         ${server}       ${volume}
                    Execute             ozone sh key put ${protocol}${server}/${volume}/bb1/key1 /opt/hadoop/NOTICE.txt
                    Execute             rm -f NOTICE.txt.1
                    Execute             ozone sh key get ${protocol}${server}/${volume}/bb1/key1 NOTICE.txt.1
                    Execute             ls -l NOTICE.txt.1
    ${result} =     Execute             ozone sh key info ${protocol}${server}/${volume}/bb1/key1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.keyName=="key1")'
                    Should contain      ${result}       createdOn
    ${result} =     Execute             ozone sh key list ${protocol}${server}/${volume}/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[] | select(.keyName=="key1") | .keyName'
                    Should Be Equal     ${result}       key1
                    Execute             ozone sh key rename ${protocol}${server}/${volume}/bb1 key1 key2
    ${result} =     Execute             ozone sh key list ${protocol}${server}/${volume}/bb1 | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '.[].keyName'
                    Should Be Equal     ${result}       key2
                    Execute             ozone sh key delete ${protocol}${server}/${volume}/bb1/key2

Test key Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute         ozone sh key put ${protocol}${server}/${volume}/bb1/key2 /opt/hadoop/NOTICE.txt
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh key addacl ${protocol}${server}/${volume}/bb1/key2 -a user:superuser1:rwxy
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    ${result} =     Execute             ozone sh key removeacl ${protocol}${server}/${volume}/bb1/key2 -a user:superuser1:xy
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\"
    ${result} =     Execute             ozone sh key setacl ${protocol}${server}/${volume}/bb1/key2 -al user:superuser1:rwxy,group:superuser1:a,user:testuser/scm@EXAMPLE.COM:rwxyc
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .