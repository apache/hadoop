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
Library             String
Library             BuiltIn
Resource            ../commonlib.robot

*** Variables ***
${ENDPOINT_URL}    http://s3g:9878

*** Keywords ***
Setup volume names
    ${random}            Generate Random String  2   [NUMBERS]
    Set Suite Variable   ${volume1}            fstest${random}
    Set Suite Variable   ${volume2}            fstest2${random}
    Set Suite Variable   ${volume3}            fstest3${random}

*** Test Cases ***
Create volume bucket with wrong credentials
    Execute             kdestroy
    ${rc}               ${output} =          Run And Return Rc And Output       ozone sh volume create o3://om/fstest
    Should contain      ${output}       Client cannot authenticate via

Create volume bucket with credentials
                        # Authenticate testuser
    Run Keyword         Kinit test user     testuser     testuser.keytab
    Run Keyword         Setup volume names
    Execute             ozone sh volume create o3://om/${volume1} 
    Execute             ozone sh volume create o3://om/${volume2}
    Execute             ozone sh bucket create o3://om/${volume1}/bucket1
    Execute             ozone sh bucket create o3://om/${volume1}/bucket2
    Execute             ozone sh bucket create o3://om/${volume2}/bucket3

Check volume from ozonefs
    ${result} =         Execute          ozone fs -ls o3fs://bucket1.${volume1}/

Test Volume Acls
    ${result} =     Execute             ozone sh volume create ${volume3}
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh volume getacl ${volume3}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh volume addacl ${volume3} -a user:superuser1:rwxy[DEFAULT]
    ${result} =     Execute             ozone sh volume getacl ${volume3}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    ${result} =     Execute             ozone sh volume removeacl ${volume3} -a user:superuser1:xy
    ${result} =     Execute             ozone sh volume getacl ${volume3}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"READ\", \"WRITE\"
    ${result} =     Execute             ozone sh volume setacl ${volume3} -al user:superuser1:rwxy,user:testuser/scm@EXAMPLE.COM:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute             ozone sh volume getacl ${volume3}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"ALL\"

Test Bucket Acls
    ${result} =     Execute             ozone sh bucket create ${volume3}/bk1
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket getacl ${volume3}/bk1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh bucket addacl ${volume3}/bk1 -a user:superuser1:rwxy
    ${result} =     Execute             ozone sh bucket getacl ${volume3}/bk1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    ${result} =     Execute             ozone sh bucket removeacl ${volume3}/bk1 -a user:superuser1:xy
    ${result} =     Execute             ozone sh bucket getacl ${volume3}/bk1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\"
    ${result} =     Execute             ozone sh bucket setacl ${volume3}/bk1 -al user:superuser1:rwxy,group:superuser1:a,user:testuser/scm@EXAMPLE.COM:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute             ozone sh bucket getacl ${volume3}/bk1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\"

Test key Acls
    Execute            ozone sh key put ${volume3}/bk1/key1 /opt/hadoop/NOTICE.txt
    ${result} =     Execute             ozone sh key getacl ${volume3}/bk1/key1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh key addacl ${volume3}/bk1/key1 -a user:superuser1:rwxy
    ${result} =     Execute             ozone sh key getacl ${volume3}/bk1/key1
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    ${result} =     Execute             ozone sh key removeacl ${volume3}/bk1/key1 -a user:superuser1:xy
    ${result} =     Execute             ozone sh key getacl ${volume3}/bk1/key1
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\"
    ${result} =     Execute             ozone sh key setacl ${volume3}/bk1/key1 -al user:superuser1:rwxy,group:superuser1:a,user:testuser/scm@EXAMPLE.COM:rwxyc
    ${result} =     Execute             ozone sh key getacl ${volume3}/bk1/key1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\"

Test native authorizer
    Execute         ozone sh volume removeacl ${volume3} -a group:root:a
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser2    testuser2.keytab
    ${result} =     Execute And Ignore Error         ozone sh bucket list /${volume3}/    
                    Should contain      ${result}    PERMISSION_DENIED
    ${result} =     Execute And Ignore Error         ozone sh key list /${volume3}/bk1      
                    Should contain      ${result}    PERMISSION_DENIED
    ${result} =     Execute And Ignore Error         ozone sh volume addacl ${volume3} -a user:testuser2/scm@EXAMPLE.COM:xy
                    Should contain      ${result}    PERMISSION_DENIED User testuser2/scm@EXAMPLE.COM doesn't have WRITE_ACL permission to access volume
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser     testuser.keytab
    Execute         ozone sh volume addacl ${volume3} -a user:testuser2/scm@EXAMPLE.COM:xyrw
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser2    testuser2.keytab
    ${result} =     Execute And Ignore Error         ozone sh bucket list /${volume3}/
                    Should contain      ${result}    PERMISSION_DENIED org.apache.hadoop.ozone.om.exceptions.OMException: User testuser2/scm@EXAMPLE.COM doesn't have LIST permission to access volume
    Execute         ozone sh volume addacl ${volume3} -a user:testuser2/scm@EXAMPLE.COM:l
    Execute         ozone sh bucket list /${volume3}/
    Execute         ozone sh volume getacl /${volume3}/
    
    ${result} =     Execute And Ignore Error         ozone sh key list /${volume3}/bk1  
    Should contain      ${result}    PERMISSION_DENIED
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser     testuser.keytab
    Execute         ozone sh bucket addacl ${volume3}/bk1 -a user:testuser2/scm@EXAMPLE.COM:a
    Execute         ozone sh bucket getacl /${volume3}/bk1
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser2    testuser2.keytab
    Execute         ozone sh bucket getacl /${volume3}/bk1
    Execute         ozone sh key list /${volume3}/bk1
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser    testuser.keytab