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
Documentation       S3 gateway test with aws cli
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Setup          Setup s3 tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated
${DESTBUCKET}         generated1


*** Keywords ***
Create Dest Bucket

    ${postfix} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable   ${DESTBUCKET}             destbucket-${postfix}
    Execute AWSS3APICli  create-bucket --bucket ${DESTBUCKET}


*** Test Cases ***
Copy Object Happy Scenario
    Run Keyword if    '${DESTBUCKET}' == 'generated1'    Create Dest Bucket
                        Execute                    date > /tmp/copyfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key copyobject/f1 --body /tmp/copyfile
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix copyobject/
                        Should contain             ${result}         f1

    ${result} =         Execute AWSS3ApiCli        copy-object --bucket ${DESTBUCKET} --key copyobject/f1 --copy-source ${BUCKET}/copyobject/f1
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${DESTBUCKET} --prefix copyobject/
                        Should contain             ${result}         f1
    #copying again will not throw error
    ${result} =         Execute AWSS3ApiCli        copy-object --bucket ${DESTBUCKET} --key copyobject/f1 --copy-source ${BUCKET}/copyobject/f1
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${DESTBUCKET} --prefix copyobject/
                        Should contain             ${result}         f1

Copy Object Where Bucket is not available
    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket dfdfdfdfdfnonexistent --key copyobject/f1 --copy-source ${BUCKET}/copyobject/f1      255
                        Should contain             ${result}        NoSuchBucket
    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket ${DESTBUCKET} --key copyobject/f1 --copy-source dfdfdfdfdfnonexistent/copyobject/f1  255
                        Should contain             ${result}        NoSuchBucket

Copy Object Where both source and dest are same with change to storageclass
     ${result} =         Execute AWSS3APICli        copy-object --storage-class REDUCED_REDUNDANCY --bucket ${DESTBUCKET} --key copyobject/f1 --copy-source ${DESTBUCKET}/copyobject/f1
                         Should contain             ${result}        ETag

Copy Object Where Key not available
    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket ${DESTBUCKET} --key copyobject/f1 --copy-source ${BUCKET}/nonnonexistentkey       255
                        Should contain             ${result}        NoSuchKey
