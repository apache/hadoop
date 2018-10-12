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
Documentation       S3 gateway test with aws cli for bucket operations
Library             String
Library             OperatingSystem
Resource            commonawslib.robot

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${OZONE_TEST}         true
${BUCKET}             generated
${NONEXIST-BUCKET}    generated1
*** Keywords ***

Install aws s3 cli
                        Execute                    sudo apt-get install -y awscli
                        Set Environment Variable   AWS_ACCESS_KEY_ID       default
                        Set Environment Variable   AWS_SECRET_ACCESS_KEY   defaultsecret
    ${postfix1} =       Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${BUCKET}                  bucket-${postfix1}

Check Volume
    # as we know bucket to volume map. Volume name  bucket mapped is s3 + AWS_ACCESS_KEY_ID
    ${result} =         Execute                     ozone sh volume info /s3default
                        Should contain              ${result}         s3default
                        Should not contain          ${result}         VOLUME_NOT_FOUND

*** Test Cases ***

Setup s3 Tests
    Run Keyword if    '${OZONE_TEST}' == 'true'    Install aws s3 cli

Create Bucket
    ${result} =         Execute AWSS3APICli         create-bucket --bucket ${BUCKET}
                        Should contain              ${result}         ${BUCKET}
                        Should contain              ${result}         Location
    # create an already existing bucket
    ${result} =         Execute AWSS3APICli         create-bucket --bucket ${BUCKET}
                        Should contain              ${result}         ${BUCKET}
                        Should contain              ${result}         Location

    Run Keyword if     '${OZONE_TEST}' == 'true'    Check Volume

Head Bucket
    ${result} =         Execute AWSS3APICli     head-bucket --bucket ${BUCKET}
    ${result} =         Execute AWSS3APICli and checkrc      head-bucket --bucket ${NONEXIST-BUCKET}  255
                        Should contain          ${result}    Not Found
                        Should contain          ${result}    404
Delete Bucket
    ${result} =         Execute AWSS3APICli     head-bucket --bucket ${BUCKET}
    ${result} =         Execute AWSS3APICli and checkrc      delete-bucket --bucket ${NONEXIST-BUCKET}  255
                        Should contain          ${result}    NoSuchBucket