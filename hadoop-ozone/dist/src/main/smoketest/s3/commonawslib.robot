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
Resource            ../commonlib.robot

*** Variables ***
${OZONE_S3_HEADER_VERSION}     v2
${OZONE_S3_SET_CREDENTIALS}    true

*** Keywords ***
Execute AWSS3APICli
    [Arguments]       ${command}
    ${output} =       Execute          aws s3api --endpoint-url ${ENDPOINT_URL} ${command}
    [return]          ${output}

Execute AWSS3APICli and checkrc
    [Arguments]       ${command}             ${expected_error_code}
    ${output} =       Execute and checkrc    aws s3api --endpoint-url ${ENDPOINT_URL} ${command}  ${expected_error_code}
    [return]          ${output}

Execute AWSS3Cli
    [Arguments]       ${command}
    ${output} =       Execute          aws s3 --endpoint-url ${ENDPOINT_URL} ${command}
    [return]          ${output}

Install aws cli
    ${rc}              ${output} =                 Run And Return Rc And Output           which apt-get
    Run Keyword if     '${rc}' == '0'              Install aws cli s3 debian
    ${rc}              ${output} =                 Run And Return Rc And Output           yum --help
    Run Keyword if     '${rc}' == '0'              Install aws cli s3 centos


Install aws cli s3 centos
    Execute                    sudo yum install -y awscli
Install aws cli s3 debian
    Execute                    sudo apt-get install -y awscli

Setup v2 headers
                        Set Environment Variable   AWS_ACCESS_KEY_ID       ANYID
                        Set Environment Variable   AWS_SECRET_ACCESS_KEY   ANYKEY

Setup v4 headers
                        Execute                    aws configure set default.s3.signature_version s3v4
                        Execute                    aws configure set aws_access_key_id default1
                        Execute                    aws configure set aws_secret_access_key defaultsecret
                        Execute                    aws configure set region us-west-1
Create bucket
    ${postfix} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable   ${BUCKET}                  bucket-${postfix}
    Execute AWSS3APICli  create-bucket --bucket ${BUCKET}

Setup credentials
    Run Keyword if    '${OZONE_S3_HEADER_VERSION}' == 'v4'       Setup v4 headers
    Run Keyword if    '${OZONE_S3_HEADER_VERSION}' != 'v4'       Setup v2 headers

Setup s3 tests
    Run Keyword        Install aws cli
    Run Keyword if    '${OZONE_S3_SET_CREDENTIALS}' == 'true'    Setup credentials
    Run Keyword if    '${BUCKET}' == 'generated'                 Create bucket
