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

*** Keywords ***
Create Random file for mac
    Execute                 dd if=/dev/urandom of=/tmp/part1 bs=1m count=5

Create Random file for linux
    Execute                 dd if=/dev/urandom of=/tmp/part1 bs=1M count=5


*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

Test Multipart Upload
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key multipartKey --storage-class REDUCED_REDUNDANCY
    ${uploadID} =       Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    multipartKey
                        Should contain          ${result}    UploadId
# initiate again
    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${BUCKET} --key multipartKey --storage-class REDUCED_REDUNDANCY
    ${nextUploadID} =   Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
                        Should contain          ${result}    ${BUCKET}
                        Should contain          ${result}    multipartKey
                        Should contain          ${result}    UploadId
                        Should Not Be Equal     ${uploadID}  ${nextUploadID}

# upload part
# each part should be minimum 5mb, other wise during complete multipart
# upload we get error entity too small. So, considering further complete
# multipart upload, uploading each part as 5MB file, exception is for last part

	${system} =         Evaluate    platform.system()    platform
	Run Keyword if      '${system}' == 'Darwin'  Create Random file for mac
	Run Keyword if      '${system}' == 'Linux'   Create Random file for linux
	${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key multipartKey --part-number 1 --body /tmp/part1 --upload-id ${nextUploadID}
	                    Should contain          ${result}    ETag
# override part
	Run Keyword if      '${system}' == 'Darwin'    Create Random file for mac
	Run Keyword if      '${system}' == 'Linux'     Create Random file for linux
	${result} =         Execute AWSS3APICli     upload-part --bucket ${BUCKET} --key multipartKey --part-number 1 --body /tmp/part1 --upload-id ${nextUploadID}
	                    Should contain          ${result}    ETag

Upload part with Incorrect uploadID
	                    Execute                 echo "Multipart upload" > /tmp/testfile
	    ${result} =     Execute AWSS3APICli and checkrc     upload-part --bucket ${BUCKET} --key multipartKey --part-number 1 --body /tmp/testfile --upload-id "random"  255
	                    Should contain          ${result}    NoSuchUpload