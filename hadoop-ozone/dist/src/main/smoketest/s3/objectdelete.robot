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

*** Test Cases ***
Delete file with s3api
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key deletetestapi/f1 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix deletetestapi/
                        Should contain             ${result}         f1
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key deletetestapi/f1
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix deletetestapi/
                        Should not contain         ${result}         f1
#In case of HTTP 500, the error code is printed out to the console.
                        Should not contain         ${result}         500

Delete file with s3api, file doesn't exist
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/
                        Should not contain         ${result}         thereisnosuchfile
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key thereisnosuchfile
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/
                        Should not contain         ${result}         thereisnosuchfile

Delete dir with s3api
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3Cli           cp /tmp/testfile s3://${BUCKET}/deletetestapidir/f1
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/deletetestapidir/
                        Should contain             ${result}         f1
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key deletetestapidir/
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/deletetestapidir/
                        Should contain             ${result}         f1
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key deletetestapidir/f1


Delete file with s3api, file doesn't exist, prefix of a real file
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3Cli           cp /tmp/testfile s3://${BUCKET}/deletetestapiprefix/filefile
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/deletetestapiprefix/
                        Should contain             ${result}         filefile
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key deletetestapiprefix/file
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/deletetestapiprefix/
                        Should contain             ${result}         filefile
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key deletetestapiprefix/filefile



Delete file with s3api, bucket doesn't exist
    ${result} =         Execute AWSS3APICli and checkrc   delete-object --bucket ${BUCKET}-nosuchbucket --key f1      255
                        Should contain                    ${result}         NoSuchBucket
