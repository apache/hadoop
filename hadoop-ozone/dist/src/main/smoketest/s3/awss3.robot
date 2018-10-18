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
Resource            ./commonawslib.robot
Suite Setup         Setup s3 tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

File upload and directory list
                        Execute                   date > /tmp/testfile
    ${result} =         Execute AWSS3Cli          cp /tmp/testfile s3://${BUCKET}
                        Should contain            ${result}         upload
    ${result} =         Execute AWSS3Cli          cp /tmp/testfile s3://${BUCKET}/dir1/dir2/file
                        Should contain            ${result}         upload
    ${result} =         Execute AWSS3Cli          ls s3://${BUCKET}
                        Should contain            ${result}         testfile
                        Should contain            ${result}         dir1
                        Should not contain        ${result}         dir2
    ${result} =         Execute AWSS3Cli          ls s3://${BUCKET}/dir1/
                        Should not contain        ${result}         testfile
                        Should not contain        ${result}         dir1
                        Should contain            ${result}         dir2
    ${result} =         Execute AWSS3Cli          ls s3://${BUCKET}/dir1/dir2/
                        Should not contain        ${result}         testfile
                        Should not contain        ${result}         dir1
                        Should contain            ${result}         file
