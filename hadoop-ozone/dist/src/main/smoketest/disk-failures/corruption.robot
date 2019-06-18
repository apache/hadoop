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
Documentation       Test disk corruption
Library             OperatingSystem
Test Timeout        5 minute
Resource            ../commonlib.robot

*** Variables ***
${volume}       vol1
${bucket}       bucket1

*** Keywords ***
Remove write permission
   Execute       ls -1 /data/metadata/scm.db | xargs -n1 -I FILE bash -c "sudo chown 999:999 /data/metadata/scm.db/FILE"
   Execute       ls -1 /data/metadata/scm.db | xargs -n1 -I FILE bash -c "sudo chmod 700 /data/metadata/scm.db/FILE"


Test upload failure
   ${result} =      Execute      ozone fs -get o3fs://bucket1.fstest/KEY.txt GET.txt

*** Test Cases ***

Corruption test
                    Ensure Bucket and Volume are created
                    Execute                  echo "testcontent" > /tmp/key
                    Execute                  ozone sh key put /vol1/bucket1/KEY1 /tmp/key
                    Create key with content  /vol1/bucket1/KEY1       CONTENT
                    Remove write permission
    ${output} =     Execute and checkrc      ozone sh key put /vol1/bucket1/KEY2 /tmp/key       255
                    Should contain           ${output}                "ERROR"
