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
Documentation       Create directories required for MR test
Library             OperatingSystem
Resource            commonlib.robot
Test Timeout        2 minute


*** Variables ***
${volume}       vol1
${bucket}       bucket1


*** Keywords ***
Create volume
    ${result} =     Execute             ozone sh volume create /${volume} --user hadoop --quota 100TB
                    Should not contain  ${result}       Failed
                    Should contain      ${result}       Creating Volume: ${volume}
Create bucket
                    Execute             ozone sh bucket create /${volume}/${bucket}

*** Test Cases ***
Create test volume, bucket and key
    ${result} =     Execute And Ignore Error             ozone sh bucket info /${volume}/${bucket}
                    Run Keyword if      "VOLUME_NOT_FOUND" in """${result}"""       Create volume
                    Run Keyword if      "VOLUME_NOT_FOUND" in """${result}"""       Create bucket
                    Run Keyword if      "BUCKET_NOT_FOUND" in """${result}"""       Create bucket
    ${result} =     Execute             ozone sh bucket info /${volume}/${bucket}
                    Should not contain  ${result}  NOT_FOUND
                    Execute             ozone sh key put /vol1/bucket1/key1 LICENSE.txt

Create user dir for hadoop
         Execute        ozone fs -mkdir /user
         Execute        ozone fs -mkdir /user/hadoop
