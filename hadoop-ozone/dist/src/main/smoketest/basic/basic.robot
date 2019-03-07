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
Documentation       Smoketest ozone cluster startup
Library             OperatingSystem
Resource            ../commonlib.robot

*** Variables ***
${DATANODE_HOST}        datanode


*** Test Cases ***

Check webui static resources
    ${result} =        Execute                curl -s -I http://scm:9876/static/bootstrap-3.3.7/js/bootstrap.min.js
                       Should contain         ${result}    200
    ${result} =        Execute                curl -s -I http://om:9874/static/bootstrap-3.3.7/js/bootstrap.min.js
                       Should contain         ${result}    200

Start freon testing
    ${result} =        Execute              ozone freon randomkeys --numOfVolumes 5 --numOfBuckets 5 --numOfKeys 5 --numOfThreads 1
                       Wait Until Keyword Succeeds      3min       10sec     Should contain   ${result}   Number of Keys added: 125
                       Should Not Contain               ${result}  ERROR
