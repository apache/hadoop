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
Suite Setup         Startup Ozone cluster with size          5
Suite Teardown      Teardown Ozone cluster
Resource            ../commonlib.robot

*** Variables ***
${COMMON_REST_HEADER}   -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H  "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE root"
${COMPOSEFILE}          ${CURDIR}/docker-compose.yaml
${PROJECTDIR}           ${CURDIR}/../../../../../..


*** Test Cases ***

Test rest interface
    ${result} =     Execute on          datanode        curl -i -X POST ${COMMON_RESTHEADER} "http://localhost:9880/volume1"
                    Should contain      ${result}       201 Created
    ${result} =     Execute on          datanode        curl -i -X POST ${COMMON_RESTHEADER} "http://localhost:9880/volume1/bucket1"
                    Should contain      ${result}       201 Created
    ${result} =     Execute on          datanode        curl -i -X DELETE ${COMMON_RESTHEADER} "http://localhost:9880/volume1/bucket1"
                    Should contain      ${result}       200 OK
    ${result} =     Execute on          datanode        curl -i -X DELETE ${COMMON_RESTHEADER} "http://localhost:9880/volume1"
                    Should contain      ${result}       200 OK

Check webui static resources
    ${result} =			Execute on		scm		curl -s -I http://localhost:9876/static/bootstrap-3.3.7/js/bootstrap.min.js
	 Should contain		${result}		200
    ${result} =			Execute on		ksm		curl -s -I http://localhost:9874/static/bootstrap-3.3.7/js/bootstrap.min.js
	 Should contain		${result}		200

Start freon testing
    ${result} =		Execute on		ksm		ozone freon -numOfVolumes 5 -numOfBuckets 5 -numOfKeys 5 -numOfThreads 10
	 Wait Until Keyword Succeeds	3min	10sec		Should contain		${result}		Number of Keys added: 125
	 Should Not Contain		${result}		ERROR
