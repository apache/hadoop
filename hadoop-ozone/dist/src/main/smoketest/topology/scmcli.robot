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
Library             BuiltIn
Resource            ../commonlib.robot

*** Variables ***


*** Test Cases ***
Run printTopology
    ${output} =         Execute          ozone scmcli printTopology
                        Should contain   ${output}         10.5.0.7(ozone-topology_datanode_4_1.ozone-topology_net)    /rack2
Run printTopology -o
    ${output} =         Execute          ozone scmcli printTopology -o
                        Should contain   ${output}         Location: /rack2
                        Should contain   ${output}         10.5.0.7(ozone-topology_datanode_4_1.ozone-topology_net)
