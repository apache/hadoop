#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../../testlib.sh"

start_docker_env

execute_robot_test scm createmrenv.robot


#rm is the container name (resource manager) and not the rm command
execute_command_in_container rm sudo apk add --update py-pip
execute_command_in_container rm sudo pip install robotframework

# reinitialize the directories to use
export OZONE_DIR=/opt/ozone
# shellcheck source=/dev/null
source "$COMPOSE_DIR/../../testlib.sh"

execute_robot_test rm ozonefs/hadoopo3fs.robot

execute_robot_test rm  -v hadoop.version:3.1.2 mapreduce.robot


stop_docker_env

generate_report
