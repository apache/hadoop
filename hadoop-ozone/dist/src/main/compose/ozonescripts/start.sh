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
docker-compose ps | grep datanode | awk '{print $1}' | xargs -n1  docker inspect --format '{{ .Config.Hostname }}' > ../../etc/hadoop/workers
docker-compose exec scm /opt/hadoop/bin/ozone scm --init
docker-compose exec scm /opt/hadoop/sbin/start-ozone.sh
#We need a running SCM for om objectstore creation
#TODO create a utility to wait for the startup
sleep 10
docker-compose exec om /opt/hadoop/bin/ozone om --init
docker-compose exec scm /opt/hadoop/sbin/start-ozone.sh
