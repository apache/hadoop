#!/bin/bash -ex

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

# Run a script downloaded at boot time to avoid Rackspace's 10K limitation.

wget -qO/usr/bin/runurl run.alestic.com/runurl
chmod 755 /usr/bin/runurl
%ENV% runurl http://hadoop-dev-test.s3.amazonaws.com/boot-rackspace.sh