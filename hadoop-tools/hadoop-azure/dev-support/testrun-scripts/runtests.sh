#!/usr/bin/env bash

# shellcheck disable=SC2034
# unused variables are global in nature and used in testsupport.sh

set -eo pipefail

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

# shellcheck disable=SC1091
. dev-support/testrun-scripts/testsupport.sh

begin

### ADD THE TEST COMBINATIONS BELOW. DO NOT EDIT THE ABOVE LINES.


combination=HNS-OAuth
properties=("fs.azure.abfs.account.name" "fs.azure.test.namespace.enabled"
"fs.azure.account.auth.type")
values=("{account name}.dfs.core.windows.net" "true" "OAuth")
generateconfigs

combination=HNS-SharedKey
properties=("fs.azure.abfs.account.name" "fs.azure.test.namespace.enabled" "fs.azure.account.auth.type")
values=("{account name}.dfs.core.windows.net" "true" "SharedKey")
generateconfigs

combination=NonHNS-SharedKey
properties=("fs.azure.abfs.account.name" "fs.azure.test.namespace.enabled" "fs.azure.account.auth.type")
values=("{account name}.dfs.core.windows.net" "false" "SharedKey")
generateconfigs


### DO NOT EDIT THE LINES BELOW.

runtests "$@"
