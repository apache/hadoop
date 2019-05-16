#!/usr/bin/env bash
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
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SRC_DIR="$SCRIPT_DIR/runConfigurations"
DEST_DIR="$SCRIPT_DIR/../../../.idea/runConfigurations/"
mkdir -p "$DEST_DIR"
#shellcheck disable=SC2010
ls -1 "$SRC_DIR" | grep -v ozone-site.xml | xargs -n1 -I FILE cp "$SRC_DIR/FILE" "$DEST_DIR"
