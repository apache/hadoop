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
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

OUTPUT_FILE="$DIR/../../../target/shell-problems.txt"
mkdir -p "$(dirname "$OUTPUT_FILE")"
echo "" > "$OUTPUT_FILE"
if [[ "$(uname -s)" = "Darwin" ]]; then
  find hadoop-hdds hadoop-ozone -type f -perm '-500'
else
  find hadoop-hdds hadoop-ozone -type f -executable
fi \
  | grep -v -e target/ -e node_modules/ -e '\.\(ico\|py\|yml\)$' \
  | xargs -n1 shellcheck \
  | tee "$OUTPUT_FILE"

if [ "$(cat "$OUTPUT_FILE")" ]; then
   exit 1
fi
