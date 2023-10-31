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

# This script will determine the timestamp of the last transaction appearing in a
# given fsimage by looking at the corresponding edits file. This is useful to determine
# from whence to start collecting audit logs to replay against the fsimage.

if [ $# -lt 1 ] || [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
  echo "Usage: ./parse-start-timestamp.sh image-txid [ edits-dir ]"
  echo "       Finds the last timestamp present in the edit file which ends in"
  echo "       the specified transaction ID (leading 0s not required)."
  echo "       If edits-dir is specified, looks for edit files under"
  echo "       edits-dir/current. Otherwise, looks in the current directory."
  exit 1
fi
if [[ $(command -v gawk) == "" ]]; then
  echo "This script requires gawk to be available."
  exit 1
fi
image_txid="$1"
if [[ $# -ge 2 ]]; then
  edits_dir="$2/current"
else
  edits_dir="$(pwd)"
fi

edits_file_count="$(find -H "${edits_dir}" -maxdepth 1 -type f -name "edits_*-*$image_txid" | wc -l)"
if [[ "$edits_file_count" != 1 ]]; then
  echo "Error; found $edits_file_count matching edit files."
  exit 1
fi
edits_file="$(find -H "${edits_dir}" -maxdepth 1 -type f -name "edits_*-*$image_txid")"

# Shellcheck complains about the $ in the single-quote because it won't expand, but this is intentional
# shellcheck disable=SC2016
awk_script='/TIMESTAMP/ { line=$0 } END { match(line, />([[:digit:]]+)</, output); print output[1] }'
echo "Start timestamp for $image_txid is: (this may take a moment)"
hdfs oev -i "$edits_dir/$edits_file" -o >(gawk "$awk_script")
