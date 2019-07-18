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

if [[ "$#" != 1 ]] || [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
  echo "Usage: ./create-slim-hadoop-tar.sh path-to-hadoop-tar"
  echo "  Takes path-to-hadoop-tar as a hadoop.tar.gz binary distribution"
  echo "  and removes portions of it that are unnecessary for dynamometer"
  echo "  (e.g. unrelated components like YARN)."
  echo "  This overwrites the original file."
  echo "  This is idempotent; you can safely rerun it on the same tar."
  exit 1
fi

hadoopTar="$1"

# ls output is intended for human consumption
# shellcheck disable=SC2012
echo "Slimming $hadoopTar; size before is $(ls -lh "$hadoopTar" | awk '{ print $5 }')"

hadoopTarTmp="$hadoopTar.temporary"

mkdir -p "$hadoopTarTmp"

tar xzf "$hadoopTar" -C "$hadoopTarTmp"
baseDir="$(find -H "$hadoopTarTmp" -maxdepth 1 -mindepth 1 -type d | head -n 1)" # Should only be one subdir
hadoopShare="$baseDir/share/hadoop"

# Remove unnecessary files
rm -rf "${baseDir}/share/doc" "${hadoopShare}/mapreduce ${hadoopShare}/yarn" \
       "${hadoopShare}/kms" "${hadoopShare}/tools" "${hadoopShare}/httpfs" \
       "${hadoopShare}"/*/sources "${hadoopShare}"/*/jdiff

tar czf "$hadoopTarTmp.tar.gz" -C "$hadoopTarTmp" .
rm -rf "$hadoopTarTmp"
mv -f "$hadoopTarTmp.tar.gz" "$hadoopTar"

# ls output is intended for human consumption
# shellcheck disable=SC2012
echo "Finished; size after is $(ls -lh "$hadoopTar" | awk '{ print $5 }')"