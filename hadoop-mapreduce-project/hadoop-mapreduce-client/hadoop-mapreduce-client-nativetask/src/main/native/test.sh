#!/bin/sh
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# only do normal tests by default
FILTER="--gtest_filter=-Perf.*"

# do all tests
if [ "$1" = "all" ]; then
  shift
  FILTER=""
fi

# do performance tests only
if [ "$1" = "perf" ]; then
  shift
  FILTER="--gtest_filter=Perf.*"
fi

if [ "${SYSTEM_MAC}" = "TRUE" ]; then
  # MACOSX already setup RPATH, no extra help required
  ./nttest $FILTER $@
else
  JAVA_JVM_LIBRARY_DIR=`dirname ${JAVA_JVM_LIBRARY}`
  LD_LIBRARY_PATH=$JAVA_JVM_LIBRARY_DIR:$LD_LIBRARY_PATH ./nttest $FILTER $@
fi

