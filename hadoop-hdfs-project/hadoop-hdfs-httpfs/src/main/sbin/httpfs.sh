#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

MYNAME="${0##*/}"

## @description  Print usage
## @audience     private
## @stability    stable
## @replaceable  no
function print_usage
{
  cat <<EOF
Usage: ${MYNAME} run|start|status|stop
commands:
  run     Run HttpFS server, the HDFS HTTP Gateway
  start   Start HttpFS server as a daemon
  status  Return the status of the HttpFS server daemon
  stop    Stop the HttpFS server daemon
EOF
}

echo "WARNING: ${MYNAME} is deprecated," \
  "please use 'hdfs [--daemon start|status|stop] httpfs'." >&2

if [[ $# = 0 ]]; then
  print_usage
  exit
fi

case $1 in
  run)
    args=("httpfs")
  ;;
  start|stop|status)
    args=("--daemon" "$1" "httpfs")
  ;;
  *)
    echo "Unknown sub-command \"$1\"."
    print_usage
    exit 1
  ;;
esac

# Locate bin
if [[ -n "${HADOOP_HOME}" ]]; then
  bin="${HADOOP_HOME}/bin"
else
  sbin=$(cd -P -- "$(dirname -- "$0")" >/dev/null && pwd -P)
  bin=$(cd -P -- "${sbin}/../bin" >/dev/null && pwd -P)
fi

exec "${bin}/hdfs" "${args[@]}"
