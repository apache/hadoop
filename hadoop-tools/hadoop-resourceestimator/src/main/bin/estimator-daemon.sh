#!/usr/bin/env bash
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "$bin" || exit; pwd)

# get arguments
startStop=$1
shift
command=$1
shift

pid=/tmp/hadoop-dogtail-estimator.pid

LOG_DIR=$bin/../../../../../logs
if [ ! -w "$LOG_DIR" ] ; then
  mkdir -p "$LOG_DIR"
fi

log=$LOG_DIR/hadoop-resourceestimator.out

case $startStop in

  (start)

    if [ -f $pid ]; then
      # shellcheck disable=SC2046
      if kill -0 $(cat $pid) > /dev/null 2>&1; then
        echo "$command running as process $(cat $pid).  Stop it first."
        exit 1
      fi
    fi

    echo "starting $command, logging to $log"
    bin/estimator.sh "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1
    head "$log"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_PID=$(cat $pid)
      # shellcheck disable=SC2086
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        kill "$TARGET_PID"
        sleep 5
        # shellcheck disable=SC2086
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "$command did not stop gracefully after 5 seconds: killing with kill -9"
          "kill -9 $TARGET_PID"
        fi
      else
        echo "no $command to stop"
      fi
      rm -f $pid
    else
      echo "no $command to stop"
    fi
    ;;

  (*)
    exit 1
    ;;

esac