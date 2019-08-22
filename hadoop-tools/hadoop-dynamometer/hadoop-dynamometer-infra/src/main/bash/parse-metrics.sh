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

# This script is used for fetching the standard Hadoop metrics which the
# Dynamometer NameNode generates during its execution (standard Hadoop metrics).
# Those metrics are uploaded onto HDFS when the Dynamometer application completes.
# This script will download them locally and parse out the specified metric for
# the given time period. This is useful to, for example, isolate only the metrics
# produced during the workload replay portion of a job. For this, specify startTimeMs
# as the start time of the workload job (which it logs during execution) and
# periodMinutes the period (in minutes) of the replay.

if [ $# -lt 5 ]; then
  echo "Usage:"
  echo "./parse-metrics.sh applicationID outputFileName startTimeMs periodMinutes metricName [ context ] [ isCounter ]"
  echo "If no file namenode_metrics_{applicationID} is present in the working directory,"
  echo "attempts to download one from HDFS for applicationID. Filters values"
  echo "for the specified metric, during the range"
  echo "(startTimeMs, startTimeMs + periodMinutes) optionally filtering on the context as well"
  echo "(which is just applied as a regex search across the metric line output)"
  echo "and outputs CSV pairs of (seconds_since_start_time,value)."
  echo "If isCounter is true, treats the metrics as a counter and outputs per-second rate values."
  exit 1
fi

appId="$1"
output="$2"
start_ts="$3"
period_minutes="$4"
metric="$5"
context="$6"
is_counter="$7"

localFile="namenode_metrics_$appId"
if [ ! -f "$localFile" ]; then
  remoteFile=".dynamometer/$appId/namenode_metrics"
  echo "Downloading file from HDFS: $remoteFile"
  if ! hdfs dfs -copyToLocal "$remoteFile" "$localFile"; then
    exit 1
  fi
fi

read -d '' -r awk_script <<'EOF'
BEGIN {
    metric_regex="[[:space:]]"metric"=([[:digit:].E]+)";
    end_ts=start_ts+(period_minutes*60*1000)
    last_val=0
    last_ts=start_ts
}
"true" ~ is_counter && $0 ~ metric_regex && $0 ~ context && $1 < start_ts {
    match($0, metric_regex, val_arr);
    last_val=val_arr[1]
    last_ts=$1
}
$0 ~ metric_regex && $0 ~ context && $1 >= start_ts && $1 <= end_ts {
    match($0, metric_regex, val_arr);
    val=val_arr[1]
    if (is_counter == "true") {
      tmp=val
      val=val-last_val
      val=val/(($1-last_ts)/1000)
      last_ts=$1
      last_val=tmp
    }
    printf("%.0f,%.6f\n", ($0-start_ts)/1000, val)
}
EOF

gawk -v metric="$metric" -v context="$context" -v start_ts="$start_ts" \
  -v period_minutes="$period_minutes" -v is_counter="$is_counter" -v OFS="," "$awk_script" "$localFile" > "$output"
