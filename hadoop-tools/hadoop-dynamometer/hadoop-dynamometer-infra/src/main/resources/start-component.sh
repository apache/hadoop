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

# Script for launching Dynamometer components within YARN containers.
# USAGE:
# ./start-component.sh namenode hdfs_storage
# OR
# ./start-component.sh datanode nn_servicerpc_address sleep_time_sec
# First parameter should be component being launched, either `datanode` or `namenode`
# If component is namenode, hdfs_storage is expected to point to a location to
#   write out shared files such as the file containing the information about
#   which ports the NN started on (at nn_info.prop) and the namenode's metrics
#   (at namenode_metrics)
# If component is datanode, nn_servicerpc_address is expected to point to the
#   servicerpc address of the namenode. sleep_time_sec is the amount of time that
#   should be allowed to elapse before launching anything. The
#   `org.apache.hadoop.tools.dynamometer.SimulatedDataNodes` class will be used to start multiple
#   DataNodes within the same JVM, and they will store their block files in memory.

component="$1"
if [[ "$component" != "datanode" && "$component" != "namenode" ]]; then
  echo "Unknown component type: '${component}'"
  exit 1
fi
if [[ "$component" = "namenode" ]]; then
  if [[ $# -lt 2 ]]; then
    echo "Not enough arguments for NameNode"
    exit 1
  fi
  hdfsStoragePath="$2"
else
  if [[ $# -lt 3 ]]; then
    echo "Not enough arguments for DataNode"
    exit 1
  fi
  nnServiceRpcAddress="$2"
  launchDelaySec="$3"
fi
containerID=${CONTAINER_ID##*_}

echo "Starting ${component} with ID ${containerID}"
echo "PWD is: $(pwd)"

confDir="$(pwd)/conf/etc/hadoop"
umask 022
baseDir="$(pwd)/dyno-node"

# Set Hadoop's log dir to that of the NodeManager,
# then YARN will automatically help us handle the logs
# May be a comma-separated list; just take the first one
logDir=${LOG_DIRS%%,*}

pidDir="$baseDir/pid"
baseHttpPort=50075
baseRpcPort=9000
baseServiceRpcPort=9020

rm -rf "$baseDir"
mkdir -p "$pidDir"
chmod 755 "$baseDir"
chmod 700 "$pidDir"

# Set Hadoop variables for component
hadoopHome="$(find -H "$(pwd)/hadoopBinary" -maxdepth 1 -mindepth 1 -type d | head -n 1)"
# Save real environment for later
hadoopConfOriginal=${HADOOP_CONF_DIR:-$confDir}
hadoopHomeOriginal=${HADOOP_HOME:-$hadoopHome}
echo "Saving original HADOOP_HOME as: $hadoopHomeOriginal"
echo "Saving original HADOOP_CONF_DIR as: $hadoopConfOriginal"
## @description  A function to perform an HDFS command under the system Hadoop
##               instead of the Hadoop-under-test.
## @audience     private
## @stability    evolving
function hdfs_original {
  HADOOP_HOME="${hadoopHomeOriginal}" HADOOP_CONF_DIR="${hadoopConfOriginal}" \
  HADOOP_HDFS_HOME="${hadoopHomeOriginal}" HADOOP_COMMON_HOME="${hadoopHomeOriginal}" \
  "${hadoopHomeOriginal}/bin/hdfs" "$@"
}

extraClasspathDir="$(pwd)/additionalClasspath/"
mkdir -p "${extraClasspathDir}"

# DataNodes need junit jar to run SimulatedDataNodes
junitClassPath="$(find "${hadoopHome}" -name "junit*.jar" | head -n 1)"
if [[ -z "$junitClassPath" ]]; then
  echo "Can't find junit jar file in ${hadoopHome}."
  exit 1
fi

# Change environment variables for the Hadoop process
export HADOOP_HOME="$hadoopHome"
export HADOOP_PREFIX="$hadoopHome"
export PATH="$HADOOP_HOME/bin:$PATH"
export HADOOP_HDFS_HOME="$hadoopHome"
export HADOOP_COMMON_HOME="$hadoopHome"
export HADOOP_YARN_HOME="$hadoopHome"
export LIBHDFS_OPTS="-Djava.library.path=$hadoopHome/lib/native"
export HADOOP_MAPRED_HOME="$hadoopHome"
export HADOOP_CONF_DIR="${confDir}"
export YARN_CONF_DIR="${confDir}"
export HADOOP_LOG_DIR="${logDir}"
export HADOOP_PID_DIR="${pidDir}"
HADOOP_CLASSPATH="$(pwd)/dependencies/*:$extraClasspathDir:$junitClassPath"
export HADOOP_CLASSPATH
echo "Environment variables are set as:"
echo "(note that this doesn't include changes made by hadoop-env.sh)"
printenv
echo -e "\n\n"

# Starting from base_port, add the last two digits of the containerID,
# then keep searching upwards for a free port
# find_available_port base_port
find_available_port() {
  basePort="$1"
  currPort=$((basePort+((10#$containerID)%100)))
  while netstat -nl | grep -q ":${currPort}[[:space:]]"; do
    currPort=$((currPort+1))
  done
  echo "$currPort"
}

configOverrides=(
  -D "hadoop.tmp.dir=${baseDir}"
  -D "hadoop.security.authentication=simple"
  -D "hadoop.security.authorization=false"
  -D "dfs.http.policy=HTTP_ONLY"
  -D "dfs.nameservices="
  -D "dfs.web.authentication.kerberos.principal="
  -D "dfs.web.authentication.kerberos.keytab="
  -D "hadoop.http.filter.initializers="
  -D "dfs.datanode.kerberos.principal="
  -D "dfs.datanode.keytab.file="
  -D "dfs.domain.socket.path="
  -D "dfs.client.read.shortcircuit=false"
)
# NOTE: Must manually unset dfs.namenode.shared.edits.dir in configs
#       because setting it to be empty is not enough (must be null)

if [[ "$component" = "datanode" ]]; then

  if ! dataDirsOrig="$(hdfs getconf "${configOverrides[@]}" -confKey dfs.datanode.data.dir)"; then
    echo "Unable to fetch data directories from config; using default"
    dataDirsOrig="/data-dir/1,/data-dir/2"
  fi
  dataDirsOrig=(${dataDirsOrig//,/ })
  dataDirs=""
  for dataDir in "${dataDirsOrig[@]}"; do
    stripped="file://$baseDir/${dataDir#file://}"
    dataDirs="$dataDirs,$stripped"
  done
  dataDirs=${dataDirs:1}

  echo "Going to sleep for $launchDelaySec sec..."
  for _ in $(seq 1 "${launchDelaySec}"); do
    sleep 1
    if ! kill -0 $PPID 2>/dev/null; then
      echo "Parent process ($PPID) exited while waiting; now exiting"
      exit 0
     fi
  done

  versionFile="$(pwd)/VERSION"
  bpId="$(grep "${versionFile}" -e blockpoolID | awk -F= '{print $2}')"
  listingFiles=()
  blockDir="$(pwd)/blocks"
  for listingFile in "${blockDir}"/*; do
    listingFiles+=("file://${listingFile}")
  done

  datanodeClusterConfigs=(
    -D "fs.defaultFS=${nnServiceRpcAddress}"
    -D "dfs.datanode.hostname=$(hostname)"
    -D "dfs.datanode.data.dir=${dataDirs}"
    -D "dfs.datanode.ipc.address=0.0.0.0:0"
    -D "dfs.datanode.http.address=0.0.0.0:0"
    -D "dfs.datanode.address=0.0.0.0:0"
    -D "dfs.datanode.directoryscan.interval=-1"
    -D "fs.du.interval=43200000"
    -D "fs.getspaceused.jitterMillis=21600000"
    ${configOverrides[@]}
    "${bpId}"
    ${listingFiles[@]}
  )

  echo "Executing the following:"
  echo "${HADOOP_HOME}/bin/hadoop org.apache.hadoop.tools.dynamometer.SimulatedDataNodes \
    $DN_ADDITIONAL_ARGS" "${datanodeClusterConfigs[@]}"
  # The argument splitting of DN_ADDITIONAL_ARGS is desirable behavior here
  # shellcheck disable=SC2086
  "${HADOOP_HOME}/bin/hadoop" org.apache.hadoop.tools.dynamometer.SimulatedDataNodes \
    $DN_ADDITIONAL_ARGS "${datanodeClusterConfigs[@]}" &
  launchSuccess="$?"
  componentPID="$!"
  if [[ ${launchSuccess} -ne 0 ]]; then
    echo "Unable to launch DataNode cluster; exiting."
    exit 1
  fi

elif [[ "$component" = "namenode" ]]; then

  nnHostname="${NM_HOST}"
  nnRpcPort="$(find_available_port "$baseRpcPort")"
  nnServiceRpcPort="$(find_available_port "$baseServiceRpcPort")"
  nnHttpPort="$(find_available_port "$baseHttpPort")"

  nnInfoLocalPath="$(pwd)/nn_info.prop"
  rm -f "$nnInfoLocalPath"
  # Port and host information to be uploaded to the non-Dynamometer HDFS
  # to be consumed by the AM and Client
  cat > "$nnInfoLocalPath" << EOF
NN_HOSTNAME=${nnHostname}
NN_RPC_PORT=${nnRpcPort}
NN_SERVICERPC_PORT=${nnServiceRpcPort}
NN_HTTP_PORT=${nnHttpPort}
NM_HTTP_PORT=${NM_HTTP_PORT}
CONTAINER_ID=${CONTAINER_ID}
EOF
  echo "Using the following ports for the namenode:"
  cat "$nnInfoLocalPath"
  nnInfoRemotePath="$hdfsStoragePath/nn_info.prop"
  # We use the original conf dir since we are uploading to the non-dynamometer cluster
  hdfs_original dfs -copyFromLocal -f "$nnInfoLocalPath" "$nnInfoRemotePath"
  echo "Uploaded namenode port info to $nnInfoRemotePath"

  if [[ "$NN_FILE_METRIC_PERIOD" -gt 0 ]]; then
    nnMetricOutputFileLocal="$HADOOP_LOG_DIR/namenode_metrics"
    nnMetricPropsFileLocal="$extraClasspathDir/hadoop-metrics2-namenode.properties"
    if [[ -f "$confDir/hadoop-metrics2-namenode.properties" ]]; then
      cp "$confDir/hadoop-metrics2-namenode.properties" "$nnMetricPropsFileLocal"
      chmod u+w "$nnMetricPropsFileLocal"
    elif [[ -f "$confDir/hadoop-metrics2.properties" ]]; then
      cp "$confDir/hadoop-metrics2.properties" "$nnMetricPropsFileLocal"
      chmod u+w "$nnMetricPropsFileLocal"
    fi
    cat >> "$nnMetricPropsFileLocal" << EOF
namenode.sink.dyno-file.period=${NN_FILE_METRIC_PERIOD}
namenode.sink.dyno-file.class=org.apache.hadoop.metrics2.sink.FileSink
namenode.sink.dyno-file.filename=${nnMetricOutputFileLocal}
EOF
  fi

  nameDir="${NN_NAME_DIR:-${baseDir}/name-data}"
  editsDir="${NN_EDITS_DIR:-${baseDir}/name-data}"
  checkpointDir="$baseDir/checkpoint"
  rm -rf "$nameDir" "$editsDir" "$checkpointDir"
  mkdir -p "$nameDir/current" "$editsDir/current" "$checkpointDir"
  chmod -R 700 "$nameDir" "$editsDir" "$checkpointDir"
  # Link all of the fsimage files into the name dir
  find "$(pwd)" -maxdepth 1 -mindepth 1 \( -name "fsimage_*" -or -name "VERSION" \) -execdir ln -snf "$(pwd)/{}" "$nameDir/current/{}" \;
  chmod 700 "$nameDir"/current/*

  namenodeConfigs=(
    -D "fs.defaultFS=hdfs://${nnHostname}:${nnRpcPort}"
    -D "dfs.namenode.rpc-address=${nnHostname}:${nnRpcPort}"
    -D "dfs.namenode.servicerpc-address=${nnHostname}:${nnServiceRpcPort}"
    -D "dfs.namenode.http-address=${nnHostname}:${nnHttpPort}"
    -D "dfs.namenode.https-address=${nnHostname}:0"
    -D "dfs.namenode.name.dir=file://${nameDir}"
    -D "dfs.namenode.edits.dir=file://${editsDir}"
    -D "dfs.namenode.checkpoint.dir=file://${baseDir}/checkpoint"
    -D "dfs.namenode.kerberos.internal.spnego.principal="
    -D "dfs.hosts="
    -D "dfs.hosts.exclude="
    -D "dfs.namenode.legacy-oiv-image.dir="
    -D "dfs.namenode.kerberos.principal="
    -D "dfs.namenode.keytab.file="
    -D "dfs.namenode.safemode.threshold-pct=0.0f"
    -D "dfs.permissions.enabled=true"
    -D "dfs.cluster.administrators=\"*\""
    -D "dfs.block.replicator.classname=org.apache.hadoop.tools.dynamometer.BlockPlacementPolicyAlwaysSatisfied"
    -D "hadoop.security.impersonation.provider.class=org.apache.hadoop.tools.dynamometer.AllowAllImpersonationProvider"
    ${configOverrides[@]}
  )

  echo "Executing the following:"
  echo "${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode" "${namenodeConfigs[@]}" "$NN_ADDITIONAL_ARGS"
  # The argument splitting of NN_ADDITIONAL_ARGS is desirable behavior here
  # shellcheck disable=SC2086
  if ! "${HADOOP_HOME}/sbin/hadoop-daemon.sh" start namenode "${namenodeConfigs[@]}" $NN_ADDITIONAL_ARGS; then
    echo "Unable to launch NameNode; exiting."
    exit 1
  fi
  componentPIDFile="${pidDir}/hadoop-$(whoami)-${component}.pid"
  while [[ ! -f "$componentPIDFile" ]]; do sleep 1; done
  componentPID=$(cat "$componentPIDFile")

  if [[ "$NN_FILE_METRIC_PERIOD" -gt 0 ]]; then
    nnMetricOutputFileRemote="$hdfsStoragePath/namenode_metrics"
    echo "Going to attempt to upload metrics to: $nnMetricOutputFileRemote"

    touch "$nnMetricOutputFileLocal"
    (tail -n 999999 -f "$nnMetricOutputFileLocal" & echo $! >&3) 3>metricsTailPIDFile | \
      hdfs_original dfs -appendToFile - "$nnMetricOutputFileRemote" &
    metricsTailPID="$(cat metricsTailPIDFile)"
    if [[ "$metricsTailPID" = "" ]]; then
      echo "Unable to upload metrics to HDFS"
    else
      echo "Metrics will be uploaded to HDFS by PID: $metricsTailPID"
    fi
  fi
fi

echo "Started $component at pid $componentPID"

## @description  Perform cleanup, killing any outstanding processes and deleting files
## @audience     private
## @stability    evolving
function cleanup {
  echo "Cleaning up $component at pid $componentPID"
  kill -9 "$componentPID"

  if [[ "$metricsTailPID" != "" ]]; then
    echo "Stopping metrics streaming at pid $metricsTailPID"
    kill "$metricsTailPID"
  fi

  echo "Deleting any remaining files"
  rm -rf "$baseDir"
}

trap cleanup EXIT

echo "Waiting for parent process (PID: $PPID) OR $component process to exit"
while kill -0 "${componentPID}" 2>/dev/null && kill -0 $PPID 2>/dev/null; do
  sleep 1
done

if kill -0 $PPID 2>/dev/null; then
  echo "$component process exited; continuing to finish"
  exit 1
else
  echo "Parent process exited; continuing to finish"
  exit 0
fi
