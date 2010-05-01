#!/usr/bin/env bash
#
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
#
# This script tests the Hadoop cloud scripts by running through a minimal
# sequence of steps to start a cluster, run a job, then shutdown the cluster.
#
# Example usage:
# HADOOP_HOME=~/dev/hadoop-0.20.1/ ./transient-cluster.sh
#

set -e
set -x

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

WORKSPACE=${WORKSPACE:-`pwd`}
CONFIG_DIR=${CONFIG_DIR:-$WORKSPACE/.hadoop-cloud}
CLUSTER=${CLUSTER:-hadoop-cloud-$USER-test-cluster}
IMAGE_ID=${IMAGE_ID:-ami-6159bf08} # default to Fedora 32-bit AMI
INSTANCE_TYPE=${INSTANCE_TYPE:-m1.small}
AVAILABILITY_ZONE=${AVAILABILITY_ZONE:-us-east-1c}
KEY_NAME=${KEY_NAME:-$USER}
AUTO_SHUTDOWN=${AUTO_SHUTDOWN:-15}
LOCAL_HADOOP_VERSION=${LOCAL_HADOOP_VERSION:-0.20.1}
HADOOP_HOME=${HADOOP_HOME:-$WORKSPACE/hadoop-$LOCAL_HADOOP_VERSION}
HADOOP_CLOUD_HOME=${HADOOP_CLOUD_HOME:-$bin/../py}
HADOOP_CLOUD_PROVIDER=${HADOOP_CLOUD_PROVIDER:-ec2}
PUBLIC_KEY=${PUBLIC_KEY:-~/.$HADOOP_CLOUD_PROVIDER/id_rsa-$KEY_NAME.pub}
PRIVATE_KEY=${PRIVATE_KEY:-~/.$HADOOP_CLOUD_PROVIDER/id_rsa-$KEY_NAME}
SSH_OPTIONS=${SSH_OPTIONS:-"-i $PRIVATE_KEY -o StrictHostKeyChecking=no"}
LAUNCH_ARGS=${LAUNCH_ARGS:-"1 nn,snn,jt 1 dn,tt"}

HADOOP_CLOUD_SCRIPT=$HADOOP_CLOUD_HOME/hadoop-cloud
export HADOOP_CONF_DIR=$CONFIG_DIR/$CLUSTER

# Install Hadoop locally
if [ ! -d $HADOOP_HOME ]; then
  wget http://archive.apache.org/dist/hadoop/core/hadoop-\
$LOCAL_HADOOP_VERSION/hadoop-$LOCAL_HADOOP_VERSION.tar.gz
  tar zxf hadoop-$LOCAL_HADOOP_VERSION.tar.gz -C $WORKSPACE
  rm hadoop-$LOCAL_HADOOP_VERSION.tar.gz
fi

# Launch a cluster
if [ $HADOOP_CLOUD_PROVIDER == 'ec2' ]; then
  $HADOOP_CLOUD_SCRIPT launch-cluster \
    --config-dir=$CONFIG_DIR \
    --image-id=$IMAGE_ID \
    --instance-type=$INSTANCE_TYPE \
    --key-name=$KEY_NAME \
    --auto-shutdown=$AUTO_SHUTDOWN \
    --availability-zone=$AVAILABILITY_ZONE \
    $CLIENT_CIDRS $ENVS $CLUSTER $LAUNCH_ARGS
else
  $HADOOP_CLOUD_SCRIPT launch-cluster --cloud-provider=$HADOOP_CLOUD_PROVIDER \
    --config-dir=$CONFIG_DIR \
    --image-id=$IMAGE_ID \
    --instance-type=$INSTANCE_TYPE \
    --public-key=$PUBLIC_KEY \
    --private-key=$PRIVATE_KEY \
    --auto-shutdown=$AUTO_SHUTDOWN \
    $CLIENT_CIDRS $ENVS $CLUSTER $LAUNCH_ARGS
fi
  
# List clusters
$HADOOP_CLOUD_SCRIPT list --cloud-provider=$HADOOP_CLOUD_PROVIDER \
  --config-dir=$CONFIG_DIR
$HADOOP_CLOUD_SCRIPT list --cloud-provider=$HADOOP_CLOUD_PROVIDER \
  --config-dir=$CONFIG_DIR $CLUSTER

# Run a proxy and save its pid in HADOOP_CLOUD_PROXY_PID
eval `$HADOOP_CLOUD_SCRIPT proxy --cloud-provider=$HADOOP_CLOUD_PROVIDER \
  --config-dir=$CONFIG_DIR \
  --ssh-options="$SSH_OPTIONS" $CLUSTER`
  
if [ $HADOOP_CLOUD_PROVIDER == 'rackspace' ]; then
  # Need to update /etc/hosts (interactively)
  $HADOOP_CLOUD_SCRIPT list --cloud-provider=$HADOOP_CLOUD_PROVIDER \
    --config-dir=$CONFIG_DIR $CLUSTER | grep 'nn,snn,jt' \
    | awk '{print $4 " " $3 }'  | sudo tee -a /etc/hosts
fi

# Run a job and check it works
$HADOOP_HOME/bin/hadoop fs -mkdir input
$HADOOP_HOME/bin/hadoop fs -put $HADOOP_HOME/LICENSE.txt input
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-*-examples.jar grep \
  input output Apache
# following returns a non-zero exit code if no match
$HADOOP_HOME/bin/hadoop fs -cat 'output/part-00000' | grep Apache

# Shutdown the cluster
kill $HADOOP_CLOUD_PROXY_PID
$HADOOP_CLOUD_SCRIPT terminate-cluster --cloud-provider=$HADOOP_CLOUD_PROVIDER \
  --config-dir=$CONFIG_DIR --force $CLUSTER
sleep 5 # wait for termination to take effect
$HADOOP_CLOUD_SCRIPT delete-cluster --cloud-provider=$HADOOP_CLOUD_PROVIDER \
  --config-dir=$CONFIG_DIR $CLUSTER
