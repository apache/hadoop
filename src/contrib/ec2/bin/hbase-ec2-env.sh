# Set environment variables for running Hbase on Amazon EC2 here. All are required.

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

# Your Amazon Account Number.
AWS_ACCOUNT_ID=

# Your Amazon AWS access key.
AWS_ACCESS_KEY_ID=

# Your Amazon AWS secret access key.
AWS_SECRET_ACCESS_KEY=

# Your AWS private key file -- must begin with 'pk' and end with '.pem'
EC2_PRIVATE_KEY=

# Your AWS certificate file -- must begin with 'cert' and end with '.pem'
EC2_CERT=

# Location of EC2 keys.
# The default setting is probably OK if you set up EC2 following the Amazon Getting Started guide.
EC2_KEYDIR=`dirname "$EC2_PRIVATE_KEY"`

# The EC2 key name used to launch instances.
# The default is the value used in the Amazon Getting Started guide.
KEY_NAME=root

# Where your EC2 private key is stored (created when following the Amazon Getting Started guide).
# You need to change this if you don't store this with your other EC2 keys.
PRIVATE_KEY_PATH=`echo "$EC2_KEYDIR"/"id_rsa_$KEY_NAME"`

# SSH options used when connecting to EC2 instances.
SSH_OPTS=`echo -q -i "$PRIVATE_KEY_PATH" -o StrictHostKeyChecking=no -o ServerAliveInterval=30`

# The version of HBase to use.
HBASE_VERSION=0.20.1

# The version of Hadoop to use.
HADOOP_VERSION=$HBASE_VERSION

# The Amazon S3 bucket where the HBase AMI is stored.
# The default value is for public images, so can be left if you are using running a public image.
# Change this value only if you are creating your own (private) AMI
# so you can store it in a bucket you own.
#S3_BUCKET=hbase-images
S3_BUCKET=iridiant-bundles

# Enable public access web interfaces
# XXX -- Generally, you do not want to do this
ENABLE_WEB_PORTS=false

# The script to run on instance boot.
USER_DATA_FILE=hbase-ec2-init-remote.sh

# Use only c1.xlarge unless you know what you are doing
INSTANCE_TYPE=${INSTANCE_TYPE:-c1.xlarge}

# Use only c1.medium unless you know what you are doing
ZOO_INSTANCE_TYPE=${ZOO_INSTANCE_TYPE:-c1.medium}

# The EC2 group master name. CLUSTER is set by calling scripts
CLUSTER_MASTER=$CLUSTER-master

# Cached values for a given cluster
MASTER_PRIVATE_IP_PATH=~/.hbase-private-$CLUSTER_MASTER
MASTER_IP_PATH=~/.hbase-$CLUSTER_MASTER
MASTER_ZONE_PATH=~/.hbase-zone-$CLUSTER_MASTER

# The Zookeeper EC2 group name. CLUSTER is set by calling scripts.
CLUSTER_ZOOKEEPER=$CLUSTER-zookeeper
ZOOKEEPER_QUORUM_PATH=~/.hbase-quorum-$CLUSTER_ZOOKEEPER

#
# The following variables are only used when creating an AMI.
#

# The version number of the installed JDK.
JAVA_VERSION=1.6.0_16

# SUPPORTED_ARCHITECTURES = ['i386', 'x86_64']
# The download URL for the Sun JDK. Visit http://java.sun.com/javase/downloads/index.jsp and get the URL for the "Linux self-extracting file".
if [ "$INSTANCE_TYPE" = "m1.small" -o "$INSTANCE_TYPE" = "c1.medium" ]; then
  ARCH='i386'
  BASE_AMI_IMAGE="ami-48aa4921"  # ec2-public-images/fedora-8-i386-base-v1.10.manifest.xml
  AMI_IMAGE="ami-c644a7af"
  JAVA_BINARY_URL='http://iridiant.s3.amazonaws.com/jdk/jdk-6u16-linux-i586.bin'
else
  ARCH='x86_64'
  BASE_AMI_IMAGE="ami-f61dfd9f"  # ec2-public-images/fedora-8-x86_64-base-v1.10.manifest.xml
  AMI_IMAGE="ami-f244a79b"
  JAVA_BINARY_URL='http://iridiant.s3.amazonaws.com/jdk/jdk-6u16-linux-x64.bin'
fi

if [ "$ZOO_INSTANCE_TYPE" = "m1.small" -o "$ZOO_INSTANCE_TYPE" = "c1.medium" ]; then
  ZOO_ARCH='i386'
  ZOO_AMI_IMAGE="ami-c644a7af"
else
  ZOO_ARCH='x86_64'
  ZOO_AMI_IMAGE="ami-f244a79b"
fi
