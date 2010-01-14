# Set environment variables for running Hbase on Amazon EC2 here.

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

# Your AWS root SSH key
EC2_ROOT_SSH_KEY=

# The version of HBase to use.
HBASE_VERSION=@HBASE_VERSION@

# The version of Hadoop to use.
HADOOP_VERSION=0.20.1

# The Amazon S3 bucket where the HBase AMI is stored.
# Change this value only if you are creating your own (private) AMI
# so you can store it in a bucket you own.
S3_BUCKET=apache-hbase-images

# Enable public access web interfaces
ENABLE_WEB_PORTS=false

# Extra packages
# Allows you to add a private Yum repo and pull packages from it as your
# instances boot up. Format is <repo-descriptor-URL> <pkg1> ... <pkgN>
# The repository descriptor will be fetched into /etc/yum/repos.d.
EXTRA_PACKAGES=

# Use only c1.xlarge unless you know what you are doing
MASTER_INSTANCE_TYPE=${MASTER_INSTANCE_TYPE:-c1.xlarge}

# Use only c1.xlarge unless you know what you are doing
SLAVE_INSTANCE_TYPE=${SLAVE_INSTANCE_TYPE:-c1.xlarge}

# Use only c1.medium unless you know what you are doing
ZOO_INSTANCE_TYPE=${ZOO_INSTANCE_TYPE:-c1.medium}

############################################################################

# Generally, users do not need to edit below

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/functions.sh
getCredentialSetting 'AWS_ACCOUNT_ID'
getCredentialSetting 'AWS_ACCESS_KEY_ID'
getCredentialSetting 'AWS_SECRET_ACCESS_KEY'
getCredentialSetting 'EC2_PRIVATE_KEY'
getCredentialSetting 'EC2_CERT'
getCredentialSetting 'EC2_ROOT_SSH_KEY'

# SSH options used when connecting to EC2 instances.
SSH_OPTS=`echo -q -i "$EC2_ROOT_SSH_KEY" -o StrictHostKeyChecking=no -o ServerAliveInterval=30`

# EC2 command request timeout (seconds)
REQUEST_TIMEOUT=300    # 5 minutes

# Global tool options
TOOL_OPTS=`echo -K "$EC2_PRIVATE_KEY" -C "$EC2_CERT" --request-timeout $REQUEST_TIMEOUT`

# The EC2 group master name. CLUSTER is set by calling scripts
CLUSTER_MASTER=$CLUSTER-master

# Cached values for a given cluster
MASTER_PRIVATE_IP_PATH=~/.hbase-private-$CLUSTER_MASTER
MASTER_IP_PATH=~/.hbase-$CLUSTER_MASTER
MASTER_ZONE_PATH=~/.hbase-zone-$CLUSTER_MASTER

# The Zookeeper EC2 group name. CLUSTER is set by calling scripts.
CLUSTER_ZOOKEEPER=$CLUSTER-zookeeper
ZOOKEEPER_QUORUM_PATH=~/.hbase-quorum-$CLUSTER_ZOOKEEPER

# The script to run on instance boot.
USER_DATA_FILE=hbase-ec2-init-remote.sh

# The version number of the installed JDK.
JAVA_VERSION=1.6.0_18

# SUPPORTED_ARCHITECTURES = ['i386', 'x86_64']
if [ "$SLAVE_INSTANCE_TYPE" = "m1.small" -o "$SLAVE_INSTANCE_TYPE" = "c1.medium" ]; then
  SLAVE_ARCH='i386'
  BASE_AMI_IMAGE="ami-48aa4921"  # ec2-public-images/fedora-8-i386-base-v1.10.manifest.xml
else
  SLAVE_ARCH='x86_64'
  BASE_AMI_IMAGE="ami-f61dfd9f"  # ec2-public-images/fedora-8-x86_64-base-v1.10.manifest.xml
fi

if [ "$MASTER_INSTANCE_TYPE" = "m1.small" -o "$MASTER_INSTANCE_TYPE" = "c1.medium" ]; then
  MASTER_ARCH='i386'
else
  MASTER_ARCH='x86_64'
fi

if [ "$ZOO_INSTANCE_TYPE" = "m1.small" -o "$ZOO_INSTANCE_TYPE" = "c1.medium" ]; then
  ZOO_ARCH='i386'
else
  ZOO_ARCH='x86_64'
fi
