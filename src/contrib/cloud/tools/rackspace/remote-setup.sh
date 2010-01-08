#!/bin/bash -x
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
# Given an Ubuntu base system install, install the base packages we need.
#

# We require multiverse to be enabled.
cat >> /etc/apt/sources.list << EOF
deb http://us.archive.ubuntu.com/ubuntu/ intrepid multiverse
deb-src http://us.archive.ubuntu.com/ubuntu/ intrepid multiverse
deb http://us.archive.ubuntu.com/ubuntu/ intrepid-updates multiverse
deb-src http://us.archive.ubuntu.com/ubuntu/ intrepid-updates multiverse
EOF

apt-get update

# Install Java
apt-get -y install sun-java6-jdk
echo "export JAVA_HOME=/usr/lib/jvm/java-6-sun" >> /etc/profile
export JAVA_HOME=/usr/lib/jvm/java-6-sun
java -version

# Install general packages
apt-get -y install vim curl screen ssh rsync unzip openssh-server
apt-get -y install policykit # http://www.bergek.com/2008/11/24/ubuntu-810-libpolkit-error/

# Create root's .ssh directory if it doesn't exist
mkdir -p /root/.ssh

# Run any rackspace init script injected at boot time
echo '[ -f /etc/init.d/rackspace-init.sh ] && /bin/sh /etc/init.d/rackspace-init.sh; exit 0' > /etc/rc.local
