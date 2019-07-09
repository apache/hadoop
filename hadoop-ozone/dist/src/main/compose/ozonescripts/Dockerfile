# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM apache/ozone-runner
RUN sudo apt-get update && sudo apt-get install -y openssh-server

RUN sudo mkdir -p /run/sshd
RUN sudo sed -i "s/.*UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config
RUN sudo sed -i "s/.*PermitUserEnvironment.*/PermitUserEnvironment yes/g" /etc/ssh/sshd_config
RUN sudo sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

#/opt/hadoop is mounted, we can't use it as a home
RUN sudo usermod -d /opt hadoop
ADD .ssh /opt/.ssh
RUN sudo chown -R hadoop /opt/.ssh
RUN sudo chown hadoop /opt
RUN sudo chmod 600 /opt/.ssh/*
RUN sudo chmod 700 /opt/.ssh

RUN sudo sh -c 'echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/" >> /etc/profile'
CMD ["sudo","/usr/sbin/sshd","-D"]
