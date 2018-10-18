#!/usr/bin/env bash
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

## @description  download docker rmp
## @audience     public
## @stability    stable
function download_docker_rpm()
{
  # download http server
  if [[ -n "$DOWNLOAD_HTTP" ]]; then
    MY_DOCKER_ENGINE_SELINUX_RPM="${DOWNLOAD_HTTP}/downloads/docker/${DOCKER_ENGINE_SELINUX_RPM}"
    MY_DOCKER_ENGINE_RPM="${DOWNLOAD_HTTP}/downloads/docker/${DOCKER_ENGINE_RPM}"
  else
    MY_DOCKER_ENGINE_SELINUX_RPM=${DOCKER_REPO}/${DOCKER_ENGINE_SELINUX_RPM}
    MY_DOCKER_ENGINE_RPM=${DOCKER_REPO}/${DOCKER_ENGINE_RPM}
  fi

  # download docker rpm
  if [[ -f ${DOWNLOAD_DIR}/docker/${DOCKER_ENGINE_SELINUX_RPM} ]]; then
    echo "${DOWNLOAD_DIR}/docker/${DOCKER_ENGINE_SELINUX_RPM} is exist."
  else
    echo "download ${MY_DOCKER_ENGINE_SELINUX_RPM} ..."
    wget -P ${DOWNLOAD_DIR}/docker/ ${MY_DOCKER_ENGINE_SELINUX_RPM}
  fi

  if [[ -f ${DOWNLOAD_DIR}/docker/${DOCKER_ENGINE_RPM} ]]; then
    echo "${DOWNLOAD_DIR}/docker/${DOCKER_ENGINE_RPM} is exist."
  else
    echo "download ${MY_DOCKER_ENGINE_RPM} ..."
    wget -P ${DOWNLOAD_DIR}/docker/ ${MY_DOCKER_ENGINE_RPM}
  fi
}

## @description  install docker bin
## @audience     public
## @stability    stable
function install_docker_bin()
{
  download_docker_rpm

  yum -y localinstall ${DOWNLOAD_DIR}/docker/${DOCKER_ENGINE_SELINUX_RPM}
  yum -y localinstall ${DOWNLOAD_DIR}/docker/${DOCKER_ENGINE_RPM}
}

## @description  uninstall docker bin
## @audience     public
## @stability    stable
function uninstall_docker_bin()
{
  download_docker_rpm

  yum -y remove ${DOWNLOAD_DIR}/docker/${DOCKER_ENGINE_SELINUX_RPM}
  yum -y remove ${DOWNLOAD_DIR}/docker/${DOCKER_ENGINE_RPM}
}

## @description  install docker config
## @audience     public
## @stability    stable
function install_docker_config()
{
  rm -rf ${INSTALL_TEMP_DIR}/docker
  cp -rf ${PACKAGE_DIR}/docker ${INSTALL_TEMP_DIR}/

  # replace cluster-store
  # "cluster-store":"etcd://10.196.69.173:2379,10.196.69.174:2379,10.196.69.175:2379"
  # char '/' need to escape '\/'
  clusterStore="etcd:\/\/"
  index=1
  etcdHostsSize=${#ETCD_HOSTS[@]}
  for item in ${ETCD_HOSTS[@]}
  do
    clusterStore="${clusterStore}${item}:2379"
    if [[ ${index} -lt ${etcdHostsSize}-1 ]]; then
      clusterStore=${clusterStore}","
    fi
    index=$(($index+1))
  done
  echo "clusterStore=${clusterStore}"
  sed -i "s/CLUSTER_STORE_REPLACE/${clusterStore}/g" $INSTALL_TEMP_DIR/docker/daemon.json >>$LOG

  sed -i "s/DOCKER_REGISTRY_REPLACE/${DOCKER_REGISTRY}/g" $INSTALL_TEMP_DIR/docker/daemon.json >>$LOG
  sed -i "s/LOCAL_HOST_IP_REPLACE/${LOCAL_HOST_IP}/g" $INSTALL_TEMP_DIR/docker/daemon.json >>$LOG
  sed -i "s/YARN_DNS_HOST_REPLACE/${YARN_DNS_HOST}/g" $INSTALL_TEMP_DIR/docker/daemon.json >>$LOG
  sed -i "s/LOCAL_DNS_HOST_REPLACE/${LOCAL_DNS_HOST}/g" $INSTALL_TEMP_DIR/docker/daemon.json >>$LOG

  # Delete the ASF license comment in the daemon.json file, otherwise it will cause a json format error.
  sed -i '1,16d' $INSTALL_TEMP_DIR/docker/daemon.json

  if [ ! -d "/etc/docker" ]; then
    mkdir /etc/docker
  fi

  cp $INSTALL_TEMP_DIR/docker/daemon.json /etc/docker/
  cp $INSTALL_TEMP_DIR/docker/docker.service /etc/systemd/system/ >>$LOG
}

## @description  install docker
## @audience     public
## @stability    stable
function install_docker()
{
  install_docker_bin
  install_docker_config

  systemctl daemon-reload
  systemctl enable docker.service
}

## @description  unstall docker
## @audience     public
## @stability    stable
function uninstall_docker()
{
  echo "stop docker service"
  systemctl stop docker

  echo "remove docker"
  uninstall_docker_bin

  rm /etc/docker/daemon.json >>$LOG
  rm /etc/systemd/system/docker.service >>$LOG

  systemctl daemon-reload
}

## @description  start docker
## @audience     public
## @stability    stable
function start_docker()
{
  systemctl restart docker
  systemctl status docker
  docker info
}

## @description  stop docker
## @audience     public
## @stability    stable
function stop_docker()
{
  systemctl stop docker
  systemctl status docker
}

## @description  check if the containers exist
## @audience     public
## @stability    stable
function containers_exist()
{
  local dockerContainersInfo=`docker ps -a --filter NAME=$1`
  echo ${dockerContainersInfo} | grep $1
}
