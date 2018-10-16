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

## @description  install yarn
## @audience     public
## @stability    stable
function install_yarn()
{
  install_yarn_container_executor
  install_yarn_config
}

## @description  uninstall yarn
## @audience     public
## @stability    stable
function uninstall_yarn()
{
  rm -rf /etc/yarn/sbin/Linux-amd64-64/*
  rm -rf /etc/yarn/sbin/etc/hadoop/*
}

## @description  download yarn container executor
## @audience     public
## @stability    stable
function download_yarn_container_executor()
{
  # my download http server
  if [[ -n "$DOWNLOAD_HTTP" ]]; then
    MY_YARN_CONTAINER_EXECUTOR_PATH="${DOWNLOAD_HTTP}/downloads/hadoop/container-executor"
  else
    MY_YARN_CONTAINER_EXECUTOR_PATH=${YARN_CONTAINER_EXECUTOR_PATH}
  fi

  if [ ! -d "${DOWNLOAD_DIR}/hadoop" ]; then
    mkdir -p ${DOWNLOAD_DIR}/hadoop
  fi

  if [[ -f "${DOWNLOAD_DIR}/hadoop/container-executor" ]]; then
    echo "${DOWNLOAD_DIR}/hadoop/container-executor is exist."
  else
    if [[ -n "$DOWNLOAD_HTTP" ]]; then
      echo "download ${MY_YARN_CONTAINER_EXECUTOR_PATH} ..."
      wget -P ${DOWNLOAD_DIR}/hadoop ${MY_YARN_CONTAINER_EXECUTOR_PATH}
    else
      echo "copy ${MY_YARN_CONTAINER_EXECUTOR_PATH} ..."
      cp ${MY_YARN_CONTAINER_EXECUTOR_PATH} ${DOWNLOAD_DIR}/hadoop/
    fi
  fi
}

## @description  install yarn container executor
## @audience     public
## @stability    stable
function install_yarn_container_executor()
{
  echo "install yarn container executor file ..."

  download_yarn_container_executor

  if [ ! -d "/etc/yarn/sbin/Linux-amd64-64" ]; then
    mkdir -p /etc/yarn/sbin/Linux-amd64-64
  fi
  if [ -f "/etc/yarn/sbin/Linux-amd64-64/container-executor" ]; then
    rm /etc/yarn/sbin/Linux-amd64-64/container-executor
  fi

  cp -f ${DOWNLOAD_DIR}/hadoop/container-executor /etc/yarn/sbin/Linux-amd64-64

  sudo chmod 6755 /etc/yarn/sbin/Linux-amd64-64
  sudo chown :yarn /etc/yarn/sbin/Linux-amd64-64/container-executor
  sudo chmod 6050 /etc/yarn/sbin/Linux-amd64-64/container-executor
}

## @description  install yarn config
## @audience     public
## @stability    stable
function install_yarn_config()
{
  echo "install yarn config file ..."

  cp -R ${PACKAGE_DIR}/hadoop ${INSTALL_TEMP_DIR}/

  find="/"
  replace="\/"
  escape_yarn_nodemanager_local_dirs=${YARN_NODEMANAGER_LOCAL_DIRS//$find/$replace}
  escape_yarn_nodemanager_log_dirs=${YARN_NODEMANAGER_LOG_DIRS//$find/$replace}
  escape_yarn_hierarchy=${YARN_HIERARCHY//$find/$replace}

  sed -i "s/YARN_NODEMANAGER_LOCAL_DIRS_REPLACE/${escape_yarn_nodemanager_local_dirs}/g" $INSTALL_TEMP_DIR/hadoop/container-executor.cfg >>$LOG
  sed -i "s/YARN_NODEMANAGER_LOG_DIRS_REPLACE/${escape_yarn_nodemanager_log_dirs}/g" $INSTALL_TEMP_DIR/hadoop/container-executor.cfg >>$LOG
  sed -i "s/DOCKER_REGISTRY_REPLACE/${DOCKER_REGISTRY}/g" $INSTALL_TEMP_DIR/hadoop/container-executor.cfg >>$LOG
  sed -i "s/CALICO_NETWORK_NAME_REPLACE/${CALICO_NETWORK_NAME}/g" $INSTALL_TEMP_DIR/hadoop/container-executor.cfg >>$LOG
  sed -i "s/YARN_HIERARCHY_REPLACE/${escape_yarn_hierarchy}/g" $INSTALL_TEMP_DIR/hadoop/container-executor.cfg >>$LOG

  # Delete the ASF license comment in the container-executor.cfg file, otherwise it will cause a cfg format error.
  sed -i '1,16d' $INSTALL_TEMP_DIR/hadoop/container-executor.cfg

  if [ ! -d "/etc/yarn/sbin/etc/hadoop" ]; then
    mkdir -p /etc/yarn/sbin/etc/hadoop
  fi

  cp -f $INSTALL_TEMP_DIR/hadoop/container-executor.cfg /etc/yarn/sbin/etc/hadoop/
}
