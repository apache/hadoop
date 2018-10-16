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

## @description  download etcd bin
## @audience     public
## @stability    stable
function download_etcd_bin()
{
  # my download http server
  if [[ -n "$DOWNLOAD_HTTP" ]]; then
    MY_ETCD_DOWNLOAD_URL="${DOWNLOAD_HTTP}/downloads/etcd/${ETCD_TAR_GZ}"
  else
    MY_ETCD_DOWNLOAD_URL=${ETCD_DOWNLOAD_URL}
  fi

  if [[ -f "${DOWNLOAD_DIR}/etcd/${ETCD_TAR_GZ}" ]]; then
    echo "${DOWNLOAD_DIR}/etcd/${ETCD_TAR_GZ} is exist."
  else
    echo "download ${MY_ETCD_DOWNLOAD_URL} ..."
    wget -P ${DOWNLOAD_DIR}/etcd ${MY_ETCD_DOWNLOAD_URL}
  fi
}

## @description  install etcd bin
## @audience     public
## @stability    stable
function install_etcd_bin()
{
  download_etcd_bin

  # install etcd bin
  mkdir -p ${INSTALL_TEMP_DIR}
  rm -rf ${INSTALL_TEMP_DIR}/etcd-*-linux-amd6
  tar zxvf ${DOWNLOAD_DIR}/etcd/${ETCD_TAR_GZ} -C ${INSTALL_TEMP_DIR}

  cp -f ${INSTALL_TEMP_DIR}/etcd-*-linux-amd64/etcd /usr/bin
  cp -f ${INSTALL_TEMP_DIR}/etcd-*-linux-amd64/etcdctl /usr/bin

  mkdir -p /var/lib/etcd
  chmod -R a+rw /var/lib/etcd
}

## @description  install etcd config
## @audience     public
## @stability    stable
function install_etcd_config()
{
  # config etcd.service
  rm -rf ${INSTALL_TEMP_DIR}/etcd
  cp -rf ${PACKAGE_DIR}/etcd ${INSTALL_TEMP_DIR}/

  # 1. Replace name with ETCD_NODE_NAME_REPLACE based on the location of the local IP in $ETCD_HOSTS
  indexEtcdList=$(indexByEtcdHosts ${LOCAL_HOST_IP})
  # echo ${indexEtcdList}
  etcdNodeName="etcdnode${indexEtcdList}"
  # echo ${etcdNodeName}
  sed -i "s/ETCD_NODE_NAME_REPLACE/${etcdNodeName}/g" $INSTALL_TEMP_DIR/etcd/etcd.service >>$LOG

  # 2. Replace local IP address
  sed -i "s/LOCAL_HOST_REPLACE/${LOCAL_HOST_IP}/g" $INSTALL_TEMP_DIR/etcd/etcd.service >>$LOG

  # 3. Replace the initial-cluster parameter
  # --initial-cluster=etcdnode1=http://10.196.69.173:2380,etcdnode2=http://10.196.69.174:2380,etcdnode3=http://10.196.69.175:2380 \
  initialCluster=''
  index=0
  etcdHostsSize=${#ETCD_HOSTS[@]}
  for item in ${ETCD_HOSTS[@]}
  do
    # char '/' need to escape '\/'
    initialCluster="${initialCluster}etcdnode${index}=http:\/\/${item}:2380"
    if [[ ${index} -lt ${etcdHostsSize}-1 ]]; then
      initialCluster=${initialCluster}","
    fi
    index=$(($index+1))
  done
  #echo "initialCluster=${initialCluster}"
  sed -i "s/INITIAL_CLUSTER_REPLACE/${initialCluster}/g" $INSTALL_TEMP_DIR/etcd/etcd.service >>$LOG

  cp $INSTALL_TEMP_DIR/etcd/etcd.service /etc/systemd/system/ >>$LOG
}

## @description  install etcd
## @audience     public
## @stability    stable
function install_etcd()
{
  index=$(indexByEtcdHosts ${LOCAL_HOST_IP})
  if [ -z "$index" ]; then
    echo -e "STOP: This host\033[31m[${LOCAL_HOST_IP}]\033[0m is not in the ETCD server list\033[31m[${ETCD_HOSTS[@]}]\033[0m"
    return 1
  fi

  install_etcd_bin

  install_etcd_config

  systemctl daemon-reload
  systemctl enable etcd.service
}

## @description  uninstall etcd
## @audience     public
## @stability    stable
function uninstall_etcd()
{
  echo "stop etcd.service"
  systemctl stop etcd.service

  echo "rm etcd ..."
  rm /usr/bin/etcd
  rm /usr/bin/etcdctl
  rm -rf /var/lib/etcd
  rm /etc/systemd/system/etcd.service

  systemctl daemon-reload
}

## @description  start etcd
## @audience     public
## @stability    stable
function start_etcd()
{
  systemctl restart etcd.service

  echo " ===== Check the status of the etcd service ====="
  echo " exec etcdctl cluster-health"
  etcdctl cluster-health
  echo " exec etcdctl cluster-health"
  etcdctl member list
}

## @description  stop etcd
## @audience     public
## @stability    stable
function stop_etcd()
{
  systemctl stop etcd.service
}
