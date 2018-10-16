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
# description: sumbarine install scripts.

ROOT=$(cd "$(dirname "$0")"; pwd)
SUBMARINE_INSTALLER_VERSION="v0.7"
PACKAGE_DIR=${ROOT}/package
SCRIPTS_DIR=${ROOT}/scripts
INSTALL_TEMP_DIR=${ROOT}/temp
DOWNLOAD_DIR=${ROOT}/downloads
DATE=`date +%Y%m%d-%H:%M:%S`
LOG=${ROOT}/logs/install.log.`date +%Y%m%d%H%M%S`
LOCAL_HOST_IP_LIST=()
LOCAL_HOST_IP=""
OPERATING_SYSTEM=""
DOWNLOAD_HTTP=""

# import shell script
. ${ROOT}/install.conf
. ${ROOT}/scripts/calico.sh
. ${ROOT}/scripts/docker.sh
. ${ROOT}/scripts/download-server.sh
. ${ROOT}/scripts/environment.sh
. ${ROOT}/scripts/etcd.sh
. ${ROOT}/scripts/hadoop.sh
. ${ROOT}/scripts/menu.sh
. ${ROOT}/scripts/nvidia.sh
. ${ROOT}/scripts/nvidia-docker.sh
. ${ROOT}/scripts/submarine.sh
. ${ROOT}/scripts/utils.sh

#================================= Main ========================================
mkdir $ROOT/logs/ -p
mkdir $INSTALL_TEMP_DIR -p
mkdir $DOWNLOAD_DIR -p

source /etc/os-release
OPERATING_SYSTEM=$ID

get_ip_list
ipCount=${#LOCAL_HOST_IP_LIST[@]}
if [[ $ipCount -eq 1 ]]; then
  LOCAL_HOST_IP=${LOCAL_HOST_IP_LIST[0]}
else
  echo -e "Detect the network card IP in the server, \e[31m[${LOCAL_HOST_IP_LIST[@]}]\e[0m"
  echo -n -e "please enter a valid IP address: "

  read ipInput
  if ! valid_ip $ipInput; then
    echo -e "you input \e[31m$ipInput\e[0m address format is incorrect! " | tee -a $LOG
    exit_install
  else
    LOCAL_HOST_IP=$ipInput
  fi
fi

echo -n -e "Please confirm whether the IP address of this machine is \e[31m${LOCAL_HOST_IP}\e[0m?[y|n]"
read myselect
if [[ "$myselect" != "y" && "$myselect" != "Y" ]]; then
  exit_install
fi

check_install_conf

if [[ -n "$DOWNLOAD_SERVER_IP" && -n "$DOWNLOAD_SERVER_PORT" && "$DOWNLOAD_SERVER_IP" != "$LOCAL_HOST_IP" ]]; then
  DOWNLOAD_HTTP="http://${DOWNLOAD_SERVER_IP}:${DOWNLOAD_SERVER_PORT}"
fi

check_install_user

# 清理安装临时目录
rm $INSTALL_TEMP_DIR/* -rf >>$LOG 2>&1

menu_index="0"
for ((j=1;;j++))
do
  menu
  case "$menu_index" in
    "0")
      menu_index="$menu_choice"
    ;;
    "1"|"2"|"3"|"4"|"5")
#     echo "aaaa=$menu_index-$menu_choice"
      menu_process
      if [[ $? = 1 ]]; then
        echo "Press any key to return menu!"
        read
      fi
    ;;
    "a")
      exit_install
      ;;
    "q")
      exit_install
      ;;
    *)
      menu_index="0"
      menu_choice="0"
      menu
    ;;
  esac
done
