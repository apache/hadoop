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

## @description  start download server
## @audience     public
## @stability    stable
function start_download_server()
{
  if [[ "$DOWNLOAD_SERVER_IP" != "$LOCAL_HOST_IP" ]]; then
    echo -e "\033[31mERROR: Only $DOWNLOAD_SERVER_IP can start the download service.\033[0m"
    return 1
  fi

  echo -e "You can put the install package file in the \033[34m${DOWNLOAD_DIR}\033[0m folder first, Or automatic download."
  echo -n "Do you want to start download http server?[y|n]"
  read myselect
  if [[ "$myselect" = "y" || "$myselect" = "Y" ]]
  then
    download_etcd_bin
    download_calico_bin
    download_docker_rpm
    download_nvidia_driver
    download_nvidia_docker_bin
    download_yarn_container_executor

    python -m SimpleHTTPServer ${DOWNLOAD_SERVER_PORT}
  fi
}
