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

## @description  download nvidia docker bin
## @audience     public
## @stability    stable
function download_nvidia_docker_bin()
{
  # download http server
  if [[ -n "$DOWNLOAD_HTTP" ]]; then
    MY_NVIDIA_DOCKER_RPM_URL="${DOWNLOAD_HTTP}/downloads/nvidia-docker/${NVIDIA_DOCKER_RPM}"
  else
    MY_NVIDIA_DOCKER_RPM_URL=${NVIDIA_DOCKER_RPM_URL}
  fi

  if [[ -f "${DOWNLOAD_DIR}/nvidia-docker/${NVIDIA_DOCKER_RPM}" ]]; then
    echo "${DOWNLOAD_DIR}/nvidia-docker/${NVIDIA_DOCKER_RPM} is exist."
  else
    echo "download ${MY_NVIDIA_DOCKER_RPM_URL} ..."
    wget -P ${DOWNLOAD_DIR}/nvidia-docker/ ${MY_NVIDIA_DOCKER_RPM_URL}
  fi
}

## @description  install nvidia docker
## @audience     public
## @stability    stable
function install_nvidia_docker()
{
  download_nvidia_docker_bin

  sudo rpm -i ${DOWNLOAD_DIR}/nvidia-docker/${NVIDIA_DOCKER_RPM}

  echo -e "\033[32m===== Start nvidia-docker =====\033[0m"
  sudo systemctl start nvidia-docker

  echo -e "\033[32m===== Check nvidia-docker status =====\033[0m"
  systemctl status nvidia-docker

  echo -e "\033[32m===== Check nvidia-docker log =====\033[0m"
  journalctl -u nvidia-docker

  echo -e "\033[32m===== Test nvidia-docker-plugin =====\033[0m"
  curl http://localhost:3476/v1.0/docker/cli

  # create nvidia driver library path
  if [ ! -d "/var/lib/nvidia-docker/volumes/nvidia_driver" ]; then
    echo "WARN: /var/lib/nvidia-docker/volumes/nvidia_driver folder path is not exist!"
    mkdir -p /var/lib/nvidia-docker/volumes/nvidia_driver
  fi

  local nvidiaVersion=`get_nvidia_version`
  echo -e "\033[31m nvidia detect version is ${nvidiaVersion}\033[0m"

  mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/${nvidiaVersion}
  mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/${nvidiaVersion}/bin
  mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/${nvidiaVersion}/lib64

  cp /usr/bin/nvidia* /var/lib/nvidia-docker/volumes/nvidia_driver/${nvidiaVersion}/bin
  cp /usr/lib64/libcuda* /var/lib/nvidia-docker/volumes/nvidia_driver/${nvidiaVersion}/lib64
  cp /usr/lib64/libnvidia* /var/lib/nvidia-docker/volumes/nvidia_driver/${nvidiaVersion}/lib64

  echo -e "\033[32m===== Please manually execute the following command =====\033[0m"
  echo -e "\033[32mshell:> nvidia-docker run --rm ${DOCKER_REGISTRY}/nvidia/cuda:9.0-devel nvidia-smi
# If you don't see the list of graphics cards above, the NVIDIA driver installation failed. =====
\033[0m"

  echo -e "\033[32m===== Please manually execute the following command =====\033[0m"
  echo -e "\033[32m# Test with tf.test.is_gpu_available()
shell:> nvidia-docker run -it ${DOCKER_REGISTRY}/tensorflow/tensorflow:1.9.0-gpu bash
# In docker container
container:> python
python:> import tensorflow as tf
python:> tf.test.is_gpu_available()
python:> exit()
\033[0m"
}

## @description  uninstall nvidia docker
## @audience     public
## @stability    stable
function uninstall_nvidia_docker()
{
  echo "This method is not implemented."
}

