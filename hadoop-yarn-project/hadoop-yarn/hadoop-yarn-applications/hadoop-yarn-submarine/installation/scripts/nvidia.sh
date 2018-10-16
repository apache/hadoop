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

nvidia_run_file=""

## @description  get nvidia version
## @audience     public
## @stability    stable
function get_nvidia_version()
{
  chmod +x ${DOWNLOAD_DIR}/nvidia/nvidia-detect
  local nvidia_detect_info=`${DOWNLOAD_DIR}/nvidia/nvidia-detect -v`
  echo $nvidia_detect_info | sed "s/^.*This device requires the current \([0-9.]*\).*/\1/"
}

## @description  download nvidia driver
## @audience     public
## @stability    stable
function download_nvidia_driver()
{
  # submarin http server
  if [[ -n "$DOWNLOAD_HTTP" ]]; then
    MY_NVIDIA_DETECT_URL="${DOWNLOAD_HTTP}/downloads/nvidia/nvidia-detect"
  else
    MY_NVIDIA_DETECT_URL=${NVIDIA_DETECT_URL}
  fi

  # download nvidia_detect
  if [[ -f "${DOWNLOAD_DIR}/nvidia/nvidia-detect" ]]; then
    echo "${DOWNLOAD_DIR}/nvidia/nvidia-detect is exist."
  else
    echo "download ${MY_NVIDIA_DETECT_URL} ..."
    wget -P ${DOWNLOAD_DIR}/nvidia/ ${MY_NVIDIA_DETECT_URL}
  fi

  echo "execution nvidia-detect to check the graphics card ..."
  local nvidiaVersion=`get_nvidia_version`
  echo -e "detect nvidia version is \033[31m${nvidiaVersion}\033[0m"

  # download NVIDIA driver
  if [[ "$nvidiaVersion" = "" ]]; then
    echo -e "\033[31mERROR: No graphics card device detected.\033[0m"
    return 1
  else
    nvidia_run_file="NVIDIA-Linux-x86_64-${nvidiaVersion}.run"

    # submarin http server
    if [[ -n "$DOWNLOAD_HTTP" ]]; then
      MY_NVIDIA_DRIVER_RUN_URL="${DOWNLOAD_HTTP}/downloads/nvidia/${nvidia_run_file}"
    else
      # http://us.download.nvidia.com/XFree86/Linux-x86_64/390.87/NVIDIA-Linux-x86_64-390.87.run
      MY_NVIDIA_DRIVER_RUN_URL="http://us.download.nvidia.com/XFree86/Linux-x86_64/${nvidiaVersion}/${nvidia_run_file}"
    fi

    if [[ -f ${DOWNLOAD_DIR}/nvidia/${nvidia_run_file} ]]; then
      echo "NVIDIA driver files already exist in the ${DOWNLOAD_DIR}/nvidia/${nvidia_run_file} directory."
      echo "===== Please make sure the ${DOWNLOAD_DIR}/nvidia/nvidia/${nvidia_run_file} file is complete and can be used normally. ====="
    else
      echo "Download the NVIDIA driver from the ${MY_NVIDIA_DRIVER_RUN_URL}"
      wget -P ${DOWNLOAD_DIR}/nvidia/ ${MY_NVIDIA_DRIVER_RUN_URL}
    fi
  fi
}

## @description  install nvidia
## @audience     public
## @stability    stable
function install_nvidia()
{
  download_nvidia_driver

  # Confirm that the system disables nouveau
  local disable_nouveau_info=`lsmod | grep nouveau`
  if [[ "$disable_nouveau_info" = "" ]]; then
    echo "===== Start installing the NVIDIA driver ====="
    echo -e "Some options during the installation
Would you like to register the kernel module sources with DKMS?
  This will allow DKMS to automatically build a new module, if you install a different kernel later. \033[33m[Yes]\033[0m
Install NVIDIA's 32-bit compatibility libraries \033[33m[Yes]\033[0m
centos Install NVIDIA's 32-bit compatibility libraries \033[33m[Yes]\033[0m
Would you like to run the nvidia-xconfig utility to automatically update your X configuration file... \033[33m[No]\033[0m"
    sleep 2
    sh ${DOWNLOAD_DIR}/nvidia/${nvidia_run_file}
  else
    echo -e "ERROR: Nouveau is not disabled"
    return 1
  fi

  echo -e "\033[32m===== execute nvidia-smi. You should be able to see the list of graphics cards =====\033[0m"
  sleep 1
  nvidia-smi
}

## @description  uninstall nvidia
## @audience     public
## @stability    stable
function uninstall_nvidia()
{
  if [ ! -f "/usr/bin/nvidia-uninstall" ]; then
    echo -e "\033[31mERROR: /usr/bin/nvidia-uninstall file is not exist!\033[0m"
    return 1
  fi

  echo -e "execute /usr/bin/nvidia-uninstall"
  /usr/bin/nvidia-uninstall
}
