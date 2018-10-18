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

## @description  check install user
## @audience     public
## @stability    stable
function check_install_user()
{
  if [[ $(id -u) -ne 0 ]];then
    echo "This script must be run with a ROOT user!"
    exit # don't call exit_install()
  fi
}

## @description  exit install
## @audience     public
## @stability    stable
function exit_install()
{
  echo "Exit the installation!" | tee -a $LOG
  exit $1
}

## @description  Check if the IP address format is correct
## @audience     public
## @stability    stable
function valid_ip()
{
  local ip=$1
  local stat=1

  if [[ $ip =~ ^[0-9]{1,3\}.[0-9]{1,3\}.[0-9]{1,3\}.[0-9]{1,3\}$ ]]; then
    OIFS=$IFS
    IFS='.'
    ip=($ip)
    IFS=$OIFS

    if [[ ${ip[0]} -le 255 && ${ip[1]} -le 255 && ${ip[2]} -le 255 && ${ip[3]} -le 255 ]]; then
      stat=$?
    fi
  fi

  return $stat
}

## @description  Check if the configuration file configuration is correct
## @audience     public
## @stability    stable
function check_install_conf()
{
  echo "Check if the configuration file configuration is correct ..." | tee -a $LOG

  # check etcd conf
  hostCount=${#ETCD_HOSTS[@]}
  if [[ $hostCount -lt 3 && hostCount -ne 0 ]]; then # <>2
    echo "Number of nodes = [$hostCount], must be configured to be greater than or equal to 3 servers! " | tee -a $LOG
    exit_install
  fi
  for ip in ${ETCD_HOSTS[@]}
  do
    if ! valid_ip $ip; then
      echo "]ETCD_HOSTS=[$ip], IP address format is incorrect! " | tee -a $LOG
      exit_install
    fi
  done
  echo "Check if the configuration file configuration is correct [ Done ]" | tee -a $LOG
}

## @description  index by EtcdHosts list
## @audience     public
## @stability    stable
function indexByEtcdHosts() {
  index=0
  while [ "$index" -lt "${#ETCD_HOSTS[@]}" ]; do
    if [ "${ETCD_HOSTS[$index]}" = "$1" ]; then
      echo $index
      return
    fi
    let "index++"
  done
  echo ""
}

## @description  get local IP
## @audience     public
## @stability    stable
function getLocalIP()
{
  local _ip _myip _line _nl=$'\n'
  while IFS=$': \t' read -a _line ;do
      [ -z "${_line%inet}" ] &&
         _ip=${_line[${#_line[1]}>4?1:2]} &&
         [ "${_ip#127.0.0.1}" ] && _myip=$_ip
    done< <(LANG=C /sbin/ifconfig)
  printf ${1+-v} $1 "%s${_nl:0:$[${#1}>0?0:1]}" $_myip
}

## @description  get ip list
## @audience     public
## @stability    stable
function get_ip_list()
{
  array=$(ifconfig | grep inet | grep -v inet6 | grep -v 127 | sed 's/^[ \t]*//g' | cut -d ' ' -f2)

  for ip in ${array[@]}
  do
    LOCAL_HOST_IP_LIST+=(${ip})
  done
}
