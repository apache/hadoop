#!/usr/bin/env bash
##
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
##
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -n "$SLEEP_SECONDS" ]; then
   echo "Sleeping for $SLEEP_SECONDS seconds"
   sleep "$SLEEP_SECONDS"
fi

#
# You can wait for an other TCP port with these settings.
#
# Example:
#
# export WAITFOR=localhost:9878
#
# With an optional parameter, you can also set the maximum
# time of waiting with (in seconds) with WAITFOR_TIMEOUT.
# (The default is 300 seconds / 5 minutes.)
if [ -n "$WAITFOR" ]; then
  echo "Waiting for the service $WAITFOR"
  WAITFOR_HOST=$(printf "%s\n" "$WAITFOR"| cut -d : -f 1)
  WAITFOR_PORT=$(printf "%s\n" "$WAITFOR"| cut -d : -f 2)
  for i in $(seq "${WAITFOR_TIMEOUT:-300}" -1 0) ; do
    set +e
    nc -z "$WAITFOR_HOST" "$WAITFOR_PORT" > /dev/null 2>&1
    result=$?
    set -e
    if [ $result -eq 0 ] ; then
      break
    fi
    sleep 1
  done
  if [ "$i" -eq 0 ]; then
     echo "Waiting for service $WAITFOR is timed out." >&2
     exit 1
  f
  fi
fi

if [ -n "$KERBEROS_ENABLED" ]; then
  echo "Setting up kerberos!!"
  KERBEROS_SERVER=${KERBEROS_SERVER:-krb5}
  ISSUER_SERVER=${ISSUER_SERVER:-$KERBEROS_SERVER\:8081}
  echo "KDC ISSUER_SERVER => $ISSUER_SERVER"

  if [ -n "$SLEEP_SECONDS" ]; then
    echo "Sleeping for $(SLEEP_SECONDS) seconds"
    sleep "$SLEEP_SECONDS"
  fi

  if [ -z "$KEYTAB_DIR" ]; then
    KEYTAB_DIR='/etc/security/keytabs'
  fi
  while true
    do
      set +e
      STATUS=$(curl -s -o /dev/null -w '%{http_code}' http://"$ISSUER_SERVER"/keytab/test/test)
      set -e
      if [ "$STATUS" -eq 200 ]; then
        echo "Got 200, KDC service ready!!"
        break
      else
        echo "Got $STATUS :( KDC service not ready yet..."
      fi
      sleep 5
    done

    HOST_NAME=$(hostname -f)
    export HOST_NAME
    for NAME in ${KERBEROS_KEYTABS}; do
      echo "Download $NAME/$HOSTNAME@EXAMPLE.COM keytab file to $KEYTAB_DIR/$NAME.keytab"
      wget "http://$ISSUER_SERVER/keytab/$HOST_NAME/$NAME" -O "$KEYTAB_DIR/$NAME.keytab"
      klist -kt "$KEYTAB_DIR/$NAME.keytab"
      KERBEROS_ENABLED=true
    done

    #Optional: let's try to adjust the krb5.conf
    sudo sed -i "s/krb5/$KERBEROS_SERVER/g" "/etc/krb5.conf" || true
fi

CONF_DESTINATION_DIR="${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}"

#Try to copy the defaults
set +e
if [[ -d "/opt/ozone/etc/hadoop" ]]; then
   cp /opt/hadoop/etc/hadoop/* "$CONF_DESTINATION_DIR/" > /dev/null 2>&1
elif [[ -d "/opt/hadoop/etc/hadoop" ]]; then
   cp /opt/hadoop/etc/hadoop/* "$CONF_DESTINATION_DIR/" > /dev/null 2>&1
fi
set -e

"$DIR"/envtoconf.py --destination "$CONF_DESTINATION_DIR"

if [ -n "$ENSURE_SCM_INITIALIZED" ]; then
  if [ ! -f "$ENSURE_SCM_INITIALIZED" ]; then
    # Improve om and scm start up options
    /opt/hadoop/bin/ozone scm --init || /opt/hadoop/bin/ozone scm -init
  fi
fi

if [ -n "$ENSURE_OM_INITIALIZED" ]; then
  if [ ! -f "$ENSURE_OM_INITIALIZED" ]; then
    # Improve om and scm start up options
    /opt/hadoop/bin/ozone om --init ||  /opt/hadoop/bin/ozone om -createObjectStore
  fi
fi

# Supports byteman script to instrument hadoop process with byteman script
#
#
if [ -n "$BYTEMAN_SCRIPT" ] || [ -n "$BYTEMAN_SCRIPT_URL" ]; then

  export PATH=$PATH:$BYTEMAN_DIR/bin

  if [ -n "$BYTEMAN_SCRIPT_URL" ]; then
    wget "$BYTEMAN_SCRIPT_URL" -O /tmp/byteman.btm
    export BYTEMAN_SCRIPT=/tmp/byteman.btm
  fi

  if [ ! -f "$BYTEMAN_SCRIPT" ]; then
    echo "ERROR: The defined $BYTEMAN_SCRIPT does not exist!!!"
    exit 255
  fi

  AGENT_STRING="-javaagent:/opt/byteman.jar=script:$BYTEMAN_SCRIPT"
  export HADOOP_OPTS="$AGENT_STRING $HADOOP_OPTS"
  echo "Process is instrumented with adding $AGENT_STRING to HADOOP_OPTS"
fi

exec "$@"
