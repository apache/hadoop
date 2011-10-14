#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



###############################################################################
# Run the following jobs to validate a hadoop cluster
## teragen
## terasort
## teravalidate
# If they all pass 0 will be returned and 1 otherwise
# The test will work for both secure and unsecure deploys. If the kerberos-realm
# is passed we will assume that the deploy is secure and proceed with a kinit before
# running the validation jobs.
################################################################################

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/../libexec/hadoop-config.sh

usage() {
  echo "
usage: $0 <parameters>

  Optional parameters:
     -h                                                              Display this message
     --user=hdfs
     --user_keytab=/home/hdfs/hdfs.keytab
     --kerberos-realm=KERBEROS.EXAMPLE.COM                           Set Kerberos realm
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'user:' \
  -l 'user-keytab:' \
  -l 'kerberos-realm:' \
  -o 'h' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "${OPTS}"
while true ; do
  case "$1" in
    --user)
      TEST_USER=$2; shift 2
      AUTOMATED=1
      ;;
    --user-keytab)
      USER_KEYTAB_FILE=$2; shift 2
      AUTOMATED=1
      ;;
    --kerberos-realm)
      KERBEROS_REALM=$2; shift 2
      AUTOMATED=1
      ;;
    --)
      shift ; break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

#set the hadoop command and the path to the hadoop examples jar
HADOOP_CMD="${HADOOP_PREFIX}/bin/hadoop --config $HADOOP_CONF_DIR"

#find the hadoop examples jar
HADOOP_EXAMPLES_JAR=''

#find under HADOOP_PREFIX (tar ball install)
HADOOP_EXAMPLES_JAR=`find ${HADOOP_PREFIX} -name 'hadoop-examples-*.jar' | head -n1`

#if its not found look under /usr/share/hadoop (rpm/deb installs)
if [ "$HADOOP_EXAMPLES_JAR" == '' ]
then
  HADOOP_EXAMPLES_JAR=`find /usr/share/hadoop -name 'hadoop-examples-*.jar' | head -n1`
fi

#if it is still empty then dont run the tests
if [ "$HADOOP_EXAMPLES_JAR" == '' ]
then
  echo "Did not find hadoop-examples-*.jar under '${HADOOP_PREFIX} or '/usr/share/hadoop'"
  exit 1
fi

# do a kinit if secure
if [ "${KERBEROS_REALM}" != "" ]; then
  # Determine kerberos location base on Linux distro.
  if [ -e /etc/lsb-release ]; then
    KERBEROS_BIN=/usr/bin
  else
    KERBEROS_BIN=/usr/kerberos/bin
  fi
  kinit_cmd="su -c '${KERBEROS_BIN}/kinit -kt ${USER_KEYTAB_FILE} ${TEST_USER}' ${TEST_USER}"
  echo $kinit_cmd
  eval $kinit_cmd
  if [ $? -ne 0 ]
  then
    echo "kinit command did not run successfully."
    exit 1
  fi
fi


#dir where to store the data on hdfs. The data is relative of the users home dir on hdfs.
PARENT_DIR="validate_deploy_`date +%s`"
TERA_GEN_OUTPUT_DIR="${PARENT_DIR}/tera_gen_data"
TERA_SORT_OUTPUT_DIR="${PARENT_DIR}/tera_sort_data"
TERA_VALIDATE_OUTPUT_DIR="${PARENT_DIR}/tera_validate_data"
#tera gen cmd
TERA_GEN_CMD="su -c '$HADOOP_CMD jar $HADOOP_EXAMPLES_JAR teragen 10000 $TERA_GEN_OUTPUT_DIR' $TEST_USER"

#tera sort cmd
TERA_SORT_CMD="su -c '$HADOOP_CMD jar $HADOOP_EXAMPLES_JAR terasort $TERA_GEN_OUTPUT_DIR $TERA_SORT_OUTPUT_DIR' $TEST_USER"

#tera validate cmd
TERA_VALIDATE_CMD="su -c '$HADOOP_CMD jar $HADOOP_EXAMPLES_JAR teravalidate $TERA_SORT_OUTPUT_DIR $TERA_VALIDATE_OUTPUT_DIR' $TEST_USER"

echo "Starting teragen...."

#run tera gen
echo $TERA_GEN_CMD
eval $TERA_GEN_CMD
if [ $? -ne 0 ]; then
  echo "tera gen failed."
  exit 1
fi

echo "Teragen passed starting terasort...."


#run tera sort
echo $TERA_SORT_CMD
eval $TERA_SORT_CMD
if [ $? -ne 0 ]; then
  echo "tera sort failed."
  exit 1
fi

echo "Terasort passed starting teravalidate...."

#run tera validate
echo $TERA_VALIDATE_CMD
eval $TERA_VALIDATE_CMD
if [ $? -ne 0 ]; then
  echo "tera validate failed."
  exit 1
fi

echo "teragen, terasort, teravalidate passed."
echo "Cleaning the data created by tests: $PARENT_DIR"

CLEANUP_CMD="su -c '$HADOOP_CMD dfs -rmr -skipTrash $PARENT_DIR' $TEST_USER"
echo $CLEANUP_CMD
eval $CLEANUP_CMD

exit 0
