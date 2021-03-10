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

testresourcesdir=src/test/resources
combconfsdir=$testresourcesdir/combinationConfigFiles
combtestfile=$testresourcesdir/abfs-combination-test-configs.xml

logdir=dev-support/testlogs
testresultsregex="Results:(\n|.)*?Tests run:"
testresultsfilename=
starttime=
threadcount=
defaultthreadcount=8

properties=
values=

validate() {
  if [ -z "$threadcount" ] ; then
    threadcount=$defaultthreadcount
  fi
  numberegex='^[0-9]+$'
  if ! [[ $threadcount =~ $numberegex ]] ; then
    echo "Exiting. The script param (threadcount) should be a number"
    exit -1
  fi
  if [ -z "$combination" ]; then
   echo "Exiting. combination cannot be empty"
   exit -1
  fi
  propertiessize=${#properties[@]}
  valuessize=${#values[@]}
  if [ "$propertiessize" -lt 1 ] || [ "$valuessize" -lt 1 ] || [ "$propertiessize" -ne "$valuessize" ]; then
    echo "Exiting. Both properties and values arrays has to be populated and of same size. Please check for combination $combination"
    exit -1
  fi

  for filename in "${combinations[@]}"; do
    if [[ ! -f "$combconfsdir/$filename.xml" ]]; then
      echo "Exiting. Combination config file ($combconfsdir/$combination.xml) does not exist."
      exit -1
    fi
  done
}

checkdependencies() {
  if ! [ "$(command -v pcregrep)" ]; then
    echo "Exiting. pcregrep is required to run the script."
    exit -1
  fi
  if ! [ "$(command -v xmlstarlet)" ]; then
    echo "Exiting. xmlstarlet is required to run the script."
    exit -1
  fi
}

cleancombinationconfigs() {
  rm -rf $combconfsdir
  mkdir -p $combconfsdir
}

generateconfigs() {
  combconffile="$combconfsdir/$combination.xml"
  rm -rf "$combconffile"
  cat > "$combconffile" << ENDOFFILE
<configuration>

</configuration>
ENDOFFILE

  propertiessize=${#properties[@]}
  valuessize=${#values[@]}
  if [ "$propertiessize" -ne "$valuessize" ]; then
    echo "Exiting. Number of properties and values differ for $combination"
    exit -1
  fi
  for ((i = 0; i < propertiessize; i++)); do
    key=${properties[$i]}
    val=${values[$i]}
    changeconf "$key" "$val"
  done
  formatxml "$combconffile"
}

formatxml() {
  xmlstarlet fo -s 2 "$1" > "$1.tmp"
  mv "$1.tmp" "$1"
}

setactiveconf() {
  if [[ ! -f "$combconfsdir/$combination.xml" ]]; then
    echo "Exiting. Combination config file ($combconfsdir/$combination.xml) does not exist."
    exit -1
  fi
  rm -rf $combtestfile
  cat > $combtestfile << ENDOFFILE
<configuration>

</configuration>
ENDOFFILE
  xmlstarlet ed -P -L -s /configuration -t elem -n include -v "" $combtestfile
  xmlstarlet ed -P -L -i /configuration/include -t attr -n href -v "combinationConfigFiles/$combination.xml" $combtestfile
  xmlstarlet ed -P -L -i /configuration/include -t attr -n xmlns -v "http://www.w3.org/2001/XInclude" $combtestfile
  formatxml $combtestfile
}

changeconf() {
  xmlstarlet ed -P -L -d "/configuration/property[name='$1']" "$combconffile"
  xmlstarlet ed -P -L -s /configuration -t elem -n propertyTMP -v "" -s /configuration/propertyTMP -t elem -n name -v "$1" -r /configuration/propertyTMP -v property "$combconffile"
  if ! xmlstarlet ed -P -L -s "/configuration/property[name='$1']" -t elem -n value -v "$2" "$combconffile"
  then
    echo "Exiting. Changing config property failed."
    exit -1
  fi
}

summary() {
  {
    echo ""
    echo "$combination"
    echo "========================"
    pcregrep -M "$testresultsregex" "$testlogfilename"
  } >> "$testresultsfilename"
  printf "\n----- Test results -----\n"
  pcregrep -M "$testresultsregex" "$testlogfilename"

  secondstaken=$((ENDTIME - STARTTIME))
  mins=$((secondstaken / 60))
  secs=$((secondstaken % 60))
  printf "\nTime taken: %s mins %s secs.\n" "$mins" "$secs"
  echo "Find test logs for the combination ($combination) in: $testlogfilename"
  echo "Find consolidated test results in: $testresultsfilename"
  echo "----------"
}

init() {
  checkdependencies
  if ! mvn clean install -DskipTests
  then
    echo ""
    echo "Exiting. Build failed."
    exit -1
  fi
  starttime=$(date +"%Y-%m-%d_%H-%M-%S")
  mkdir -p "$logdir"
  testresultsfilename="$logdir/$starttime/Test-Results.txt"
  if [[ -z "$combinations" ]]; then
    combinations=( $( ls $combconfsdir/*.xml ))
  fi
}

runtests() {
  parseoptions "$@"
  validate
  if [ -z "$starttime" ]; then
    init
  fi
  shopt -s nullglob
  for combconffile in "${combinations[@]}"; do
    STARTTIME=$(date +%s)
    combination=$(basename "$combconffile" .xml)
    mkdir -p "$logdir/$starttime"
    testlogfilename="$logdir/$starttime/Test-Logs-$combination.txt"
    printf "\nRunning the combination: %s..." "$combination"
    setactiveconf
    mvn -T 1C -Dparallel-tests=abfs -Dscale -DtestsThreadCount=$threadcount verify >> "$testlogfilename" || true
    ENDTIME=$(date +%s)
    summary
  done
}

begin() {
  cleancombinationconfigs
}

parseoptions() {
runactivate=0
runtests=0
  while getopts ":c:a:t:" option; do
    case "${option}" in
      a)
        if [[ "$runactivate" -eq "1" ]]; then
          echo "-a Option is not multivalued"
          exit 1
        fi
        runactivate=1
        combination=$(basename "$OPTARG" .xml)
        ;;
      c)
        runtests=1
        combination=$(basename "$OPTARG" .xml)
        combinations+=("$combination")
        ;;
      t)
        threadcount=$OPTARG
        ;;
      *|?|h)
      # shellcheck disable=SC2128
        if [[ -z "$combinations" ]]; then
          combinations=( $( ls $combconfsdir/*.xml ))
        fi
      combstr=""
        for combconffile in "${combinations[@]}"; do
          combname=$(basename "$combconffile" .xml)
          combstr="${combname}, ${combstr}"
        done
        combstr=${combstr:0:-2}

        echo "Usage: $0 [-n] [-a COMBINATION_NAME] [-c COMBINATION_NAME] [-t THREAD_COUNT]"
        echo ""
        echo "Where:"
        echo "  -a COMBINATION_NAME   Specify the combination name which needs to be activated."
        echo "                        Configured combinations: ${combstr}"
        echo "  -c COMBINATION_NAME   Specify the combination name for test runs"
        echo "  -t THREAD_COUNT       Specify the thread count"
        exit 1
        ;;
    esac
  done
  if [[ "$runactivate" -eq "1" && "$runtests" -eq "1" ]]; then
    echo "Both activate (-a option) and test run combinations (-c option) cannot be specified together"
    exit 1
  fi
  if [[ "$runactivate" -eq "1" ]]; then
        setactiveconf
        exit 0
  fi
}
