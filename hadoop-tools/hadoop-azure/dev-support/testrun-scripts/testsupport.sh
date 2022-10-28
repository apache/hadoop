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

resourceDir=src/test/resources/
accountSettingsFolderName=accountSettings
combtestfile=$resourceDir
combtestfile+=abfs-combination-test-configs.xml
logdir=dev-support/testlogs/

testresultsregex="Results:(\n|.)*?Tests run:"
accountConfigFileSuffix="_settings.xml"
testOutputLogFolder=$logdir
testlogfilename=combinationTestLogFile

fullRunStartTime=$(date +%s)
STARTTIME=$(date +%s)
ENDTIME=$(date +%s)

outputFormatOn="\033[0;95m"
outputFormatOff="\033[0m"

triggerRun()
{
  echo ' '
  combination=$1
  accountName=$2
  runTest=$3
  processcount=$4
  cleanUpTestContainers=$5

  if [ -z "$accountName" ]; then
    logOutput "ERROR: Test account not configured. Re-run the script and choose SET_OR_CHANGE_TEST_ACCOUNT to configure the test account."
    exit 1;
  fi
  accountConfigFile=$accountSettingsFolderName/$accountName$accountConfigFileSuffix
  rm -rf $combtestfile
  cat > $combtestfile << ENDOFFILE
<configuration>

</configuration>
ENDOFFILE
  propertiessize=${#PROPERTIES[@]}
  valuessize=${#VALUES[@]}
  if [ "$propertiessize" -ne "$valuessize" ]; then
    logOutput "Exiting. Number of properties and values differ for $combination"
    exit 1
  fi
  for ((i = 0; i < propertiessize; i++)); do
    key=${PROPERTIES[$i]}
    val=${VALUES[$i]}
    echo "Combination specific property setting: [ key=$key , value=$val ]"
    changeconf "$key" "$val"
  done
  formatxml "$combtestfile"
  xmlstarlet ed -P -L -s /configuration -t elem -n include -v "" $combtestfile
  xmlstarlet ed -P -L -i /configuration/include -t attr -n href -v "$accountConfigFile" $combtestfile
  xmlstarlet ed -P -L -i /configuration/include -t attr -n xmlns -v "http://www.w3.org/2001/XInclude" $combtestfile
  formatxml $combtestfile
  echo ' '
  echo "Activated [$combtestfile] - for account: $accountName for combination $combination"
  testlogfilename="$testOutputLogFolder/Test-Logs-$combination.txt"
  touch "$testlogfilename"

  if [ "$runTest" == true ]
  then
    STARTTIME=$(date +%s)
    echo "Running test for combination $combination on account $accountName [ProcessCount=$processcount]"
    logOutput "Test run report can be seen in $testlogfilename"
    mvn -T 1C -Dparallel-tests=abfs -Dscale -DtestsThreadCount="$processcount" verify >> "$testlogfilename" || true
    ENDTIME=$(date +%s)
    summary
  fi

  if [ "$cleanUpTestContainers" == true ]
  then
    mvn test -Dtest=org.apache.hadoop.fs.azurebfs.utils.CleanupTestContainers >> "$testlogfilename" || true
    if grep -q "There are test failures" "$testlogfilename";
    then logOutput "ERROR: All test containers could not be deleted. Detailed error cause in $testlogfilename"
    pcregrep -M "$testresultsregex" "$testlogfilename"
    exit 0
    fi

    logOutput "Delete test containers - complete. Test run logs in - $testlogfilename"
  fi

}

summary() {
  {
    echo ""
    echo "$combination"
    echo "========================"
    pcregrep -M "$testresultsregex" "$testlogfilename"
  } >> "$aggregatedTestResult"
  printf "\n----- Test results -----\n"
  pcregrep -M "$testresultsregex" "$testlogfilename"
  secondstaken=$((ENDTIME - STARTTIME))
  mins=$((secondstaken / 60))
  secs=$((secondstaken % 60))
  printf "\nTime taken: %s mins %s secs.\n" "$mins" "$secs"
  echo "Find test result for the combination ($combination) in: $testlogfilename"
  logOutput "Consolidated test result is saved in: $aggregatedTestResult"
  echo "------------------------"
}

checkdependencies() {
  if ! [ "$(command -v pcregrep)" ]; then
    logOutput "Exiting. pcregrep is required to run the script."
    exit 1
  fi
  if ! [ "$(command -v xmlstarlet)" ]; then
    logOutput "Exiting. xmlstarlet is required to run the script."
    exit 1
  fi
}

formatxml() {
  xmlstarlet fo -s 2 "$1" > "$1.tmp"
  mv "$1.tmp" "$1"
}

changeconf() {
  xmlstarlet ed -P -L -d "/configuration/property[name='$1']" "$combtestfile"
  xmlstarlet ed -P -L -s /configuration -t elem -n propertyTMP -v "" -s /configuration/propertyTMP -t elem -n name -v "$1" -r /configuration/propertyTMP -v property "$combtestfile"
  if ! xmlstarlet ed -P -L -s "/configuration/property[name='$1']" -t elem -n value -v "$2" "$combtestfile"
  then
    logOutput "Exiting. Changing config property failed."
    exit 1
  fi
}

init() {
  checkdependencies
  if ! mvn clean install -DskipTests
  then
    echo ""
    echo "Exiting. Build failed."
    exit 1
  fi
  starttime=$(date +"%Y-%m-%d_%H-%M-%S")
  testOutputLogFolder+=$starttime
  mkdir -p "$testOutputLogFolder"
  aggregatedTestResult="$testOutputLogFolder/Test-Results.txt"
 }

 printAggregate() {
   echo  :::: AGGREGATED TEST RESULT ::::
   cat "$aggregatedTestResult"
  fullRunEndTime=$(date +%s)
  fullRunTimeInSecs=$((fullRunEndTime - fullRunStartTime))
  mins=$((fullRunTimeInSecs / 60))
  secs=$((fullRunTimeInSecs % 60))
  printf "\nTime taken: %s mins %s secs.\n" "$mins" "$secs"
 }

logOutput() {
  echo -e "$outputFormatOn" "$1" "$outputFormatOff"
}
