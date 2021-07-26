#!/usr/bin/env bash

# shellcheck disable=SC2034
# unused variables are global in nature and used in testsupport.sh

set -eo pipefail

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

# shellcheck disable=SC1091
. dev-support/testrun-scripts/testsupport.sh
init

resourceDir=src/test/resources/
logdir=dev-support/testlogs/
azureTestXml=azure-auth-keys.xml
azureTestXmlPath=$resourceDir$azureTestXml
threadCount=8

## SECTION: TEST COMBINATION METHODS

runHNSOAuthTest()
{
  accountName=$(xmlstarlet sel -t -v '//property[name = "fs.azure.hnsTestAccountName"]/value' $azureTestXmlPath)
  PROPERTIES=("fs.azure.account.auth.type")
  VALUES=("OAuth")
  triggerRun "HNS-OAuth" "$accountName" "$runTest" $threadCount "$cleanUpTestContainers"
}

runHNSSharedKeyTest()
{
  accountName=$(xmlstarlet sel -t -v '//property[name = "fs.azure.hnsTestAccountName"]/value' $azureTestXmlPath)
  PROPERTIES=("fs.azure.account.auth.type")
  VALUES=("SharedKey")
  triggerRun "HNS-SharedKey" "$accountName"  "$runTest" $threadCount "$cleanUpTestContainers"
}

runNonHNSSharedKeyTest()
{
  accountName=$(xmlstarlet sel -t -v '//property[name = "fs.azure.nonHnsTestAccountName"]/value' $azureTestXmlPath)
  PROPERTIES=("fs.azure.account.auth.type")
  VALUES=("SharedKey")
  triggerRun "NonHNS-SharedKey" "$accountName" "$runTest" $threadCount "$cleanUpTestContainers"
}

runAppendBlobHNSOAuthTest()
{
  accountName=$(xmlstarlet sel -t -v '//property[name = "fs.azure.hnsTestAccountName"]/value' $azureTestXmlPath)
  PROPERTIES=("fs.azure.account.auth.type" "fs.azure.test.appendblob.enabled")
  VALUES=("OAuth" "true")
  triggerRun "AppendBlob-HNS-OAuth" "$accountName" "$runTest" $threadCount "$cleanUpTestContainers"
}

runTest=false
cleanUpTestContainers=false
echo 'Ensure below are complete before running script:'
echo '1. Account specific settings file is present.'
echo '   Copy accountName_settings.xml.template to accountName_settings.xml'
echo '   where accountName in copied file name should be the test account name without domain'
echo '   (accountName_settings.xml.template is present in src/test/resources/accountName_settings'
echo '   folder. New account settings file to be added to same folder.)'
echo '   Follow instructions in the template to populate settings correctly for the account'
echo '2. In azure-auth-keys.xml, update properties fs.azure.hnsTestAccountName and fs.azure.nonHnsTestAccountName'
echo '   where accountNames should be the test account names without domain'
echo ' '
echo ' '
echo 'Choose action:'
echo '[Note - SET_ACTIVE_TEST_CONFIG will help activate the config for IDE/single test class runs]'
select scriptMode in SET_ACTIVE_TEST_CONFIG RUN_TEST CLEAN_UP_OLD_TEST_CONTAINERS
do
  case $scriptMode in
  SET_ACTIVE_TEST_CONFIG)
    runTest=false
    break
    ;;
  RUN_TEST)
    runTest=true
    read -r -p "Enter parallel test run thread count [default - 8]: " threadCount
    threadCount=${threadCount:-8}
    break
    ;;
  CLEAN_UP_OLD_TEST_CONTAINERS)
    runTest=false
    cleanUpTestContainers=true
    break
    ;;
  *) echo "ERROR: Invalid selection"
      ;;
   esac
done

## SECTION: COMBINATION DEFINITIONS AND TRIGGER

echo ' '
echo 'Set the active test combination to run the action:'
select combo in HNS-OAuth HNS-SharedKey nonHNS-SharedKey AppendBlob-HNS-OAuth All Quit
do
   case $combo in
      HNS-OAuth)
         runHNSOAuthTest
         break
         ;;
      HNS-SharedKey)
         runHNSSharedKeyTest
         break
         ;;
      nonHNS-SharedKey)
         runNonHNSSharedKeyTest
         break
         ;;
       AppendBlob-HNS-OAuth)
         runAppendBlobHNSOAuthTest
         break
         ;;
      All)
        if [ $runTest == false ]
        then
          echo "ERROR: Invalid selection for SET_ACTIVE_TEST_CONFIG"
          break
        fi
         runHNSOAuthTest
         runHNSSharedKeyTest
         runNonHNSSharedKeyTest
         runAppendBlobHNSOAuthTest ## Keep this as the last run scenario always
         break
         ;;
      Quit)
         exit 0
         ;;
      *) echo "ERROR: Invalid selection"
      ;;
   esac
done

if [ $runTest == true ]
then
  printAggregate
fi

