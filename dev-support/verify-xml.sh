#!/bin/bash
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
# Script to run unit tests for xml <-> 1 or more Configuration file verification
# usage: ./verify-xml.sh <mode>
#

# Utility functions
function find_test_output_file() {
  echo "Found test output file(s) at"
  echo ""
  if [ -n "$1" ] && [ -e "$1" ] ; then
    echo "  $1"
  fi
  if [ -n "$2" ] && [ -e "$2" ] ; then
    echo "  $2"
  fi
  if [ -n "$3" ] && [ -e "$3" ] ; then
    echo "  $3"
  fi
  if [ -n "$4" ] && [ -e "$4" ] ; then
    echo "  $4"
  fi
  echo ""
  echo "Examine the file for specific information xml/Configuration mismatches."
  echo ""
}

function print_test_banner() {
  local banner_text=$1
  local banner_length=${#banner_text}
  local banner
  banner=$( printf "%${banner_length}s" ' ' )
  echo ""
  echo "${banner// /=}"
  echo "${banner_text}"
  echo "${banner// /=}"
  echo ""
}

# Wrapper functions for running unit tests
function run_all_xml_test() {
  mvn test -Dtest=TestCommonConfigurationFields,TestHdfsConfigFields,TestMapreduceConfigFields,TestYarnConfigurationFields
  if [ $? -ne 0 ] ; then
    print_test_banner "All Test*ConfigFields FAIL"
  else
    print_test_banner "All Test*ConfigFields SUCCESS"
  fi
}

function run_common_xml_test() {
  mvn test -Dtest=TestCommonConfigFields
  if [ $? -ne 0 ] ; then
    print_test_banner "TestCommonConfigurationFields FAIL"
  else
    print_test_banner "TestCommonConfigurationFields SUCCESS"
  fi
}

function run_hdfs_xml_test() {
  mvn test -Dtest=TestHdfsConfigFields
  if [ $? -ne 0 ] ; then
    print_test_banner "TestHdfsConfigFields FAIL"
  else
    print_test_banner "TestHdfsConfigFields SUCCESS"
  fi
}

function run_mapreduce_xml_test() {
  mvn test -Dtest=TestMapreduceConfigFields
  if [ $? -ne 0 ] ; then
    print_test_banner "TestMapreduceConfigFields FAIL"
  else
    print_test_banner "TestMapreduceConfigFields SUCCESS"
  fi
}

function run_yarn_xml_test() {
  mvn test -Dtest=TestYarnConfigurationFields
  if [ $? -ne 0 ] ; then
    print_test_banner "TestYarnConfigurationFields FAIL"
  else
    print_test_banner "TestYarnConfigurationFields SUCCESS"
  fi
}

# Main body
cd -P -- "$(dirname -- "${BASH_SOURCE-$0}")/.." || exit
dir="$(pwd -P)"

# - Create unit test file names
export commonOutputFile
commonOutputFile="$(find "${dir}" -name org.apache.hadoop.conf.TestCommonConfigurationFields-output.txt)"
export hdfsOutputFile
hdfsOutputFile="$(find "${dir}" -name org.apache.hadoop.tools.TestHdfsConfigFields-output.txt)"
export mrOutputFile
mrOutputFile="$(find "${dir}" -name org.apache.hadoop.mapreduce.TestMapreduceConfigFields-output.txt)"
export yarnOutputFile
yarnOutputFile="$(find "${dir}" -name org.apache.hadoop.yarn.conf.TestYarnConfigurationFields-output.txt)"

# - Determine which tests to run
case "$1" in

  all)
    run_all_xml_test
    find_test_output_file "${commonOutputFile}" "${hdfsOutputFile}" "${mrOutputFile}" "${yarnOutputFile}"
    ;;

  common)
    run_common_xml_test
    find_test_output_file "${commonOutputFile}"
    ;;

  hdfs)
    run_hdfs_xml_test
    find_test_output_file "${hdfsOutputFile}"
    ;;

  mr)
    run_mapreduce_xml_test
    find_test_output_file "${mrOutputFile}"
    ;;

  yarn)
    run_yarn_xml_test
    find_test_output_file "${yarnOutputFile}"
    ;;

  *)
    echo "Usage: $0 <mode>"
    echo "  where <mode> is one of all, common, hdfs, mr, yarn"
    exit 1
    ;;

esac
