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
# script to find hanging test from Jenkins build output
# usage: ./findHangingTest.sh <url of Jenkins build console>
#
`curl -k -o jenkins.out "$1"`
expecting=Running
cat jenkins.out | while read line; do
 if [[ "$line" =~ "Running org.apache.hadoop" ]]; then
  if [[ "$expecting" =~ "Running" ]]; then 
   expecting=Tests
  else
   echo "Hanging test: $prevLine"
  fi
 fi
 if [[ "$line" =~ "Tests run" ]]; then
  expecting=Running
 fi
 if [[ "$line" =~ "Forking command line" ]]; then
  a=$line
 else
  prevLine=$line
 fi
done
