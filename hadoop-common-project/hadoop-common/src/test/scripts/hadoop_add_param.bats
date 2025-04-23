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

load hadoop-functions_test_helper

@test "hadoop_add_param (positive 1)" {
  hadoop_add_param testvar foo foo
  echo ">${testvar}<"
  [ "${testvar}" = "foo" ]
}

@test "hadoop_add_param (negative)" {
  hadoop_add_param testvar foo foo
  hadoop_add_param testvar foo foo
  echo ">${testvar}<"
  [ "${testvar}" = "foo" ]
}

@test "hadoop_add_param (positive 2)" {
  hadoop_add_param testvar foo foo
  hadoop_add_param testvar foo foo
  hadoop_add_param testvar bar bar
  echo ">${testvar}<"
  [ "${testvar}" = "foo bar" ]
}

@test "hadoop_add_param (positive 3)" {
  hadoop_add_param testvar foo foo
  hadoop_add_param testvar foo foo
  hadoop_add_param testvar bar bar
  hadoop_add_param testvar bar bar
  hadoop_add_param testvar baz baz
  hadoop_add_param testvar baz baz

  echo ">${testvar}<"
  [ "${testvar}" = "foo bar baz" ]
}


@test "hadoop_add_param (HADOOP-16649 a)" {
  hadoop_add_param testvar hadoop-azure-datalake hadoop-azure-datalake
  hadoop_add_param testvar hadoop-azure hadoop-azure

  echo ">${testvar}<"
  [ "${testvar}" = "hadoop-azure-datalake hadoop-azure" ]
}


@test "hadoop_add_param (HADOOP-16649 b)" {
  hadoop_add_param testvar hadoop-azure hadoop-azure
  hadoop_add_param testvar hadoop-azure-datalake hadoop-azure-datalake

  echo ">${testvar}<"
  [ "${testvar}" = "hadoop-azure hadoop-azure-datalake" ]
}

@test "hadoop_add_param (HADOOP-16649 c )" {
  hadoop_add_param testvar Xmx -Xmx2048
  hadoop_add_param testvar Xmx -Xmx128
  hadoop_add_param testvar Xms -Xms32
  echo ">${testvar}<"
  [ "${testvar}" = "-Xmx2048 -Xms32" ]
}

