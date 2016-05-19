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

@test "hadoop_escape_sed (positive 1)" {
  ret="$(hadoop_sed_escape "\pass&&word\0#\$asdf/g  ><'\"~\`!@#$%^&*()_+-=")"
  expected="\\\\pass\&\&word\\\0#\$asdf\/g  ><'\"~\`!@#$%^\&*()_+-="
  echo "actual >${ret}<"
  echo "expected >${expected}<"
  [ "${ret}" = "${expected}" ]
}

@test "hadoop_escape_xml (positive 1)" {
  ret="$(hadoop_xml_escape "\pass&&word\0#\$asdf/g  ><'\"~\`!@#$%^&*()_+-=")"
  expected="\\pass&amp;&amp;word\0#\$asdf/g  \&gt;\&lt;\&apos;\&quot;~\`!@#\$%^&amp;*()_+-="
  echo "actual >${ret}<"
  echo "expected >${expected}<"
  [ "${ret}" = "${expected}" ]
}