<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

The files in this directory are purely meant to provide additional examples for how to use libhdfs. They are compiled as
part of the build and are thus guaranteed to compile against the associated version of lidhdfs. However, no tests exists
for these examples so their functionality is not guaranteed.

The examples are written to run against a mini-dfs cluster. The script `test-libhdfs.sh` can setup a mini DFS cluster
that the examples can run against. Again, none of this is tested and is thus not guaranteed to work.