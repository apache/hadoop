<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

This folder contains example byteman scripts (http://byteman.jboss.org/) to help 
Hadoop debuging.

As the startup script of the hadoop-runner docker image supports byteman 
instrumentation it's enough to set the URL of a script to a specific environment
variable to activate it with the docker runs:


```
BYTEMAN_SCRIPT_URL=https://raw.githubusercontent.com/apache/hadoop/trunk/dev-support/byteman/hadooprpc.btm
```

For more info see HADOOP-15656 and HDDS-342

