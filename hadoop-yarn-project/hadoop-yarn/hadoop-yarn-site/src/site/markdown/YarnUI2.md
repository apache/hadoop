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

Hadoop: YARN-UI V2
=================

Prerequisites
-------------

If you run RM locally in your computer for test purpose, you need the following things properly installed.

- Install Node.js with NPM: https://nodejs.org/download
- After Node.js installed, install `corsproxy`: `npm install -g corsproxy`.


Configurations
-------------

*In yarn-site.xml*

| Configuration Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.webapp.ui2.enable` | In the server side it indicates whether the new YARN-UI v2 is enabled or not. Defaults to `false`. |
| `yarn.resourcemanager.webapp.ui2.address` | Specify the address of ResourceManager and port which host YARN-UI v2, defaults to `localhost:8288`. |

*In $HADOOP_PREFIX/share/hadoop/yarn/webapps/rm/config/configs.env*

- Update timelineWebAddress and rmWebAddress to the actual addresses run resource manager and timeline server
- If you run RM locally in you computer just for test purpose, you need to keep `corsproxy` running. Otherwise, you need to set `localBaseAddress` to empty.

Use it
-------------
Open your browser, go to `rm-address:8288` and try it!
