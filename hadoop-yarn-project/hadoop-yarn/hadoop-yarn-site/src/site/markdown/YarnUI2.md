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
*This is a WIP project, nobody should use it in production.*

Prerequisites
-------------

You will need the following things properly installed on your computer.

* Install Node.js with NPM: https://nodejs.org/download/
* After Node.js installed, install bower: `npm install -g bower`.
* Install Ember-cli: `npm install -g ember-cli`

BUILD
----
* Please refer to BUILDING.txt in the top directory and pass -Pyarn-ui to build UI-related code
* Execute `mvn test -Pyarn-ui` to run unit tests

Try it
------

* Packaging and deploying Hadoop in this branch
* In `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-ui/src/main/webapp/app/config.js`, change `timelineWebUrl` and `rmWebUrl` to your YARN RM/Timeline server web address. 
* If you are running YARN RM in your localhost, you should update `localBaseUrl` to `localhost:1337/`, install `npm install -g corsproxy` and run `corsproxy` to avoid CORS errors. More details: `https://www.npmjs.com/package/corsproxy`. 
* Run `ember serve` under `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-ui/src/main/webapp/`
* Visit your app at [http://localhost:4200](http://localhost:4200).
