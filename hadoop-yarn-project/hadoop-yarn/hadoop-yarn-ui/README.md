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

# YARN UI

The YARN UI is an Ember based web-app that provides visualization of the applications running on the Apache Hadoop YARN framework.

## Configurations

You can point the UI to custom locations by setting the environment variables in `src/main/webapp/config/configs.env`.

In order to make the UI running on Ember server (started by `yarn start`)
work with independently running ResouceManager,
you need to enable CORS by setting `hadoop.http.cross-origin.enabled` to true
and adding `org.apache.hadoop.security.HttpCrossOriginFilterInitializer`
to `hadoop.http.filter.initializers` in core-site.xml of the ResourceManager.

## Development

All the following commands must be run inside `src/main/webapp`.

### Prerequisites

You will need the following things properly installed on your computer.

* Install [Yarn](https://yarnpkg.com) v1.21.1
* Install [Bower](http://bower.io/) v1.8.8
* Install all dependencies by running `yarn install` & `bower install`

### Running UI

* `yarn start`
* Visit your app at [http://localhost:4200](http://localhost:4200).

### Building

* `yarn run build` (production)
* Files would be stored in "dist/"

### Adding new dependencies

**Warning: Do not edit the _package.json_ or _bower.json_ files manually. This could make them out-of-sync with the respective lock or shrinkwrap files.**

YARN UI has replaced NPM with Yarn package manager. And hence Yarn would be used to manage dependencies defined in package.json.

* Please use the Yarn and Bower command-line tools to add new dependencies. And the tool version must be same as those defined in Prerequisites section.
* Once any dependency is added:
  *  If it's in package.json. Make sure that the respective, and only those changes are reflected in yarn.lock file.
  *  If it's in bower.json. Make sure that the respective, and only those changes are reflected in bower-shrinkwrap.json file.
* Commands to add using CLI tools:
  * Yarn: yarn add [package-name]
  * Bower: bower install --save [package-name]

### Adding new routes (pages), controllers, components etc.

* Use ember-cli blueprint generator - [Ember CLI](http://ember-cli.com/extending/#generators-and-blueprints)

### Building with Maven

[YARN-6278](https://issues.apache.org/jira/browse/YARN-6278)
added `yarn-ui` profile to pom.xml leveraging
[frontend-maven-plugin](https://github.com/eirslett/frontend-maven-plugin) which
automatically installs Node.js and Yarn locally under target/webapp directory.
After building yarn-ui by `mvn package -Pyarn-ui`, you can reuse
the locally installed Node.js and Yarn instead of manually installing them.

```
$ mvn package -Pyarn-ui
$ export PATH=$PWD/target/webapp/node:$PATH
$ export YARNJS=$PWD/target/webapp/node/yarn/dist/bin/yarn.js
$ cd src/main/webapp/
$ node $YARNJS install
$ node node_modules/.bin/bower install
$ node $YARNJS start
```
