<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
# Apache Hadoop YARN Application Catalog

## Introduction

Hadoop YARN Application Catalog is application catalog for
deploying docker enabled cloud application on Hadoop.

check it out:

```bash
git clone https://github.com/apache/hadoop.git
```

## Prerequisites
* Firefox or Chrome
* [npm](https://www.npmjs.org)
* [nodejs](http://nodejs.org)
* [JDK](http://www.oracle.com/technetwork/java/javaee/downloads/index.html)
* [IDE](http://www.jetbrains.com/)
* [bower](http://bower.io)
* [PhantomJs](http://phantomjs.org) or `brew install phantomjs`
* [Application Server](https://glassfish.java.net/download.html) or `brew install glassfish`

## Installation

```bash
mvn package
```

When running this command a couple of things happen:
* Npm and yarnpkg install will be run
* JSLint will be run in src/main/javascript sources
* Javascript will be minified
* All the other standard maven phases.

## Status of the project

See Apache [JIRA](http://issues.apache.org/jira/browse/YARN)
