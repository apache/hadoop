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
# Apache Hadoop YARN Application Catalog

## Introduction

YARN Application Catalog is application catalog for
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
* [Docker](http://docker.io)

## Installation

```bash
mvn package
```

When running this command a couple of things happen:
* YARN Application Catalog web application is built
* YARN Application Docker image is built

## Status of the project

See Apache [JIRA](http://issues.apache.org/jira/browse/HADOOP)
