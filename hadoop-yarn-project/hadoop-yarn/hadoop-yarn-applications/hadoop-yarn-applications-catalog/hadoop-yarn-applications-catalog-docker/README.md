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
# Apache Hadoop YARN AppCatalog Docker Image

## Introduction

AppCatalog Docker image is pre-packaged docker container for Hadoop Application Catalog.

check it out:

```
git clone https://github.com/apache/hadoop.git
```

## Compile

```
mvn package
```

## Run

```
docker run -d -p 8080:8080 -p 8983:8983 hadoop/appcatalog-docker:1.0-SNAPSHOT
```

When running this command a couple of things happens:
* Solr server will create appcatalog collection for hosting application catalog
* Sample applications are registered in Embedded Solr
* Tomcat will run appcatalog-webapp on port 8080

User can browse port 8080 to deploy application on a Hadoop cluster.
