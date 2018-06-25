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

# Apache Hadoop base image

This is the definition of the Apache Hadoop base image. It doesn't use any Hadoop distribution just the scripts to run any Hadoop from source or from a prebuild package.

## Build

To create a local version of this image use the following command:

```
docker build -t apache/hadoop-runner .
```

## Usage

Do a full build on Apache Hadoop trunk with the `hdds` profile enabled.
```
mvn clean install package -DskipTests -Pdist,hdds -Dtar -Dmaven.javadoc.skip=true
```

Then start HDDS services with `docker-compose`.

```
cd hadoop-dist/target/compose/ozone
docker-compose up -d
```

## Troubleshooting

If `docker-compose` fails to work, check that the `hadoop-dist/target/compose/ozone/.env` file exists and has a line like the following (the exact version number may be different):
```
    HDDS_VERSION=0.2.1-SNAPSHOT
```
