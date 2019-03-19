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

# Docker cluster definitions

This directory contains multiple docker cluster definitions to start local pseudo cluster with different configuration.

It helps to start local (multi-node like) pseudo cluster with docker and docker-compose and obviously it's not for production.

You may find more information in the specific subdirectories but in generic you can use the following commands:

## Usage

To start a cluster go to a subdirectory and start the cluster:

```
docker-compose up -d
```

You can check the logs of all the components with:

```
docker-compose logs
```

In case of a problem you can destroy the cluster an delete all the local state with:

```
docker-compose down
```

(Note: a simple docker-compose stop may not delete all the local data).

You can scale up and down the components:

```
docker-compose scale datanode=5
```

Usually the key webui ports are published on the docker host.

## Known issues

The base image used here is apache/hadoop-runner, which runs with JDK8 by default.
You may run with JDK11 by specify apache/hadoop-runner:jdk11 as base image in simple mode.
But in secure mode, JDK 11 is not fully supported yet due to JDK8 dependencies from hadoop-common jars.
