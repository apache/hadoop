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

## Hadoop Docker

This directory contains Docker Compose definitions for running the current version of Hadoop in a multi-node docker environment.

This is meant for testing code changes locally and debugging.

The image used by the Docker setup is built as part of the maven lifecycle.

In order to start the docker environment you need to do the following
* Build the project, which will also build the docker image
  * ```shell
    > mvn clean install -Dmaven.javadoc.skip=true -DskipTests -DskipShade -Pdist,src
    ```
* From the project root, navigate under the docker-compose dir under the generated dist directory
  * ```shell
    > cd hadoop-dist/target/hadoop-<current-version>/compose/hadoop
    ```
* Start the docker environment
  * ```shell
    > docker-compose up -d --scale datanode=3
    ```
* Connect to a container to execute commands
  * ```shell
    > docker exec -it hadoop_datanode_1 bash
    bash-4.2$ hdfs dfs -mkdir /test
    ```

### Config files

To add or remove properties from the `core-site.xml`, `hdfs-site.xml`, etc. files used in the docker environment,
simply edit the `config` file before starting the containers. The changes will be persisted in the docker environment.
