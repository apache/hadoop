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

# Compose files for local performance tests

This directory contains docker-compose definition for an ozone cluster where
all the metrics are saved to a prometheus instance.

 Prometheus follows a pull based approach where the metrics are published
 on a HTTP endpoint.

 Our current approach:

  1. A Java agent activates a prometheus metrics endpoint in every JVM instance
   (use `init.sh` to download the agent)

  2. The Java agent publishes all the jmx parameters in prometheus format AND
  register the endpoint address to the consul.

  3. Prometheus polls all the endpoints which are registered to consul.



## How to use

First of all download the required Java agent with running `./init.sh`

After that you can start the cluster with docker-compose:

```
docker-compose up -d
```

After a while the cluster will be started. You can check the ozone web ui-s:

https://localhost:9874
https://localhost:9876

You can also scale up the datanodes:

```
docker-compose scale datanode=3
```

Freon (Ozone test generator tool) is not part of docker-compose by default,
you can activate it using `compose-all.sh` instead of `docker-compose`:

```
compose-all.sh up -d
```

Now Freon is running. Let's try to check the metrics from the local Prometheus:

http://localhost:9090/graph

Example queries:

```
Hadoop_OzoneManager_NumKeyCommits
rate(Hadoop_OzoneManager_NumKeyCommits[10m])
rate(Hadoop_Ozone_BYTES_WRITTEN[10m])
```
