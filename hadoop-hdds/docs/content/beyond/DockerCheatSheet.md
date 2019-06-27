---
title: "Docker Cheat Sheet"
date: 2017-08-10
summary: Docker Compose cheat sheet to help you remember the common commands to control an Ozone cluster running on top of Docker.
weight: 4
---

<!---
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

In the `compose` directory of the ozone distribution there are multiple pseudo-cluster setup which can be used to run Ozone in different way (for example with secure cluster, with tracing enabled, with prometheus etc.).

If the usage is not document in a specific directory the default usage is the following:

```bash
cd compose/ozone
docker-compose up -d
```

The data of the container is ephemeral and deleted together with the docker volumes. To force the deletion of existing data you can always delete all the temporary data:

```bash
docker-compose down
```

## Useful Docker & Ozone Commands

If you make any modifications to ozone, the simplest way to test it is to run freon and unit tests.

Here are the instructions to run freon in a docker-based cluster.

{{< highlight bash >}}
docker-compose exec datanode bash
{{< /highlight >}}

This will open a bash shell on the data node container.
Now we can execute freon for load generation.

{{< highlight bash >}}
ozone freon randomkeys --numOfVolumes=10 --numOfBuckets 10 --numOfKeys 10
{{< /highlight >}}

Here is a set of helpful commands for working with docker for ozone.
To check the status of the components:

{{< highlight bash >}}
docker-compose ps
{{< /highlight >}}

To get logs from a specific node/service:

{{< highlight bash >}}
docker-compose logs scm
{{< /highlight >}}


As the WebUI ports are forwarded to the external machine, you can check the web UI:

* For the Storage Container Manager: http://localhost:9876
* For the Ozone Manager: http://localhost:9874
* For the Datanode: check the port with `docker ps` (as there could be multiple data nodes, ports are mapped to the ephemeral port range)

You can start multiple data nodes with:

{{< highlight bash >}}
docker-compose scale datanode=3
{{< /highlight >}}

You can test the commands from the [Ozone CLI]({{< ref "shell/_index.md" >}}) after opening a new bash shell in one of the containers:

{{< highlight bash >}}
docker-compose exec datanode bash
{{< /highlight >}}
