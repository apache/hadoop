---
title: "Dozone & Dev Tools"
date: 2017-08-10
menu:
   main:
      parent: Tools
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

Dozone stands for docker for ozone. Ozone supports docker to make it easy to develop and test ozone.  Starting a docker-based ozone container is simple.

In the `compose/ozone` directory there are two files that define the docker and ozone settings.

Developers can

{{< highlight bash >}}
cd compose/ozone
{{< /highlight >}}

and simply run

{{< highlight bash >}}
docker-compose up -d
{{< /highlight >}}

to run a ozone cluster on docker.

This command will launch OM, SCM and a data node.

To access the OM UI, one can view http://localhost:9874.

_Please note_: dozone does not map the data node ports to the 9864. Instead, it maps to the ephemeral port range. So many examples in the command shell will not work if you run those commands from the host machine. To find out where the data node port is listening, you can run the `docker ps` command or always ssh into a container before running ozone commands.

To shutdown a running docker-based ozone cluster, please run

{{< highlight bash >}}
docker-compose down
{{< /highlight >}}


Adding more config settings
---------------------------
The file called `docker-config` contains all ozone specific config settings. This file is processed to create the ozone-site.xml.

Useful Docker & Ozone Commands
------------------------------

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

You can test the commands from the [Ozone CLI]({{< ref "CommandShell.md#shell" >}}) after opening a new bash shell in one of the containers:

{{< highlight bash >}}
docker-compose exec datanode bash
{{< /highlight >}}
