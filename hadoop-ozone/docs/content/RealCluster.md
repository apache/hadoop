---
title: Starting an Ozone Cluster
weight: 1
menu:
   main:
      parent: Starting
      weight: 3
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

Before we boot up the Ozone cluster, we need to initialize both SCM and Ozone Manager.

{{< highlight bash >}}
ozone scm -init
{{< /highlight >}}
This allows SCM to create the cluster Identity and initialize its state.
The ```init``` command is similar to Namenode format. Init command is executed only once, that allows SCM to create all the required on-disk structures to work correctly.
{{< highlight bash >}}
ozone --daemon start scm
{{< /highlight >}}

Once we know SCM is up and running, we can create an Object Store for our use. This is done by running the following command.

{{< highlight bash >}}
ozone om -createObjectStore
{{< /highlight >}}


Once Ozone manager has created the Object Store, we are ready to run the name
services.

{{< highlight bash >}}
ozone --daemon start om
{{< /highlight >}}

At this point Ozone's name services, the Ozone manager, and the block service  SCM is both running.
**Please note**: If SCM is not running
```createObjectStore``` command will fail. SCM start will fail if on-disk data structures are missing. So please make sure you have done both ```init``` and ```createObjectStore``` commands.

Now we need to start the data nodes. Please run the following command on each datanode.
{{< highlight bash >}}
ozone --daemon start datanode
{{< /highlight >}}

At this point SCM, Ozone Manager and data nodes are up and running.

***Congratulations!, You have set up a functional ozone cluster.***

-------
If you want to make your life simpler, you can just run
{{< highlight bash >}}
ozone scm -init
ozone om -createObjectStore
start-ozone.sh
{{< /highlight >}}
This assumes that you have set up the slaves file correctly and ssh
configuration that allows ssh-ing to all data nodes. This is the same as the
HDFS configuration, so please refer to HDFS documentation on how to set this
up.
