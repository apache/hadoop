---
title: Volume Commands
menu:
   main:
      parent: Client
      weight: 2
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

Volume commands generally need administrator privileges. The ozone shell supports the following volume commands.

  * [create](#create)
  * [delete](#delete)
  * [info](#info)
  * [list](#list)
  * [update](#update)

### Create

The volume create command allows an administrator to create a volume and
assign it to a user.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
| -q, --quota                    | Optional, This argument that specifies the maximum size this volume can use in the Ozone cluster.                    |
| -u, --user                     |  Required, The name of the user who owns this volume. This user can create, buckets and keys on this volume.                                       |
|  Uri                           | The name of the volume.                                        |

{{< highlight bash >}}
ozone sh volume create --quota=1TB --user=bilbo /hive
{{< /highlight >}}

The above command will create a volume called _hive_ on the ozone cluster. This
volume has a quota of 1TB, and the owner is _bilbo_.

### Delete

The volume delete command allows an administrator to delete a volume. If the
volume is not empty then this command will fail.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the volume.

{{< highlight bash >}}
ozone sh volume delete /hive
{{< /highlight >}}

The above command will delete the volume hive, if the volume has no buckets
inside it.

### Info

The volume info commands returns the information about the volume including
quota and owner information.
***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the volume.

{{< highlight bash >}}
ozone sh volume info /hive
{{< /highlight >}}

The above command will print out the information about hive volume.

### List

The volume list command will list the volumes owned by a user.

{{< highlight bash >}}
ozone sh volume list --user hadoop
{{< /highlight >}}

The above command will print out all the volumes owned by the user hadoop.

### Update

The volume update command allows changing of owner and quota on a given volume.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
| -q, --quota                    | Optional, This argument that specifies the maximum size this volume can use in the Ozone cluster.                    |
| -u, --user                     |  Optional, The name of the user who owns this volume. This user can create, buckets and keys on this volume.                                       |
|  Uri                           | The name of the volume.                                        |

{{< highlight bash >}}
ozone sh volume update --quota=10TB /hive
{{< /highlight >}}

The above command updates the volume quota to 10TB.

You can try out these commands from the docker instance of the [Alpha
Cluster](runningviadocker.html).
