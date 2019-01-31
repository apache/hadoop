---
title: Key Commands
menu:
   main:
      parent: Client
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

Ozone shell supports the following key commands.

  * [get](#get)
  * [put](#put)
  * [delete](#delete)
  * [info](#info)
  * [list](#list)
  * [rename](#rename)


### Get

The key get command downloads a key from Ozone cluster to local file system.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the key in **/volume/bucket/key** format.
|  FileName                      | Local file to download the key to.


{{< highlight bash >}}
ozone sh key get /hive/jan/sales.orc sales.orc
{{< /highlight >}}
Downloads the file sales.orc from the _/hive/jan_ bucket and writes to the
local file sales.orc.

### Put

Uploads a file from the local file system to the specified bucket.

***Params:***


| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the key in **/volume/bucket/key** format.
|  FileName                      | Local file to upload.
| -r, --replication              | Optional, Number of copies, ONE or THREE are the options. Picks up the default from cluster configuration.

{{< highlight bash >}}
ozone sh key put /hive/jan/corrected-sales.orc sales.orc
{{< /highlight >}}
The above command will put the sales.orc as a new key into _/hive/jan/corrected-sales.orc_.

### Delete

The key delete command removes the key from the bucket.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the key.

{{< highlight bash >}}
ozone sh key delete /hive/jan/corrected-sales.orc
{{< /highlight >}}

The above command deletes the key _/hive/jan/corrected-sales.orc_.


### Info

The key info commands returns the information about the key.
***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the key.

{{< highlight bash >}}
ozone sh key info /hive/jan/sales.orc
{{< /highlight >}}

The above command will print out the information about _/hive/jan/sales.orc_
key.

### List

The key list command allows user to list all keys in a bucket.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
| -l, --length                   | Maximum number of results to return. Default: 1000
| -p, --prefix                   | Optional, Only buckets that match this prefix will be returned.
| -s, --start                    | The listing will start from key after the start key.
|  Uri                           | The name of the _volume_.

{{< highlight bash >}}
ozone sh key list /hive/jan
{{< /highlight >}}

This command will list all keys in the bucket _/hive/jan_.

### Rename

The `key rename` command changes the name of an existing key in the specified bucket.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the bucket in **/volume/bucket** format.
|  FromKey                       | The existing key to be renamed
|  ToKey                         | The new desired name of the key

{{< highlight bash >}}
ozone sh key rename /hive/jan sales.orc new_name.orc
{{< /highlight >}}
The above command will rename `sales.orc` to `new_name.orc` in the bucket `/hive/jan`.




You can try out these commands from the docker instance of the [Alpha
Cluster](runningviadocker.html).
