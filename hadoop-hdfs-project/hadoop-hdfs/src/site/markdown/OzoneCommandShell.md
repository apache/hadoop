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

Ozone Command Shell
===================

Ozone command shell gives a command shell interface to work against ozone.
Please note that this  document assumes that cluster is deployed
with simple authentication.

The Ozone commands take the following format.

* `hdfs oz --command_ http://hostname:port/volume/bucket/key -user
<name> -root`

The *port* specified in command should match the port mentioned in the config
property `dfs.datanode.http.address`. This property can be set in `hdfs-site.xml`.
The default value for the port is `9864` and is used in below commands.

The *--root* option is a command line short cut that allows *hdfs oz*
commands to be run as the user that started the cluster. This is useful to
indicate that you want the commands to be run as some admin user. The only
reason for this option is that it makes the life of a lazy developer more
easier.

Ozone Volume Commands
--------------------

The volume commands allow users to create, delete and list the volumes in the
ozone cluster.

### Create Volume

Volumes can be created only by Admins. Here is an example of creating a volume.

* `hdfs oz -createVolume http://localhost:9864/hive -user bilbo -quota
100TB -root`

The above command creates a volume called `hive` owned by user `bilbo`. The
`--root` option allows the command to be executed as user `hdfs` which is an
admin in the cluster.

### Update Volume

Updates information like ownership and quota on an existing volume.

* `hdfs oz  -updateVolume  http://localhost:9864/hive -quota 500TB -root`

The above command changes the volume quota of hive from 100TB to 500TB.

### Delete Volume
Deletes a Volume if it is empty.

* `hdfs oz -deleteVolume http://localhost:9864/hive -root`


### Info Volume
Info volume command allows the owner or the administrator of the cluster to read meta-data about a specific volume.

* `hdfs oz -infoVolume http://localhost:9864/hive -root`

### List Volumes

List volume command can be used by administrator to list volumes of any user. It can also be used by a user to list volumes owned by him.

* `hdfs oz -listVolume http://localhost:9864/ -user bilbo -root`

The above command lists all volumes owned by user bilbo.

Ozone Bucket Commands
--------------------

Bucket commands follow a similar pattern as volume commands. However bucket commands are designed to be run by the owner of the volume.
Following examples assume that these commands are run by the owner of the volume or bucket.


### Create Bucket

Create bucket call allows the owner of a volume to create a bucket.

* `hdfs oz -createBucket http://localhost:9864/hive/january`

This call creates a bucket called `january` in the volume called `hive`. If
the volume does not exist, then this call will fail.


### Update Bucket
Updates bucket meta-data, like ACLs.

* `hdfs oz -updateBucket http://localhost:9864/hive/january  -addAcl
user:spark:rw`

### Delete Bucket
Deletes a bucket if it is empty.

* `hdfs oz -deleteBucket http://localhost:9864/hive/january`

### Info Bucket
Returns information about a given bucket.

* `hdfs oz -infoBucket http://localhost:9864/hive/january`

### List Buckets
List buckets on a given volume.

* `hdfs oz -listtBucket http://localhost:9864/hive`

Ozone Key Commands
------------------

Ozone key commands allows users to put, delete and get keys from ozone buckets.

### Put Key
Creates or overwrites a key in ozone store, -file points to the file you want
to upload.

* `hdfs oz -putKey  http://localhost:9864/hive/january/processed.orc  -file
processed.orc`

### Get Key
Downloads a file from the ozone bucket.

* `hdfs oz -getKey  http://localhost:9864/hive/january/processed.orc  -file
  processed.orc.copy`

### Delete Key
Deletes a key  from the ozone store.

* `hdfs oz -deleteKey http://localhost:9864/hive/january/processed.orc`

### Info Key
Reads  key metadata from the ozone store.

* `hdfs oz -infoKey http://localhost:9864/hive/january/processed.orc`

### List Keys
List all keys in an ozone bucket.

* `hdfs oz -listKey  http://localhost:9864/hive/january`
