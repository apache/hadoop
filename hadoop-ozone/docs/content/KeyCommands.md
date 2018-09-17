---
title: Key Commands
menu:
   main:
      parent: Client
      weight: 3
---

Ozone shell supports the following key commands.

  * [get](#get)
  * [put](#put)
  * [delete](#delete)
  * [info](#info)
  * [list](#list)


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

The delete key command removes the key from the bucket.

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

The key list commands allows user to list all keys in a bucket.

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

This command will  list all key in the bucket _/hive/jan_.





You can try out these commands from the docker instance of the [Alpha
Cluster](runningviadocker.html).
