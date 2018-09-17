---
title: Bucket Commands
menu:
   main:
      parent: Client
      weight: 3
---

Ozone shell supports the following bucket commands.

  * [create](#create)
  * [delete](#delete)
  * [info](#info)
  * [list](#list)
  * [update](#update)

### Create

The bucket create command allows a user to create a bucket.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the bucket in **/volume/bucket** format.

{{< highlight bash >}}
ozone sh bucket create /hive/jan
{{< /highlight >}}

The above command will create a bucket called _jan_ in the _hive_ volume.
Since no scheme was specified this command defaults to O3 (RPC) protocol.

### Delete

The bucket delete commands allows an user to delete a volume. If the
bucket is not empty then this command will fail.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the bucket

{{< highlight bash >}}
ozone sh volume delete /hive/jan
{{< /highlight >}}

The above command will delete _jan_ bucket if it is empty.

### Info

The bucket info commands returns the information about the bucket.
***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the bucket.

{{< highlight bash >}}
ozone sh bucket info /hive/jan
{{< /highlight >}}

The above command will print out the information about _jan_ bucket.

### List

The bucket list commands allows uset to list the buckets in a volume.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
| -l, --length                   | Maximum number of results to return. Default: 100
| -p, --prefix                   | Optional, Only buckets that match this prefix will be returned.
| -s, --start                    | The listing will start from key after the start key.
|  Uri                           | The name of the _volume_.

{{< highlight bash >}}
ozone sh bucket list /hive
{{< /highlight >}}

This command will  list all buckets on the volume _hive_.



### Update

The bucket update command allows changing access permissions on bucket.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
| --addAcl                       | Optional, Comma separated ACLs that will added to bucket.
|  --removeAcl                   | Optional, Comma separated list of acl to remove.
|  Uri                           | The name of the bucket.

{{< highlight bash >}}
ozone sh bucket update --addAcl=user:bilbo:rw /hive/jan
{{< /highlight >}}

The above command gives user bilbo read/write permission to the bucket.

You can try out these commands from the docker instance of the [Alpha
Cluster](runningviadocker.html).
