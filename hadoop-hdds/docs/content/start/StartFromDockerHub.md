---
title: Simple Single Ozone

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

{{< requirements >}}
 * Working docker setup
 * AWS CLI (optional)
{{< /requirements >}}

# Ozone in a Single Container

The easiest way to start up an all-in-one ozone container is to use the latest
docker image from docker hub:

```bash
docker run -P 9878:9878 -P 9876:9876 apache/ozone
```
This command will pull down the ozone image from docker hub and start all
ozone services in a single container. <br>
This container will run the required metadata servers (Ozone Manager, Storage
Container Manager) one data node  and the S3 compatible REST server
(S3 Gateway).

# Local multi-container cluster

If you would like to use a more realistic pseud-cluster where each components
run in own containers, you can start it with a docker-compose file.

We have shipped a docker-compose and an enviorment file as part of the
container image  that is uploaded to docker hub.

The following commands can be used to extract these files from the image in the docker hub.
```bash
docker run apache/ozone cat docker-compose.yaml > docker-compose.yaml
docker run apache/ozone cat docker-config > docker-config
```

 Now you can start the cluster with docker-compose:

```bash
docker-compose up -d
```

If you need multiple datanodes, we can just scale it up:

```bash
 docker-compose scale datanode=3
 ```
# Running S3 Clients

Once the cluster is booted up and ready, you can verify it is running by
connecting to the SCM's UI at [http://localhost:9876](http://localhost:9876).

The S3 gateway endpoint will be exposed at port 9878. You can use Ozone's S3
support as if you are working against the real S3.


Here is how you create buckets from command line:

```bash
aws s3api --endpoint http://localhost:9878/ create-bucket --bucket=bucket1
```

Only notable difference in the above command line is the fact that you have
to tell the _endpoint_ address to the aws s3api command.

Now let us put a simple file into the S3 Bucket hosted by Ozone. We will
start by creating a temporary file that we can upload to Ozone via S3 support.
```bash
ls -1 > /tmp/testfile
 ```
 This command creates a temporary file that
 we can upload to Ozone. The next command actually uploads to Ozone's S3
 bucket using the standard aws s3 command line interface.

```bash
aws s3 --endpoint http://localhost:9878 cp --storage-class REDUCED_REDUNDANCY  /tmp/testfile  s3://bucket1/testfile
```
<div class="alert alert-info" role="alert">
Note: REDUCED_REDUNDANCY is required for the single container ozone, since it
 has a single datanode. </div>
We can now verify that file got uploaded by running the list command against
our bucket.

```bash
aws s3 --endpoint http://localhost:9878 ls s3://bucket1/testfile
```

.
<div class="alert alert-info" role="alert"> You can also check the internal
bucket browser supported by Ozone S3 interface by clicking on the below link.
<br>
</div>
http://localhost:9878/bucket1?browser
