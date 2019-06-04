---
title: All-in-one container
menu:
   main:
      parent: Dockerhub
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

The easiest way to start up an all-in-one container is to use the latest docker image from dockerhub:

```bash
docker run -P 9878:9878 -P 9876:9876 apache/ozone
```

After a while the container will be started. This is one container which contains all the required metadata server (Ozone Manager, Storage Container Manager) one slave node (Datanode) and the S3 compatible REST server (S3 Gateway).

After a few seconds you can check the Web UI of the SCM:

http://localhost:9876

Or the S3 gateway endpoint:

http://localhost:9878

You can try to create buckets from command line:

```bash
aws s3api --endpoint http://localhost:9878/ create-bucket --bucket=bucket1

ls -1 > /tmp/testfile

aws s3 --endpoint http://localhost:9878 cp --storage-class REDUCED_REDUNDANCY  /tmp/testfile  s3://bucket1/testfile

aws s3 --endpoint http://localhost:9878 ls s3://bucket1/testfile
```

And you can check the internal bucket browser:

http://localhost:9878/bucket1?browser

<div class="alert alert-info" role="alert">Note: REDUCED_REDUNDANCY is required as the all-in-one container has only datanode</div>