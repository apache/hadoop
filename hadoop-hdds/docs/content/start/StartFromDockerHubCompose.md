---
title: Local multi-container cluster
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
 * docker-compose
 * AWS CLI (optional)
{{< /requirements >}}

If you would like to use a more realistic pseud-cluster where each components run in own containers you can start it with a docker-compose file.

```bash
docker run apache/ozone cat docker-compose.yaml > docker-compose.yaml
docker run apache/ozone cat docker-config > docker-config
```

Now you can start the cluster with docker-compose:

```
docker-compose up -d
```

If you need multiple datanodes, just scale it up:

```bash
docker-compose scale datanode=3
```

From here, everything is similar to the all-in-one deployment:

You can check the SCM web UI:

http://localhost:9876

Or the S3 gateway endpoint:

http://localhost:9878

You can try to create buckets from command line:

```bash
aws s3api --endpoint http://localhost:9878/ create-bucket --bucket=bucket1

ls -1 > /tmp/testfile

aws s3 --endpoint http://localhost:9878 cp  /tmp/testfile  s3://bucket1/testfile

aws s3 --endpoint http://localhost:9878 ls s3://bucket1/testfile
```

And you can check the internal bucket browser:

http://localhost:9878/bucket1?browser
