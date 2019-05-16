<!--
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
# Tutorial: Running a standalone Cifar10 PyTorch Estimator Example.

Currently, PyTorch integration with Submarine only supports PyTorch in standalone (non-distributed mode).
Please also note that HDFS as a data source is not yet supported by PyTorch.

## What is CIFAR-10?
CIFAR-10 is a common benchmark in machine learning for image recognition. Below example is based on CIFAR-10 dataset.

**Warning:**

Please note that YARN service doesn't allow multiple services with the same name, so please run following command
```
yarn application -destroy <service-name>
```
to delete services if you want to reuse the same service name.

## Prepare Docker images

Refer to [Write Dockerfile](WriteDockerfilePT.html) to build a Docker image or use prebuilt one.

## Running PyTorch jobs

### Run standalone training

```
export HADOOP_CLASSPATH="/home/systest/hadoop-submarine-score-yarnservice-runtime-0.2.0-SNAPSHOT.jar:/home/systest/hadoop-submarine-core-0.2.0-SNAPSHOT.jar"
/opt/hadoop/bin/yarn jar /home/systest/hadoop-submarine-core-0.2.0-SNAPSHOT.jar job run \
--name pytorch-job-001 \
--verbose \
--framework pytorch \
--wait_job_finish \
--docker_image pytorch-latest-gpu:0.0.1 \
--input_path hdfs://unused \
--env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre \
--env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.2 \
--env YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL=true \
--num_workers 1 \
--worker_resources memory=5G,vcores=2 \
--worker_launch_cmd "cd /test/ && python cifar10_tutorial.py"

```

For the meaning of the individual parameters, see the [QuickStart](QuickStart.html) page!

**Remarks:**
Please note that the input path parameter is mandatory, but not yet used by the PyTorch docker container.