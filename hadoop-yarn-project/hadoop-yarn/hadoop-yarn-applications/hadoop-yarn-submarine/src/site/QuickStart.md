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

# Quick Start Guide

## Prerequisite

Must:
- Apache Hadoop 3.1.0, YARN service enabled.

Optional:
- Enable YARN DNS. (When distributed training required.)
- Enable GPU on YARN support. (When GPU-based training required.)

## Run jobs

### Commandline options

```$xslt
usage: job run
 -checkpoint_path <arg>       Training output directory of the job, could
                              be local or other FS directory. This
                              typically includes checkpoint files and
                              exported model
 -docker_image <arg>          Docker image name/tag
 -env <arg>                   Common environment variable of worker/ps
 -input_path <arg>            Input of the job, could be local or other FS
                              directory
 -name <arg>                  Name of the job
 -num_ps <arg>                Number of PS tasks of the job, by default
                              it's 0
 -num_workers <arg>           Numnber of worker tasks of the job, by
                              default it's 1
 -ps_docker_image <arg>       Specify docker image for PS, when this is
                              not specified, PS uses --docker_image as
                              default.
 -ps_launch_cmd <arg>         Commandline of worker, arguments will be
                              directly used to launch the PS
 -ps_resources <arg>          Resource of each PS, for example
                              memory-mb=2048,vcores=2,yarn.io/gpu=2
 -queue <arg>                 Name of queue to run the job, by default it
                              uses default queue
 -saved_model_path <arg>      Model exported path (savedmodel) of the job,
                              which is needed when exported model is not
                              placed under ${checkpoint_path}could be
                              local or other FS directory. This will be
                              used to serve.
 -tensorboard <arg>           Should we run TensorBoard for this job? By
                              default it's true
 -verbose                     Print verbose log for troubleshooting
 -wait_job_finish             Specified when user want to wait the job
                              finish
 -worker_docker_image <arg>   Specify docker image for WORKER, when this
                              is not specified, WORKER uses --docker_image
                              as default.
 -worker_launch_cmd <arg>     Commandline of worker, arguments will be
                              directly used to launch the worker
 -worker_resources <arg>      Resource of each worker, for example
                              memory-mb=2048,vcores=2,yarn.io/gpu=2
```

### Launch Standalone Tensorflow Application:

#### Commandline
```
yarn jar path-to/hadoop-yarn-applications-submarine-3.2.0-SNAPSHOT.jar job run \
  --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ \
  --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 --name tf-job-001 \
  --docker_image <your-docker-image> \
  --input_path hdfs://default/dataset/cifar-10-data  \
  --checkpoint_path hdfs://default/tmp/cifar-10-jobdir \
  --worker_resources memory=4G,vcores=2,gpu=2  \
  --worker_launch_cmd "python ... (Your training application cmd)"
```

#### Notes:

1) `DOCKER_JAVA_HOME` points to JAVA_HOME inside Docker image.
2) `DOCKER_HADOOP_HDFS_HOME` points to HADOOP_HDFS_HOME inside Docker image.
3) `--worker_resources` can include gpu when you need GPU to train your task.

### Launch Distributed Tensorflow Application:

#### Commandline

```
yarn jar hadoop-yarn-applications-submarine-<version>.jar job run \
 --name tf-job-001 --docker_image <your docker image> \
 --input_path hdfs://default/dataset/cifar-10-data \
 --checkpoint_path hdfs://default/tmp/cifar-10-jobdir \
 --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 \
 --num_workers 2 \
 --worker_resources memory=8G,vcores=2,gpu=1 --worker_launch_cmd "cmd for worker ..." \
 --num_ps 2 \
 --ps_resources memory=4G,vcores=2,gpu=0 --ps_launch_cmd "cmd for ps" \
```

#### Notes:

1) Very similar to standalone TF application, but you need to specify #worker/#ps
2) Different resources can be specified for worker and PS.
3) `TF_CONFIG` environment will be auto generated and set before executing user's launch command.

## Run jobs

### Get Job Status

```
yarn jar hadoop-yarn-applications-submarine-3.2.0-SNAPSHOT.jar job show --name tf-job-001
```

Output looks like:
```
Job Meta Info:
	Application Id: application_1532131617202_0005
	Input Path: hdfs://default/dataset/cifar-10-data
	Checkpoint Path: hdfs://default/tmp/cifar-10-jobdir
	Run Parameters: --name tf-job-001 --docker_image wtan/tf-1.8.0-gpu:0.0.3
	                (... all your commandline before run the job)
```

After that, you can run ```tensorboard --logdir=<checkpoint-path>``` to view Tensorboard of the job.