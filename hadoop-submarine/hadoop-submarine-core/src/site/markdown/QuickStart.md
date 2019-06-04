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

- Apache Hadoop version newer than 2.7.3

Optional:

- Enable YARN DNS. (Only when YARN Service runtime is required)
- Enable GPU on YARN support. (When GPU-based training is required)
- Docker images for Submarine jobs. (When docker container is required)
```
  # Get prebuilt docker images (No liability)
  docker pull hadoopsubmarine/tf-1.13.1-gpu:0.0.1
  # Or build your own docker images
  docker build . -f Dockerfile.gpu.tf_1.13.1 -t tf-1.13.1-gpu-base:0.0.1
```
For more details, please refer to:

- [How to write Dockerfile for Submarine TensorFlow jobs](WriteDockerfileTF.html)

- [How to write Dockerfile for Submarine PyTorch jobs](WriteDockerfilePT.html)

## Submarine runtimes
After submarine 0.2.0, it supports two runtimes which are YARN native service
 runtime and Linkedin's TonY runtime. Each runtime can support both Tensorflow
 and Pytorch framework. And the user don't need to worry about the usage
 because the two runtime implements the same interface.

To use the TonY runtime, please set below value in the submarine configuration.

|Configuration Name | Description |
|:---- |:---- |
| `submarine.runtime.class` | org.apache.hadoop.yarn.submarine.runtimes.tony.TonyRuntimeFactory |

For more details of TonY runtime, please check [TonY runtime guide](TonYRuntimeGuide.html)

## Run jobs

### Commandline options

```$xslt
usage: job run

 -framework <arg>             Framework to use.
                              Valid values are: tensorflow, pytorch.
                              The default framework is Tensorflow.
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
 -localization <arg>          Specify localization to remote/local
                              file/directory available to all container(Docker).
                              Argument format is "RemoteUri:LocalFilePath[:rw]"
                              (ro permission is not supported yet).
                              The RemoteUri can be a file or directory in local
                              or HDFS or s3 or abfs or http .etc.
                              The LocalFilePath can be absolute or relative.
                              If relative, it'll be under container's implied
                              working directory.
                              This option can be set mutiple times.
                              Examples are
                              -localization "hdfs:///user/yarn/mydir2:/opt/data"
                              -localization "s3a:///a/b/myfile1:./"
                              -localization "https:///a/b/myfile2:./myfile"
                              -localization "/user/yarn/mydir3:/opt/mydir3"
                              -localization "./mydir1:."
```

#### Notes:
When using `localization` option to make a collection of dependency Python
scripts available to entry python script in the container, you may also need to
set the `PYTHONPATH` environment variable as below to avoid module import errors
reported from `entry_script.py`.

```
... job run
  # the entry point
  --localization entry_script.py:<path>/entry_script.py
  # the dependency Python scripts of the entry point
  --localization other_scripts_dir:<path>/other_scripts_dir
  # the PYTHONPATH env to make dependency available to entry script
  --env PYTHONPATH="<path>/other_scripts_dir"
  --worker_launch_cmd "python <path>/entry_script.py ..."
```

### Submarine Configuration

For Submarine internal configuration, please create a `submarine.xml` file which should be placed under `$HADOOP_CONF_DIR`.

|Configuration Name | Description |
|:---- |:---- |
| `submarine.runtime.class` | Optional. Full qualified class name for your runtime factory. |
| `submarine.localization.max-allowed-file-size-mb` | Optional. This sets a size limit to the file/directory to be localized in "-localization" CLI option. 2GB by default. |



### Launch Standalone Tensorflow Application:

#### Commandline
```
yarn jar path-to/hadoop-yarn-applications-submarine-3.2.0-SNAPSHOT.jar job run \
  --framework tensorflow \
  --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ \
  --env DOCKER_HADOOP_HDFS_HOME=/hadoop-current --name tf-job-001 \
  --docker_image <your-docker-image> \
  --input_path hdfs://default/dataset/cifar-10-data  \
  --checkpoint_path hdfs://default/tmp/cifar-10-jobdir \
  --worker_resources memory=4G,vcores=2,gpu=2 \
  --worker_launch_cmd "python ... (Your training application cmd)" \
  --tensorboard # this will launch a companion tensorboard container for monitoring
```

#### Notes:

1) `DOCKER_JAVA_HOME` points to JAVA_HOME inside Docker image.

2) `DOCKER_HADOOP_HDFS_HOME` points to HADOOP_HDFS_HOME inside Docker image.

3) `--worker_resources` can include GPU when you need GPU to train your task.

4) When `--tensorboard` is specified, you can go to YARN new UI, go to services -> `<you specified service>` -> Click `...` to access Tensorboard.

This will launch Tensorboard to monitor *all your jobs*.
By access the YARN UI (new UI), you can go to the Services page, then go to the `tensorboard-service`, click quick links (`Tensorboard`)
This will lead you to Tensorboard.

See below screenshot:

![alt text](./images/tensorboard-service.png "Tensorboard service")

After v0.2.0, if there is no hadoop client, we can also use the java command
and the uber jar, hadoop-submarine-all-*.jar, to submit the job.

```
java -cp /path-to/hadoop-conf:/path-to/hadoop-submarine-all-*.jar \
  org.apache.hadoop.yarn.submarine.client.cli.Cli job run \
  --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ \
  --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 --name tf-job-001 \
  --docker_image <your-docker-image> \
  --input_path hdfs://default/dataset/cifar-10-data  \
  --checkpoint_path hdfs://default/tmp/cifar-10-jobdir \
  --worker_resources memory=4G,vcores=2,gpu=2  \
  --worker_launch_cmd "python ... (Your training application cmd)" \
  --tensorboard # this will launch a companion tensorboard container for monitoring
```


### Launch Distributed Tensorflow Application:

#### Commandline

```
yarn jar hadoop-yarn-applications-submarine-<version>.jar job run \
 --name tf-job-001 --docker_image <your-docker-image> \
 --framework tensorflow \
 --input_path hdfs://default/dataset/cifar-10-data \
 --checkpoint_path hdfs://default/tmp/cifar-10-jobdir \
 --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-current \
 --num_workers 2 \
 --worker_resources memory=8G,vcores=2,gpu=1 --worker_launch_cmd "cmd for worker ..." \
 --num_ps 2 \
 --ps_resources memory=4G,vcores=2,gpu=0 --ps_launch_cmd "cmd for ps" \
```
Or
```
java -cp /path-to/hadoop-conf:/path-to/hadoop-submarine-all-*.jar \
 org.apache.hadoop.yarn.submarine.client.cli.Cli job run \
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

1) Very similar to standalone TF application, but you need to specify number of workers / PS processes.

2) Different resources can be specified for worker and PS.

3) `TF_CONFIG` environment will be auto generated and set before executing user's launch command.

## Get job history / logs

### Get Job Status from CLI

```
yarn jar hadoop-yarn-applications-submarine-3.2.0-SNAPSHOT.jar job show --name tf-job-001
```
Or
```
java -cp /path-to/hadoop-conf:/path-to/hadoop-submarine-all-*.jar \
 org.apache.hadoop.yarn.submarine.client.cli.Cli job show --name tf-job-001
```
Output looks like:
```
Job Meta Info:
  Application Id: application_1532131617202_0005
  Input Path: hdfs://default/dataset/cifar-10-data
  Checkpoint Path: hdfs://default/tmp/cifar-10-jobdir
  Run Parameters: --name tf-job-001 --docker_image <your-docker-image>
                  (... all your commandline before run the job)
```

After that, you can run ```tensorboard --logdir=<checkpoint-path>``` to view Tensorboard of the job.

### Run tensorboard to monitor your jobs

```
# Cleanup previous service if needed
yarn app -destroy tensorboard-service; \
yarn jar /tmp/hadoop-yarn-applications-submarine-3.2.0-SNAPSHOT.jar \
  job run --name tensorboard-service --verbose --docker_image <your-docker-image> \
  --framework tensorflow \
  --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ \
  --env DOCKER_HADOOP_HDFS_HOME=/hadoop-current \
  --num_workers 0 --tensorboard
```
Or
```
# Cleanup previous service if needed
yarn app -destroy tensorboard-service; \
java -cp /path-to/hadoop-conf:/path-to/hadoop-submarine-all-*.jar \
  org.apache.hadoop.yarn.submarine.client.cli.Cli job run \
  --name tensorboard-service --verbose --docker_image wtan/tf-1.8.0-cpu:0.0.3 \
  --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ \
  --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 \
  --num_workers 0 --tensorboard
```

You can view multiple job training history from the `Tensorboard` link:

![alt text](./images/multiple-tensorboard-jobs.png "Tensorboard for multiple jobs")


### Get component logs from a training job

There are two ways to get the logs of a training job.
First, from YARN UI (new or old):

![alt text](./images/job-logs-ui.png "Job logs UI")

Alternatively, you can use `yarn logs -applicationId <applicationId>` to get logs from CLI.

## Build from source code

If you want to build the Submarine project by yourself, you should follow these steps:

- Run 'mvn install -DskipTests' from Hadoop source top level once.

- Navigate to hadoop-submarine folder and run 'mvn clean package'.

    - By Default, hadoop-submarine is built based on hadoop 3.1.2 dependencies.
      Both yarn service runtime and tony runtime are built.
      You can also add a parameter of "-Phadoop-3.2" to specify the dependencies
      to hadoop 3.2.0.

    - Hadoop-submarine can support hadoop 2.9.2 and hadoop 2.7.4 as well.
      You can add "-Phadoop-2.9" to build submarine based on hadoop 2.9.2.
      For example:
      ```
      mvn clean package -Phadoop-2.9
      ```
      As yarn service is based on hadoop 3.*, so only tony runtime is built
      in this case.
