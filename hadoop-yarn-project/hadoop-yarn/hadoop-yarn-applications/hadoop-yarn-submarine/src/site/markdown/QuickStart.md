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

- Apache Hadoop 3.1.x, YARN service enabled.

Optional:

- Enable YARN DNS. (When distributed training is required.)
- Enable GPU on YARN support. (When GPU-based training is required.)

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
 set `PYTHONPATH` environment variable as below to avoid module import error 
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

For submarine internal configuration, please create a `submarine.xml` which should be placed under `$HADOOP_CONF_DIR`.

|Configuration Name | Description |
|:---- |:---- |
| `submarine.runtime.class` | Optional. Full qualified class name for your runtime factory. |
| `submarine.localization.max-allowed-file-size-mb` | Optional. This sets a size limit to the file/directory to be localized in "-localization" CLI option. 2GB by default. |



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
  --worker_launch_cmd "python ... (Your training application cmd)" \
  --tensorboard # this will launch a companion tensorboard container for monitoring
```

#### Notes:

1) `DOCKER_JAVA_HOME` points to JAVA_HOME inside Docker image.

2) `DOCKER_HADOOP_HDFS_HOME` points to HADOOP_HDFS_HOME inside Docker image.

3) `--worker_resources` can include gpu when you need GPU to train your task.

4) When `--tensorboard` is specified, you can go to YARN new UI, go to services -> `<you specified service>` -> Click `...` to access Tensorboard.

This will launch a Tensorboard to monitor *all your jobs*. By access YARN UI (the new UI). You can go to services page, go to the `tensorboard-service`, click quick links (`Tensorboard`) can lead you to the tensorboard.

See below screenshot:

![alt text](./images/tensorboard-service.png "Tensorboard service")

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

## Get job history / logs

### Get Job Status from CLI

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

### Run tensorboard to monitor your jobs

```
# Cleanup previous service if needed
yarn app -destroy tensorboard-service; \
yarn jar /tmp/hadoop-yarn-applications-submarine-3.2.0-SNAPSHOT.jar \
  job run --name tensorboard-service --verbose --docker_image wtan/tf-1.8.0-cpu:0.0.3 \
  --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ \
  --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 \
  --num_workers 0 --tensorboard
```

You can view multiple job training history like from the `Tensorboard` link:

![alt text](./images/multiple-tensorboard-jobs.png "Tensorboard for multiple jobs")


### Get component logs from a training job

There're two ways to get training job logs, one is from YARN UI (new or old):

![alt text](./images/job-logs-ui.png "Job logs UI")

Or you can use `yarn logs -applicationId <applicationId>` to get logs from CLI
