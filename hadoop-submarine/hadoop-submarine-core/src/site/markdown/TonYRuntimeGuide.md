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

- Apache Hadoop 2.7 or above.
- TonY library 0.3.2 or above. You could download latest TonY jar from
https://github.com/linkedin/TonY/releases.

Optional:

- Enable GPU on YARN support (when GPU-based training is required, Hadoop 3.1 and above).
- Enable Docker support on Hadoop (Hadoop 2.9 and above).

## Run jobs

### Commandline options

```$xslt
usage:
 -docker_image <arg>          Docker image name/tag
 -env <arg>                   Common environment variable of worker/ps
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
                              Argument format is "RemoteUri:LocalFileName"
                              The LocalFilePath is the local file or folder name.
                              You should access it with relative path to working directory.
                              This option can be set mutiple times.
                              Examples are
                              -localization "hdfs:///user/yarn/mydir2:data"
                              -localization "s3a:///a/b/myfile1:file1"
                              -localization "https:///a/b/myfile2:myfile"
                              -localization "/user/yarn/mydir3:mydir3"
                              -localization "./mydir1:mydir1"
 -insecure                    Whether running in an insecure cluster
 -conf                        Override configurations via commandline
```

> Note: all --localization files will be localized to working directory. You should access them use
relative path. Alternatively, you could use `--conf tony.containers.resources
=src_file::dest_file_name,src_file2::dest_file_name2`. It accepts a list of resources to be localized to all containers,
delimited by comma. If a resource has no scheme like `hdfs://` or `s3://`, the file is considered a local file. You
could add #archive annotation, if an entry has `#archive`, the file will be automatically unzipped when localized to the
containers, folder name is the same as the file name. For example: `/user/khu/abc.zip#archive` would be inferred as a
local file and will be unarchived in containers. You would anticipate an abc.zip/ folder in your container's working
directory. Annotation `::` is added since TonY 0.3.3. If you use `PATH/TO/abc.txt::def.txt`, the `abc.txt` file
would be localized as `def.txt` in the container working directory.
Details: [tony configurations](https://github.com/linkedin/TonY/wiki/TonY-Configurations)

### Submarine Configuration

For submarine internal configuration, please create a `submarine.xml` which should be placed under `$HADOOP_CONF_DIR`.
Make sure you set `submarine.runtime.class` to `org.apache.hadoop.yarn.submarine.runtimes.tony.TonyRuntimeFactory`

|Configuration Name | Description |
|:---- |:---- |
| `submarine.runtime.class` | org.apache.hadoop.yarn.submarine.runtimes.tony.TonyRuntimeFactory
| `submarine.localization.max-allowed-file-size-mb` | Optional. This sets a size limit to the file/directory to be localized in "-localization" CLI option. 2GB by default. |



### Launch TensorFlow Application:

#### Commandline

### Without Docker

You need:
* Build a Python virtual environment with TensorFlow 1.13.1 installed
* A cluster with Hadoop 2.7 or above.

### Building a Python virtual environment with TensorFlow

TonY requires a Python virtual environment zip with TensorFlow and any needed Python libraries already installed.

```
wget https://files.pythonhosted.org/packages/33/bc/fa0b5347139cd9564f0d44ebd2b147ac97c36b2403943dbee8a25fd74012/virtualenv-16.0.0.tar.gz
tar xf virtualenv-16.0.0.tar.gz

# Make sure to install using Python 3, as TensorFlow only provides Python 3 artifacts
python virtualenv-16.0.0/virtualenv.py venv
. venv/bin/activate
pip install tensorflow==1.13.1
zip -r venv.zip venv
```

### TensorFlow version

 - Version 1.13.1

**Note:** If you require a past version of TensorFlow and TensorBoard, take a look at [this](https://github.com/linkedin/TonY/issues/42) issue.


### Installing Hadoop

TonY only requires YARN, not HDFS. Please see the [open-source documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) on how to set YARN up.

### Get the training examples

Get mnist_distributed.py from https://github.com/linkedin/TonY/tree/master/tony-examples/mnist-tensorflow


```
CLASSPATH=$(hadoop classpath --glob): \
./hadoop-submarine-core/target/hadoop-submarine-core-0.2.0-SNAPSHOT.jar: \
./hadoop-submarine-yarnservice-runtime/target/hadoop-submarine-score-yarnservice-runtime-0.2.0-SNAPSHOT.jar: \
./hadoop-submarine-tony-runtime/target/hadoop-submarine-tony-runtime-0.2.0-SNAPSHOT.jar: \
/home/pi/hadoop/TonY/tony-cli/build/libs/tony-cli-0.3.11-all.jar \

java org.apache.hadoop.yarn.submarine.client.cli.Cli job run --name tf-job-001 \
 --framework tensorflow \
 --num_workers 2 \
 --worker_resources memory=3G,vcores=2 \
 --num_ps 2 \
 --ps_resources memory=3G,vcores=2 \
 --worker_launch_cmd "venv.zip/venv/bin/python mnist_distributed.py --steps 1000 --data_dir /tmp/data --working_dir /tmp/mode" \
 --ps_launch_cmd "venv.zip/venv/bin/python mnist_distributed.py --steps 1000 --data_dir /tmp/data --working_dir /tmp/mode" \
 --insecure
 --conf tony.containers.resources=PATH_TO_VENV_YOU_CREATED/venv.zip#archive,PATH_TO_MNIST_EXAMPLE/mnist_distributed.py, \
PATH_TO_TONY_CLI_JAR/tony-cli-0.3.11-all.jar

```

You should then be able to see links and status of the jobs from command line:

```
2019-04-22 20:30:42,611 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: worker index: 0 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000003/pi status: RUNNING
2019-04-22 20:30:42,612 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: worker index: 1 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000004/pi status: RUNNING
2019-04-22 20:30:42,612 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: ps index: 0 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000002/pi status: RUNNING
2019-04-22 20:30:42,612 INFO tony.TonyClient: Logs for ps 0 at: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000002/pi
2019-04-22 20:30:42,612 INFO tony.TonyClient: Logs for worker 0 at: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000003/pi
2019-04-22 20:30:42,612 INFO tony.TonyClient: Logs for worker 1 at: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000004/pi
2019-04-22 20:30:44,625 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: ps index: 0 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000002/pi status: FINISHED
2019-04-22 20:30:44,625 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: worker index: 0 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000003/pi status: FINISHED
2019-04-22 20:30:44,626 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: worker index: 1 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000004/pi status: FINISHED

```

### With Docker

```
CLASSPATH=$(hadoop classpath --glob): \
./hadoop-submarine-core/target/hadoop-submarine-core-0.2.0-SNAPSHOT.jar: \
./hadoop-submarine-yarnservice-runtime/target/hadoop-submarine-score-yarnservice-runtime-0.2.0-SNAPSHOT.jar: \
./hadoop-submarine-tony-runtime/target/hadoop-submarine-tony-runtime-0.2.0-SNAPSHOT.jar: \
/home/pi/hadoop/TonY/tony-cli/build/libs/tony-cli-0.3.11-all.jar \

java org.apache.hadoop.yarn.submarine.client.cli.Cli job run --name tf-job-001 \
 --framework tensorflow \
 --docker_image hadoopsubmarine/tf-1.8.0-cpu:0.0.3 \
 --input_path hdfs://pi-aw:9000/dataset/cifar-10-data \
 --worker_resources memory=3G,vcores=2 \
 --worker_launch_cmd "export CLASSPATH=\$(/hadoop-3.1.0/bin/hadoop classpath --glob) && cd /test/models/tutorials/image/cifar10_estimator && python cifar10_main.py --data-dir=%input_path% --job-dir=%checkpoint_path% --train-steps=10000 --eval-batch-size=16 --train-batch-size=16 --variable-strategy=CPU --num-gpus=0 --sync" \
 --env JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 \
 --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
 --env HADOOP_HOME=/hadoop-3.1.0 \
 --env HADOOP_YARN_HOME=/hadoop-3.1.0 \
 --env HADOOP_COMMON_HOME=/hadoop-3.1.0 \
 --env HADOOP_HDFS_HOME=/hadoop-3.1.0 \
 --env HADOOP_CONF_DIR=/hadoop-3.1.0/etc/hadoop \
 --conf tony.containers.resources=/home/pi/hadoop/TonY/tony-cli/build/libs/tony-cli-0.3.11-all.jar
```


### Launch PyToch Application:

#### Commandline

### Without Docker

You need:
* Build a Python virtual environment with PyTorch 0.4.* installed
* A cluster with Hadoop 2.7 or above.

### Building a Python virtual environment with PyTorch

TonY requires a Python virtual environment zip with PyTorch and any needed Python libraries already installed.

```
wget https://files.pythonhosted.org/packages/33/bc/fa0b5347139cd9564f0d44ebd2b147ac97c36b2403943dbee8a25fd74012/virtualenv-16.0.0.tar.gz
tar xf virtualenv-16.0.0.tar.gz

python virtualenv-16.0.0/virtualenv.py venv
. venv/bin/activate
pip install pytorch==0.4.0
zip -r venv.zip venv
```

### PyTorch version

 - Version 0.4.0+


### Installing Hadoop

TonY only requires YARN, not HDFS. Please see the [open-source documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) on how to set YARN up.

### Get the training examples

Get mnist_distributed.py from https://github.com/linkedin/TonY/tree/master/tony-examples/mnist-pytorch


```
CLASSPATH=$(hadoop classpath --glob): \
./hadoop-submarine-core/target/hadoop-submarine-core-0.2.0-SNAPSHOT.jar: \
./hadoop-submarine-yarnservice-runtime/target/hadoop-submarine-score-yarnservice-runtime-0.2.0-SNAPSHOT.jar: \
./hadoop-submarine-tony-runtime/target/hadoop-submarine-tony-runtime-0.2.0-SNAPSHOT.jar: \
/home/pi/hadoop/TonY/tony-cli/build/libs/tony-cli-0.3.11-all.jar \

java org.apache.hadoop.yarn.submarine.client.cli.Cli job run --name tf-job-001 \
 --num_workers 2 \
 --worker_resources memory=3G,vcores=2 \
 --num_ps 2 \
 --ps_resources memory=3G,vcores=2 \
 --worker_launch_cmd "venv.zip/venv/bin/python mnist_distributed.py" \
 --ps_launch_cmd "venv.zip/venv/bin/python mnist_distributed.py" \
 --insecure \
 --conf tony.containers.resources=PATH_TO_VENV_YOU_CREATED/venv.zip#archive,PATH_TO_MNIST_EXAMPLE/mnist_distributed.py, \
PATH_TO_TONY_CLI_JAR/tony-cli-0.3.11-all.jar \
--conf tony.application.framework=pytorch

```
You should then be able to see links and status of the jobs from command line:

```
2019-04-22 20:30:42,611 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: worker index: 0 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000003/pi status: RUNNING
2019-04-22 20:30:42,612 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: worker index: 1 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000004/pi status: RUNNING
2019-04-22 20:30:42,612 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: ps index: 0 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000002/pi status: RUNNING
2019-04-22 20:30:42,612 INFO tony.TonyClient: Logs for ps 0 at: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000002/pi
2019-04-22 20:30:42,612 INFO tony.TonyClient: Logs for worker 0 at: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000003/pi
2019-04-22 20:30:42,612 INFO tony.TonyClient: Logs for worker 1 at: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000004/pi
2019-04-22 20:30:44,625 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: ps index: 0 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000002/pi status: FINISHED
2019-04-22 20:30:44,625 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: worker index: 0 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000003/pi status: FINISHED
2019-04-22 20:30:44,626 INFO tony.TonyClient: Tasks Status Updated: [TaskInfo] name: worker index: 1 url: http://pi-aw:8042/node/containerlogs/container_1555916523933_0030_01_000004/pi status: FINISHED

```

### With Docker

```
CLASSPATH=$(hadoop classpath --glob): \
./hadoop-submarine-core/target/hadoop-submarine-core-0.2.0-SNAPSHOT.jar: \
./hadoop-submarine-yarnservice-runtime/target/hadoop-submarine-score-yarnservice-runtime-0.2.0-SNAPSHOT.jar: \
./hadoop-submarine-tony-runtime/target/hadoop-submarine-tony-runtime-0.2.0-SNAPSHOT.jar: \
/home/pi/hadoop/TonY/tony-cli/build/libs/tony-cli-0.3.11-all.jar \

java org.apache.hadoop.yarn.submarine.client.cli.Cli job run --name tf-job-001 \
 --docker_image hadoopsubmarine/tf-1.8.0-cpu:0.0.3 \
 --input_path hdfs://pi-aw:9000/dataset/cifar-10-data \
 --worker_resources memory=3G,vcores=2 \
 --worker_launch_cmd "export CLASSPATH=\$(/hadoop-3.1.0/bin/hadoop classpath --glob) && cd /test/models/tutorials/image/cifar10_estimator && python cifar10_main.py --data-dir=%input_path% --job-dir=%checkpoint_path% --train-steps=10000 --eval-batch-size=16 --train-batch-size=16 --variable-strategy=CPU --num-gpus=0 --sync" \
 --env JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 \
 --env DOCKER_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
 --env HADOOP_HOME=/hadoop-3.1.0 \
 --env HADOOP_YARN_HOME=/hadoop-3.1.0 \
 --env HADOOP_COMMON_HOME=/hadoop-3.1.0 \
 --env HADOOP_HDFS_HOME=/hadoop-3.1.0 \
 --env HADOOP_CONF_DIR=/hadoop-3.1.0/etc/hadoop \
 --conf tony.containers.resources=PATH_TO_TONY_CLI_JAR/tony-cli-0.3.11-all.jar \
 --conf tony.application.framework=pytorch
```
