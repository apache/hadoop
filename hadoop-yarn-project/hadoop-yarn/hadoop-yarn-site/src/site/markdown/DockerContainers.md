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

Launching Applications Using Docker Containers
==============================================

<!-- MACRO{toc|fromDepth=0|toDepth=1} -->

Overview
--------

[Docker](https://www.docker.io/) combines an easy-to-use interface to Linux
containers with easy-to-construct image files for those containers. In short,
Docker enables users to bundle an application together with its preferred
execution environment to be executed on a target machine. For more information
about Docker, see their [documentation](http://docs.docker.com).

The Linux Container Executor (LCE) allows the YARN NodeManager to launch YARN
containers to run either directly on the host machine or inside Docker
containers. The application requesting the resources can specify for each
container how it should be executed. The LCE also provides enhanced security
and is required when deploying a secure cluster. When the LCE launches a YARN
container to execute in a Docker container, the application can specify the
Docker image to be used.

Docker containers provide a custom execution environment in which the
application's code runs, isolated from the execution environment of the
NodeManager and other applications. These containers can include special
libraries needed by the application, and they can have different versions of
native tools and libraries including Perl, Python, and Java. Docker
containers can even run a different flavor of Linux than what is running on the
NodeManager.

Docker for YARN provides both consistency (all YARN containers will have the
same software environment) and isolation (no interference with whatever is
installed on the physical machine).

Docker support in the LCE is still evolving. To track progress, follow
[YARN-3611](https://issues.apache.org/jira/browse/YARN-3611), the umbrella JIRA
for Docker support improvements.

Cluster Configuration
---------------------

The LCE requires that container-executor binary be owned by root:hadoop and have
6050 permissions. In order to launch Docker containers, the Docker daemon must
be running on all NodeManager hosts where Docker containers will be launched.
The Docker client must also be installed on all NodeManager hosts where Docker
containers will be launched and able to start Docker containers.

To prevent timeouts while starting jobs, any large Docker images to be used by
an application should already be loaded in the Docker daemon's cache on the
NodeManager hosts. A simple way to load an image is by issuing a Docker pull
request. For example:

```
    sudo docker pull images/hadoop-docker:latest
```

The following properties should be set in yarn-site.xml:

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.container-executor.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value>
    <description>
      This is the container executor setting that ensures that all applications
      are started with the LinuxContainerExecutor.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.linux-container-executor.group</name>
    <value>hadoop</value>
    <description>
      The POSIX group of the NodeManager. It should match the setting in
      "container-executor.cfg". This configuration is required for validating
      the secure access of the container-executor binary.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users</name>
    <value>false</value>
    <description>
      Whether all applications should be run as the NodeManager process' owner.
      When false, applications are launched instead as the application owner.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.allowed-runtimes</name>
    <value>default,docker</value>
    <description>
      Comma separated list of runtimes that are allowed when using
      LinuxContainerExecutor. The allowed values are default, docker, and
      javasandbox.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.allowed-container-networks</name>
    <value>host,none,bridge</value>
    <description>
      Optional. A comma-separated set of networks allowed when launching
      containers. Valid values are determined by Docker networks available from
      `docker network ls`
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.default-container-network</name>
    <value>host</value>
    <description>
      The network used when launching Docker containers when no
      network is specified in the request. This network must be one of the
      (configurable) set of allowed container networks.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed</name>
    <value>false</value>
    <description>
      Optional. Whether applications are allowed to run in privileged
      containers.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.privileged-containers.acl</name>
    <value></value>
    <description>
      Optional. A comma-separated list of users who are allowed to request
      privileged contains if privileged containers are allowed.
    </description>
  </property>
</configuration>
```

In addition, a container-executer.cfg file must exist and contain settings for
the container executor. The file must be owned by root with permissions 0400.
The format of the file is the standard Java properties file format, for example

    `key=value`

The following properties are required to enable Docker support:

|Configuration Name | Description |
|:---- |:---- |
| `yarn.nodemanager.linux-container-executor.group` | The Unix group of the NodeManager. It should match the yarn.nodemanager.linux-container-executor.group in the yarn-site.xml file. |
| `feature.docker.enabled` | Must be 0 or 1. 0 means launching Docker containers is disabled. 1 means launching Docker containers is allowed. |

The following properties are optional:

|Configuration Name | Description |
|:---- |:---- |
| `min.user.id` | The minimum UID that is allowed to launch applications. The default is no minimum |
| `banned.users` | A comma-separated list of usernames who should not be allowed to launch applications. The default setting is: yarn, mapred, hdfs, and bin. |
| `allowed.system.users` | A comma-separated list of usernames who should be allowed to launch applications even if their UIDs are below the configured minimum. If a user appears in allowed.system.users and banned.users, the user will be considered banned. |
| `docker.binary` | The path to the Docker binary. The default is "docker". |
| `feature.tc.enabled` | Must be 0 or 1. 0 means traffic control commands are disabled. 1 means traffic control commands are allowed. |

Docker Image Requirements
-------------------------

In order to work with YARN, there are two requirements for Docker images.

First, the Docker container will be explicitly launched with the application
owner as the container user.  If the application owner is not a valid user
in the Docker image, the application will fail. The container user is specified
by the user's UID. If the user's UID is different between the NodeManager host
and the Docker image, the container may be launched as the wrong user or may
fail to launch because the UID does not exist.

Second, the Docker image must have whatever is expected by the application
in order to execute.  In the case of Hadoop (MapReduce or Spark), the Docker
image must contain the JRE and Hadoop libraries and have the necessary
environment variables set: JAVA_HOME, HADOOP_COMMON_PATH, HADOOP_HDFS_HOME,
HADOOP_MAPRED_HOME, HADOOP_YARN_HOME, and HADOOP_CONF_DIR. Note that the
Java and Hadoop component versions available in the Docker image must be
compatible with what's installed on the cluster and in any other Docker images
being used for other tasks of the same job. Otherwise the Hadoop components
started in the Docker container may be unable to communicate with external
Hadoop components.

If a Docker image has a
[command](https://docs.docker.com/engine/reference/builder/#cmd)
set, the behavior will depend on whether the
`YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE` is set to true. If so,
the command will be overridden when LCE launches the image with YARN's
container launch script.

If a Docker image has an
[entry point](https://docs.docker.com/engine/reference/builder/#entrypoint)
set, the entry point will be honored, but the default command may be
overridden, as just mentioned above. Unless the entry point is
something similar to `sh -c` or
`YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE` is set to true, the net
result will likely be undesirable. Because the YARN container launch script
is required to correctly launch the YARN task, use of entry points is
discouraged.

If an application requests a Docker image that has not already been loaded by
the Docker daemon on the host where it is to execute, the Docker daemon will
implicitly perform a Docker pull command. Both MapReduce and Spark assume that
tasks which take more that 10 minutes to report progress have stalled, so
specifying a large Docker image may cause the application to fail.

Application Submission
----------------------

Before attempting to launch a Docker container, make sure that the LCE
configuration is working for applications requesting regular YARN containers.
If after enabling the LCE one or more NodeManagers fail to start, the cause is
most likely that the ownership and/or permissions on the container-executer
binary are incorrect. Check the logs to confirm.

In order to run an application in a Docker container, set the following
environment variables in the application's environment:

| Environment Variable Name | Description |
| :------------------------ | :---------- |
| `YARN_CONTAINER_RUNTIME_TYPE` | Determines whether an application will be launched in a Docker container. If the value is "docker", the application will be launched in a Docker container. Otherwise a regular process tree container will be used. |
| `YARN_CONTAINER_RUNTIME_DOCKER_IMAGE` | Names which image will be used to launch the Docker container. Any image name that could be passed to the Docker client's run command may be used. The image name may include a repo prefix. |
| `YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE` | Controls whether the Docker container's default command is overridden.  When set to true, the Docker container's command will be "bash _path\_to\_launch\_script_". When unset or set to false, the Docker container's default command is used. |
| `YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK` | Sets the network type to be used by the Docker container. It must be a valid value as determined by the yarn.nodemanager.runtime.linux.docker.allowed-container-networks property. |
| `YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER` | Controls whether the Docker container is a privileged container. In order to use privileged containers, the yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed property must be set to true, and the application owner must appear in the value of the yarn.nodemanager.runtime.linux.docker.privileged-containers.acl property. If this environment variable is set to true, a privileged Docker container will be used if allowed. No other value is allowed, so the environment variable should be left unset rather than setting it to false. |
| `YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS` | Adds additional volume mounts to the Docker container. The value of the environment variable should be a comma-separated list of mounts. All such mounts must be given as "source:dest", where the source is an absolute path that is not a symlink and that points to a localized resource. Note that as of YARN-5298, localized directories are automatically mounted into the container as volumes. |

The first two are required. The remainder can be set as needed. While
controlling the container type through environment variables is somewhat less
than ideal, it allows applications with no awareness of YARN's Docker support
(such as MapReduce and Spark) to nonetheless take advantage of it through their
support for configuring the application environment.

Once an application has been submitted to be launched in a Docker container,
the application will behave exactly as any other YARN application. Logs will be
aggregated and stored in the relevant history server. The application life cycle
will be the same as for a non-Docker application.

Connecting to a Secure Docker Repository
----------------------------------------

Until YARN-5428 is complete, the Docker client command will draw its
configuration from the default location, which is $HOME/.docker/config.json on
the NodeManager host. The Docker configuration is where secure repository
credentials are stored, so use of the LCE with secure Docker repos is
discouraged until YARN-5428 is complete.

As a work-around, you may manually log the Docker daemon on every NodeManager
host into the secure repo using the Docker login command:

```
  docker login [OPTIONS] [SERVER]

  Register or log in to a Docker registry server, if no server is specified
  "https://index.docker.io/v1/" is the default.

  -e, --email=""       Email
  -p, --password=""    Password
  -u, --username=""    Username
```

Note that this approach means that all users will have access to the secure
repo.

Example: MapReduce
------------------

To submit the pi job to run in Docker containers, run the following commands:

```
    vars="YARN_CONTAINER_RUNTIME_TYPE=docker,YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=hadoop-docker"
    hadoop jar hadoop-examples.jar pi -Dyarn.app.mapreduce.am.env=$vars \
        -Dmapreduce.map.env=$vars -Dmapreduce.reduce.env=$vars 10 100
```

Note that the application master, map tasks, and reduce tasks are configured
independently. In this example, we are using the hadoop-docker image for all
three.

Example: Spark
--------------

To run a Spark shell in Docker containers, run the following command:

```
    spark-shell --master yarn --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
        --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=hadoop-docker \
        --conf spark.yarn.AppMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=hadoop-docker \
        --conf spark.yarn.AppMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker
```

Note that the application master and executors are configured
independently. In this example, we are using the hadoop-docker image for both.
