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

Launching Applications Using runC Containers
==============================================

<!-- MACRO{toc|fromDepth=0|toDepth=1} -->

Security Warning
---------------
**IMPORTANT**

This feature is UNSTABLE. As this feature continues to evolve, APIs may not
be maintained and functionality may be changed or removed.

Enabling this feature and running runC containers in your
cluster has security implications. Given runC's integration with many powerful
kernel features, it is imperative that administrators understand
runC security before enabling this feature.

Overview
--------

[runC](https://github.com/opencontainers/runc) is a CLI tool for spawning and
running containers according to the Open Container Initiative (OCI)
specification. runC was originally [spun out](https://www.docker.com/blog/runc/)
of the original Docker infrastructure. Together with a rootfs mountpoint that
is created via squashFS images, runC enables users to bundle an application
together with its preferred execution environment to be executed on a
target machine. For more information about the OCI, see their
[website](https://www.opencontainers.org/).

The Linux Container Executor (LCE) allows the YARN NodeManager to launch YARN
containers to run either directly on the host machine, inside of Docker
containers, and now inside of runC containers.
The application requesting the resources can specify for each
container how it should be executed. The LCE also provides enhanced security
and is required when deploying a secure cluster. When the LCE launches a YARN
container to execute in a runC container, the application can specify the
runC image to be used. These runC images can be built from Docker images.

runC containers provide a custom execution environment in which the
application's code runs, isolated from the execution environment of the
NodeManager and other applications. These containers can include special
libraries needed by the application, and they can have different versions of
native tools and libraries including Perl, Python, and Java. runC
containers can even run a different flavor of Linux than what is running on the
NodeManager.

runC for YARN provides both consistency (all YARN containers will have the
same software environment) and isolation (no interference with whatever is
installed on the physical machine).

runC support in the LCE is still evolving. To track progress and take a look
at the runC design document, check out
[YARN-9014](https://issues.apache.org/jira/browse/YARN-9014), the umbrella JIRA
for runC support improvements.

Cluster Configuration
---------------------

The LCE requires that container-executor binary be owned by root:hadoop and have
6050 permissions. In order to launch runC containers, runC must be installed
on all NodeManager hosts where runC containers will be launched.

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
    <value>default,runc</value>
    <description>
      Comma separated list of runtimes that are allowed when using
      LinuxContainerExecutor. The allowed values are default, docker, runc, and
      javasandbox.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.type</name>
    <value></value>
    <description>
      Optional. Sets the default container runtime to use.
    </description>
  </property>

  <property>
    <description>The runC image tag to manifest plugin
      class to be used.</description>
    <name>yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageTagToManifestPlugin</value>
  </property>

  <property>
    <description>The runC manifest to resources plugin class to
      be used.</description>
    <name>yarn.nodemanager.runtime.linux.runc.manifest-to-resources-plugin</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.HdfsManifestToResourcesPlugin</value>
  </property>

  <property>
    <description>The HDFS location under which the oci image manifests, layers,
      and configs directories exist.</description>
    <name>yarn.nodemanager.runtime.linux.runc.image-toplevel-dir</name>
    <value>/runc-root</value>
  </property>

  <property>
    <description>Target count of layer mounts that we should keep on disk
      at one time.</description>
    <name>yarn.nodemanager.runtime.linux.runc.layer-mounts-to-keep</name>
    <value>100</value>
  </property>

  <property>
    <description>The interval in seconds between executions of
      reaping layer mounts.</description>
    <name>yarn.nodemanager.runtime.linux.runc.layer-mounts-interval-secs</name>
    <value>600</value>
  </property>

  <property>
    <description>Image to be used if no other image is specified.</description>
    <name>yarn.nodemanager.runtime.linux.runc.image-name</name>
    <value></value>
  </property>

  <property>
    <description>Allow or disallow privileged containers.</description>
    <name>yarn.nodemanager.runtime.linux.runc.privileged-containers.allowed</name>
    <value>false</value>
  </property>

  <property>
    <description>The set of networks allowed when launching containers
      using the RuncContainerRuntime.</description>
    <name>yarn.nodemanager.runtime.linux.runc.allowed-container-networks</name>
    <value>host,none,bridge</value>
  </property>

  <property>
    <description>The set of runtimes allowed when launching containers
      using the RuncContainerRuntime.</description>
    <name>yarn.nodemanager.runtime.linux.runc.allowed-container-runtimes</name>
    <value>runc</value>
  </property>

  <property>
    <description>ACL list for users allowed to run privileged
      containers.</description>
    <name>yarn.nodemanager.runtime.linux.runc.privileged-containers.acl</name>
    <value></value>
  </property>

  <property>
    <description>Allow host pid namespace for runC containers.
      Use with care.</description>
    <name>yarn.nodemanager.runtime.linux.runc.host-pid-namespace.allowed</name>
    <value>false</value>
  </property>

  <property>
    <description>The default list of read-only mounts to be bind-mounted
      into all runC containers that use RuncContainerRuntime.</description>
    <name>yarn.nodemanager.runtime.linux.runc.default-ro-mounts</name>
    <value></value>
  </property>

  <property>
    <description>The default list of read-write mounts to be bind-mounted
      into all runC containers that use RuncContainerRuntime.</description>
    <name>yarn.nodemanager.runtime.linux.runc.default-rw-mounts</name>
    <value></value>
  </property>

  <property>
    <description>Path to the seccomp profile to use with runC
      containers</description>
    <name>yarn.nodemanager.runtime.linux.runc.seccomp-profile</name>
    <value></value>
  </property>

  <property>
    <description>The HDFS location where the runC image tag to hash
      file exists.</description>
    <name>yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin.hdfs-hash-file</name>
    <value>/runc-root/image-tag-to-hash</value>
  </property>

  <property>
    <description>The local file system location where the runC image tag
      to hash file exists.</description>
    <name>yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin.local-hash-file</name>
    <value></value>
  </property>

  <property>
    <description>The interval in seconds between refreshing the hdfs image tag
      to hash cache.</description>
    <name>yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin.cache-refresh-interval-secs</name>
    <value>60</value>
  </property>

  <property>
    <description>The number of manifests to cache in the image tag
      to hash cache.</description>
    <name>yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin.num-manifests-to-cache</name>
    <value>10</value>
  </property>

  <property>
    <description>The timeout value in seconds for the values in
      the stat cache.</description>
    <name>yarn.nodemanager.runtime.linux.runc.hdfs-manifest-to-resources-plugin.stat-cache-timeout-interval-secs</name>
    <value>360</value>
  </property>

  <property>
    <description>The size of the stat cache which stores stats of the
      layers and config.</description>
    <name>yarn.nodemanager.runtime.linux.runc.hdfs-manifest-to-resources-plugin.stat-cache-size</name>
    <value>500</value>
  </property>
</configuration>
```

In addition, a container-executor.cfg file must exist and contain settings for
the container executor. The file must be owned by root with permissions 0400.
The format of the file is the standard Java properties file format, for example

    `key=value`

The following properties are required to enable runC support:

|Configuration Name | Description |
|:---- |:---- |
| `yarn.nodemanager.linux-container-executor.group` | The Unix group of the NodeManager. It should match the yarn.nodemanager.linux-container-executor.group in the yarn-site.xml file. |

The container-executor.cfg must contain a section to determine the capabilities that containers
are allowed. It contains the following properties:

|Configuration Name | Description |
|:---- |:---- |
| `module.enabled` | Must be "true" or "false" to enable or disable launching runC containers respectively. Default value is 0. |
| `runc.binary` | The binary used to launch runC containers. /usr/bin/runc by default. |
| `runc.run-root` | The directory where all runtime mounts and overlay mounts will be placed. |
| `runc.allowed.ro-mounts` | Comma separated directories that containers are allowed to mount in read-only mode. By default, no directories are allowed to mounted. |
| `runc.allowed.rw-mounts` | Comma separated directories that containers are allowed to mount in read-write mode. By default, no directories are allowed to mounted. |

Please note that if you wish to run runC containers that require access to the YARN local directories, you must add them to the runc.allowed.rw-mounts list.

In addition, containers are not permitted to mount any parent of the container-executor.cfg directory in read-write mode.

The following properties are optional:

|Configuration Name | Description |
|:---- |:---- |
| `min.user.id` | The minimum UID that is allowed to launch applications. The default is no minimum |
| `banned.users` | A comma-separated list of usernames who should not be allowed to launch applications. The default setting is: yarn, mapred, hdfs, and bin. |
| `allowed.system.users` | A comma-separated list of usernames who should be allowed to launch applications even if their UIDs are below the configured minimum. If a user appears in allowed.system.users and banned.users, the user will be considered banned. |
| `feature.tc.enabled` | Must be "true" or "false". "false" means traffic control commands are disabled. "true" means traffic control commands are allowed. |
| `feature.yarn.sysfs.enabled` | Must be "true" or "false". See YARN sysfs support for detail. The default setting is disabled. |

Part of a container-executor.cfg which allows runC containers to be launched is below:

```
yarn.nodemanager.linux-container-executor.group=yarn
[runc]
  module.enabled=true
  runc.binary=/usr/bin/runc
  runc.run-root=/run/yarn-container-executor
  runc.allowed.ro-mounts=/sys/fs/cgroup
  runc.allowed.rw-mounts=/var/hadoop/yarn/local-dir,/var/hadoop/yarn/log-dir

```

Image Requirements
-------------------------

runC containers are run inside of images that are derived from Docker images.
The docker images are transformed into a set of squashFS file images and
uploaded into HDFS.
In order to work with YARN, there are a few requirements for these
Docker images.

1. The runC container will be explicitly launched with the application
owner as the container user.  If the application owner is not a valid user
in the Docker image, the application will fail. The container user is specified
by the user's UID. If the user's UID is different between the NodeManager host
and the Docker image, the container may be launched as the wrong user or may
fail to launch because the UID does not exist.  See
[User Management in runC Container](#user-management) section for more details.

2. The Docker image must have whatever is expected by the application
in order to execute.  In the case of Hadoop (MapReduce or Spark), the Docker
image must contain the JRE and Hadoop libraries and have the necessary
environment variables set: JAVA_HOME, HADOOP_COMMON_PATH, HADOOP_HDFS_HOME,
HADOOP_MAPRED_HOME, HADOOP_YARN_HOME, and HADOOP_CONF_DIR. Note that the
Java and Hadoop component versions available in the Docker image must be
compatible with what's installed on the cluster and in any other Docker images
being used for other tasks of the same job. Otherwise the Hadoop components
started in the runC container may be unable to communicate with external
Hadoop components.

3. `/bin/bash` must be available inside the image. This is generally true,
however, tiny Docker images (eg. ones which use busybox for shell commands)
might not have bash installed. In this case, the following error is
displayed:

    ```
    Container id: container_1561638268473_0015_01_000002
    Exit code: 7
    Exception message: Launch container failed
    Shell error output: /usr/bin/docker-current: Error response from daemon: oci runtime error: container_linux.go:235: starting container process caused "exec: \"bash\": executable file not found in $PATH".
    Shell output: main : command provided 4
    ```

4. `find` command must also be available inside the image. Not having
`find` causes this error:

    ```
    Container exited with a non-zero exit code 127. Error file: prelaunch.err.
    Last 4096 bytes of prelaunch.err :
    /tmp/hadoop-systest/nm-local-dir/usercache/hadoopuser/appcache/application_1561638268473_0017/container_1561638268473_0017_01_000002/launch_container.sh: line 44: find: command not found
    ```

If a Docker image has an entry point set, the entry point will be executed
with the launch command of the container as its arguments.

The runC images that are derived from Docker images are localized onto the
hosts where the runC containers will execute just like any other localized
resource would be. Both MapReduce and Spark assume that
tasks which take more that 10 minutes to report progress have stalled, so
specifying a large image may cause the application to fail if the localization
takes too long.

<a href="#docker-to-squash"></a>Transforming a Docker Image into a runC Image
-----------------------------------

Every Docker image is comprised of 3 things:
  - A set of layers that create the file system.
  - A config file that holds information relative to the environment of the
  image.
  - A manifest that describes what layers and config are needed for that image.

Together, these 3 pieces combine to create an Open Container Initiative (OCI)
compliant image. runC runs on top of OCI-compliant containers, but with a small
twist. Each layer that the runC runtime uses is compressed into squashFS
file system. The squashFS layers, along with the config, and manifest are
uploaded to HDFS along with an `image-tag-to-hash mapping` file that
describes the mapping between image tags and the manifest associated with
that image. Getting this all setup is a complicated and tedious process.
There is a patch on
[YARN-9564](https://issues.apache.org/jira/browse/YARN-9564) that contains
an unofficial Python script named `docker-to-squash.py`
to help out with the conversion process. This tool will
take in a Docker image as input, convert all of its layers
into squashFS file systems, and upload the squashFS layers, config, and manifest
to HDFS underneath the runc-root. It will also create or update the
`image-tag-to-hash` mapping file. Below is an example invocation of the script
to upload an image named `centos:latest` to HDFS with the
runC image name `centos`

```
[user@foobar sbin]$ pwd
/home/user/hadoop/hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/sbin
[user@foobar sbin]$ ls
distribute-exclude.sh  hadoop-daemons.sh        refresh-namenodes.sh  start-dfs.cmd        start-yarn.sh     stop-dfs.cmd        stop-yarn.sh
docker_to_squash.py    httpfs.sh                start-all.cmd         start-dfs.sh         stop-all.cmd      stop-dfs.sh         workers.sh
FederationStateStore   kms.sh                   start-all.sh          start-secure-dns.sh  stop-all.sh       stop-secure-dns.sh  yarn-daemon.sh
hadoop-daemon.sh       mr-jobhistory-daemon.sh  start-balancer.sh     start-yarn.cmd       stop-balancer.sh  stop-yarn.cmd       yarn-daemons.sh
[user@foobar sbin]$ hadoop fs -ls /
Found 3 items
drwxrwx---   - user supergroup          0 2019-08-07 19:35 /home
drwx------   - user supergroup          0 2019-08-07 19:35 /tmp
drwx------   - user supergroup          0 2019-08-07 19:35 /user
[user@foobar sbin]$ ./docker_to_squash.py --working-dir /tmp --log=DEBUG pull-build-push-update centos:latest,centos
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'version']
DEBUG: command: ['skopeo', '-v']
DEBUG: command: ['mksquashfs', '-version']
DEBUG: args: Namespace(LOG_LEVEL='DEBUG', check_magic_file=False, force=False, func=<function pull_build_push_update at 0x7fe6974cd9b0>, hadoop_prefix='/hadoop-2.8.6-SNAPSHOT', hdfs_root='/runc-root', image_tag_to_hash='image-tag-to-hash', images_and_tags=['centos:latest,centos'], magic_file='etc/dockerfile-version', pull_format='docker', replication=1, skopeo_format='dir', sub_command='pull-build-push-update', working_dir='/tmp')
DEBUG: extra: []
DEBUG: image-tag-to-hash: image-tag-to-hash
DEBUG: LOG_LEVEL: DEBUG
DEBUG: HADOOP_BIN_DIR: /hadoop-2.8.6-SNAPSHOT/bin
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root']
ls: `/runc-root': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-mkdir', '/runc-root']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-chmod', '755', '/runc-root']
DEBUG: Setting up squashfs dirs: ['/runc-root/layers', '/runc-root/config', '/runc-root/manifests']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/layers']
ls: `/runc-root/layers': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-mkdir', '/runc-root/layers']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/layers']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-chmod', '755', '/runc-root/layers']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/config']
ls: `/runc-root/config': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-mkdir', '/runc-root/config']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/config']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-chmod', '755', '/runc-root/config']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/manifests']
ls: `/runc-root/manifests': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-mkdir', '/runc-root/manifests']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/manifests']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-chmod', '755', '/runc-root/manifests']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/image-tag-to-hash']
ls: `/runc-root/image-tag-to-hash': No such file or directory
INFO: Working on image centos:latest with tags ['centos']
DEBUG: command: ['skopeo', 'inspect', '--raw', 'docker://centos:latest']
DEBUG: skopeo inspect --raw returned a list of manifests
DEBUG: amd64 manifest sha is: sha256:ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66
DEBUG: command: ['skopeo', 'inspect', '--raw', u'docker://centos@sha256:ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66']
INFO: manifest: {u'layers': [{u'mediaType': u'application/vnd.docker.image.rootfs.diff.tar.gzip', u'digest': u'sha256:8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df', u'size': 75403831}], u'schemaVersion': 2, u'config': {u'mediaType': u'application/vnd.docker.container.image.v1+json', u'digest': u'sha256:9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1', u'size': 2182}, u'mediaType': u'application/vnd.docker.distribution.manifest.v2+json'}
INFO: manifest: {u'layers': [{u'mediaType': u'application/vnd.docker.image.rootfs.diff.tar.gzip', u'digest': u'sha256:8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df', u'size': 75403831}], u'schemaVersion': 2, u'config': {u'mediaType': u'application/vnd.docker.container.image.v1+json', u'digest': u'sha256:9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1', u'size': 2182}, u'mediaType': u'application/vnd.docker.distribution.manifest.v2+json'}
DEBUG: Layers: [u'8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df']
DEBUG: Config: 9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1
DEBUG: hash_to_tags is null. Not removing tag centos
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66']
ls: `/runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', u'/runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1']
ls: `/runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', u'/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh']
ls: `/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh': No such file or directory
DEBUG: skopeo_dir: /tmp/docker-to-squash/centos:latest
INFO: Pulling image: centos:latest
DEBUG: command: ['skopeo', 'copy', 'docker://centos:latest', 'dir:/tmp/docker-to-squash/centos:latest']
INFO: Squashifying and uploading layer: 8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', u'/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh']
ls: `/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh': No such file or directory
DEBUG: command: ['sudo', 'tar', '-C', u'/tmp/docker-to-squash/expand_archive_8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df', '--xattrs', "--xattrs-include='*'", '-xzf', u'/tmp/docker-to-squash/centos:latest/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df']
DEBUG: command: ['sudo', 'find', u'/tmp/docker-to-squash/expand_archive_8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df', '-name', '.wh.*']
DEBUG: command: ['sudo', 'mksquashfs', u'/tmp/docker-to-squash/expand_archive_8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df', u'/tmp/docker-to-squash/centos:latest/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh']
DEBUG: command: ['sudo', 'rm', '-rf', u'/tmp/docker-to-squash/expand_archive_8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', u'/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh']
ls: `/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-put', u'/tmp/docker-to-squash/centos:latest/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh', u'/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-setrep', '1', u'/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-chmod', '444', u'/runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh']
INFO: Uploaded file /runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh with replication 1 and permissions 444
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', u'/runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1']
ls: `/runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-put', u'/tmp/docker-to-squash/centos:latest/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1', u'/runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-setrep', '1', u'/runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-chmod', '444', u'/runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1']
INFO: Uploaded file /runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1 with replication 1 and permissions 444
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-ls', '/runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66']
ls: `/runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66': No such file or directory
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-put', '/tmp/docker-to-squash/centos:latest/manifest.json', '/runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-setrep', '1', '/runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-chmod', '444', '/runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66']
INFO: Uploaded file /runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66 with replication 1 and permissions 444
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-put', '-f', '/tmp/docker-to-squash/image-tag-to-hash', '/runc-root/image-tag-to-hash']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-setrep', '1', '/runc-root/image-tag-to-hash']
DEBUG: command: ['/hadoop-2.8.6-SNAPSHOT/bin/hadoop', 'fs', '-chmod', '444', '/runc-root/image-tag-to-hash']
DEBUG: command: ['sudo', 'rm', '-rf', '/tmp/docker-to-squash']
[user@foobar sbin]$ hadoop fs -ls /
Found 4 items
drwxrwx---   - user supergroup          0 2019-08-07 19:35 /home
drwxr-xr-x   - user supergroup          0 2019-08-08 22:38 /runc-root
drwx------   - user supergroup          0 2019-08-07 19:35 /tmp
drwx------   - user supergroup          0 2019-08-07 19:35 /user
[user@foobar sbin]$ hadoop fs -ls /runc-root/*
Found 1 items
-r--r--r--   1 user supergroup       2182 2019-08-08 22:38 /runc-root/config/9f38484d220fa527b1fb19747638497179500a1bed8bf0498eb788229229e6e1
-r--r--r--   1 user supergroup         86 2019-08-08 22:38 /runc-root/image-tag-to-hash
Found 1 items
-r--r--r--   1 user supergroup   73625600 2019-08-08 22:38 /runc-root/layers/8ba884070f611d31cb2c42eddb691319dc9facf5e0ec67672fcfa135181ab3df.sqsh
Found 1 items
-r--r--r--   1 user supergroup        529 2019-08-08 22:38 /runc-root/manifests/ca58fe458b8d94bc6e3072f1cfbd334855858e05e1fd633aa07cf7f82b048e66
```


Application Submission
----------------------

Before attempting to launch a runC container, make sure that the LCE
configuration is working for applications requesting regular YARN containers.
If after enabling the LCE one or more NodeManagers fail to start, the cause is
most likely that the ownership and/or permissions on the container-executor
binary are incorrect. Check the logs to confirm.

In order to run an application in a runC container, set the following
environment variables in the application's environment:

| Environment Variable Name | Description |
| :------------------------ | :---------- |
| `YARN_CONTAINER_RUNTIME_TYPE` | Determines whether an application will be launched in a runC container. If the value is "runc", the application will be launched in a runC container. Otherwise a regular process tree container will be used. |
| `YARN_CONTAINER_RUNTIME_RUNC_IMAGE` | Names which image will be used to launch the runC container. |
| `YARN_CONTAINER_RUNTIME_RUNC_CONTAINER_HOSTNAME` | Sets the hostname to be used by the runC container. |
| `YARN_CONTAINER_RUNTIME_RUNC_MOUNTS` | Adds additional volume mounts to the runC container. The value of the environment variable should be a comma-separated list of mounts. All such mounts must be given as "source:dest:mode" and the mode must be "ro" (read-only) or "rw" (read-write) to specify the type of access being requested. If neither is specified, read-write will be assumed. The requested mounts will be validated by container-executor based on the values set in container-executor.cfg for runc.allowed.ro-mounts and runc.allowed.rw-mounts. |

The first two are required. The remainder can be set as needed. While
controlling the container type through environment variables is somewhat less
than ideal, it allows applications with no awareness of YARN's runC support
(such as MapReduce and Spark) to nonetheless take advantage of it through their
support for configuring the application environment.

**Note** The runtime will not work if you mount anything onto /tmp or /var/tmp
in the container.

Once an application has been submitted to be launched in a runC container,
the application will behave exactly as any other YARN application. Logs will be
aggregated and stored in the relevant history server. The application life cycle
will be the same as for a non-runC application.

Using runC Bind Mounted Volumes
---------------------------------

**WARNING** Care should be taken when enabling this feature. Enabling access to
directories such as, but not limited to, /, /etc, /run, or /home is not
advisable and can result in containers negatively impacting the host or leaking
sensitive information. **WARNING**

Files and directories from the host are commonly needed within the runC
containers, which runC provides through mounts into the container.
Examples include localized resources, Apache Hadoop binaries, and sockets.

In order to mount anything into the container, the following must be configured.

* The administrator must define the volume whitelist in container-executor.cfg by setting `runc.allowed.ro-mounts` and `runc.allowed.rw-mounts` to the list of parent directories that are allowed to be mounted.

The administrator supplied whitelist is defined as a comma separated list of
directories that are allowed to be mounted into containers. The source directory
supplied by the user must either match or be a child of the specified
directory.

The user supplied mount list is defined as a comma separated list in the form
*source*:*destination* or *source*:*destination*:*mode*. The source is the file
or directory on the host. The destination is the path within the container
where the source will be bind mounted. The mode defines the mode the user
expects for the mount, which can be ro (read-only) or rw (read-write). If not
specified, rw is assumed. The mode may also include a bind propagation option
 (shared, rshared, slave, rslave, private, or rprivate). In that case, the
 mode should be of the form *option*, rw+*option*, or ro+*option*.

The following example outlines how to use this feature to mount the commonly
needed /sys/fs/cgroup directory into the container running on YARN.

The administrator sets runc.allowed.ro-mounts in container-executor.cfg to
"/sys/fs/cgroup". Applications can now request that "/sys/fs/cgroup" be mounted
from the host into the container in read-only mode.

The Nodemanager has the option to setup a default list of read-only or
read-write mounts to be added to the container via
`yarn.nodemanager.runtime.linux.runc.default-ro-mount`"
and `yarn.nodemanager.runtime.linux.runc.default-rw-mounts` in yarn-site.xml.
In this example, `yarn.nodemanager.runtime.linux.runc.default-ro-mounts`
would be set to `/sys/fs/cgroup:/sys/fs/cgroup`.

<a href="#user-management"></a>User Management in runC Container
-----------------------------------

YARN's runC container support launches container processes using the uid:gid
identity of the user, as defined on the NodeManager host. User and group name
mismatches between the NodeManager host and container can lead to permission
issues, failed container launches, or even security holes. Centralizing user and
group management for both hosts and containers greatly reduces these risks. When
running containerized applications on YARN, it is necessary to understand which
uid:gid pair will be used to launch the container's process.

As an example of what is meant by uid:gid pair, consider the following. By
default, in non-secure mode, YARN will launch processes as the user `nobody`
(see the table at the bottom of
[Using CGroups with YARN](./NodeManagerCgroups.html) for how the run as user is
determined in non-secure mode). On CentOS based systems, the `nobody` user's uid
is `99` and the `nobody` group is `99`. As a result, YARN will invoke runC
with uid `99` and gid `99`. If the `nobody` user does not have the uid `99` in the
container, the launch may fail or have unexpected results.

There are many ways to address user and group management. runC, by default,
will authenticate users against `/etc/passwd` (and `/etc/shadow`) within the
container. Using the default `/etc/passwd` supplied in the runC image is
unlikely to contain the appropriate user entries and will result in launch
failures. It is highly recommended to centralize user and group management.
Several approaches to user and group management are outlined below.

### Static user management

The most basic approach to managing user and groups is to modify the user and
group within the runC image. This approach is only viable in non-secure mode
where all container processes will be launched as a single known user, for
instance `nobody`. In this case, the only requirement is that the uid:gid pair
of the nobody user and group must match between the host and container. On a
CentOS based system, this means that the nobody user in the container needs the
UID `99` and the nobody group in the container needs GID `99`.

One approach to change the UID and GID is by leveraging `usermod` and
`groupmod`. The following sets the correct UID and GID for the nobody
user/group.
```
usermod -u 99 nobody
groupmod -g 99 nobody
```

This approach is not recommended beyond testing given the inflexibility to add
users.

### Bind mounting

When organizations already have automation in place to create local users on
each system, it may be appropriate to bind mount /etc/passwd and /etc/group
into the container as an alternative to modifying the container image directly.
To enable the ability to bind mount /etc/passwd and /etc/group, update
`runc.allowed.ro-mounts` in `container-executor.cfg` to include those paths.
For this to work on runC,
"yarn.nodemanager.runtime.linux.runc.default-ro-mounts"  will need to include
`/etc/passwd:/etc/passwd:ro` and `/etc/group:/etc/group:ro`.

There are several challenges with this bind mount approach that need to be
considered.

1. Any users and groups defined in the image will be overwritten by the host's users and groups
2. No users and groups can be added once the container is started, as /etc/passwd and /etc/group are immutible in the container. Do not mount these read-write as it can render the host inoperable.

This approach is not recommended beyond testing given the inflexibility to
modify running containers.

### SSSD

An alternative approach that allows for centrally managing users and groups is
SSSD. System Security Services Daemon (SSSD) provides access to different
identity and authentication providers, such as LDAP or Active Directory.

The traditional schema for Linux authentication is as follows:
```
application -> libpam -> pam_authenticate -> pam_unix.so -> /etc/passwd
```

If we use SSSD for user lookup, it becomes:
```
application -> libpam -> pam_authenticate -> pam_sss.so -> SSSD -> pam_unix.so -> /etc/passwd
```

We can bind-mount the UNIX sockets SSSD communicates over into the container.
This will allow the SSSD client side libraries to authenticate against the SSSD
running on the host. As a result, user information does not need to exist in
/etc/passwd of the docker image and will instead be serviced by SSSD.

Step by step configuration for host and container:

1. Host config

   - Install packages
     ```
     # yum -y install sssd-common sssd-proxy
     ```
   - create a PAM service for the container.
     ```
     # cat /etc/pam.d/sss_proxy
     auth required pam_unix.so
     account required pam_unix.so
     password required pam_unix.so
     session required pam_unix.so
     ```
   - create SSSD config file, /etc/sssd/sssd.conf
     Please note that the permissions must be 0600 and the file must be owned by root:root.
     ```
     # cat /etc/sssd/sssd/conf
     [sssd]
     services = nss,pam
     config_file_version = 2
     domains = proxy
     [nss]
     [pam]
     [domain/proxy]
     id_provider = proxy
     proxy_lib_name = files
     proxy_pam_target = sss_proxy
     ```
   - start sssd
     ```
     # systemctl start sssd
     ```
   - verify a user can be retrieved with sssd
     ```
     # getent passwd -s sss localuser
     ```

2. Container setup

   It's important to bind-mount the /var/lib/sss/pipes directory from the host to the container since SSSD UNIX sockets are located there.
   ```
   -v /var/lib/sss/pipes:/var/lib/sss/pipes:rw
   ```

3. Container config

   All the steps below should be executed on the container itself.

   - Install only the sss client libraries
     ```
     # yum -y install sssd-client
     ```

   - make sure sss is configured for passwd and group databases in
     ```
     /etc/nsswitch.conf
     ```

   - configure the PAM service that the application uses to call into SSSD
     ```
     # cat /etc/pam.d/system-auth
     #%PAM-1.0
     # This file is auto-generated.
     # User changes will be destroyed the next time authconfig is run.
     auth        required      pam_env.so
     auth        sufficient    pam_unix.so try_first_pass nullok
     auth        sufficient    pam_sss.so forward_pass
     auth        required      pam_deny.so

     account     required      pam_unix.so
     account     [default=bad success=ok user_unknown=ignore] pam_sss.so
     account     required      pam_permit.so

     password    requisite     pam_pwquality.so try_first_pass local_users_only retry=3 authtok_type=
     password    sufficient    pam_unix.so try_first_pass use_authtok nullok sha512 shadow
     password    sufficient    pam_sss.so use_authtok
     password    required      pam_deny.so

     session     optional      pam_keyinit.so revoke
     session     required      pam_limits.so
     -session     optional      pam_systemd.so
     session     [success=1 default=ignore] pam_succeed_if.so service in crond quiet use_uid
     session     required      pam_unix.so
     session     optional      pam_sss.so
     ```

   - Save the docker image and use the docker image as base image for your applications.

   - test the docker image launched in YARN environment.
     ```
     $ id
     uid=5000(localuser) gid=5000(localuser) groups=5000(localuser),1337(hadoop)
     ```

Example: MapReduce
------------------

This example assumes that Hadoop is installed to `/usr/local/hadoop`.

You will also need to squashify a Docker image and upload it to HDFS before
you can run with that image.
See [Transforming a Docker Image into a runC Image](#docker-to-squash)
for instructions on how to transform a Docker image into a image that
runC can use.
For this example, we will assume that you have
done with that an image named `hadoop-image`.

Additionally, `runc.allowed.ro-mounts` in `container-executor.cfg` has been
updated to include the directories: `/usr/local/hadoop,/etc/passwd,/etc/group`.

To submit the pi job to run in runC containers, run the following commands:

```
  HADOOP_HOME=/usr/local/hadoop
  YARN_EXAMPLES_JAR=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar
  MOUNTS="$HADOOP_HOME:$HADOOP_HOME:ro,/etc/passwd:/etc/passwd:ro,/etc/group:/etc/group:ro"
  IMAGE_ID="hadoop-image"

  export YARN_CONTAINER_RUNTIME_TYPE=runc
  export YARN_CONTAINER_RUNTIME_RUNC_IMAGE=$IMAGE_ID
  export YARN_CONTAINER_RUNTIME_RUNC_MOUNTS=$MOUNTS

  yarn jar $YARN_EXAMPLES_JAR pi \
    -Dmapreduce.map.env.YARN_CONTAINER_RUNTIME_TYPE=runc \
    -Dmapreduce.map.env.YARN_CONTAINER_RUNTIME_RUNC_MOUNTS=$MOUNTS \
    -Dmapreduce.map.env.YARN_CONTAINER_RUNTIME_RUNC_IMAGE=$IMAGE_ID \
    -Dmapreduce.reduce.env.YARN_CONTAINER_RUNTIME_TYPE=runc \
    -Dmapreduce.reduce.env.YARN_CONTAINER_RUNTIME_RUNC_MOUNTS=$MOUNTS \
    -Dmapreduce.reduce.env.YARN_CONTAINER_RUNTIME_RUNC_IMAGE=$IMAGE_ID \
    1 40000
```

Note that the application master, map tasks, and reduce tasks are configured
independently. In this example, we are using the `hadoop-image` image for all three.

Example: Spark
--------------

This example assumes that Hadoop is installed to `/usr/local/hadoop` and Spark
is installed to `/usr/local/spark`.

You will also need to squashify a Docker image and upload it to HDFS before
you can run with that image.
See [Transforming a Docker Image into a runC Image](#docker-to-squash)
for instructions on how to transform a Docker image into a image that
runC can use.
For this example, we will assume that you have
done with that an image named `hadoop-image`.

Additionally, `runc.allowed.ro-mounts` in `container-executor.cfg` has been
updated to include the directories: `/usr/local/hadoop,/etc/passwd,/etc/group`.

To run a Spark shell in runC containers, run the following command:

```
  HADOOP_HOME=/usr/local/hadoop
  SPARK_HOME=/usr/local/spark
  MOUNTS="$HADOOP_HOME:$HADOOP_HOME:ro,/etc/passwd:/etc/passwd:ro,/etc/group:/etc/group:ro"
  IMAGE_ID="hadoop-image"

  $SPARK_HOME/bin/spark-shell --master yarn \
    --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=runc \
    --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_RUNC_IMAGE=$IMAGE_ID \
    --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_RUNC_MOUNTS=$MOUNTS \
    --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=runc \
    --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_RUNC_IMAGE=$IMAGE_ID \
    --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_RUNC_MOUNTS=$MOUNTS
```

Note that the application master and executors are configured
independently. In this example, we are using the `hadoop-image` image for both.
