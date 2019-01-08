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

Security Warning
---------------
**IMPORTANT** Enabling this feature and running Docker containers in your
cluster has security implications. Given Docker's integration with many powerful
kernel features, it is imperative that administrators understand
[Docker security](https://docs.docker.com/engine/security/security/) before
enabling this feature.

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
    sudo docker pull library/openjdk:8
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
    <name>yarn.nodemanager.runtime.linux.type</name>
    <value></value>
    <description>
      Optional. Sets the default container runtime to use.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.image-name</name>
    <value></value>
    <description>
      Optional. Default docker image to be used when the docker runtime is
      selected.
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
    <name>yarn.nodemanager.runtime.linux.docker.host-pid-namespace.allowed</name>
    <value>false</value>
    <description>
      Optional. Whether containers are allowed to use the host PID namespace.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed</name>
    <value>false</value>
    <description>
      Optional. Whether applications are allowed to run in privileged
      containers. Privileged containers are granted the complete set of
      capabilities and are not subject to the limitations imposed by the device
      cgroup controller. In other words, privileged containers can do almost
      everything that the host can do. Use with extreme care.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.delayed-removal.allowed</name>
    <value>false</value>
    <description>
      Optional. Whether or not users are allowed to request that Docker
      containers honor the debug deletion delay. This is useful for
      troubleshooting Docker container related launch failures.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.stop.grace-period</name>
    <value>10</value>
    <description>
      Optional. A configurable value to pass to the Docker Stop command. This
      value defines the number of seconds between the docker stop command sending
      a SIGTERM and a SIGKILL.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.privileged-containers.acl</name>
    <value></value>
    <description>
      Optional. A comma-separated list of users who are allowed to request
      privileged containers if privileged containers are allowed.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.capabilities</name>
    <value>CHOWN,DAC_OVERRIDE,FSETID,FOWNER,MKNOD,NET_RAW,SETGID,SETUID,SETFCAP,SETPCAP,NET_BIND_SERVICE,SYS_CHROOT,KILL,AUDIT_WRITE</value>
    <description>
      Optional. This configuration setting determines the capabilities
      assigned to docker containers when they are launched. While these may not
      be case-sensitive from a docker perspective, it is best to keep these
      uppercase. To run without any capabilites, set this value to
      "none" or "NONE"
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.enable-userremapping.allowed</name>
    <value>true</value>
    <description>
      Optional. Whether docker containers are run with the UID and GID of the
      calling user.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.userremapping-uid-threshold</name>
    <value>1</value>
    <description>
      Optional. The minimum acceptable UID for a remapped user. Users with UIDs
      lower than this value will not be allowed to launch containers when user
      remapping is enabled.
    </description>
  </property>

  <property>
    <name>yarn.nodemanager.runtime.linux.docker.userremapping-gid-threshold</name>
    <value>1</value>
    <description>
      Optional. The minimum acceptable GID for a remapped user. Users belonging
      to any group with a GID lower than this value will not be allowed to
      launch containers when user remapping is enabled.
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

The container-executor.cfg must contain a section to determine the capabilities that containers
are allowed. It contains the following properties:

|Configuration Name | Description |
|:---- |:---- |
| `module.enabled` | Must be "true" or "false" to enable or disable launching Docker containers respectively. Default value is 0. |
| `docker.binary` | The binary used to launch Docker containers. /usr/bin/docker by default. |
| `docker.allowed.capabilities` | Comma separated capabilities that containers are allowed to add. By default no capabilities are allowed to be added. |
| `docker.allowed.devices` | Comma separated devices that containers are allowed to mount. By default no devices are allowed to be added. |
| `docker.allowed.networks` | Comma separated networks that containers are allowed to use. If no network is specified when launching the container, the default Docker network will be used. |
| `docker.allowed.ro-mounts` | Comma separated directories that containers are allowed to mount in read-only mode. By default, no directories are allowed to mounted. |
| `docker.allowed.rw-mounts` | Comma separated directories that containers are allowed to mount in read-write mode. By default, no directories are allowed to mounted. |
| `docker.allowed.volume-drivers` | Comma separated list of volume drivers which are allowed to be used. By default, no volume drivers are allowed. |
| `docker.host-pid-namespace.enabled` | Set to "true" or "false" to enable or disable using the host's PID namespace. Default value is "false". |
| `docker.privileged-containers.enabled` | Set to "true" or "false" to enable or disable launching privileged containers. Default value is "false". |
| `docker.trusted.registries` | Comma separated list of trusted docker registries for running trusted privileged docker containers.  By default, no registries are defined. |
| `docker.inspect.max.retries` | Integer value to check docker container readiness.  Each inspection is set with 3 seconds delay.  Default value of 10 will wait 30 seconds for docker container to become ready before marked as container failed. |
| `docker.no-new-privileges.enabled` | Enable/disable the no-new-privileges flag for docker run. Set to "true" to enable, disabled by default. |
| `docker.allowed.runtimes` | Comma seperated runtimes that containers are allowed to use. By default no runtimes are allowed to be added.|

Please note that if you wish to run Docker containers that require access to the YARN local directories, you must add them to the docker.allowed.rw-mounts list.

In addition, containers are not permitted to mount any parent of the container-executor.cfg directory in read-write mode.

The following properties are optional:

|Configuration Name | Description |
|:---- |:---- |
| `min.user.id` | The minimum UID that is allowed to launch applications. The default is no minimum |
| `banned.users` | A comma-separated list of usernames who should not be allowed to launch applications. The default setting is: yarn, mapred, hdfs, and bin. |
| `allowed.system.users` | A comma-separated list of usernames who should be allowed to launch applications even if their UIDs are below the configured minimum. If a user appears in allowed.system.users and banned.users, the user will be considered banned. |
| `feature.tc.enabled` | Must be "true" or "false". "false" means traffic control commands are disabled. "true" means traffic control commands are allowed. |

Part of a container-executor.cfg which allows Docker containers to be launched is below:

```
yarn.nodemanager.linux-container-executor.group=yarn
[docker]
  module.enabled=true
  docker.privileged-containers.enabled=true
  docker.trusted.registries=centos
  docker.allowed.capabilities=SYS_CHROOT,MKNOD,SETFCAP,SETPCAP,FSETID,CHOWN,AUDIT_WRITE,SETGID,NET_RAW,FOWNER,SETUID,DAC_OVERRIDE,KILL,NET_BIND_SERVICE
  docker.allowed.networks=bridge,host,none
  docker.allowed.ro-mounts=/sys/fs/cgroup
  docker.allowed.rw-mounts=/var/hadoop/yarn/local-dir,/var/hadoop/yarn/log-dir

```

Docker Image Requirements
-------------------------

In order to work with YARN, there are two requirements for Docker images.

First, the Docker container will be explicitly launched with the application
owner as the container user.  If the application owner is not a valid user
in the Docker image, the application will fail. The container user is specified
by the user's UID. If the user's UID is different between the NodeManager host
and the Docker image, the container may be launched as the wrong user or may
fail to launch because the UID does not exist.  See
[User Management in Docker Container](#user-management) section for more details.

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

If a Docker image has an entry point set and
YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE is set to true,
launch_command will be passed to ENTRYPOINT program as CMD parameters in
Docker.  The format of launch_command looks like: param1,param2 and this
translates to CMD [ "param1","param2" ] in Docker.

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
| `YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_PID_NAMESPACE` | Controls which PID namespace will be used by the Docker container. By default, each Docker container has its own PID namespace. To share the namespace of the host, the yarn.nodemanager.runtime.linux.docker.host-pid-namespace.allowed property must be set to true. If the host PID namespace is allowed and this environment variable is set to host, the Docker container will share the host's PID namespace. No other value is allowed. |
| `YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER` | Controls whether the Docker container is a privileged container. In order to use privileged containers, the yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed property must be set to true, and the application owner must appear in the value of the yarn.nodemanager.runtime.linux.docker.privileged-containers.acl property. If this environment variable is set to true, a privileged Docker container will be used if allowed. No other value is allowed, so the environment variable should be left unset rather than setting it to false. |
| `YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS` | Adds additional volume mounts to the Docker container. The value of the environment variable should be a comma-separated list of mounts. All such mounts must be given as `source:dest[:mode]` and the mode must be "ro" (read-only) or "rw" (read-write) to specify the type of access being requested. If neither is specified, read-write will be  assumed. The mode may include a bind propagation option. In that case, the mode should either be of the form `[option]`, `rw+[option]`, or `ro+[option]`. Valid bind propagation options are shared, rshared, slave, rslave, private, and rprivate. The requested mounts will be validated by container-executor based on the values set in container-executor.cfg for `docker.allowed.ro-mounts` and `docker.allowed.rw-mounts`. |
| `YARN_CONTAINER_RUNTIME_DOCKER_TMPFS_MOUNTS` | Adds additional tmpfs mounts to the Docker container. The value of the environment variable should be a comma-separated list of absolute mount points within the container. |
| `YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL` | Allows a user to request delayed deletion of the Docker container on a per container basis. If true, Docker containers will not be removed until the duration defined by yarn.nodemanager.delete.debug-delay-sec has elapsed. Administrators can disable this feature through the yarn-site property yarn.nodemanager.runtime.linux.docker.delayed-removal.allowed. This feature is disabled by default. When this feature is disabled or set to false, the container will be removed as soon as it exits. |

The first two are required. The remainder can be set as needed. While
controlling the container type through environment variables is somewhat less
than ideal, it allows applications with no awareness of YARN's Docker support
(such as MapReduce and Spark) to nonetheless take advantage of it through their
support for configuring the application environment.

Once an application has been submitted to be launched in a Docker container,
the application will behave exactly as any other YARN application. Logs will be
aggregated and stored in the relevant history server. The application life cycle
will be the same as for a non-Docker application.

Using Docker Bind Mounted Volumes
---------------------------------

**WARNING** Care should be taken when enabling this feature. Enabling access to
directories such as, but not limited to, /, /etc, /run, or /home is not
advisable and can result in containers negatively impacting the host or leaking
sensitive information. **WARNING**

Files and directories from the host are commonly needed within the Docker
containers, which Docker provides through
[volumes](https://docs.docker.com/engine/tutorials/dockervolumes/).
Examples include localized resources, Apache Hadoop binaries, and sockets. To
facilitate this need, YARN-6623 added the ability for administrators to set a
whitelist of host directories that are allowed to be bind mounted as volumes
into containers. YARN-5534 added the ability for users to supply a list of
mounts that will be mounted into the containers, if allowed by the
administrative whitelist.

In order to make use of this feature, the following must be configured.

* The administrator must define the volume whitelist in container-executor.cfg by setting `docker.allowed.ro-mounts` and `docker.allowed.rw-mounts` to the list of parent directories that are allowed to be mounted.
* The application submitter requests the required volumes at application submission time using the `YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS` environment variable.

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

The administrator sets docker.allowed.ro-mounts in container-executor.cfg to
"/sys/fs/cgroup". Applications can now request that "/sys/fs/cgroup" be mounted
from the host into the container in read-only mode.

At application submission time, the YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS
environment variable can then be set to request this mount. In this example,
the environment variable would be set to "/sys/fs/cgroup:/sys/fs/cgroup:ro".
The destination path is not restricted, "/sys/fs/cgroup:/cgroup:ro" would also
be valid given the example admin whitelist.

<a href="#user-management"></a>User Management in Docker Container
-----------------------------------

YARN's Docker container support launches container processes using the uid:gid
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
is `99` and the `nobody` group is `99`. As a result, YARN will call `docker run`
with `--user 99:99`. If the `nobody` user does not have the uid `99` in the
container, the launch may fail or have unexpected results.

One exception to this rule is the use of Privileged Docker containers.
Privileged containers will not set the uid:gid pair when launching the container
and will honor the USER or GROUP entries in the Dockerfile. This allows running
privileged containers as any user which has security implications. Please
understand these implications before enabling Privileged Docker containers.

There are many ways to address user and group management. Docker, by default,
will authenticate users against `/etc/passwd` (and `/etc/shadow`) within the
container. Using the default `/etc/passwd` supplied in the Docker image is
unlikely to contain the appropriate user entries and will result in launch
failures. It is highly recommended to centralize user and group management.
Several approaches to user and group management are outlined below.

### Static user management

The most basic approach to managing user and groups is to modify the user and
group within the Docker image. This approach is only viable in non-secure mode
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
`docker.allowed.ro-mounts` in `container-executor.cfg` to include those paths.
When submitting the application, `YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS` will
need to include `/etc/passwd:/etc/passwd:ro` and `/etc/group:/etc/group:ro`.

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

Privileged Container Security Consideration
-------------------------------------------

Privileged docker container can interact with host system devices.  This can cause harm to host operating system without proper care.  In order to mitigate risk of allowing privileged container to run on Hadoop cluster, we implemented a controlled process to sandbox unauthorized privileged docker images.

The default behavior is disallow any privileged docker containers.  When `docker.privileged-containers.enabled` is set to enabled, docker image can run with root privileges in the docker container, but access to host level devices are disabled.  This allows developer and tester to run docker images from internet without causing harm to host operating system.

When docker images have been certified by developers and testers to be trustworthy.  The trusted image can be promoted to trusted docker registry.  System administrator can define `docker.trusted.registries`, and setup private docker registry server to promote trusted images.

Trusted images are allowed to mount external devices such as HDFS via NFS gateway, or host level Hadoop configuration.  If system administrators allow writing to external volumes using `docker.allow.rw-mounts directive`, privileged docker container can have full control of host level files in the predefined volumes.

For [YARN Service HTTPD example](./yarn-service/Examples.html), container-executor.cfg must define centos docker registry to be trusted for the example to run.

Container Reacquisition Requirements
------------------------------------
On restart, the NodeManager, as part of the NodeManager's recovery process, will
validate that a container is still running by checking for the existence of the
container's PID directory in the /proc filesystem. For security purposes,
operating system administrator may enable the _hidepid_ mount option for the
/proc filesystem. If the _hidepid_ option is enabled, the _yarn_ user's primary
group must be whitelisted by setting the gid mount flag similar to below.
Without the _yarn_ user's primary group whitelisted, container reacquisition
will fail and the container will be killed on NodeManager restart.

```
proc     /proc     proc     nosuid,nodev,noexec,hidepid=2,gid=yarn     0 0
```

Connecting to a Secure Docker Repository
----------------------------------------

The Docker client command will draw its configuration from the default location,
which is $HOME/.docker/config.json on the NodeManager host. The Docker
configuration is where secure repository credentials are stored, so use of the
LCE with secure Docker repos is discouraged using this method.

YARN-5428 added support to Distributed Shell for securely supplying the Docker
client configuration. See the Distributed Shell help for usage. Support for
additional frameworks is planned.

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

This example assumes that Hadoop is installed to `/usr/local/hadoop`.

Additionally, `docker.allowed.ro-mounts` in `container-executor.cfg` has been
updated to include the directories: `/usr/local/hadoop,/etc/passwd,/etc/group`.

To submit the pi job to run in Docker containers, run the following commands:

```
  HADOOP_HOME=/usr/local/hadoop
  YARN_EXAMPLES_JAR=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar
  MOUNTS="$HADOOP_HOME:$HADOOP_HOME:ro,/etc/passwd:/etc/passwd:ro,/etc/group:/etc/group:ro"
  IMAGE_ID="library/openjdk:8"

  export YARN_CONTAINER_RUNTIME_TYPE=docker
  export YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=$IMAGE_ID
  export YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS

  yarn jar $YARN_EXAMPLES_JAR pi \
    -Dmapreduce.map.env.YARN_CONTAINER_RUNTIME_TYPE=docker \
    -Dmapreduce.map.env.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS \
    -Dmapreduce.map.env.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=$IMAGE_ID \
    -Dmapreduce.reduce.env.YARN_CONTAINER_RUNTIME_TYPE=docker \
    -Dmapreduce.reduce.env.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS \
    -Dmapreduce.reduce.env.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=$IMAGE_ID \
    1 40000
```

Note that the application master, map tasks, and reduce tasks are configured
independently. In this example, we are using the `openjdk:8` image for all three.

Example: Spark
--------------

This example assumes that Hadoop is installed to `/usr/local/hadoop` and Spark
is installed to `/usr/local/spark`.

Additionally, `docker.allowed.ro-mounts` in `container-executor.cfg` has been
updated to include the directories: `/usr/local/hadoop,/etc/passwd,/etc/group`.

To run a Spark shell in Docker containers, run the following command:

```
  HADOOP_HOME=/usr/local/hadoop
  SPARK_HOME=/usr/local/spark
  MOUNTS="$HADOOP_HOME:$HADOOP_HOME:ro,/etc/passwd:/etc/passwd:ro,/etc/group:/etc/group:ro"
  IMAGE_ID="library/openjdk:8"

  $SPARK_HOME/bin/spark-shell --master yarn \
    --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
    --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=$IMAGE_ID \
    --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS \
    --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
    --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=$IMAGE_ID \
    --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS
```

Note that the application master and executors are configured
independently. In this example, we are using the `openjdk:8` image for both.

Docker Container ENTRYPOINT Support
------------------------------------

When Docker support was introduced to Hadoop 2.x, the platform was designed to
run existing Hadoop programs inside Docker container.  Log redirection and
environment setup are integrated with Node Manager.  In Hadoop 3.x, Hadoop
Docker support extends beyond running Hadoop workload, and support Docker container
in Docker native form using ENTRYPOINT from dockerfile.  Application can decide to
support YARN mode as default or Docker mode as default by defining
YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE environment variable.
System administrator can also set as default setting for the cluster to make
ENTRY_POINT as default mode of operation.

In yarn-site.xml, add YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE to
node manager environment white list:
```
<property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME,YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE</value>
</property>
```

In yarn-env.sh, define:
```
export YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE=true
```
