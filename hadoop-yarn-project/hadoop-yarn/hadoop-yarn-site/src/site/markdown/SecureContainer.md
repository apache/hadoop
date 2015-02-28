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

YARN Secure Containers
======================

* [Overview](#Overview)

Overview
--------

YARN containers in a secure cluster use the operating system facilities to offer execution isolation for containers. Secure containers execute under the credentials of the job user. The operating system enforces access restriction for the container. The container must run as the use that submitted the application.

Secure Containers work only in the context of secured YARN clusters.

###Container isolation requirements

  The container executor must access the local files and directories needed by the container such as jars, configuration files, log files, shared objects etc. Although it is launched by the NodeManager, the container should not have access to the NodeManager private files and configuration. Container running applications submitted by different users should be isolated and unable to access each other files and directories. Similar requirements apply to other system non-file securable objects like named pipes, critical sections, LPC queues, shared memory etc.

###Linux Secure Container Executor

  On Linux environment the secure container executor is the `LinuxContainerExecutor`. It uses an external program called the **container-executor**\> to launch the container. This program has the `setuid` access right flag set which allows it to launch the container with the permissions of the YARN application user.

###Configuration

  The configured directories for `yarn.nodemanager.local-dirs` and `yarn.nodemanager.log-dirs` must be owned by the configured NodeManager user (`yarn`) and group (`hadoop`). The permission set on these directories must be `drwxr-xr-x`.

  The `container-executor` program must be owned by `root` and have the permission set `---sr-s---`.

  To configure the `NodeManager` to use the `LinuxContainerExecutor` set the following in the **conf/yarn-site.xml**:

```xml
<property>
  <name>yarn.nodemanager.container-executor.class</name>
  <value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value>
</property>

<property>
  <name>yarn.nodemanager.linux-container-executor.group</name>
  <value>hadoop</value>
</property>
```

  Additionally the LCE requires the `container-executor.cfg` file, which is read by the `container-executor` program.

```
yarn.nodemanager.linux-container-executor.group=#configured value of yarn.nodemanager.linux-container-executor.group
banned.users=#comma separated list of users who can not run applications
allowed.system.users=#comma separated list of allowed system users
min.user.id=1000#Prevent other super-users
```

###Windows Secure Container Executor (WSCE)

  The Windows environment secure container executor is the `WindowsSecureContainerExecutor`. It uses the Windows S4U infrastructure to launch the container as the YARN application user. The WSCE requires the presense of the `hadoopwinutilsvc` service. This services is hosted by `%HADOOP_HOME%\bin\winutils.exe` started with the `service` command line argument. This service offers some privileged operations that require LocalSystem authority so that the NM is not required to run the entire JVM and all the NM code in an elevated context. The NM interacts with the `hadoopwintulsvc` service by means of Local RPC (LRPC) via calls JNI to the RCP client hosted in `hadoop.dll`.

###Configuration

  To configure the `NodeManager` to use the `WindowsSecureContainerExecutor` set the following in the **conf/yarn-site.xml**:

```xml
        <property>
          <name>yarn.nodemanager.container-executor.class</name>
          <value>org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor</value>
        </property>

        <property>
          <name>yarn.nodemanager.windows-secure-container-executor.group</name>
          <value>yarn</value>
        </property>
```
   
  The hadoopwinutilsvc uses `%HADOOP_HOME%\etc\hadoop\wsce_site.xml` to configure access to the privileged operations.

```xml
<property>
 <name>yarn.nodemanager.windows-secure-container-executor.impersonate.allowed</name>
  <value>HadoopUsers</value>
</property>

<property>
  <name>yarn.nodemanager.windows-secure-container-executor.impersonate.denied</name>
  <value>HadoopServices,Administrators</value>
</property>

<property>
  <name>yarn.nodemanager.windows-secure-container-executor.allowed</name>
  <value>nodemanager</value>
</property>

<property>
  <name>yarn.nodemanager.windows-secure-container-executor.local-dirs</name>
  <value>nm-local-dir, nm-log-dirs</value>
</property>

<property>
  <name>yarn.nodemanager.windows-secure-container-executor.job-name</name>
  <value>nodemanager-job-name</value>
</property>  
```

  `yarn.nodemanager.windows-secure-container-executor.allowed` should contain the name of the service account running the nodemanager. This user will be allowed to access the hadoopwintuilsvc functions.

  `yarn.nodemanager.windows-secure-container-executor.impersonate.allowed` should contain the users that are allowed to create containers in the cluster. These users will be allowed to be impersonated by hadoopwinutilsvc.

  `yarn.nodemanager.windows-secure-container-executor.impersonate.denied` should contain users that are explictly forbiden from creating containers. hadoopwinutilsvc will refuse to impersonate these users.

  `yarn.nodemanager.windows-secure-container-executor.local-dirs` should contain the nodemanager local dirs. hadoopwinutilsvc will allow only file operations under these directories. This should contain the same values as `$yarn.nodemanager.local-dirs, $yarn.nodemanager.log-dirs` but note that hadoopwinutilsvc XML configuration processing does not do substitutions so the value must be the final value. All paths must be absolute and no environment variable substitution will be performed. The paths are compared LOCAL\_INVARIANT case insensitive string comparison, the file path validated must start with one of the paths listed in local-dirs configuration. Use comma as path separator:`,`

  `yarn.nodemanager.windows-secure-container-executor.job-name` should contain an Windows NT job name that all containers should be added to. This configuration is optional. If not set, the container is not added to a global NodeManager job. Normally this should be set to the job that the NM is assigned to, so that killing the NM kills also all containers. Hadoopwinutilsvc will not attempt to create this job, the job must exists when the container is launched. If the value is set and the job does not exists, container launch will fail with error 2 `The system cannot find the file specified`. Note that this global NM job is not related to the container job, which always gets created for each container and is named after the container ID. This setting controls a global job that spans all containers and the parent NM, and as such it requires nested jobs. Nested jobs are available only post Windows 8 and Windows Server 2012.

####Useful Links

  * [Exploring S4U Kerberos Extensions in Windows Server 2003](http://msdn.microsoft.com/en-us/magazine/cc188757.aspx)

  * [Nested Jobs](http://msdn.microsoft.com/en-us/library/windows/desktop/hh448388.aspx)

  * [Winutils needs ability to create task as domain user](https://issues.apache.org/jira/browse/YARN-1063)

  * [Implement secure Windows Container Executor](https://issues.apache.org/jira/browse/YARN-1972)

  * [Remove the need to run NodeManager as privileged account for Windows Secure Container Executor](https://issues.apache.org/jira/browse/YARN-2198)


