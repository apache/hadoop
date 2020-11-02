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

NodeManager
===========

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

The NodeManager is responsible for launching and managing containers on a node. Containers execute tasks as specified by the AppMaster.


Health Checker Service
----------------------

The NodeManager runs services to determine the health of the node it is executing on. The services perform checks on the disk as well as any user specified tests. If any health check fails, the NodeManager marks the node as unhealthy and communicates this to the ResourceManager, which then stops assigning containers to the node. Communication of the node status is done as part of the heartbeat between the NodeManager and the ResourceManager. The intervals at which the disk checker and health monitor(described below) run don't affect the heartbeat intervals. When the heartbeat takes place, the status of both checks is used to determine the health of the node.

###Disk Checker

  The disk checker checks the state of the disks that the NodeManager is configured to use(local-dirs and log-dirs, configured using yarn.nodemanager.local-dirs and yarn.nodemanager.log-dirs respectively). The checks include permissions and free disk space. It also checks that the filesystem isn't in a read-only state. The checks are run at 2 minute intervals by default but can be configured to run as often as the user desires. If a disk fails the check, the NodeManager stops using that particular disk but still reports the node status as healthy. However if a number of disks fail the check(the number can be configured, as explained below), then the node is reported as unhealthy to the ResourceManager and new containers will not be assigned to the node.

The following configuration parameters can be used to modify the disk checks:

| Configuration Name | Allowed Values | Description |
|:---- |:---- |:---- |
| `yarn.nodemanager.disk-health-checker.enable` | true, false | Enable or disable the disk health checker service |
| `yarn.nodemanager.disk-health-checker.interval-ms` | Positive integer | The interval, in milliseconds, at which the disk checker should run; the default value is 2 minutes |
| `yarn.nodemanager.disk-health-checker.min-healthy-disks` | Float between 0-1 | The minimum fraction of disks that must pass the check for the NodeManager to mark the node as healthy; the default is 0.25 |
| `yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage` | Float between 0-100 | The maximum percentage of disk space that may be utilized before a disk is marked as unhealthy by the disk checker service. This check is run for every disk used by the NodeManager. The default value is 90 i.e. 90% of the disk can be used. |
| `yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb` | Integer | The minimum amount of free space that must be available on the disk for the disk checker service to mark the disk as healthy. This check is run for every disk used by the NodeManager. The default value is 0 i.e. the entire disk can be used. |

### External Health Script

Users may specify their own health checker scripts that will be invoked by the health checker service. Users may specify a timeout as well as options to be passed to the script. If the script times out, results in an exception being thrown or outputs a line which begins with the string ERROR, the node is marked as unhealthy. Please note that:

  * Exit code other than 0 is **not** considered to be a failure because it might have been caused by a syntax error. Therefore the node will **not** be marked as unhealthy.

  * If the script cannot be executed due to permissions or an incorrect path, etc, then it counts as a failure and the node will be reported as unhealthy.

  * Specifying a health check script is not mandatory. If no script is specified, only the disk checker status will be used to determine the health of the node.

Users can specify up to 4 scripts to run individually with the `yarn.nodemanager.health-checker.scripts` configuration. Also these options can be configured for all scripts (global configurations):

| Configuration Name | Allowed Values | Description |
|:---- |:---- |:---- |
|`yarn.nodemanager.health-checker.script`| String | The keywords for the health checker scripts separated by a comma. The default is "script". |
| `yarn.nodemanager.health-checker.interval-ms` | Positive integer | The interval, in milliseconds, at which health checker service runs; the default value is 10 minutes. |
| `yarn.nodemanager.health-checker.timeout-ms` | Positive integer | The timeout for the health script that's executed; the default value is 20 minutes. |

The following options can be set for every health checker script. The %s symbol is substituted with each keyword provided in `yarn.nodemanager.health-checker.script`.

| Configuration Name | Allowed Values | Description |
|:---- |:---- |:---- |
| `yarn.nodemanager.health-checker.%s.path` | String | Absolute path to the health check script to be run. Mandatory argument for each script. |
| `yarn.nodemanager.health-checker.%s.opts` | String | Arguments to be passed to the script when the script is executed. Mandatory argument for each script. |
| `yarn.nodemanager.health-checker.%s.interval-ms` | Positive integer | The interval, in milliseconds, at which health checker service runs.  |
| `yarn.nodemanager.health-checker.%s.timeout-ms` | Positive integer | The timeout for the health script that's executed. |

The interval and timeout options are not required to be specified. In that case the global configurations will be used.

NodeManager Restart
-------------------

### Introduction

This document gives an overview of NodeManager (NM) restart, a feature that enables the NodeManager to be restarted without losing the active containers running on the node. At a high level, the NM stores any necessary state to a local state-store as it processes container-management requests. When the NM restarts, it recovers by first loading state for various subsystems and then letting those subsystems perform recovery using the loaded state.

### Enabling NM Restart

Step 1. To enable NM Restart functionality, set the following property in **conf/yarn-site.xml** to *true*.

| Property | Value |
|:---- |:---- |
| `yarn.nodemanager.recovery.enabled` | `true`, (default value is set to false) |

Step 2.  Configure a path to the local file-system directory where the NodeManager can save its run state.

| Property | Description |
|:---- |:---- |
| `yarn.nodemanager.recovery.dir` | The local filesystem directory in which the node manager will store state when recovery is enabled. The default value is set to `$hadoop.tmp.dir/yarn-nm-recovery`. |

Step 3: Enable NM supervision under recovery to prevent running containers from getting cleaned up when NM exits.

| Property | Description |
|:---- |:---- |
| `yarn.nodemanager.recovery.supervised` | If enabled, NodeManager running will not try to cleanup containers as it exits with the assumption it will be immediately be restarted and recover containers The default value is set to 'false'. |

Step 4.  Configure a valid RPC address for the NodeManager.

| Property | Description |
|:---- |:---- |
| `yarn.nodemanager.address` | Ephemeral ports (port 0, which is default) cannot be used for the NodeManager's RPC server specified via yarn.nodemanager.address as it can make NM use different ports before and after a restart. This will break any previously running clients that were communicating with the NM before restart. Explicitly setting yarn.nodemanager.address to an address with specific port number (for e.g 0.0.0.0:45454) is a precondition for enabling NM restart. |

Step 5.  Auxiliary services.

  * NodeManagers in a YARN cluster can be configured to run auxiliary services. For a completely functional NM restart, YARN relies on any auxiliary service configured to also support recovery. This usually includes (1) avoiding usage of ephemeral ports so that previously running clients (in this case, usually containers) are not disrupted after restart and (2) having the auxiliary service itself support recoverability by reloading any previous state when NodeManager restarts and reinitializes the auxiliary service.

  * A simple example for the above is the auxiliary service 'ShuffleHandler' for MapReduce (MR). ShuffleHandler respects the above two requirements already, so users/admins don't have to do anything for it to support NM restart: (1) The configuration property **mapreduce.shuffle.port** controls which port the ShuffleHandler on a NodeManager host binds to, and it defaults to a non-ephemeral port. (2) The ShuffleHandler service also already supports recovery of previous state after NM restarts.

  * There are two ways to configure auxiliary services, through a manifest or through the Configuration. Auxiliary services will only be loaded via the prior method of using Configuration properties when an auxiliary services manifest is not enabled. One advantage of using a manifest is that NMs can dynamically reload auxiliary services based on changes to the manifest. To support reloading, AuxiliaryService implementations must perform any cleanup that is needed during the service stop phase for the NM to be able to create a new instance of the auxiliary service.

Auxiliary Service Classpath Isolation
-------------------------------------

### Introduction
To launch auxiliary services on a NodeManager, users have to add their jar to NodeManager's classpath directly, thus put them on the system classloader. But if multiple versions of the plugin are present on the classpath, there is no control over which version actually gets loaded. Or if there are any conflicts between the dependencies introduced by the auxiliary services and the NodeManager itself, they can break the NodeManager, the auxiliary services, or both. To solve this issue, we can instantiate auxiliary services using a classloader that is different from the system classloader.

### Manifest
This section describes the auxiliary service manifest for aux-service classpath isolation.
To use a manifest, the property `yarn.nodemanager.aux-services.manifest.enabled` must be set to true in *yarn-site.xml*.

To load the manifest file from a filesystem, set the file path in *yarn-site.xml* under the property `yarn.nodemanager.aux-services.manifest`. The NMs will check this file for new modifications at an interval specified by `yarn.nodemanager.aux-services.manifest.reload-ms` (defaults to 0; setting interval <= 0 means it will not be reloaded automatically).
Alternatively, the manifest file may be sent to an NM via REST API by making a PUT call to the endpoint `http://nm-http-address:port/ws/v1/node/auxiliaryservices`. Note this only updates the manifest on one NM.
When it reads a new manifest, the NM will add, remove, or reload auxiliary services as needed based on the service names and versions found in the manifest.

An example manifest that configures classpath isolation for a CustomAuxService follows. One or more files may be specified to make up the classpath of a service, with jar or archive formats being supported.
```
{
  "services": [
    {
      "name": "mapreduce_shuffle",
      "version": "2",
      "configuration": {
        "properties": {
          "class.name": "org.apache.hadoop.mapred.ShuffleHandler",
          "mapreduce.shuffle.transfer.buffer.size": "102400",
          "mapreduce.shuffle.port": "13562"
        }
      }
    },
    {
      "name": "CustomAuxService",
      "version": "1",
      "configuration": {
        "properties": {
          "class.name": "org.aux.CustomAuxService"
        },
        "files": [
          {
            "src_file": "${remote-dir}/CustomAuxService.jar",
            "type": "STATIC"
          },
          {
            "src_file": "${remote-dir}/CustomAuxService.tgz",
            "type": "ARCHIVE"
          }
        ]
      }
    }
  ]
}
```

### Configuration
This section describes the configuration variables for aux-service classpath isolation. Aux services will only be loaded from the configuration if a manifest file is not specified.

The following settings need to be set in *yarn-site.xml*.

|Configuration Name | Description |
|:---- |:---- |
| `yarn.nodemanager.aux-services.%s.classpath` | Provide local directory which includes the related jar file as well as all the dependencies’ jar file. We could specify the single jar file or use ${local_dir_to_jar}/* to load all jars under the dep directory. |
| `yarn.nodemanager.aux-services.%s.remote-classpath` | Provide remote absolute or relative path to jar file(We also support zip, tar.gz, tgz, tar and gz files as well). For the same aux-service class, we can only specify one of the configurations: yarn.nodemanager.aux-services.%s.classpath or yarn.nodemanager.aux-services.%s.remote-classpath. The YarnRuntimeException will be thrown. Please also make sure that the owner of the jar file must be the same as the NodeManager user and the permbits should satisfy (permbits & 0022)==0 (such as 600, it's not writable by group or other).|
| `yarn.nodemanager.aux-services.%s.system-classes` | Normally, we do not need to set this configuration. The class would be loaded from customized classpath if it does not belongs to system-classes. For example, by default, the package org.apache.hadoop is in the system-classes, if your class CustomAuxService is in the package org.apache.hadoop, it would not be loaded from customized classpath. To solve this, either we could change the package for CustomAuxService, or configure our own system-classes which exclude org.apache.hadoop. |

### Configuration Examples

	<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle,CustomAuxService</value>
	</property>

	<property>
		<name>yarn.nodemanager.aux-services.CustomAuxService.classpath</name>
		<value>${local_dir_to_jar}/CustomAuxService.jar</value>
	</property>

    <!--
	<property>
		<name>yarn.nodemanager.aux-services.CustomAuxService.remote-classpath</name>
		<value>${remote-dir_to_jar}/CustomAuxService.jar</value>
	</property>
    -->

	<property>
		<name>yarn.nodemanager.aux-services.CustomAuxService.class</name>
		<value>org.aux.CustomAuxService</value>
	</property>

	<property>
		<name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
		<value>org.apache.hadoop.mapred.ShuffleHandler</value>
	</property>

Prevent Container Logs From Getting Too Big
-------------------------------------------

This allows a cluster admin to configure a cluster such that a task attempt will be killed if any container log exceeds a configured size. This helps prevent logs from filling disks and also prevent the need to aggregate enormous logs.

### Configuration

The following parameters can be used to configure the container log dir sizes.

| Configuration Name | Allowed Values | Description |
|:---- |:---- |:---- |
| `yarn.nodemanager.container-log-monitor.enable` | true, false | Flag to enable the container log monitor which enforces container log directory size limits. Default is false. |
| `yarn.nodemanager.container-log-monitor.interval-ms` | Positive integer | How often to check the usage of a container's log directories in milliseconds. Default is 60000 ms. |
| `yarn.nodemanager.container-log-monitor.dir-size-limit-bytes` | Long | The disk space limit, in bytes, for a single container log directory. Default is 1000000000. |
| `yarn.nodemanager.container-log-monitor.total-size-limit-bytes` | Long | The disk space limit, in bytes, for all of a container's logs. The default is 10000000000. |

Scale Heart-beat Interval Based on CPU Utilization
-------------------------------------------------

This allows a cluster admin to configure a cluster to allow the heart-beat between the Resource Manager and each NodeManager to be scaled based on the CPU utilization of the node compared to the overall CPU utilization of the cluster. 

### Configuration

The following parameters can be used to configure the heart-beat interval and whether and how it scales.

| Configuration Name | Allowed Values | Description |
|:---- |:---- |:---- |
| `yarn.resourcemanager.nodemanagers.heartbeat-interval-ms` | Long | Specifies the default heart-beat interval in milliseconds for every NodeManager in the cluster. Default is 1000 ms. |
| `yarn.resourcemanager.nodemanagers.heartbeat-interval-scaling-enable` | true, false | Enables heart-beat interval scaling.  If true, The NodeManager heart-beat interval will scale based on the difference between the CPU utilization on the node and the cluster-wide average CPU utilization. Default is false. |
| `yarn.resourcemanager.nodemanagers.heartbeat-interval-min-ms` | Positive Long | If heart-beat interval scaling is enabled, this is the minimum heart-beat interval in milliseconds. Default is 1000 ms. |
| `yarn.resourcemanager.nodemanagers.heartbeat-interval-max-ms` | Positive Long | If heart-beat interval scaling is enabled, this is the maximum heart-beat interval in milliseconds. Default is 1000 ms. |
| `yarn.resourcemanager.nodemanagers.heartbeat-interval-speedup-factor` | Positive Float | If heart-beat interval scaling is enabled, this controls the degree of adjustment when speeding up heartbeat intervals. At 1.0, 20% less than the average cluster-wide CPU utilization will result in a 20% decrease in the heartbeat interval. Default is 1.0. |
| `yarn.resourcemanager.nodemanagers.heartbeat-interval-slowdown-factor` | Positive Float | If heart-beat interval scaling is enabled, this controls the degree of adjustment when slowing down heartbeat intervals. At 1.0, 20% greater than the average cluster-wide CPU utilization will result in a 20% increase in the heartbeat interval. Default is 1.0. |
