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

NodeManager Overview
=====================

* [Overview](#Overview)
* [Health Checker Service](#Health_checker_service)
    * [Disk Checker](#Disk_Checker)
    * [External Health Script](#External_Health_Script)

Overview
--------

The NodeManager is responsible for launching and managing containers on a node. Containers execute tasks as specified by the AppMaster.

Health Checker Service
----------------------

The NodeManager runs services to determine the health of the node it is executing on. The services perform checks on the disk as well as any user specified tests. If any health check fails, the NodeManager marks the node as unhealthy and communicates this to the ResourceManager, which then stops assigning containers to the node. Communication of the node status is done as part of the heartbeat between the NodeManager and the ResourceManager. The intervals at which the disk checker and health monitor(described below) run don't affect the heartbeat intervals. When the heartbeat takes place, the status of both checks is used to determine the health of the node.

###Disk Checker

  The disk checker checks the state of the disks that the NodeManager is configured to use(local-dirs and log-dirs, configured using yarn.nodemanager.local-dirs and yarn.nodemanager.log-dirs respectively). The checks include permissions and free disk space. It also checks that the filesystem isn't in a read-only state. The checks are run at 2 minute intervals by default but can be configured to run as often as the user desires. If a disk fails the check, the NodeManager stops using that particular disk but still reports the node status as healthy. However if a number of disks fail the check(the number can be configured, as explained below), then the node is reported as unhealthy to the ResourceManager and new containers will not be assigned to the node. In addition, once a disk is marked as unhealthy, the NodeManager stops checking it to see if it has recovered(e.g. disk became full and was then cleaned up). The only way for the NodeManager to use that disk to restart the software on the node. The following configuration parameters can be used to modify the disk checks:

| Configuration Name | Allowed Values | Description |
|:---- |:---- |:---- |
| `yarn.nodemanager.disk-health-checker.enable` | true, false | Enable or disable the disk health checker service |
| `yarn.nodemanager.disk-health-checker.interval-ms` | Positive integer | The interval, in milliseconds, at which the disk checker should run; the default value is 2 minutes |
| `yarn.nodemanager.disk-health-checker.min-healthy-disks` | Float between 0-1 | The minimum fraction of disks that must pass the check for the NodeManager to mark the node as healthy; the default is 0.25 |
| `yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage` | Float between 0-100 | The maximum percentage of disk space that may be utilized before a disk is marked as unhealthy by the disk checker service. This check is run for every disk used by the NodeManager. The default value is 90 i.e. 90% of the disk can be used. |
| `yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb` | Integer | The minimum amount of free space that must be available on the disk for the disk checker service to mark the disk as healthy. This check is run for every disk used by the NodeManager. The default value is 0 i.e. the entire disk can be used. |


###External Health Script

  Users may specify their own health checker script that will be invoked by the health checker service. Users may specify a timeout as well as options to be passed to the script. If the script exits with a non-zero exit code, times out or results in an exception being thrown, the node is marked as unhealthy. Please note that if the script cannot be executed due to permissions or an incorrect path, etc, then it counts as a failure and the node will be reported as unhealthy. Please note that speifying a health check script is not mandatory. If no script is specified, only the disk checker status will be used to determine the health of the node. The following configuration parameters can be used to set the health script:

| Configuration Name | Allowed Values | Description |
|:---- |:---- |:---- |
| `yarn.nodemanager.health-checker.interval-ms` | Postive integer | The interval, in milliseconds, at which health checker service runs; the default value is 10 minutes. |
| `yarn.nodemanager.health-checker.script.timeout-ms` | Postive integer | The timeout for the health script that's executed; the default value is 20 minutes. |
| `yarn.nodemanager.health-checker.script.path` | String | Absolute path to the health check script to be run. |
| `yarn.nodemanager.health-checker.script.opts` | String | Arguments to be passed to the script when the script is executed. |


