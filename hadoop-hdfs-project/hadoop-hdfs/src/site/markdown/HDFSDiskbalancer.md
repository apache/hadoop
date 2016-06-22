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

HDFS Disk Balancer
===================

* [Overview](#Overview)
* [Architecture](#Architecture)
* [Commands](#Commands)
* [Settings](#Settings)


Overview
--------

Diskbalancer is a command line tool that distributes data evenly on all disks of a datanode.
This tool is different from  [Balancer](./HdfsUserGuide.html#Balancer)  which
takes care of cluster-wide data balancing. Data can have uneven spread between
disks on a node due to several reasons. This can happen due to large amount of
writes and deletes or due to a disk replacement.This tool operates against a given datanode and moves blocks from one disk to another.



Architecture
------------

Disk Balancer operates by creating a plan and goes on to execute that plan on the datanode.
A plan is a set of statements that describe how much data should move between two disks.
A plan is composed of multiple move steps. A move step has source disk, destination
disk and number of bytes to move.A plan can be executed against an operational data node. Disk balancer should not
interfere with other processes since it throttles how much data is copied
every second. Please note that disk balancer is not enabled by default on a cluster.
To enable diskbalancer `dfs.disk.balancer.enabled` must be set to `true` in hdfs-site.xml.


Commands
--------
The following sections discusses what commands are supported by disk balancer
 and how to use them.

### Plan

 The plan command can be run against a given datanode by running

 `hdfs diskbalancer -plan node1.mycluster.com`

 The command accepts [Generic Options](../hadoop-common/CommandsManual.html#Generic_Options).

 The plan command also has a set of parameters that allows user to control
 the output and execution of the plan.

| COMMAND\_OPTION    | Description |
|:---- |:---- |
| `-out`| Allows user to control the output location of the plan file.|
| `-bandwidth`| Since datanode is operational and might be running other jobs, diskbalancer limits the amount of data moved per second. This parameter allows user to set the maximum bandwidth to be used. This is not required to be set since diskBalancer will use the deafult bandwidth if this is not specified.|
| `-thresholdPercentage`| Since we operate against a snap-shot of datanode, themove operations have a tolerance percentage to declare success. If user specifies 10% and move operation is say 20GB in size, if we can move 18GB that operation is considered successful. This is to accomodate the changes in datanode in real time. This parameter is not needed and a default is used if not specified.|
| `-maxerror` | Max error allows users to specify how many block copy operations must fail before we abort a move step. Once again, this is not a needed parameter and a system-default is used if not specified.|
| `-v`| Verbose mode, specifying this parameter forces the plan command to print out a summary of the plan on stdout.|

The plan command writes two output files. They are `<nodename>.before.json` which
captures the state of the datanode before the diskbalancer is run, and `<nodename>.plan.json`.

### Execute

Execute command takes a plan command executes it against the datanode that plan was generated against.

`hdfs diskbalancer -execute /system/diskbalancer/nodename.plan.json`

This executes the plan by reading datanode’s address from the plan file.

### Query

Query command gets the current status of the diskbalancer from a datanode.

`hdfs diskbalancer -query nodename.mycluster.com`

| COMMAND\_OPTION | Description |
|:---- |:---- |
|`-v` | Verbose mode, Prints out status of individual moves|


### Cancel
Cancel command cancels a running plan. Restarting datanode has the same effect as cancel command since plan information on the datanode is transient.

`hdfs diskbalancer -cancel /system/diskbalancer/nodename.plan.json`

or

`hdfs diskbalancer -cancel planID -node nodename`

Plan ID can be read from datanode using query command.

### Report
Report command provides detailed report about a node.

`hdfs diskbalancer -fs http://namenode.uri -report -node {DataNodeID | IP | Hostname}`


Settings
--------

There is a set of diskbalancer settings that can be controlled via hdfs-site.xml

| Setting | Description |
|:---- |:---- |
|`dfs.disk.balancer.enabled`| This parameter controls if diskbalancer is enabled for a cluster. if this is not enabled, any execute command will be rejected by the datanode.The default value is false.|
|`dfs.disk.balancer.max.disk.throughputInMBperSec` | This controls the maximum disk bandwidth consumed by diskbalancer while copying data. If a value like 10MB is specified then diskbalancer on the average will only copy 10MB/S. The default value is 10MB/S.|
|`dfs.disk.balancer.max.disk.errors`| sets the value of maximum number of errors we can ignore for a specific move between two disks before it is abandoned. For example, if a plan has 3 pair of disks to copy between , and the first disk set encounters more than 5 errors, then we abandon the first copy and start the second copy in the plan. The default value of max errors is set to 5.|
|`dfs.disk.balancer.block.tolerance.percent`| The tolerance percent sepcifies when we have reached a good enough value for any copy step. For example, if you specify 10% then getting close to 10% of the target value is good enough.|
