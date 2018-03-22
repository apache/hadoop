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

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->


Overview
--------

Diskbalancer is a command line tool that distributes data evenly on all disks of a datanode.
This tool is different from  [Balancer](./HdfsUserGuide.html#Balancer)  which
takes care of cluster-wide data balancing. Data can have uneven spread between
disks on a node due to several reasons. This can happen due to large amount of
writes and deletes or due to a disk replacement. This tool operates against a given datanode and moves blocks from one disk to another.



Architecture
------------

Disk Balancer operates by creating a plan and goes on to execute that plan on the datanode.
A plan is a set of statements that describe how much data should move between two disks.
A plan is composed of multiple move steps. A move step has source disk, destination
disk and number of bytes to move. A plan can be executed against an operational data node. Disk balancer should not
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
| `-bandwidth`| Since datanode is operational and might be running other jobs, diskbalancer limits the amount of data moved per second. This parameter allows user to set the maximum bandwidth to be used. This is not required to be set since diskBalancer will use the default bandwidth if this is not specified.|
| `-thresholdPercentage`| Since we operate against a snap-shot of datanode, the move operations have a tolerance percentage to declare success. If user specifies 10% and move operation is say 20GB in size, if we can move 18GB that operation is considered successful. This is to accommodate the changes in datanode in real time. This parameter is not needed and a default is used if not specified.|
| `-maxerror` | Max error allows users to specify how many block copy operations must fail before we abort a move step. Once again, this is not a needed parameter and a system-default is used if not specified.|
| `-v`| Verbose mode, specifying this parameter forces the plan command to print out a summary of the plan on stdout.|
|`-fs`| - Specifies the namenode to use. if not specified default from config  is used. |


The plan command writes two output files. They are `<nodename>.before.json` which
captures the state of the cluster before the diskbalancer is run, and
`<nodename>.plan.json`.

### Execute

Execute command takes a plan command executes it against the datanode that plan was generated against.

`hdfs diskbalancer -execute /system/diskbalancer/nodename.plan.json`

This executes the plan by reading datanode’s address from the plan file.

| COMMAND\_OPTION    | Description |
|:---- |:---- |
| `-skipDateCheck` |  Skip date check and force execute the plan.|

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
Report command provides detailed report of specified node(s) or top nodes that will benefit from running disk balancer. The node(s) can be specified by a host file or comma-separated list of nodes.

`hdfs diskbalancer -fs http://namenode.uri -report -node <file://> | [<DataNodeID|IP|Hostname>,...]`

or

`hdfs diskbalancer -fs http://namenode.uri -report -top topnum`

Settings
--------

There is a set of diskbalancer settings that can be controlled via hdfs-site.xml

| Setting | Description |
|:---- |:---- |
|`dfs.disk.balancer.enabled`| This parameter controls if diskbalancer is enabled for a cluster. if this is not enabled, any execute command will be rejected by the datanode.The default value is false.|
|`dfs.disk.balancer.max.disk.throughputInMBperSec` | This controls the maximum disk bandwidth consumed by diskbalancer while copying data. If a value like 10MB is specified then diskbalancer on the average will only copy 10MB/S. The default value is 10MB/S.|
|`dfs.disk.balancer.max.disk.errors`| sets the value of maximum number of errors we can ignore for a specific move between two disks before it is abandoned. For example, if a plan has 3 pair of disks to copy between , and the first disk set encounters more than 5 errors, then we abandon the first copy and start the second copy in the plan. The default value of max errors is set to 5.|
|`dfs.disk.balancer.block.tolerance.percent`| The tolerance percent specifies when we have reached a good enough value for any copy step. For example, if you specify 10% then getting close to 10% of the target value is good enough.|
|`dfs.disk.balancer.plan.threshold.percent`| The percentage threshold value for volume Data Density in a plan. If the absolute value of volume Data Density which is out of threshold value in a node, it means that the volumes corresponding to the disks should do the balancing in the plan. The default value is 10.|
|`dfs.disk.balancer.plan.valid.interval`| Maximum amount of time disk balancer plan is valid. Supports the following suffixes (case insensitive): ms(millis), s(sec), m(min), h(hour), d(day) to specify the time (such as 2s, 2m, 1h, etc.). If no suffix is specified then milliseconds is assumed. Default value is 1d|
 Debugging
---------

Disk balancer generates two output files. The nodename.before.json contains
the state of cluster that we read from the  namenode. This file
contains detailed information about  datanodes and volumes.

if you plan to post this file to an apache JIRA, you might want to
replace your hostnames and volume paths since it may leak your personal
information.

You can also trim this file down to focus only on the nodes that you want to
report in the JIRA.

The nodename.plan.json contains the plan for the specific node. This plan
file contains as a series of steps. A step is executed as a series of move
operations inside the datanode.

To diff the state of a node before and after, you can either re-run a plan
command and diff the new nodename.before.json with older before.json or run
report command against the node.

To see the progress of a running plan, please run query command with option -v.
This will print out a set of steps -- Each step represents a move operation
from one disk to another.

The speed of move is limited by the bandwidth that is specified. The default
value of bandwidth is set to 10MB/sec. if you do a query with -v option you
will see the following values.


      "sourcePath" : "/data/disk2/hdfs/dn",

      "destPath" : "/data/disk3/hdfs/dn",

      "workItem" :

        "startTime" : 1466575335493,

        "secondsElapsed" : 16486,

        "bytesToCopy" : 181242049353,

        "bytesCopied" : 172655116288,

        "errorCount" : 0,

        "errMsg" : null,

        "blocksCopied" : 1287,

        "maxDiskErrors" : 5,

        "tolerancePercent" : 10,

        "bandwidth" : 10


 *source path* - is the volume we are copying from.

 *dest path* - is the volume to where we are copying to.

 *start time* - is current time in milliseconds.

 *seconds elapsed* - is updated whenever we update the stats. This might be
 slower than the wall clock time.

 *bytes to copy* - is number of bytes we are supposed to copy. We copy plus or
 minus a certain percentage. So often you will see bytesCopied -- as a value
 lesser than bytes to copy. In the default case, getting within 10% of bytes
 to move is considered good enough.

 *bytes copied* - is the actual number of bytes that we moved from source disk to
 destination disk.

 *error count* - Each time we encounter an error we will increment the error
 count. As long as error count remains less than max error count (default
 value is 5), we will try to complete this move. if we hit the max error count
 we will abandon this current step and execute the next step in the plan.

 *error message* - Currently a single string that reports the last error message.
 Older messages should be in the datanode log.


 *blocks copied* - Number of blocks copied.

 *max disk errors* - The configuration used for this move step. currently it will
 report the default config value, since the user interface to control these
 values per step is not in place. It is a future work item. The default or
 the command line value specified in plan command is used for this value.

*tolerance percent* - This represents how much off we can be while moving data.
In a busy cluster this allows admin to say, compute a plan, but I know this
node is being used so it is okay if disk balancer can reach +/- 10% of the
bytes to be copied.

*bandwidth* - This is the maximum aggregate source disk bandwidth used by the
disk balancer. After moving a block disk balancer computes how many seconds it
should have taken to move that block with the specified bandwidth. If the
actual move took less time than expected, then disk balancer will sleep for
that duration. Please note that currently all moves are executed
sequentially by a single thread.
