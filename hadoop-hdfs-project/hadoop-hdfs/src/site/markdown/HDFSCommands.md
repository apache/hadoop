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

HDFS Commands Guide
===================

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

Overview
--------

All HDFS commands are invoked by the `bin/hdfs` script. Running the hdfs script without any arguments prints the description for all commands.

Usage: `hdfs [SHELL_OPTIONS] COMMAND [GENERIC_OPTIONS] [COMMAND_OPTIONS]`

Hadoop has an option parsing framework that employs parsing generic options as well as running classes.

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| SHELL\_OPTIONS | The common set of shell options. These are documented on the [Commands Manual](../../hadoop-project-dist/hadoop-common/CommandsManual.html#Shell_Options) page. |
| GENERIC\_OPTIONS | The common set of options supported by multiple commands. See the Hadoop [Commands Manual](../../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options) for more information. |
| COMMAND COMMAND\_OPTIONS | Various commands with their options are described in the following sections. The commands have been grouped into [User Commands](#User_Commands) and [Administration Commands](#Administration_Commands). |

User Commands
-------------

Commands useful for users of a hadoop cluster.

### `classpath`

Usage: `hdfs classpath [--glob |--jar <path> |-h |--help]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `--glob` | expand wildcards |
| `--jar` *path* | write classpath as manifest in jar named *path* |
| `-h`, `--help` | print help |

Prints the class path needed to get the Hadoop jar and the required libraries. If called without arguments, then prints the classpath set up by the command scripts, which is likely to contain wildcards in the classpath entries. Additional options print the classpath after wildcard expansion or write the classpath into the manifest of a jar file. The latter is useful in environments where wildcards cannot be used and the expanded classpath exceeds the maximum supported command line length.

### `dfs`

Usage: `hdfs dfs [COMMAND [COMMAND_OPTIONS]]`

Run a filesystem command on the file system supported in Hadoop. The various COMMAND\_OPTIONS can be found at [File System Shell Guide](../hadoop-common/FileSystemShell.html).

### `envvars`

Usage: `hdfs envvars`

display computed Hadoop environment variables.

### `fetchdt`

Usage: `hdfs fetchdt <opts> <token_file_path> `

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `--webservice` *NN_Url* | Url to contact NN on (starts with http or https)|
| `--renewer` *name* | Name of the delegation token renewer |
| `--cancel` | Cancel the delegation token |
| `--renew` | Renew the delegation token.  Delegation token must have been fetched using the --renewer *name* option.|
| `--print` | Print the delegation token |
| *token_file_path* | File path to store the token into. |

Gets Delegation Token from a NameNode. See [fetchdt](./HdfsUserGuide.html#fetchdt) for more info.

### `fsck`

Usage:

       hdfs fsck <path>
              [-list-corruptfileblocks |
              [-move | -delete | -openforwrite]
              [-files [-blocks [-locations | -racks | -replicaDetails | -upgradedomains]]]
              [-includeSnapshots] [-showprogress]
              [-storagepolicies] [-maintenance]
              [-blockId <blk_Id>] [-replicate]

| COMMAND\_OPTION | Description |
|:---- |:---- |
| *path* | Start checking from this path. |
| `-delete` | Delete corrupted files. |
| `-files` | Print out files being checked. |
| `-files` `-blocks` | Print out the block report |
| `-files` `-blocks` `-locations` | Print out locations for every block. |
| `-files` `-blocks` `-racks` | Print out network topology for data-node locations. |
| `-files` `-blocks` `-replicaDetails` | Print out each replica details. |
| `-files` `-blocks` `-upgradedomains` | Print out upgrade domains for every block. |
| `-includeSnapshots` | Include snapshot data if the given path indicates a snapshottable directory or there are snapshottable directories under it. |
| `-list-corruptfileblocks` | Print out list of missing blocks and files they belong to. |
| `-move` | Move corrupted files to /lost+found. |
| `-openforwrite` | Print out files opened for write. |
| `-showprogress` | Print out dots for progress in output. Default is OFF (no progress). |
| `-storagepolicies` | Print out storage policy summary for the blocks. |
| `-maintenance` | Print out maintenance state node details. |
| `-blockId` | Print out information about the block. |
| `-replicate` | Initiate replication work to make mis-replicated blocks satisfy block placement policy. |

Runs the HDFS filesystem checking utility. See [fsck](./HdfsUserGuide.html#fsck) for more info.

### `getconf`

Usage:

       hdfs getconf -namenodes
       hdfs getconf -secondaryNameNodes
       hdfs getconf -backupNodes
       hdfs getconf -journalNodes
       hdfs getconf -includeFile
       hdfs getconf -excludeFile
       hdfs getconf -nnRpcAddresses
       hdfs getconf -confKey [key]

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-namenodes` | gets list of namenodes in the cluster. |
| `-secondaryNameNodes` | gets list of secondary namenodes in the cluster. |
| `-backupNodes` | gets list of backup nodes in the cluster. |
| `-journalNodes` | gets list of journal nodes in the cluster. |
| `-includeFile` | gets the include file path that defines the datanodes that can join the cluster. |
| `-excludeFile` | gets the exclude file path that defines the datanodes that need to decommissioned. |
| `-nnRpcAddresses` | gets the namenode rpc addresses |
| `-confKey` [key] | gets a specific key from the configuration |

Gets configuration information from the configuration directory, post-processing.

### `groups`

Usage: `hdfs groups [username ...]`

Returns the group information given one or more usernames.

### `httpfs`

Usage: `hdfs httpfs`

Run HttpFS server, the HDFS HTTP Gateway.

### `lsSnapshottableDir`

Usage: `hdfs lsSnapshottableDir [-help]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-help` | print help |

Get the list of snapshottable directories. When this is run as a super user, it returns all snapshottable directories. Otherwise it returns those directories that are owned by the current user.

### `jmxget`

Usage: `hdfs jmxget [-localVM ConnectorURL | -port port | -server mbeanserver | -service service]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-help` | print help |
| `-localVM` ConnectorURL | connect to the VM on the same machine |
| `-port` *mbean server port* | specify mbean server port, if missing it will try to connect to MBean Server in the same VM |
| `-server` | specify mbean server (localhost by default) |
| `-service` NameNode\|DataNode | specify jmx service. NameNode by default. |

Dump JMX information from a service.

### `oev`

Usage: `hdfs oev [OPTIONS] -i INPUT_FILE -o OUTPUT_FILE`

#### Required command line arguments:

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-i`,`--inputFile` *arg* | edits file to process, xml (case insensitive) extension means XML format, any other filename means binary format |
| `-o`,`--outputFile` *arg* | Name of output file. If the specified file exists, it will be overwritten, format of the file is determined by -p option |

#### Optional command line arguments:

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-f`,`--fix-txids` | Renumber the transaction IDs in the input, so that there are no gaps or invalid transaction IDs. |
| `-h`,`--help` | Display usage information and exit |
| `-r`,`--recover` | When reading binary edit logs, use recovery mode. This will give you the chance to skip corrupt parts of the edit log. |
| `-p`,`--processor` *arg* | Select which type of processor to apply against image file, currently supported processors are: binary (native binary format that Hadoop uses), xml (default, XML format), stats (prints statistics about edits file) |
| `-v`,`--verbose` | More verbose output, prints the input and output filenames, for processors that write to a file, also output to screen. On large image files this will dramatically increase processing time (default is false). |

Hadoop offline edits viewer. See [Offline Edits Viewer Guide](./HdfsEditsViewer.html) for more info.

### `oiv`

Usage: `hdfs oiv [OPTIONS] -i INPUT_FILE`

#### Required command line arguments:

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-i`\|`--inputFile` *input file* | Specify the input fsimage file (or XML file, if ReverseXML processor is used) to process. |


#### Optional command line arguments:

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-o`,`--outputFile` *output file* | Specify the output filename, if the specified output processor generates one. If the specified file already exists, it is silently overwritten. (output to stdout by default) If the input file is an XML file, it also creates an &lt;outputFile&gt;.md5. |
| `-p`,`--processor` *processor* | Specify the image processor to apply against the image file. Currently valid options are `Web` (default), `XML`, `Delimited`, `FileDistribution` and `ReverseXML`. |
| `-addr` *address* | Specify the address(host:port) to listen. (localhost:5978 by default). This option is used with Web processor. |
| `-maxSize` *size* | Specify the range [0, maxSize] of file sizes to be analyzed in bytes (128GB by default). This option is used with FileDistribution processor. |
| `-step` *size* | Specify the granularity of the distribution in bytes (2MB by default). This option is used with FileDistribution processor. |
| `-format` | Format the output result in a human-readable fashion rather than a number of bytes. (false by default). This option is used with FileDistribution processor. |
| `-delimiter` *arg* | Delimiting string to use with Delimited processor. |
| `-t`,`--temp` *temporary dir* | Use temporary dir to cache intermediate result to generate Delimited outputs. If not set, Delimited processor constructs the namespace in memory before outputting text. |
| `-h`,`--help` | Display the tool usage and help information and exit. |


Hadoop Offline Image Viewer for image files in Hadoop 2.4 or up. See [Offline Image Viewer Guide](./HdfsImageViewer.html) for more info.

### `oiv_legacy`

Usage: `hdfs oiv_legacy [OPTIONS] -i INPUT_FILE -o OUTPUT_FILE`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-i`,`--inputFile` *input file* | Specify the input fsimage file to process. |
| `-o`,`--outputFile` *output file* | Specify the output filename, if the specified output processor generates one. If the specified file already exists, it is silently overwritten. |

#### Optional command line arguments:

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-p`\|`--processor` *processor* | Specify the image processor to apply against the image file. Valid options are Ls (default), XML, Delimited, Indented, FileDistribution and NameDistribution. |
| `-maxSize` *size* | Specify the range [0, maxSize] of file sizes to be analyzed in bytes (128GB by default). This option is used with FileDistribution processor. |
| `-step` *size* | Specify the granularity of the distribution in bytes (2MB by default). This option is used with FileDistribution processor. |
| `-format` | Format the output result in a human-readable fashion rather than a number of bytes. (false by default). This option is used with FileDistribution processor. |
| `-skipBlocks` | Do not enumerate individual blocks within files. This may save processing time and outfile file space on namespaces with very large files. The Ls processor reads the blocks to correctly determine file sizes and ignores this option. |
| `-printToScreen` | Pipe output of processor to console as well as specified file. On extremely large namespaces, this may increase processing time by an order of magnitude. |
| `-delimiter` *arg* | When used in conjunction with the Delimited processor, replaces the default tab delimiter with the string specified by *arg*. |
| `-h`\|`--help` | Display the tool usage and help information and exit. |


Hadoop offline image viewer for older versions of Hadoop. See [oiv\_legacy Command](./HdfsImageViewer.html#oiv_legacy_Command) for more info.

### `snapshotDiff`

Usage: `hdfs snapshotDiff <path> <fromSnapshot> <toSnapshot> `

Determine the difference between HDFS snapshots. See the [HDFS Snapshot Documentation](./HdfsSnapshots.html#Get_Snapshots_Difference_Report) for more information.

### `version`

Usage: `hdfs version`

Prints the version.

Administration Commands
-----------------------

Commands useful for administrators of a hadoop cluster.

### `balancer`

Usage:

        hdfs balancer
              [-policy <policy>]
              [-threshold <threshold>]
              [-exclude [-f <hosts-file> | <comma-separated list of hosts>]]
              [-include [-f <hosts-file> | <comma-separated list of hosts>]]
              [-source [-f <hosts-file> | <comma-separated list of hosts>]]
              [-blockpools <comma-separated list of blockpool ids>]
              [-idleiterations <idleiterations>]
              [-runDuringUpgrade]

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-policy` \<policy\> | `datanode` (default): Cluster is balanced if each datanode is balanced.<br/> `blockpool`: Cluster is balanced if each block pool in each datanode is balanced. |
| `-threshold` \<threshold\> | Percentage of disk capacity. This overwrites the default threshold. |
| `-exclude -f` \<hosts-file\> \| \<comma-separated list of hosts\> | Excludes the specified datanodes from being balanced by the balancer. |
| `-include -f` \<hosts-file\> \| \<comma-separated list of hosts\> | Includes only the specified datanodes to be balanced by the balancer. |
| `-source -f` \<hosts-file\> \| \<comma-separated list of hosts\> | Pick only the specified datanodes as source nodes. |
| `-blockpools` \<comma-separated list of blockpool ids\> | The balancer will only run on blockpools included in this list. |
| `-idleiterations` \<iterations\> | Maximum number of idle iterations before exit. This overwrites the default idleiterations(5). |
| `-runDuringUpgrade` | Whether to run the balancer during an ongoing HDFS upgrade. This is usually not desired since it will not affect used space on over-utilized machines. |
| `-h`\|`--help` | Display the tool usage and help information and exit. |

Runs a cluster balancing utility. An administrator can simply press Ctrl-C to stop the rebalancing process. See [Balancer](./HdfsUserGuide.html#Balancer) for more details.

Note that the `blockpool` policy is more strict than the `datanode` policy.

Besides the above command options, a pinning feature is introduced starting from 2.7.0 to prevent certain replicas from getting moved by balancer/mover. This pinning feature is disabled by default, and can be enabled by configuration property "dfs.datanode.block-pinning.enabled". When enabled, this feature only affects blocks that are written to favored nodes specified in the create() call. This feature is useful when we want to maintain the data locality, for applications such as HBase regionserver.

### `cacheadmin`

Usage:

    hdfs cacheadmin [-addDirective -path <path> -pool <pool-name> [-force] [-replication <replication>] [-ttl <time-to-live>]]
    hdfs cacheadmin [-modifyDirective -id <id> [-path <path>] [-force] [-replication <replication>] [-pool <pool-name>] [-ttl <time-to-live>]]
    hdfs cacheadmin [-listDirectives [-stats] [-path <path>] [-pool <pool>] [-id <id>]]
    hdfs cacheadmin [-removeDirective <id>]
    hdfs cacheadmin [-removeDirectives -path <path>]
    hdfs cacheadmin [-addPool <name> [-owner <owner>] [-group <group>] [-mode <mode>] [-limit <limit>] [-maxTtl <maxTtl>]]
    hdfs cacheadmin [-modifyPool <name> [-owner <owner>] [-group <group>] [-mode <mode>] [-limit <limit>] [-maxTtl <maxTtl>]]
    hdfs cacheadmin [-removePool <name>]
    hdfs cacheadmin [-listPools [-stats] [<name>]]
    hdfs cacheadmin [-help <command-name>]

See the [HDFS Cache Administration Documentation](./CentralizedCacheManagement.html#cacheadmin_command-line_interface) for more information.

### `crypto`

Usage:

      hdfs crypto -createZone -keyName <keyName> -path <path>
      hdfs crypto -listZones
      hdfs crypto -provisionTrash -path <path>
      hdfs crypto -help <command-name>

See the [HDFS Transparent Encryption Documentation](./TransparentEncryption.html#crypto_command-line_interface) for more information.

### `datanode`

Usage: `hdfs datanode [-regular | -rollback | -rollingupgrade rollback]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-regular` | Normal datanode startup (default). |
| `-rollback` | Rollback the datanode to the previous version. This should be used after stopping the datanode and distributing the old hadoop version. |
| `-rollingupgrade` rollback | Rollback a rolling upgrade operation. |

Runs a HDFS datanode.

### `dfsadmin`

Usage:

        hdfs dfsadmin [-report [-live] [-dead] [-decommissioning] [-enteringmaintenance] [-inmaintenance]]
        hdfs dfsadmin [-safemode enter | leave | get | wait | forceExit]
        hdfs dfsadmin [-saveNamespace [-beforeShutdown]]
        hdfs dfsadmin [-rollEdits]
        hdfs dfsadmin [-restoreFailedStorage true |false |check]
        hdfs dfsadmin [-refreshNodes]
        hdfs dfsadmin [-setQuota <quota> <dirname>...<dirname>]
        hdfs dfsadmin [-clrQuota <dirname>...<dirname>]
        hdfs dfsadmin [-setSpaceQuota <quota> [-storageType <storagetype>] <dirname>...<dirname>]
        hdfs dfsadmin [-clrSpaceQuota [-storageType <storagetype>] <dirname>...<dirname>]
        hdfs dfsadmin [-finalizeUpgrade]
        hdfs dfsadmin [-rollingUpgrade [<query> |<prepare> |<finalize>]]
        hdfs dfsadmin [-upgrade [query | finalize]
        hdfs dfsadmin [-refreshServiceAcl]
        hdfs dfsadmin [-refreshUserToGroupsMappings]
        hdfs dfsadmin [-refreshSuperUserGroupsConfiguration]
        hdfs dfsadmin [-refreshCallQueue]
        hdfs dfsadmin [-refresh <host:ipc_port> <key> [arg1..argn]]
        hdfs dfsadmin [-reconfig <namenode|datanode> <host:ipc_port> <start |status |properties>]
        hdfs dfsadmin [-printTopology]
        hdfs dfsadmin [-refreshNamenodes datanodehost:port]
        hdfs dfsadmin [-getVolumeReport datanodehost:port]
        hdfs dfsadmin [-deleteBlockPool datanode-host:port blockpoolId [force]]
        hdfs dfsadmin [-setBalancerBandwidth <bandwidth in bytes per second>]
        hdfs dfsadmin [-getBalancerBandwidth <datanode_host:ipc_port>]
        hdfs dfsadmin [-fetchImage <local directory>]
        hdfs dfsadmin [-allowSnapshot <snapshotDir>]
        hdfs dfsadmin [-disallowSnapshot <snapshotDir>]
        hdfs dfsadmin [-shutdownDatanode <datanode_host:ipc_port> [upgrade]]
        hdfs dfsadmin [-evictWriters <datanode_host:ipc_port>]
        hdfs dfsadmin [-getDatanodeInfo <datanode_host:ipc_port>]
        hdfs dfsadmin [-metasave filename]
        hdfs dfsadmin [-triggerBlockReport [-incremental] <datanode_host:ipc_port>]
        hdfs dfsadmin [-listOpenFiles [-blockingDecommission] [-path <path>]]
        hdfs dfsadmin [-help [cmd]]

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-report` `[-live]` `[-dead]` `[-decommissioning]` `[-enteringmaintenance]` `[-inmaintenance]` | Reports basic filesystem information and statistics, The dfs usage can be different from "du" usage, because it measures raw space used by replication, checksums, snapshots and etc. on all the DNs. Optional flags may be used to filter the list of displayed DataNodes. |
| `-safemode` enter\|leave\|get\|wait\|forceExit | Safe mode maintenance command. Safe mode is a Namenode state in which it <br/>1. does not accept changes to the name space (read-only) <br/>2. does not replicate or delete blocks. <br/>Safe mode is entered automatically at Namenode startup, and leaves safe mode automatically when the configured minimum percentage of blocks satisfies the minimum replication condition. If Namenode detects any anomaly then it will linger in safe mode till that issue is resolved. If that anomaly is the consequence of a deliberate action, then administrator can use -safemode forceExit to exit safe mode. The cases where forceExit may be required are<br/> 1. Namenode metadata is not consistent. If Namenode detects that metadata has been modified out of band and can cause data loss, then Namenode will enter forceExit state. At that point user can either restart Namenode with correct metadata files or forceExit (if data loss is acceptable).<br/>2. Rollback causes metadata to be replaced and rarely it can trigger safe mode forceExit state in Namenode. In that case you may proceed by issuing -safemode forceExit.<br/> Safe mode can also be entered manually, but then it can only be turned off manually as well. |
| `-saveNamespace` `[-beforeShutdown]` | Save current namespace into storage directories and reset edits log. Requires safe mode. If the "beforeShutdown" option is given, the NameNode does a checkpoint if and only if no checkpoint has been done during a time window (a configurable number of checkpoint periods). This is usually used before shutting down the NameNode to prevent potential fsimage/editlog corruption. |
| `-rollEdits` | Rolls the edit log on the active NameNode. |
| `-restoreFailedStorage` true\|false\|check | This option will turn on/off automatic attempt to restore failed storage replicas. If a failed storage becomes available again the system will attempt to restore edits and/or fsimage during checkpoint. 'check' option will return current setting. |
| `-refreshNodes` | Re-read the hosts and exclude files to update the set of Datanodes that are allowed to connect to the Namenode and those that should be decommissioned or recommissioned. |
| `-setQuota` \<quota\> \<dirname\>...\<dirname\> | See [HDFS Quotas Guide](../hadoop-hdfs/HdfsQuotaAdminGuide.html#Administrative_Commands) for the detail. |
| `-clrQuota` \<dirname\>...\<dirname\> | See [HDFS Quotas Guide](../hadoop-hdfs/HdfsQuotaAdminGuide.html#Administrative_Commands) for the detail. |
| `-setSpaceQuota` \<quota\> `[-storageType <storagetype>]` \<dirname\>...\<dirname\> | See [HDFS Quotas Guide](../hadoop-hdfs/HdfsQuotaAdminGuide.html#Administrative_Commands) for the detail. |
| `-clrSpaceQuota` `[-storageType <storagetype>]` \<dirname\>...\<dirname\> | See [HDFS Quotas Guide](../hadoop-hdfs/HdfsQuotaAdminGuide.html#Administrative_Commands) for the detail. |
| `-finalizeUpgrade` | Finalize upgrade of HDFS. Datanodes delete their previous version working directories, followed by Namenode doing the same. This completes the upgrade process. |
| `-rollingUpgrade` [\<query\>\|\<prepare\>\|\<finalize\>] | See [Rolling Upgrade document](../hadoop-hdfs/HdfsRollingUpgrade.html#dfsadmin_-rollingUpgrade) for the detail. |
| `-upgrade` query\|finalize | Query the current upgrade status.<br/>Finalize upgrade of HDFS (equivalent to -finalizeUpgrade). |
| `-refreshServiceAcl` | Reload the service-level authorization policy file. |
| `-refreshUserToGroupsMappings` | Refresh user-to-groups mappings. |
| `-refreshSuperUserGroupsConfiguration` | Refresh superuser proxy groups mappings |
| `-refreshCallQueue` | Reload the call queue from config. |
| `-refresh` \<host:ipc\_port\> \<key\> [arg1..argn] | Triggers a runtime-refresh of the resource specified by \<key\> on \<host:ipc\_port\>. All other args after are sent to the host. |
| `-reconfig` \<datanode \|namenode\> \<host:ipc\_port\> \<start\|status\|properties\> | Starts reconfiguration or gets the status of an ongoing reconfiguration, or gets a list of reconfigurable properties. The second parameter specifies the node type. |
| `-printTopology` | Print a tree of the racks and their nodes as reported by the Namenode |
| `-refreshNamenodes` datanodehost:port | For the given datanode, reloads the configuration files, stops serving the removed block-pools and starts serving new block-pools. |
| `-getVolumeReport` datanodehost:port | For the given datanode, get the volume report. |
| `-deleteBlockPool` datanode-host:port blockpoolId [force] | If force is passed, block pool directory for the given blockpool id on the given datanode is deleted along with its contents, otherwise the directory is deleted only if it is empty. The command will fail if datanode is still serving the block pool. Refer to refreshNamenodes to shutdown a block pool service on a datanode. |
| `-setBalancerBandwidth` \<bandwidth in bytes per second\> | Changes the network bandwidth used by each datanode during HDFS block balancing. \<bandwidth\> is the maximum number of bytes per second that will be used by each datanode. This value overrides the dfs.datanode.balance.bandwidthPerSec parameter. NOTE: The new value is not persistent on the DataNode. |
| `-getBalancerBandwidth` \<datanode\_host:ipc\_port\> | Get the network bandwidth(in bytes per second) for the given datanode. This is the maximum network bandwidth used by the datanode during HDFS block balancing.|
| `-fetchImage` \<local directory\> | Downloads the most recent fsimage from the NameNode and saves it in the specified local directory. |
| `-allowSnapshot` \<snapshotDir\> | Allowing snapshots of a directory to be created. If the operation completes successfully, the directory becomes snapshottable. See the [HDFS Snapshot Documentation](./HdfsSnapshots.html) for more information. |
| `-disallowSnapshot` \<snapshotDir\> | Disallowing snapshots of a directory to be created. All snapshots of the directory must be deleted before disallowing snapshots. See the [HDFS Snapshot Documentation](./HdfsSnapshots.html) for more information. |
| `-shutdownDatanode` \<datanode\_host:ipc\_port\> [upgrade] | Submit a shutdown request for the given datanode. See [Rolling Upgrade document](./HdfsRollingUpgrade.html#dfsadmin_-shutdownDatanode) for the detail. |
| `-evictWriters` \<datanode\_host:ipc\_port\> | Make the datanode evict all clients that are writing a block. This is useful if decommissioning is hung due to slow writers. |
| `-getDatanodeInfo` \<datanode\_host:ipc\_port\> | Get the information about the given datanode. See [Rolling Upgrade document](./HdfsRollingUpgrade.html#dfsadmin_-getDatanodeInfo) for the detail. |
| `-metasave` filename | Save Namenode's primary data structures to *filename* in the directory specified by hadoop.log.dir property. *filename* is overwritten if it exists. *filename* will contain one line for each of the following<br/>1. Datanodes heart beating with Namenode<br/>2. Blocks waiting to be replicated<br/>3. Blocks currently being replicated<br/>4. Blocks waiting to be deleted |
| `-triggerBlockReport` `[-incremental]` \<datanode\_host:ipc\_port\> | Trigger a block report for the given datanode. If 'incremental' is specified, it will be otherwise, it will be a full block report. |
| `-listOpenFiles` `[-blockingDecommission]` `[-path <path>]` | List all open files currently managed by the NameNode along with client name and client machine accessing them. Open files list will be filtered by given type and path. |
| `-help` [cmd] | Displays help for the given command or all commands if none is specified. |

Runs a HDFS dfsadmin client.

### `dfsrouter`

Usage: `hdfs dfsrouter`

Runs the DFS router. See [Router](../hadoop-hdfs-rbf/HDFSRouterFederation.html#Router) for more info.

### `dfsrouteradmin`

Usage:

      hdfs dfsrouteradmin
          [-add <source> <nameservice1, nameservice2, ...> <destination> [-readonly] [-order HASH|LOCAL|RANDOM|HASH_ALL] -owner <owner> -group <group> -mode <mode>]
          [-update <source> <nameservice1, nameservice2, ...> <destination> [-readonly] [-order HASH|LOCAL|RANDOM|HASH_ALL] -owner <owner> -group <group> -mode <mode>]
          [-rm <source>]
          [-ls <path>]
          [-setQuota <path> -nsQuota <nsQuota> -ssQuota <quota in bytes or quota size string>]
          [-clrQuota <path>]
          [-safemode enter | leave | get]
          [-nameservice disable | enable <nameservice>]
          [-getDisabledNameservices]

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-add` *source* *nameservices* *destination* | Add a mount table entry or update if it exists. |
| `-update` *source* *nameservices* *destination* | Update a mount table entry or create one if it does not exist. |
| `-rm` *source* | Remove mount point of specified path. |
| `-ls` *path* | List mount points under specified path. |
| `-setQuota` *path* `-nsQuota` *nsQuota* `-ssQuota` *ssQuota* | Set quota for specified path. See [HDFS Quotas Guide](./HdfsQuotaAdminGuide.html) for the quota detail. |
| `-clrQuota` *path* | Clear quota of given mount point. See [HDFS Quotas Guide](./HdfsQuotaAdminGuide.html) for the quota detail. |
| `-safemode` `enter` `leave` `get` | Manually set the Router entering or leaving safe mode. The option *get* will be used for verifying if the Router is in safe mode state. |
| `-nameservice` `disable` `enable` *nameservice* | Disable/enable  a name service from the federation. If disabled, requests will not go to that name service. |
| `-getDisabledNameservices` | Get the name services that are disabled in the federation. |

The commands for managing Router-based federation. See [Mount table management](../hadoop-hdfs-rbf/HDFSRouterFederation.html#Mount_table_management) for more info.

### `diskbalancer`

Usage:

       hdfs diskbalancer
         [-plan <datanode> -fs <namenodeURI>]
         [-execute <planfile>]
         [-query <datanode>]
         [-cancel <planfile>]
         [-cancel <planID> -node <datanode>]
         [-report -node <file://> | [<DataNodeID|IP|Hostname>,...]]
         [-report -node -top <topnum>]

| COMMAND\_OPTION | Description |
|:---- |:---- |
|-plan| Creates a disbalancer plan|
|-execute| Executes a given plan on a datanode|
|-query| Gets the current diskbalancer status from a datanode|
|-cancel| Cancels a running plan|
|-report| Reports the volume information from datanode(s)|

Runs the diskbalancer CLI. See [HDFS Diskbalancer](./HDFSDiskbalancer.html) for more information on this command.

### `ec`

Usage:

       hdfs ec [generic options]
         [-setPolicy -policy <policyName> -path <path>]
         [-getPolicy -path <path>]
         [-unsetPolicy -path <path>]
         [-listPolicies]
         [-addPolicies -policyFile <file>]
         [-listCodecs]
         [-enablePolicy -policy <policyName>]
         [-disablePolicy -policy <policyName>]
         [-verifyClusterSetup -policy <policyName>...<policyName>]
         [-help [cmd ...]]

| COMMAND\_OPTION | Description |
|:---- |:---- |
|-setPolicy| Set a specified ErasureCoding policy to a directory|
|-getPolicy| Get ErasureCoding policy information about a specified path|
|-unsetPolicy| Unset an ErasureCoding policy set by a previous call to "setPolicy" on a directory |
|-listPolicies| Lists all supported ErasureCoding policies|
|-addPolicies| Add a list of erasure coding policies|
|-listCodecs| Get the list of supported erasure coding codecs and coders in system|
|-enablePolicy| Enable an ErasureCoding policy in system|
|-disablePolicy| Disable an ErasureCoding policy in system|
|-verifyClusterSetup| Verify if the cluster setup can support a list of erasure coding policies|

Runs the ErasureCoding CLI. See [HDFS ErasureCoding](./HDFSErasureCoding.html#Administrative_commands) for more information on this command.

### `haadmin`

Usage:

        hdfs haadmin -transitionToActive <serviceId> [--forceactive]
        hdfs haadmin -transitionToStandby <serviceId>
        hdfs haadmin -transitionToObserver <serviceId>
        hdfs haadmin -failover [--forcefence] [--forceactive] <serviceId> <serviceId>
        hdfs haadmin -getServiceState <serviceId>
        hdfs haadmin -getAllServiceState
        hdfs haadmin -checkHealth <serviceId>
        hdfs haadmin -help <command>


| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-checkHealth` | check the health of the given NameNode |
| `-failover` | initiate a failover between two NameNodes |
| `-getServiceState` | determine whether the given NameNode is Active or Standby |
| `-getAllServiceState` | returns the state of all the NameNodes | |
| `-transitionToActive` | transition the state of the given NameNode to Active (Warning: No fencing is done) |
| `-transitionToStandby` | transition the state of the given NameNode to Standby (Warning: No fencing is done) |
| `-transitionToObserver` | transition the state of the given NameNode to Observer (Warning: No fencing is done) |
| `-help` [cmd] | Displays help for the given command or all commands if none is specified. |

See [HDFS HA with NFS](./HDFSHighAvailabilityWithNFS.html#Administrative_commands) or [HDFS HA with QJM](./HDFSHighAvailabilityWithQJM.html#Administrative_commands) for more information on this command.

### `journalnode`

Usage: `hdfs journalnode`

This comamnd starts a journalnode for use with [HDFS HA with QJM](./HDFSHighAvailabilityWithQJM.html#Administrative_commands).

### `mover`

Usage: `hdfs mover [-p <files/dirs> | -f <local file name>]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-f` \<local file\> | Specify a local file containing a list of HDFS files/dirs to migrate. |
| `-p` \<files/dirs\> | Specify a space separated list of HDFS files/dirs to migrate. |

Runs the data migration utility. See [Mover](./ArchivalStorage.html#Mover_-_A_New_Data_Migration_Tool) for more details.

Note that, when both -p and -f options are omitted, the default path is the root directory.

In addition, a pinning feature is introduced starting from 2.7.0 to prevent certain replicas from getting moved by balancer/mover. This pinning feature is disabled by default, and can be enabled by configuration property "dfs.datanode.block-pinning.enabled". When enabled, this feature only affects blocks that are written to favored nodes specified in the create() call. This feature is useful when we want to maintain the data locality, for applications such as HBase regionserver.

### `namenode`

Usage:

      hdfs namenode [-backup] |
              [-checkpoint] |
              [-format [-clusterid cid ] [-force] [-nonInteractive] ] |
              [-upgrade [-clusterid cid] [-renameReserved<k-v pairs>] ] |
              [-upgradeOnly [-clusterid cid] [-renameReserved<k-v pairs>] ] |
              [-rollback] |
              [-rollingUpgrade <rollback |started> ] |
              [-importCheckpoint] |
              [-initializeSharedEdits] |
              [-bootstrapStandby [-force] [-nonInteractive] [-skipSharedEditsCheck] ] |
              [-recover [-force] ] |
              [-metadataVersion ]

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-backup` | Start backup node. |
| `-checkpoint` | Start checkpoint node. |
| `-format` `[-clusterid cid]` | Formats the specified NameNode. It starts the NameNode, formats it and then shut it down. Will throw NameNodeFormatException if name dir already exist and if reformat is disabled for cluster. |
| `-upgrade` `[-clusterid cid]` [`-renameReserved` \<k-v pairs\>] | Namenode should be started with upgrade option after the distribution of new Hadoop version. |
| `-upgradeOnly` `[-clusterid cid]` [`-renameReserved` \<k-v pairs\>] | Upgrade the specified NameNode and then shutdown it. |
| `-rollback` | Rollback the NameNode to the previous version. This should be used after stopping the cluster and distributing the old Hadoop version. |
| `-rollingUpgrade` \<rollback\|started\> | See [Rolling Upgrade document](./HdfsRollingUpgrade.html#NameNode_Startup_Options) for the detail. |
| `-importCheckpoint` | Loads image from a checkpoint directory and save it into the current one. Checkpoint dir is read from property dfs.namenode.checkpoint.dir |
| `-initializeSharedEdits` | Format a new shared edits dir and copy in enough edit log segments so that the standby NameNode can start up. |
| `-bootstrapStandby` `[-force]` `[-nonInteractive]` `[-skipSharedEditsCheck]` | Allows the standby NameNode's storage directories to be bootstrapped by copying the latest namespace snapshot from the active NameNode. This is used when first configuring an HA cluster. The option -force or -nonInteractive has the same meaning as that described in namenode -format command. -skipSharedEditsCheck option skips edits check which ensures that we have enough edits already in the shared directory to start up from the last checkpoint on the active. |
| `-recover` `[-force]` | Recover lost metadata on a corrupt filesystem. See [HDFS User Guide](./HdfsUserGuide.html#Recovery_Mode) for the detail. |
| `-metadataVersion` | Verify that configured directories exist, then print the metadata versions of the software and the image. |

Runs the namenode. More info about the upgrade and rollback is at [Upgrade Rollback](./HdfsUserGuide.html#Upgrade_and_Rollback).

### `nfs3`

Usage: `hdfs nfs3`

This comamnd starts the NFS3 gateway for use with the [HDFS NFS3 Service](./HdfsNfsGateway.html#Start_and_stop_NFS_gateway_service).

### `portmap`

Usage: `hdfs portmap`

This comamnd starts the RPC portmap for use with the [HDFS NFS3 Service](./HdfsNfsGateway.html#Start_and_stop_NFS_gateway_service).

### `secondarynamenode`

Usage: `hdfs secondarynamenode [-checkpoint [force]] | [-format] | [-geteditsize]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-checkpoint` [force] | Checkpoints the SecondaryNameNode if EditLog size \>= fs.checkpoint.size. If `force` is used, checkpoint irrespective of EditLog size. |
| `-format` | Format the local storage during startup. |
| `-geteditsize` | Prints the number of uncheckpointed transactions on the NameNode. |

Runs the HDFS secondary namenode. See [Secondary Namenode](./HdfsUserGuide.html#Secondary_NameNode) for more info.

### `storagepolicies`

Usage:

      hdfs storagepolicies
          [-listPolicies]
          [-setStoragePolicy -path <path> -policy <policy>]
          [-getStoragePolicy -path <path>]
          [-unsetStoragePolicy -path <path>]
          [-satisfyStoragePolicy -path <path>]
          [-isSatisfierRunning]
          [-help <command-name>]

Lists out all/Gets/sets/unsets storage policies. See the [HDFS Storage Policy Documentation](./ArchivalStorage.html) for more information.

### `zkfc`

Usage: `hdfs zkfc [-formatZK [-force] [-nonInteractive]]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-formatZK` | Format the Zookeeper instance. -force: formats the znode if the znode exists. -nonInteractive: formats the znode aborts if the znode exists, unless -force option is specified. |
| `-h` | Display help |

This comamnd starts a Zookeeper Failover Controller process for use with [HDFS HA with QJM](./HDFSHighAvailabilityWithQJM.html#Administrative_commands).

Debug Commands
--------------

Useful commands to help administrators debug HDFS issues. These commands are for advanced users only.

### `verifyMeta`

Usage: `hdfs debug verifyMeta -meta <metadata-file> [-block <block-file>]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-block` *block-file* | Optional parameter to specify the absolute path for the block file on the local file system of the data node. |
| `-meta` *metadata-file* | Absolute path for the metadata file on the local file system of the data node. |

Verify HDFS metadata and block files. If a block file is specified, we will verify that the checksums in the metadata file match the block file.

### `computeMeta`

Usage: `hdfs debug computeMeta -block <block-file> -out <output-metadata-file>`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `-block` *block-file* | Absolute path for the block file on the local file system of the data node. |
| `-out` *output-metadata-file* | Absolute path for the output metadata file to store the checksum computation result from the block file. |

Compute HDFS metadata from block files. If a block file is specified, we will compute the checksums from the block file, and save it to the specified output metadata file.

**NOTE**: Use at your own risk! If the block file is corrupt and you overwrite it's meta file, it will show up as 'good' in HDFS, but you can't read the data. Only use as a last measure, and when you are 100% certain the block file is good.

### `recoverLease`

Usage: `hdfs debug recoverLease -path <path> [-retries <num-retries>]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| [`-path` *path*] | HDFS path for which to recover the lease. |
| [`-retries` *num-retries*] | Number of times the client will retry calling recoverLease. The default number of retries is 1. |

Recover the lease on the specified path. The path must reside on an HDFS filesystem. The default number of retries is 1.
