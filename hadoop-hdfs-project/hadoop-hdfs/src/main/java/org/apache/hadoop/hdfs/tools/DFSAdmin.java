/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeVolumeInfo;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.ReconfigurationProtocol;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

/**
 * This class provides some DFS administrative access shell commands.
 */
@InterfaceAudience.Private
public class DFSAdmin extends FsShell {

  static {
    HdfsConfiguration.init();
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(DFSAdmin.class);

  /**
   * An abstract class for the execution of a file system command
   */
  abstract private static class DFSAdminCommand extends Command {
    protected DistributedFileSystem dfs;
    /** Constructor */
    public DFSAdminCommand(Configuration conf) {
      super(conf);
    }

    @Override
    public void run(PathData pathData) throws IOException {
      FileSystem fs = pathData.fs;
      if (!(fs instanceof DistributedFileSystem)) {
        throw new IllegalArgumentException("FileSystem " + fs.getUri()
            + " is not an HDFS file system");
      }
      this.dfs = (DistributedFileSystem) fs;
      run(pathData.path);
    }
  }
  
  /** A class that supports command clearQuota */
  private static class ClearQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "clrQuota";
    private static final String USAGE = "-"+NAME+" <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the quota for each directory <dirName>.\n" +
    "\t\tFor each directory, attempt to clear the quota. An error will be reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** Constructor */
    ClearQuotaCommand(String[] args, int pos, Configuration conf) {
      super(conf);
      CommandFormat c = new CommandFormat(1, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the clrQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_DONT_SET);
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname>";
    private static final String DESCRIPTION =
        "-setQuota <quota> <dirname>...<dirname>: " +
        "Set the quota <quota> for each directory <dirName>.\n" +
        "\t\tThe directory quota is a long integer that puts a hard limit\n" +
        "\t\ton the number of names in the directory tree\n" +
        "\t\tFor each directory, attempt to set the quota. An error will be reported if\n" +
        "\t\t1. quota is not a positive integer, or\n" +
        "\t\t2. User is not an administrator, or\n" +
        "\t\t3. The directory does not exist or is a file.\n" +
        "\t\tNote: A quota of 1 would force the directory to remain empty.\n";

    private final long quota; // the quota to be set

    /** Constructor */
    SetQuotaCommand(String[] args, int pos, Configuration conf) {
      super(conf);
      CommandFormat c = new CommandFormat(2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.quota =
          StringUtils.TraditionalBinaryPrefix.string2long(parameters.remove(0));
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, quota, HdfsConstants.QUOTA_DONT_SET);
    }
  }
  
  /** A class that supports command clearSpaceQuota */
  private static class ClearSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "clrSpaceQuota";
    private static final String USAGE = "-"+NAME+" [-storageType <storagetype>] <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
        "Clear the space quota for each directory <dirName>.\n" +
        "\t\tFor each directory, attempt to clear the quota. " +
        "An error will be reported if\n" +
        "\t\t1. the directory does not exist or is a file, or\n" +
        "\t\t2. user is not an administrator.\n" +
        "\t\tIt does not fault if the directory has no quota.\n" +
        "\t\tThe storage type specific quota is cleared when -storageType " +
        "option is specified.\n" +
        "\t\tAvailable storageTypes are \n" +
        "\t\t- RAM_DISK\n" +
        "\t\t- DISK\n" +
        "\t\t- SSD\n" +
        "\t\t- ARCHIVE";


    private StorageType type;

    /** Constructor */
    ClearSpaceQuotaCommand(String[] args, int pos, Configuration conf) {
      super(conf);
      CommandFormat c = new CommandFormat(1, Integer.MAX_VALUE);
      c.addOptionWithValue("storageType");
      List<String> parameters = c.parse(args, pos);
      String storageTypeString = c.getOptValue("storageType");
      if (storageTypeString != null) {
        this.type = StorageType.parseStorageType(storageTypeString);
      }
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the clrQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      if (type != null) {
        dfs.setQuotaByStorageType(path, type, HdfsConstants.QUOTA_RESET);
      } else {
        dfs.setQuota(path, HdfsConstants.QUOTA_DONT_SET, HdfsConstants.QUOTA_RESET);
      }
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setSpaceQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> [-storageType <storagetype>] <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
        "Set the space quota <quota> for each directory <dirName>.\n" +
        "\t\tThe space quota is a long integer that puts a hard limit\n" +
        "\t\ton the total size of all the files under the directory tree.\n" +
        "\t\tThe extra space required for replication is also counted. E.g.\n" +
        "\t\ta 1GB file with replication of 3 consumes 3GB of the quota.\n\n" +
        "\t\tQuota can also be specified with a binary prefix for terabytes,\n" +
        "\t\tpetabytes etc (e.g. 50t is 50TB, 5m is 5MB, 3p is 3PB).\n" +
        "\t\tFor each directory, attempt to set the quota. An error will be reported if\n" +
        "\t\t1. quota is not a positive integer or zero, or\n" +
        "\t\t2. user is not an administrator, or\n" +
        "\t\t3. the directory does not exist or is a file.\n" +
        "\t\tThe storage type specific quota is set when -storageType option is specified.\n" +
        "\t\tAvailable storageTypes are \n" +
        "\t\t- RAM_DISK\n" +
        "\t\t- DISK\n" +
        "\t\t- SSD\n" +
        "\t\t- ARCHIVE";

    private long quota; // the quota to be set
    private StorageType type;
    
    /** Constructor */
    SetSpaceQuotaCommand(String[] args, int pos, Configuration conf) {
      super(conf);
      CommandFormat c = new CommandFormat(2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      String str = parameters.remove(0).trim();
      try {
        quota = StringUtils.TraditionalBinaryPrefix.string2long(str);
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("\"" + str + "\" is not a valid value for a quota.");
      }
      String storageTypeString =
          StringUtils.popOptionWithArgument("-storageType", parameters);
      if (storageTypeString != null) {
        try {
          this.type = StorageType.parseStorageType(storageTypeString);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Storage type "
              + storageTypeString
              + " is not available. Available storage types are "
              + StorageType.getTypesSupportingQuota());
        }
      }
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      if (type != null) {
        dfs.setQuotaByStorageType(path, type, quota);
      } else {
        dfs.setQuota(path, HdfsConstants.QUOTA_DONT_SET, quota);
      }
    }
  }

  private static class RollingUpgradeCommand {
    static final String NAME = "rollingUpgrade";
    static final String USAGE = "-"+NAME+" [<query|prepare|finalize>]";
    static final String DESCRIPTION = USAGE + ":\n"
        + "     query: query the current rolling upgrade status.\n"
        + "   prepare: prepare a new rolling upgrade.\n"
        + "  finalize: finalize the current rolling upgrade.";

    /** Check if a command is the rollingUpgrade command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    private static void printMessage(RollingUpgradeInfo info,
        PrintStream out) {
      if (info != null && info.isStarted()) {
        if (!info.createdRollbackImages() && !info.isFinalized()) {
          out.println(
              "Preparing for upgrade. Data is being saved for rollback."
              + "\nRun \"dfsadmin -rollingUpgrade query\" to check the status"
              + "\nfor proceeding with rolling upgrade");
            out.println(info);
        } else if (!info.isFinalized()) {
          out.println("Proceed with rolling upgrade:");
          out.println(info);
        } else {
          out.println("Rolling upgrade is finalized.");
          out.println(info);
        }
      } else {
        out.println("There is no rolling upgrade in progress or rolling " +
            "upgrade has already been finalized.");
      }
    }

    static int run(DistributedFileSystem dfs, String[] argv, int idx) throws IOException {
      final RollingUpgradeAction action = RollingUpgradeAction.fromString(
          argv.length >= 2? argv[1]: "");
      if (action == null) {
        throw new IllegalArgumentException("Failed to convert \"" + argv[1]
            +"\" to " + RollingUpgradeAction.class.getSimpleName());
      }

      System.out.println(action + " rolling upgrade ...");

      final RollingUpgradeInfo info = dfs.rollingUpgrade(action);
      switch(action){
      case QUERY:
        break;
      case PREPARE:
        Preconditions.checkState(info.isStarted());
        break;
      case FINALIZE:
        Preconditions.checkState(info == null || info.isFinalized());
        break;
      }
      printMessage(info, System.out);
      return 0;
    }
  }

  /**
   * Common usage summary shared between "hdfs dfsadmin -help" and
   * "hdfs dfsadmin"
   */
  private static final String commonUsageSummary =
    "\t[-report [-live] [-dead] [-decommissioning] " +
    "[-enteringmaintenance] [-inmaintenance]]\n" +
    "\t[-safemode <enter | leave | get | wait>]\n" +
    "\t[-saveNamespace [-beforeShutdown]]\n" +
    "\t[-rollEdits]\n" +
    "\t[-restoreFailedStorage true|false|check]\n" +
    "\t[-refreshNodes]\n" +
    "\t[" + SetQuotaCommand.USAGE + "]\n" +
    "\t[" + ClearQuotaCommand.USAGE +"]\n" +
    "\t[" + SetSpaceQuotaCommand.USAGE + "]\n" +
    "\t[" + ClearSpaceQuotaCommand.USAGE +"]\n" +
    "\t[-finalizeUpgrade]\n" +
    "\t[" + RollingUpgradeCommand.USAGE +"]\n" +
    "\t[-upgrade <query | finalize>]\n" +
    "\t[-refreshServiceAcl]\n" +
    "\t[-refreshUserToGroupsMappings]\n" +
    "\t[-refreshSuperUserGroupsConfiguration]\n" +
    "\t[-refreshCallQueue]\n" +
    "\t[-refresh <host:ipc_port> <key> [arg1..argn]\n" +
    "\t[-reconfig <namenode|datanode> <host:ipc_port> " +
      "<start|status|properties>]\n" +
    "\t[-printTopology]\n" +
      "\t[-refreshNamenodes datanode_host:ipc_port]\n" +
      "\t[-getVolumeReport datanode_host:ipc_port]\n" +
    "\t[-deleteBlockPool datanode_host:ipc_port blockpoolId [force]]\n"+
    "\t[-setBalancerBandwidth <bandwidth in bytes per second>]\n" +
    "\t[-getBalancerBandwidth <datanode_host:ipc_port>]\n" +
    "\t[-fetchImage <local directory>]\n" +
    "\t[-allowSnapshot <snapshotDir>]\n" +
    "\t[-disallowSnapshot <snapshotDir>]\n" +
    "\t[-shutdownDatanode <datanode_host:ipc_port> [upgrade]]\n" +
    "\t[-evictWriters <datanode_host:ipc_port>]\n" +
    "\t[-getDatanodeInfo <datanode_host:ipc_port>]\n" +
    "\t[-metasave filename]\n" +
    "\t[-triggerBlockReport [-incremental] <datanode_host:ipc_port>]\n" +
    "\t[-listOpenFiles [-blockingDecommission] [-path <path>]]\n" +
    "\t[-help [cmd]]\n";

  /**
   * Construct a DFSAdmin object.
   */
  public DFSAdmin() {
    this(new HdfsConfiguration());
  }

  /**
   * Construct a DFSAdmin object.
   */
  public DFSAdmin(Configuration conf) {
    super(conf);
  }
  
  protected DistributedFileSystem getDFS() throws IOException {
    FileSystem fs = getFS();
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs.getUri() + 
      " is not an HDFS file system");
    }
    return (DistributedFileSystem)fs;
  }
  
  /**
   * Gives a report on how the FileSystem is doing.
   * @exception IOException if the filesystem does not exist.
   */
  public void report(String[] argv, int i) throws IOException {
    DistributedFileSystem dfs = getDFS();
    FsStatus ds = dfs.getStatus();
    long capacity = ds.getCapacity();
    long used = ds.getUsed();
    long remaining = ds.getRemaining();
    long bytesInFuture = dfs.getBytesWithFutureGenerationStamps();
    long presentCapacity = used + remaining;
    boolean mode = dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET);
    if (mode) {
      System.out.println("Safe mode is ON");
      if (bytesInFuture > 0) {
        System.out.println("\nWARNING: ");
        System.out.println("Name node has detected blocks with generation " +
            "stamps in future.");
        System.out.println("Forcing exit from safemode will cause " +
            bytesInFuture + " byte(s) to be deleted.");
        System.out.println("If you are sure that the NameNode was started with"
            + " the correct metadata files then you may proceed with " +
            "'-safemode forceExit'\n");
      }
    }
    System.out.println("Configured Capacity: " + capacity
                       + " (" + StringUtils.byteDesc(capacity) + ")");
    System.out.println("Present Capacity: " + presentCapacity
        + " (" + StringUtils.byteDesc(presentCapacity) + ")");
    System.out.println("DFS Remaining: " + remaining
        + " (" + StringUtils.byteDesc(remaining) + ")");
    System.out.println("DFS Used: " + used
                       + " (" + StringUtils.byteDesc(used) + ")");
    double dfsUsedPercent = 0;
    if (presentCapacity != 0) {
      dfsUsedPercent = used/(double)presentCapacity;
    }
    System.out.println("DFS Used%: "
        + StringUtils.formatPercent(dfsUsedPercent, 2));

    /* These counts are not always upto date. They are updated after  
     * iteration of an internal list. Should be updated in a few seconds to 
     * minutes. Use "-metaSave" to list of all such blocks and accurate 
     * counts.
     */
    ReplicatedBlockStats replicatedBlockStats =
        dfs.getClient().getNamenode().getReplicatedBlockStats();
    System.out.println("Replicated Blocks:");
    System.out.println("\tUnder replicated blocks: " +
        replicatedBlockStats.getLowRedundancyBlocks());
    System.out.println("\tBlocks with corrupt replicas: " +
        replicatedBlockStats.getCorruptBlocks());
    System.out.println("\tMissing blocks: " +
        replicatedBlockStats.getMissingReplicaBlocks());
    System.out.println("\tMissing blocks (with replication factor 1): " +
        replicatedBlockStats.getMissingReplicationOneBlocks());
    if (replicatedBlockStats.hasHighestPriorityLowRedundancyBlocks()) {
      System.out.println("\tLow redundancy blocks with highest priority " +
          "to recover: " +
          replicatedBlockStats.getHighestPriorityLowRedundancyBlocks());
    }
    System.out.println("\tPending deletion blocks: " +
        replicatedBlockStats.getPendingDeletionBlocks());

    ECBlockGroupStats ecBlockGroupStats =
        dfs.getClient().getNamenode().getECBlockGroupStats();
    System.out.println("Erasure Coded Block Groups: ");
    System.out.println("\tLow redundancy block groups: " +
        ecBlockGroupStats.getLowRedundancyBlockGroups());
    System.out.println("\tBlock groups with corrupt internal blocks: " +
        ecBlockGroupStats.getCorruptBlockGroups());
    System.out.println("\tMissing block groups: " +
        ecBlockGroupStats.getMissingBlockGroups());
    if (ecBlockGroupStats.hasHighestPriorityLowRedundancyBlocks()) {
      System.out.println("\tLow redundancy blocks with highest priority " +
          "to recover: " +
          ecBlockGroupStats.getHighestPriorityLowRedundancyBlocks());
    }
    System.out.println("\tPending deletion blocks: " +
        ecBlockGroupStats.getPendingDeletionBlocks());

    System.out.println();

    System.out.println("-------------------------------------------------");
    
    // Parse arguments for filtering the node list
    List<String> args = Arrays.asList(argv);
    // Truncate already handled arguments before parsing report()-specific ones
    args = new ArrayList<String>(args.subList(i, args.size()));
    final boolean listLive = StringUtils.popOption("-live", args);
    final boolean listDead = StringUtils.popOption("-dead", args);
    final boolean listDecommissioning =
        StringUtils.popOption("-decommissioning", args);
    final boolean listEnteringMaintenance =
        StringUtils.popOption("-enteringmaintenance", args);
    final boolean listInMaintenance =
        StringUtils.popOption("-inmaintenance", args);


    // If no filter flags are found, then list all DN types
    boolean listAll = (!listLive && !listDead && !listDecommissioning
        && !listEnteringMaintenance && !listInMaintenance);

    if (listAll || listLive) {
      printDataNodeReports(dfs, DatanodeReportType.LIVE, listLive, "Live");
    }

    if (listAll || listDead) {
      printDataNodeReports(dfs, DatanodeReportType.DEAD, listDead, "Dead");
    }

    if (listAll || listDecommissioning) {
      printDataNodeReports(dfs, DatanodeReportType.DECOMMISSIONING,
          listDecommissioning, "Decommissioning");
    }

    if (listAll || listEnteringMaintenance) {
      printDataNodeReports(dfs, DatanodeReportType.ENTERING_MAINTENANCE,
          listEnteringMaintenance, "Entering maintenance");
    }

    if (listAll || listInMaintenance) {
      printDataNodeReports(dfs, DatanodeReportType.IN_MAINTENANCE,
          listInMaintenance, "In maintenance");
    }
  }

  private static void printDataNodeReports(DistributedFileSystem dfs,
      DatanodeReportType type, boolean listNodes, String nodeState)
      throws IOException {
    DatanodeInfo[] nodes = dfs.getDataNodeStats(type);
    if (nodes.length > 0 || listNodes) {
      System.out.println(nodeState + " datanodes (" + nodes.length + "):\n");
    }
    if (nodes.length > 0) {
      for (DatanodeInfo dn : nodes) {
        System.out.println(dn.getDatanodeReport());
        System.out.println();
      }
    }
  }

  /**
   * Safe mode maintenance command.
   * Usage: hdfs dfsadmin -safemode [enter | leave | get | wait | forceExit]
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException if the filesystem does not exist.
   */
  public void setSafeMode(String[] argv, int idx) throws IOException {
    if (idx != argv.length - 1) {
      printUsage("-safemode");
      return;
    }
    HdfsConstants.SafeModeAction action;
    Boolean waitExitSafe = false;

    if ("leave".equalsIgnoreCase(argv[idx])) {
      action = HdfsConstants.SafeModeAction.SAFEMODE_LEAVE;
    } else if ("enter".equalsIgnoreCase(argv[idx])) {
      action = HdfsConstants.SafeModeAction.SAFEMODE_ENTER;
    } else if ("get".equalsIgnoreCase(argv[idx])) {
      action = HdfsConstants.SafeModeAction.SAFEMODE_GET;
    } else if ("wait".equalsIgnoreCase(argv[idx])) {
      action = HdfsConstants.SafeModeAction.SAFEMODE_GET;
      waitExitSafe = true;
    } else if ("forceExit".equalsIgnoreCase(argv[idx])){
      action = HdfsConstants.SafeModeAction.SAFEMODE_FORCE_EXIT;
    } else {
      printUsage("-safemode");
      return;
    }

    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(
          dfsConf, nsId, ClientProtocol.class);
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        ClientProtocol haNn = proxy.getProxy();
        boolean inSafeMode = haNn.setSafeMode(action, false);
        if (waitExitSafe) {
          inSafeMode = waitExitSafeMode(haNn, inSafeMode);
        }
        System.out.println("Safe mode is " + (inSafeMode ? "ON" : "OFF")
            + " in " + proxy.getAddress());
      }
    } else {
      boolean inSafeMode = dfs.setSafeMode(action);
      if (waitExitSafe) {
        inSafeMode = waitExitSafeMode(dfs, inSafeMode);
      }
      System.out.println("Safe mode is " + (inSafeMode ? "ON" : "OFF"));
    }

  }

  private boolean waitExitSafeMode(DistributedFileSystem dfs, boolean inSafeMode)
      throws IOException {
    while (inSafeMode) {
      try {
        Thread.sleep(5000);
      } catch (java.lang.InterruptedException e) {
        throw new IOException("Wait Interrupted");
      }
      inSafeMode = dfs.setSafeMode(SafeModeAction.SAFEMODE_GET, false);
    }
    return inSafeMode;
  }

  private boolean waitExitSafeMode(ClientProtocol nn, boolean inSafeMode)
      throws IOException {
    while (inSafeMode) {
      try {
        Thread.sleep(5000);
      } catch (java.lang.InterruptedException e) {
        throw new IOException("Wait Interrupted");
      }
      inSafeMode = nn.setSafeMode(SafeModeAction.SAFEMODE_GET, false);
    }
    return inSafeMode;
  }

  public int triggerBlockReport(String[] argv) throws IOException {
    List<String> args = new LinkedList<String>();
    for (int j = 1; j < argv.length; j++) {
      args.add(argv[j]);
    }
    boolean incremental = StringUtils.popOption("-incremental", args);
    String hostPort = StringUtils.popFirstNonOption(args);
    if (hostPort == null) {
      System.err.println("You must specify a host:port pair.");
      return 1;
    }
    if (!args.isEmpty()) {
      System.err.print("Can't understand arguments: " +
        Joiner.on(" ").join(args) + "\n");
      return 1;
    }
    ClientDatanodeProtocol dnProxy = getDataNodeProxy(hostPort);
    try {
      dnProxy.triggerBlockReport(
          new BlockReportOptions.Factory().
              setIncremental(incremental).
              build());
    } catch (IOException e) {
      System.err.println("triggerBlockReport error: " + e);
      return 1;
    }
    System.out.println("Triggering " +
        (incremental ? "an incremental " : "a full ") +
        "block report on " + hostPort + ".");
    return 0;
  }

  /**
   * Allow snapshot on a directory.
   * Usage: hdfs dfsadmin -allowSnapshot snapshotDir
   * @param argv List of of command line parameters.
   * @exception IOException
   */
  public void allowSnapshot(String[] argv) throws IOException {
    Path p = new Path(argv[1]);
    final DistributedFileSystem dfs = AdminHelper.getDFS(p.toUri(), getConf());
    try {
      dfs.allowSnapshot(p);
    } catch (SnapshotException e) {
      throw new RemoteException(e.getClass().getName(), e.getMessage());
    }
    System.out.println("Allowing snapshot on " + argv[1] + " succeeded");
  }
  
  /**
   * Disallow snapshot on a directory.
   * Usage: hdfs dfsadmin -disallowSnapshot snapshotDir
   * @param argv List of of command line parameters.
   * @exception IOException
   */
  public void disallowSnapshot(String[] argv) throws IOException {
    Path p = new Path(argv[1]);
    final DistributedFileSystem dfs = AdminHelper.getDFS(p.toUri(), getConf());
    try {
      dfs.disallowSnapshot(p);
    } catch (SnapshotException e) {
      throw new RemoteException(e.getClass().getName(), e.getMessage());
    }
    System.out.println("Disallowing snapshot on " + argv[1] + " succeeded");
  }
  
  /**
   * Command to ask the namenode to save the namespace.
   * Usage: hdfs dfsadmin -saveNamespace
   * @see ClientProtocol#saveNamespace(long, long)
   */
  public int saveNamespace(String[] argv) throws IOException {
    final DistributedFileSystem dfs = getDFS();
    final Configuration dfsConf = dfs.getConf();

    long timeWindow = 0;
    long txGap = 0;
    if (argv.length > 1 && "-beforeShutdown".equals(argv[1])) {
      final long checkpointPeriod = dfsConf.getTimeDuration(
          DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY,
          DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT,
          TimeUnit.SECONDS);
      final long checkpointTxnCount = dfsConf.getLong(
          DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
          DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
      final int toleratePeriodNum = dfsConf.getInt(
          DFSConfigKeys.DFS_NAMENODE_MISSING_CHECKPOINT_PERIODS_BEFORE_SHUTDOWN_KEY,
          DFSConfigKeys.DFS_NAMENODE_MISSING_CHECKPOINT_PERIODS_BEFORE_SHUTDOWN_DEFAULT);
      timeWindow = checkpointPeriod * toleratePeriodNum;
      txGap = checkpointTxnCount * toleratePeriodNum;
      System.out.println("Do checkpoint if necessary before stopping " +
          "namenode. The time window is " + timeWindow + " seconds, and the " +
          "transaction gap is " + txGap);
    }

    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);
    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        try{
          boolean saved = proxy.getProxy().saveNamespace(timeWindow, txGap);
          if (saved) {
            System.out.println("Save namespace successful for " +
                proxy.getAddress());
          } else {
            System.out.println("No extra checkpoint has been made for "
                + proxy.getAddress());
          }
        }catch (IOException ioe){
          System.out.println("Save namespace failed for " +
              proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      boolean saved = dfs.saveNamespace(timeWindow, txGap);
      if (saved) {
        System.out.println("Save namespace successful");
      } else {
        System.out.println("No extra checkpoint has been made");
      }
    }
    return 0;
  }

  public int rollEdits() throws IOException {
    DistributedFileSystem dfs = getDFS();
    long txid = dfs.rollEdits();
    System.out.println("Successfully rolled edit logs.");
    System.out.println("New segment starts at txid " + txid);
    return 0;
  }
  
  /**
   * Command to enable/disable/check restoring of failed storage replicas in the namenode.
   * Usage: hdfs dfsadmin -restoreFailedStorage true|false|check
   * @exception IOException 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#restoreFailedStorage(String arg)
   */
  public int restoreFailedStorage(String arg) throws IOException {
    int exitCode = -1;
    if(!arg.equals("check") && !arg.equals("true") && !arg.equals("false")) {
      System.err.println("restoreFailedStorage valid args are true|false|check");
      return exitCode;
    }
    
    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        try{
          Boolean res = proxy.getProxy().restoreFailedStorage(arg);
          System.out.println("restoreFailedStorage is set to " + res + " for "
              + proxy.getAddress());
        } catch (IOException ioe){
          System.out.println("restoreFailedStorage failed for "
              + proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      Boolean res = dfs.restoreFailedStorage(arg);
      System.out.println("restoreFailedStorage is set to " + res);
    }
    exitCode = 0;

    return exitCode;
  }

  /**
   * Command to ask the namenode to reread the hosts and excluded hosts 
   * file.
   * Usage: hdfs dfsadmin -refreshNodes
   * @exception IOException 
   */
  public int refreshNodes() throws IOException {
    int exitCode = -1;

    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<ClientProtocol> proxy: proxies) {
        try{
          proxy.getProxy().refreshNodes();
          System.out.println("Refresh nodes successful for " +
              proxy.getAddress());
        }catch (IOException ioe){
          System.out.println("Refresh nodes failed for " +
              proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      dfs.refreshNodes();
      System.out.println("Refresh nodes successful");
    }
    exitCode = 0;
   
    return exitCode;
  }

  /**
   * Command to list all the open files currently managed by NameNode.
   * Usage: hdfs dfsadmin -listOpenFiles
   *
   * @throws IOException
   * @param argv
   */
  public int listOpenFiles(String[] argv) throws IOException {
    String path = null;
    List<OpenFilesType> types = new ArrayList<>();
    if (argv != null) {
      List<String> args = new ArrayList<>(Arrays.asList(argv));
      if (StringUtils.popOption("-blockingDecommission", args)) {
        types.add(OpenFilesType.BLOCKING_DECOMMISSION);
      }

      path = StringUtils.popOptionWithArgument("-path", args);
    }
    if (types.isEmpty()) {
      types.add(OpenFilesType.ALL_OPEN_FILES);
    }

    if (path != null) {
      path = path.trim();
      if (path.length() == 0) {
        path = OpenFilesIterator.FILTER_PATH_DEFAULT;
      }
    } else {
      path = OpenFilesIterator.FILTER_PATH_DEFAULT;
    }

    EnumSet<OpenFilesType> openFilesTypes = EnumSet.copyOf(types);

    DistributedFileSystem dfs = getDFS();
    RemoteIterator<OpenFileEntry> openFilesRemoteIterator;
    try{
      openFilesRemoteIterator = dfs.listOpenFiles(openFilesTypes, path);
      printOpenFiles(openFilesRemoteIterator);
    } catch (IOException ioe){
      System.out.println("List open files failed.");
      throw ioe;
    }
    return 0;
  }

  private void printOpenFiles(RemoteIterator<OpenFileEntry> openFilesIterator)
      throws IOException {
    System.out.println(String.format("%-20s\t%-20s\t%s", "Client Host",
          "Client Name", "Open File Path"));
    while (openFilesIterator.hasNext()) {
      OpenFileEntry openFileEntry = openFilesIterator.next();
      System.out.println(String.format("%-20s\t%-20s\t%20s",
          openFileEntry.getClientMachine(),
          openFileEntry.getClientName(),
          openFileEntry.getFilePath()));
    }
  }

  /**
   * Command to ask the active namenode to set the balancer bandwidth.
   * Usage: hdfs dfsadmin -setBalancerBandwidth bandwidth
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException 
   */
  public int setBalancerBandwidth(String[] argv, int idx) throws IOException {
    long bandwidth;
    int exitCode = -1;

    try {
      bandwidth = StringUtils.TraditionalBinaryPrefix.string2long(argv[idx]);
    } catch (NumberFormatException nfe) {
      System.err.println("NumberFormatException: " + nfe.getMessage());
      System.err.println("Usage: hdfs dfsadmin"
                  + " [-setBalancerBandwidth <bandwidth in bytes per second>]");
      return exitCode;
    }

    if (bandwidth < 0) {
      System.err.println("Bandwidth should be a non-negative integer");
      return exitCode;
    }

    FileSystem fs = getFS();
    if (!(fs instanceof DistributedFileSystem)) {
      System.err.println("FileSystem is " + fs.getUri());
      return exitCode;
    }

    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    try{
      dfs.setBalancerBandwidth(bandwidth);
      System.out.println("Balancer bandwidth is set to " + bandwidth);
    } catch (IOException ioe){
      System.err.println("Balancer bandwidth is set failed.");
      throw ioe;
    }
    exitCode = 0;

    return exitCode;
  }

  /**
   * Command to get balancer bandwidth for the given datanode. Usage: hdfs
   * dfsadmin -getBalancerBandwidth {@literal <datanode_host:ipc_port>}
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException
   */
  public int getBalancerBandwidth(String[] argv, int idx) throws IOException {
    ClientDatanodeProtocol dnProxy = getDataNodeProxy(argv[idx]);
    try {
      long bandwidth = dnProxy.getBalancerBandwidth();
      System.out.println("Balancer bandwidth is " + bandwidth
          + " bytes per second.");
    } catch (IOException ioe) {
      throw new IOException("Datanode unreachable. " + ioe, ioe);
    }
    return 0;
  }

  /**
   * Download the most recent fsimage from the name node, and save it to a local
   * file in the given directory.
   * 
   * @param argv
   *          List of of command line parameters.
   * @param idx
   *          The index of the command that is being processed.
   * @return an exit code indicating success or failure.
   * @throws IOException
   */
  public int fetchImage(final String[] argv, final int idx) throws IOException {
    Configuration conf = getConf();
    final URL infoServer = DFSUtil.getInfoServer(
        HAUtil.getAddressOfActive(getDFS()), conf,
        DFSUtil.getHttpClientScheme(conf)).toURL();
    SecurityUtil.doAsCurrentUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        TransferFsImage.downloadMostRecentImageToDirectory(infoServer,
            new File(argv[idx]));
        return null;
      }
    });
    return 0;
  }

  private void printHelp(String cmd) {
    String summary = "hdfs dfsadmin performs DFS administrative commands.\n" +
      "Note: Administrative commands can only be run with superuser permission.\n" +
      "The full syntax is: \n\n" +
      "hdfs dfsadmin\n" +
      commonUsageSummary;

    String report ="-report [-live] [-dead] [-decommissioning] "
        + "[-enteringmaintenance] [-inmaintenance]:\n" +
        "\tReports basic filesystem information and statistics. \n" +
        "\tThe dfs usage can be different from \"du\" usage, because it\n" +
        "\tmeasures raw space used by replication, checksums, snapshots\n" +
        "\tand etc. on all the DNs.\n" +
        "\tOptional flags may be used to filter the list of displayed DNs.\n";

    String safemode = "-safemode <enter|leave|get|wait|forceExit>:  Safe mode " +
        "maintenance command.\n" +
      "\t\tSafe mode is a Namenode state in which it\n" +
      "\t\t\t1.  does not accept changes to the name space (read-only)\n" +
      "\t\t\t2.  does not replicate or delete blocks.\n" +
      "\t\tSafe mode is entered automatically at Namenode startup, and\n" +
      "\t\tleaves safe mode automatically when the configured minimum\n" +
      "\t\tpercentage of blocks satisfies the minimum replication\n" +
      "\t\tcondition.  Safe mode can also be entered manually, but then\n" +
      "\t\tit can only be turned off manually as well.\n";

    String saveNamespace = "-saveNamespace [-beforeShutdown]:\t" +
        "Save current namespace into storage directories and reset edits \n" +
        "\t\t log. Requires safe mode.\n" +
        "\t\tIf the \"beforeShutdown\" option is given, the NameNode does a \n" +
        "\t\tcheckpoint if and only if there is no checkpoint done during \n" +
        "\t\ta time window (a configurable number of checkpoint periods).\n" +
        "\t\tThis is usually used before shutting down the NameNode to \n" +
        "\t\tprevent potential fsimage/editlog corruption.\n";

    String rollEdits = "-rollEdits:\t" +
    "Rolls the edit log.\n";
    
    String restoreFailedStorage = "-restoreFailedStorage:\t" +
    "Set/Unset/Check flag to attempt restore of failed storage replicas if they become available.\n";
    
    String refreshNodes = "-refreshNodes: \tUpdates the namenode with the " +
      "set of datanodes allowed to connect to the namenode.\n\n" +
      "\t\tNamenode re-reads datanode hostnames from the file defined by \n" +
      "\t\tdfs.hosts, dfs.hosts.exclude configuration parameters.\n" +
      "\t\tHosts defined in dfs.hosts are the datanodes that are part of \n" +
      "\t\tthe cluster. If there are entries in dfs.hosts, only the hosts \n" +
      "\t\tin it are allowed to register with the namenode.\n\n" +
      "\t\tEntries in dfs.hosts.exclude are datanodes that need to be \n" +
      "\t\tdecommissioned. Datanodes complete decommissioning when \n" + 
      "\t\tall the replicas from them are replicated to other datanodes.\n" +
      "\t\tDecommissioned nodes are not automatically shutdown and \n" +
      "\t\tare not chosen for writing new replicas.\n";

    String finalizeUpgrade = "-finalizeUpgrade: Finalize upgrade of HDFS.\n" +
      "\t\tDatanodes delete their previous version working directories,\n" +
      "\t\tfollowed by Namenode doing the same.\n" + 
      "\t\tThis completes the upgrade process.\n";

    String upgrade = "-upgrade <query | finalize>:\n"
        + "     query: query the current upgrade status.\n"
        + "  finalize: finalize the upgrade of HDFS (equivalent to " +
        "-finalizeUpgrade.";

    String metaSave = "-metasave <filename>: \tSave Namenode's primary data structures\n" +
      "\t\tto <filename> in the directory specified by hadoop.log.dir property.\n" +
      "\t\t<filename> is overwritten if it exists.\n" +
      "\t\t<filename> will contain one line for each of the following\n" +
      "\t\t\t1. Datanodes heart beating with Namenode\n" +
      "\t\t\t2. Blocks waiting to be replicated\n" +
      "\t\t\t3. Blocks currrently being replicated\n" +
      "\t\t\t4. Blocks waiting to be deleted\n";

    String refreshServiceAcl = "-refreshServiceAcl: Reload the service-level authorization policy file\n" +
      "\t\tNamenode will reload the authorization policy file.\n";
    
    String refreshUserToGroupsMappings = 
      "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";
    
    String refreshSuperUserGroupsConfiguration = 
      "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";

    String refreshCallQueue = "-refreshCallQueue: Reload the call queue from config\n";

    String reconfig = "-reconfig <namenode|datanode> <host:ipc_port> " +
        "<start|status|properties>:\n" +
        "\tStarts or gets the status of a reconfiguration operation, \n" +
        "\tor gets a list of reconfigurable properties.\n" +

        "\tThe second parameter specifies the node type\n";
    String genericRefresh = "-refresh: Arguments are <hostname:ipc_port>" +
            " <resource_identifier> [arg1..argn]\n" +
            "\tTriggers a runtime-refresh of the resource specified by " +
            "<resource_identifier> on <hostname:ipc_port>.\n" +
            "\tAll other args after are sent to the host.\n" +
            "\tThe ipc_port is determined by 'dfs.datanode.ipc.address'," +
            "default is DFS_DATANODE_IPC_DEFAULT_PORT.\n";

    String printTopology = "-printTopology: Print a tree of the racks and their\n" +
                           "\t\tnodes as reported by the Namenode\n";
    
    String refreshNamenodes = "-refreshNamenodes: Takes a " +
            "datanodehost:ipc_port as argument,For the given datanode\n" +
            "\t\treloads the configuration files,stops serving the removed\n" +
            "\t\tblock-pools and starts serving new block-pools.\n" +
            "\t\tThe ipc_port is determined by 'dfs.datanode.ipc.address'," +
            "default is DFS_DATANODE_IPC_DEFAULT_PORT.\n";
    
    String getVolumeReport = "-getVolumeReport: Takes a datanodehost:ipc_port"+
            " as argument,For the given datanode,get the volume report.\n" +
            "\t\tThe ipc_port is determined by 'dfs.datanode.ipc.address'," +
            "default is DFS_DATANODE_IPC_DEFAULT_PORT.\n";

    String deleteBlockPool = "-deleteBlockPool: Arguments are " +
            "datanodehost:ipc_port, blockpool id and an optional argument\n" +
            "\t\t\"force\". If force is passed,block pool directory for\n" +
            "\t\tthe given blockpool id on the given datanode is deleted\n" +
            "\t\talong with its contents,otherwise the directory is deleted\n"+
            "\t\tonly if it is empty.The command will fail if datanode is\n" +
            "\t\tstill serving the block pool.Refer to refreshNamenodes to\n" +
            "\t\tshutdown a block pool service on a datanode.\n" +
            "\t\tThe ipc_port is determined by 'dfs.datanode.ipc.address'," +
            "default is DFS_DATANODE_IPC_DEFAULT_PORT.\n";

    String setBalancerBandwidth = "-setBalancerBandwidth <bandwidth>:\n" +
      "\tChanges the network bandwidth used by each datanode during\n" +
      "\tHDFS block balancing.\n\n" +
      "\t\t<bandwidth> is the maximum number of bytes per second\n" +
      "\t\tthat will be used by each datanode. This value overrides\n" +
      "\t\tthe dfs.datanode.balance.bandwidthPerSec parameter.\n\n" +
      "\t\t--- NOTE: The new value is not persistent on the DataNode.---\n";

    String getBalancerBandwidth = "-getBalancerBandwidth <datanode_host:ipc_port>:\n" +
        "\tGet the network bandwidth for the given datanode.\n" +
        "\tThis is the maximum network bandwidth used by the datanode\n" +
        "\tduring HDFS block balancing.\n\n" +
        "\t--- NOTE: This value is not persistent on the DataNode.---\n";

    String fetchImage = "-fetchImage <local directory>:\n" +
      "\tDownloads the most recent fsimage from the Name Node and saves it in" +
      "\tthe specified local directory.\n";
    
    String allowSnapshot = "-allowSnapshot <snapshotDir>:\n" +
        "\tAllow snapshots to be taken on a directory.\n";
    
    String disallowSnapshot = "-disallowSnapshot <snapshotDir>:\n" +
        "\tDo not allow snapshots to be taken on a directory any more.\n";

    String shutdownDatanode = "-shutdownDatanode <datanode_host:ipc_port> [upgrade]\n"
        + "\tSubmit a shutdown request for the given datanode. If an optional\n"
        + "\t\"upgrade\" argument is specified, clients accessing the datanode\n"
        + "\twill be advised to wait for it to restart and the fast start-up\n"
        + "\tmode will be enabled. When the restart does not happen in time,\n"
        + "\tclients will timeout and ignore the datanode. In such case, the\n"
        + "\tfast start-up mode will also be disabled.\n";

    String evictWriters = "-evictWriters <datanode_host:ipc_port>\n"
        + "\tMake the datanode evict all clients that are writing a block.\n"
        + "\tThis is useful if decommissioning is hung due to slow writers.\n";

    String getDatanodeInfo = "-getDatanodeInfo <datanode_host:ipc_port>\n"
        + "\tGet the information about the given datanode. This command can\n"
        + "\tbe used for checking if a datanode is alive.\n";

    String triggerBlockReport =
      "-triggerBlockReport [-incremental] <datanode_host:ipc_port>\n"
        + "\tTrigger a block report for the datanode.\n"
        + "\tIf 'incremental' is specified, it will be an incremental\n"
        + "\tblock report; otherwise, it will be a full block report.\n";

    String listOpenFiles = "-listOpenFiles [-blockingDecommission]\n"
        + "\tList all open files currently managed by the NameNode along\n"
        + "\twith client name and client machine accessing them.\n"
        + "\tIf 'blockingDecommission' option is specified, it will list the\n"
        + "\topen files only that are blocking the ongoing Decommission.";

    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n" +
      "\t\tis specified.\n";

    if ("report".equals(cmd)) {
      System.out.println(report);
    } else if ("safemode".equals(cmd)) {
      System.out.println(safemode);
    } else if ("saveNamespace".equals(cmd)) {
      System.out.println(saveNamespace);
    } else if ("rollEdits".equals(cmd)) {
      System.out.println(rollEdits);
    } else if ("restoreFailedStorage".equals(cmd)) {
      System.out.println(restoreFailedStorage);
    } else if ("refreshNodes".equals(cmd)) {
      System.out.println(refreshNodes);
    } else if ("finalizeUpgrade".equals(cmd)) {
      System.out.println(finalizeUpgrade);
    } else if (RollingUpgradeCommand.matches("-"+cmd)) {
      System.out.println(RollingUpgradeCommand.DESCRIPTION);
    } else if ("upgrade".equals(cmd)) {
      System.out.println(upgrade);
    } else if ("metasave".equals(cmd)) {
      System.out.println(metaSave);
    } else if (SetQuotaCommand.matches("-"+cmd)) {
      System.out.println(SetQuotaCommand.DESCRIPTION);
    } else if (ClearQuotaCommand.matches("-"+cmd)) {
      System.out.println(ClearQuotaCommand.DESCRIPTION);
    } else if (SetSpaceQuotaCommand.matches("-"+cmd)) {
      System.out.println(SetSpaceQuotaCommand.DESCRIPTION);
    } else if (ClearSpaceQuotaCommand.matches("-"+cmd)) {
      System.out.println(ClearSpaceQuotaCommand.DESCRIPTION);
    } else if ("refreshServiceAcl".equals(cmd)) {
      System.out.println(refreshServiceAcl);
    } else if ("refreshUserToGroupsMappings".equals(cmd)) {
      System.out.println(refreshUserToGroupsMappings);
    } else if ("refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.out.println(refreshSuperUserGroupsConfiguration);
    } else if ("refreshCallQueue".equals(cmd)) {
      System.out.println(refreshCallQueue);
    } else if ("refresh".equals(cmd)) {
      System.out.println(genericRefresh);
    } else if ("reconfig".equals(cmd)) {
      System.out.println(reconfig);
    } else if ("printTopology".equals(cmd)) {
      System.out.println(printTopology);
    } else if ("refreshNamenodes".equals(cmd)) {
      System.out.println(refreshNamenodes);
    } else if ("getVolumeReport".equals(cmd)) {
      System.out.println(getVolumeReport);
    } else if ("deleteBlockPool".equals(cmd)) {
      System.out.println(deleteBlockPool);
    } else if ("setBalancerBandwidth".equals(cmd)) {
      System.out.println(setBalancerBandwidth);
    } else if ("getBalancerBandwidth".equals(cmd)) {
      System.out.println(getBalancerBandwidth);
    } else if ("fetchImage".equals(cmd)) {
      System.out.println(fetchImage);
    } else if ("allowSnapshot".equalsIgnoreCase(cmd)) {
      System.out.println(allowSnapshot);
    } else if ("disallowSnapshot".equalsIgnoreCase(cmd)) {
      System.out.println(disallowSnapshot);
    } else if ("shutdownDatanode".equalsIgnoreCase(cmd)) {
      System.out.println(shutdownDatanode);
    } else if ("evictWriters".equalsIgnoreCase(cmd)) {
      System.out.println(evictWriters);
    } else if ("getDatanodeInfo".equalsIgnoreCase(cmd)) {
      System.out.println(getDatanodeInfo);
    } else if ("triggerBlockReport".equalsIgnoreCase(cmd)) {
      System.out.println(triggerBlockReport);
    } else if ("listOpenFiles".equalsIgnoreCase(cmd)) {
      System.out.println(listOpenFiles);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(report);
      System.out.println(safemode);
      System.out.println(saveNamespace);
      System.out.println(rollEdits);
      System.out.println(restoreFailedStorage);
      System.out.println(refreshNodes);
      System.out.println(finalizeUpgrade);
      System.out.println(RollingUpgradeCommand.DESCRIPTION);
      System.out.println(upgrade);
      System.out.println(metaSave);
      System.out.println(SetQuotaCommand.DESCRIPTION);
      System.out.println(ClearQuotaCommand.DESCRIPTION);
      System.out.println(SetSpaceQuotaCommand.DESCRIPTION);
      System.out.println(ClearSpaceQuotaCommand.DESCRIPTION);
      System.out.println(refreshServiceAcl);
      System.out.println(refreshUserToGroupsMappings);
      System.out.println(refreshSuperUserGroupsConfiguration);
      System.out.println(refreshCallQueue);
      System.out.println(genericRefresh);
      System.out.println(reconfig);
      System.out.println(printTopology);
      System.out.println(refreshNamenodes);
      System.out.println(deleteBlockPool);
      System.out.println(setBalancerBandwidth);
      System.out.println(getBalancerBandwidth);
      System.out.println(fetchImage);
      System.out.println(allowSnapshot);
      System.out.println(disallowSnapshot);
      System.out.println(shutdownDatanode);
      System.out.println(evictWriters);
      System.out.println(getDatanodeInfo);
      System.out.println(triggerBlockReport);
      System.out.println(listOpenFiles);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }

  }

  /**
   * Command to ask the namenode to finalize previously performed upgrade.
   * Usage: hdfs dfsadmin -finalizeUpgrade
   * @exception IOException 
   */
  public int finalizeUpgrade() throws IOException {
    DistributedFileSystem dfs = getDFS();
    
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaAndLogicalUri = HAUtilClient.isLogicalUri(dfsConf, dfsUri);
    if (isHaAndLogicalUri) {
      // In the case of HA and logical URI, run finalizeUpgrade for all
      // NNs in this nameservice.
      String nsId = dfsUri.getHost();
      List<ClientProtocol> namenodes =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf, nsId);
      if (!HAUtil.isAtLeastOneActive(namenodes)) {
        throw new IOException("Cannot finalize with no NameNode active");
      }

      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        try{
          proxy.getProxy().finalizeUpgrade();
          System.out.println("Finalize upgrade successful for " +
              proxy.getAddress());
        }catch (IOException ioe){
          System.out.println("Finalize upgrade failed for " +
              proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      dfs.finalizeUpgrade();
      System.out.println("Finalize upgrade successful");
    }
    
    return 0;
  }

  /**
   * Command to get the upgrade status of each namenode in the nameservice.
   * Usage: hdfs dfsadmin -upgrade query
   * @exception IOException
   */
  public int getUpgradeStatus() throws IOException {
    DistributedFileSystem dfs = getDFS();

    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();

    boolean isHaAndLogicalUri = HAUtilClient.isLogicalUri(dfsConf, dfsUri);
    if (isHaAndLogicalUri) {
      // In the case of HA and logical URI, run upgrade query for all
      // NNs in this nameservice.
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
              nsId, ClientProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        try {
          boolean upgradeFinalized = proxy.getProxy().upgradeStatus();
          if (upgradeFinalized) {
            System.out.println("Upgrade finalized for " + proxy.getAddress());
          } else {
            System.out.println("Upgrade not finalized for " +
                proxy.getAddress());
          }
        } catch (IOException ioe){
          System.err.println("Getting upgrade status failed for " +
              proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if (!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      if (dfs.upgradeStatus()) {
        System.out.println("Upgrade finalized");
      } else {
        System.out.println("Upgrade not finalized");
      }
    }

    return 0;
  }

  /**
   * Upgrade command to get the status of upgrade or ask NameNode to finalize
   * the previously performed upgrade.
   * Usage: hdfs dfsadmin -upgrade [query | finalize]
   * @exception IOException
   */
  public int upgrade(String arg) throws IOException {
    UpgradeAction action;
    if ("query".equalsIgnoreCase(arg)) {
      action = UpgradeAction.QUERY;
    } else if ("finalize".equalsIgnoreCase(arg)) {
      action = UpgradeAction.FINALIZE;
    } else {
      printUsage("-upgrade");
      return -1;
    }

    switch (action) {
    case QUERY:
      return getUpgradeStatus();
    case FINALIZE:
      return finalizeUpgrade();
    default:
      printUsage("-upgrade");
      return -1;
    }
  }

  /**
   * Dumps DFS data structures into specified file.
   * Usage: hdfs dfsadmin -metasave filename
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException if an error occurred while accessing
   *            the file or path.
   */
  public int metaSave(String[] argv, int idx) throws IOException {
    String pathname = argv[idx];
    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        try{
          proxy.getProxy().metaSave(pathname);
          System.out.println("Created metasave file " + pathname
              + " in the log directory of namenode " + proxy.getAddress());
        } catch (IOException ioe){
          System.out.println("Created metasave file " + pathname
              + " in the log directory of namenode " + proxy.getAddress()
              + " failed");
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      dfs.metaSave(pathname);
      System.out.println("Created metasave file " + pathname + " in the log " +
          "directory of namenode " + dfs.getUri());
    }
    return 0;
  }

  /**
   * Display each rack and the nodes assigned to that rack, as determined
   * by the NameNode, in a hierarchical manner.  The nodes and racks are
   * sorted alphabetically.
   * 
   * @throws IOException If an error while getting datanode report
   */
  public int printTopology() throws IOException {
      DistributedFileSystem dfs = getDFS();
      final DatanodeInfo[] report = dfs.getDataNodeStats();

      // Build a map of rack -> nodes from the datanode report
      HashMap<String, TreeSet<String> > tree = new HashMap<String, TreeSet<String>>();
      for(DatanodeInfo dni : report) {
        String location = dni.getNetworkLocation();
        String name = dni.getName();
        
        if(!tree.containsKey(location)) {
          tree.put(location, new TreeSet<String>());
        }
        
        tree.get(location).add(name);
      }
      
      // Sort the racks (and nodes) alphabetically, display in order
      ArrayList<String> racks = new ArrayList<String>(tree.keySet());
      Collections.sort(racks);
      
      for(String r : racks) {
        System.out.println("Rack: " + r);
        TreeSet<String> nodes = tree.get(r);

        for(String n : nodes) {
          System.out.print("   " + n);
          String hostname = NetUtils.getHostNameOfIP(n);
          if(hostname != null)
            System.out.print(" (" + hostname + ")");
          System.out.println();
        }

        System.out.println();
      }
    return 0;
  }
  
  private static UserGroupInformation getUGI() 
  throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  /**
   * Refresh the authorization policy on the {@link NameNode}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  public int refreshServiceAcl() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();

    // for security authorization
    // server principal for this call   
    // should be NN's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, ""));

    DistributedFileSystem dfs = getDFS();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {
      // Run refreshServiceAcl for all NNs if HA is enabled
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<RefreshAuthorizationPolicyProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
              RefreshAuthorizationPolicyProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<RefreshAuthorizationPolicyProtocol> proxy : proxies) {
        try{
          proxy.getProxy().refreshServiceAcl();
          System.out.println("Refresh service acl successful for "
              + proxy.getAddress());
        }catch (IOException ioe){
          System.out.println("Refresh service acl failed for "
              + proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()) {
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      // Create the client
      RefreshAuthorizationPolicyProtocol refreshProtocol =
          NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
              RefreshAuthorizationPolicyProtocol.class).getProxy();
      // Refresh the authorization policy in-effect
      refreshProtocol.refreshServiceAcl();
      System.out.println("Refresh service acl successful");
    }
    
    return 0;
  }
  
  /**
   * Refresh the user-to-groups mappings on the {@link NameNode}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  public int refreshUserToGroupsMappings() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // for security authorization
    // server principal for this call   
    // should be NN's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, ""));

    DistributedFileSystem dfs = getDFS();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {
      // Run refreshUserToGroupsMapings for all NNs if HA is enabled
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<RefreshUserMappingsProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
              RefreshUserMappingsProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<RefreshUserMappingsProtocol> proxy : proxies) {
        try{
          proxy.getProxy().refreshUserToGroupsMappings();
          System.out.println("Refresh user to groups mapping successful for "
              + proxy.getAddress());
        }catch (IOException ioe){
          System.out.println("Refresh user to groups mapping failed for "
              + proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      // Create the client
      RefreshUserMappingsProtocol refreshProtocol =
          NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
              RefreshUserMappingsProtocol.class).getProxy();

      // Refresh the user-to-groups mappings
      refreshProtocol.refreshUserToGroupsMappings();
      System.out.println("Refresh user to groups mapping successful");
    }
    
    return 0;
  }
  

  /**
   * refreshSuperUserGroupsConfiguration {@link NameNode}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  public int refreshSuperUserGroupsConfiguration() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();

    // for security authorization
    // server principal for this call 
    // should be NAMENODE's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, ""));

    DistributedFileSystem dfs = getDFS();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {
      // Run refreshSuperUserGroupsConfiguration for all NNs if HA is enabled
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<RefreshUserMappingsProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
              RefreshUserMappingsProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<RefreshUserMappingsProtocol> proxy : proxies) {
        try{
          proxy.getProxy().refreshSuperUserGroupsConfiguration();
          System.out.println("Refresh super user groups configuration " +
              "successful for " + proxy.getAddress());
        }catch (IOException ioe){
          System.out.println("Refresh super user groups configuration " +
              "failed for " + proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      // Create the client
      RefreshUserMappingsProtocol refreshProtocol =
          NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
              RefreshUserMappingsProtocol.class).getProxy();

      // Refresh the user-to-groups mappings
      refreshProtocol.refreshSuperUserGroupsConfiguration();
      System.out.println("Refresh super user groups configuration successful");
    }

    return 0;
  }

  public int refreshCallQueue() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // for security authorization
    // server principal for this call   
    // should be NN's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, ""));

    DistributedFileSystem dfs = getDFS();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {
      // Run refreshCallQueue for all NNs if HA is enabled
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<RefreshCallQueueProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
              RefreshCallQueueProtocol.class);
      List<IOException> exceptions = new ArrayList<>();
      for (ProxyAndInfo<RefreshCallQueueProtocol> proxy : proxies) {
        try{
          proxy.getProxy().refreshCallQueue();
          System.out.println("Refresh call queue successful for "
              + proxy.getAddress());
        }catch (IOException ioe){
          System.out.println("Refresh call queue failed for "
              + proxy.getAddress());
          exceptions.add(ioe);
        }
      }
      if(!exceptions.isEmpty()){
        throw MultipleIOException.createIOException(exceptions);
      }
    } else {
      // Create the client
      RefreshCallQueueProtocol refreshProtocol =
          NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
              RefreshCallQueueProtocol.class).getProxy();

      // Refresh the call queue
      refreshProtocol.refreshCallQueue();
      System.out.println("Refresh call queue successful");
    }

    return 0;
  }

  public int reconfig(String[] argv, int i) throws IOException {
    String nodeType = argv[i];
    String address = argv[i + 1];
    String op = argv[i + 2];

    if ("start".equals(op)) {
      return startReconfiguration(nodeType, address, System.out, System.err);
    } else if ("status".equals(op)) {
      return getReconfigurationStatus(nodeType, address, System.out, System.err);
    } else if ("properties".equals(op)) {
      return getReconfigurableProperties(nodeType, address, System.out,
          System.err);
    }
    System.err.println("Unknown operation: " + op);
    return -1;
  }

  int startReconfiguration(final String nodeThpe, final String address)
      throws IOException {
    return startReconfiguration(nodeThpe, address, System.out, System.err);
  }

  int startReconfiguration(final String nodeType, final String address,
      final PrintStream out, final PrintStream err) throws IOException {
    String outMsg = null;
    String errMsg = null;
    int ret = 0;

    try {
      ret = startReconfigurationDispatch(nodeType, address, out, err);
      outMsg = String.format("Started reconfiguration task on node [%s].",
          address);
    } catch (IOException e) {
      errMsg = String.format("Node [%s] reconfiguring: %s.", address,
          e.toString());
    }

    if (errMsg != null) {
      err.println(errMsg);
      return 1;
    } else {
      out.println(outMsg);
      return ret;
    }
  }

  int startReconfigurationDispatch(final String nodeType,
      final String address, final PrintStream out, final PrintStream err)
      throws IOException {
    if ("namenode".equals(nodeType)) {
      ReconfigurationProtocol reconfProxy = getNameNodeProxy(address);
      reconfProxy.startReconfiguration();
      return 0;
    } else if ("datanode".equals(nodeType)) {
      ClientDatanodeProtocol reconfProxy = getDataNodeProxy(address);
      reconfProxy.startReconfiguration();
      return 0;
    } else {
      System.err.println("Node type " + nodeType
          + " does not support reconfiguration.");
      return 1;
    }
  }

  int getReconfigurationStatus(final String nodeType, final String address,
      final PrintStream out, final PrintStream err) throws IOException {
    String outMsg = null;
    String errMsg = null;
    ReconfigurationTaskStatus status = null;

    try {
      status = getReconfigurationStatusDispatch(nodeType, address, out, err);
      outMsg = String.format("Reconfiguring status for node [%s]: ", address);
    } catch (IOException e) {
      errMsg = String.format("Node [%s] reloading configuration: %s.", address,
          e.toString());
    }

    if (errMsg != null) {
      err.println(errMsg);
      return 1;
    } else {
      out.print(outMsg);
    }

    if (status != null) {
      if (!status.hasTask()) {
        out.println("no task was found.");
        return 0;
      }
      out.print("started at " + new Date(status.getStartTime()));
      if (!status.stopped()) {
        out.println(" and is still running.");
        return 0;
      }

      out.println(" and finished at "
          + new Date(status.getEndTime()).toString() + ".");
      if (status.getStatus() == null) {
        // Nothing to report.
        return 0;
      }
      for (Map.Entry<PropertyChange, Optional<String>> result : status
          .getStatus().entrySet()) {
        if (!result.getValue().isPresent()) {
          out.printf(
              "SUCCESS: Changed property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n",
              result.getKey().prop, result.getKey().oldVal,
              result.getKey().newVal);
        } else {
          final String errorMsg = result.getValue().get();
          out.printf(
              "FAILED: Change property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n",
              result.getKey().prop, result.getKey().oldVal,
              result.getKey().newVal);
          out.println("\tError: " + errorMsg + ".");
        }
      }
    } else {
      return 1;
    }

    return 0;
  }

  ReconfigurationTaskStatus getReconfigurationStatusDispatch(
      final String nodeType, final String address, final PrintStream out,
      final PrintStream err) throws IOException {
    if ("namenode".equals(nodeType)) {
      ReconfigurationProtocol reconfProxy = getNameNodeProxy(address);
      return reconfProxy.getReconfigurationStatus();
    } else if ("datanode".equals(nodeType)) {
      ClientDatanodeProtocol reconfProxy = getDataNodeProxy(address);
      return reconfProxy.getReconfigurationStatus();
    } else {
      err.println("Node type " + nodeType
          + " does not support reconfiguration.");
      return null;
    }
  }

  int getReconfigurableProperties(final String nodeType, final String address,
      final PrintStream out, final PrintStream err) throws IOException {
    String outMsg = null;
    String errMsg = null;
    List<String> properties = null;

    try {
      properties = getReconfigurablePropertiesDispatch(nodeType, address, out,
          err);
      outMsg = String.format("Node [%s] Reconfigurable properties:", address);
    } catch (IOException e) {
      errMsg = String.format("Node [%s] reconfiguration: %s.", address,
          e.toString());
    }

    if (errMsg != null) {
      err.println(errMsg);
      return 1;
    } else if (properties == null) {
      return 1;
    } else {
      out.println(outMsg);
      for (String name : properties) {
        out.println(name);
      }
      return 0;
    }
  }

  List<String> getReconfigurablePropertiesDispatch(final String nodeType,
      final String address, final PrintStream out, final PrintStream err)
      throws IOException {
    if ("namenode".equals(nodeType)) {
      ReconfigurationProtocol reconfProxy = getNameNodeProxy(address);
      return reconfProxy.listReconfigurableProperties();
    } else if ("datanode".equals(nodeType)) {
      ClientDatanodeProtocol reconfProxy = getDataNodeProxy(address);
      return reconfProxy.listReconfigurableProperties();
    } else {
      err.println("Node type " + nodeType
          + " does not support reconfiguration.");
      return null;
    }
  }

  public int genericRefresh(String[] argv, int i) throws IOException {
    String hostport = argv[i++];
    String identifier = argv[i++];
    String[] args = Arrays.copyOfRange(argv, i, argv.length);

    // Get the current configuration
    Configuration conf = getConf();

    // for security authorization
    // server principal for this call
    // should be NN's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
      conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, ""));

    // Create the client
    Class<?> xface = GenericRefreshProtocolPB.class;
    InetSocketAddress address = NetUtils.createSocketAddr(hostport);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    RPC.setProtocolEngine(conf, xface, ProtobufRpcEngine.class);
    GenericRefreshProtocolPB proxy = (GenericRefreshProtocolPB)
      RPC.getProxy(xface, RPC.getProtocolVersion(xface), address,
        ugi, conf, NetUtils.getDefaultSocketFactory(conf), 0);

    Collection<RefreshResponse> responses = null;
    try (GenericRefreshProtocolClientSideTranslatorPB xlator =
        new GenericRefreshProtocolClientSideTranslatorPB(proxy);) {
      // Refresh
      responses = xlator.refresh(identifier, args);

      int returnCode = 0;

      // Print refresh responses
      System.out.println("Refresh Responses:\n");
      for (RefreshResponse response : responses) {
        System.out.println(response.toString());

        if (returnCode == 0 && response.getReturnCode() != 0) {
          // This is the first non-zero return code, so we should return this
          returnCode = response.getReturnCode();
        } else if (returnCode != 0 && response.getReturnCode() != 0) {
          // Then now we have multiple non-zero return codes,
          // so we merge them into -1
          returnCode = - 1;
        }
      }
      return returnCode;
    } finally {
      if (responses == null) {
        System.out.println("Failed to get response.\n");
        return -1;
      }
    }
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-report".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-report] [-live] [-dead] [-decommissioning]"
          + " [-enteringmaintenance] [-inmaintenance]");
    } else if ("-safemode".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-safemode enter | leave | get | wait | forceExit]");
    } else if ("-allowSnapshot".equalsIgnoreCase(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-allowSnapshot <snapshotDir>]");
    } else if ("-disallowSnapshot".equalsIgnoreCase(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-disallowSnapshot <snapshotDir>]");
    } else if ("-saveNamespace".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-saveNamespace [-beforeShutdown]]");
    } else if ("-rollEdits".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin [-rollEdits]");
    } else if ("-restoreFailedStorage".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-restoreFailedStorage true|false|check ]");
    } else if ("-refreshNodes".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [-refreshNodes]");
    } else if ("-finalizeUpgrade".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [-finalizeUpgrade]");
    } else if (RollingUpgradeCommand.matches(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [" + RollingUpgradeCommand.USAGE+"]");
    } else if ("-upgrade".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-upgrade query | finalize]");
    } else if ("-metasave".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-metasave filename]");
    } else if (SetQuotaCommand.matches(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [" + SetQuotaCommand.USAGE+"]");
    } else if (ClearQuotaCommand.matches(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " ["+ClearQuotaCommand.USAGE+"]");
    } else if (SetSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [" + SetSpaceQuotaCommand.USAGE+"]");
    } else if (ClearSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " ["+ClearSpaceQuotaCommand.USAGE+"]");
    } else if ("-refreshServiceAcl".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [-refreshServiceAcl]");
    } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [-refreshUserToGroupsMappings]");
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [-refreshSuperUserGroupsConfiguration]");
    } else if ("-refreshCallQueue".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [-refreshCallQueue]");
    } else if ("-reconfig".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-reconfig <namenode|datanode> <host:ipc_port> "
          + "<start|status|properties>]");
    } else if ("-refresh".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-refresh <hostname:ipc_port> "
          + "<resource_identifier> [arg1..argn]");
    } else if ("-printTopology".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [-printTopology]");
    } else if ("-refreshNamenodes".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                         + " [-refreshNamenodes datanode-host:ipc_port]");
    } else if ("-getVolumeReport".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-getVolumeReport datanode-host:ipc_port]");
    } else if ("-deleteBlockPool".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-deleteBlockPool datanode-host:ipc_port blockpoolId [force]]");
    } else if ("-setBalancerBandwidth".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
                  + " [-setBalancerBandwidth <bandwidth in bytes per second>]");
    } else if ("-getBalancerBandwidth".equalsIgnoreCase(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-getBalancerBandwidth <datanode_host:ipc_port>]");
    } else if ("-fetchImage".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-fetchImage <local directory>]");
    } else if ("-shutdownDatanode".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-shutdownDatanode <datanode_host:ipc_port> [upgrade]]");
    } else if ("-evictWriters".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-evictWriters <datanode_host:ipc_port>]");
    } else if ("-getDatanodeInfo".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-getDatanodeInfo <datanode_host:ipc_port>]");
    } else if ("-triggerBlockReport".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-triggerBlockReport [-incremental] <datanode_host:ipc_port>]");
    } else if ("-listOpenFiles".equals(cmd)) {
      System.err.println("Usage: hdfs dfsadmin"
          + " [-listOpenFiles [-blockingDecommission] [-path <path>]]");
    } else {
      System.err.println("Usage: hdfs dfsadmin");
      System.err.println("Note: Administrative commands can only be run as the HDFS superuser.");
      System.err.println(commonUsageSummary);
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * @param argv The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  @Override
  public int run(String[] argv) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-safemode".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-allowSnapshot".equalsIgnoreCase(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-disallowSnapshot".equalsIgnoreCase(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-report".equals(cmd)) {
      if (argv.length > 6) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-saveNamespace".equals(cmd)) {
      if (argv.length != 1 && argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-rollEdits".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }      
    } else if ("-restoreFailedStorage".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshNodes".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-finalizeUpgrade".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if (RollingUpgradeCommand.matches(cmd)) {
      if (argv.length > 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-upgrade".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-metasave".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshServiceAcl".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refresh".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-printTopology".equals(cmd)) {
      if(argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshNamenodes".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-getVolumeReport".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-reconfig".equals(cmd)) {
      if (argv.length != 4) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-deleteBlockPool".equals(cmd)) {
      if ((argv.length != 3) && (argv.length != 4)) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-setBalancerBandwidth".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-getBalancerBandwidth".equalsIgnoreCase(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-fetchImage".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-shutdownDatanode".equals(cmd)) {
      if ((argv.length != 2) && (argv.length != 3)) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-getDatanodeInfo".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-triggerBlockReport".equals(cmd)) {
      if ((argv.length != 2) && (argv.length != 3)) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-listOpenFiles".equals(cmd)) {
      if ((argv.length > 4)) {
        printUsage(cmd);
        return exitCode;
      }
    }
    
    // initialize DFSAdmin
    try {
      init();
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server"
                         + "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to DFS... command aborted.");
      return exitCode;
    }

    Exception debugException = null;
    exitCode = 0;
    try {
      if ("-report".equals(cmd)) {
        report(argv, i);
      } else if ("-safemode".equals(cmd)) {
        setSafeMode(argv, i);
      } else if ("-allowSnapshot".equalsIgnoreCase(cmd)) {
        allowSnapshot(argv);
      } else if ("-disallowSnapshot".equalsIgnoreCase(cmd)) {
        disallowSnapshot(argv);
      } else if ("-saveNamespace".equals(cmd)) {
        exitCode = saveNamespace(argv);
      } else if ("-rollEdits".equals(cmd)) {
        exitCode = rollEdits();
      } else if ("-restoreFailedStorage".equals(cmd)) {
        exitCode = restoreFailedStorage(argv[i]);
      } else if ("-refreshNodes".equals(cmd)) {
        exitCode = refreshNodes();
      } else if ("-finalizeUpgrade".equals(cmd)) {
        exitCode = finalizeUpgrade();
      } else if (RollingUpgradeCommand.matches(cmd)) {
        exitCode = RollingUpgradeCommand.run(getDFS(), argv, i);
      } else if ("-upgrade".equals(cmd)) {
        exitCode = upgrade(argv[i]);
      } else if ("-metasave".equals(cmd)) {
        exitCode = metaSave(argv, i);
      } else if (ClearQuotaCommand.matches(cmd)) {
        exitCode = new ClearQuotaCommand(argv, i, getConf()).runAll();
      } else if (SetQuotaCommand.matches(cmd)) {
        exitCode = new SetQuotaCommand(argv, i, getConf()).runAll();
      } else if (ClearSpaceQuotaCommand.matches(cmd)) {
        exitCode = new ClearSpaceQuotaCommand(argv, i, getConf()).runAll();
      } else if (SetSpaceQuotaCommand.matches(cmd)) {
        exitCode = new SetSpaceQuotaCommand(argv, i, getConf()).runAll();
      } else if ("-refreshServiceAcl".equals(cmd)) {
        exitCode = refreshServiceAcl();
      } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
        exitCode = refreshUserToGroupsMappings();
      } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
        exitCode = refreshSuperUserGroupsConfiguration();
      } else if ("-refreshCallQueue".equals(cmd)) {
        exitCode = refreshCallQueue();
      } else if ("-refresh".equals(cmd)) {
        exitCode = genericRefresh(argv, i);
      } else if ("-printTopology".equals(cmd)) {
        exitCode = printTopology();
      } else if ("-refreshNamenodes".equals(cmd)) {
        exitCode = refreshNamenodes(argv, i);
      } else if ("-getVolumeReport".equals(cmd)) {
        exitCode = getVolumeReport(argv, i);
      } else if ("-deleteBlockPool".equals(cmd)) {
        exitCode = deleteBlockPool(argv, i);
      } else if ("-setBalancerBandwidth".equals(cmd)) {
        exitCode = setBalancerBandwidth(argv, i);
      } else if ("-getBalancerBandwidth".equals(cmd)) {
        exitCode = getBalancerBandwidth(argv, i);
      } else if ("-fetchImage".equals(cmd)) {
        exitCode = fetchImage(argv, i);
      } else if ("-shutdownDatanode".equals(cmd)) {
        exitCode = shutdownDatanode(argv, i);
      } else if ("-evictWriters".equals(cmd)) {
        exitCode = evictWriters(argv, i);
      } else if ("-getDatanodeInfo".equals(cmd)) {
        exitCode = getDatanodeInfo(argv, i);
      } else if ("-reconfig".equals(cmd)) {
        exitCode = reconfig(argv, i);
      } else if ("-triggerBlockReport".equals(cmd)) {
        exitCode = triggerBlockReport(argv);
      } else if ("-listOpenFiles".equals(cmd)) {
        exitCode = listOpenFiles(argv);
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i]);
        } else {
          printHelp("");
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      debugException = arge;
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error message, ignore the stack trace.
      exitCode = -1;
      debugException = e;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": "
                           + content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": "
                           + ex.getLocalizedMessage());
        debugException = ex;
      }
    } catch (Exception e) {
      exitCode = -1;
      debugException = e;
      System.err.println(cmd.substring(1) + ": "
                         + e.getLocalizedMessage());
    }
    if (LOG.isDebugEnabled() && debugException != null) {
      LOG.debug("Exception encountered:", debugException);
    }
    return exitCode;
  }

  private int getVolumeReport(String[] argv, int i) throws IOException {
    ClientDatanodeProtocol datanode = getDataNodeProxy(argv[i]);
    List<DatanodeVolumeInfo> volumeReport = datanode
        .getVolumeReport();
    System.out.println("Active Volumes : " + volumeReport.size());
    for (DatanodeVolumeInfo info : volumeReport) {
      System.out.println("\n" + info.getDatanodeVolumeReport());
    }
    return 0;
  }

  private ClientDatanodeProtocol getDataNodeProxy(String datanode)
      throws IOException {
    InetSocketAddress datanodeAddr = NetUtils.createSocketAddr(datanode);
    // Get the current configuration
    Configuration conf = getConf();

    // For datanode proxy the server principal should be DN's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        conf.get(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, ""));

    // Create the client
    ClientDatanodeProtocol dnProtocol =     
        DFSUtilClient.createClientDatanodeProtocolProxy(datanodeAddr, getUGI(), conf,
            NetUtils.getSocketFactory(conf, ClientDatanodeProtocol.class));
    return dnProtocol;
  }

  private ReconfigurationProtocol getNameNodeProxy(String node)
      throws IOException {
    InetSocketAddress nodeAddr = NetUtils.createSocketAddr(node);
    // Get the current configuration
    Configuration conf = getConf();

    // For namenode proxy the server principal should be NN's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, ""));

    // Create the client
    ReconfigurationProtocol reconfigProtocol = DFSUtilClient
        .createReconfigurationProtocolProxy(nodeAddr, getUGI(), conf,
            NetUtils.getSocketFactory(conf, ReconfigurationProtocol.class));
    return reconfigProtocol;
  }
  
  private int deleteBlockPool(String[] argv, int i) throws IOException {
    ClientDatanodeProtocol dnProxy = getDataNodeProxy(argv[i]);
    boolean force = false;
    if (argv.length-1 == i+2) {
      if ("force".equals(argv[i+2])) {
        force = true;
      } else {
        printUsage("-deleteBlockPool");
        return -1;
      }
    }
    dnProxy.deleteBlockPool(argv[i+1], force);
    return 0;
  }
  
  private int refreshNamenodes(String[] argv, int i) throws IOException {
    String datanode = argv[i];
    ClientDatanodeProtocol refreshProtocol = getDataNodeProxy(datanode);
    refreshProtocol.refreshNamenodes();
    
    return 0;
  }

  private int shutdownDatanode(String[] argv, int i) throws IOException {
    final String dn = argv[i];
    ClientDatanodeProtocol dnProxy = getDataNodeProxy(dn);
    boolean upgrade = false;
    if (argv.length-1 == i+1) {
      if ("upgrade".equalsIgnoreCase(argv[i+1])) {
        upgrade = true;
      } else {
        printUsage("-shutdownDatanode");
        return -1;
      }
    }
    dnProxy.shutdownDatanode(upgrade);
    System.out.println("Submitted a shutdown request to datanode " + dn);
    return 0;
  }

  private int evictWriters(String[] argv, int i) throws IOException {
    final String dn = argv[i];
    ClientDatanodeProtocol dnProxy = getDataNodeProxy(dn);
    try {
      dnProxy.evictWriters();
      System.out.println("Requested writer eviction to datanode " + dn);
    } catch (IOException ioe) {
      throw new IOException("Datanode unreachable. " + ioe, ioe);
    }
    return 0;
  }

  private int getDatanodeInfo(String[] argv, int i) throws IOException {
    ClientDatanodeProtocol dnProxy = getDataNodeProxy(argv[i]);
    try {
      DatanodeLocalInfo dnInfo = dnProxy.getDatanodeInfo();
      System.out.println(dnInfo.getDatanodeLocalReport());
    } catch (IOException ioe) {
      throw new IOException("Datanode unreachable. " + ioe, ioe);
    }
    return 0;
  }

  /**
   * main() has some simple utility methods.
   * @param argv Command line parameters.
   * @exception Exception if the filesystem does not exist.
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new DFSAdmin(), argv);
    System.exit(res);
  }
}
