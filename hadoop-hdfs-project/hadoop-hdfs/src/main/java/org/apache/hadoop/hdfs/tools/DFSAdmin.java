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
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.NameNodeProxies.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.GenericRefreshProtocol;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB;
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
  
  private static final Log LOG = LogFactory.getLog(DFSAdmin.class);

  /**
   * An abstract class for the execution of a file system command
   */
  abstract private static class DFSAdminCommand extends Command {
    final DistributedFileSystem dfs;
    /** Constructor */
    public DFSAdminCommand(FileSystem fs) {
      super(fs.getConf());
      if (!(fs instanceof DistributedFileSystem)) {
        throw new IllegalArgumentException("FileSystem " + fs.getUri() + 
            " is not an HDFS file system");
      }
      this.dfs = (DistributedFileSystem)fs;
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
    ClearQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
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
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. User is not an administrator, or\n" +
      "\t\t3. The directory does not exist or is a file.\n" +
      "\t\tNote: A quota of 1 would force the directory to remain empty.\n";

    private final long quota; // the quota to be set
    
    /** Constructor */
    SetQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.quota = Long.parseLong(parameters.remove(0));
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
    private static final String USAGE = "-"+NAME+" <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the disk space quota for each directory <dirName>.\n" +
    "\t\tFor each directory, attempt to clear the quota. An error will be reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** Constructor */
    ClearSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
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
      dfs.setQuota(path, HdfsConstants.QUOTA_DONT_SET, HdfsConstants.QUOTA_RESET);
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setSpaceQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
      "Set the disk space quota <quota> for each directory <dirName>.\n" + 
      "\t\tThe space quota is a long integer that puts a hard limit\n" +
      "\t\ton the total size of all the files under the directory tree.\n" +
      "\t\tThe extra space required for replication is also counted. E.g.\n" +
      "\t\ta 1GB file with replication of 3 consumes 3GB of the quota.\n\n" +
      "\t\tQuota can also be specified with a binary prefix for terabytes,\n" +
      "\t\tpetabytes etc (e.g. 50t is 50TB, 5m is 5MB, 3p is 3PB).\n" + 
      "\t\tFor each directory, attempt to set the quota. An error will be reported if\n" +
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. user is not an administrator, or\n" +
      "\t\t3. the directory does not exist or is a file, or\n";

    private long quota; // the quota to be set
    
    /** Constructor */
    SetSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      String str = parameters.remove(0).trim();
      try {
        quota = StringUtils.TraditionalBinaryPrefix.string2long(str);
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("\"" + str + "\" is not a valid value for a quota.");
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
      dfs.setQuota(path, HdfsConstants.QUOTA_DONT_SET, quota);
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
        if (!info.createdRollbackImages()) {
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
        out.println("There is no rolling upgrade in progress.");
      }
    }

    static int run(DistributedFileSystem dfs, String[] argv, int idx) throws IOException {
      final RollingUpgradeAction action = RollingUpgradeAction.fromString(
          argv.length >= 2? argv[1]: "");
      if (action == null) {
        throw new IllegalArgumentException("Failed to covert \"" + argv[1]
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
        Preconditions.checkState(info.isFinalized());
        break;
      }
      printMessage(info, System.out);
      return 0;
    }
  }

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
    long presentCapacity = used + remaining;
    boolean mode = dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET);
    if (mode) {
      System.out.println("Safe mode is ON");
    }
    System.out.println("Configured Capacity: " + capacity
                       + " (" + StringUtils.byteDesc(capacity) + ")");
    System.out.println("Present Capacity: " + presentCapacity
        + " (" + StringUtils.byteDesc(presentCapacity) + ")");
    System.out.println("DFS Remaining: " + remaining
        + " (" + StringUtils.byteDesc(remaining) + ")");
    System.out.println("DFS Used: " + used
                       + " (" + StringUtils.byteDesc(used) + ")");
    System.out.println("DFS Used%: "
        + StringUtils.formatPercent(used/(double)presentCapacity, 2));
    
    /* These counts are not always upto date. They are updated after  
     * iteration of an internal list. Should be updated in a few seconds to 
     * minutes. Use "-metaSave" to list of all such blocks and accurate 
     * counts.
     */
    System.out.println("Under replicated blocks: " + 
                       dfs.getUnderReplicatedBlocksCount());
    System.out.println("Blocks with corrupt replicas: " + 
                       dfs.getCorruptBlocksCount());
    System.out.println("Missing blocks: " + 
                       dfs.getMissingBlocksCount());

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

    // If no filter flags are found, then list all DN types
    boolean listAll = (!listLive && !listDead && !listDecommissioning);

    if (listAll || listLive) {
      DatanodeInfo[] live = dfs.getDataNodeStats(DatanodeReportType.LIVE);
      if (live.length > 0 || listLive) {
        System.out.println("Live datanodes (" + live.length + "):\n");
      }
      if (live.length > 0) {
        for (DatanodeInfo dn : live) {
          System.out.println(dn.getDatanodeReport());
          System.out.println();
        }
      }
    }

    if (listAll || listDead) {
      DatanodeInfo[] dead = dfs.getDataNodeStats(DatanodeReportType.DEAD);
      if (dead.length > 0 || listDead) {
        System.out.println("Dead datanodes (" + dead.length + "):\n");
      }
      if (dead.length > 0) {
        for (DatanodeInfo dn : dead) {
          System.out.println(dn.getDatanodeReport());
          System.out.println();
        }
      }
    }

    if (listAll || listDecommissioning) {
      DatanodeInfo[] decom =
          dfs.getDataNodeStats(DatanodeReportType.DECOMMISSIONING);
      if (decom.length > 0 || listDecommissioning) {
        System.out.println("Decommissioning datanodes (" + decom.length
            + "):\n");
      }
      if (decom.length > 0) {
        for (DatanodeInfo dn : decom) {
          System.out.println(dn.getDatanodeReport());
          System.out.println();
        }
      }
    }
  }

  /**
   * Safe mode maintenance command.
   * Usage: java DFSAdmin -safemode [enter | leave | get]
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
    } else {
      printUsage("-safemode");
      return;
    }

    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtil.isLogicalUri(dfsConf, dfsUri);

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

  /**
   * Allow snapshot on a directory.
   * Usage: java DFSAdmin -allowSnapshot snapshotDir
   * @param argv List of of command line parameters.
   * @exception IOException
   */
  public void allowSnapshot(String[] argv) throws IOException {   
    DistributedFileSystem dfs = getDFS();
    try {
      dfs.allowSnapshot(new Path(argv[1]));
    } catch (SnapshotException e) {
      throw new RemoteException(e.getClass().getName(), e.getMessage());
    }
    System.out.println("Allowing snaphot on " + argv[1] + " succeeded");
  }
  
  /**
   * Allow snapshot on a directory.
   * Usage: java DFSAdmin -disallowSnapshot snapshotDir
   * @param argv List of of command line parameters.
   * @exception IOException
   */
  public void disallowSnapshot(String[] argv) throws IOException {  
    DistributedFileSystem dfs = getDFS();
    try {
      dfs.disallowSnapshot(new Path(argv[1]));
    } catch (SnapshotException e) {
      throw new RemoteException(e.getClass().getName(), e.getMessage());
    }
    System.out.println("Disallowing snaphot on " + argv[1] + " succeeded");
  }
  
  /**
   * Command to ask the namenode to save the namespace.
   * Usage: java DFSAdmin -saveNamespace
   * @exception IOException 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#saveNamespace()
   */
  public int saveNamespace() throws IOException {
    int exitCode = -1;

    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtil.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        proxy.getProxy().saveNamespace();
        System.out.println("Save namespace successful for " +
            proxy.getAddress());
      }
    } else {
      dfs.saveNamespace();
      System.out.println("Save namespace successful");
    }
    exitCode = 0;
   
    return exitCode;
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
   * Usage: java DFSAdmin -restoreFailedStorage true|false|check
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
    boolean isHaEnabled = HAUtil.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        Boolean res = proxy.getProxy().restoreFailedStorage(arg);
        System.out.println("restoreFailedStorage is set to " + res + " for "
            + proxy.getAddress());
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
   * Usage: java DFSAdmin -refreshNodes
   * @exception IOException 
   */
  public int refreshNodes() throws IOException {
    int exitCode = -1;

    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtil.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      for (ProxyAndInfo<ClientProtocol> proxy: proxies) {
        proxy.getProxy().refreshNodes();
        System.out.println("Refresh nodes successful for " +
            proxy.getAddress());
      }
    } else {
      dfs.refreshNodes();
      System.out.println("Refresh nodes successful");
    }
    exitCode = 0;
   
    return exitCode;
  }

  /**
   * Command to ask the namenode to set the balancer bandwidth for all of the
   * datanodes.
   * Usage: java DFSAdmin -setBalancerBandwidth bandwidth
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException 
   */
  public int setBalancerBandwidth(String[] argv, int idx) throws IOException {
    long bandwidth;
    int exitCode = -1;

    try {
      bandwidth = Long.parseLong(argv[idx]);
    } catch (NumberFormatException nfe) {
      System.err.println("NumberFormatException: " + nfe.getMessage());
      System.err.println("Usage: java DFSAdmin"
                  + " [-setBalancerBandwidth <bandwidth in bytes per second>]");
      return exitCode;
    }

    FileSystem fs = getFS();
    if (!(fs instanceof DistributedFileSystem)) {
      System.err.println("FileSystem is " + fs.getUri());
      return exitCode;
    }

    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtil.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        proxy.getProxy().setBalancerBandwidth(bandwidth);
        System.out.println("Balancer bandwidth is set to " + bandwidth +
            " for " + proxy.getAddress());
      }
    } else {
      dfs.setBalancerBandwidth(bandwidth);
      System.out.println("Balancer bandwidth is set to " + bandwidth);
    }
    exitCode = 0;

    return exitCode;
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
    String summary = "hadoop dfsadmin performs DFS administrative commands.\n" +
      "The full syntax is: \n\n" +
      "hadoop dfsadmin\n" +
      "\t[-report [-live] [-dead] [-decommissioning]]\n" +
      "\t[-safemode <enter | leave | get | wait>]\n" +
      "\t[-saveNamespace]\n" +
      "\t[-rollEdits]\n" +
      "\t[-restoreFailedStorage true|false|check]\n" +
      "\t[-refreshNodes]\n" +
      "\t[" + SetQuotaCommand.USAGE + "]\n" +
      "\t[" + ClearQuotaCommand.USAGE +"]\n" +
      "\t[" + SetSpaceQuotaCommand.USAGE + "]\n" +
      "\t[" + ClearSpaceQuotaCommand.USAGE +"]\n" +
      "\t[-finalizeUpgrade]\n" +
      "\t[" + RollingUpgradeCommand.USAGE +"]\n" +
      "\t[-refreshServiceAcl]\n" +
      "\t[-refreshUserToGroupsMappings]\n" +
      "\t[-refreshSuperUserGroupsConfiguration]\n" +
      "\t[-refreshCallQueue]\n" +
      "\t[-refresh <host:ipc_port> <key> [arg1..argn]\n" +
      "\t[-printTopology]\n" +
      "\t[-refreshNamenodes datanodehost:port]\n"+
      "\t[-deleteBlockPool datanodehost:port blockpoolId [force]]\n"+
      "\t[-setBalancerBandwidth <bandwidth>]\n" +
      "\t[-fetchImage <local directory>]\n" +
      "\t[-allowSnapshot <snapshotDir>]\n" +
      "\t[-disallowSnapshot <snapshotDir>]\n" +
      "\t[-shutdownDatanode <datanode_host:ipc_port> [upgrade]]\n" +
      "\t[-getDatanodeInfo <datanode_host:ipc_port>\n" +
      "\t[-help [cmd]]\n";

    String report ="-report [-live] [-dead] [-decommissioning]:\n" +
      "\tReports basic filesystem information and statistics.\n" +
      "\tOptional flags may be used to filter the list of displayed DNs.\n";
    

    String safemode = "-safemode <enter|leave|get|wait>:  Safe mode maintenance command.\n" + 
      "\t\tSafe mode is a Namenode state in which it\n" +
      "\t\t\t1.  does not accept changes to the name space (read-only)\n" +
      "\t\t\t2.  does not replicate or delete blocks.\n" +
      "\t\tSafe mode is entered automatically at Namenode startup, and\n" +
      "\t\tleaves safe mode automatically when the configured minimum\n" +
      "\t\tpercentage of blocks satisfies the minimum replication\n" +
      "\t\tcondition.  Safe mode can also be entered manually, but then\n" +
      "\t\tit can only be turned off manually as well.\n";

    String saveNamespace = "-saveNamespace:\t" +
    "Save current namespace into storage directories and reset edits log.\n" +
    "\t\tRequires superuser permissions and safe mode.\n";

    String rollEdits = "-rollEdits:\t" +
    "Rolls the edit log.\n" +
    "\t\tRequires superuser permissions.\n";
    
    String restoreFailedStorage = "-restoreFailedStorage:\t" +
    "Set/Unset/Check flag to attempt restore of failed storage replicas if they become available.\n" +
    "\t\tRequires superuser permissions.\n";
    
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

    String genericRefresh = "-refresh: Arguments are <hostname:port> <resource_identifier> [arg1..argn]\n" +
      "\tTriggers a runtime-refresh of the resource specified by <resource_identifier>\n" +
      "\ton <hostname:port>. All other args after are sent to the host.";

    String printTopology = "-printTopology: Print a tree of the racks and their\n" +
                           "\t\tnodes as reported by the Namenode\n";
    
    String refreshNamenodes = "-refreshNamenodes: Takes a datanodehost:port as argument,\n"+
                              "\t\tFor the given datanode, reloads the configuration files,\n" +
                              "\t\tstops serving the removed block-pools\n"+
                              "\t\tand starts serving new block-pools\n";
    
    String deleteBlockPool = "-deleteBlockPool: Arguments are datanodehost:port, blockpool id\n"+
                             "\t\t and an optional argument \"force\". If force is passed,\n"+
                             "\t\t block pool directory for the given blockpool id on the given\n"+
                             "\t\t datanode is deleted along with its contents, otherwise\n"+
                             "\t\t the directory is deleted only if it is empty. The command\n" +
                             "\t\t will fail if datanode is still serving the block pool.\n" +
                             "\t\t   Refer to refreshNamenodes to shutdown a block pool\n" +
                             "\t\t service on a datanode.\n";

    String setBalancerBandwidth = "-setBalancerBandwidth <bandwidth>:\n" +
      "\tChanges the network bandwidth used by each datanode during\n" +
      "\tHDFS block balancing.\n\n" +
      "\t\t<bandwidth> is the maximum number of bytes per second\n" +
      "\t\tthat will be used by each datanode. This value overrides\n" +
      "\t\tthe dfs.balance.bandwidthPerSec parameter.\n\n" +
      "\t\t--- NOTE: The new value is not persistent on the DataNode.---\n";
    
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

    String getDatanodeInfo = "-getDatanodeInfo <datanode_host:ipc_port>\n"
        + "\tGet the information about the given datanode. This command can\n"
        + "\tbe used for checking if a datanode is alive.\n";
    
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
    } else if ("printTopology".equals(cmd)) {
      System.out.println(printTopology);
    } else if ("refreshNamenodes".equals(cmd)) {
      System.out.println(refreshNamenodes);
    } else if ("deleteBlockPool".equals(cmd)) {
      System.out.println(deleteBlockPool);
    } else if ("setBalancerBandwidth".equals(cmd)) {
      System.out.println(setBalancerBandwidth);
    } else if ("fetchImage".equals(cmd)) {
      System.out.println(fetchImage);
    } else if ("allowSnapshot".equalsIgnoreCase(cmd)) {
      System.out.println(allowSnapshot);
    } else if ("disallowSnapshot".equalsIgnoreCase(cmd)) {
      System.out.println(disallowSnapshot);
    } else if ("shutdownDatanode".equalsIgnoreCase(cmd)) {
      System.out.println(shutdownDatanode);
    } else if ("getDatanodeInfo".equalsIgnoreCase(cmd)) {
      System.out.println(getDatanodeInfo);
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
      System.out.println(printTopology);
      System.out.println(refreshNamenodes);
      System.out.println(deleteBlockPool);
      System.out.println(setBalancerBandwidth);
      System.out.println(fetchImage);
      System.out.println(allowSnapshot);
      System.out.println(disallowSnapshot);
      System.out.println(shutdownDatanode);
      System.out.println(getDatanodeInfo);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }

  }

  /**
   * Command to ask the namenode to finalize previously performed upgrade.
   * Usage: java DFSAdmin -finalizeUpgrade
   * @exception IOException 
   */
  public int finalizeUpgrade() throws IOException {
    DistributedFileSystem dfs = getDFS();
    
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaAndLogicalUri = HAUtil.isLogicalUri(dfsConf, dfsUri);
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
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        proxy.getProxy().finalizeUpgrade();
        System.out.println("Finalize upgrade successful for " +
            proxy.getAddress());
      }
    } else {
      dfs.finalizeUpgrade();
      System.out.println("Finalize upgrade successful");
    }
    
    return 0;
  }

  /**
   * Dumps DFS data structures into specified file.
   * Usage: java DFSAdmin -metasave filename
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
    boolean isHaEnabled = HAUtil.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf,
          nsId, ClientProtocol.class);
      for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
        proxy.getProxy().metaSave(pathname);
        System.out.println("Created metasave file " + pathname + " in the log "
            + "directory of namenode " + proxy.getAddress());
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
    boolean isHaEnabled = HAUtil.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {
      // Run refreshServiceAcl for all NNs if HA is enabled
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<RefreshAuthorizationPolicyProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
              RefreshAuthorizationPolicyProtocol.class);
      for (ProxyAndInfo<RefreshAuthorizationPolicyProtocol> proxy : proxies) {
        proxy.getProxy().refreshServiceAcl();
        System.out.println("Refresh service acl successful for "
            + proxy.getAddress());
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
    boolean isHaEnabled = HAUtil.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {
      // Run refreshUserToGroupsMapings for all NNs if HA is enabled
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<RefreshUserMappingsProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
              RefreshUserMappingsProtocol.class);
      for (ProxyAndInfo<RefreshUserMappingsProtocol> proxy : proxies) {
        proxy.getProxy().refreshUserToGroupsMappings();
        System.out.println("Refresh user to groups mapping successful for "
            + proxy.getAddress());
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
    boolean isHaEnabled = HAUtil.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {
      // Run refreshSuperUserGroupsConfiguration for all NNs if HA is enabled
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<RefreshUserMappingsProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
              RefreshUserMappingsProtocol.class);
      for (ProxyAndInfo<RefreshUserMappingsProtocol> proxy : proxies) {
        proxy.getProxy().refreshSuperUserGroupsConfiguration();
        System.out.println("Refresh super user groups configuration " +
            "successful for " + proxy.getAddress());
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
    boolean isHaEnabled = HAUtil.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {
      // Run refreshCallQueue for all NNs if HA is enabled
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<RefreshCallQueueProtocol>> proxies =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
              RefreshCallQueueProtocol.class);
      for (ProxyAndInfo<RefreshCallQueueProtocol> proxy : proxies) {
        proxy.getProxy().refreshCallQueue();
        System.out.println("Refresh call queue successful for "
            + proxy.getAddress());
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

    GenericRefreshProtocol xlator =
      new GenericRefreshProtocolClientSideTranslatorPB(proxy);

    // Refresh
    Collection<RefreshResponse> responses = xlator.refresh(identifier, args);

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
        returnCode = -1;
      }
    }

    return returnCode;
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-report".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-report] [-live] [-dead] [-decommissioning]");
    } else if ("-safemode".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-safemode enter | leave | get | wait]");
    } else if ("-allowSnapshot".equalsIgnoreCase(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-allowSnapshot <snapshotDir>]");
    } else if ("-disallowSnapshot".equalsIgnoreCase(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-disallowSnapshot <snapshotDir>]");
    } else if ("-saveNamespace".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-saveNamespace]");
    } else if ("-rollEdits".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-rollEdits]");
    } else if ("-restoreFailedStorage".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-restoreFailedStorage true|false|check ]");
    } else if ("-refreshNodes".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshNodes]");
    } else if ("-finalizeUpgrade".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-finalizeUpgrade]");
    } else if (RollingUpgradeCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [" + RollingUpgradeCommand.USAGE+"]");
    } else if ("-metasave".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-metasave filename]");
    } else if (SetQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [" + SetQuotaCommand.USAGE+"]");
    } else if (ClearQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " ["+ClearQuotaCommand.USAGE+"]");
    } else if (SetSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [" + SetSpaceQuotaCommand.USAGE+"]");
    } else if (ClearSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " ["+ClearSpaceQuotaCommand.USAGE+"]");
    } else if ("-refreshServiceAcl".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshServiceAcl]");
    } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshUserToGroupsMappings]");
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshSuperUserGroupsConfiguration]");
    } else if ("-refreshCallQueue".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshCallQueue]");
    } else if ("-refresh".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refresh <hostname:port> <resource_identifier> [arg1..argn]");
    } else if ("-printTopology".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-printTopology]");
    } else if ("-refreshNamenodes".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshNamenodes datanode-host:port]");
    } else if ("-deleteBlockPool".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-deleteBlockPool datanode-host:port blockpoolId [force]]");
    } else if ("-setBalancerBandwidth".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                  + " [-setBalancerBandwidth <bandwidth in bytes per second>]");
    } else if ("-fetchImage".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-fetchImage <local directory>]");
    } else if ("-shutdownDatanode".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-shutdownDatanode <datanode_host:ipc_port> [upgrade]]");
    } else if ("-getDatanodeInfo".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-getDatanodeInfo <datanode_host:ipc_port>]");
    } else {
      System.err.println("Usage: java DFSAdmin");
      System.err.println("Note: Administrative commands can only be run as the HDFS superuser.");
      System.err.println("           [-report]");
      System.err.println("           [-safemode enter | leave | get | wait]"); 
      System.err.println("           [-allowSnapshot <snapshotDir>]");
      System.err.println("           [-disallowSnapshot <snapshotDir>]");
      System.err.println("           [-saveNamespace]");
      System.err.println("           [-rollEdits]");
      System.err.println("           [-restoreFailedStorage true|false|check]");
      System.err.println("           [-refreshNodes]");
      System.err.println("           [-finalizeUpgrade]");
      System.err.println("           ["+RollingUpgradeCommand.USAGE+"]");
      System.err.println("           [-metasave filename]");
      System.err.println("           [-refreshServiceAcl]");
      System.err.println("           [-refreshUserToGroupsMappings]");
      System.err.println("           [-refreshSuperUserGroupsConfiguration]");
      System.err.println("           [-refreshCallQueue]");
      System.err.println("           [-refresh]");
      System.err.println("           [-printTopology]");
      System.err.println("           [-refreshNamenodes datanodehost:port]");
      System.err.println("           [-deleteBlockPool datanode-host:port blockpoolId [force]]");
      System.err.println("           ["+SetQuotaCommand.USAGE+"]");
      System.err.println("           ["+ClearQuotaCommand.USAGE+"]");
      System.err.println("           ["+SetSpaceQuotaCommand.USAGE+"]");
      System.err.println("           ["+ClearSpaceQuotaCommand.USAGE+"]");      
      System.err.println("           [-setBalancerBandwidth <bandwidth in bytes per second>]");
      System.err.println("           [-fetchImage <local directory>]");
      System.err.println("           [-shutdownDatanode <datanode_host:ipc_port> [upgrade]]");
      System.err.println("           [-getDatanodeInfo <datanode_host:ipc_port>]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
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
      if (argv.length < 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-saveNamespace".equals(cmd)) {
      if (argv.length != 1) {
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
      if (argv.length < 1 || argv.length > 2) {
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
        exitCode = saveNamespace();
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
      } else if ("-metasave".equals(cmd)) {
        exitCode = metaSave(argv, i);
      } else if (ClearQuotaCommand.matches(cmd)) {
        exitCode = new ClearQuotaCommand(argv, i, getDFS()).runAll();
      } else if (SetQuotaCommand.matches(cmd)) {
        exitCode = new SetQuotaCommand(argv, i, getDFS()).runAll();
      } else if (ClearSpaceQuotaCommand.matches(cmd)) {
        exitCode = new ClearSpaceQuotaCommand(argv, i, getDFS()).runAll();
      } else if (SetSpaceQuotaCommand.matches(cmd)) {
        exitCode = new SetSpaceQuotaCommand(argv, i, getDFS()).runAll();
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
      } else if ("-deleteBlockPool".equals(cmd)) {
        exitCode = deleteBlockPool(argv, i);
      } else if ("-setBalancerBandwidth".equals(cmd)) {
        exitCode = setBalancerBandwidth(argv, i);
      } else if ("-fetchImage".equals(cmd)) {
        exitCode = fetchImage(argv, i);
      } else if ("-shutdownDatanode".equals(cmd)) {
        exitCode = shutdownDatanode(argv, i);
      } else if ("-getDatanodeInfo".equals(cmd)) {
        exitCode = getDatanodeInfo(argv, i);
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exception encountered:", debugException);
    }
    return exitCode;
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
        DFSUtil.createClientDatanodeProtocolProxy(datanodeAddr, getUGI(), conf,
            NetUtils.getSocketFactory(conf, ClientDatanodeProtocol.class));
    return dnProtocol;
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

  private int getDatanodeInfo(String[] argv, int i) throws IOException {
    ClientDatanodeProtocol dnProxy = getDataNodeProxy(argv[i]);
    try {
      DatanodeLocalInfo dnInfo = dnProxy.getDatanodeInfo();
      System.out.println(dnInfo.getDatanodeLocalReport());
    } catch (IOException ioe) {
      System.err.println("Datanode unreachable.");
      return -1;
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
