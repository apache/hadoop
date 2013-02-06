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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERSIST_BLOCKS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_KEY;
import static org.apache.hadoop.util.Time.now;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.namenode.ha.StandbyCheckpointer;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * FSNamesystem is a container of both transient
 * and persisted name-space state, and does all the book-keeping
 * work on a NameNode.
 *
 * Its roles are briefly described below:
 *
 * 1) Is the container for BlockManager, DatanodeManager,
 *    DelegationTokens, LeaseManager, etc. services.
 * 2) RPC calls that modify or inspect the name-space
 *    should get delegated here.
 * 3) Anything that touches only blocks (eg. block reports),
 *    it delegates to BlockManager.
 * 4) Anything that touches only file information (eg. permissions, mkdirs),
 *    it delegates to FSDirectory.
 * 5) Anything that crosses two of the above components should be
 *    coordinated here.
 * 6) Logs mutations to FSEditLog.
 *
 * This class and its contents keep:
 *
 * 1)  Valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 */
@InterfaceAudience.Private
@Metrics(context="dfs")
public class FSNamesystem implements Namesystem, FSClusterStats,
    FSNamesystemMBean, NameNodeMXBean {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);

  private static final ThreadLocal<StringBuilder> auditBuffer =
    new ThreadLocal<StringBuilder>() {
      @Override
      protected StringBuilder initialValue() {
        return new StringBuilder();
      }
  };

  private boolean isAuditEnabled() {
    return !isDefaultAuditLogger || auditLog.isInfoEnabled();
  }

  private void logAuditEvent(UserGroupInformation ugi,
      InetAddress addr, String cmd, String src, String dst,
      HdfsFileStatus stat) {
    logAuditEvent(true, ugi, addr, cmd, src, dst, stat);
  }

  private void logAuditEvent(boolean succeeded,
      UserGroupInformation ugi, InetAddress addr, String cmd, String src,
      String dst, HdfsFileStatus stat) {
    FileStatus status = null;
    if (stat != null) {
      Path symlink = stat.isSymlink() ? new Path(stat.getSymlink()) : null;
      Path path = dst != null ? new Path(dst) : new Path(src);
      status = new FileStatus(stat.getLen(), stat.isDir(),
          stat.getReplication(), stat.getBlockSize(), stat.getModificationTime(),
          stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
          stat.getGroup(), symlink, path);
    }
    for (AuditLogger logger : auditLoggers) {
      logger.logAuditEvent(succeeded, ugi.toString(), addr,
          cmd, src, dst, status);
    }
  }

  /**
   * Logger for audit events, noting successful FSNamesystem operations. Emits
   * to FSNamesystem.audit at INFO. Each event causes a set of tab-separated
   * <code>key=value</code> pairs to be written for the following properties:
   * <code>
   * ugi=&lt;ugi in RPC&gt;
   * ip=&lt;remote IP&gt;
   * cmd=&lt;command&gt;
   * src=&lt;src path&gt;
   * dst=&lt;dst path (optional)&gt;
   * perm=&lt;permissions (optional)&gt;
   * </code>
   */
  public static final Log auditLog = LogFactory.getLog(
      FSNamesystem.class.getName() + ".audit");

  static final int DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED = 100;
  static int BLOCK_DELETION_INCREMENT = 1000;
  private final boolean isPermissionEnabled;
  private final boolean persistBlocks;
  private final UserGroupInformation fsOwner;
  private final String supergroup;
  private final boolean standbyShouldCheckpoint;
  
  // Scan interval is not configurable.
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
    TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  final DelegationTokenSecretManager dtSecretManager;
  private final boolean alwaysUseDelegationTokensForTests;
  
  // Tracks whether the default audit logger is the only configured audit
  // logger; this allows isAuditEnabled() to return false in case the
  // underlying logger is disabled, and avoid some unnecessary work.
  private final boolean isDefaultAuditLogger;
  private final List<AuditLogger> auditLoggers;

  /** The namespace tree. */
  FSDirectory dir;
  private final BlockManager blockManager;
  private final DatanodeStatistics datanodeStatistics;

  // Block pool ID used by this namenode
  private String blockPoolId;

  final LeaseManager leaseManager = new LeaseManager(this); 

  Daemon smmthread = null;  // SafeModeMonitor thread
  
  Daemon nnrmthread = null; // NamenodeResourceMonitor thread

  private volatile boolean hasResourcesAvailable = false;
  private volatile boolean fsRunning = true;
  
  /** The start time of the namesystem. */
  private final long startTime = now();

  /** The interval of namenode checking for the disk space availability */
  private final long resourceRecheckInterval;

  // The actual resource checker instance.
  NameNodeResourceChecker nnResourceChecker;

  private final FsServerDefaults serverDefaults;
  private final boolean supportAppends;
  private final ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;

  private volatile SafeModeInfo safeMode;  // safe mode information

  private final long maxFsObjects;          // maximum number of fs objects

  /**
   * The global generation stamp for this file system. 
   */
  private final GenerationStamp generationStamp = new GenerationStamp();

  // precision of access times.
  private final long accessTimePrecision;

  /** Lock to protect FSNamesystem. */
  private ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);

  /**
   * Used when this NN is in standby state to read from the shared edit log.
   */
  private EditLogTailer editLogTailer = null;

  /**
   * Used when this NN is in standby state to perform checkpoints.
   */
  private StandbyCheckpointer standbyCheckpointer;

  /**
   * Reference to the NN's HAContext object. This is only set once
   * {@link #startCommonServices(Configuration, HAContext)} is called. 
   */
  private HAContext haContext;

  private final boolean haEnabled;
    
  private INodeId inodeId;
  
  /**
   * Set the last allocated inode id when fsimage or editlog is loaded. 
   */
  public void resetLastInodeId(long newValue) throws IOException {
    try {
      inodeId.skipTo(newValue);
    } catch(IllegalStateException ise) {
      throw new IOException(ise);
    }
  }

  /** Should only be used for tests to reset to any value */
  void resetLastInodeIdWithoutChecking(long newValue) {
    inodeId.setCurrentValue(newValue);
  }
  
  /** @return the last inode ID. */
  public long getLastInodeId() {
    return inodeId.getCurrentValue();
  }

  /** Allocate a new inode ID. */
  public long allocateNewInodeId() {
    return inodeId.nextValue();
  }
  
  /**
   * Clear all loaded data
   */
  void clear() {
    dir.reset();
    dtSecretManager.reset();
    generationStamp.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    leaseManager.removeAllLeases();
    inodeId.setCurrentValue(INodeId.LAST_RESERVED_ID);
  }

  @VisibleForTesting
  LeaseManager getLeaseManager() {
    return leaseManager;
  }
  
  /**

  /**
   * Instantiates an FSNamesystem loaded from the image and edits
   * directories specified in the passed Configuration.
   * 
   * @param conf the Configuration which specifies the storage directories
   *             from which to load
   * @return an FSNamesystem which contains the loaded namespace
   * @throws IOException if loading fails
   */
  public static FSNamesystem loadFromDisk(Configuration conf)
      throws IOException {
    Collection<URI> namespaceDirs = FSNamesystem.getNamespaceDirs(conf);
    List<URI> namespaceEditsDirs = 
      FSNamesystem.getNamespaceEditsDirs(conf);
    return loadFromDisk(conf, namespaceDirs, namespaceEditsDirs);
  }

  /**
   * Instantiates an FSNamesystem loaded from the image and edits
   * directories passed.
   * 
   * @param conf the Configuration which specifies the storage directories
   *             from which to load
   * @param namespaceDirs directories to load the fsimages
   * @param namespaceEditsDirs directories to load the edits from
   * @return an FSNamesystem which contains the loaded namespace
   * @throws IOException if loading fails
   */
  public static FSNamesystem loadFromDisk(Configuration conf,
      Collection<URI> namespaceDirs, List<URI> namespaceEditsDirs)
      throws IOException {

    if (namespaceDirs.size() == 1) {
      LOG.warn("Only one image storage directory ("
          + DFS_NAMENODE_NAME_DIR_KEY + ") configured. Beware of dataloss"
          + " due to lack of redundant storage directories!");
    }
    if (namespaceEditsDirs.size() == 1) {
      LOG.warn("Only one namespace edits storage directory ("
          + DFS_NAMENODE_EDITS_DIR_KEY + ") configured. Beware of dataloss"
          + " due to lack of redundant storage directories!");
    }

    FSImage fsImage = new FSImage(conf, namespaceDirs, namespaceEditsDirs);
    FSNamesystem namesystem = new FSNamesystem(conf, fsImage);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if (startOpt == StartupOption.RECOVER) {
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    }

    long loadStart = now();
    String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
    namesystem.loadFSImage(startOpt, fsImage,
      HAUtil.isHAEnabled(conf, nameserviceId));
    long timeTakenToLoadFSImage = now() - loadStart;
    LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
    NameNodeMetrics nnMetrics = NameNode.getNameNodeMetrics();
    if (nnMetrics != null) {
      nnMetrics.setFsImageLoadTime((int) timeTakenToLoadFSImage);
    }
    return namesystem;
  }

  /**
   * Create an FSNamesystem associated with the specified image.
   * 
   * Note that this does not load any data off of disk -- if you would
   * like that behavior, use {@link #loadFromDisk(Configuration)}

   * @param fnImage The FSImage to associate with
   * @param conf configuration
   * @throws IOException on bad configuration
   */
  FSNamesystem(Configuration conf, FSImage fsImage) throws IOException {
    try {
      resourceRecheckInterval = conf.getLong(
          DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
          DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);

      this.blockManager = new BlockManager(this, this, conf);
      this.datanodeStatistics = blockManager.getDatanodeManager().getDatanodeStatistics();

      this.fsOwner = UserGroupInformation.getCurrentUser();
      this.supergroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY, 
                                 DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
      this.isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
                                                 DFS_PERMISSIONS_ENABLED_DEFAULT);
      LOG.info("fsOwner             = " + fsOwner);
      LOG.info("supergroup          = " + supergroup);
      LOG.info("isPermissionEnabled = " + isPermissionEnabled);

      final boolean persistBlocks = conf.getBoolean(DFS_PERSIST_BLOCKS_KEY,
                                                    DFS_PERSIST_BLOCKS_DEFAULT);
      // block allocation has to be persisted in HA using a shared edits directory
      // so that the standby has up-to-date namespace information
      String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
      this.haEnabled = HAUtil.isHAEnabled(conf, nameserviceId);  
      this.persistBlocks = persistBlocks || (haEnabled && HAUtil.usesSharedEditsDir(conf));
      
      // Sanity check the HA-related config.
      if (nameserviceId != null) {
        LOG.info("Determined nameservice ID: " + nameserviceId);
      }
      LOG.info("HA Enabled: " + haEnabled);
      if (!haEnabled && HAUtil.usesSharedEditsDir(conf)) {
        LOG.warn("Configured NNs:\n" + DFSUtil.nnAddressesAsString(conf));
        throw new IOException("Invalid configuration: a shared edits dir " +
            "must not be specified if HA is not enabled.");
      }

      // Get the checksum type from config
      String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY, DFS_CHECKSUM_TYPE_DEFAULT);
      DataChecksum.Type checksumType;
      try {
         checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
      } catch (IllegalArgumentException iae) {
         throw new IOException("Invalid checksum type in "
            + DFS_CHECKSUM_TYPE_KEY + ": " + checksumTypeStr);
      }

      this.serverDefaults = new FsServerDefaults(
          conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
          conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
          conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
          (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
          conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
          conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
          conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
          checksumType);
      
      this.maxFsObjects = conf.getLong(DFS_NAMENODE_MAX_OBJECTS_KEY, 
                                       DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

      this.accessTimePrecision = conf.getLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 0);
      this.supportAppends = conf.getBoolean(DFS_SUPPORT_APPEND_KEY, DFS_SUPPORT_APPEND_DEFAULT);
      LOG.info("Append Enabled: " + supportAppends);

      this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
      
      this.standbyShouldCheckpoint = conf.getBoolean(
          DFS_HA_STANDBY_CHECKPOINTS_KEY, DFS_HA_STANDBY_CHECKPOINTS_DEFAULT);
      
      this.inodeId = new INodeId();
      
      // For testing purposes, allow the DT secret manager to be started regardless
      // of whether security is enabled.
      alwaysUseDelegationTokensForTests = conf.getBoolean(
          DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
          DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT);

      this.dtSecretManager = createDelegationTokenSecretManager(conf);
      this.dir = new FSDirectory(fsImage, this, conf);
      this.safeMode = new SafeModeInfo(conf);
      this.auditLoggers = initAuditLoggers(conf);
      this.isDefaultAuditLogger = auditLoggers.size() == 1 &&
        auditLoggers.get(0) instanceof DefaultAuditLogger;
    } catch(IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    } catch (RuntimeException re) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", re);
      close();
      throw re;
    }
  }

  private List<AuditLogger> initAuditLoggers(Configuration conf) {
    // Initialize the custom access loggers if configured.
    Collection<String> alClasses = conf.getStringCollection(DFS_NAMENODE_AUDIT_LOGGERS_KEY);
    List<AuditLogger> auditLoggers = Lists.newArrayList();
    if (alClasses != null && !alClasses.isEmpty()) {
      for (String className : alClasses) {
        try {
          AuditLogger logger;
          if (DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME.equals(className)) {
            logger = new DefaultAuditLogger();
          } else {
            logger = (AuditLogger) Class.forName(className).newInstance();
          }
          logger.initialize(conf);
          auditLoggers.add(logger);
        } catch (RuntimeException re) {
          throw re;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Make sure there is at least one logger installed.
    if (auditLoggers.isEmpty()) {
      auditLoggers.add(new DefaultAuditLogger());
    }
    return auditLoggers;
  }

  void loadFSImage(StartupOption startOpt, FSImage fsImage, boolean haEnabled)
      throws IOException {
    // format before starting up if requested
    if (startOpt == StartupOption.FORMAT) {
      
      fsImage.format(this, fsImage.getStorage().determineClusterId());// reuse current id

      startOpt = StartupOption.REGULAR;
    }
    boolean success = false;
    writeLock();
    try {
      // We shouldn't be calling saveNamespace if we've come up in standby state.
      MetaRecoveryContext recovery = startOpt.createRecoveryContext();
      if (fsImage.recoverTransitionRead(startOpt, this, recovery) && !haEnabled) {
        fsImage.saveNamespace(this);
      }
      // This will start a new log segment and write to the seen_txid file, so
      // we shouldn't do it when coming up in standby state
      if (!haEnabled) {
        fsImage.openEditLogForWrite();
      }
      
      success = true;
    } finally {
      if (!success) {
        fsImage.close();
      }
      writeUnlock();
    }
    dir.imageLoadComplete();
  }

  private void startSecretManager() {
    if (dtSecretManager != null) {
      try {
        dtSecretManager.startThreads();
      } catch (IOException e) {
        // Inability to start secret manager
        // can't be recovered from.
        throw new RuntimeException(e);
      }
    }
  }
  
  private void startSecretManagerIfNecessary() {
    boolean shouldRun = shouldUseDelegationTokens() &&
      !isInSafeMode() && getEditLog().isOpenForWrite();
    boolean running = dtSecretManager.isRunning();
    if (shouldRun && !running) {
      startSecretManager();
    }
  }

  private void stopSecretManager() {
    if (dtSecretManager != null) {
      dtSecretManager.stopThreads();
    }
  }
  
  /** 
   * Start services common to both active and standby states
   * @param haContext 
   * @throws IOException
   */
  void startCommonServices(Configuration conf, HAContext haContext) throws IOException {
    this.registerMBean(); // register the MBean for the FSNamesystemState
    writeLock();
    this.haContext = haContext;
    try {
      nnResourceChecker = new NameNodeResourceChecker(conf);
      checkAvailableResources();
      assert safeMode != null &&
        !safeMode.isPopulatingReplQueues();
      setBlockTotal();
      blockManager.activate(conf);
    } finally {
      writeUnlock();
    }
    
    registerMXBean();
    DefaultMetricsSystem.instance().register(this);
  }
  
  /** 
   * Stop services common to both active and standby states
   * @throws IOException
   */
  void stopCommonServices() {
    writeLock();
    try {
      if (blockManager != null) blockManager.close();
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Start services required in active state
   * @throws IOException
   */
  void startActiveServices() throws IOException {
    LOG.info("Starting services required for active state");
    writeLock();
    try {
      FSEditLog editLog = dir.fsImage.getEditLog();
      
      if (!editLog.isOpenForWrite()) {
        // During startup, we're already open for write during initialization.
        editLog.initJournalsForWrite();
        // May need to recover
        editLog.recoverUnclosedStreams();
        
        LOG.info("Catching up to latest edits from old active before " +
            "taking over writer role in edits logs");
        editLogTailer.catchupDuringFailover();
        
        blockManager.setPostponeBlocksFromFuture(false);
        blockManager.getDatanodeManager().markAllDatanodesStale();
        blockManager.clearQueues();
        blockManager.processAllPendingDNMessages();
        
        if (!isInSafeMode() ||
            (isInSafeMode() && safeMode.isPopulatingReplQueues())) {
          LOG.info("Reprocessing replication and invalidation queues");
          blockManager.processMisReplicatedBlocks();
        }
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("NameNode metadata after re-processing " +
              "replication and invalidation queues during failover:\n" +
              metaSaveAsString());
        }
        
        long nextTxId = dir.fsImage.getLastAppliedTxId() + 1;
        LOG.info("Will take over writing edit logs at txnid " + 
            nextTxId);
        editLog.setNextTxId(nextTxId);

        dir.fsImage.editLog.openForWrite();
      }
      if (haEnabled) {
        // Renew all of the leases before becoming active.
        // This is because, while we were in standby mode,
        // the leases weren't getting renewed on this NN.
        // Give them all a fresh start here.
        leaseManager.renewAllLeases();
      }
      leaseManager.startMonitor();
      startSecretManagerIfNecessary();

      //ResourceMonitor required only at ActiveNN. See HDFS-2914
      this.nnrmthread = new Daemon(new NameNodeResourceMonitor());
      nnrmthread.start();
    } finally {
      writeUnlock();
    }
  }

  private boolean shouldUseDelegationTokens() {
    return UserGroupInformation.isSecurityEnabled() ||
      alwaysUseDelegationTokensForTests;
  }

  /** 
   * Stop services required in active state
   * @throws InterruptedException
   */
  void stopActiveServices() {
    LOG.info("Stopping services started for active state");
    writeLock();
    try {
      stopSecretManager();
      if (leaseManager != null) {
        leaseManager.stopMonitor();
      }
      if (nnrmthread != null) {
        ((NameNodeResourceMonitor) nnrmthread.getRunnable()).stopMonitor();
        nnrmthread.interrupt();
      }
      if (dir != null && dir.fsImage != null) {
        if (dir.fsImage.editLog != null) {
          dir.fsImage.editLog.close();
        }
        // Update the fsimage with the last txid that we wrote
        // so that the tailer starts from the right spot.
        dir.fsImage.updateLastAppliedTxIdFromWritten();
      }
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Start services required in standby state 
   * 
   * @throws IOException
   */
  void startStandbyServices(final Configuration conf) throws IOException {
    LOG.info("Starting services required for standby state");
    if (!dir.fsImage.editLog.isOpenForRead()) {
      // During startup, we're already open for read.
      dir.fsImage.editLog.initSharedJournalsForRead();
    }
    
    blockManager.setPostponeBlocksFromFuture(true);

    editLogTailer = new EditLogTailer(this, conf);
    editLogTailer.start();
    if (standbyShouldCheckpoint) {
      standbyCheckpointer = new StandbyCheckpointer(conf, this);
      standbyCheckpointer.start();
    }
  }


  /**
   * Called while the NN is in Standby state, but just about to be
   * asked to enter Active state. This cancels any checkpoints
   * currently being taken.
   */
  void prepareToStopStandbyServices() throws ServiceFailedException {
    if (standbyCheckpointer != null) {
      standbyCheckpointer.cancelAndPreventCheckpoints(
          "About to leave standby state");
    }
  }

  /** Stop services required in standby state */
  void stopStandbyServices() throws IOException {
    LOG.info("Stopping services started for standby state");
    if (standbyCheckpointer != null) {
      standbyCheckpointer.stop();
    }
    if (editLogTailer != null) {
      editLogTailer.stop();
    }
    if (dir != null && dir.fsImage != null && dir.fsImage.editLog != null) {
      dir.fsImage.editLog.close();
    }
  }
  
  
  public void checkOperation(OperationCategory op) throws StandbyException {
    if (haContext != null) {
      // null in some unit tests
      haContext.checkOperation(op);
    }
  }
  
  public static Collection<URI> getNamespaceDirs(Configuration conf) {
    return getStorageDirs(conf, DFS_NAMENODE_NAME_DIR_KEY);
  }

  /**
   * Get all edits dirs which are required. If any shared edits dirs are
   * configured, these are also included in the set of required dirs.
   * 
   * @param conf the HDFS configuration.
   * @return all required dirs.
   */
  public static Collection<URI> getRequiredNamespaceEditsDirs(Configuration conf) {
    Set<URI> ret = new HashSet<URI>();
    ret.addAll(getStorageDirs(conf, DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY));
    ret.addAll(getSharedEditsDirs(conf));
    return ret;
  }

  private static Collection<URI> getStorageDirs(Configuration conf,
                                                String propertyName) {
    Collection<String> dirNames = conf.getTrimmedStringCollection(propertyName);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if(startOpt == StartupOption.IMPORT) {
      // In case of IMPORT this will get rid of default directories 
      // but will retain directories specified in hdfs-site.xml
      // When importing image from a checkpoint, the name-node can
      // start with empty set of storage directories.
      Configuration cE = new HdfsConfiguration(false);
      cE.addResource("core-default.xml");
      cE.addResource("core-site.xml");
      cE.addResource("hdfs-default.xml");
      Collection<String> dirNames2 = cE.getTrimmedStringCollection(propertyName);
      dirNames.removeAll(dirNames2);
      if(dirNames.isEmpty())
        LOG.warn("!!! WARNING !!!" +
          "\n\tThe NameNode currently runs without persistent storage." +
          "\n\tAny changes to the file system meta-data may be lost." +
          "\n\tRecommended actions:" +
          "\n\t\t- shutdown and restart NameNode with configured \"" 
          + propertyName + "\" in hdfs-site.xml;" +
          "\n\t\t- use Backup Node as a persistent and up-to-date storage " +
          "of the file system meta-data.");
    } else if (dirNames.isEmpty()) {
      dirNames = Collections.singletonList("file:///tmp/hadoop/dfs/name");
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  /**
   * Return an ordered list of edits directories to write to.
   * The list is ordered such that all shared edits directories
   * are ordered before non-shared directories, and any duplicates
   * are removed. The order they are specified in the configuration
   * is retained.
   * @return Collection of shared edits directories.
   * @throws IOException if multiple shared edits directories are configured
   */
  public static List<URI> getNamespaceEditsDirs(Configuration conf)
      throws IOException {
    return getNamespaceEditsDirs(conf, true);
  }
  
  public static List<URI> getNamespaceEditsDirs(Configuration conf,
      boolean includeShared)
      throws IOException {
    // Use a LinkedHashSet so that order is maintained while we de-dup
    // the entries.
    LinkedHashSet<URI> editsDirs = new LinkedHashSet<URI>();
    
    if (includeShared) {
      List<URI> sharedDirs = getSharedEditsDirs(conf);
  
      // Fail until multiple shared edits directories are supported (HDFS-2782)
      if (sharedDirs.size() > 1) {
        throw new IOException(
            "Multiple shared edits directories are not yet supported");
      }
  
      // First add the shared edits dirs. It's critical that the shared dirs
      // are added first, since JournalSet syncs them in the order they are listed,
      // and we need to make sure all edits are in place in the shared storage
      // before they are replicated locally. See HDFS-2874.
      for (URI dir : sharedDirs) {
        if (!editsDirs.add(dir)) {
          LOG.warn("Edits URI " + dir + " listed multiple times in " + 
              DFS_NAMENODE_SHARED_EDITS_DIR_KEY + ". Ignoring duplicates.");
        }
      }
    }    
    // Now add the non-shared dirs.
    for (URI dir : getStorageDirs(conf, DFS_NAMENODE_EDITS_DIR_KEY)) {
      if (!editsDirs.add(dir)) {
        LOG.warn("Edits URI " + dir + " listed multiple times in " + 
            DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " and " +
            DFS_NAMENODE_EDITS_DIR_KEY + ". Ignoring duplicates.");
      }
    }

    if (editsDirs.isEmpty()) {
      // If this is the case, no edit dirs have been explicitly configured.
      // Image dirs are to be used for edits too.
      return Lists.newArrayList(getNamespaceDirs(conf));
    } else {
      return Lists.newArrayList(editsDirs);
    }
  }
  
  /**
   * Returns edit directories that are shared between primary and secondary.
   * @param conf
   * @return Collection of edit directories.
   */
  public static List<URI> getSharedEditsDirs(Configuration conf) {
    // don't use getStorageDirs here, because we want an empty default
    // rather than the dir in /tmp
    Collection<String> dirNames = conf.getTrimmedStringCollection(
        DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
    return Util.stringCollectionAsURIs(dirNames);
  }

  @Override
  public void readLock() {
    this.fsLock.readLock().lock();
  }
  @Override
  public void readUnlock() {
    this.fsLock.readLock().unlock();
  }
  @Override
  public void writeLock() {
    this.fsLock.writeLock().lock();
  }
  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    this.fsLock.writeLock().lockInterruptibly();
  }
  @Override
  public void writeUnlock() {
    this.fsLock.writeLock().unlock();
  }
  @Override
  public boolean hasWriteLock() {
    return this.fsLock.isWriteLockedByCurrentThread();
  }
  @Override
  public boolean hasReadLock() {
    return this.fsLock.getReadHoldCount() > 0;
  }
  @Override
  public boolean hasReadOrWriteLock() {
    return hasReadLock() || hasWriteLock();
  }

  NamespaceInfo getNamespaceInfo() {
    readLock();
    try {
      return unprotectedGetNamespaceInfo();
    } finally {
      readUnlock();
    }
  }

  /**
   * Version of @see #getNamespaceInfo() that is not protected by a lock.
   */
  NamespaceInfo unprotectedGetNamespaceInfo() {
    return new NamespaceInfo(dir.fsImage.getStorage().getNamespaceID(),
        getClusterId(), getBlockPoolId(),
        dir.fsImage.getStorage().getCTime());
  }

  /**
   * Close down this file system manager.
   * Causes heartbeat and lease daemons to stop; waits briefly for
   * them to finish, but a short timeout returns control back to caller.
   */
  void close() {
    fsRunning = false;
    try {
      stopCommonServices();
      if (smmthread != null) smmthread.interrupt();
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        stopActiveServices();
        stopStandbyServices();
        if (dir != null) {
          dir.close();
        }
      } catch (IOException ie) {
        LOG.error("Error closing FSDirectory", ie);
        IOUtils.cleanup(LOG, dir);
      }
    }
  }

  @Override
  public boolean isRunning() {
    return fsRunning;
  }
  
  @Override
  public boolean isInStandbyState() {
    if (haContext == null || haContext.getState() == null) {
      // We're still starting up. In this case, if HA is
      // on for the cluster, we always start in standby. Otherwise
      // start in active.
      return haEnabled;
    }

    return HAServiceState.STANDBY == haContext.getState().getServiceState();
  }

  /**
   * Dump all metadata into specified file
   */
  void metaSave(String filename) throws IOException {
    writeLock();
    try {
      checkSuperuserPrivilege();
      File file = new File(System.getProperty("hadoop.log.dir"), filename);
      PrintWriter out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(file, true), Charsets.UTF_8)));
      metaSave(out);
      out.flush();
      out.close();
    } finally {
      writeUnlock();
    }
  }

  private void metaSave(PrintWriter out) {
    assert hasWriteLock();
    long totalInodes = this.dir.totalInodes();
    long totalBlocks = this.getBlocksTotal();
    out.println(totalInodes + " files and directories, " + totalBlocks
        + " blocks = " + (totalInodes + totalBlocks)
        + " total filesystem objects");

    blockManager.metaSave(out);
  }

  private String metaSaveAsString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    metaSave(pw);
    pw.flush();
    return sw.toString();
  }
  

  long getDefaultBlockSize() {
    return serverDefaults.getBlockSize();
  }

  FsServerDefaults getServerDefaults() throws StandbyException {
    checkOperation(OperationCategory.READ);
    return serverDefaults;
  }

  long getAccessTimePrecision() {
    return accessTimePrecision;
  }

  private boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  /////////////////////////////////////////////////////////
  /**
   * Set permissions for an existing file.
   * @throws IOException
   */
  void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    try {
      setPermissionInt(src, permission);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "setPermission", src, null, null);
      }
      throw e;
    }
  }

  private void setPermissionInt(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    HdfsFileStatus resultingStat = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set permission for " + src, safeMode);
      }
      checkOwner(src);
      dir.setPermission(src, permission);
      if (isAuditEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(src, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "setPermission", src, null, resultingStat);
    }
  }

  /**
   * Set owner for an existing file.
   * @throws IOException
   */
  void setOwner(String src, String username, String group)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    try {
      setOwnerInt(src, username, group);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "setOwner", src, null, null);
      }
      throw e;
    } 
  }

  private void setOwnerInt(String src, String username, String group)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    HdfsFileStatus resultingStat = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set owner for " + src, safeMode);
      }
      FSPermissionChecker pc = checkOwner(src);
      if (!pc.isSuper) {
        if (username != null && !pc.user.equals(username)) {
          throw new AccessControlException("Non-super user cannot change owner.");
        }
        if (group != null && !pc.containsGroup(group)) {
          throw new AccessControlException("User does not belong to " + group
            + " .");
        }
      }
      dir.setOwner(src, username, group);
      if (isAuditEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(src, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "setOwner", src, null, resultingStat);
    }
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String src,
      long offset, long length) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    LocatedBlocks blocks = getBlockLocations(src, offset, length, true, true,
        true);
    if (blocks != null) {
      blockManager.getDatanodeManager().sortLocatedBlocks(
          clientMachine, blocks.getLocatedBlocks());
      
      LocatedBlock lastBlock = blocks.getLastLocatedBlock();
      if (lastBlock != null) {
        ArrayList<LocatedBlock> lastBlockList = new ArrayList<LocatedBlock>();
        lastBlockList.add(lastBlock);
        blockManager.getDatanodeManager().sortLocatedBlocks(
                              clientMachine, lastBlockList);
      }
    }
    return blocks;
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @throws FileNotFoundException, UnresolvedLinkException, IOException
   */
  LocatedBlocks getBlockLocations(String src, long offset, long length,
      boolean doAccessTime, boolean needBlockToken, boolean checkSafeMode)
      throws FileNotFoundException, UnresolvedLinkException, IOException {
    try {
      return getBlockLocationsInt(src, offset, length, doAccessTime,
                                  needBlockToken, checkSafeMode);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "open", src, null, null);
      }
      throw e;
    }
  }

  private LocatedBlocks getBlockLocationsInt(String src, long offset, long length,
      boolean doAccessTime, boolean needBlockToken, boolean checkSafeMode)
      throws FileNotFoundException, UnresolvedLinkException, IOException {
    if (isPermissionEnabled) {
      checkPathAccess(src, FsAction.READ);
    }

    if (offset < 0) {
      throw new HadoopIllegalArgumentException(
          "Negative offset is not supported. File: " + src);
    }
    if (length < 0) {
      throw new HadoopIllegalArgumentException(
          "Negative length is not supported. File: " + src);
    }
    final LocatedBlocks ret = getBlockLocationsUpdateTimes(src,
        offset, length, doAccessTime, needBlockToken);  
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "open", src, null, null);
    }
    if (checkSafeMode && isInSafeMode()) {
      for (LocatedBlock b : ret.getLocatedBlocks()) {
        // if safemode & no block locations yet then throw safemodeException
        if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
          throw new SafeModeException("Zero blocklocations for " + src,
              safeMode);
        }
      }
    }
    return ret;
  }

  /*
   * Get block locations within the specified range, updating the
   * access times if necessary. 
   */
  private LocatedBlocks getBlockLocationsUpdateTimes(String src,
                                                       long offset, 
                                                       long length,
                                                       boolean doAccessTime, 
                                                       boolean needBlockToken)
      throws FileNotFoundException, UnresolvedLinkException, IOException {

    for (int attempt = 0; attempt < 2; attempt++) {
      if (attempt == 0) { // first attempt is with readlock
        readLock();
      }  else { // second attempt is with  write lock
        writeLock(); // writelock is needed to set accesstime
      }
      try {
        checkOperation(OperationCategory.READ);

        // if the namenode is in safemode, then do not update access time
        if (isInSafeMode()) {
          doAccessTime = false;
        }

        long now = now();
        final INodeFile inode = INodeFile.valueOf(dir.getINode(src), src);
        if (doAccessTime && isAccessTimeSupported()) {
          if (now <= inode.getAccessTime() + getAccessTimePrecision()) {
            // if we have to set access time but we only have the readlock, then
            // restart this entire operation with the writeLock.
            if (attempt == 0) {
              continue;
            }
          }
          dir.setTimes(src, inode, -1, now, false);
        }
        return blockManager.createLocatedBlocks(inode.getBlocks(),
            inode.computeFileSize(false), inode.isUnderConstruction(),
            offset, length, needBlockToken);
      } finally {
        if (attempt == 0) {
          readUnlock();
        } else {
          writeUnlock();
        }
      }
    }
    return null; // can never reach here
  }

  /**
   * Moves all the blocks from srcs and appends them to trg
   * To avoid rollbacks we will verify validitity of ALL of the args
   * before we start actual move.
   * @param target
   * @param srcs
   * @throws IOException
   */
  void concat(String target, String [] srcs) 
      throws IOException, UnresolvedLinkException {
    try {
      concatInt(target, srcs);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getLoginUser(),
                      getRemoteIp(),
                      "concat", Arrays.toString(srcs), target, null);
      }
      throw e;
    }
  }

  private void concatInt(String target, String [] srcs) 
      throws IOException, UnresolvedLinkException {
    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("concat " + Arrays.toString(srcs) +
          " to " + target);
    }
    
    // verify args
    if(target.isEmpty()) {
      throw new IllegalArgumentException("Target file name is empty");
    }
    if(srcs == null || srcs.length == 0) {
      throw new IllegalArgumentException("No sources given");
    }
    
    // We require all files be in the same directory
    String trgParent = 
      target.substring(0, target.lastIndexOf(Path.SEPARATOR_CHAR));
    for (String s : srcs) {
      String srcParent = s.substring(0, s.lastIndexOf(Path.SEPARATOR_CHAR));
      if (!srcParent.equals(trgParent)) {
        throw new IllegalArgumentException(
           "Sources and target are not in the same directory");
      }
    }

    HdfsFileStatus resultingStat = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot concat " + target, safeMode);
      }
      concatInternal(target, srcs);
      if (isAuditEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(target, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getLoginUser(),
                    getRemoteIp(),
                    "concat", Arrays.toString(srcs), target, resultingStat);
    }
  }

  /** See {@link #concat(String, String[])} */
  private void concatInternal(String target, String [] srcs) 
      throws IOException, UnresolvedLinkException {
    assert hasWriteLock();

    // write permission for the target
    if (isPermissionEnabled) {
      checkPathAccess(target, FsAction.WRITE);

      // and srcs
      for(String aSrc: srcs) {
        checkPathAccess(aSrc, FsAction.READ); // read the file
        checkParentAccess(aSrc, FsAction.WRITE); // for delete 
      }
    }

    // to make sure no two files are the same
    Set<INode> si = new HashSet<INode>();

    // we put the following prerequisite for the operation
    // replication and blocks sizes should be the same for ALL the blocks

    // check the target
    final INodeFile trgInode = INodeFile.valueOf(dir.getINode(target), target);
    if(trgInode.isUnderConstruction()) {
      throw new HadoopIllegalArgumentException("concat: target file "
          + target + " is under construction");
    }
    // per design target shouldn't be empty and all the blocks same size
    if(trgInode.numBlocks() == 0) {
      throw new HadoopIllegalArgumentException("concat: target file "
          + target + " is empty");
    }

    long blockSize = trgInode.getPreferredBlockSize();

    // check the end block to be full
    final BlockInfo last = trgInode.getLastBlock();
    if(blockSize != last.getNumBytes()) {
      throw new HadoopIllegalArgumentException("The last block in " + target
          + " is not full; last block size = " + last.getNumBytes()
          + " but file block size = " + blockSize);
    }

    si.add(trgInode);
    short repl = trgInode.getBlockReplication();

    // now check the srcs
    boolean endSrc = false; // final src file doesn't have to have full end block
    for(int i=0; i<srcs.length; i++) {
      String src = srcs[i];
      if(i==srcs.length-1)
        endSrc=true;

      final INodeFile srcInode = INodeFile.valueOf(dir.getINode(src), src);
      if(src.isEmpty() 
          || srcInode.isUnderConstruction()
          || srcInode.numBlocks() == 0) {
        throw new HadoopIllegalArgumentException("concat: source file " + src
            + " is invalid or empty or underConstruction");
      }

      // check replication and blocks size
      if(repl != srcInode.getBlockReplication()) {
        throw new HadoopIllegalArgumentException("concat: the soruce file "
            + src + " and the target file " + target
            + " should have the same replication: source replication is "
            + srcInode.getBlockReplication()
            + " but target replication is " + repl);
      }

      //boolean endBlock=false;
      // verify that all the blocks are of the same length as target
      // should be enough to check the end blocks
      final BlockInfo[] srcBlocks = srcInode.getBlocks();
      int idx = srcBlocks.length-1;
      if(endSrc)
        idx = srcBlocks.length-2; // end block of endSrc is OK not to be full
      if(idx >= 0 && srcBlocks[idx].getNumBytes() != blockSize) {
        throw new HadoopIllegalArgumentException("concat: the soruce file "
            + src + " and the target file " + target
            + " should have the same blocks sizes: target block size is "
            + blockSize + " but the size of source block " + idx + " is "
            + srcBlocks[idx].getNumBytes());
      }

      si.add(srcInode);
    }

    // make sure no two files are the same
    if(si.size() < srcs.length+1) { // trg + srcs
      // it means at least two files are the same
      throw new HadoopIllegalArgumentException(
          "concat: at least two of the source files are the same");
    }

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " + 
          Arrays.toString(srcs) + " to " + target);
    }

    dir.concat(target,srcs);
  }
  
  /**
   * stores the modification and access time for this inode. 
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  void setTimes(String src, long mtime, long atime) 
      throws IOException, UnresolvedLinkException {
    try {
      setTimesInt(src, mtime, atime);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "setTimes", src, null, null);
      }
      throw e;
    }
  }

  private void setTimesInt(String src, long mtime, long atime) 
    throws IOException, UnresolvedLinkException {
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. " +
                            " Please set " + DFS_NAMENODE_ACCESSTIME_PRECISION_KEY + " configuration parameter.");
    }
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      // Write access is required to set access and modification times
      if (isPermissionEnabled) {
        checkPathAccess(src, FsAction.WRITE);
      }
      INode inode = dir.getINode(src);
      if (inode != null) {
        dir.setTimes(src, inode, mtime, atime, true);
        if (isAuditEnabled() && isExternalInvocation()) {
          final HdfsFileStatus stat = dir.getFileInfo(src, false);
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                        getRemoteIp(),
                        "setTimes", src, null, stat);
        }
      } else {
        throw new FileNotFoundException("File/Directory " + src + " does not exist.");
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Create a symbolic link.
   */
  void createSymlink(String target, String link,
      PermissionStatus dirPerms, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    try {
      createSymlinkInt(target, link, dirPerms, createParent);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "createSymlink", link, target, null);
      }
      throw e;
    }
  }

  private void createSymlinkInt(String target, String link,
      PermissionStatus dirPerms, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    HdfsFileStatus resultingStat = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (!createParent) {
        verifyParentDir(link);
      }
      createSymlinkInternal(target, link, dirPerms, createParent);
      if (isAuditEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(link, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "createSymlink", link, target, resultingStat);
    }
  }

  /**
   * Create a symbolic link.
   */
  private void createSymlinkInternal(String target, String link,
      PermissionStatus dirPerms, boolean createParent)
      throws IOException, UnresolvedLinkException {
    assert hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target=" + 
        target + " link=" + link);
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create symlink " + link, safeMode);
    }
    if (!DFSUtil.isValidName(link)) {
      throw new InvalidPathException("Invalid file name: " + link);
    }
    if (!dir.isValidToCreate(link)) {
      throw new IOException("failed to create link " + link 
          +" either because the filename is invalid or the file exists");
    }
    if (isPermissionEnabled) {
      checkAncestorAccess(link, FsAction.WRITE);
    }
    // validate that we have enough inodes.
    checkFsObjectLimit();

    // add symbolic link to namespace
    dir.addSymlink(link, target, dirPerms, createParent);
  }

  /**
   * Set replication for an existing file.
   * 
   * The NameNode sets new replication and schedules either replication of 
   * under-replicated data blocks or removal of the excessive block copies 
   * if the blocks are over-replicated.
   * 
   * @see ClientProtocol#setReplication(String, short)
   * @param src file name
   * @param replication new replication
   * @return true if successful; 
   *         false if file does not exist or is a directory
   */
  boolean setReplication(final String src, final short replication)
      throws IOException {
    try {
      return setReplicationInt(src, replication);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "setReplication", src, null, null);
      }
      throw e;
    }
  }

  private boolean setReplicationInt(final String src, final short replication)
      throws IOException {
    blockManager.verifyReplication(src, replication, null);

    final boolean isFile;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set replication for " + src, safeMode);
      }
      if (isPermissionEnabled) {
        checkPathAccess(src, FsAction.WRITE);
      }

      final short[] oldReplication = new short[1];
      final Block[] blocks = dir.setReplication(src, replication, oldReplication);
      isFile = blocks != null;
      if (isFile) {
        blockManager.setReplication(oldReplication[0], replication, src, blocks);
      }
    } finally {
      writeUnlock();
    }

    getEditLog().logSync();
    if (isFile && isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "setReplication", src, null, null);
    }
    return isFile;
  }

  long getPreferredBlockSize(String filename) 
      throws IOException, UnresolvedLinkException {
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      if (isPermissionEnabled) {
        checkTraverse(filename);
      }
      return dir.getPreferredBlockSize(filename);
    } finally {
      readUnlock();
    }
  }

  /**
   * Verify that parent directory of src exists.
   */
  private void verifyParentDir(String src) throws FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException {
    assert hasReadOrWriteLock();
    Path parent = new Path(src).getParent();
    if (parent != null) {
      final INode parentNode = dir.getINode(parent.toString());
      if (parentNode == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent);
      } else if (!parentNode.isDirectory() && !parentNode.isSymlink()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + parent);
      }
    }
  }

  /**
   * Create a new file entry in the namespace.
   * 
   * For description of parameters and exceptions thrown see 
   * {@link ClientProtocol#create()}, except it returns valid  file status
   * upon success
   */
  HdfsFileStatus startFile(String src, PermissionStatus permissions,
      String holder, String clientMachine, EnumSet<CreateFlag> flag,
      boolean createParent, short replication, long blockSize)
      throws AccessControlException, SafeModeException,
      FileAlreadyExistsException, UnresolvedLinkException,
      FileNotFoundException, ParentNotDirectoryException, IOException {
    try {
      return startFileInt(src, permissions, holder, clientMachine, flag,
          createParent, replication, blockSize);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "create", src, null, null);
      }
      throw e;
    }
  }

  private HdfsFileStatus startFileInt(String src, PermissionStatus permissions,
      String holder, String clientMachine, EnumSet<CreateFlag> flag,
      boolean createParent, short replication, long blockSize)
      throws AccessControlException, SafeModeException,
      FileAlreadyExistsException, UnresolvedLinkException,
      FileNotFoundException, ParentNotDirectoryException, IOException {
    boolean skipSync = false;
    final HdfsFileStatus stat;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      startFileInternal(src, permissions, holder, clientMachine, flag,
          createParent, replication, blockSize);
      stat = dir.getFileInfo(src, false);
    } catch (StandbyException se) {
      skipSync = true;
      throw se;
    } finally {
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      if (!skipSync) {
        getEditLog().logSync();
      }
    } 

    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "create", src, null, stat);
    }
    return stat;
  }

  /**
   * Create new or open an existing file for append.<p>
   * 
   * In case of opening the file for append, the method returns the last
   * block of the file if this is a partial block, which can still be used
   * for writing more data. The client uses the returned block locations
   * to form the data pipeline for this block.<br>
   * The method returns null if the last block is full or if this is a 
   * new file. The client then allocates a new block with the next call
   * using {@link NameNode#addBlock()}.<p>
   *
   * For description of parameters and exceptions thrown see 
   * {@link ClientProtocol#create()}
   * 
   * @return the last block locations if the block is partial or null otherwise
   */
  private LocatedBlock startFileInternal(String src,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize) throws SafeModeException, FileAlreadyExistsException,
      AccessControlException, UnresolvedLinkException, FileNotFoundException,
      ParentNotDirectoryException, IOException {
    assert hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src
          + ", holder=" + holder
          + ", clientMachine=" + clientMachine
          + ", createParent=" + createParent
          + ", replication=" + replication
          + ", createFlag=" + flag.toString());
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create file" + src, safeMode);
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }

    // Verify that the destination does not exist as a directory already.
    boolean pathExists = dir.exists(src);
    if (pathExists && dir.isDir(src)) {
      throw new FileAlreadyExistsException("Cannot create file " + src
          + "; already exists as a directory.");
    }

    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean append = flag.contains(CreateFlag.APPEND);
    if (isPermissionEnabled) {
      if (append || (overwrite && pathExists)) {
        checkPathAccess(src, FsAction.WRITE);
      } else {
        checkAncestorAccess(src, FsAction.WRITE);
      }
    }

    if (!createParent) {
      verifyParentDir(src);
    }

    try {
      blockManager.verifyReplication(src, replication, clientMachine);
      boolean create = flag.contains(CreateFlag.CREATE);
      final INode myFile = dir.getINode(src);
      if (myFile == null) {
        if (!create) {
          throw new FileNotFoundException("failed to overwrite or append to non-existent file "
            + src + " on client " + clientMachine);
        }
      } else {
        // File exists - must be one of append or overwrite
        if (overwrite) {
          delete(src, true);
        } else {
          // Opening an existing file for write - may need to recover lease.
          recoverLeaseInternal(myFile, src, holder, clientMachine, false);

          if (!append) {
            throw new FileAlreadyExistsException("failed to create file " + src
                + " on client " + clientMachine
                + " because the file exists");
          }
        }
      }

      final DatanodeDescriptor clientNode = 
          blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);

      if (append && myFile != null) {
        final INodeFile f = INodeFile.valueOf(myFile, src); 
        return prepareFileForWrite(
            src, f, holder, clientMachine, clientNode, true);
      } else {
       // Now we can add the name to the filesystem. This file has no
       // blocks associated with it.
       //
       checkFsObjectLimit();

        // increment global generation stamp
        long genstamp = nextGenerationStamp();
        INodeFileUnderConstruction newNode = dir.addFile(src, permissions,
            replication, blockSize, holder, clientMachine, clientNode, genstamp);
        if (newNode == null) {
          throw new IOException("DIR* NameSystem.startFile: " +
                                "Unable to add file to namespace.");
        }
        leaseManager.addLease(newNode.getClientName(), src);

        // record file record in log, record new generation stamp
        getEditLog().logOpenFile(src, newNode);
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: "
                                     +"add "+src+" to namespace for "+holder);
        }
      }
    } catch (IOException ie) {
      NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: "
                                   +ie.getMessage());
      throw ie;
    }
    return null;
  }
  
  /**
   * Replace current node with a INodeUnderConstruction.
   * Recreate in-memory lease record.
   * 
   * @param src path to the file
   * @param file existing file object
   * @param leaseHolder identifier of the lease holder on this file
   * @param clientMachine identifier of the client machine
   * @param clientNode if the client is collocated with a DN, that DN's descriptor
   * @param writeToEditLog whether to persist this change to the edit log
   * @return the last block locations if the block is partial or null otherwise
   * @throws UnresolvedLinkException
   * @throws IOException
   */
  LocatedBlock prepareFileForWrite(String src, INodeFile file,
      String leaseHolder, String clientMachine, DatanodeDescriptor clientNode,
      boolean writeToEditLog) throws IOException {
    INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
                                    file.getId(),
                                    file.getLocalNameBytes(),
                                    file.getBlockReplication(),
                                    file.getModificationTime(),
                                    file.getPreferredBlockSize(),
                                    file.getBlocks(),
                                    file.getPermissionStatus(),
                                    leaseHolder,
                                    clientMachine,
                                    clientNode);
    dir.replaceNode(src, file, cons);
    leaseManager.addLease(cons.getClientName(), src);
    
    LocatedBlock ret = blockManager.convertLastBlockToUnderConstruction(cons);
    if (writeToEditLog) {
      getEditLog().logOpenFile(src, cons);
    }
    return ret;
  }

  /**
   * Recover lease;
   * Immediately revoke the lease of the current lease holder and start lease
   * recovery so that the file can be forced to be closed.
   * 
   * @param src the path of the file to start lease recovery
   * @param holder the lease holder's name
   * @param clientMachine the client machine's name
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(String src, String holder, String clientMachine)
      throws IOException {
    boolean skipSync = false;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot recover the lease of " + src, safeMode);
      }
      if (!DFSUtil.isValidName(src)) {
        throw new IOException("Invalid file name: " + src);
      }
  
      final INodeFile inode = INodeFile.valueOf(dir.getINode(src), src);
      if (!inode.isUnderConstruction()) {
        return true;
      }
      if (isPermissionEnabled) {
        checkPathAccess(src, FsAction.WRITE);
      }
  
      recoverLeaseInternal(inode, src, holder, clientMachine, true);
    } catch (StandbyException se) {
      skipSync = true;
      throw se;
    } finally {
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      if (!skipSync) {
        getEditLog().logSync();
      }
    }
    return false;
  }

  private void recoverLeaseInternal(INode fileInode, 
      String src, String holder, String clientMachine, boolean force)
      throws IOException {
    assert hasWriteLock();
    if (fileInode != null && fileInode.isUnderConstruction()) {
      INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) fileInode;
      //
      // If the file is under construction , then it must be in our
      // leases. Find the appropriate lease record.
      //
      Lease lease = leaseManager.getLease(holder);
      //
      // We found the lease for this file. And surprisingly the original
      // holder is trying to recreate this file. This should never occur.
      //
      if (!force && lease != null) {
        Lease leaseFile = leaseManager.getLeaseByPath(src);
        if ((leaseFile != null && leaseFile.equals(lease)) ||
            lease.getHolder().equals(holder)) { 
          throw new AlreadyBeingCreatedException(
            "failed to create file " + src + " for " + holder +
            " on client " + clientMachine + 
            " because current leaseholder is trying to recreate file.");
        }
      }
      //
      // Find the original holder.
      //
      lease = leaseManager.getLease(pendingFile.getClientName());
      if (lease == null) {
        throw new AlreadyBeingCreatedException(
          "failed to create file " + src + " for " + holder +
          " on client " + clientMachine + 
          " because pendingCreates is non-null but no leases found.");
      }
      if (force) {
        // close now: no need to wait for soft lease expiration and 
        // close only the file src
        LOG.info("recoverLease: " + lease + ", src=" + src +
          " from client " + pendingFile.getClientName());
        internalReleaseLease(lease, src, holder);
      } else {
        assert lease.getHolder().equals(pendingFile.getClientName()) :
          "Current lease holder " + lease.getHolder() +
          " does not match file creator " + pendingFile.getClientName();
        //
        // If the original holder has not renewed in the last SOFTLIMIT 
        // period, then start lease recovery.
        //
        if (lease.expiredSoftLimit()) {
          LOG.info("startFile: recover " + lease + ", src=" + src + " client "
              + pendingFile.getClientName());
          boolean isClosed = internalReleaseLease(lease, src, null);
          if(!isClosed)
            throw new RecoveryInProgressException(
                "Failed to close file " + src +
                ". Lease recovery is in progress. Try again later.");
        } else {
          final BlockInfo lastBlock = pendingFile.getLastBlock();
          if (lastBlock != null
              && lastBlock.getBlockUCState() == BlockUCState.UNDER_RECOVERY) {
            throw new RecoveryInProgressException("Recovery in progress, file ["
                + src + "], " + "lease owner [" + lease.getHolder() + "]");
          } else {
            throw new AlreadyBeingCreatedException("Failed to create file ["
                + src + "] for [" + holder + "] on client [" + clientMachine
                + "], because this file is already being created by ["
                + pendingFile.getClientName() + "] on ["
                + pendingFile.getClientMachine() + "]");
          }
        }
      }
    }
  }

  /**
   * Append to an existing file in the namespace.
   */
  LocatedBlock appendFile(String src, String holder, String clientMachine)
      throws AccessControlException, SafeModeException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, IOException {
    try {
      return appendFileInt(src, holder, clientMachine);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "append", src, null, null);
      }
      throw e;
    }
  }

  private LocatedBlock appendFileInt(String src, String holder, String clientMachine)
      throws AccessControlException, SafeModeException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, IOException {
    boolean skipSync = false;
    if (!supportAppends) {
      throw new UnsupportedOperationException(
          "Append is not enabled on this NameNode. Use the " +
          DFS_SUPPORT_APPEND_KEY + " configuration option to enable it.");
    }
    LocatedBlock lb = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      lb = startFileInternal(src, null, holder, clientMachine, 
                        EnumSet.of(CreateFlag.APPEND), 
                        false, blockManager.maxReplication, 0);
    } catch (StandbyException se) {
      skipSync = true;
      throw se;
    } finally {
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      if (!skipSync) {
        getEditLog().logSync();
      }
    }
    if (lb != null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.appendFile: file "
            +src+" for "+holder+" at "+clientMachine
            +" block " + lb.getBlock()
            +" block size " + lb.getBlock().getNumBytes());
      }
    }
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "append", src, null, null);
    }
    return lb;
  }

  ExtendedBlock getExtendedBlock(Block blk) {
    return new ExtendedBlock(blockPoolId, blk);
  }
  
  void setBlockPoolId(String bpid) {
    blockPoolId = bpid;
    blockManager.setBlockPoolId(blockPoolId);
  }

  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to).  Return an array that consists
   * of the block, plus a set of machines.  The first on this list should
   * be where the client writes data.  Subsequent items in the list must
   * be provided in the connection to the first datanode.
   *
   * Make sure the previous blocks have been reported by datanodes and
   * are replicated.  Will return an empty 2-elt array if we want the
   * client to "try again later".
   */
  LocatedBlock getAdditionalBlock(String src, long fileId, String clientName,
      ExtendedBlock previous, HashMap<Node, Node> excludedNodes)
      throws LeaseExpiredException, NotReplicatedYetException,
      QuotaExceededException, SafeModeException, UnresolvedLinkException,
      IOException {
    long blockSize;
    int replication;
    DatanodeDescriptor clientNode = null;

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.getAdditionalBlock: file "
          +src+" for "+clientName);
    }

    // Part I. Analyze the state of the file with respect to the input data.
    readLock();
    try {
      LocatedBlock[] onRetryBlock = new LocatedBlock[1];
      final INode[] inodes = analyzeFileState(
          src, fileId, clientName, previous, onRetryBlock).getINodes();
      final INodeFileUnderConstruction pendingFile =
          (INodeFileUnderConstruction) inodes[inodes.length - 1];

      if(onRetryBlock[0] != null) {
        // This is a retry. Just return the last block.
        return onRetryBlock[0];
      }

      blockSize = pendingFile.getPreferredBlockSize();
      clientNode = pendingFile.getClientNode();
      replication = pendingFile.getBlockReplication();
    } finally {
      readUnlock();
    }

    // choose targets for the new block to be allocated.
    final DatanodeDescriptor targets[] = getBlockManager().chooseTarget(
        src, replication, clientNode, excludedNodes, blockSize);

    // Part II.
    // Allocate a new block, add it to the INode and the BlocksMap. 
    Block newBlock = null;
    long offset;
    writeLock();
    try {
      // Run the full analysis again, since things could have changed
      // while chooseTarget() was executing.
      LocatedBlock[] onRetryBlock = new LocatedBlock[1];
      INodesInPath inodesInPath =
          analyzeFileState(src, fileId, clientName, previous, onRetryBlock);
      INode[] inodes = inodesInPath.getINodes();
      final INodeFileUnderConstruction pendingFile =
          (INodeFileUnderConstruction) inodes[inodes.length - 1];

      if(onRetryBlock[0] != null) {
        // This is a retry. Just return the last block.
        return onRetryBlock[0];
      }

      // commit the last block and complete it if it has minimum replicas
      commitOrCompleteLastBlock(pendingFile,
                                ExtendedBlock.getLocalBlock(previous));

      // allocate new block, record block locations in INode.
      newBlock = createNewBlock();
      saveAllocatedBlock(src, inodesInPath, newBlock, targets);

      dir.persistBlocks(src, pendingFile);
      offset = pendingFile.computeFileSize(true);
    } finally {
      writeUnlock();
    }
    if (persistBlocks) {
      getEditLog().logSync();
    }

    // Return located block
    return makeLocatedBlock(newBlock, targets, offset);
  }

  INodesInPath analyzeFileState(String src,
                                long fileId,
                                String clientName,
                                ExtendedBlock previous,
                                LocatedBlock[] onRetryBlock)
          throws IOException  {
    assert hasReadOrWriteLock();

    checkBlock(previous);
    onRetryBlock[0] = null;
    checkOperation(OperationCategory.WRITE);
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot add block to " + src, safeMode);
    }

    // have we exceeded the configured limit of fs objects.
    checkFsObjectLimit();

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INodesInPath inodesInPath =
        dir.rootDir.getExistingPathINodes(src, true);
    final INode[] inodes = inodesInPath.getINodes();
    final INodeFileUnderConstruction pendingFile
        = checkLease(src, fileId, clientName, inodes[inodes.length - 1]);
    BlockInfo lastBlockInFile = pendingFile.getLastBlock();
    if (!Block.matchingIdAndGenStamp(previousBlock, lastBlockInFile)) {
      // The block that the client claims is the current last block
      // doesn't match up with what we think is the last block. There are
      // four possibilities:
      // 1) This is the first block allocation of an append() pipeline
      //    which started appending exactly at a block boundary.
      //    In this case, the client isn't passed the previous block,
      //    so it makes the allocateBlock() call with previous=null.
      //    We can distinguish this since the last block of the file
      //    will be exactly a full block.
      // 2) This is a retry from a client that missed the response of a
      //    prior getAdditionalBlock() call, perhaps because of a network
      //    timeout, or because of an HA failover. In that case, we know
      //    by the fact that the client is re-issuing the RPC that it
      //    never began to write to the old block. Hence it is safe to
      //    to return the existing block.
      // 3) This is an entirely bogus request/bug -- we should error out
      //    rather than potentially appending a new block with an empty
      //    one in the middle, etc
      // 4) This is a retry from a client that timed out while
      //    the prior getAdditionalBlock() is still being processed,
      //    currently working on chooseTarget(). 
      //    There are no means to distinguish between the first and 
      //    the second attempts in Part I, because the first one hasn't
      //    changed the namesystem state yet.
      //    We run this analysis again in Part II where case 4 is impossible.

      BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();
      if (previous == null &&
          lastBlockInFile != null &&
          lastBlockInFile.getNumBytes() == pendingFile.getPreferredBlockSize() &&
          lastBlockInFile.isComplete()) {
        // Case 1
        if (NameNode.stateChangeLog.isDebugEnabled()) {
           NameNode.stateChangeLog.debug(
               "BLOCK* NameSystem.allocateBlock: handling block allocation" +
               " writing to a file with a complete previous block: src=" +
               src + " lastBlock=" + lastBlockInFile);
        }
      } else if (Block.matchingIdAndGenStamp(penultimateBlock, previousBlock)) {
        if (lastBlockInFile.getNumBytes() != 0) {
          throw new IOException(
              "Request looked like a retry to allocate block " +
              lastBlockInFile + " but it already contains " +
              lastBlockInFile.getNumBytes() + " bytes");
        }

        // Case 2
        // Return the last block.
        NameNode.stateChangeLog.info("BLOCK* allocateBlock: " +
            "caught retry for allocation of a new block in " +
            src + ". Returning previously allocated block " + lastBlockInFile);
        long offset = pendingFile.computeFileSize(true);
        onRetryBlock[0] = makeLocatedBlock(lastBlockInFile,
            ((BlockInfoUnderConstruction)lastBlockInFile).getExpectedLocations(),
            offset);
        return inodesInPath;
      } else {
        // Case 3
        throw new IOException("Cannot allocate block in " + src + ": " +
            "passed 'previous' block " + previous + " does not match actual " +
            "last block in file " + lastBlockInFile);
      }
    }

    // Check if the penultimate block is minimally replicated
    if (!checkFileProgress(pendingFile, false)) {
      throw new NotReplicatedYetException("Not replicated yet: " + src);
    }
    return inodesInPath;
  }

  LocatedBlock makeLocatedBlock(Block blk,
                                        DatanodeInfo[] locs,
                                        long offset) throws IOException {
    LocatedBlock lBlk = new LocatedBlock(
        getExtendedBlock(blk), locs, offset);
    getBlockManager().setBlockToken(
        lBlk, BlockTokenSecretManager.AccessMode.WRITE);
    return lBlk;
  }

  /** @see NameNode#getAdditionalDatanode(String, ExtendedBlock, DatanodeInfo[], DatanodeInfo[], int, String) */
  LocatedBlock getAdditionalDatanode(final String src, final ExtendedBlock blk,
      final DatanodeInfo[] existings,  final HashMap<Node, Node> excludes,
      final int numAdditionalNodes, final String clientName
      ) throws IOException {
    //check if the feature is enabled
    dtpReplaceDatanodeOnFailure.checkEnabled();

    final DatanodeDescriptor clientnode;
    final long preferredblocksize;
    final List<DatanodeDescriptor> chosen;
    readLock();
    try {
      checkOperation(OperationCategory.WRITE);
      //check safe mode
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot add datanode; src=" + src
            + ", blk=" + blk, safeMode);
      }

      //check lease
      final INodeFileUnderConstruction file = checkLease(src, clientName);
      clientnode = file.getClientNode();
      preferredblocksize = file.getPreferredBlockSize();

      //find datanode descriptors
      chosen = new ArrayList<DatanodeDescriptor>();
      for(DatanodeInfo d : existings) {
        final DatanodeDescriptor descriptor = blockManager.getDatanodeManager(
            ).getDatanode(d);
        if (descriptor != null) {
          chosen.add(descriptor);
        }
      }
    } finally {
      readUnlock();
    }

    // choose new datanodes.
    final DatanodeInfo[] targets = blockManager.getBlockPlacementPolicy(
        ).chooseTarget(src, numAdditionalNodes, clientnode, chosen, true,
        excludes, preferredblocksize);
    final LocatedBlock lb = new LocatedBlock(blk, targets);
    blockManager.setBlockToken(lb, AccessMode.COPY);
    return lb;
  }

  /**
   * The client would like to let go of the given block
   */
  boolean abandonBlock(ExtendedBlock b, String src, String holder)
      throws LeaseExpiredException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      //
      // Remove the block from the pending creates list
      //
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                                      +b+"of file "+src);
      }
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot abandon block " + b +
                                    " for fle" + src, safeMode);
      }
      INodeFileUnderConstruction file = checkLease(src, holder);
      dir.removeBlock(src, file, ExtendedBlock.getLocalBlock(b));
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                                      + b + " is removed from pendingCreates");
      }
      dir.persistBlocks(src, file);
    } finally {
      writeUnlock();
    }
    if (persistBlocks) {
      getEditLog().logSync();
    }

    return true;
  }
  
  // make sure that we still have the lease on this file.
  private INodeFileUnderConstruction checkLease(String src, String holder)
      throws LeaseExpiredException, UnresolvedLinkException,
      FileNotFoundException {
    assert hasReadOrWriteLock();
    return checkLease(src, INodeId.GRANDFATHER_INODE_ID, holder,
        dir.getINode(src));
  }
  
  private INodeFileUnderConstruction checkLease(String src, long fileId,
      String holder, INode file) throws LeaseExpiredException,
      FileNotFoundException {
    assert hasReadOrWriteLock();
    if (file == null || !(file instanceof INodeFile)) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + src + ": File does not exist. "
          + (lease != null ? lease.toString()
              : "Holder " + holder + " does not have any open files."));
    }
    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + src + ": File is not open for writing. "
          + (lease != null ? lease.toString()
              : "Holder " + holder + " does not have any open files."));
    }
    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)file;
    if (holder != null && !pendingFile.getClientName().equals(holder)) {
      throw new LeaseExpiredException("Lease mismatch on " + src + " owned by "
          + pendingFile.getClientName() + " but is accessed by " + holder);
    }
    INodeId.checkId(fileId, pendingFile);
    return pendingFile;
  }
 
  /**
   * Complete in-progress write to the given file.
   * @return true if successful, false if the client should continue to retry
   *         (e.g if not all blocks have reached minimum replication yet)
   * @throws IOException on error (eg lease mismatch, file not open, file deleted)
   */
  boolean completeFile(String src, String holder, ExtendedBlock last) 
    throws SafeModeException, UnresolvedLinkException, IOException {
    checkBlock(last);
    boolean success = false;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      success = completeFileInternal(src, holder, 
        ExtendedBlock.getLocalBlock(last));
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    return success;
  }

  private boolean completeFileInternal(String src, 
      String holder, Block last) throws SafeModeException,
      UnresolvedLinkException, IOException {
    assert hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " +
          src + " for " + holder);
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot complete file " + src, safeMode);
    }

    INodeFileUnderConstruction pendingFile;
    try {
      pendingFile = checkLease(src, holder);
    } catch (LeaseExpiredException lee) {
      final INode inode = dir.getINode(src);
      if (inode != null && inode instanceof INodeFile && !inode.isUnderConstruction()) {
        // This could be a retry RPC - i.e the client tried to close
        // the file, but missed the RPC response. Thus, it is trying
        // again to close the file. If the file still exists and
        // the client's view of the last block matches the actual
        // last block, then we'll treat it as a successful close.
        // See HDFS-3031.
        final Block realLastBlock = ((INodeFile)inode).getLastBlock();
        if (Block.matchingIdAndGenStamp(last, realLastBlock)) {
          NameNode.stateChangeLog.info("DIR* completeFile: " +
              "request from " + holder + " to complete " + src +
              " which is already closed. But, it appears to be an RPC " +
              "retry. Returning success");
          return true;
        }
      }
      throw lee;
    }
    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, last);

    if (!checkFileProgress(pendingFile, true)) {
      return false;
    }

    finalizeINodeFileUnderConstruction(src, pendingFile);

    NameNode.stateChangeLog.info("DIR* completeFile: " + src + " is closed by "
        + holder);
    return true;
  }

  /**
   * Save allocated block at the given pending filename
   * 
   * @param src path to the file
   * @param inodesInPath representing each of the components of src. 
   *                     The last INode is the INode for the file.
   * @throws QuotaExceededException If addition of block exceeds space quota
   */
  BlockInfo saveAllocatedBlock(String src, INodesInPath inodesInPath,
      Block newBlock, DatanodeDescriptor targets[]) throws IOException {
    assert hasWriteLock();
    BlockInfo b = dir.addBlock(src, inodesInPath, newBlock, targets);
    NameNode.stateChangeLog.info("BLOCK* allocateBlock: " + src + ". "
        + getBlockPoolId() + " " + b);
    for (DatanodeDescriptor dn : targets) {
      dn.incBlocksScheduled();
    }
    return b;
  }

  /**
   * Create new block with a unique block id and a new generation stamp.
   */
  Block createNewBlock() throws IOException {
    assert hasWriteLock();
    Block b = new Block(getFSImage().getUniqueBlockId(), 0, 0); 
    // Increment the generation stamp for every new block.
    b.setGenerationStamp(nextGenerationStamp());
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  boolean checkFileProgress(INodeFile v, boolean checkall) {
    readLock();
    try {
      if (checkall) {
        //
        // check all blocks of the file.
        //
        for (BlockInfo block: v.getBlocks()) {
          if (!block.isComplete()) {
            LOG.info("BLOCK* checkFileProgress: " + block
                + " has not reached minimal replication "
                + blockManager.minReplication);
            return false;
          }
        }
      } else {
        //
        // check the penultimate block of this file
        //
        BlockInfo b = v.getPenultimateBlock();
        if (b != null && !b.isComplete()) {
          LOG.info("BLOCK* checkFileProgress: " + b
              + " has not reached minimal replication "
              + blockManager.minReplication);
          return false;
        }
      }
      return true;
    } finally {
      readUnlock();
    }
  }

  ////////////////////////////////////////////////////////////////
  // Here's how to handle block-copy failure during client write:
  // -- As usual, the client's write should result in a streaming
  // backup write to a k-machine sequence.
  // -- If one of the backup machines fails, no worries.  Fail silently.
  // -- Before client is allowed to close and finalize file, make sure
  // that the blocks are backed up.  Namenode may have to issue specific backup
  // commands to make up for earlier datanode failures.  Once all copies
  // are made, edit namespace and return to client.
  ////////////////////////////////////////////////////////////////

  /** 
   * Change the indicated filename. 
   * @deprecated Use {@link #renameTo(String, String, Options.Rename...)} instead.
   */
  @Deprecated
  boolean renameTo(String src, String dst) 
      throws IOException, UnresolvedLinkException {
    try {
      return renameToInt(src, dst);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "rename", src, dst, null);
      }
      throw e;
    }
  }

  private boolean renameToInt(String src, String dst) 
    throws IOException, UnresolvedLinkException {
    boolean status = false;
    HdfsFileStatus resultingStat = null;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src +
          " to " + dst);
    }
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      status = renameToInternal(src, dst);
      if (status && isAuditEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(dst, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (status && isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "rename", src, dst, resultingStat);
    }
    return status;
  }

  /** @deprecated See {@link #renameTo(String, String)} */
  @Deprecated
  private boolean renameToInternal(String src, String dst)
    throws IOException, UnresolvedLinkException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }
    if (isPermissionEnabled) {
      //We should not be doing this.  This is move() not renameTo().
      //but for now,
      //NOTE: yes, this is bad!  it's assuming much lower level behavior
      //      of rewriting the dst
      String actualdst = dir.isDir(dst)?
          dst + Path.SEPARATOR + new Path(src).getName(): dst;
      checkParentAccess(src, FsAction.WRITE);
      checkAncestorAccess(actualdst, FsAction.WRITE);
    }

    if (dir.renameTo(src, dst)) {
      return true;
    }
    return false;
  }
  

  /** Rename src to dst */
  void renameTo(String src, String dst, Options.Rename... options)
      throws IOException, UnresolvedLinkException {
    HdfsFileStatus resultingStat = null;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: with options - "
          + src + " to " + dst);
    }
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      renameToInternal(src, dst, options);
      if (isAuditEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(dst, false); 
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (isAuditEnabled() && isExternalInvocation()) {
      StringBuilder cmd = new StringBuilder("rename options=");
      for (Rename option : options) {
        cmd.append(option.value()).append(" ");
      }
      logAuditEvent(UserGroupInformation.getCurrentUser(), getRemoteIp(),
                    cmd.toString(), src, dst, resultingStat);
    }
  }

  private void renameToInternal(String src, String dst,
      Options.Rename... options) throws IOException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new InvalidPathException("Invalid name: " + dst);
    }
    if (isPermissionEnabled) {
      checkParentAccess(src, FsAction.WRITE);
      checkAncestorAccess(dst, FsAction.WRITE);
    }

    dir.renameTo(src, dst, options);
  }
  
  /**
   * Remove the indicated file from namespace.
   * 
   * @see ClientProtocol#delete(String, boolean) for detailed descriptoin and 
   * description of exceptions
   */
  boolean delete(String src, boolean recursive)
      throws AccessControlException, SafeModeException,
      UnresolvedLinkException, IOException {
    try {
      return deleteInt(src, recursive);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "delete", src, null, null);
      }
      throw e;
    }
  }
      
  private boolean deleteInt(String src, boolean recursive)
      throws AccessControlException, SafeModeException,
      UnresolvedLinkException, IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }
    boolean status = deleteInternal(src, recursive, true);
    if (status && isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "delete", src, null, null);
    }
    return status;
  }
    
  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   * 
   * @see ClientProtocol#delete(String, boolean) for description of exceptions
   */
  private boolean deleteInternal(String src, boolean recursive,
      boolean enforcePermission)
      throws AccessControlException, SafeModeException, UnresolvedLinkException,
             IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();

    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot delete " + src, safeMode);
      }
      if (!recursive && dir.isNonEmptyDirectory(src)) {
        throw new IOException(src + " is non empty");
      }
      if (enforcePermission && isPermissionEnabled) {
        checkPermission(src, false, null, FsAction.WRITE, null, FsAction.ALL);
      }
      // Unlink the target directory from directory tree
      if (!dir.delete(src, collectedBlocks)) {
        return false;
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync(); 
    removeBlocks(collectedBlocks); // Incremental deletion of blocks
    collectedBlocks.clear();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* Namesystem.delete: "
        + src +" is removed");
    }
    return true;
  }

  /**
   * From the given list, incrementally remove the blocks from blockManager
   * Writelock is dropped and reacquired every BLOCK_DELETION_INCREMENT to
   * ensure that other waiters on the lock can get in. See HDFS-2938
   * 
   * @param blocks
   *          An instance of {@link BlocksMapUpdateInfo} which contains a list
   *          of blocks that need to be removed from blocksMap
   */
  private void removeBlocks(BlocksMapUpdateInfo blocks) {
    int start = 0;
    int end = 0;
    List<Block> toDeleteList = blocks.getToDeleteList();
    while (start < toDeleteList.size()) {
      end = BLOCK_DELETION_INCREMENT + start;
      end = end > toDeleteList.size() ? toDeleteList.size() : end;
      writeLock();
      try {
        for (int i = start; i < end; i++) {
          blockManager.removeBlock(toDeleteList.get(i));
        }
      } finally {
        writeUnlock();
      }
      start = end;
    }
  }
  
  /**
   * Remove leases and blocks related to a given path
   * @param src The given path
   * @param blocks Containing the list of blocks to be deleted from blocksMap
   */
  void removePathAndBlocks(String src, BlocksMapUpdateInfo blocks) {
    assert hasWriteLock();
    leaseManager.removeLeaseWithPrefixPath(src);
    if (blocks == null) {
      return;
    }
    
    // In the case that we are a Standby tailing edits from the
    // active while in safe-mode, we need to track the total number
    // of blocks and safe blocks in the system.
    boolean trackBlockCounts = isSafeModeTrackingBlocks();
    int numRemovedComplete = 0, numRemovedSafe = 0;

    for (Block b : blocks.getToDeleteList()) {
      if (trackBlockCounts) {
        BlockInfo bi = blockManager.getStoredBlock(b);
        if (bi.isComplete()) {
          numRemovedComplete++;
          if (bi.numNodes() >= blockManager.minReplication) {
            numRemovedSafe++;
          }
        }
      }
      blockManager.removeBlock(b);
    }
    if (trackBlockCounts) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adjusting safe-mode totals for deletion of " + src + ":" +
            "decreasing safeBlocks by " + numRemovedSafe +
            ", totalBlocks by " + numRemovedComplete);
      }
      adjustSafeModeBlockTotals(-numRemovedSafe, -numRemovedComplete);
    }
  }

  /**
   * @see SafeModeInfo#shouldIncrementallyTrackBlocks
   */
  private boolean isSafeModeTrackingBlocks() {
    if (!haEnabled) {
      // Never track blocks incrementally in non-HA code.
      return false;
    }
    SafeModeInfo sm = this.safeMode;
    return sm != null && sm.shouldIncrementallyTrackBlocks();
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException 
   *        if src refers to a symlink
   *
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if a symlink is encountered.
   *
   * @return object containing information regarding the file
   *         or null if file not found
   * @throws StandbyException 
   */
  HdfsFileStatus getFileInfo(String src, boolean resolveLink) 
    throws AccessControlException, UnresolvedLinkException,
           StandbyException, IOException {
    HdfsFileStatus stat = null;

    readLock();

    try {
      checkOperation(OperationCategory.READ);

      if (!DFSUtil.isValidName(src)) {
        throw new InvalidPathException("Invalid file name: " + src);
      }
      if (isPermissionEnabled) {
        checkTraverse(src);
      }
      stat = dir.getFileInfo(src, resolveLink);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "getfileinfo", src, null, null);
      }
      throw e;
    } finally {
      readUnlock();
    }
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "getfileinfo", src, null, null);
    }
    return stat;
  }

  /**
   * Create all the necessary directories
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean createParent) throws IOException, UnresolvedLinkException {
    try {
      return mkdirsInt(src, permissions, createParent);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "mkdirs", src, null, null);
      }
      throw e;
    }
  }

  private boolean mkdirsInt(String src, PermissionStatus permissions,
      boolean createParent) throws IOException, UnresolvedLinkException {
    boolean status = false;
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    }
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      status = mkdirsInternal(src, permissions, createParent);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (status && isAuditEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    getRemoteIp(),
                    "mkdirs", src, null, stat);
    }
    return status;
  }
    
  /**
   * Create all the necessary directories
   */
  private boolean mkdirsInternal(String src,
      PermissionStatus permissions, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create directory " + src, safeMode);
    }
    if (isPermissionEnabled) {
      checkTraverse(src);
    }
    if (dir.isDir(src)) {
      // all the users of mkdirs() are used to expect 'true' even if
      // a new directory is not created.
      return true;
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }
    if (isPermissionEnabled) {
      checkAncestorAccess(src, FsAction.WRITE);
    }
    if (!createParent) {
      verifyParentDir(src);
    }

    // validate that we have enough inodes. This is, at best, a 
    // heuristic because the mkdirs() operation migth need to 
    // create multiple inodes.
    checkFsObjectLimit();

    if (!dir.mkdirs(src, permissions, false, now())) {
      throw new IOException("Failed to create directory: " + src);
    }
    return true;
  }

  ContentSummary getContentSummary(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, StandbyException {
    readLock();
    try {
      checkOperation(OperationCategory.READ);

      if (isPermissionEnabled) {
        checkPermission(src, false, null, null, null, FsAction.READ_EXECUTE);
      }
      return dir.getContentSummary(src);
    } finally {
      readUnlock();
    }
  }

  /**
   * Set the namespace quota and diskspace quota for a directory.
   * See {@link ClientProtocol#setQuota(String, long, long)} for the 
   * contract.
   */
  void setQuota(String path, long nsQuota, long dsQuota) 
      throws IOException, UnresolvedLinkException {
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set quota on " + path, safeMode);
      }
      if (isPermissionEnabled) {
        checkSuperuserPrivilege();
      }
      dir.setQuota(path, nsQuota, dsQuota);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }
  
  /** Persist all metadata about this file.
   * @param src The string representation of the path
   * @param clientName The string representation of the client
   * @param lastBlockLength The length of the last block 
   *                        under construction reported from client.
   * @throws IOException if path does not exist
   */
  void fsync(String src, String clientName, long lastBlockLength) 
      throws IOException, UnresolvedLinkException {
    NameNode.stateChangeLog.info("BLOCK* fsync: " + src + " for " + clientName);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot fsync file " + src, safeMode);
      }
      INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);
      if (lastBlockLength > 0) {
        pendingFile.updateLengthOfLastBlock(lastBlockLength);
      }
      dir.persistBlocks(src, pendingFile);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }

  /**
   * Move a file that is being written to be immutable.
   * @param src The filename
   * @param lease The lease for the client creating the file
   * @param recoveryLeaseHolder reassign lease to this holder if the last block
   *        needs recovery; keep current holder if null.
   * @throws AlreadyBeingCreatedException if file is waiting to achieve minimal
   *         replication;<br>
   *         RecoveryInProgressException if lease recovery is in progress.<br>
   *         IOException in case of an error.
   * @return true  if file has been successfully finalized and closed or 
   *         false if block recovery has been initiated. Since the lease owner
   *         has been changed and logged, caller should call logSync().
   */
  boolean internalReleaseLease(Lease lease, String src, 
      String recoveryLeaseHolder) throws AlreadyBeingCreatedException, 
      IOException, UnresolvedLinkException {
    LOG.info("Recovering " + lease + ", src=" + src);
    assert !isInSafeMode();
    assert hasWriteLock();

    final INodeFileUnderConstruction pendingFile
        = INodeFileUnderConstruction.valueOf(dir.getINode(src), src);
    int nrBlocks = pendingFile.numBlocks();
    BlockInfo[] blocks = pendingFile.getBlocks();

    int nrCompleteBlocks;
    BlockInfo curBlock = null;
    for(nrCompleteBlocks = 0; nrCompleteBlocks < nrBlocks; nrCompleteBlocks++) {
      curBlock = blocks[nrCompleteBlocks];
      if(!curBlock.isComplete())
        break;
      assert blockManager.checkMinReplication(curBlock) :
              "A COMPLETE block is not minimally replicated in " + src;
    }

    // If there are no incomplete blocks associated with this file,
    // then reap lease immediately and close the file.
    if(nrCompleteBlocks == nrBlocks) {
      finalizeINodeFileUnderConstruction(src, pendingFile);
      NameNode.stateChangeLog.warn("BLOCK*"
        + " internalReleaseLease: All existing blocks are COMPLETE,"
        + " lease removed, file closed.");
      return true;  // closed!
    }

    // Only the last and the penultimate blocks may be in non COMPLETE state.
    // If the penultimate block is not COMPLETE, then it must be COMMITTED.
    if(nrCompleteBlocks < nrBlocks - 2 ||
       nrCompleteBlocks == nrBlocks - 2 &&
         curBlock != null &&
         curBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      final String message = "DIR* NameSystem.internalReleaseLease: "
        + "attempt to release a create lock on "
        + src + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    // The last block is not COMPLETE, and
    // that the penultimate block if exists is either COMPLETE or COMMITTED
    final BlockInfo lastBlock = pendingFile.getLastBlock();
    BlockUCState lastBlockState = lastBlock.getBlockUCState();
    BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();
    boolean penultimateBlockMinReplication;
    BlockUCState penultimateBlockState;
    if (penultimateBlock == null) {
      penultimateBlockState = BlockUCState.COMPLETE;
      // If penultimate block doesn't exist then its minReplication is met
      penultimateBlockMinReplication = true;
    } else {
      penultimateBlockState = BlockUCState.COMMITTED;
      penultimateBlockMinReplication = 
        blockManager.checkMinReplication(penultimateBlock);
    }
    assert penultimateBlockState == BlockUCState.COMPLETE ||
           penultimateBlockState == BlockUCState.COMMITTED :
           "Unexpected state of penultimate block in " + src;

    switch(lastBlockState) {
    case COMPLETE:
      assert false : "Already checked that the last block is incomplete";
      break;
    case COMMITTED:
      // Close file if committed blocks are minimally replicated
      if(penultimateBlockMinReplication &&
          blockManager.checkMinReplication(lastBlock)) {
        finalizeINodeFileUnderConstruction(src, pendingFile);
        NameNode.stateChangeLog.warn("BLOCK*"
          + " internalReleaseLease: Committed blocks are minimally replicated,"
          + " lease removed, file closed.");
        return true;  // closed!
      }
      // Cannot close file right now, since some blocks 
      // are not yet minimally replicated.
      // This may potentially cause infinite loop in lease recovery
      // if there are no valid replicas on data-nodes.
      String message = "DIR* NameSystem.internalReleaseLease: " +
          "Failed to release lease for file " + src +
          ". Committed blocks are waiting to be minimally replicated." +
          " Try again later.";
      NameNode.stateChangeLog.warn(message);
      throw new AlreadyBeingCreatedException(message);
    case UNDER_CONSTRUCTION:
    case UNDER_RECOVERY:
      final BlockInfoUnderConstruction uc = (BlockInfoUnderConstruction)lastBlock;
      // setup the last block locations from the blockManager if not known
      if (uc.getNumExpectedLocations() == 0) {
        uc.setExpectedLocations(blockManager.getNodes(lastBlock));
      }
      // start recovery of the last block for this file
      long blockRecoveryId = nextGenerationStamp();
      lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);
      uc.initializeBlockRecovery(blockRecoveryId);
      leaseManager.renewLease(lease);
      // Cannot close file right now, since the last block requires recovery.
      // This may potentially cause infinite loop in lease recovery
      // if there are no valid replicas on data-nodes.
      NameNode.stateChangeLog.warn(
                "DIR* NameSystem.internalReleaseLease: " +
                "File " + src + " has not been closed." +
               " Lease recovery is in progress. " +
                "RecoveryId = " + blockRecoveryId + " for block " + lastBlock);
      break;
    }
    return false;
  }

  private Lease reassignLease(Lease lease, String src, String newHolder,
      INodeFileUnderConstruction pendingFile) {
    assert hasWriteLock();
    if(newHolder == null)
      return lease;
    // The following transaction is not synced. Make sure it's sync'ed later.
    logReassignLease(lease.getHolder(), src, newHolder);
    return reassignLeaseInternal(lease, src, newHolder, pendingFile);
  }
  
  Lease reassignLeaseInternal(Lease lease, String src, String newHolder,
      INodeFileUnderConstruction pendingFile) {
    assert hasWriteLock();
    pendingFile.setClientName(newHolder);
    return leaseManager.reassignLease(lease, src, newHolder);
  }

  private void commitOrCompleteLastBlock(final INodeFileUnderConstruction fileINode,
      final Block commitBlock) throws IOException {
    assert hasWriteLock();
    if (!blockManager.commitOrCompleteLastBlock(fileINode, commitBlock)) {
      return;
    }

    // Adjust disk space consumption if required
    final long diff = fileINode.getPreferredBlockSize() - commitBlock.getNumBytes();    
    if (diff > 0) {
      try {
        String path = leaseManager.findPath(fileINode);
        dir.updateSpaceConsumed(path, 0, -diff * fileINode.getBlockReplication());
      } catch (IOException e) {
        LOG.warn("Unexpected exception while updating disk space.", e);
      }
    }
  }

  private void finalizeINodeFileUnderConstruction(String src, 
      INodeFileUnderConstruction pendingFile) 
      throws IOException, UnresolvedLinkException {
    assert hasWriteLock();
    leaseManager.removeLease(pendingFile.getClientName(), src);

    // The file is no longer pending.
    // Create permanent INode, update blocks
    INodeFile newFile = pendingFile.convertToInodeFile();
    dir.replaceNode(src, pendingFile, newFile);

    // close file and persist block allocations for this file
    dir.closeFile(src, newFile);

    blockManager.checkReplication(newFile);
  }

  void commitBlockSynchronization(ExtendedBlock lastblock,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
      String[] newtargetstorages)
      throws IOException, UnresolvedLinkException {
    String src = "";
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      // If a DN tries to commit to the standby, the recovery will
      // fail, and the next retry will succeed on the new NN.
  
      if (isInSafeMode()) {
        throw new SafeModeException(
          "Cannot commitBlockSynchronization while in safe mode",
          safeMode);
      }
      LOG.info("commitBlockSynchronization(lastblock=" + lastblock
               + ", newgenerationstamp=" + newgenerationstamp
               + ", newlength=" + newlength
               + ", newtargets=" + Arrays.asList(newtargets)
               + ", closeFile=" + closeFile
               + ", deleteBlock=" + deleteblock
               + ")");
      final BlockInfo storedBlock = blockManager.getStoredBlock(ExtendedBlock
        .getLocalBlock(lastblock));
      if (storedBlock == null) {
        throw new IOException("Block (=" + lastblock + ") not found");
      }
      INodeFile iFile = (INodeFile) storedBlock.getBlockCollection();
      if (!iFile.isUnderConstruction() || storedBlock.isComplete()) {
        throw new IOException("Unexpected block (=" + lastblock
                              + ") since the file (=" + iFile.getLocalName()
                              + ") is not under construction");
      }

      long recoveryId =
        ((BlockInfoUnderConstruction)storedBlock).getBlockRecoveryId();
      if(recoveryId != newgenerationstamp) {
        throw new IOException("The recovery id " + newgenerationstamp
                              + " does not match current recovery id "
                              + recoveryId + " for block " + lastblock); 
      }

      INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)iFile;

      if (deleteblock) {
        pendingFile.removeLastBlock(ExtendedBlock.getLocalBlock(lastblock));
        blockManager.removeBlockFromMap(storedBlock);
      }
      else {
        // update last block
        storedBlock.setGenerationStamp(newgenerationstamp);
        storedBlock.setNumBytes(newlength);

        // find the DatanodeDescriptor objects
        // There should be no locations in the blockManager till now because the
        // file is underConstruction
        DatanodeDescriptor[] descriptors = null;
        if (newtargets.length > 0) {
          descriptors = new DatanodeDescriptor[newtargets.length];
          for(int i = 0; i < newtargets.length; i++) {
            descriptors[i] = blockManager.getDatanodeManager().getDatanode(
                newtargets[i]);
          }
        }
        if ((closeFile) && (descriptors != null)) {
          // the file is getting closed. Insert block locations into blockManager.
          // Otherwise fsck will report these blocks as MISSING, especially if the
          // blocksReceived from Datanodes take a long time to arrive.
          for (int i = 0; i < descriptors.length; i++) {
            descriptors[i].addBlock(storedBlock);
          }
        }
        // add pipeline locations into the INodeUnderConstruction
        pendingFile.setLastBlock(storedBlock, descriptors);
      }

      src = leaseManager.findPath(pendingFile);
      if (closeFile) {
        // commit the last block and complete it if it has minimum replicas
        commitOrCompleteLastBlock(pendingFile, storedBlock);

        //remove lease, close file
        finalizeINodeFileUnderConstruction(src, pendingFile);
      } else {
        // If this commit does not want to close the file, persist blocks
        dir.persistBlocks(src, pendingFile);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (closeFile) {
      LOG.info("commitBlockSynchronization(newblock=" + lastblock
          + ", file=" + src
          + ", newgenerationstamp=" + newgenerationstamp
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
    } else {
      LOG.info("commitBlockSynchronization(" + lastblock + ") successful");
    }
  }


  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws IOException {
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException("Cannot renew lease for " + holder, safeMode);
      }
      leaseManager.renewLease(holder);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src the directory name
   * @param startAfter the name to start after
   * @param needLocation if blockLocations need to be returned
   * @return a partial listing starting after startAfter
   * 
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if symbolic link is encountered
   * @throws IOException if other I/O error occurred
   */
  DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) 
      throws AccessControlException, UnresolvedLinkException, IOException {
    try {
      return getListingInt(src, startAfter, needLocation);
    } catch (AccessControlException e) {
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "listStatus", src, null, null);
      }
      throw e;
    }
  }

  private DirectoryListing getListingInt(String src, byte[] startAfter,
      boolean needLocation) 
    throws AccessControlException, UnresolvedLinkException, IOException {
    DirectoryListing dl;
    readLock();
    try {
      checkOperation(OperationCategory.READ);

      if (isPermissionEnabled) {
        if (dir.isDir(src)) {
          checkPathAccess(src, FsAction.READ_EXECUTE);
        } else {
          checkTraverse(src);
        }
      }
      if (isAuditEnabled() && isExternalInvocation()) {
        logAuditEvent(UserGroupInformation.getCurrentUser(),
                      getRemoteIp(),
                      "listStatus", src, null, null);
      }
      dl = dir.getListing(src, startAfter, needLocation);
    } finally {
      readUnlock();
    }
    return dl;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by datanodes
  //
  /////////////////////////////////////////////////////////
  /**
   * Register Datanode.
   * <p>
   * The purpose of registration is to identify whether the new datanode
   * serves a new data storage, and will report new data block copies,
   * which the namenode was not aware of; or the datanode is a replacement
   * node for the data storage that was previously served by a different
   * or the same (in terms of host:port) datanode.
   * The data storages are distinguished by their storageIDs. When a new
   * data storage is reported the namenode issues a new unique storageID.
   * <p>
   * Finally, the namenode returns its namespaceID as the registrationID
   * for the datanodes. 
   * namespaceID is a persistent attribute of the name space.
   * The registrationID is checked every time the datanode is communicating
   * with the namenode. 
   * Datanodes with inappropriate registrationID are rejected.
   * If the namenode stops, and then restarts it can restore its 
   * namespaceID and will continue serving the datanodes that has previously
   * registered with the namenode without restarting the whole cluster.
   * 
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode
   */
  void registerDatanode(DatanodeRegistration nodeReg) throws IOException {
    writeLock();
    try {
      getBlockManager().getDatanodeManager().registerDatanode(nodeReg);
      checkSafeMode();
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Get registrationID for datanodes based on the namespaceID.
   * 
   * @see #registerDatanode(DatanodeRegistration)
   * @return registration ID
   */
  String getRegistrationID() {
    return Storage.getRegistrationID(dir.fsImage.getStorage());
  }

  /**
   * The given node has reported in.  This method should:
   * 1) Record the heartbeat, so the datanode isn't timed out
   * 2) Adjust usage stats for future block allocation
   * 
   * If a substantial amount of time passed since the last datanode 
   * heartbeat then request an immediate block report.  
   * 
   * @return an array of datanode commands 
   * @throws IOException
   */
  HeartbeatResponse handleHeartbeat(DatanodeRegistration nodeReg,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      int xceiverCount, int xmitsInProgress, int failedVolumes) 
        throws IOException {
    readLock();
    try {
      final int maxTransfer = blockManager.getMaxReplicationStreams()
          - xmitsInProgress;
      DatanodeCommand[] cmds = blockManager.getDatanodeManager().handleHeartbeat(
          nodeReg, blockPoolId, capacity, dfsUsed, remaining, blockPoolUsed,
          xceiverCount, maxTransfer, failedVolumes);
      return new HeartbeatResponse(cmds, createHaStatusHeartbeat());
    } finally {
      readUnlock();
    }
  }

  private NNHAStatusHeartbeat createHaStatusHeartbeat() {
    HAState state = haContext.getState();
    return new NNHAStatusHeartbeat(state.getServiceState(),
        getFSImage().getLastAppliedOrWrittenTxId());
  }

  /**
   * Returns whether or not there were available resources at the last check of
   * resources.
   *
   * @return true if there were sufficient resources available, false otherwise.
   */
  boolean nameNodeHasResourcesAvailable() {
    return hasResourcesAvailable;
  }

  /**
   * Perform resource checks and cache the results.
   * @throws IOException
   */
  void checkAvailableResources() {
    Preconditions.checkState(nnResourceChecker != null,
        "nnResourceChecker not initialized");
    hasResourcesAvailable = nnResourceChecker.hasAvailableDiskSpace();
  }

  /**
   * Periodically calls hasAvailableResources of NameNodeResourceChecker, and if
   * there are found to be insufficient resources available, causes the NN to
   * enter safe mode. If resources are later found to have returned to
   * acceptable levels, this daemon will cause the NN to exit safe mode.
   */
  class NameNodeResourceMonitor implements Runnable  {
    boolean shouldNNRmRun = true;
    @Override
    public void run () {
      try {
        while (fsRunning && shouldNNRmRun) {
          checkAvailableResources();
          if(!nameNodeHasResourcesAvailable()) {
            String lowResourcesMsg = "NameNode low on available disk space. ";
            if (!isInSafeMode()) {
              FSNamesystem.LOG.warn(lowResourcesMsg + "Entering safe mode.");
            } else {
              FSNamesystem.LOG.warn(lowResourcesMsg + "Already in safe mode.");
            }
            enterSafeMode(true);
          }
          try {
            Thread.sleep(resourceRecheckInterval);
          } catch (InterruptedException ie) {
            // Deliberately ignore
          }
        }
      } catch (Exception e) {
        FSNamesystem.LOG.error("Exception in NameNodeResourceMonitor: ", e);
      }
    }

    public void stopMonitor() {
      shouldNNRmRun = false;
    }
 }
  
  public FSImage getFSImage() {
    return dir.fsImage;
  }

  public FSEditLog getEditLog() {
    return getFSImage().getEditLog();
  }    

  private void checkBlock(ExtendedBlock block) throws IOException {
    if (block != null && !this.blockPoolId.equals(block.getBlockPoolId())) {
      throw new IOException("Unexpected BlockPoolId " + block.getBlockPoolId()
          + " - expected " + blockPoolId);
    }
  }

  @Metric({"MissingBlocks", "Number of missing blocks"})
  public long getMissingBlocksCount() {
    // not locking
    return blockManager.getMissingBlocksCount();
  }
  
  @Metric({"ExpiredHeartbeats", "Number of expired heartbeats"})
  public int getExpiredHeartbeats() {
    return datanodeStatistics.getExpiredHeartbeats();
  }
  
  @Metric({"TransactionsSinceLastCheckpoint",
      "Number of transactions since last checkpoint"})
  public long getTransactionsSinceLastCheckpoint() {
    return getEditLog().getLastWrittenTxId() -
        getFSImage().getStorage().getMostRecentCheckpointTxId();
  }
  
  @Metric({"TransactionsSinceLastLogRoll",
      "Number of transactions since last edit log roll"})
  public long getTransactionsSinceLastLogRoll() {
    if (isInStandbyState() || !getEditLog().isSegmentOpen()) {
      return 0;
    } else {
      return getEditLog().getLastWrittenTxId() -
        getEditLog().getCurSegmentTxId() + 1;
    }
  }
  
  @Metric({"LastWrittenTransactionId", "Transaction ID written to the edit log"})
  public long getLastWrittenTransactionId() {
    return getEditLog().getLastWrittenTxId();
  }
  
  @Metric({"LastCheckpointTime",
      "Time in milliseconds since the epoch of the last checkpoint"})
  public long getLastCheckpointTime() {
    return getFSImage().getStorage().getMostRecentCheckpointTime();
  }

  /** @see ClientProtocol#getStats() */
  long[] getStats() {
    final long[] stats = datanodeStatistics.getStats();
    stats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] = getUnderReplicatedBlocks();
    stats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] = getCorruptReplicaBlocks();
    stats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] = getMissingBlocksCount();
    return stats;
  }

  /**
   * Total raw bytes including non-dfs used space.
   */
  @Override // FSNamesystemMBean
  public long getCapacityTotal() {
    return datanodeStatistics.getCapacityTotal();
  }

  @Metric
  public float getCapacityTotalGB() {
    return DFSUtil.roundBytesToGB(getCapacityTotal());
  }

  /**
   * Total used space by data nodes
   */
  @Override // FSNamesystemMBean
  public long getCapacityUsed() {
    return datanodeStatistics.getCapacityUsed();
  }

  @Metric
  public float getCapacityUsedGB() {
    return DFSUtil.roundBytesToGB(getCapacityUsed());
  }

  @Override
  public long getCapacityRemaining() {
    return datanodeStatistics.getCapacityRemaining();
  }

  @Metric
  public float getCapacityRemainingGB() {
    return DFSUtil.roundBytesToGB(getCapacityRemaining());
  }

  /**
   * Total number of connections.
   */
  @Override // FSNamesystemMBean
  @Metric
  public int getTotalLoad() {
    return datanodeStatistics.getXceiverCount();
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    readLock();
    try {
      return getBlockManager().getDatanodeManager().getDatanodeListForReport(
          type).size(); 
    } finally {
      readUnlock();
    }
  }

  DatanodeInfo[] datanodeReport(final DatanodeReportType type
      ) throws AccessControlException {
    checkSuperuserPrivilege();
    readLock();
    try {
      final DatanodeManager dm = getBlockManager().getDatanodeManager();      
      final List<DatanodeDescriptor> results = dm.getDatanodeListForReport(type);

      DatanodeInfo[] arr = new DatanodeInfo[results.size()];
      for (int i=0; i<arr.length; i++) {
        arr[i] = new DatanodeInfo(results.get(i));
      }
      return arr;
    } finally {
      readUnlock();
    }
  }

  /**
   * Save namespace image.
   * This will save current namespace into fsimage file and empty edits file.
   * Requires superuser privilege and safe mode.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   * @throws IOException if 
   */
  void saveNamespace() throws AccessControlException, IOException {
    readLock();
    try {
      checkSuperuserPrivilege();
      if (!isInSafeMode()) {
        throw new IOException("Safe mode should be turned ON " +
                              "in order to create namespace image.");
      }
      getFSImage().saveNamespace(this);
      LOG.info("New namespace image has been created");
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Enables/Disables/Checks restoring failed storage replicas if the storage becomes available again.
   * Requires superuser privilege.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   */
  boolean restoreFailedStorage(String arg) throws AccessControlException {
    writeLock();
    try {
      checkSuperuserPrivilege();
      
      // if it is disabled - enable it and vice versa.
      if(arg.equals("check"))
        return getFSImage().getStorage().getRestoreFailedStorage();
      
      boolean val = arg.equals("true");  // false if not
      getFSImage().getStorage().setRestoreFailedStorage(val);
      
      return val;
    } finally {
      writeUnlock();
    }
  }

  Date getStartTime() {
    return new Date(startTime); 
  }
    
  void finalizeUpgrade() throws IOException {
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkSuperuserPrivilege();
      getFSImage().finalizeUpgrade();
    } finally {
      writeUnlock();
    }
  }

  void refreshNodes() throws IOException {
    checkOperation(OperationCategory.UNCHECKED);
    checkSuperuserPrivilege();
    getBlockManager().getDatanodeManager().refreshNodes(new HdfsConfiguration());
  }

  void setBalancerBandwidth(long bandwidth) throws IOException {
    checkOperation(OperationCategory.UNCHECKED);
    checkSuperuserPrivilege();
    getBlockManager().getDatanodeManager().setBalancerBandwidth(bandwidth);
  }

  /**
   * SafeModeInfo contains information related to the safe mode.
   * <p>
   * An instance of {@link SafeModeInfo} is created when the name node
   * enters safe mode.
   * <p>
   * During name node startup {@link SafeModeInfo} counts the number of
   * <em>safe blocks</em>, those that have at least the minimal number of
   * replicas, and calculates the ratio of safe blocks to the total number
   * of blocks in the system, which is the size of blocks in
   * {@link FSNamesystem#blockManager}. When the ratio reaches the
   * {@link #threshold} it starts the {@link SafeModeMonitor} daemon in order
   * to monitor whether the safe mode {@link #extension} is passed.
   * Then it leaves safe mode and destroys itself.
   * <p>
   * If safe mode is turned on manually then the number of safe blocks is
   * not tracked because the name node is not intended to leave safe mode
   * automatically in the case.
   *
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction)
   * @see SafeModeMonitor
   */
  class SafeModeInfo {
    // configuration fields
    /** Safe mode threshold condition %.*/
    private double threshold;
    /** Safe mode minimum number of datanodes alive */
    private int datanodeThreshold;
    /** Safe mode extension after the threshold. */
    private int extension;
    /** Min replication required by safe mode. */
    private int safeReplication;
    /** threshold for populating needed replication queues */
    private double replQueueThreshold;
      
    // internal fields
    /** Time when threshold was reached.
     * 
     * <br>-1 safe mode is off
     * <br> 0 safe mode is on, but threshold is not reached yet 
     */
    private long reached = -1;  
    /** Total number of blocks. */
    int blockTotal; 
    /** Number of safe blocks. */
    int blockSafe;
    /** Number of blocks needed to satisfy safe mode threshold condition */
    private int blockThreshold;
    /** Number of blocks needed before populating replication queues */
    private int blockReplQueueThreshold;
    /** time of the last status printout */
    private long lastStatusReport = 0;
    /** flag indicating whether replication queues have been initialized */
    boolean initializedReplQueues = false;
    /** Was safemode entered automatically because available resources were low. */
    private boolean resourcesLow = false;
    /** Should safemode adjust its block totals as blocks come in */
    private boolean shouldIncrementallyTrackBlocks = false;
    
    /**
     * Creates SafeModeInfo when the name node enters
     * automatic safe mode at startup.
     *  
     * @param conf configuration
     */
    private SafeModeInfo(Configuration conf) {
      this.threshold = conf.getFloat(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
          DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
      if(threshold > 1.0) {
        LOG.warn("The threshold value should't be greater than 1, threshold: " + threshold);
      }
      this.datanodeThreshold = conf.getInt(
        DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
        DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT);
      this.extension = conf.getInt(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
      this.safeReplication = conf.getInt(DFS_NAMENODE_REPLICATION_MIN_KEY, 
                                         DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
      
      LOG.info(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY + " = " + threshold);
      LOG.info(DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY + " = " + datanodeThreshold);
      LOG.info(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY + "     = " + extension);

      // default to safe mode threshold (i.e., don't populate queues before leaving safe mode)
      this.replQueueThreshold = 
        conf.getFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
                      (float) threshold);
      this.blockTotal = 0; 
      this.blockSafe = 0;
    }

    /**
     * In the HA case, the StandbyNode can be in safemode while the namespace
     * is modified by the edit log tailer. In this case, the number of total
     * blocks changes as edits are processed (eg blocks are added and deleted).
     * However, we don't want to do the incremental tracking during the
     * startup-time loading process -- only once the initial total has been
     * set after the image has been loaded.
     */
    private boolean shouldIncrementallyTrackBlocks() {
      return shouldIncrementallyTrackBlocks;
    }

    /**
     * Creates SafeModeInfo when safe mode is entered manually, or because
     * available resources are low.
     *
     * The {@link #threshold} is set to 1.5 so that it could never be reached.
     * {@link #blockTotal} is set to -1 to indicate that safe mode is manual.
     * 
     * @see SafeModeInfo
     */
    private SafeModeInfo(boolean resourcesLow) {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.replQueueThreshold = 1.5f; // can never be reached
      this.blockTotal = -1;
      this.blockSafe = -1;
      this.reached = -1;
      this.resourcesLow = resourcesLow;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }
      
    /**
     * Check if safe mode is on.
     * @return true if in safe mode
     */
    private synchronized boolean isOn() {
      doConsistencyCheck();
      return this.reached >= 0;
    }
      
    /**
     * Check if we are populating replication queues.
     */
    private synchronized boolean isPopulatingReplQueues() {
      return initializedReplQueues;
    }

    /**
     * Enter safe mode.
     */
    private void enter() {
      this.reached = 0;
    }
      
    /**
     * Leave safe mode.
     * <p>
     * Check for invalid, under- & over-replicated blocks in the end of startup.
     */
    private synchronized void leave() {
      // if not done yet, initialize replication queues.
      // In the standby, do not populate repl queues
      if (!isPopulatingReplQueues() && shouldPopulateReplQueues()) {
        initializeReplQueues();
      }
      long timeInSafemode = now() - startTime;
      NameNode.stateChangeLog.info("STATE* Leaving safe mode after " 
                                    + timeInSafemode/1000 + " secs");
      NameNode.getNameNodeMetrics().setSafeModeTime((int) timeInSafemode);
      
      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF"); 
      }
      reached = -1;
      safeMode = null;
      final NetworkTopology nt = blockManager.getDatanodeManager().getNetworkTopology();
      NameNode.stateChangeLog.info("STATE* Network topology has "
          + nt.getNumOfRacks() + " racks and "
          + nt.getNumOfLeaves() + " datanodes");
      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has "
          + blockManager.numOfUnderReplicatedBlocks() + " blocks");

      startSecretManagerIfNecessary();
    }

    /**
     * Initialize replication queues.
     */
    private synchronized void initializeReplQueues() {
      LOG.info("initializing replication queues");
      assert !isPopulatingReplQueues() : "Already initialized repl queues";
      long startTimeMisReplicatedScan = now();
      blockManager.processMisReplicatedBlocks();
      initializedReplQueues = true;
      NameNode.stateChangeLog.info("STATE* Replication Queue initialization "
          + "scan for invalid, over- and under-replicated blocks "
          + "completed in " + (now() - startTimeMisReplicatedScan)
          + " msec");
    }

    /**
     * Check whether we have reached the threshold for 
     * initializing replication queues.
     */
    private synchronized boolean canInitializeReplQueues() {
      return shouldPopulateReplQueues()
          && blockSafe >= blockReplQueueThreshold;
    }
      
    /** 
     * Safe mode can be turned off iff 
     * the threshold is reached and 
     * the extension time have passed.
     * @return true if can leave or false otherwise.
     */
    private synchronized boolean canLeave() {
      if (reached == 0)
        return false;
      if (now() - reached < extension) {
        reportStatus("STATE* Safe mode ON.", false);
        return false;
      }
      return !needEnter();
    }
      
    /** 
     * There is no need to enter safe mode 
     * if DFS is empty or {@link #threshold} == 0
     */
    private boolean needEnter() {
      return (threshold != 0 && blockSafe < blockThreshold) ||
        (getNumLiveDataNodes() < datanodeThreshold) ||
        (!nameNodeHasResourcesAvailable());
    }
      
    /**
     * Check and trigger safe mode if needed. 
     */
    private void checkMode() {
      // Have to have write-lock since leaving safemode initializes
      // repl queues, which requires write lock
      assert hasWriteLock();
      if (needEnter()) {
        enter();
        // check if we are ready to initialize replication queues
        if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
          initializeReplQueues();
        }
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // the threshold is reached
      if (!isOn() ||                           // safe mode is off
          extension <= 0 || threshold <= 0) {  // don't need to wait
        this.leave(); // leave safe mode
        return;
      }
      if (reached > 0) {  // threshold has already been reached before
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // start monitor
      reached = now();
      smmthread = new Daemon(new SafeModeMonitor());
      smmthread.start();
      reportStatus("STATE* Safe mode extension entered.", true);

      // check if we are ready to initialize replication queues
      if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
        initializeReplQueues();
      }
    }
      
    /**
     * Set total number of blocks.
     */
    private synchronized void setBlockTotal(int total) {
      this.blockTotal = total;
      this.blockThreshold = (int) (blockTotal * threshold);
      this.blockReplQueueThreshold = 
        (int) (blockTotal * replQueueThreshold);
      if (haEnabled) {
        // After we initialize the block count, any further namespace
        // modifications done while in safe mode need to keep track
        // of the number of total blocks in the system.
        this.shouldIncrementallyTrackBlocks = true;
      }
      if(blockSafe < 0)
        this.blockSafe = 0;
      checkMode();
    }
      
    /**
     * Increment number of safe blocks if current block has 
     * reached minimal replication.
     * @param replication current replication 
     */
    private synchronized void incrementSafeBlockCount(short replication) {
      if (replication == safeReplication) {
        this.blockSafe++;
        checkMode();
      }
    }
      
    /**
     * Decrement number of safe blocks if current block has 
     * fallen below minimal replication.
     * @param replication current replication 
     */
    private synchronized void decrementSafeBlockCount(short replication) {
      if (replication == safeReplication-1) {
        this.blockSafe--;
        assert blockSafe >= 0 || isManual();
        checkMode();
      }
    }

    /**
     * Check if safe mode was entered manually or automatically (at startup, or
     * when disk space is low).
     */
    private boolean isManual() {
      return extension == Integer.MAX_VALUE && !resourcesLow;
    }

    /**
     * Set manual safe mode.
     */
    private synchronized void setManual() {
      extension = Integer.MAX_VALUE;
    }

    /**
     * Check if safe mode was entered due to resources being low.
     */
    private boolean areResourcesLow() {
      return resourcesLow;
    }

    /**
     * Set that resources are low for this instance of safe mode.
     */
    private void setResourcesLow() {
      resourcesLow = true;
    }

    /**
     * A tip on how safe mode is to be turned off: manually or automatically.
     */
    String getTurnOffTip() {
      if(reached < 0)
        return "Safe mode is OFF.";
      String leaveMsg = "";
      if (areResourcesLow()) {
        leaveMsg = "Resources are low on NN. " 
        	+ "Please add or free up more resources then turn off safe mode manually.  "
        	+ "NOTE:  If you turn off safe mode before adding resources, "
        	+ "the NN will immediately return to safe mode.";
      } else {
        leaveMsg = "Safe mode will be turned off automatically";
      }
      if(isManual()) {
        leaveMsg = "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off";
      }

      if(blockTotal < 0)
        return leaveMsg + ".";

      int numLive = getNumLiveDataNodes();
      String msg = "";
      if (reached == 0) {
        if (blockSafe < blockThreshold) {
          msg += String.format(
            "The reported blocks %d needs additional %d"
            + " blocks to reach the threshold %.4f of total blocks %d.",
            blockSafe, (blockThreshold - blockSafe) + 1, threshold, blockTotal);
        }
        if (numLive < datanodeThreshold) {
          if (!"".equals(msg)) {
            msg += "\n";
          }
          msg += String.format(
            "The number of live datanodes %d needs an additional %d live "
            + "datanodes to reach the minimum number %d.",
            numLive, (datanodeThreshold - numLive), datanodeThreshold);
        }
        msg += " " + leaveMsg;
      } else {
        msg = String.format("The reported blocks %d has reached the threshold"
            + " %.4f of total blocks %d.", blockSafe, threshold, 
            blockTotal);

        if (datanodeThreshold > 0) {
          msg += String.format(" The number of live datanodes %d has reached "
                               + "the minimum number %d.",
                               numLive, datanodeThreshold);
        }
        msg += " " + leaveMsg;
      }
      if(reached == 0 || isManual()) {  // threshold is not reached or manual       
        return msg + ".";
      }
      // extension period is in progress
      return msg + " in " + Math.abs(reached + extension - now()) / 1000
          + " seconds.";
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow) {
      long curTime = now();
      if(!rightNow && (curTime - lastStatusReport < 20 * 1000))
        return;
      NameNode.stateChangeLog.info(msg + " \n" + getTurnOffTip());
      lastStatusReport = curTime;
    }

    @Override
    public String toString() {
      String resText = "Current safe blocks = " 
        + blockSafe 
        + ". Target blocks = " + blockThreshold + " for threshold = %" + threshold
        + ". Minimal replication = " + safeReplication + ".";
      if (reached > 0) 
        resText += " Threshold was reached " + new Date(reached) + ".";
      return resText;
    }
      
    /**
     * Checks consistency of the class state.
     * This is costly so only runs if asserts are enabled.
     */
    private void doConsistencyCheck() {
      boolean assertsOn = false;
      assert assertsOn = true; // set to true if asserts are on
      if (!assertsOn) return;
      
      if (blockTotal == -1 && blockSafe == -1) {
        return; // manual safe mode
      }
      int activeBlocks = blockManager.getActiveBlockCount();
      if ((blockTotal != activeBlocks) &&
          !(blockSafe >= 0 && blockSafe <= blockTotal)) {
        throw new AssertionError(
            " SafeMode: Inconsistent filesystem state: "
        + "SafeMode data: blockTotal=" + blockTotal
        + " blockSafe=" + blockSafe + "; "
        + "BlockManager data: active="  + activeBlocks);
      }
    }

    private synchronized void adjustBlockTotals(int deltaSafe, int deltaTotal) {
      if (!shouldIncrementallyTrackBlocks) {
        return;
      }
      assert haEnabled;
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adjusting block totals from " +
            blockSafe + "/" + blockTotal + " to " +
            (blockSafe + deltaSafe) + "/" + (blockTotal + deltaTotal));
      }
      assert blockSafe + deltaSafe >= 0 : "Can't reduce blockSafe " +
        blockSafe + " by " + deltaSafe + ": would be negative";
      assert blockTotal + deltaTotal >= 0 : "Can't reduce blockTotal " +
        blockTotal + " by " + deltaTotal + ": would be negative";
      
      blockSafe += deltaSafe;
      setBlockTotal(blockTotal + deltaTotal);
    }
  }
    
  /**
   * Periodically check whether it is time to leave safe mode.
   * This thread starts when the threshold level is reached.
   *
   */
  class SafeModeMonitor implements Runnable {
    /** interval in msec for checking safe mode: {@value} */
    private static final long recheckInterval = 1000;
      
    /**
     */
    @Override
    public void run() {
      while (fsRunning && (safeMode != null && !safeMode.canLeave())) {
        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
        }
      }
      if (!fsRunning) {
        LOG.info("NameNode is being shutdown, exit SafeModeMonitor thread");
      } else {
        // leave safe mode and stop the monitor
        leaveSafeMode();
      }
      smmthread = null;
    }
  }
    
  boolean setSafeMode(SafeModeAction action) throws IOException {
    if (action != SafeModeAction.SAFEMODE_GET) {
      checkSuperuserPrivilege();
      switch(action) {
      case SAFEMODE_LEAVE: // leave safe mode
        leaveSafeMode();
        break;
      case SAFEMODE_ENTER: // enter safe mode
        enterSafeMode(false);
        break;
      default:
        LOG.error("Unexpected safe mode action");
      }
    }
    return isInSafeMode();
  }

  @Override
  public void checkSafeMode() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode != null) {
      safeMode.checkMode();
    }
  }

  @Override
  public boolean isInSafeMode() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return false;
    return safeMode.isOn();
  }

  @Override
  public boolean isInStartupSafeMode() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return false;
    return !safeMode.isManual() && safeMode.isOn();
  }

  @Override
  public boolean isPopulatingReplQueues() {
    if (!shouldPopulateReplQueues()) {
      return false;
    }
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return true;
    return safeMode.isPopulatingReplQueues();
  }

  private boolean shouldPopulateReplQueues() {
    if(haContext == null || haContext.getState() == null)
      return false;
    return haContext.getState().shouldPopulateReplQueues();
  }

  @Override
  public void incrementSafeBlockCount(int replication) {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return;
    safeMode.incrementSafeBlockCount((short)replication);
  }

  @Override
  public void decrementSafeBlockCount(Block b) {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) // mostly true
      return;
    BlockInfo storedBlock = blockManager.getStoredBlock(b);
    if (storedBlock.isComplete()) {
      safeMode.decrementSafeBlockCount((short)blockManager.countNodes(b).liveReplicas());
    }
  }
  
  /**
   * Adjust the total number of blocks safe and expected during safe mode.
   * If safe mode is not currently on, this is a no-op.
   * @param deltaSafe the change in number of safe blocks
   * @param deltaTotal the change i nnumber of total blocks expected
   */
  @Override
  public void adjustSafeModeBlockTotals(int deltaSafe, int deltaTotal) {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return;
    safeMode.adjustBlockTotals(deltaSafe, deltaTotal);
  }

  /**
   * Set the total number of blocks in the system. 
   */
  public void setBlockTotal() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return;
    safeMode.setBlockTotal((int)getCompleteBlocksTotal());
  }

  /**
   * Get the total number of blocks in the system. 
   */
  @Override // FSNamesystemMBean
  @Metric
  public long getBlocksTotal() {
    return blockManager.getTotalBlocks();
  }

  /**
   * Get the total number of COMPLETE blocks in the system.
   * For safe mode only complete blocks are counted.
   */
  private long getCompleteBlocksTotal() {
    // Calculate number of blocks under construction
    long numUCBlocks = 0;
    readLock();
    try {
      for (Lease lease : leaseManager.getSortedLeases()) {
        for (String path : lease.getPaths()) {
          final INodeFileUnderConstruction cons;
          try {
            cons = INodeFileUnderConstruction.valueOf(dir.getINode(path), path);
          } catch (UnresolvedLinkException e) {
            throw new AssertionError("Lease files should reside on this FS");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          BlockInfo[] blocks = cons.getBlocks();
          if(blocks == null)
            continue;
          for(BlockInfo b : blocks) {
            if(!b.isComplete())
              numUCBlocks++;
          }
        }
      }
      LOG.info("Number of blocks under construction: " + numUCBlocks);
      return getBlocksTotal() - numUCBlocks;
    } finally {
      readUnlock();
    }
  }

  /**
   * Enter safe mode manually.
   * @throws IOException
   */
  void enterSafeMode(boolean resourcesLow) throws IOException {
    writeLock();
    try {
      // Stop the secret manager, since rolling the master key would
      // try to write to the edit log
      stopSecretManager();

      // Ensure that any concurrent operations have been fully synced
      // before entering safe mode. This ensures that the FSImage
      // is entirely stable on disk as soon as we're in safe mode.
      boolean isEditlogOpenForWrite = getEditLog().isOpenForWrite();
      // Before Editlog is in OpenForWrite mode, editLogStream will be null. So,
      // logSyncAll call can be called only when Edlitlog is in OpenForWrite mode
      if (isEditlogOpenForWrite) {
        getEditLog().logSyncAll();
      }
      if (!isInSafeMode()) {
        safeMode = new SafeModeInfo(resourcesLow);
        return;
      }
      if (resourcesLow) {
        safeMode.setResourcesLow();
      }
      safeMode.setManual();
      if (isEditlogOpenForWrite) {
        getEditLog().logSyncAll();
      }
      NameNode.stateChangeLog.info("STATE* Safe mode is ON"
          + safeMode.getTurnOffTip());
    } finally {
      writeUnlock();
    }
  }

  /**
   * Leave safe mode.
   * @throws IOException
   */
  void leaveSafeMode() {
    writeLock();
    try {
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("STATE* Safe mode is already OFF"); 
        return;
      }
      safeMode.leave();
    } finally {
      writeUnlock();
    }
  }
    
  String getSafeModeTip() {
    readLock();
    try {
      if (!isInSafeMode()) {
        return "";
      }
      return safeMode.getTurnOffTip();
    } finally {
      readUnlock();
    }
  }

  CheckpointSignature rollEditLog() throws IOException {
    writeLock();
    try {
      checkOperation(OperationCategory.JOURNAL);
      checkSuperuserPrivilege();
      if (isInSafeMode()) {
        throw new SafeModeException("Log not rolled", safeMode);
      }
      LOG.info("Roll Edit Log from " + Server.getRemoteAddress());
      return getFSImage().rollEditLog();
    } finally {
      writeUnlock();
    }
  }

  NamenodeCommand startCheckpoint(
                                NamenodeRegistration bnReg, // backup node
                                NamenodeRegistration nnReg) // active name-node
  throws IOException {
    writeLock();
    try {
      checkOperation(OperationCategory.CHECKPOINT);

      if (isInSafeMode()) {
        throw new SafeModeException("Checkpoint not started", safeMode);
      }
      LOG.info("Start checkpoint for " + bnReg.getAddress());
      NamenodeCommand cmd = getFSImage().startCheckpoint(bnReg, nnReg);
      getEditLog().logSync();
      return cmd;
    } finally {
      writeUnlock();
    }
  }

  void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    readLock();
    try {
      checkOperation(OperationCategory.CHECKPOINT);

      if (isInSafeMode()) {
        throw new SafeModeException("Checkpoint not ended", safeMode);
      }
      LOG.info("End checkpoint for " + registration.getAddress());
      getFSImage().endCheckpoint(sig);
    } finally {
      readUnlock();
    }
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), supergroup, permission);
  }

  private FSPermissionChecker checkOwner(String path
      ) throws AccessControlException, UnresolvedLinkException {
    return checkPermission(path, true, null, null, null, null);
  }

  private FSPermissionChecker checkPathAccess(String path, FsAction access
      ) throws AccessControlException, UnresolvedLinkException {
    return checkPermission(path, false, null, null, access, null);
  }

  private FSPermissionChecker checkParentAccess(String path, FsAction access
      ) throws AccessControlException, UnresolvedLinkException {
    return checkPermission(path, false, null, access, null, null);
  }

  private FSPermissionChecker checkAncestorAccess(String path, FsAction access
      ) throws AccessControlException, UnresolvedLinkException {
    return checkPermission(path, false, access, null, null, null);
  }

  private FSPermissionChecker checkTraverse(String path
      ) throws AccessControlException, UnresolvedLinkException {
    return checkPermission(path, false, null, null, null, null);
  }

  @Override
  public void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      FSPermissionChecker.checkSuperuserPrivilege(fsOwner, supergroup);
    }
  }

  /**
   * Check whether current user have permissions to access the path.
   * For more details of the parameters, see
   * {@link FSPermissionChecker#checkPermission(String, INodeDirectory, boolean, FsAction, FsAction, FsAction, FsAction)}.
   */
  private FSPermissionChecker checkPermission(String path, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess) throws AccessControlException, UnresolvedLinkException {
    FSPermissionChecker pc = new FSPermissionChecker(
        fsOwner.getShortUserName(), supergroup);
    if (!pc.isSuper) {
      dir.waitForReady();
      readLock();
      try {
        pc.checkPermission(path, dir.rootDir, doCheckOwner,
            ancestorAccess, parentAccess, access, subAccess);
      } finally {
        readUnlock();
      } 
    }
    return pc;
  }

  /**
   * Check to see if we have exceeded the limit on the number
   * of inodes.
   */
  void checkFsObjectLimit() throws IOException {
    if (maxFsObjects != 0 &&
        maxFsObjects <= dir.totalInodes() + getBlocksTotal()) {
      throw new IOException("Exceeded the configured number of objects " +
                             maxFsObjects + " in the filesystem.");
    }
  }

  /**
   * Get the total number of objects in the system. 
   */
  long getMaxObjects() {
    return maxFsObjects;
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getFilesTotal() {
    readLock();
    try {
      return this.dir.totalInodes();
    } finally {
      readUnlock();
    }
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getPendingReplicationBlocks() {
    return blockManager.getPendingReplicationBlocksCount();
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getUnderReplicatedBlocks() {
    return blockManager.getUnderReplicatedBlocksCount();
  }

  /** Returns number of blocks with corrupt replicas */
  @Metric({"CorruptBlocks", "Number of blocks with corrupt replicas"})
  public long getCorruptReplicaBlocks() {
    return blockManager.getCorruptReplicaBlocksCount();
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getScheduledReplicationBlocks() {
    return blockManager.getScheduledReplicationBlocksCount();
  }

  @Metric
  public long getPendingDeletionBlocks() {
    return blockManager.getPendingDeletionBlocksCount();
  }

  @Metric
  public long getExcessBlocks() {
    return blockManager.getExcessBlocksCount();
  }
  
  // HA-only metric
  @Metric
  public long getPostponedMisreplicatedBlocks() {
    return blockManager.getPostponedMisreplicatedBlocksCount();
  }

  // HA-only metric
  @Metric
  public int getPendingDataNodeMessageCount() {
    return blockManager.getPendingDataNodeMessageCount();
  }
  
  // HA-only metric
  @Metric
  public String getHAState() {
    return haContext.getState().toString();
  }

  // HA-only metric
  @Metric
  public long getMillisSinceLastLoadedEdits() {
    if (isInStandbyState() && editLogTailer != null) {
      return now() - editLogTailer.getLastLoadTimestamp();
    } else {
      return 0;
    }
  }
  
  @Metric
  public int getBlockCapacity() {
    return blockManager.getCapacity();
  }

  @Override // FSNamesystemMBean
  public String getFSState() {
    return isInSafeMode() ? "safeMode" : "Operational";
  }
  
  private ObjectName mbeanName;

  /**
   * Register the FSNamesystem MBean using the name
   *        "hadoop:service=NameNode,name=FSNamesystemState"
   */
  private void registerMBean() {
    // We can only implement one MXBean interface, so we keep the old one.
    try {
      StandardMBean bean = new StandardMBean(this, FSNamesystemMBean.class);
      mbeanName = MBeans.register("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad MBean setup", e);
    }

    LOG.info("Registered FSNamesystemState MBean");
  }

  /**
   * shutdown FSNamesystem
   */
  void shutdown() {
    if (mbeanName != null)
      MBeans.unregister(mbeanName);
  }
  

  @Override // FSNamesystemMBean
  public int getNumLiveDataNodes() {
    return getBlockManager().getDatanodeManager().getNumLiveDataNodes();
  }

  @Override // FSNamesystemMBean
  public int getNumDeadDataNodes() {
    return getBlockManager().getDatanodeManager().getNumDeadDataNodes();
  }
  
  @Override // FSNamesystemMBean
  @Metric({"StaleDataNodes", 
    "Number of datanodes marked stale due to delayed heartbeat"})
  public int getNumStaleDataNodes() {
    return getBlockManager().getDatanodeManager().getNumStaleNodes();
  }

  /**
   * Sets the generation stamp for this filesystem
   */
  void setGenerationStamp(long stamp) {
    generationStamp.setCurrentValue(stamp);
  }

  /**
   * Gets the generation stamp for this filesystem
   */
  long getGenerationStamp() {
    return generationStamp.getCurrentValue();
  }

  /**
   * Increments, logs and then returns the stamp
   */
  private long nextGenerationStamp() throws SafeModeException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException(
          "Cannot get next generation stamp", safeMode);
    }
    final long gs = generationStamp.nextValue();
    getEditLog().logGenerationStamp(gs);
    // NB: callers sync the log
    return gs;
  }

  private INodeFileUnderConstruction checkUCBlock(ExtendedBlock block,
      String clientName) throws IOException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot get a new generation stamp and an " +
                                "access token for block " + block, safeMode);
    }
    
    // check stored block state
    BlockInfo storedBlock = blockManager.getStoredBlock(ExtendedBlock.getLocalBlock(block));
    if (storedBlock == null || 
        storedBlock.getBlockUCState() != BlockUCState.UNDER_CONSTRUCTION) {
        throw new IOException(block + 
            " does not exist or is not under Construction" + storedBlock);
    }
    
    // check file inode
    INodeFile file = (INodeFile) storedBlock.getBlockCollection();
    if (file==null || !file.isUnderConstruction()) {
      throw new IOException("The file " + storedBlock + 
          " belonged to does not exist or it is not under construction.");
    }
    
    // check lease
    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)file;
    if (clientName == null || !clientName.equals(pendingFile.getClientName())) {
      throw new LeaseExpiredException("Lease mismatch: " + block + 
          " is accessed by a non lease holder " + clientName); 
    }

    return pendingFile;
  }
  
  /**
   * Client is reporting some bad block locations.
   */
  void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      
      NameNode.stateChangeLog.info("*DIR* reportBadBlocks");
      for (int i = 0; i < blocks.length; i++) {
        ExtendedBlock blk = blocks[i].getBlock();
        DatanodeInfo[] nodes = blocks[i].getLocations();
        for (int j = 0; j < nodes.length; j++) {
          DatanodeInfo dn = nodes[j];
          blockManager.findAndMarkBlockAsCorrupt(blk, dn,
              "client machine reported it");
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Get a new generation stamp together with an access token for 
   * a block under construction
   * 
   * This method is called for recovering a failed pipeline or setting up
   * a pipeline to append to a block.
   * 
   * @param block a block
   * @param clientName the name of a client
   * @return a located block with a new generation stamp and an access token
   * @throws IOException if any error occurs
   */
  LocatedBlock updateBlockForPipeline(ExtendedBlock block, 
      String clientName) throws IOException {
    LocatedBlock locatedBlock;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      // check vadility of parameters
      checkUCBlock(block, clientName);
  
      // get a new generation stamp and an access token
      block.setGenerationStamp(nextGenerationStamp());
      locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
      blockManager.setBlockToken(locatedBlock, AccessMode.WRITE);
    } finally {
      writeUnlock();
    }
    // Ensure we record the new generation stamp
    getEditLog().logSync();
    return locatedBlock;
  }
  
  /**
   * Update a pipeline for a block under construction
   * 
   * @param clientName the name of the client
   * @param oldblock and old block
   * @param newBlock a new block with a new generation stamp and length
   * @param newNodes datanodes in the pipeline
   * @throws IOException if any error occurs
   */
  void updatePipeline(String clientName, ExtendedBlock oldBlock, 
      ExtendedBlock newBlock, DatanodeID[] newNodes)
      throws IOException {
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException("Pipeline not updated", safeMode);
      }
      assert newBlock.getBlockId()==oldBlock.getBlockId() : newBlock + " and "
        + oldBlock + " has different block identifier";
      LOG.info("updatePipeline(block=" + oldBlock
               + ", newGenerationStamp=" + newBlock.getGenerationStamp()
               + ", newLength=" + newBlock.getNumBytes()
               + ", newNodes=" + Arrays.asList(newNodes)
               + ", clientName=" + clientName
               + ")");
      updatePipelineInternal(clientName, oldBlock, newBlock, newNodes);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    LOG.info("updatePipeline(" + oldBlock + ") successfully to " + newBlock);
  }

  /** @see #updatePipeline(String, ExtendedBlock, ExtendedBlock, DatanodeID[]) */
  private void updatePipelineInternal(String clientName, ExtendedBlock oldBlock, 
      ExtendedBlock newBlock, DatanodeID[] newNodes)
      throws IOException {
    assert hasWriteLock();
    // check the vadility of the block and lease holder name
    final INodeFileUnderConstruction pendingFile
        = checkUCBlock(oldBlock, clientName);
    final BlockInfoUnderConstruction blockinfo
        = (BlockInfoUnderConstruction)pendingFile.getLastBlock();

    // check new GS & length: this is not expected
    if (newBlock.getGenerationStamp() <= blockinfo.getGenerationStamp() ||
        newBlock.getNumBytes() < blockinfo.getNumBytes()) {
      String msg = "Update " + oldBlock + " (len = " + 
        blockinfo.getNumBytes() + ") to an older state: " + newBlock + 
        " (len = " + newBlock.getNumBytes() +")";
      LOG.warn(msg);
      throw new IOException(msg);
    }

    // Update old block with the new generation stamp and new length
    blockinfo.setGenerationStamp(newBlock.getGenerationStamp());
    blockinfo.setNumBytes(newBlock.getNumBytes());

    // find the DatanodeDescriptor objects
    final DatanodeManager dm = getBlockManager().getDatanodeManager();
    DatanodeDescriptor[] descriptors = null;
    if (newNodes.length > 0) {
      descriptors = new DatanodeDescriptor[newNodes.length];
      for(int i = 0; i < newNodes.length; i++) {
        descriptors[i] = dm.getDatanode(newNodes[i]);
      }
    }
    blockinfo.setExpectedLocations(descriptors);

    String src = leaseManager.findPath(pendingFile);
    dir.persistBlocks(src, pendingFile);
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  void unprotectedChangeLease(String src, String dst) {
    assert hasWriteLock();
    leaseManager.changeLease(src, dst);
  }

  /**
   * Serializes leases. 
   */
  void saveFilesUnderConstruction(DataOutputStream out) throws IOException {
    // This is run by an inferior thread of saveNamespace, which holds a read
    // lock on our behalf. If we took the read lock here, we could block
    // for fairness if a writer is waiting on the lock.
    synchronized (leaseManager) {
      Map<String, INodeFileUnderConstruction> nodes =
          leaseManager.getINodesUnderConstruction();
      out.writeInt(nodes.size()); // write the size    
      for (Map.Entry<String, INodeFileUnderConstruction> entry
           : nodes.entrySet()) {
        FSImageSerialization.writeINodeUnderConstruction(
            out, entry.getValue(), entry.getKey());
      }
    }
  }

  /**
   * Register a Backup name-node, verifying that it belongs
   * to the correct namespace, and adding it to the set of
   * active journals if necessary.
   * 
   * @param bnReg registration of the new BackupNode
   * @param nnReg registration of this NameNode
   * @throws IOException if the namespace IDs do not match
   */
  void registerBackupNode(NamenodeRegistration bnReg,
      NamenodeRegistration nnReg) throws IOException {
    writeLock();
    try {
      if(getFSImage().getStorage().getNamespaceID() 
         != bnReg.getNamespaceID())
        throw new IOException("Incompatible namespaceIDs: "
            + " Namenode namespaceID = "
            + getFSImage().getStorage().getNamespaceID() + "; "
            + bnReg.getRole() +
            " node namespaceID = " + bnReg.getNamespaceID());
      if (bnReg.getRole() == NamenodeRole.BACKUP) {
        getFSImage().getEditLog().registerBackupNode(
            bnReg, nnReg);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Release (unregister) backup node.
   * <p>
   * Find and remove the backup stream corresponding to the node.
   * @param registration
   * @throws IOException
   */
  void releaseBackupNode(NamenodeRegistration registration)
    throws IOException {
    writeLock();
    try {
      if(getFSImage().getStorage().getNamespaceID()
         != registration.getNamespaceID())
        throw new IOException("Incompatible namespaceIDs: "
            + " Namenode namespaceID = "
            + getFSImage().getStorage().getNamespaceID() + "; "
            + registration.getRole() +
            " node namespaceID = " + registration.getNamespaceID());
      getEditLog().releaseBackupStream(registration);
    } finally {
      writeUnlock();
    }
  }

  static class CorruptFileBlockInfo {
    String path;
    Block block;
    
    public CorruptFileBlockInfo(String p, Block b) {
      path = p;
      block = b;
    }
    
    @Override
    public String toString() {
      return block.getBlockName() + "\t" + path;
    }
  }
  /**
   * @param path Restrict corrupt files to this portion of namespace.
   * @param startBlockAfter Support for continuation; the set of files we return
   *  back is ordered by blockid; startBlockAfter tells where to start from
   * @return a list in which each entry describes a corrupt file/block
   * @throws AccessControlException
   * @throws IOException
   */
  Collection<CorruptFileBlockInfo> listCorruptFileBlocks(String path,
	String[] cookieTab) throws IOException {

    readLock();
    try {
      checkOperation(OperationCategory.READ);

      if (!isPopulatingReplQueues()) {
        throw new IOException("Cannot run listCorruptFileBlocks because " +
                              "replication queues have not been initialized.");
      }
      checkSuperuserPrivilege();
      // print a limited # of corrupt files per call
      int count = 0;
      ArrayList<CorruptFileBlockInfo> corruptFiles = new ArrayList<CorruptFileBlockInfo>();

      final Iterator<Block> blkIterator = blockManager.getCorruptReplicaBlockIterator();

      if (cookieTab == null) {
        cookieTab = new String[] { null };
      }
      int skip = getIntCookie(cookieTab[0]);
      for (int i = 0; i < skip && blkIterator.hasNext(); i++) {
        blkIterator.next();
      }

      while (blkIterator.hasNext()) {
        Block blk = blkIterator.next();
        INode inode = (INodeFile) blockManager.getBlockCollection(blk);
        skip++;
        if (inode != null && blockManager.countNodes(blk).liveReplicas() == 0) {
          String src = FSDirectory.getFullPathName(inode);
          if (src.startsWith(path)){
            corruptFiles.add(new CorruptFileBlockInfo(src, blk));
            count++;
            if (count >= DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED)
              break;
          }
        }
      }
      cookieTab[0] = String.valueOf(skip);
      LOG.info("list corrupt file blocks returned: " + count);
      return corruptFiles;
    } finally {
      readUnlock();
    }
  }

  /**
   * Convert string cookie to integer.
   */
  private static int getIntCookie(String cookie){
    int c;
    if(cookie == null){
      c = 0;
    } else {
      try{
        c = Integer.parseInt(cookie);
      }catch (NumberFormatException e) {
        c = 0;
      }
    }
    c = Math.max(0, c);
    return c;
  }

  /**
   * Create delegation token secret manager
   */
  private DelegationTokenSecretManager createDelegationTokenSecretManager(
      Configuration conf) {
    return new DelegationTokenSecretManager(conf.getLong(
        DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
        DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT),
        conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT),
        conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT),
        DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL, this);
  }

  /**
   * Returns the DelegationTokenSecretManager instance in the namesystem.
   * @return delegation token secret manager object
   */
  DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return dtSecretManager;
  }

  /**
   * @param renewer
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   */
  Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    Token<DelegationTokenIdentifier> token;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException("Cannot issue delegation token", safeMode);
      }
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
          "Delegation Token can be issued only with kerberos or web authentication");
      }
      if (dtSecretManager == null || !dtSecretManager.isRunning()) {
        LOG.warn("trying to get DT with no secret manager running");
        return null;
      }

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String user = ugi.getUserName();
      Text owner = new Text(user);
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }
      DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner,
        renewer, realUser);
      token = new Token<DelegationTokenIdentifier>(
        dtId, dtSecretManager);
      long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
      getEditLog().logGetDelegationToken(dtId, expiryTime);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    return token;
  }

  /**
   * 
   * @param token
   * @return New expiryTime of the token
   * @throws InvalidToken
   * @throws IOException
   */
  long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    long expiryTime;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException("Cannot renew delegation token", safeMode);
      }
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
            "Delegation Token can be renewed only with kerberos or web authentication");
      }
      String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
      expiryTime = dtSecretManager.renewToken(token, renewer);
      DelegationTokenIdentifier id = new DelegationTokenIdentifier();
      ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
      DataInputStream in = new DataInputStream(buf);
      id.readFields(in);
      getEditLog().logRenewDelegationToken(id, expiryTime);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    return expiryTime;
  }

  /**
   * 
   * @param token
   * @throws IOException
   */
  void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      if (isInSafeMode()) {
        throw new SafeModeException("Cannot cancel delegation token", safeMode);
      }
      String canceller = UserGroupInformation.getCurrentUser().getUserName();
      DelegationTokenIdentifier id = dtSecretManager
        .cancelToken(token, canceller);
      getEditLog().logCancelDelegationToken(id);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }
  
  /**
   * @param out save state of the secret manager
   */
  void saveSecretManagerState(DataOutputStream out) throws IOException {
    dtSecretManager.saveSecretManagerState(out);
  }

  /**
   * @param in load the state of secret manager from input stream
   */
  void loadSecretManagerState(DataInputStream in) throws IOException {
    dtSecretManager.loadSecretManagerState(in);
  }

  /**
   * Log the updateMasterKey operation to edit logs
   * 
   * @param key new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) {
    
    assert !isInSafeMode() :
      "this should never be called while in safemode, since we stop " +
      "the DT manager before entering safemode!";
    // No need to hold FSN lock since we don't access any internal
    // structures, and this is stopped before the FSN shuts itself
    // down, etc.
    getEditLog().logUpdateMasterKey(key);
    getEditLog().logSync();
  }
  
  private void logReassignLease(String leaseHolder, String src,
      String newHolder) {
    assert hasWriteLock();
    getEditLog().logReassignLease(leaseHolder, src, newHolder);
  }
  
  /**
   * 
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL)
        && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }
  
  /**
   * Returns authentication method used to establish the connection
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }
  
  /**
   * Client invoked methods are invoked over RPC and will be in 
   * RPC call context even if the client exits.
   */
  private boolean isExternalInvocation() {
    return Server.isRpcInvocation() || NamenodeWebHdfsMethods.isWebHdfsInvocation();
  }

  private static InetAddress getRemoteIp() {
    InetAddress ip = Server.getRemoteIp();
    if (ip != null) {
      return ip;
    }
    return NamenodeWebHdfsMethods.getRemoteIp();
  }
  
  /**
   * Log fsck event in the audit log 
   */
  void logFsckEvent(String src, InetAddress remoteAddress) throws IOException {
    if (isAuditEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    remoteAddress,
                    "fsck", src, null, null);
    }
  }
  /**
   * Register NameNodeMXBean
   */
  private void registerMXBean() {
    MBeans.register("NameNode", "NameNodeInfo", this);
  }

  /**
   * Class representing Namenode information for JMX interfaces
   */
  @Override // NameNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override // NameNodeMXBean
  public long getUsed() {
    return this.getCapacityUsed();
  }

  @Override // NameNodeMXBean
  public long getFree() {
    return this.getCapacityRemaining();
  }

  @Override // NameNodeMXBean
  public long getTotal() {
    return this.getCapacityTotal();
  }

  @Override // NameNodeMXBean
  public String getSafemode() {
    if (!this.isInSafeMode())
      return "";
    return "Safe mode is ON." + this.getSafeModeTip();
  }

  @Override // NameNodeMXBean
  public boolean isUpgradeFinalized() {
    return this.getFSImage().isUpgradeFinalized();
  }

  @Override // NameNodeMXBean
  public long getNonDfsUsedSpace() {
    return datanodeStatistics.getCapacityUsedNonDFS();
  }

  @Override // NameNodeMXBean
  public float getPercentUsed() {
    return datanodeStatistics.getCapacityUsedPercent();
  }

  @Override // NameNodeMXBean
  public long getBlockPoolUsedSpace() {
    return datanodeStatistics.getBlockPoolUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentBlockPoolUsed() {
    return datanodeStatistics.getPercentBlockPoolUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentRemaining() {
    return datanodeStatistics.getCapacityRemainingPercent();
  }

  @Override // NameNodeMXBean
  public long getTotalBlocks() {
    return getBlocksTotal();
  }

  @Override // NameNodeMXBean
  @Metric
  public long getTotalFiles() {
    return getFilesTotal();
  }

  @Override // NameNodeMXBean
  public long getNumberOfMissingBlocks() {
    return getMissingBlocksCount();
  }
  
  @Override // NameNodeMXBean
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of live node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getLiveNodes() {
    final Map<String, Map<String,Object>> info = 
      new HashMap<String, Map<String,Object>>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, true);
    for (DatanodeDescriptor node : live) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("lastContact", getLastContact(node));
      innerinfo.put("usedSpace", getDfsUsed(node));
      innerinfo.put("adminState", node.getAdminState().toString());
      innerinfo.put("nonDfsUsedSpace", node.getNonDfsUsed());
      innerinfo.put("capacity", node.getCapacity());
      innerinfo.put("numBlocks", node.numBlocks());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of dead node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDeadNodes() {
    final Map<String, Map<String, Object>> info = 
      new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(null, dead, true);
    for (DatanodeDescriptor node : dead) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("lastContact", getLastContact(node));
      innerinfo.put("decommissioned", node.isDecommissioned());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decomisioning node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDecomNodes() {
    final Map<String, Map<String, Object>> info = 
      new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> decomNodeList = blockManager.getDatanodeManager(
        ).getDecommissioningNodes();
    for (DatanodeDescriptor node : decomNodeList) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("underReplicatedBlocks", node.decommissioningStatus
          .getUnderReplicatedBlocks());
      innerinfo.put("decommissionOnlyReplicas", node.decommissioningStatus
          .getDecommissionOnlyReplicas());
      innerinfo.put("underReplicateInOpenFiles", node.decommissioningStatus
          .getUnderReplicatedInOpenFiles());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (Time.now() - alivenode.getLastUpdate())/1000;
  }

  private long getDfsUsed(DatanodeDescriptor alivenode) {
    return alivenode.getDfsUsed();
  }

  @Override  // NameNodeMXBean
  public String getClusterId() {
    return dir.fsImage.getStorage().getClusterID();
  }
  
  @Override  // NameNodeMXBean
  public String getBlockPoolId() {
    return blockPoolId;
  }
  
  @Override  // NameNodeMXBean
  public String getNameDirStatuses() {
    Map<String, Map<File, StorageDirType>> statusMap =
      new HashMap<String, Map<File, StorageDirType>>();
    
    Map<File, StorageDirType> activeDirs = new HashMap<File, StorageDirType>();
    for (Iterator<StorageDirectory> it
        = getFSImage().getStorage().dirIterator(); it.hasNext();) {
      StorageDirectory st = it.next();
      activeDirs.put(st.getRoot(), st.getStorageDirType());
    }
    statusMap.put("active", activeDirs);
    
    List<Storage.StorageDirectory> removedStorageDirs
        = getFSImage().getStorage().getRemovedStorageDirs();
    Map<File, StorageDirType> failedDirs = new HashMap<File, StorageDirType>();
    for (StorageDirectory st : removedStorageDirs) {
      failedDirs.put(st.getRoot(), st.getStorageDirType());
    }
    statusMap.put("failed", failedDirs);
    
    return JSON.toString(statusMap);
  }

  /** @return the block manager. */
  public BlockManager getBlockManager() {
    return blockManager;
  }
  /** @return the FSDirectory. */
  public FSDirectory getFSDirectory() {
    return dir;
  }

  /**
   * Verifies that the given identifier and password are valid and match.
   * @param identifier Token identifier.
   * @param password Password in the token.
   * @throws InvalidToken
   */
  public synchronized void verifyToken(DelegationTokenIdentifier identifier,
      byte[] password) throws InvalidToken {
    getDelegationTokenSecretManager().verifyToken(identifier, password);
  }
  
  @Override
  public boolean isGenStampInFuture(long genStamp) {
    return (genStamp > getGenerationStamp());
  }
  @VisibleForTesting
  public EditLogTailer getEditLogTailer() {
    return editLogTailer;
  }
  
  @VisibleForTesting
  void setFsLockForTests(ReentrantReadWriteLock lock) {
    this.fsLock = lock;
  }
  
  @VisibleForTesting
  ReentrantReadWriteLock getFsLockForTests() {
    return fsLock;
  }

  @VisibleForTesting
  public SafeModeInfo getSafeModeInfoForTests() {
    return safeMode;
  }
  
  @VisibleForTesting
  public void setNNResourceChecker(NameNodeResourceChecker nnResourceChecker) {
    this.nnResourceChecker = nnResourceChecker;
  }

  @Override
  public boolean isAvoidingStaleDataNodesForWrite() {
    return this.blockManager.getDatanodeManager()
        .shouldAvoidStaleDataNodesForWrite();
  }

  /**
   * Default AuditLogger implementation; used when no access logger is
   * defined in the config file. It can also be explicitly listed in the
   * config file.
   */
  private static class DefaultAuditLogger implements AuditLogger {

    @Override
    public void initialize(Configuration conf) {
      // Nothing to do.
    }

    @Override
    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus status) {
      if (auditLog.isInfoEnabled()) {
        final StringBuilder sb = auditBuffer.get();
        sb.setLength(0);
        sb.append("allowed=").append(succeeded).append("\t");
        sb.append("ugi=").append(userName).append("\t");
        sb.append("ip=").append(addr).append("\t");
        sb.append("cmd=").append(cmd).append("\t");
        sb.append("src=").append(src).append("\t");
        sb.append("dst=").append(dst).append("\t");
        if (null == status) {
          sb.append("perm=null");
        } else {
          sb.append("perm=");
          sb.append(status.getOwner()).append(":");
          sb.append(status.getGroup()).append(":");
          sb.append(status.getPermission());
        }
        auditLog.info(sb);
      }
    }

  }

}
