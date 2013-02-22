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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_UPGRADE_PERMISSION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_UPGRADE_PERMISSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_KEY;
import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
import org.apache.hadoop.hdfs.DFSUtil;
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
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
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
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import com.google.common.annotations.VisibleForTesting;

/***************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 *
 * It tracks several important tables.
 *
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 ***************************************************/
@InterfaceAudience.Private
@Metrics(context="dfs")
public class FSNamesystem implements Namesystem, FSClusterStats,
    FSNamesystemMBean, NameNodeMXBean {
  static final Log LOG = LogFactory.getLog(FSNamesystem.class);

  private static final ThreadLocal<StringBuilder> auditBuffer =
    new ThreadLocal<StringBuilder>() {
      protected StringBuilder initialValue() {
        return new StringBuilder();
      }
  };

  private static final void logAuditEvent(UserGroupInformation ugi,
      InetAddress addr, String cmd, String src, String dst,
      HdfsFileStatus stat) {
    final StringBuilder sb = auditBuffer.get();
    sb.setLength(0);
    sb.append("ugi=").append(ugi).append("\t");
    sb.append("ip=").append(addr).append("\t");
    sb.append("cmd=").append(cmd).append("\t");
    sb.append("src=").append(src).append("\t");
    sb.append("dst=").append(dst).append("\t");
    if (null == stat) {
      sb.append("perm=null");
    } else {
      sb.append("perm=");
      sb.append(stat.getOwner()).append(":");
      sb.append(stat.getGroup()).append(":");
      sb.append(stat.getPermission());
    }
    auditLog.info(sb);
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
  private boolean isPermissionEnabled;
  private UserGroupInformation fsOwner;
  private String fsOwnerShortUserName;
  private String supergroup;
  private PermissionStatus defaultPermission;
  
  // Scan interval is not configurable.
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
    TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  private DelegationTokenSecretManager dtSecretManager;

  //
  // Stores the correct file name hierarchy
  //
  FSDirectory dir;
  private BlockManager blockManager;
  private DatanodeStatistics datanodeStatistics;

  // Block pool ID used by this namenode
  private String blockPoolId;

  LeaseManager leaseManager = new LeaseManager(this); 

  Daemon lmthread = null;   // LeaseMonitor thread
  Daemon smmthread = null;  // SafeModeMonitor thread
  
  Daemon nnrmthread = null; // NamenodeResourceMonitor thread

  private volatile boolean hasResourcesAvailable = false;
  private volatile boolean fsRunning = true;
  long systemStart = 0;

  //resourceRecheckInterval is how often namenode checks for the disk space availability
  private long resourceRecheckInterval;

  // The actual resource checker instance.
  NameNodeResourceChecker nnResourceChecker;

  private FsServerDefaults serverDefaults;
  // allow appending to hdfs files
  private boolean supportAppends = true;
  private ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure = 
      ReplaceDatanodeOnFailure.DEFAULT;

  private volatile SafeModeInfo safeMode;  // safe mode information

  private long maxFsObjects = 0;          // maximum number of fs objects

  /**
   * The global generation stamp for this file system. 
   */
  private final GenerationStamp generationStamp = new GenerationStamp();

  // precision of access times.
  private long accessTimePrecision = 0;

  // lock to protect FSNamesystem.
  private ReentrantReadWriteLock fsLock;

  /**
   * FSNamesystem constructor.
   */
  FSNamesystem(Configuration conf) throws IOException {
    try {
      initialize(conf, null);
    } catch(IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    }
  }

  /**
   * Initialize FSNamesystem.
   */
  private void initialize(Configuration conf, FSImage fsImage)
      throws IOException {
    resourceRecheckInterval = conf.getLong(
        DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
        DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);
    nnResourceChecker = new NameNodeResourceChecker(conf);
    checkAvailableResources();
    this.systemStart = now();
    this.blockManager = new BlockManager(this, this, conf);
    this.datanodeStatistics = blockManager.getDatanodeManager().getDatanodeStatistics();
    this.fsLock = new ReentrantReadWriteLock(true); // fair locking
    setConfigurationParameters(conf);
    dtSecretManager = createDelegationTokenSecretManager(conf);
    this.registerMBean(); // register the MBean for the FSNamesystemState
    if(fsImage == null) {
      this.dir = new FSDirectory(this, conf);
      StartupOption startOpt = NameNode.getStartupOption(conf);
      this.dir.loadFSImage(startOpt);
      long timeTakenToLoadFSImage = now() - systemStart;
      LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
      NameNode.getNameNodeMetrics().setFsImageLoadTime(
                                (int) timeTakenToLoadFSImage);
    } else {
      this.dir = new FSDirectory(fsImage, this, conf);
    }
    this.safeMode = new SafeModeInfo(conf);
  }

  void activateSecretManager() throws IOException {
    if (dtSecretManager != null) {
      dtSecretManager.startThreads();
    }
  }
  
  /**
   * Activate FSNamesystem daemons.
   */
  void activate(Configuration conf) throws IOException {
    writeLock();
    try {
      setBlockTotal();
      blockManager.activate(conf);

      this.lmthread = new Daemon(leaseManager.new Monitor());
      lmthread.start();
      this.nnrmthread = new Daemon(new NameNodeResourceMonitor());
      nnrmthread.start();
    } finally {
      writeUnlock();
    }
    
    registerMXBean();
    DefaultMetricsSystem.instance().register(this);
  }

  public static Collection<URI> getNamespaceDirs(Configuration conf) {
    return getStorageDirs(conf, DFS_NAMENODE_NAME_DIR_KEY);
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

  public static Collection<URI> getNamespaceEditsDirs(Configuration conf) {
    return getStorageDirs(conf, DFS_NAMENODE_EDITS_DIR_KEY);
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

  /**
   * dirs is a list of directories where the filesystem directory state 
   * is stored
   */
  FSNamesystem(FSImage fsImage, Configuration conf) throws IOException {
    this.fsLock = new ReentrantReadWriteLock(true);
    this.blockManager = new BlockManager(this, this, conf);
    setConfigurationParameters(conf);
    this.dir = new FSDirectory(fsImage, this, conf);
    dtSecretManager = createDelegationTokenSecretManager(conf);
  }

  /**
   * Create FSNamesystem for {@link BackupNode}.
   * Should do everything that would be done for the NameNode,
   * except for loading the image.
   * 
   * @param bnImage {@link BackupImage}
   * @param conf configuration
   * @throws IOException
   */
  FSNamesystem(Configuration conf, BackupImage bnImage) throws IOException {
    try {
      initialize(conf, bnImage);
    } catch(IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    }
  }

  /**
   * Initializes some of the members from configuration
   */
  private void setConfigurationParameters(Configuration conf) 
                                          throws IOException {
    fsOwner = UserGroupInformation.getCurrentUser();
    fsOwnerShortUserName = fsOwner.getShortUserName();
    
    LOG.info("fsOwner=" + fsOwner);

    this.supergroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY, 
                               DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
                                               DFS_PERMISSIONS_ENABLED_DEFAULT);
    LOG.info("supergroup=" + supergroup);
    LOG.info("isPermissionEnabled=" + isPermissionEnabled);
    short filePermission = (short)conf.getInt(DFS_NAMENODE_UPGRADE_PERMISSION_KEY,
                                              DFS_NAMENODE_UPGRADE_PERMISSION_DEFAULT);
    this.defaultPermission = PermissionStatus.createImmutable(
        fsOwner.getShortUserName(), supergroup, new FsPermission(filePermission));

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
        checksumType);
    
    this.maxFsObjects = conf.getLong(DFS_NAMENODE_MAX_OBJECTS_KEY, 
                                     DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

    this.accessTimePrecision = conf.getLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 0);
    this.supportAppends = conf.getBoolean(DFS_SUPPORT_APPEND_KEY,
        DFS_SUPPORT_APPEND_DEFAULT);

    this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
  }

  /**
   * Return the default path permission when upgrading from releases with no
   * permissions (<=0.15) to releases with permissions (>=0.16)
   */
  protected PermissionStatus getUpgradePermission() {
    return defaultPermission;
  }
  
  NamespaceInfo getNamespaceInfo() {
    readLock();
    try {
      return new NamespaceInfo(dir.fsImage.getStorage().getNamespaceID(),
          getClusterId(), getBlockPoolId(),
          dir.fsImage.getStorage().getCTime(),
          upgradeManager.getUpgradeVersion());
    } finally {
      readUnlock();
    }
  }

  /**
   * Close down this file system manager.
   * Causes heartbeat and lease daemons to stop; waits briefly for
   * them to finish, but a short timeout returns control back to caller.
   */
  void close() {
    fsRunning = false;
    try {
      if (blockManager != null) blockManager.close();
      if (smmthread != null) smmthread.interrupt();
      if (dtSecretManager != null) dtSecretManager.stopThreads();
      if (nnrmthread != null) nnrmthread.interrupt();
    } catch (Exception e) {
      LOG.warn("Exception shutting down FSNamesystem", e);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        if (lmthread != null) {
          lmthread.interrupt();
          lmthread.join(3000);
        }
        if (dir != null) {
          dir.close();
        }
      } catch (InterruptedException ie) {
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

  /**
   * Dump all metadata into specified file
   */
  void metaSave(String filename) throws IOException {
    checkSuperuserPrivilege();
    writeLock();
    try {
      File file = new File(System.getProperty("hadoop.log.dir"), filename);
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file,
          true)));
  
      long totalInodes = this.dir.totalInodes();
      long totalBlocks = this.getBlocksTotal();
      out.println(totalInodes + " files and directories, " + totalBlocks
          + " blocks = " + (totalInodes + totalBlocks) + " total");

      blockManager.metaSave(out);

      out.flush();
      out.close();
    } finally {
      writeUnlock();
    }
  }

  long getDefaultBlockSize() {
    return serverDefaults.getBlockSize();
  }

  FsServerDefaults getServerDefaults() {
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
    HdfsFileStatus resultingStat = null;
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set permission for " + src, safeMode);
      }
      checkOwner(pc, src);
      dir.setPermission(src, permission);
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(src, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
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
    HdfsFileStatus resultingStat = null;
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set owner for " + src, safeMode);
      }
      checkOwner(pc, src);
      if (!pc.isSuperUser()) {
        if (username != null && !pc.getUser().equals(username)) {
          throw new AccessControlException("Non-super user cannot change owner");
        }
        if (group != null && !pc.containsGroup(group)) {
          throw new AccessControlException("User does not belong to " + group);
        }
      }
      dir.setOwner(src, username, group);
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(src, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
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
    LocatedBlocks blocks = getBlockLocations(src, offset, length, true, true);
    if (blocks != null) {
      blockManager.getDatanodeManager().sortLocatedBlocks(
          clientMachine, blocks.getLocatedBlocks());
    }
    return blocks;
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @throws FileNotFoundException, UnresolvedLinkException, IOException
   */
  LocatedBlocks getBlockLocations(String src, long offset, long length,
      boolean doAccessTime, boolean needBlockToken) throws FileNotFoundException,
      UnresolvedLinkException, IOException {
    FSPermissionChecker pc = getPermissionChecker();
    if (isPermissionEnabled) {
      checkPathAccess(pc, src, FsAction.READ);
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
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "open", src, null, null);
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

      // if the namenode is in safemode, then do not update access time
      if (isInSafeMode()) {
        doAccessTime = false;
      }

      try {
        long now = now();
        INodeFile inode = dir.getFileINode(src);
        if (inode == null) {
          throw new FileNotFoundException("File does not exist: " + src);
        }
        assert !inode.isLink();
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
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot concat " + target, safeMode);
      }
      concatInternal(pc, target, srcs);
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(target, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getLoginUser(),
                    Server.getRemoteIp(),
                    "concat", Arrays.toString(srcs), target, resultingStat);
    }
  }

  /** See {@link #concat(String, String[])} */
  private void concatInternal(FSPermissionChecker pc, String target, String [] srcs) 
      throws IOException, UnresolvedLinkException {
    assert hasWriteLock();

    // write permission for the target
    if (isPermissionEnabled) {
      checkPathAccess(pc, target, FsAction.WRITE);

      // and srcs
      for(String aSrc: srcs) {
        checkPathAccess(pc, aSrc, FsAction.READ); // read the file
        checkParentAccess(pc, aSrc, FsAction.WRITE); // for delete 
      }
    }

    // to make sure no two files are the same
    Set<INode> si = new HashSet<INode>();

    // we put the following prerequisite for the operation
    // replication and blocks sizes should be the same for ALL the blocks
    // check the target
    INode inode = dir.getFileINode(target);

    if(inode == null) {
      throw new IllegalArgumentException("concat: trg file doesn't exist");
    }
    if(inode.isUnderConstruction()) {
      throw new IllegalArgumentException("concat: trg file is uner construction");
    }

    INodeFile trgInode = (INodeFile) inode;

    // per design trg shouldn't be empty and all the blocks same size
    if(trgInode.blocks.length == 0) {
      throw new IllegalArgumentException("concat: "+ target + " file is empty");
    }

    long blockSize = trgInode.getPreferredBlockSize();

    // check the end block to be full
    if(blockSize != trgInode.blocks[trgInode.blocks.length-1].getNumBytes()) {
      throw new IllegalArgumentException(target + " blocks size should be the same");
    }

    si.add(trgInode);
    short repl = trgInode.getReplication();

    // now check the srcs
    boolean endSrc = false; // final src file doesn't have to have full end block
    for(int i=0; i<srcs.length; i++) {
      String src = srcs[i];
      if(i==srcs.length-1)
        endSrc=true;

      INodeFile srcInode = dir.getFileINode(src);

      if(src.isEmpty() 
          || srcInode == null
          || srcInode.isUnderConstruction()
          || srcInode.blocks.length == 0) {
        throw new IllegalArgumentException("concat: file " + src + 
        " is invalid or empty or underConstruction");
      }

      // check replication and blocks size
      if(repl != srcInode.getReplication()) {
        throw new IllegalArgumentException(src + " and " + target + " " +
            "should have same replication: "
            + repl + " vs. " + srcInode.getReplication());
      }

      //boolean endBlock=false;
      // verify that all the blocks are of the same length as target
      // should be enough to check the end blocks
      int idx = srcInode.blocks.length-1;
      if(endSrc)
        idx = srcInode.blocks.length-2; // end block of endSrc is OK not to be full
      if(idx >= 0 && srcInode.blocks[idx].getNumBytes() != blockSize) {
        throw new IllegalArgumentException("concat: blocks sizes of " + 
            src + " and " + target + " should all be the same");
      }

      si.add(srcInode);
    }

    // make sure no two files are the same
    if(si.size() < srcs.length+1) { // trg + srcs
      // it means at least two files are the same
      throw new IllegalArgumentException("at least two files are the same");
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
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. " +
                            " Please set " + DFS_NAMENODE_ACCESSTIME_PRECISION_KEY + " configuration parameter.");
    }
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      // Write access is required to set access and modification times
      if (isPermissionEnabled) {
        checkPathAccess(pc, src, FsAction.WRITE);
      }
      INode inode = dir.getINode(src);
      if (inode != null) {
        dir.setTimes(src, inode, mtime, atime, true);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          final HdfsFileStatus stat = dir.getFileInfo(src, false);
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                        Server.getRemoteIp(),
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
    HdfsFileStatus resultingStat = null;
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      if (!createParent) {
        verifyParentDir(link);
      }
      createSymlinkInternal(pc, target, link, dirPerms, createParent);
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(link, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "createSymlink", link, target, resultingStat);
    }
  }

  /**
   * Create a symbolic link.
   */
  private void createSymlinkInternal(FSPermissionChecker pc, String target,
      String link, PermissionStatus dirPerms, boolean createParent)
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
      checkAncestorAccess(pc, link, FsAction.WRITE);
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
  boolean setReplication(final String src, final short replication
      ) throws IOException {
    blockManager.verifyReplication(src, replication, null);
    final boolean isFile;
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set replication for " + src, safeMode);
      }
      if (isPermissionEnabled) {
        checkPathAccess(pc, src, FsAction.WRITE);
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
    if (isFile && auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setReplication", src, null, null);
    }
    return isFile;
  }
    
  long getPreferredBlockSize(String filename) 
      throws IOException, UnresolvedLinkException {
    FSPermissionChecker pc = getPermissionChecker();
    readLock();
    try {
      if (isPermissionEnabled) {
        checkTraverse(pc, filename);
      }
      return dir.getPreferredBlockSize(filename);
    } finally {
      readUnlock();
    }
  }

  /*
   * Verify that parent directory of src exists.
   */
  private void verifyParentDir(String src) throws FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException {
    assert hasReadOrWriteLock();
    Path parent = new Path(src).getParent();
    if (parent != null) {
      INode[] pathINodes = dir.getExistingPathINodes(parent.toString());
      INode parentNode = pathINodes[pathINodes.length - 1];
      if (parentNode == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent.toString());
      } else if (!parentNode.isDirectory() && !parentNode.isLink()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + parent.toString());
      }
    }
  }

  /**
   * Create a new file entry in the namespace.
   * 
   * For description of parameters and exceptions thrown see 
   * {@link ClientProtocol#create()}
   */
  void startFile(String src, PermissionStatus permissions, String holder,
      String clientMachine, EnumSet<CreateFlag> flag, boolean createParent,
      short replication, long blockSize) throws AccessControlException,
      SafeModeException, FileAlreadyExistsException, UnresolvedLinkException,
      FileNotFoundException, ParentNotDirectoryException, IOException {
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      startFileInternal(pc, src, permissions, holder, clientMachine, flag,
          createParent, replication, blockSize);
    } finally {
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      getEditLog().logSync();
    }
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "create", src, null, stat);
    }
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
  private LocatedBlock startFileInternal(FSPermissionChecker pc, String src,
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
        checkPathAccess(pc, src, FsAction.WRITE);
      } else {
        checkAncestorAccess(pc, src, FsAction.WRITE);
      }
    }

    if (!createParent) {
      verifyParentDir(src);
    }

    try {
      INodeFile myFile = dir.getFileINode(src);
      recoverLeaseInternal(myFile, src, holder, clientMachine, false);

      try {
        blockManager.verifyReplication(src, replication, clientMachine);
      } catch(IOException e) {
        throw new IOException("failed to create "+e.getMessage());
      }
      boolean create = flag.contains(CreateFlag.CREATE);
      if (myFile == null) {
        if (!create) {
          throw new FileNotFoundException("failed to overwrite or append to non-existent file "
            + src + " on client " + clientMachine);
        }
      } else {
        // File exists - must be one of append or overwrite
        if (overwrite) {
          delete(src, true);
        } else if (!append) {
          throw new FileAlreadyExistsException("failed to create file " + src
              + " on client " + clientMachine
              + " because the file exists");
        }
      }

      final DatanodeDescriptor clientNode = 
          blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);

      if (append && myFile != null) {
        return prepareFileForWrite(
            src, myFile, holder, clientMachine, clientNode, true);
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
                                    file.getLocalNameBytes(),
                                    file.getReplication(),
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
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot recover the lease of " + src, safeMode);
      }
      if (!DFSUtil.isValidName(src)) {
        throw new IOException("Invalid file name: " + src);
      }
  
      INode inode = dir.getFileINode(src);
      if (inode == null) {
        throw new FileNotFoundException("File not found " + src);
      }
  
      if (!inode.isUnderConstruction()) {
        return true;
      }
      if (isPermissionEnabled) {
        checkPathAccess(pc, src, FsAction.WRITE);
      }
  
      recoverLeaseInternal(inode, src, holder, clientMachine, true);
    } finally {
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      getEditLog().logSync();
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
        LOG.info("recoverLease: recover lease " + lease + ", src=" + src +
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
          LOG.info("startFile: recover lease " + lease + ", src=" + src +
              " from client " + pendingFile.getClientName());
          boolean isClosed = internalReleaseLease(lease, src, null);
          if(!isClosed)
            throw new RecoveryInProgressException(
                "Failed to close file " + src +
                ". Lease recovery is in progress. Try again later.");
        } else {
          BlockInfoUnderConstruction lastBlock=pendingFile.getLastBlock();
          if(lastBlock != null && lastBlock.getBlockUCState() ==
            BlockUCState.UNDER_RECOVERY) {
            throw new RecoveryInProgressException(
              "Recovery in progress, file [" + src + "], " +
              "lease owner [" + lease.getHolder() + "]");
            } else {
              throw new AlreadyBeingCreatedException(
                "Failed to create file [" + src + "] for [" + holder +
                "] on client [" + clientMachine +
                "], because this file is already being created by [" +
                pendingFile.getClientName() + "] on [" +
                pendingFile.getClientMachine() + "]");
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
    if (supportAppends == false) {
      throw new UnsupportedOperationException("Append to hdfs not supported." +
                            " Please refer to dfs.support.append configuration parameter.");
    }
    LocatedBlock lb = null;
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      lb = startFileInternal(pc, src, null, holder, clientMachine, 
                        EnumSet.of(CreateFlag.APPEND), 
                        false, blockManager.maxReplication, (long)0);
    } finally {
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      getEditLog().logSync();
    }
    if (lb != null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.appendFile: file "
            +src+" for "+holder+" at "+clientMachine
            +" block " + lb.getBlock()
            +" block size " + lb.getBlock().getNumBytes());
      }
    }
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "append", src, null, null);
    }
    return lb;
  }

  ExtendedBlock getExtendedBlock(Block blk) {
    return new ExtendedBlock(blockPoolId, blk);
  }
  
  void setBlockPoolId(String bpid) {
    blockPoolId = bpid;
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
  LocatedBlock getAdditionalBlock(String src,
                                         String clientName,
                                         ExtendedBlock previous,
                                         HashMap<Node, Node> excludedNodes
                                         ) 
      throws LeaseExpiredException, NotReplicatedYetException,
      QuotaExceededException, SafeModeException, UnresolvedLinkException,
      IOException {
    checkBlock(previous);
    long fileLength, blockSize;
    int replication;
    DatanodeDescriptor clientNode = null;
    Block newBlock = null;

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.getAdditionalBlock: file "
          +src+" for "+clientName);
    }

    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot add block to " + src, safeMode);
      }

      // have we exceeded the configured limit of fs objects.
      checkFsObjectLimit();

      INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);

      // commit the last block and complete it if it has minimum replicas
      commitOrCompleteLastBlock(pendingFile, ExtendedBlock.getLocalBlock(previous));

      //
      // If we fail this, bad things happen!
      //
      if (!checkFileProgress(pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet:" + src);
      }
      fileLength = pendingFile.computeContentSummary().getLength();
      blockSize = pendingFile.getPreferredBlockSize();
      clientNode = pendingFile.getClientNode();
      replication = (int)pendingFile.getReplication();
    } finally {
      writeUnlock();
    }

    // choose targets for the new block to be allocated.
    final DatanodeDescriptor targets[] = blockManager.chooseTarget(
        src, replication, clientNode, excludedNodes, blockSize);

    // Allocate a new block and record it in the INode. 
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot add block to " + src, safeMode);
      }
      INode[] pathINodes = dir.getExistingPathINodes(src);
      int inodesLen = pathINodes.length;
      checkLease(src, clientName, pathINodes[inodesLen-1]);
      INodeFileUnderConstruction pendingFile  = (INodeFileUnderConstruction) 
                                                pathINodes[inodesLen - 1];
                                                           
      if (!checkFileProgress(pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet:" + src);
      }

      // allocate new block record block locations in INode.
      newBlock = allocateBlock(src, pathINodes, targets);
      
      for (DatanodeDescriptor dn : targets) {
        dn.incBlocksScheduled();
      }      
    } finally {
      writeUnlock();
    }

    // Create next block
    LocatedBlock b = new LocatedBlock(getExtendedBlock(newBlock), targets, fileLength);
    blockManager.setBlockToken(b, BlockTokenSecretManager.AccessMode.WRITE);
    return b;
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
      return true;
    } finally {
      writeUnlock();
    }
  }
  
  // make sure that we still have the lease on this file.
  private INodeFileUnderConstruction checkLease(String src, String holder) 
      throws LeaseExpiredException, UnresolvedLinkException {
    assert hasReadOrWriteLock();
    INodeFile file = dir.getFileINode(src);
    checkLease(src, holder, file);
    return (INodeFileUnderConstruction)file;
  }

  private void checkLease(String src, String holder, INode file)
      throws LeaseExpiredException {
    assert hasReadOrWriteLock();
    if (file == null || file.isDirectory()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException("No lease on " + src +
                                      " File does not exist. " +
                                      (lease != null ? lease.toString() :
                                       "Holder " + holder + 
                                       " does not have any open files."));
    }
    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException("No lease on " + src + 
                                      " File is not open for writing. " +
                                      (lease != null ? lease.toString() :
                                       "Holder " + holder + 
                                       " does not have any open files."));
    }
    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)file;
    if (holder != null && !pendingFile.getClientName().equals(holder)) {
      throw new LeaseExpiredException("Lease mismatch on " + src + " owned by "
          + pendingFile.getClientName() + " but is accessed by " + holder);
    }
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

    INodeFileUnderConstruction pendingFile = checkLease(src, holder);
    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, last);

    if (!checkFileProgress(pendingFile, true)) {
      return false;
    }

    finalizeINodeFileUnderConstruction(src, pendingFile);

    NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " + src
                                  + " is closed by " + holder);
    return true;
  }

  /** 
   * Check all blocks of a file. If any blocks are lower than their intended
   * replication factor, then insert them into neededReplication and if 
   * the blocks are more than the intended replication factor then insert 
   * them into invalidateBlocks.
   */
  private void checkReplicationFactor(INodeFile file) {
    short numExpectedReplicas = file.getReplication();
    Block[] pendingBlocks = file.getBlocks();
    int nrBlocks = pendingBlocks.length;
    for (int i = 0; i < nrBlocks; i++) {
      blockManager.checkReplication(pendingBlocks[i], numExpectedReplicas);
    }
  }
    
  /**
   * Allocate a block at the given pending filename
   * 
   * @param src path to the file
   * @param inodes INode representing each of the components of src. 
   *        <code>inodes[inodes.length-1]</code> is the INode for the file.
   *        
   * @throws QuotaExceededException If addition of block exceeds space quota
   */
  private Block allocateBlock(String src, INode[] inodes,
      DatanodeDescriptor targets[]) throws QuotaExceededException {
    assert hasWriteLock();
    Block b = new Block(DFSUtil.getRandom().nextLong(), 0, 0); 
    while(isValidBlock(b)) {
      b.setBlockId(DFSUtil.getRandom().nextLong());
    }
    b.setGenerationStamp(getGenerationStamp());
    b = dir.addBlock(src, inodes, b, targets);
    NameNode.stateChangeLog.info("BLOCK* NameSystem.allocateBlock: "
                                 +src+ ". " + blockPoolId + " "+ b);
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
            LOG.info("BLOCK* NameSystem.checkFileProgress: "
                + "block " + block + " has not reached minimal replication "
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
          LOG.info("BLOCK* NameSystem.checkFileProgress: "
              + "block " + b + " has not reached minimal replication "
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
    boolean status = false;
    HdfsFileStatus resultingStat = null;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src +
          " to " + dst);
    }
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      status = renameToInternal(pc, src, dst);
      if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(dst, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "rename", src, dst, resultingStat);
    }
    return status;
  }

  /** @deprecated See {@link #renameTo(String, String)} */
  @Deprecated
  private boolean renameToInternal(FSPermissionChecker pc, String src, String dst)
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
      checkParentAccess(pc, src, FsAction.WRITE);
      checkAncestorAccess(pc, actualdst, FsAction.WRITE);
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
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      renameToInternal(pc, src, dst, options);
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(dst, false); 
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      StringBuilder cmd = new StringBuilder("rename options=");
      for (Rename option : options) {
        cmd.append(option.value()).append(" ");
      }
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(),
                    cmd.toString(), src, dst, resultingStat);
    }
  }

  private void renameToInternal(FSPermissionChecker pc, String src, String dst,
      Options.Rename... options) throws IOException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new InvalidPathException("Invalid name: " + dst);
    }
    if (isPermissionEnabled) {
      checkParentAccess(pc, src, FsAction.WRITE);
      checkAncestorAccess(pc, dst, FsAction.WRITE);
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
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
      }
      boolean status = deleteInternal(src, recursive, true);
      if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
        logAuditEvent(UserGroupInformation.getCurrentUser(),
                      Server.getRemoteIp(),
                      "delete", src, null, null);
      }
      return status;
    }
    
  private FSPermissionChecker getPermissionChecker()
      throws AccessControlException {
    return new FSPermissionChecker(fsOwnerShortUserName, supergroup);
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
    ArrayList<Block> collectedBlocks = new ArrayList<Block>();
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot delete " + src, safeMode);
      }
      if (!recursive && !dir.isDirEmpty(src)) {
        throw new IOException(src + " is non empty");
      }
      if (enforcePermission && isPermissionEnabled) {
        checkPermission(pc, src, false, null, FsAction.WRITE, null, FsAction.ALL);
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
   */
  private void removeBlocks(List<Block> blocks) {
    int start = 0;
    int end = 0;
    while (start < blocks.size()) {
      end = BLOCK_DELETION_INCREMENT + start;
      end = end > blocks.size() ? blocks.size() : end;
      writeLock();
      try {
        for (int i = start; i < end; i++) {
          blockManager.removeBlock(blocks.get(i));
        }
      } finally {
        writeUnlock();
      }
      start = end;
    }
  }
  
  void removePathAndBlocks(String src, List<Block> blocks) {
    assert hasWriteLock();
    leaseManager.removeLeaseWithPrefixPath(src);
    if (blocks == null) {
      return;
    }
    for(Block b : blocks) {
      blockManager.removeBlock(b);
    }
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
   */
  HdfsFileStatus getFileInfo(String src, boolean resolveLink) 
    throws AccessControlException, UnresolvedLinkException {
    FSPermissionChecker pc = getPermissionChecker();
    readLock();
    try {
      if (!DFSUtil.isValidName(src)) {
        throw new InvalidPathException("Invalid file name: " + src);
      }
      if (isPermissionEnabled) {
        checkTraverse(pc, src);
      }
      return dir.getFileInfo(src, resolveLink);
    } finally {
      readUnlock();
    }
  }

  /**
   * Create all the necessary directories
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean createParent) throws IOException, UnresolvedLinkException {
    boolean status = false;
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    }
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      status = mkdirsInternal(pc, src, permissions, createParent);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "mkdirs", src, null, stat);
    }
    return status;
  }
    
  /**
   * Create all the necessary directories
   */
  private boolean mkdirsInternal(FSPermissionChecker pc, String src,
      PermissionStatus permissions, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create directory " + src, safeMode);
    }
    if (isPermissionEnabled) {
      checkTraverse(pc, src);
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
      checkAncestorAccess(pc, src, FsAction.WRITE);
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
      FileNotFoundException, UnresolvedLinkException {
    FSPermissionChecker pc = new FSPermissionChecker(fsOwnerShortUserName,
        supergroup);
    readLock();
    try {
      if (isPermissionEnabled) {
        checkPermission(pc, src, false, null, null, null, FsAction.READ_EXECUTE);
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
    checkSuperuserPrivilege();
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set quota on " + path, safeMode);
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
   * @throws IOException if path does not exist
   */
  void fsync(String src, String clientName) 
      throws IOException, UnresolvedLinkException {
    NameNode.stateChangeLog.info("BLOCK* NameSystem.fsync: file "
                                  + src + " for " + clientName);
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot fsync file " + src, safeMode);
      }
      INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);
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
    LOG.info("Recovering lease=" + lease + ", src=" + src);
    assert !isInSafeMode();
    assert hasWriteLock();
    INodeFile iFile = dir.getFileINode(src);
    if (iFile == null) {
      final String message = "DIR* NameSystem.internalReleaseLease: "
        + "attempt to release a create lock on "
        + src + " file does not exist.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
    if (!iFile.isUnderConstruction()) {
      final String message = "DIR* NameSystem.internalReleaseLease: "
        + "attempt to release a create lock on "
        + src + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) iFile;
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
         curBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      final String message = "DIR* NameSystem.internalReleaseLease: "
        + "attempt to release a create lock on "
        + src + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    // no we know that the last block is not COMPLETE, and
    // that the penultimate block if exists is either COMPLETE or COMMITTED
    BlockInfoUnderConstruction lastBlock = pendingFile.getLastBlock();
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
      // setup the last block locations from the blockManager if not known
      if(lastBlock.getNumExpectedLocations() == 0)
        lastBlock.setExpectedLocations(blockManager.getNodes(lastBlock));
      // start recovery of the last block for this file
      long blockRecoveryId = nextGenerationStamp();
      lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);
      lastBlock.initializeBlockRecovery(blockRecoveryId);
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
      INodeFileUnderConstruction pendingFile) throws IOException {
    assert hasWriteLock();
    if(newHolder == null)
      return lease;
    // The following transaction is not synced. Make sure it's sync'ed later.
    logReassignLease(lease.getHolder(), src, newHolder);
    return reassignLeaseInternal(lease, src, newHolder, pendingFile);
  }
  
  Lease reassignLeaseInternal(Lease lease, String src, String newHolder,
      INodeFileUnderConstruction pendingFile) throws IOException {
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
        dir.updateSpaceConsumed(path, 0, -diff * fileINode.getReplication());
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

    checkReplicationFactor(newFile);
  }

  void commitBlockSynchronization(ExtendedBlock lastblock,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets)
      throws IOException, UnresolvedLinkException {
    String src = "";
    writeLock();
    try {
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
      INodeFile iFile = storedBlock.getINode();
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
        if (closeFile) {
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
      } else if (supportAppends) {
        // If this commit does not want to close the file, persist
        // blocks only if append is supported 
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
    DirectoryListing dl;
    FSPermissionChecker pc = getPermissionChecker();
    readLock();
    try {
      if (isPermissionEnabled) {
        if (dir.isDir(src)) {
          checkPathAccess(pc, src, FsAction.READ_EXECUTE);
        } else {
          checkTraverse(pc, src);
        }
      }
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        logAuditEvent(UserGroupInformation.getCurrentUser(),
                      Server.getRemoteIp(),
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
  DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
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
      if (cmds != null) {
        return cmds;
      }

      //check distributed upgrade
      DatanodeCommand cmd = upgradeManager.getBroadcastCommand();
      if (cmd != null) {
        return new DatanodeCommand[] {cmd};
      }
      return null;
    } finally {
      readUnlock();
    }
  }

  /**
   * Returns whether or not there were available resources at the last check of
   * resources.
   *
   * @return true if there were sufficient resources available, false otherwise.
   */
  private boolean nameNodeHasResourcesAvailable() {
    return hasResourcesAvailable;
  }

  /**
   * Perform resource checks and cache the results.
   * @throws IOException
   */
  private void checkAvailableResources() throws IOException {
    hasResourcesAvailable = nnResourceChecker.hasAvailableDiskSpace();
  }

  /**
   * Periodically calls hasAvailableResources of NameNodeResourceChecker, and if
   * there are found to be insufficient resources available, causes the NN to
   * enter safe mode. If resources are later found to have returned to
   * acceptable levels, this daemon will cause the NN to exit safe mode.
   */
  class NameNodeResourceMonitor implements Runnable  {
    @Override
    public void run () {
      try {
        while (fsRunning) {
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
  }
  
  FSImage getFSImage() {
    return dir.fsImage;
  }

  FSEditLog getEditLog() {
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
    return (getEditLog().getLastWrittenTxId() -
        getEditLog().getCurSegmentTxId()) + 1;
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
    checkSuperuserPrivilege();
    readLock();
    try {
      if (!isInSafeMode()) {
        throw new IOException("Safe mode should be turned ON " +
                              "in order to create namespace image.");
      }
      getFSImage().saveNamespace();
      LOG.info("New namespace image has been created.");
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
    checkSuperuserPrivilege();
    writeLock();
    try {
      
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
    return new Date(systemStart); 
  }
    
  void finalizeUpgrade() throws IOException {
    checkSuperuserPrivilege();
    writeLock();
    try {
      getFSImage().finalizeUpgrade();
    } finally {
      writeUnlock();
    }
  }

  void refreshNodes() throws IOException {
    checkSuperuserPrivilege();
    getBlockManager().getDatanodeManager().refreshNodes(new HdfsConfiguration());
  }

  void setBalancerBandwidth(long bandwidth) throws IOException {
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
      // default to safe mode threshold (i.e., don't populate queues before leaving safe mode)
      this.replQueueThreshold = 
        conf.getFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
                      (float) threshold);
      this.blockTotal = 0; 
      this.blockSafe = 0;
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
      try {
        assert isConsistent() : " SafeMode: Inconsistent filesystem state: "
          + "Total num of blocks, active blocks, or "
          + "total safe blocks don't match.";
      } catch(IOException e) {
        System.err.print(StringUtils.stringifyException(e));
      }
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
     * Switch to manual safe mode if distributed upgrade is required.<br>
     * Check for invalid, under- & over-replicated blocks in the end of startup.
     */
    private synchronized void leave(boolean checkForUpgrades) {
      if(checkForUpgrades) {
        // verify whether a distributed upgrade needs to be started
        boolean needUpgrade = false;
        try {
          needUpgrade = upgradeManager.startUpgrade();
        } catch(IOException e) {
          FSNamesystem.LOG.error("IOException in startDistributedUpgradeIfNeeded", e);
        }
        if(needUpgrade) {
          // switch to manual safe mode
          safeMode = new SafeModeInfo(false);
          return;
        }
      }
      // if not done yet, initialize replication queues
      if (!isPopulatingReplQueues()) {
        initializeReplQueues();
      }
      long timeInSafemode = now() - systemStart;
      NameNode.stateChangeLog.info("STATE* Leaving safe mode after " 
                                    + timeInSafemode/1000 + " secs.");
      NameNode.getNameNodeMetrics().setSafeModeTime((int) timeInSafemode);
      
      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF."); 
      }
      reached = -1;
      safeMode = null;
      final NetworkTopology nt = blockManager.getDatanodeManager().getNetworkTopology();
      NameNode.stateChangeLog.info("STATE* Network topology has "
          + nt.getNumOfRacks() + " racks and "
          + nt.getNumOfLeaves() + " datanodes");
      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has "
          + blockManager.numOfUnderReplicatedBlocks() + " blocks");
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
      return blockSafe >= blockReplQueueThreshold;
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
        this.leave(true); // leave safe mode
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
        (int) (((double) blockTotal) * replQueueThreshold);
      checkMode();
    }
      
    /**
     * Increment number of safe blocks if current block has 
     * reached minimal replication.
     * @param replication current replication 
     */
    private synchronized void incrementSafeBlockCount(short replication) {
      if ((int)replication == safeReplication)
        this.blockSafe++;
      checkMode();
    }
      
    /**
     * Decrement number of safe blocks if current block has 
     * fallen below minimal replication.
     * @param replication current replication 
     */
    private synchronized void decrementSafeBlockCount(short replication) {
      if (replication == safeReplication-1)
        this.blockSafe--;
      checkMode();
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
        leaveMsg = "Resources are low on NN. Safe mode must be turned off manually";
      } else {
        leaveMsg = "Safe mode will be turned off automatically";
      }
      if(isManual()) {
        if(upgradeManager.getUpgradeState())
          return leaveMsg + " upon completion of " + 
            "the distributed upgrade: upgrade progress = " + 
            upgradeManager.getUpgradeStatus() + "%";
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
     * This is costly and currently called only in assert.
     */
    private boolean isConsistent() throws IOException {
      if (blockTotal == -1 && blockSafe == -1) {
        return true; // manual safe mode
      }
      int activeBlocks = blockManager.getActiveBlockCount();
      return (blockTotal == activeBlocks) ||
        (blockSafe >= 0 && blockSafe <= blockTotal);
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
    public void run() {
      while (fsRunning && (safeMode != null && !safeMode.canLeave())) {
        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
        }
      }
      if (!fsRunning) {
        LOG.info("NameNode is being shutdown, exit SafeModeMonitor thread. ");
      } else {
        // leave safe mode and stop the monitor
        try {
          leaveSafeMode(true);
        } catch(SafeModeException es) { // should never happen
          String msg = "SafeModeMonitor may not run during distributed upgrade.";
          assert false : msg;
          throw new RuntimeException(msg, es);
        }
      }
      smmthread = null;
    }
  }
    
  boolean setSafeMode(SafeModeAction action) throws IOException {
    if (action != SafeModeAction.SAFEMODE_GET) {
      checkSuperuserPrivilege();
      switch(action) {
      case SAFEMODE_LEAVE: // leave safe mode
        leaveSafeMode(false);
        break;
      case SAFEMODE_ENTER: // enter safe mode
        enterSafeMode(false);
        break;
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
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return true;
    return safeMode.isPopulatingReplQueues();
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
    safeMode.decrementSafeBlockCount((short)blockManager.countNodes(b).liveReplicas());
  }

  /**
   * Set the total number of blocks in the system. 
   */
  private void setBlockTotal() {
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
          INode node;
          try {
            node = dir.getFileINode(path);
          } catch (UnresolvedLinkException e) {
            throw new AssertionError("Lease files should reside on this FS");
          }
          assert node != null : "Found a lease for nonexisting file.";
          assert node.isUnderConstruction() :
            "Found a lease for file that is not under construction.";
          INodeFileUnderConstruction cons = (INodeFileUnderConstruction) node;
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
    // Ensure that any concurrent operations have been fully synced
    // before entering safe mode. This ensures that the FSImage
    // is entirely stable on disk as soon as we're in safe mode.
    getEditLog().logSyncAll();
    if (!isInSafeMode()) {
      safeMode = new SafeModeInfo(resourcesLow);
      return;
    }
    if (resourcesLow) {
      safeMode.setResourcesLow();
    }
    safeMode.setManual();
    getEditLog().logSyncAll();
    NameNode.stateChangeLog.info("STATE* Safe mode is ON. " 
                                + safeMode.getTurnOffTip());
    } finally {
      writeUnlock();
    }
  }

  /**
   * Leave safe mode.
   * @throws IOException
   */
  void leaveSafeMode(boolean checkForUpgrades) throws SafeModeException {
    writeLock();
    try {
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("STATE* Safe mode is already OFF."); 
        return;
      }
      if(upgradeManager.getUpgradeState())
        throw new SafeModeException("Distributed upgrade is in progress",
                                    safeMode);
      safeMode.leave(checkForUpgrades);
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
    checkSuperuserPrivilege();
    writeLock();
    try {
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
      if (isInSafeMode()) {
        throw new SafeModeException("Checkpoint not ended", safeMode);
      }
      LOG.info("End checkpoint for " + registration.getAddress());
      getFSImage().endCheckpoint(sig);
    } finally {
      readUnlock();
    }
  }

  /**
   * Returns whether the given block is one pointed-to by a file.
   */
  private boolean isValidBlock(Block b) {
    return (blockManager.getINode(b) != null);
  }

  // Distributed upgrade manager
  final UpgradeManagerNamenode upgradeManager = new UpgradeManagerNamenode(this);

  UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action 
                                                 ) throws IOException {
    return upgradeManager.distributedUpgradeProgress(action);
  }

  UpgradeCommand processDistributedUpgradeCommand(UpgradeCommand comm) throws IOException {
    return upgradeManager.processUpgradeCommand(comm);
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), supergroup, permission);
  }

  private void checkOwner(FSPermissionChecker pc, String path)
      throws AccessControlException, UnresolvedLinkException {
    checkPermission(pc, path, true, null, null, null, null);
  }

  private void checkPathAccess(FSPermissionChecker pc,
      String path, FsAction access) throws AccessControlException,
      UnresolvedLinkException {
    checkPermission(pc, path, false, null, null, access, null);
  }

  private void checkParentAccess(FSPermissionChecker pc,
      String path, FsAction access) throws AccessControlException,
      UnresolvedLinkException {
    checkPermission(pc, path, false, null, access, null, null);
  }

  private void checkAncestorAccess(FSPermissionChecker pc,
      String path, FsAction access) throws AccessControlException,
      UnresolvedLinkException {
    checkPermission(pc, path, false, access, null, null, null);
  }

  private void checkTraverse(FSPermissionChecker pc, String path)
      throws AccessControlException, UnresolvedLinkException {
    checkPermission(pc, path, false, null, null, null, null);
  }

  @Override
  public void checkSuperuserPrivilege()
      throws AccessControlException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getPermissionChecker();
      pc.checkSuperuserPrivilege();
    }
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission()}.
   */
  private void checkPermission(FSPermissionChecker pc,
      String path, boolean doCheckOwner, FsAction ancestorAccess,
      FsAction parentAccess, FsAction access, FsAction subAccess)
      throws AccessControlException, UnresolvedLinkException {
    if (!pc.isSuperUser()) {
      dir.waitForReady();
      readLock();
      try {
        pc.checkPermission(path, dir.rootDir, doCheckOwner, ancestorAccess,
            parentAccess, access, subAccess);
      } finally {
        readUnlock();
      }
    }
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

  /**
   * Sets the generation stamp for this filesystem
   */
  void setGenerationStamp(long stamp) {
    generationStamp.setStamp(stamp);
  }

  /**
   * Gets the generation stamp for this filesystem
   */
  long getGenerationStamp() {
    return generationStamp.getStamp();
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
    long gs = generationStamp.nextStamp();
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
    INodeFile file = storedBlock.getINode();
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
    if (supportAppends) {
      getEditLog().logSync();
    }
    LOG.info("updatePipeline(" + oldBlock + ") successfully to " + newBlock);
  }

  /** @see updatePipeline(String, ExtendedBlock, ExtendedBlock, DatanodeID[]) */
  private void updatePipelineInternal(String clientName, ExtendedBlock oldBlock, 
      ExtendedBlock newBlock, DatanodeID[] newNodes)
      throws IOException {
    assert hasWriteLock();
    // check the vadility of the block and lease holder name
    final INodeFileUnderConstruction pendingFile = 
      checkUCBlock(oldBlock, clientName);
    final BlockInfoUnderConstruction blockinfo = pendingFile.getLastBlock();

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

    // persist blocks only if append is supported
    String src = leaseManager.findPath(pendingFile);
    if (supportAppends) {
      dir.persistBlocks(src, pendingFile);
    }
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  void unprotectedChangeLease(String src, String dst) {
    assert hasWriteLock();
    leaseManager.changeLease(src, dst);
  }
           
  private boolean isRoot(Path path) {
    return path.getParent() == null;
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
    checkSuperuserPrivilege();
    readLock();
    try {
      if (!isPopulatingReplQueues()) {
        throw new IOException("Cannot run listCorruptFileBlocks because " +
                              "replication queues have not been initialized.");
      }
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
        INode inode = blockManager.getINode(blk);
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
  public void logUpdateMasterKey(DelegationKey key) throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
          "Cannot log master key update in safe mode", safeMode);
      }
      getEditLog().logUpdateMasterKey(key);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }
  
  private void logReassignLease(String leaseHolder, String src,
      String newHolder) throws IOException {
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
    return Server.isRpcInvocation();
  }
  
  /**
   * Log fsck event in the audit log 
   */
  void logFsckEvent(String src, InetAddress remoteAddress) throws IOException {
    if (auditLog.isInfoEnabled()) {
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
    return (System.currentTimeMillis() - alivenode.getLastUpdate())/1000;
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

  @VisibleForTesting
  public SafeModeInfo getSafeModeInfoForTests() {
    return safeMode;
  }
}
