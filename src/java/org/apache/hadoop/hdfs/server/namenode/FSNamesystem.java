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

import org.apache.commons.logging.*;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.Util;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMetrics;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.util.*;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.UnderReplicatedBlocks.BlockIterator;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.mortbay.util.ajax.JSON;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.MBeanServer;

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
public class FSNamesystem implements FSConstants, FSNamesystemMBean, FSClusterStats,
    NameNodeMXBean {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);

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
  private String supergroup;
  private PermissionStatus defaultPermission;
  // FSNamesystemMetrics counter variables
  private FSNamesystemMetrics myFSMetrics;
  private long capacityTotal = 0L, capacityUsed = 0L, capacityRemaining = 0L;
  private long blockPoolUsed = 0L;
  private int totalLoad = 0;
  boolean isBlockTokenEnabled;
  BlockTokenSecretManager blockTokenSecretManager;
  private long blockKeyUpdateInterval;
  private long blockTokenLifetime;
  
  // Scan interval is not configurable.
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
    TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  private DelegationTokenSecretManager dtSecretManager;

  //
  // Stores the correct file name hierarchy
  //
  public FSDirectory dir;
  BlockManager blockManager;
  
  // Block pool ID used by this namenode
  String blockPoolId;
    
  /**
   * Stores the datanode -> block map.  
   * <p>
   * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by 
   * storage id. In order to keep the storage map consistent it tracks 
   * all storages ever registered with the namenode.
   * A descriptor corresponding to a specific storage id can be
   * <ul> 
   * <li>added to the map if it is a new storage id;</li>
   * <li>updated with a new datanode started as a replacement for the old one 
   * with the same storage id; and </li>
   * <li>removed if and only if an existing datanode is restarted to serve a
   * different storage id.</li>
   * </ul> <br>
   * The list of the {@link DatanodeDescriptor}s in the map is checkpointed
   * in the namespace image file. Only the {@link DatanodeInfo} part is 
   * persistent, the list of blocks is restored from the datanode block
   * reports. 
   * <p>
   * Mapping: StorageID -> DatanodeDescriptor
   */
  NavigableMap<String, DatanodeDescriptor> datanodeMap = 
    new TreeMap<String, DatanodeDescriptor>();

  Random r = new Random();

  /**
   * Stores a set of DatanodeDescriptor objects.
   * This is a subset of {@link #datanodeMap}, containing nodes that are 
   * considered alive.
   * The {@link HeartbeatMonitor} periodically checks for outdated entries,
   * and removes them from the list.
   */
  ArrayList<DatanodeDescriptor> heartbeats = new ArrayList<DatanodeDescriptor>();

  public LeaseManager leaseManager = new LeaseManager(this); 

  //
  // Threaded object that checks to see if we have been
  // getting heartbeats from all clients. 
  //
  Daemon hbthread = null;   // HeartbeatMonitor thread
  public Daemon lmthread = null;   // LeaseMonitor thread
  Daemon smmthread = null;  // SafeModeMonitor thread
  public Daemon replthread = null;  // Replication thread
  
  private volatile boolean fsRunning = true;
  long systemStart = 0;

  // heartbeatRecheckInterval is how often namenode checks for expired datanodes
  private long heartbeatRecheckInterval;
  // heartbeatExpireInterval is how long namenode waits for datanode to report
  // heartbeat
  private long heartbeatExpireInterval;
  //replicationRecheckInterval is how often namenode checks for new replication work
  private long replicationRecheckInterval;
  private FsServerDefaults serverDefaults;
  // allow appending to hdfs files
  private boolean supportAppends = true;

  private volatile SafeModeInfo safeMode;  // safe mode information
  private Host2NodesMap host2DataNodeMap = new Host2NodesMap();
    
  // datanode networktoplogy
  NetworkTopology clusterMap = new NetworkTopology();
  private DNSToSwitchMapping dnsToSwitchMapping;

  private HostsFileReader hostsReader; 
  private Daemon dnthread = null;

  private long maxFsObjects = 0;          // maximum number of fs objects

  /**
   * The global generation stamp for this file system. 
   */
  private final GenerationStamp generationStamp = new GenerationStamp();

  // Ask Datanode only up to this many blocks to delete.
  int blockInvalidateLimit = DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT;

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
    this.systemStart = now();
    this.blockManager = new BlockManager(this, conf);
    this.fsLock = new ReentrantReadWriteLock(true); // fair locking
    setConfigurationParameters(conf);
    dtSecretManager = createDelegationTokenSecretManager(conf);
    this.registerMBean(conf); // register the MBean for the FSNamesystemStutus
    if(fsImage == null) {
      this.dir = new FSDirectory(this, conf);
      StartupOption startOpt = NameNode.getStartupOption(conf);
      this.dir.loadFSImage(getNamespaceDirs(conf),
                           getNamespaceEditsDirs(conf), startOpt);
      long timeTakenToLoadFSImage = now() - systemStart;
      LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
      NameNode.getNameNodeMetrics().fsImageLoadTime.set(
                                (int) timeTakenToLoadFSImage);
    } else {
      this.dir = new FSDirectory(fsImage, this, conf);
    }
    this.safeMode = new SafeModeInfo(conf);
    this.hostsReader = new HostsFileReader(
      conf.get(DFSConfigKeys.DFS_HOSTS,""),
      conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE,""));
    if (isBlockTokenEnabled) {
      blockTokenSecretManager = new BlockTokenSecretManager(true,
          blockKeyUpdateInterval, blockTokenLifetime);
    }
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
    setBlockTotal();
    blockManager.activate();
    this.hbthread = new Daemon(new HeartbeatMonitor());
    this.lmthread = new Daemon(leaseManager.new Monitor());
    this.replthread = new Daemon(new ReplicationMonitor());
    hbthread.start();
    lmthread.start();
    replthread.start();

    this.dnthread = new Daemon(new DecommissionManager(this).new Monitor(
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 
                    DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_KEY, 
                    DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_DEFAULT)));
    dnthread.start();

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, 
                      ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);
    
    /* If the dns to swith mapping supports cache, resolve network 
     * locations of those hosts in the include list, 
     * and store the mapping in the cache; so future calls to resolve
     * will be fast.
     */
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      dnsToSwitchMapping.resolve(new ArrayList<String>(hostsReader.getHosts()));
    }
    registerMXBean();
  }

  public static Collection<URI> getNamespaceDirs(Configuration conf) {
    return getStorageDirs(conf, DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
  }

  public static Collection<URI> getStorageDirs(Configuration conf,
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
    } else if (dirNames.isEmpty())
      dirNames.add("file:///tmp/hadoop/dfs/name");
    return Util.stringCollectionAsURIs(dirNames);
  }

  public static Collection<URI> getNamespaceEditsDirs(Configuration conf) {
    return getStorageDirs(conf, DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY);
  }

  // utility methods to acquire and release read lock and write lock
  void readLock() {
    this.fsLock.readLock().lock();
  }

  void readUnlock() {
    this.fsLock.readLock().unlock();
  }

  void writeLock() {
    this.fsLock.writeLock().lock();
  }

  void writeUnlock() {
    this.fsLock.writeLock().unlock();
  }

  boolean hasWriteLock() {
    return this.fsLock.isWriteLockedByCurrentThread();
  }

  /**
   * dirs is a list of directories where the filesystem directory state 
   * is stored
   */
  FSNamesystem(FSImage fsImage, Configuration conf) throws IOException {
    this.fsLock = new ReentrantReadWriteLock(true);
    this.blockManager = new BlockManager(this, conf);
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
    
    LOG.info("fsOwner=" + fsOwner);

    this.supergroup = conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY, 
                               DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.isPermissionEnabled = conf.getBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
                                               DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT);
    LOG.info("supergroup=" + supergroup);
    LOG.info("isPermissionEnabled=" + isPermissionEnabled);
    short filePermission = (short)conf.getInt(DFSConfigKeys.DFS_NAMENODE_UPGRADE_PERMISSION_KEY,
                                              DFSConfigKeys.DFS_NAMENODE_UPGRADE_PERMISSION_DEFAULT);
    this.defaultPermission = PermissionStatus.createImmutable(
        fsOwner.getShortUserName(), supergroup, new FsPermission(filePermission));

    long heartbeatInterval = conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000;
    this.heartbeatRecheckInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 minutes
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval +
      10 * heartbeatInterval;
    this.replicationRecheckInterval = 
      conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 
                  DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000L;
    this.serverDefaults = new FsServerDefaults(
        conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE),
        conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BYTES_PER_CHECKSUM),
        conf.getInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DEFAULT_WRITE_PACKET_SIZE),
        (short) conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, DEFAULT_REPLICATION_FACTOR),
        conf.getInt("io.file.buffer.size", DEFAULT_FILE_BUFFER_SIZE));
    this.maxFsObjects = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY, 
                                     DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

    //default limit
    this.blockInvalidateLimit = Math.max(this.blockInvalidateLimit, 
                                         20*(int)(heartbeatInterval/1000));
    //use conf value if it is set.
    this.blockInvalidateLimit = conf.getInt(
        DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY, this.blockInvalidateLimit);
    LOG.info(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY + "=" + this.blockInvalidateLimit);

    this.accessTimePrecision = conf.getLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 0);
    this.supportAppends = conf.getBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY,
                                      DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT);
    this.isBlockTokenEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    if (isBlockTokenEnabled) {
      this.blockKeyUpdateInterval = conf.getLong(
          DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 
          DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT) * 60 * 1000L; // 10 hrs
      this.blockTokenLifetime = conf.getLong(
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT) * 60 * 1000L; // 10 hrs
    }
    LOG.info("isBlockTokenEnabled=" + isBlockTokenEnabled
        + " blockKeyUpdateInterval=" + blockKeyUpdateInterval / (60 * 1000)
        + " min(s), blockTokenLifetime=" + blockTokenLifetime / (60 * 1000)
        + " min(s)");
  }

  /**
   * Return the default path permission when upgrading from releases with no
   * permissions (<=0.15) to releases with permissions (>=0.16)
   */
  protected PermissionStatus getUpgradePermission() {
    return defaultPermission;
  }
  
  NamespaceInfo getNamespaceInfo() {
    return new NamespaceInfo(dir.fsImage.getStorage().getNamespaceID(),
                             getClusterId(),
                             getBlockPoolId(),
                             dir.fsImage.getStorage().getCTime(),
                             getDistributedUpgradeVersion());
  }

  /**
   * Close down this file system manager.
   * Causes heartbeat and lease daemons to stop; waits briefly for
   * them to finish, but a short timeout returns control back to caller.
   */
  public void close() {
    fsRunning = false;
    try {
      if (blockManager != null) blockManager.close();
      if (hbthread != null) hbthread.interrupt();
      if (replthread != null) replthread.interrupt();
      if (dnthread != null) dnthread.interrupt();
      if (smmthread != null) smmthread.interrupt();
      if (dtSecretManager != null) dtSecretManager.stopThreads();
    } catch (Exception e) {
      LOG.warn("Exception shutting down FSNamesystem", e);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        if (lmthread != null) {
          lmthread.interrupt();
          lmthread.join(3000);
        }
        dir.close();
      } catch (InterruptedException ie) {
      } catch (IOException ie) {
        LOG.error("Error closing FSDirectory", ie);
        IOUtils.cleanup(LOG, dir);
      }
    }
  }

  /** Is this name system running? */
  boolean isRunning() {
    return fsRunning;
  }

  /**
   * Dump all metadata into specified file
   */
  void metaSave(String filename) throws IOException {
    writeLock();
    try {
    checkSuperuserPrivilege();
    File file = new File(System.getProperty("hadoop.log.dir"), filename);
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file,
        true)));

    long totalInodes = this.dir.totalInodes();
    long totalBlocks = this.getBlocksTotal();

    ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    this.DFSNodesStatus(live, dead);
    
    String str = totalInodes + " files and directories, " + totalBlocks
        + " blocks = " + (totalInodes + totalBlocks) + " total";
    out.println(str);
    out.println("Live Datanodes: "+live.size());
    out.println("Dead Datanodes: "+dead.size());
    blockManager.metaSave(out);

    //
    // Dump all datanodes
    //
    datanodeDump(out);

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
  // These methods are called by secondary namenodes
  //
  /////////////////////////////////////////////////////////
  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   * 
   * @param datanode on which blocks are located
   * @param size total size of blocks
   */
  BlocksWithLocations getBlocks(DatanodeID datanode, long size)
      throws IOException {
    readLock();
    try {
    checkSuperuserPrivilege();

    DatanodeDescriptor node = getDatanode(datanode);
    if (node == null) {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.getBlocks: "
          + "Asking for blocks from an unrecorded node " + datanode.getName());
      throw new IllegalArgumentException(
          "Unexpected exception.  Got getBlocks message for datanode " +
          datanode.getName() + ", but there is no info for it");
    }

    int numBlocks = node.numBlocks();
    if(numBlocks == 0) {
      return new BlocksWithLocations(new BlockWithLocations[0]);
    }
    Iterator<BlockInfo> iter = node.getBlockIterator();
    int startBlock = r.nextInt(numBlocks); // starting from a random block
    // skip blocks
    for(int i=0; i<startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    long totalSize = 0;
    BlockInfo curBlock;
    while(totalSize<size && iter.hasNext()) {
      curBlock = iter.next();
      if(!curBlock.isComplete())  continue;
      totalSize += addBlock(curBlock, results);
    }
    if(totalSize<size) {
      iter = node.getBlockIterator(); // start from the beginning
      for(int i=0; i<startBlock&&totalSize<size; i++) {
        curBlock = iter.next();
        if(!curBlock.isComplete())  continue;
        totalSize += addBlock(curBlock, results);
      }
    }

    return new BlocksWithLocations(
        results.toArray(new BlockWithLocations[results.size()]));
    } finally {
      readUnlock();
    }
  }

  /**
   * Get access keys
   * 
   * @return current access keys
   */
  ExportedBlockKeys getBlockKeys() {
    return isBlockTokenEnabled ? blockTokenSecretManager.exportKeys()
        : ExportedBlockKeys.DUMMY_KEYS;
  }

  /**
   * Get all valid locations of the block & add the block to results
   * return the length of the added block; 0 if the block is not added
   */
  private long addBlock(Block block, List<BlockWithLocations> results) {
    ArrayList<String> machineSet = blockManager.getValidLocations(block);
    if(machineSet.size() == 0) {
      return 0;
    } else {
      results.add(new BlockWithLocations(block, 
          machineSet.toArray(new String[machineSet.size()])));
      return block.getNumBytes();
    }
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
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    writeLock();
    try {
    if (isInSafeMode())
      throw new SafeModeException("Cannot set permission for " + src, safeMode);
    checkOwner(src);
    dir.setPermission(src, permission);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setPermission", src, null, stat);
    }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Set owner for an existing file.
   * @throws IOException
   */
  public void setOwner(String src, String username, String group)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    writeLock();
    try {
    if (isInSafeMode())
        throw new SafeModeException("Cannot set owner for " + src, safeMode);
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
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setOwner", src, null, stat);
    }
    } finally {
      writeUnlock();
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
      //sort the blocks
      DatanodeDescriptor client = host2DataNodeMap.getDatanodeByHost(
          clientMachine);
      for (LocatedBlock b : blocks.getLocatedBlocks()) {
        clusterMap.pseudoSortByDistance(client, b.getLocations());
        
        // Move decommissioned datanodes to the bottom
        Arrays.sort(b.getLocations(), DFSUtil.DECOM_COMPARATOR);
      }
    }
    return blocks;
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @throws FileNotFoundException
   */
  LocatedBlocks getBlockLocations(String src, long offset, long length,
      boolean doAccessTime, boolean needBlockToken) throws FileNotFoundException,
      UnresolvedLinkException, IOException {
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
    final LocatedBlocks ret = getBlockLocationsInternal(src,
        offset, length, doAccessTime, needBlockToken);  
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "open", src, null, null);
    }
    return ret;
  }

  private LocatedBlocks getBlockLocationsInternal(String src,
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
        return getBlockLocationsInternal(inode, offset, length, needBlockToken);
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
  
  LocatedBlocks getBlockLocationsInternal(INodeFile inode,
      long offset, long length, boolean needBlockToken)
  throws IOException {
    readLock();
    try {
    final BlockInfo[] blocks = inode.getBlocks();
    if (LOG.isDebugEnabled()) {
      LOG.debug("blocks = " + java.util.Arrays.asList(blocks));
    }
    if (blocks == null) {
      return null;
    }

    if (blocks.length == 0) {
      return new LocatedBlocks(0, inode.isUnderConstruction(),
          Collections.<LocatedBlock>emptyList(), null, false);
    } else {
      final long n = inode.computeFileSize(false);
      final List<LocatedBlock> locatedblocks = blockManager.getBlockLocations(
          blocks, offset, length, Integer.MAX_VALUE);
      final BlockInfo last = inode.getLastBlock();
      if (LOG.isDebugEnabled()) {
        LOG.debug("last = " + last);
      }
      
      LocatedBlock lastBlock = last.isComplete() ? blockManager
          .getBlockLocation(last, n - last.getNumBytes()) : blockManager
          .getBlockLocation(last, n);
          
      if (isBlockTokenEnabled && needBlockToken) {
        setBlockTokens(locatedblocks);
        setBlockToken(lastBlock);
      }
      return new LocatedBlocks(n, inode.isUnderConstruction(), locatedblocks,
          lastBlock, last.isComplete());
    }
    } finally {
      readUnlock();
    }
  }

  /** Create a LocatedBlock. */
  LocatedBlock createLocatedBlock(final Block b, final DatanodeInfo[] locations,
      final long offset, final boolean corrupt) throws IOException {
    return new LocatedBlock(getExtendedBlock(b), locations, offset, corrupt);
  }
  
  /** Generate block tokens for the blocks to be returned. */
  private void setBlockTokens(List<LocatedBlock> locatedBlocks) throws IOException {
    for(LocatedBlock l : locatedBlocks) {
      setBlockToken(l);
    }
  }
  
  /** Generate block token for a LocatedBlock. */
  private void setBlockToken(LocatedBlock l) throws IOException {
    Token<BlockTokenIdentifier> token = blockTokenSecretManager.generateToken(l
        .getBlock(), EnumSet.of(BlockTokenSecretManager.AccessMode.READ));
    l.setBlockToken(token);
  }

  /**
   * Moves all the blocks from srcs and appends them to trg
   * To avoid rollbacks we will verify validitity of ALL of the args
   * before we start actual move.
   * @param target
   * @param srcs
   * @throws IOException
   */
  public void concat(String target, String [] srcs) 
    throws IOException, UnresolvedLinkException {
    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("concat " + Arrays.toString(srcs) +
          " to " + target);
    }
    // check safe mode
    if (isInSafeMode()) {
      throw new SafeModeException("concat: cannot concat " + target, safeMode);
    }
    
    // verify args
    if(target.isEmpty()) {
      throw new IllegalArgumentException("concat: trg file name is empty");
    }
    if(srcs == null || srcs.length == 0) {
      throw new IllegalArgumentException("concat: srcs list is empty or null");
    }
    
    // currently we require all the files to be in the same dir
    String trgParent = 
      target.substring(0, target.lastIndexOf(Path.SEPARATOR_CHAR));
    for(String s : srcs) {
      String srcParent = s.substring(0, s.lastIndexOf(Path.SEPARATOR_CHAR));
      if(! srcParent.equals(trgParent)) {
        throw new IllegalArgumentException
           ("concat:  srcs and target shoould be in same dir");
      }
    }
    
    writeLock();
    try {
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

      dir.concatInternal(target,srcs);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
   
    
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(target, false);
      logAuditEvent(UserGroupInformation.getLoginUser(),
                    Server.getRemoteIp(),
                    "concat", Arrays.toString(srcs), target, stat);
    }
   
  }

  /**
   * stores the modification and access time for this inode. 
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  public void setTimes(String src, long mtime, long atime) 
    throws IOException, UnresolvedLinkException {
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. " +
                            " Please set dfs.support.accessTime configuration parameter.");
    }
    writeLock();
    try {
    //
    // The caller needs to have write access to set access & modification times.
    if (isPermissionEnabled) {
      checkPathAccess(src, FsAction.WRITE);
    }
    INodeFile inode = dir.getFileINode(src);
    if (inode != null) {
      dir.setTimes(src, inode, mtime, atime, true);
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        final HdfsFileStatus stat = dir.getFileInfo(src, false);
        logAuditEvent(UserGroupInformation.getCurrentUser(),
                      Server.getRemoteIp(),
                      "setTimes", src, null, stat);
      }
    } else {
      throw new FileNotFoundException("File " + src + " does not exist.");
    }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Create a symbolic link.
   */
  public void createSymlink(String target, String link,
      PermissionStatus dirPerms, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    writeLock();
    try {
    if (!createParent) {
      verifyParentDir(link);
    }
    createSymlinkInternal(target, link, dirPerms, createParent);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(link, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "createSymlink", link, target, stat);
    }
  }

  /**
   * Create a symbolic link.
   */
  private void createSymlinkInternal(String target, String link,
      PermissionStatus dirPerms, boolean createParent)
      throws IOException, UnresolvedLinkException {
    writeLock();
    try {
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
    } finally {
      writeUnlock();
    }
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
  public boolean setReplication(String src, short replication) 
    throws IOException, UnresolvedLinkException {
    boolean status = setReplicationInternal(src, replication);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setReplication", src, null, null);
    }
    return status;
  }

  private boolean setReplicationInternal(String src,
      short replication) throws AccessControlException, QuotaExceededException,
      SafeModeException, UnresolvedLinkException, IOException {
    writeLock();
    try {
    if (isInSafeMode())
      throw new SafeModeException("Cannot set replication for " + src, safeMode);
    blockManager.verifyReplication(src, replication, null);
    if (isPermissionEnabled) {
      checkPathAccess(src, FsAction.WRITE);
    }

    int[] oldReplication = new int[1];
    Block[] fileBlocks;
    fileBlocks = dir.setReplication(src, replication, oldReplication);
    if (fileBlocks == null)  // file not found or is a directory
      return false;
    int oldRepl = oldReplication[0];
    if (oldRepl == replication) // the same replication
      return true;

    // update needReplication priority queues
    for(int idx = 0; idx < fileBlocks.length; idx++)
      blockManager.updateNeededReplications(fileBlocks[idx], 0, replication-oldRepl);
      
    if (oldRepl > replication) {  
      // old replication > the new one; need to remove copies
      LOG.info("Reducing replication for file " + src 
               + ". New replication is " + replication);
      for(int idx = 0; idx < fileBlocks.length; idx++)
        blockManager.processOverReplicatedBlock(fileBlocks[idx], replication, null, null);
    } else { // replication factor is increased
      LOG.info("Increasing replication for file " + src 
          + ". New replication is " + replication);
    }
    return true;
    } finally {
      writeUnlock();
    }
  }
    
  long getPreferredBlockSize(String filename) 
    throws IOException, UnresolvedLinkException {
    if (isPermissionEnabled) {
      checkTraverse(filename);
    }
    return dir.getPreferredBlockSize(filename);
  }

  /*
   * Verify that parent directory of src exists.
   */
  private void verifyParentDir(String src) throws FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException {
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
    startFileInternal(src, permissions, holder, clientMachine, flag,
        createParent, replication, blockSize);
    getEditLog().logSync();
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
  private LocatedBlock startFileInternal(String src,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize) throws SafeModeException, FileAlreadyExistsException,
      AccessControlException, UnresolvedLinkException, FileNotFoundException,
      ParentNotDirectoryException, IOException {
    writeLock();
    try {
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean append = flag.contains(CreateFlag.APPEND);
    boolean create = flag.contains(CreateFlag.CREATE);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src
          + ", holder=" + holder
          + ", clientMachine=" + clientMachine
          + ", createParent=" + createParent
          + ", replication=" + replication
          + ", overwrite=" + overwrite
          + ", append=" + append);
    }

    if (isInSafeMode())
      throw new SafeModeException("Cannot create file" + src, safeMode);
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }

    // Verify that the destination does not exist as a directory already.
    boolean pathExists = dir.exists(src);
    if (pathExists && dir.isDir(src)) {
      throw new FileAlreadyExistsException("Cannot create file "+ src + "; already exists as a directory.");
    }

    if (isPermissionEnabled) {
      if (append || (overwrite && pathExists)) {
        checkPathAccess(src, FsAction.WRITE);
      }
      else {
        checkAncestorAccess(src, FsAction.WRITE);
      }
    }

    if (!createParent) {
      verifyParentDir(src);
    }

    try {
      INode myFile = dir.getFileINode(src);
      if (myFile != null && myFile.isUnderConstruction()) {
        INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) myFile;
        //
        // If the file is under construction , then it must be in our
        // leases. Find the appropriate lease record.
        //
        Lease lease = leaseManager.getLeaseByPath(src);
        if (lease == null) {
          throw new AlreadyBeingCreatedException(
              "failed to create file " + src + " for " + holder +
              " on client " + clientMachine + 
              " because pendingCreates is non-null but no leases found.");
        }
        //
        // We found the lease for this file. And surprisingly the original
        // holder is trying to recreate this file. This should never occur.
        //
        if (lease.getHolder().equals(holder)) {
          throw new AlreadyBeingCreatedException(
              "failed to create file " + src + " for " + holder +
              " on client " + clientMachine + 
              " because current leaseholder is trying to recreate file.");
        }
        assert lease.getHolder().equals(pendingFile.getClientName()) :
          "Current lease holder " + lease.getHolder() +
          " does not match file creator " + pendingFile.getClientName();
        //
        // Current lease holder is different from the requester.
        // If the original holder has not renewed in the last SOFTLIMIT 
        // period, then start lease recovery, otherwise fail.
        //
        if (lease.expiredSoftLimit()) {
          LOG.info("startFile: recover lease " + lease + ", src=" + src);
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

      try {
        blockManager.verifyReplication(src, replication, clientMachine);
      } catch(IOException e) {
        throw new IOException("failed to create "+e.getMessage());
      }
      if (append) {
        if (myFile == null) {
          if(!create)
            throw new FileNotFoundException("failed to append to non-existent file "
              + src + " on client " + clientMachine);
          else {
            //append & create a nonexist file equals to overwrite
            return startFileInternal(src, permissions, holder, clientMachine,
                EnumSet.of(CreateFlag.OVERWRITE), createParent, replication, blockSize);
          }
        } else if (myFile.isDirectory()) {
          throw new IOException("failed to append to directory " + src 
                                +" on client " + clientMachine);
        }
      } else if (!dir.isValidToCreate(src)) {
        if (overwrite) {
          delete(src, true);
        } else {
          throw new IOException("failed to create file " + src 
                                +" on client " + clientMachine
                                +" either because the filename is invalid or the file exists");
        }
      }

      DatanodeDescriptor clientNode = 
        host2DataNodeMap.getDatanodeByHost(clientMachine);

      if (append) {
        //
        // Replace current node with a INodeUnderConstruction.
        // Recreate in-memory lease record.
        //
        INodeFile node = (INodeFile) myFile;
        INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
                                        node.getLocalNameBytes(),
                                        node.getReplication(),
                                        node.getModificationTime(),
                                        node.getPreferredBlockSize(),
                                        node.getBlocks(),
                                        node.getPermissionStatus(),
                                        holder,
                                        clientMachine,
                                        clientNode);
        dir.replaceNode(src, node, cons);
        leaseManager.addLease(cons.getClientName(), src);

        // convert last block to under-construction
        LocatedBlock lb = 
          blockManager.convertLastBlockToUnderConstruction(cons);

        if (lb != null && isBlockTokenEnabled) {
          lb.setBlockToken(blockTokenSecretManager.generateToken(lb.getBlock(), 
              EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
        }
        return lb;
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
    } finally {
      writeUnlock();
    }
    return null;
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
    LocatedBlock lb = 
      startFileInternal(src, null, holder, clientMachine, 
                        EnumSet.of(CreateFlag.APPEND), 
                        false, (short)blockManager.maxReplication, (long)0);
    getEditLog().logSync();

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
  public LocatedBlock getAdditionalBlock(String src,
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
      blockManager.commitOrCompleteLastBlock(pendingFile, ExtendedBlock
          .getLocalBlock(previous));

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
    DatanodeDescriptor targets[] = blockManager.replicator.chooseTarget(
        src, replication, clientNode, excludedNodes, blockSize);
    if (targets.length < blockManager.minReplication) {
      throw new IOException("File " + src + " could only be replicated to " +
                            targets.length + " nodes, instead of " +
                            blockManager.minReplication);
    }

    // Allocate a new block and record it in the INode. 
    writeLock();
    try {
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
    if (isBlockTokenEnabled) {
      b.setBlockToken(blockTokenSecretManager.generateToken(b.getBlock(), 
          EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
    }
    return b;
  }

  /**
   * The client would like to let go of the given block
   */
  public boolean abandonBlock(ExtendedBlock b, String src, String holder)
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
    INodeFileUnderConstruction file = checkLease(src, holder);
    dir.removeBlock(src, file, ExtendedBlock.getLocalBlock(b));
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                                    + b
                                    + " is removed from pendingCreates");
    }
    return true;
    } finally {
      writeUnlock();
    }
  }
  
  // make sure that we still have the lease on this file.
  private INodeFileUnderConstruction checkLease(String src, String holder) 
    throws LeaseExpiredException, UnresolvedLinkException {
    INodeFile file = dir.getFileINode(src);
    checkLease(src, holder, file);
    return (INodeFileUnderConstruction)file;
  }

  private void checkLease(String src, String holder, INode file)
      throws LeaseExpiredException {

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
  public boolean completeFile(String src, String holder, ExtendedBlock last) 
    throws SafeModeException, UnresolvedLinkException, IOException {
    checkBlock(last);
    boolean success = completeFileInternal(src, holder, 
        ExtendedBlock.getLocalBlock(last));
    getEditLog().logSync();
    return success ;
  }

  private boolean completeFileInternal(String src, 
      String holder, Block last) throws SafeModeException,
      UnresolvedLinkException, IOException {
    writeLock();
    try {
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " +
          src + " for " + holder);
    }
    if (isInSafeMode())
      throw new SafeModeException("Cannot complete file " + src, safeMode);

    INodeFileUnderConstruction pendingFile = checkLease(src, holder);
    // commit the last block and complete it if it has minimum replicas
    blockManager.commitOrCompleteLastBlock(pendingFile, last);

    if (!checkFileProgress(pendingFile, true)) {
      return false;
    }

    finalizeINodeFileUnderConstruction(src, pendingFile);

    NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " + src
                                  + " is closed by " + holder);
    return true;
    } finally {
      writeUnlock();
    }
  }

  /** 
   * Check all blocks of a file. If any blocks are lower than their intended
   * replication factor, then insert them into neededReplication
   */
  private void checkReplicationFactor(INodeFile file) {
    int numExpectedReplicas = file.getReplication();
    Block[] pendingBlocks = file.getBlocks();
    int nrBlocks = pendingBlocks.length;
    for (int i = 0; i < nrBlocks; i++) {
      blockManager.checkReplication(pendingBlocks[i], numExpectedReplicas);
    }
  }

  static Random randBlockId = new Random();
    
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
    Block b = new Block(FSNamesystem.randBlockId.nextLong(), 0, 0); 
    while(isValidBlock(b)) {
      b.setBlockId(FSNamesystem.randBlockId.nextLong());
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
    writeLock();
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
      writeUnlock();
    }
  }


  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   */
  public void markBlockAsCorrupt(ExtendedBlock blk, DatanodeInfo dn)
    throws IOException {
    writeLock();
    try {
      blockManager.findAndMarkBlockAsCorrupt(blk.getLocalBlock(), dn);
    } finally {
      writeUnlock();
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
    boolean status = renameToInternal(src, dst);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(dst, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "rename", src, dst, stat);
    }
    return status;
  }

  /** @deprecated See {@link #renameTo(String, String)} */
  @Deprecated
  private boolean renameToInternal(String src, String dst)
    throws IOException, UnresolvedLinkException {

    writeLock();
    try {
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src +
          " to " + dst);
    }
    if (isInSafeMode())
      throw new SafeModeException("Cannot rename " + src, safeMode);
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }

    if (isPermissionEnabled) {
      //We should not be doing this.  This is move() not renameTo().
      //but for now,
      String actualdst = dir.isDir(dst)?
          dst + Path.SEPARATOR + new Path(src).getName(): dst;
      checkParentAccess(src, FsAction.WRITE);
      checkAncestorAccess(actualdst, FsAction.WRITE);
    }

    HdfsFileStatus dinfo = dir.getFileInfo(dst, false);
    if (dir.renameTo(src, dst)) {
      changeLease(src, dst, dinfo);     // update lease with new filename
      return true;
    }
    return false;
    } finally {
      writeUnlock();
    }
  }
  

  /** Rename src to dst */
  void renameTo(String src, String dst, Options.Rename... options)
      throws IOException, UnresolvedLinkException {
    renameToInternal(src, dst, options);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      StringBuilder cmd = new StringBuilder("rename options=");
      for (Rename option : options) {
        cmd.append(option.value()).append(" ");
      }
      final HdfsFileStatus stat = dir.getFileInfo(dst, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(),
                    cmd.toString(), src, dst, stat);
    }
  }

  private void renameToInternal(String src, String dst,
      Options.Rename... options) throws IOException {
    writeLock();
    try {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: with options - "
          + src + " to " + dst);
    }
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

    HdfsFileStatus dinfo = dir.getFileInfo(dst, false);
    dir.renameTo(src, dst, options);
    changeLease(src, dst, dinfo); // update lease with new filename
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Remove the indicated file from namespace.
   * 
   * @see ClientProtocol#delete(String, boolean) for detailed descriptoin and 
   * description of exceptions
   */
    public boolean delete(String src, boolean recursive)
      throws AccessControlException, SafeModeException,
      UnresolvedLinkException, IOException {
      if ((!recursive) && (!dir.isDirEmpty(src))) {
        throw new IOException(src + " is non empty");
      }
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
      }
      boolean status = deleteInternal(src, true);
      if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
        logAuditEvent(UserGroupInformation.getCurrentUser(),
                      Server.getRemoteIp(),
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
  private boolean deleteInternal(String src, boolean enforcePermission)
      throws AccessControlException, SafeModeException,
      UnresolvedLinkException, IOException{
    boolean deleteNow = false;
    ArrayList<Block> collectedBlocks = new ArrayList<Block>();

    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot delete " + src, safeMode);
      }
      if (enforcePermission && isPermissionEnabled) {
        checkPermission(src, false, null, FsAction.WRITE, null, FsAction.ALL);
      }
      // Unlink the target directory from directory tree
      if (!dir.delete(src, collectedBlocks)) {
        return false;
      }
      deleteNow = collectedBlocks.size() <= BLOCK_DELETION_INCREMENT;
      if (deleteNow) { // Perform small deletes right away
        removeBlocks(collectedBlocks);
      }
    } finally {
      writeUnlock();
    }
    // Log directory deletion to editlog
    getEditLog().logSync();
    if (!deleteNow) {
      removeBlocks(collectedBlocks); // Incremental deletion of blocks
    }
    collectedBlocks.clear();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* Namesystem.delete: "
        + src +" is removed");
    }
    return true;
  }

  /** From the given list, incrementally remove the blocks from blockManager */
  private void removeBlocks(List<Block> blocks) {
    int start = 0;
    int end = 0;
    while (start < blocks.size()) {
      end = BLOCK_DELETION_INCREMENT + start;
      end = end > blocks.size() ? blocks.size() : end;
      writeLock();
      try {
        for (int i=start; i<end; i++) {
          blockManager.removeBlock(blocks.get(i));
        }
      } finally {
        writeUnlock();
      }
      start = end;
    }
  }
  
  void removePathAndBlocks(String src, List<Block> blocks) {
    leaseManager.removeLeaseWithPrefixPath(src);
    if (blocks == null) {
      return;
    }
    for(Block b : blocks) {
      blockManager.removeBlock(b);
    }
  }

  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException 
   *        if src refers to a symlinks
   *
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if a symlink is encountered.
   *
   * @return object containing information regarding the file
   *         or null if file not found
   */
  HdfsFileStatus getFileInfo(String src, boolean resolveLink) 
    throws AccessControlException, UnresolvedLinkException {
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException("Invalid file name: " + src);
    }
    if (isPermissionEnabled) {
      checkTraverse(src);
    }
    return dir.getFileInfo(src, resolveLink);
  }

  /**
   * Create all the necessary directories
   */
  public boolean mkdirs(String src, PermissionStatus permissions,
      boolean createParent) throws IOException, UnresolvedLinkException {
    boolean status = mkdirsInternal(src, permissions, createParent);
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
  private boolean mkdirsInternal(String src,
      PermissionStatus permissions, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    writeLock();
    try {
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    }
    if (isPermissionEnabled) {
      checkTraverse(src);
    }
    if (dir.isDir(src)) {
      // all the users of mkdirs() are used to expect 'true' even if
      // a new directory is not created.
      return true;
    }
    if (isInSafeMode())
      throw new SafeModeException("Cannot create directory " + src, safeMode);
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
    } finally {
      writeUnlock();
    }
  }

  ContentSummary getContentSummary(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException {
    if (isPermissionEnabled) {
      checkPermission(src, false, null, null, null, FsAction.READ_EXECUTE);
    }
    return dir.getContentSummary(src);
  }

  /**
   * Set the namespace quota and diskspace quota for a directory.
   * See {@link ClientProtocol#setQuota(String, long, long)} for the 
   * contract.
   */
  void setQuota(String path, long nsQuota, long dsQuota) 
      throws IOException, UnresolvedLinkException {
    if (isInSafeMode())
      throw new SafeModeException("Cannot set quota on " + path, safeMode);
    if (isPermissionEnabled) {
      checkSuperuserPrivilege();
    }
    dir.setQuota(path, nsQuota, dsQuota);
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
   *         false if block recovery has been initiated
   */
  boolean internalReleaseLease(Lease lease, String src, 
      String recoveryLeaseHolder) throws AlreadyBeingCreatedException, 
      IOException, UnresolvedLinkException {
    LOG.info("Recovering lease=" + lease + ", src=" + src);

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

  Lease reassignLease(Lease lease, String src, String newHolder,
                      INodeFileUnderConstruction pendingFile) {
    if(newHolder == null)
      return lease;
    pendingFile.setClientName(newHolder);
    return leaseManager.reassignLease(lease, src, newHolder);
  }


  private void finalizeINodeFileUnderConstruction(String src, 
      INodeFileUnderConstruction pendingFile) 
      throws IOException, UnresolvedLinkException {
      
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
          descriptors[i] = getDatanode(newtargets[i]);
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

    // If this commit does not want to close the file, persist
    // blocks only if append is supported and return
    src = leaseManager.findPath(pendingFile);
    if (!closeFile) {
      if (supportAppends) {
        dir.persistBlocks(src, pendingFile);
        getEditLog().logSync();
      }
      LOG.info("commitBlockSynchronization(" + lastblock + ") successful");
      return;
    }

    // commit the last block and complete it if it has minimum replicas
    blockManager.commitOrCompleteLastBlock(pendingFile, storedBlock);

    //remove lease, close file
    finalizeINodeFileUnderConstruction(src, pendingFile);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    LOG.info("commitBlockSynchronization(newblock=" + lastblock
          + ", file=" + src
          + ", newgenerationstamp=" + newgenerationstamp
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
  }


  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws IOException {
    if (isInSafeMode())
      throw new SafeModeException("Cannot renew lease for " + holder, safeMode);
    leaseManager.renewLease(holder);
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
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) 
    throws AccessControlException, UnresolvedLinkException, IOException {
    if (isPermissionEnabled) {
      if (dir.isDir(src)) {
        checkPathAccess(src, FsAction.READ_EXECUTE);
      }
      else {
        checkTraverse(src);
      }
    }
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "listStatus", src, null, null);
    }
    return dir.getListing(src, startAfter, needLocation);
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
  public void registerDatanode(DatanodeRegistration nodeReg
                                            ) throws IOException {
    writeLock();
    try {
    String dnAddress = Server.getRemoteAddress();
    if (dnAddress == null) {
      // Mostly called inside an RPC.
      // But if not, use address passed by the data-node.
      dnAddress = nodeReg.getHost();
    }      

    // check if the datanode is allowed to be connect to the namenode
    if (!verifyNodeRegistration(nodeReg, dnAddress)) {
      throw new DisallowedDatanodeException(nodeReg);
    }

    String hostName = nodeReg.getHost();
      
    // update the datanode's name with ip:port
    DatanodeID dnReg = new DatanodeID(dnAddress + ":" + nodeReg.getPort(),
                                      nodeReg.getStorageID(),
                                      nodeReg.getInfoPort(),
                                      nodeReg.getIpcPort());
    nodeReg.updateRegInfo(dnReg);
    nodeReg.exportedKeys = getBlockKeys();
      
    NameNode.stateChangeLog.info(
                                 "BLOCK* NameSystem.registerDatanode: "
                                 + "node registration from " + nodeReg.getName()
                                 + " storage " + nodeReg.getStorageID());

    DatanodeDescriptor nodeS = datanodeMap.get(nodeReg.getStorageID());
    DatanodeDescriptor nodeN = host2DataNodeMap.getDatanodeByName(nodeReg.getName());
      
    if (nodeN != null && nodeN != nodeS) {
      NameNode.LOG.info("BLOCK* NameSystem.registerDatanode: "
                        + "node from name: " + nodeN.getName());
      // nodeN previously served a different data storage, 
      // which is not served by anybody anymore.
      removeDatanode(nodeN);
      // physically remove node from datanodeMap
      wipeDatanode(nodeN);
      nodeN = null;
    }

    if (nodeS != null) {
      if (nodeN == nodeS) {
        // The same datanode has been just restarted to serve the same data 
        // storage. We do not need to remove old data blocks, the delta will
        // be calculated on the next block report from the datanode
        if(NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.registerDatanode: "
                                        + "node restarted.");
        }
      } else {
        // nodeS is found
        /* The registering datanode is a replacement node for the existing 
          data storage, which from now on will be served by a new node.
          If this message repeats, both nodes might have same storageID 
          by (insanely rare) random chance. User needs to restart one of the
          nodes with its data cleared (or user can just remove the StorageID
          value in "VERSION" file under the data directory of the datanode,
          but this is might not work if VERSION file format has changed 
       */        
        NameNode.stateChangeLog.info( "BLOCK* NameSystem.registerDatanode: "
                                      + "node " + nodeS.getName()
                                      + " is replaced by " + nodeReg.getName() + 
                                      " with the same storageID " +
                                      nodeReg.getStorageID());
      }
      // update cluster map
      clusterMap.remove(nodeS);
      nodeS.updateRegInfo(nodeReg);
      nodeS.setHostName(hostName);
      nodeS.setDisallowed(false); // Node is in the include list
      
      // resolve network location
      resolveNetworkLocation(nodeS);
      clusterMap.add(nodeS);
        
      // also treat the registration message as a heartbeat
      synchronized(heartbeats) {
        if( !heartbeats.contains(nodeS)) {
          heartbeats.add(nodeS);
          //update its timestamp
          nodeS.updateHeartbeat(0L, 0L, 0L, 0L, 0);
          nodeS.isAlive = true;
        }
      }
      checkDecommissioning(nodeS, dnAddress);
      return;
    } 

    // this is a new datanode serving a new data storage
    if (nodeReg.getStorageID().equals("")) {
      // this data storage has never been registered
      // it is either empty or was created by pre-storageID version of DFS
      nodeReg.storageID = newStorageID();
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "BLOCK* NameSystem.registerDatanode: "
            + "new storageID " + nodeReg.getStorageID() + " assigned.");
      }
    }
    // register new datanode
    DatanodeDescriptor nodeDescr 
      = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK, hostName);
    resolveNetworkLocation(nodeDescr);
    unprotectedAddDatanode(nodeDescr);
    clusterMap.add(nodeDescr);
    checkDecommissioning(nodeDescr, dnAddress);
    
    // also treat the registration message as a heartbeat
    synchronized(heartbeats) {
      heartbeats.add(nodeDescr);
      nodeDescr.isAlive = true;
      // no need to update its timestamp
      // because its is done when the descriptor is created
    }

    if (safeMode != null) {
      safeMode.checkMode();
    }
    return;
    } finally {
      writeUnlock();
    }
  }
    
  /* Resolve a node's network location */
  private void resolveNetworkLocation (DatanodeDescriptor node) {
    List<String> names = new ArrayList<String>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      // get the node's IP address
      names.add(node.getHost());
    } else {
      // get the node's host name
      String hostName = node.getHostName();
      int colon = hostName.indexOf(":");
      hostName = (colon==-1)?hostName:hostName.substring(0,colon);
      names.add(hostName);
    }
    
    // resolve its network location
    List<String> rName = dnsToSwitchMapping.resolve(names);
    String networkLocation;
    if (rName == null) {
      LOG.error("The resolve call returned null! Using " + 
          NetworkTopology.DEFAULT_RACK + " for host " + names);
      networkLocation = NetworkTopology.DEFAULT_RACK;
    } else {
      networkLocation = rName.get(0);
    }
    node.setNetworkLocation(networkLocation);
  }
  
  /**
   * Get registrationID for datanodes based on the namespaceID.
   * 
   * @see #registerDatanode(DatanodeRegistration)
   * @see FSImage#newNamespaceID()
   * @return registration ID
   */
  public String getRegistrationID() {
    return Storage.getRegistrationID(dir.fsImage.getStorage());
  }
    
  /**
   * Generate new storage ID.
   * 
   * @return unique storage ID
   * 
   * Note: that collisions are still possible if somebody will try 
   * to bring in a data storage from a different cluster.
   */
  private String newStorageID() {
    String newID = null;
    while(newID == null) {
      newID = "DS" + Integer.toString(r.nextInt());
      if (datanodeMap.get(newID) != null)
        newID = null;
    }
    return newID;
  }
    
  private boolean isDatanodeDead(DatanodeDescriptor node) {
    return (node.getLastUpdate() <
            (now() - heartbeatExpireInterval));
  }
    
  private void setDatanodeDead(DatanodeDescriptor node) throws IOException {
    node.setLastUpdate(0);
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
      int xceiverCount, int xmitsInProgress) throws IOException {
    DatanodeCommand cmd = null;
    synchronized (heartbeats) {
      synchronized (datanodeMap) {
        DatanodeDescriptor nodeinfo = null;
        try {
          nodeinfo = getDatanode(nodeReg);
        } catch(UnregisteredNodeException e) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }
        
        // Check if this datanode should actually be shutdown instead. 
        if (nodeinfo != null && nodeinfo.isDisallowed()) {
          setDatanodeDead(nodeinfo);
          throw new DisallowedDatanodeException(nodeinfo);
        }
         
        if (nodeinfo == null || !nodeinfo.isAlive) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }

        updateStats(nodeinfo, false);
        nodeinfo.updateHeartbeat(capacity, dfsUsed, remaining, blockPoolUsed,
            xceiverCount);
        updateStats(nodeinfo, true);
        
        //check lease recovery
        BlockInfoUnderConstruction[] blocks = nodeinfo
            .getLeaseRecoveryCommand(Integer.MAX_VALUE);
        if (blocks != null) {
          BlockRecoveryCommand brCommand = new BlockRecoveryCommand(
              blocks.length);
          for (BlockInfoUnderConstruction b : blocks) {
            brCommand.add(new RecoveringBlock(
                new ExtendedBlock(blockPoolId, b), b.getExpectedLocations(), b
                    .getBlockRecoveryId()));
          }
          return new DatanodeCommand[] { brCommand };
        }
      
        ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>(3);
        //check pending replication
        List<BlockTargetPair> pendingList = nodeinfo.getReplicationCommand(
              blockManager.maxReplicationStreams - xmitsInProgress);
        if (pendingList != null) {
          cmd = new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blockPoolId,
              pendingList);
          cmds.add(cmd);
        }
        //check block invalidation
        Block[] blks = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
        if (blks != null) {
          cmd = new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, blockPoolId, blks);
          cmds.add(cmd);
        }
        // check access key update
        if (isBlockTokenEnabled && nodeinfo.needKeyUpdate) {
          cmds.add(new KeyUpdateCommand(blockTokenSecretManager.exportKeys()));
          nodeinfo.needKeyUpdate = false;
        }
        if (!cmds.isEmpty()) {
          return cmds.toArray(new DatanodeCommand[cmds.size()]);
        }
      }
    }

    //check distributed upgrade
    cmd = getDistributedUpgradeCommand();
    if (cmd != null) {
      return new DatanodeCommand[] {cmd};
    }
    return null;
  }

  private void updateStats(DatanodeDescriptor node, boolean isAdded) {
    //
    // The statistics are protected by the heartbeat lock
    // For decommissioning/decommissioned nodes, only used capacity
    // is counted.
    //
    assert(Thread.holdsLock(heartbeats));
    if (isAdded) {
      capacityUsed += node.getDfsUsed();
      blockPoolUsed += node.getBlockPoolUsed();
      totalLoad += node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        capacityTotal += node.getCapacity();
        capacityRemaining += node.getRemaining();
      } else {
        capacityTotal += node.getDfsUsed();
      }
    } else {
      capacityUsed -= node.getDfsUsed();
      blockPoolUsed -= node.getBlockPoolUsed();
      totalLoad -= node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        capacityTotal -= node.getCapacity();
        capacityRemaining -= node.getRemaining();
      } else {
        capacityTotal -= node.getDfsUsed();
      }
    }
  }

  /**
   * Update access keys.
   */
  void updateBlockKey() throws IOException {
    this.blockTokenSecretManager.updateKeys();
    synchronized (heartbeats) {
      for (DatanodeDescriptor nodeInfo : heartbeats) {
        nodeInfo.needKeyUpdate = true;
      }
    }
  }

  /**
   * Periodically calls heartbeatCheck() and updateBlockKey()
   */
  class HeartbeatMonitor implements Runnable {
    private long lastHeartbeatCheck;
    private long lastBlockKeyUpdate;
    /**
     */
    public void run() {
      while (fsRunning) {
        try {
          long now = now();
          if (lastHeartbeatCheck + heartbeatRecheckInterval < now) {
            heartbeatCheck();
            lastHeartbeatCheck = now;
          }
          if (isBlockTokenEnabled && (lastBlockKeyUpdate + blockKeyUpdateInterval < now)) {
            updateBlockKey();
            lastBlockKeyUpdate = now;
          }
        } catch (Exception e) {
          FSNamesystem.LOG.error(StringUtils.stringifyException(e));
        }
        try {
          Thread.sleep(5000);  // 5 seconds
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   * Periodically calls computeReplicationWork().
   */
  class ReplicationMonitor implements Runnable {
    static final int INVALIDATE_WORK_PCT_PER_ITERATION = 32;
    static final float REPLICATION_WORK_MULTIPLIER_PER_ITERATION = 2;
    public void run() {
      while (fsRunning) {
        try {
          computeDatanodeWork();
          blockManager.processPendingReplications();
          Thread.sleep(replicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("ReplicationMonitor thread received InterruptedException." + ie);
          break;
        } catch (IOException ie) {
          LOG.warn("ReplicationMonitor thread received exception. " + ie);
        } catch (Throwable t) {
          LOG.warn("ReplicationMonitor thread received Runtime exception. " + t);
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by the Namenode system, to see
  // if there is any work for registered datanodes.
  //
  /////////////////////////////////////////////////////////
  /**
   * Compute block replication and block invalidation work 
   * that can be scheduled on data-nodes.
   * The datanode will be informed of this work at the next heartbeat.
   * 
   * @return number of blocks scheduled for replication or removal.
   * @throws IOException
   */
  public int computeDatanodeWork() throws IOException {
    int workFound = 0;
    int blocksToProcess = 0;
    int nodesToProcess = 0;
    // blocks should not be replicated or removed if safe mode is on
    if (isInSafeMode())
      return workFound;
    synchronized(heartbeats) {
      blocksToProcess = (int)(heartbeats.size() 
          * ReplicationMonitor.REPLICATION_WORK_MULTIPLIER_PER_ITERATION);
      nodesToProcess = (int)Math.ceil((double)heartbeats.size() 
          * ReplicationMonitor.INVALIDATE_WORK_PCT_PER_ITERATION / 100);
    }

    workFound = blockManager.computeReplicationWork(blocksToProcess);
    
    // Update FSNamesystemMetrics counters
    writeLock();
    try {
      blockManager.updateState();
      blockManager.scheduledReplicationBlocksCount = workFound;
    } finally {
      writeUnlock();
    }
    workFound += blockManager.computeInvalidateWork(nodesToProcess);
    return workFound;
  }

  public void setNodeReplicationLimit(int limit) {
    blockManager.maxReplicationStreams = limit;
  }

  /**
   * Update the descriptor for the datanode to reflect a volume failure.
   * @param nodeID DatanodeID to update count for.
   * @throws IOException
   */
  synchronized public void incVolumeFailure(DatanodeID nodeID)
    throws IOException {
    DatanodeDescriptor nodeInfo = getDatanode(nodeID);
    if (nodeInfo != null) {
      nodeInfo.incVolumeFailure();
    } else {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.incVolumeFailure: "
                                   + nodeID.getName() + " does not exist");
    }
  }

  /**
   * Remove a datanode descriptor.
   * @param nodeID datanode ID.
   * @throws IOException
   */
  public void removeDatanode(DatanodeID nodeID) 
    throws IOException {
    writeLock();
    try {
    DatanodeDescriptor nodeInfo = getDatanode(nodeID);
    if (nodeInfo != null) {
      removeDatanode(nodeInfo);
    } else {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.removeDatanode: "
                                   + nodeID.getName() + " does not exist");
    }
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Remove a datanode descriptor.
   * @param nodeInfo datanode descriptor.
   */
  private void removeDatanode(DatanodeDescriptor nodeInfo) {
    synchronized (heartbeats) {
      if (nodeInfo.isAlive) {
        updateStats(nodeInfo, false);
        heartbeats.remove(nodeInfo);
        nodeInfo.isAlive = false;
      }
    }

    Iterator<? extends Block> it = nodeInfo.getBlockIterator();
    while(it.hasNext()) {
      blockManager.removeStoredBlock(it.next(), nodeInfo);
    }
    unprotectedRemoveDatanode(nodeInfo);
    clusterMap.remove(nodeInfo);

    if (safeMode != null) {
      safeMode.checkMode();
    }
  }

  void unprotectedRemoveDatanode(DatanodeDescriptor nodeDescr) {
    nodeDescr.resetBlocks();
    blockManager.removeFromInvalidates(nodeDescr.getStorageID());
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.unprotectedRemoveDatanode: "
          + nodeDescr.getName() + " is out of service now.");
    }
  }
    
  void unprotectedAddDatanode(DatanodeDescriptor nodeDescr) {
    /* To keep host2DataNodeMap consistent with datanodeMap,
       remove  from host2DataNodeMap the datanodeDescriptor removed
       from datanodeMap before adding nodeDescr to host2DataNodeMap.
    */
    host2DataNodeMap.remove(
                            datanodeMap.put(nodeDescr.getStorageID(), nodeDescr));
    host2DataNodeMap.add(nodeDescr);
      
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.unprotectedAddDatanode: "
          + "node " + nodeDescr.getName() + " is added to datanodeMap.");
    }
  }

  /**
   * Physically remove node from datanodeMap.
   *
   * @param nodeID node
   * @throws IOException
   */
  void wipeDatanode(DatanodeID nodeID) throws IOException {
    String key = nodeID.getStorageID();
    host2DataNodeMap.remove(datanodeMap.remove(key));
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.wipeDatanode: "
          + nodeID.getName() + " storage " + key 
          + " is removed from datanodeMap.");
    }
  }

  FSImage getFSImage() {
    return dir.fsImage;
  }

  FSEditLog getEditLog() {
    return getFSImage().getEditLog();
  }

  /**
   * Check if there are any expired heartbeats, and if so,
   * whether any blocks have to be re-replicated.
   * While removing dead datanodes, make sure that only one datanode is marked
   * dead at a time within the synchronized section. Otherwise, a cascading
   * effect causes more datanodes to be declared dead.
   */
  void heartbeatCheck() {
    if (isInSafeMode()) {
      // not to check dead nodes if in safemode
      return;
    }
    boolean allAlive = false;
    while (!allAlive) {
      boolean foundDead = false;
      DatanodeID nodeID = null;

      // locate the first dead node.
      synchronized(heartbeats) {
        for (Iterator<DatanodeDescriptor> it = heartbeats.iterator();
             it.hasNext();) {
          DatanodeDescriptor nodeInfo = it.next();
          if (isDatanodeDead(nodeInfo)) {
            myFSMetrics.numExpiredHeartbeats.inc();
            foundDead = true;
            nodeID = nodeInfo;
            break;
          }
        }
      }

      // acquire the fsnamesystem lock, and then remove the dead node.
      if (foundDead) {
        writeLock();
        try {
          synchronized(heartbeats) {
            synchronized (datanodeMap) {
              DatanodeDescriptor nodeInfo = null;
              try {
                nodeInfo = getDatanode(nodeID);
              } catch (IOException e) {
                nodeInfo = null;
              }
              if (nodeInfo != null && isDatanodeDead(nodeInfo)) {
                NameNode.stateChangeLog.info("BLOCK* NameSystem.heartbeatCheck: "
                                             + "lost heartbeat from " + nodeInfo.getName());
                removeDatanode(nodeInfo);
              }
            }
          }
        } finally {
          writeUnlock();
        }
      }
      allAlive = !foundDead;
    }
  }
    
  /**
   * The given node is reporting all its blocks.  Use this info to 
   * update the (machine-->blocklist) and (block-->machinelist) tables.
   */
  public void processReport(DatanodeID nodeID, String poolId,
      BlockListAsLongs newReport) throws IOException {

    writeLock();
    try {
    long startTime = now();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: "
                             + "from " + nodeID.getName()+" " + 
                             newReport.getNumberOfBlocks()+" blocks");
    }
    DatanodeDescriptor node = getDatanode(nodeID);
    if (node == null || !node.isAlive) {
      throw new IOException("ProcessReport from dead or unregisterted node: "
                            + nodeID.getName());
    }
    // To minimize startup time, we discard any second (or later) block reports
    // that we receive while still in startup phase.
    if (isInStartupSafeMode() && node.numBlocks() > 0) {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: "
          + "discarded non-initial block report from " + nodeID.getName()
          + " because namenode still in startup phase");
      return;
    }

    blockManager.processReport(node, newReport);
    NameNode.getNameNodeMetrics().blockReport.inc((int) (now() - startTime));
    } finally {
      writeUnlock();
    }
  }

  /**
   * We want "replication" replicates for the block, but we now have too many.  
   * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
   *
   * srcNodes.size() - dstNodes.size() == replication
   *
   * We pick node that make sure that replicas are spread across racks and
   * also try hard to pick one with least free space.
   * The algorithm is first to pick a node with least free space from nodes
   * that are on a rack holding more than one replicas of the block.
   * So removing such a replica won't remove a rack. 
   * If no such a node is available,
   * then pick a node with least free space
   */
  void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess, 
                              Block b, short replication,
                              DatanodeDescriptor addedNode,
                              DatanodeDescriptor delNodeHint,
                              BlockPlacementPolicy replicator) {
    // first form a rack to datanodes map and
    INodeFile inode = blockManager.getINode(b);
    HashMap<String, ArrayList<DatanodeDescriptor>> rackMap =
      new HashMap<String, ArrayList<DatanodeDescriptor>>();
    for (Iterator<DatanodeDescriptor> iter = nonExcess.iterator();
         iter.hasNext();) {
      DatanodeDescriptor node = iter.next();
      String rackName = node.getNetworkLocation();
      ArrayList<DatanodeDescriptor> datanodeList = rackMap.get(rackName);
      if(datanodeList==null) {
        datanodeList = new ArrayList<DatanodeDescriptor>();
      }
      datanodeList.add(node);
      rackMap.put(rackName, datanodeList);
    }
    
    // split nodes into two sets
    // priSet contains nodes on rack with more than one replica
    // remains contains the remaining nodes
    ArrayList<DatanodeDescriptor> priSet = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> remains = new ArrayList<DatanodeDescriptor>();
    for( Iterator<Entry<String, ArrayList<DatanodeDescriptor>>> iter = 
      rackMap.entrySet().iterator(); iter.hasNext(); ) {
      Entry<String, ArrayList<DatanodeDescriptor>> rackEntry = iter.next();
      ArrayList<DatanodeDescriptor> datanodeList = rackEntry.getValue(); 
      if( datanodeList.size() == 1 ) {
        remains.add(datanodeList.get(0));
      } else {
        priSet.addAll(datanodeList);
      }
    }
    
    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    while (nonExcess.size() - replication > 0) {
      DatanodeInfo cur = null;

      // check if we can del delNodeHint
      if (firstOne && delNodeHint !=null && nonExcess.contains(delNodeHint) &&
            (priSet.contains(delNodeHint) || (addedNode != null && !priSet.contains(addedNode))) ) {
          cur = delNodeHint;
      } else { // regular excessive replica removal
        cur = replicator.chooseReplicaToDelete(inode, b, replication, priSet, remains);
      }

      firstOne = false;
      // adjust rackmap, priSet, and remains
      String rack = cur.getNetworkLocation();
      ArrayList<DatanodeDescriptor> datanodes = rackMap.get(rack);
      datanodes.remove(cur);
      if(datanodes.isEmpty()) {
        rackMap.remove(rack);
      }
      if( priSet.remove(cur) ) {
        if (datanodes.size() == 1) {
          priSet.remove(datanodes.get(0));
          remains.add(datanodes.get(0));
        }
      } else {
        remains.remove(cur);
      }

      nonExcess.remove(cur);
      blockManager.addToExcessReplicate(cur, b);

      //
      // The 'excessblocks' tracks blocks until we get confirmation
      // that the datanode has deleted them; the only way we remove them
      // is when we get a "removeBlock" message.  
      //
      // The 'invalidate' list is used to inform the datanode the block 
      // should be deleted.  Items are removed from the invalidate list
      // upon giving instructions to the namenode.
      //
      blockManager.addToInvalidates(b, cur);
      NameNode.stateChangeLog.info("BLOCK* NameSystem.chooseExcessReplicates: "
                +"("+cur.getName()+", "+b+") is added to recentInvalidateSets");
    }
  }


  /**
   * The given node is reporting that it received a certain block.
   */
  public void blockReceived(DatanodeID nodeID,  
                                         String poolId,
                                         Block block,
                                         String delHint
                                         ) throws IOException {
    writeLock();
    try {
    DatanodeDescriptor node = getDatanode(nodeID);
    if (node == null || !node.isAlive) {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: " + block
          + " is received from dead or unregistered node " + nodeID.getName());
      throw new IOException(
          "Got blockReceived message from unregistered or dead node " + block);
    }
        
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockReceived: "
                                    +block+" is received from " + nodeID.getName());
    }

    blockManager.addBlock(node, block, delHint);
    } finally {
      writeUnlock();
    }
  }

  private void checkBlock(ExtendedBlock block) throws IOException {
    if (block != null && !this.blockPoolId.equals(block.getBlockPoolId())) {
      throw new IOException("Unexpected BlockPoolId " + block.getBlockPoolId()
          + " - expected " + blockPoolId);
    }
  }

  public long getMissingBlocksCount() {
    // not locking
    return blockManager.getMissingBlocksCount();
  }
  
  long[] getStats() {
    synchronized(heartbeats) {
      return new long[] {this.capacityTotal, this.capacityUsed, 
                         this.capacityRemaining,
                         getUnderReplicatedBlocks(),
                         getCorruptReplicaBlocks(),
                         getMissingBlocksCount(),
                         getBlockPoolUsedSpace()};
    }
  }

  /**
   * Total raw bytes including non-dfs used space.
   */
  @Override // FSNamesystemMBean
  public long getCapacityTotal() {
    synchronized(heartbeats) {
      return capacityTotal;
    }
  }

  /**
   * Total used space by data nodes
   */
  @Override // FSNamesystemMBean
  public long getCapacityUsed() {
    synchronized(heartbeats) {
      return capacityUsed;
    }
  }
  /**
   * Total used space by data nodes as percentage of total capacity
   */
  public float getCapacityUsedPercent() {
    synchronized(heartbeats){
      return DFSUtil.getPercentUsed(capacityUsed, capacityTotal);
    }
  }
  /**
   * Total used space by data nodes for non DFS purposes such
   * as storing temporary files on the local file system
   */
  public long getCapacityUsedNonDFS() {
    long nonDFSUsed = 0;
    synchronized(heartbeats){
      nonDFSUsed = capacityTotal - capacityRemaining - capacityUsed;
    }
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }
  /**
   * Total non-used raw bytes.
   */
  public long getCapacityRemaining() {
    synchronized(heartbeats) {
      return capacityRemaining;
    }
  }

  /**
   * Total remaining space by data nodes as percentage of total capacity
   */
  public float getCapacityRemainingPercent() {
    synchronized(heartbeats){
      return DFSUtil.getPercentRemaining(capacityRemaining, capacityTotal);
    }
  }
  /**
   * Total number of connections.
   */
  @Override // FSNamesystemMBean
  public int getTotalLoad() {
    synchronized (heartbeats) {
      return this.totalLoad;
    }
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    return getDatanodeListForReport(type).size(); 
  }

  private ArrayList<DatanodeDescriptor> getDatanodeListForReport(
                                                      DatanodeReportType type) {
    boolean listLiveNodes = type == DatanodeReportType.ALL ||
                            type == DatanodeReportType.LIVE;
    boolean listDeadNodes = type == DatanodeReportType.ALL ||
                            type == DatanodeReportType.DEAD;

    HashMap<String, String> mustList = new HashMap<String, String>();

    readLock();
    try {
    if (listDeadNodes) {
      //first load all the nodes listed in include and exclude files.
      for (Iterator<String> it = hostsReader.getHosts().iterator(); 
           it.hasNext();) {
        mustList.put(it.next(), "");
      }
      for (Iterator<String> it = hostsReader.getExcludedHosts().iterator(); 
           it.hasNext();) {
        mustList.put(it.next(), "");
      }
    }
   
    ArrayList<DatanodeDescriptor> nodes = null;
    
    synchronized (datanodeMap) {
      nodes = new ArrayList<DatanodeDescriptor>(datanodeMap.size() + 
                                                mustList.size());
      
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); 
                                                               it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        boolean isDead = isDatanodeDead(dn);
        if ( (isDead && listDeadNodes) || (!isDead && listLiveNodes) ) {
          nodes.add(dn);
        }
        //Remove any form of the this datanode in include/exclude lists.
        mustList.remove(dn.getName());
        mustList.remove(dn.getHost());
        mustList.remove(dn.getHostName());
      }
    }
    
    if (listDeadNodes) {
      for (Iterator<String> it = mustList.keySet().iterator(); it.hasNext();) {
        DatanodeDescriptor dn = 
            new DatanodeDescriptor(new DatanodeID(it.next()));
        dn.setLastUpdate(0);
        nodes.add(dn);
      }
    }
    
    return nodes;
    } finally {
      readUnlock();
    }
  }

  public DatanodeInfo[] datanodeReport( DatanodeReportType type
      ) throws AccessControlException {
    readLock();
    try {
    checkSuperuserPrivilege();

    ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(type);
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
    writeLock();
    try {
    checkSuperuserPrivilege();
    if(!isInSafeMode()) {
      throw new IOException("Safe mode should be turned ON " +
                            "in order to create namespace image.");
    }
    getFSImage().saveNamespace(true);
    LOG.info("New namespace image has been created.");
    } finally {
      writeUnlock();
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

  /**
   */
  public void DFSNodesStatus(ArrayList<DatanodeDescriptor> live, 
                                          ArrayList<DatanodeDescriptor> dead) {
    readLock();
    try {
    ArrayList<DatanodeDescriptor> results = 
                            getDatanodeListForReport(DatanodeReportType.ALL);    
    for(Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (isDatanodeDead(node))
        dead.add(node);
      else
        live.add(node);
    }
    } finally {
      readUnlock();
    }
  }

  /**
   * Prints information about all datanodes.
   */
  private void datanodeDump(PrintWriter out) {
    readLock();
    try {
    synchronized (datanodeMap) {
      out.println("Metasave: Number of datanodes: " + datanodeMap.size());
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext();) {
        DatanodeDescriptor node = it.next();
        out.println(node.dumpDatanode());
      }
    }
    } finally {
      readUnlock();
    }
  }

  /**
   * Start decommissioning the specified datanode. 
   */
  private void startDecommission (DatanodeDescriptor node) 
    throws IOException {

    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      LOG.info("Start Decommissioning node " + node.getName() + " with " + 
          node.numBlocks() +  " blocks.");
      synchronized (heartbeats) {
        updateStats(node, false);
        node.startDecommission();
        updateStats(node, true);
      }
      node.decommissioningStatus.setStartTime(now());
      
      // all the blocks that reside on this node have to be replicated.
      checkDecommissionStateInternal(node);
    }
  }

  /**
   * Stop decommissioning the specified datanodes.
   */
  public void stopDecommission (DatanodeDescriptor node) 
    throws IOException {
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      LOG.info("Stop Decommissioning node " + node.getName());
      synchronized (heartbeats) {
        updateStats(node, false);
        node.stopDecommission();
        updateStats(node, true);
      }
    }
  }

  /** 
   */
  public DatanodeInfo getDataNodeInfo(String name) {
    return datanodeMap.get(name);
  }

  public Date getStartTime() {
    return new Date(systemStart); 
  }
    
  short getMaxReplication()     { return (short)blockManager.maxReplication; }
  short getMinReplication()     { return (short)blockManager.minReplication; }
  short getDefaultReplication() { return (short)blockManager.defaultReplication; }

  /**
   * Clamp the specified replication between the minimum and maximum
   * replication levels for this namesystem.
   */
  short adjustReplication(short replication) {
    short minReplication = getMinReplication();
    if (replication < minReplication) {
      replication = minReplication;
    }
    short maxReplication = getMaxReplication();
    if (replication > maxReplication) {
      replication = maxReplication;
    }
    return replication;
  }
    
  /**
   * A immutable object that stores the number of live replicas and
   * the number of decommissined Replicas.
   */
  static class NumberReplicas {
    private int liveReplicas;
    int decommissionedReplicas;
    private int corruptReplicas;
    private int excessReplicas;

    NumberReplicas() {
      initialize(0, 0, 0, 0);
    }

    NumberReplicas(int live, int decommissioned, int corrupt, int excess) {
      initialize(live, decommissioned, corrupt, excess);
    }

    void initialize(int live, int decommissioned, int corrupt, int excess) {
      liveReplicas = live;
      decommissionedReplicas = decommissioned;
      corruptReplicas = corrupt;
      excessReplicas = excess;
    }

    int liveReplicas() {
      return liveReplicas;
    }
    int decommissionedReplicas() {
      return decommissionedReplicas;
    }
    int corruptReplicas() {
      return corruptReplicas;
    }
    int excessReplicas() {
      return excessReplicas;
    }
  } 

  /**
   * Change, if appropriate, the admin state of a datanode to 
   * decommission completed. Return true if decommission is complete.
   */
  boolean checkDecommissionStateInternal(DatanodeDescriptor node) {
    //
    // Check to see if all blocks in this decommissioned
    // node has reached their target replication factor.
    //
    if (node.isDecommissionInProgress()) {
      if (!blockManager.isReplicationInProgress(node)) {
        node.setDecommissioned();
        LOG.info("Decommission complete for node " + node.getName());
      }
    }
    return node.isDecommissioned();
  }

  /** 
   * Keeps track of which datanodes/ipaddress are allowed to connect to the namenode.
   */
  private boolean inHostsList(DatanodeID node, String ipAddr) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || 
            (ipAddr != null && hostsList.contains(ipAddr)) ||
            hostsList.contains(node.getHost()) ||
            hostsList.contains(node.getName()) || 
            ((node instanceof DatanodeInfo) && 
             hostsList.contains(((DatanodeInfo)node).getHostName())));
  }
  
  private boolean inExcludedHostsList(DatanodeID node, String ipAddr) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return  ((ipAddr != null && excludeList.contains(ipAddr)) ||
            excludeList.contains(node.getHost()) ||
            excludeList.contains(node.getName()) ||
            ((node instanceof DatanodeInfo) && 
             excludeList.contains(((DatanodeInfo)node).getHostName())));
  }

  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   * 1. Added to hosts  --> no further work needed here.
   * 2. Removed from hosts --> mark AdminState as decommissioned. 
   * 3. Added to exclude --> start decommission.
   * 4. Removed from exclude --> stop decommission.
   */
  public void refreshNodes(Configuration conf) throws IOException {
    checkSuperuserPrivilege();
    // Reread the config to get dfs.hosts and dfs.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list
    if (conf == null)
      conf = new HdfsConfiguration();
    hostsReader.updateFileNames(conf.get(DFSConfigKeys.DFS_HOSTS,""), 
                                conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));
    hostsReader.refresh();
    writeLock();
    try {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
           it.hasNext();) {
        DatanodeDescriptor node = it.next();
        // Check if not include.
        if (!inHostsList(node, null)) {
          node.setDisallowed(true);  // case 2.
        } else {
          if (inExcludedHostsList(node, null)) {
            startDecommission(node);   // case 3.
          } else {
            stopDecommission(node);   // case 4.
          }
        }
      }
    } finally {
      writeUnlock();
    }
  }
    
  void finalizeUpgrade() throws IOException {
    checkSuperuserPrivilege();
    getFSImage().finalizeUpgrade();
  }

  /**
   * Checks if the node is not on the hosts list.  If it is not, then
   * it will be disallowed from registering. 
   */
  private boolean verifyNodeRegistration(DatanodeID nodeReg, String ipAddr) {
    assert (hasWriteLock());
    return inHostsList(nodeReg, ipAddr);
  }
    
  /**
   * Decommission the node if it is in exclude list.
   */
  private void checkDecommissioning(DatanodeDescriptor nodeReg, String ipAddr) 
    throws IOException {
    // If the registered node is in exclude list, then decommission it
    if (inExcludedHostsList(nodeReg, ipAddr)) {
      startDecommission(nodeReg);
    }
  }
    
  /**
   * Get data node by storage ID.
   * 
   * @param nodeID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws IOException
   */
  public DatanodeDescriptor getDatanode(DatanodeID nodeID) throws IOException {
    UnregisteredNodeException e = null;
    DatanodeDescriptor node = datanodeMap.get(nodeID.getStorageID());
    if (node == null) 
      return null;
    if (!node.getName().equals(nodeID.getName())) {
      e = new UnregisteredNodeException(nodeID, node);
      NameNode.stateChangeLog.fatal("BLOCK* NameSystem.getDatanode: "
                                    + e.getLocalizedMessage());
      throw e;
    }
    return node;
  }

  /** Choose a random datanode
   * 
   * @return a randomly chosen datanode
   */
  DatanodeDescriptor getRandomDatanode() {
    return (DatanodeDescriptor)clusterMap.chooseRandom(NodeBase.ROOT);
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
   * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
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
    private int blockSafe;
    /** Number of blocks needed to satisfy safe mode threshold condition */
    private int blockThreshold;
    /** Number of blocks needed before populating replication queues */
    private int blockReplQueueThreshold;
    /** time of the last status printout */
    private long lastStatusReport = 0;
    /** flag indicating whether replication queues have been initialized */
    private boolean initializedReplQueues = false;
    
    /**
     * Creates SafeModeInfo when the name node enters
     * automatic safe mode at startup.
     *  
     * @param conf configuration
     */
    SafeModeInfo(Configuration conf) {
      this.threshold = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 0.95f);
      this.datanodeThreshold = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT);
      this.extension = conf.getInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
      this.safeReplication = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 
                                         DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
      // default to safe mode threshold (i.e., don't populate queues before leaving safe mode)
      this.replQueueThreshold = 
        conf.getFloat(DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
                      (float) threshold);
      this.blockTotal = 0; 
      this.blockSafe = 0;
    }

    /**
     * Creates SafeModeInfo when safe mode is entered manually.
     *
     * The {@link #threshold} is set to 1.5 so that it could never be reached.
     * {@link #blockTotal} is set to -1 to indicate that safe mode is manual.
     * 
     * @see SafeModeInfo
     */
    private SafeModeInfo() {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.replQueueThreshold = 1.5f; // can never be reached
      this.blockTotal = -1;
      this.blockSafe = -1;
      this.reached = -1;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }
      
    /**
     * Check if safe mode is on.
     * @return true if in safe mode
     */
    synchronized boolean isOn() {
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
    synchronized boolean isPopulatingReplQueues() {
      return initializedReplQueues;
    }

    /**
     * Enter safe mode.
     */
    void enter() {
      this.reached = 0;
    }
      
    /**
     * Leave safe mode.
     * <p>
     * Switch to manual safe mode if distributed upgrade is required.<br>
     * Check for invalid, under- & over-replicated blocks in the end of startup.
     */
    synchronized void leave(boolean checkForUpgrades) {
      if(checkForUpgrades) {
        // verify whether a distributed upgrade needs to be started
        boolean needUpgrade = false;
        try {
          needUpgrade = startDistributedUpgradeIfNeeded();
        } catch(IOException e) {
          FSNamesystem.LOG.error(StringUtils.stringifyException(e));
        }
        if(needUpgrade) {
          // switch to manual safe mode
          safeMode = new SafeModeInfo();
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
      NameNode.getNameNodeMetrics().safeModeTime.set((int) timeInSafemode);
      
      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF."); 
      }
      reached = -1;
      safeMode = null;
      NameNode.stateChangeLog.info("STATE* Network topology has "
                                   +clusterMap.getNumOfRacks()+" racks and "
                                   +clusterMap.getNumOfLeaves()+ " datanodes");
      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has "
                                   +blockManager.neededReplications.size()+" blocks");
    }

    /**
     * Initialize replication queues.
     */
    synchronized void initializeReplQueues() {
      LOG.info("initializing replication queues");
      if (isPopulatingReplQueues()) {
        LOG.warn("Replication queues already initialized.");
      }
      blockManager.processMisReplicatedBlocks();
      initializedReplQueues = true;
    }

    /**
     * Check whether we have reached the threshold for 
     * initializing replication queues.
     */
    synchronized boolean canInitializeReplQueues() {
      return blockSafe >= blockReplQueueThreshold;
    }
      
    /** 
     * Safe mode can be turned off iff 
     * the threshold is reached and 
     * the extension time have passed.
     * @return true if can leave or false otherwise.
     */
    synchronized boolean canLeave() {
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
    boolean needEnter() {
      return (threshold != 0 && blockSafe < blockThreshold) ||
        (getNumLiveDataNodes() < datanodeThreshold);
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
    synchronized void setBlockTotal(int total) {
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
    synchronized void incrementSafeBlockCount(short replication) {
      if ((int)replication == safeReplication)
        this.blockSafe++;
      checkMode();
    }
      
    /**
     * Decrement number of safe blocks if current block has 
     * fallen below minimal replication.
     * @param replication current replication 
     */
    synchronized void decrementSafeBlockCount(short replication) {
      if (replication == safeReplication-1)
        this.blockSafe--;
      checkMode();
    }

    /**
     * Check if safe mode was entered manually or at startup.
     */
    boolean isManual() {
      return extension == Integer.MAX_VALUE;
    }

    /**
     * Set manual safe mode.
     */
    synchronized void setManual() {
      extension = Integer.MAX_VALUE;
    }

    /**
     * A tip on how safe mode is to be turned off: manually or automatically.
     */
    String getTurnOffTip() {
      if(reached < 0)
        return "Safe mode is OFF.";
      String leaveMsg = "Safe mode will be turned off automatically";
      if(isManual()) {
        if(getDistributedUpgradeState())
          return leaveMsg + " upon completion of " + 
            "the distributed upgrade: upgrade progress = " + 
            getDistributedUpgradeStatus() + "%";
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
            blockSafe, (blockThreshold - blockSafe), threshold, blockTotal);
        }
        if (numLive < datanodeThreshold) {
          if (!"".equals(msg)) {
            msg += "\n";
          }
          msg += String.format(
            "The number of live datanodes %d needs an additional %d live "
            + "datanodes to reach the minimum number %d.",
            numLive, datanodeThreshold - numLive, datanodeThreshold);
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

    /**
     * Returns printable state of the class.
     */
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
    boolean isConsistent() throws IOException {
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
        enterSafeMode();
        break;
      }
    }
    return isInSafeMode();
  }

  /**
   * Check whether the name node is in safe mode.
   * @return true if safe mode is ON, false otherwise
   */
  synchronized boolean isInSafeMode() {
    if (safeMode == null)
      return false;
    return safeMode.isOn();
  }
  
  /**
   * Check whether the name node is in startup mode.
   */
  synchronized boolean isInStartupSafeMode() {
    if (safeMode == null)
      return false;
    return safeMode.isOn() && !safeMode.isManual();
  }

  /**
   * Check whether replication queues are populated.
   */
  synchronized boolean isPopulatingReplQueues() {
    return (!isInSafeMode() ||
            safeMode.isPopulatingReplQueues());
  }
    
  /**
   * Increment number of blocks that reached minimal replication.
   * @param replication current replication 
   */
  void incrementSafeBlockCount(int replication) {
    if (safeMode == null)
      return;
    safeMode.incrementSafeBlockCount((short)replication);
  }

  /**
   * Decrement number of blocks that reached minimal replication.
   */
  void decrementSafeBlockCount(Block b) {
    if (safeMode == null) // mostly true
      return;
    safeMode.decrementSafeBlockCount((short)blockManager.countNodes(b).liveReplicas());
  }

  /**
   * Set the total number of blocks in the system. 
   */
  void setBlockTotal() {
    if (safeMode == null)
      return;
    safeMode.setBlockTotal((int)getCompleteBlocksTotal());
  }

  /**
   * Get the total number of blocks in the system. 
   */
  @Override // FSNamesystemMBean
  public long getBlocksTotal() {
    return blockManager.getTotalBlocks();
  }

  /**
   * Get the total number of COMPLETE blocks in the system.
   * For safe mode only complete blocks are counted.
   */
  long getCompleteBlocksTotal() {
    // Calculate number of blocks under construction
    long numUCBlocks = 0;
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
  }

  /**
   * Enter safe mode manually.
   * @throws IOException
   */
  void enterSafeMode() throws IOException {
    writeLock();
    try {
    // Ensure that any concurrent operations have been fully synced
    // before entering safe mode. This ensures that the FSImage
    // is entirely stable on disk as soon as we're in safe mode.
    getEditLog().logSyncAll();
    if (!isInSafeMode()) {
      safeMode = new SafeModeInfo();
      return;
    }
    safeMode.setManual();
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
    if(getDistributedUpgradeState())
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
    if (!isInSafeMode())
      return "";
    return safeMode.getTurnOffTip();
    } finally {
      readUnlock();
    }
  }

  long getEditLogSize() throws IOException {
    return getEditLog().getEditLogSize();
  }

  CheckpointSignature rollEditLog() throws IOException {
    writeLock();
    try {
    if (isInSafeMode()) {
      throw new SafeModeException("Checkpoint not created",
                                  safeMode);
    }
    LOG.info("Roll Edit Log from " + Server.getRemoteAddress());
    return getFSImage().rollEditLog();
    } finally {
      writeUnlock();
    }
  }

  /**
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   * Reopens the new edits file.
   *
   * @param sig the signature of this checkpoint (old image)
   */
  void rollFSImage(CheckpointSignature sig) throws IOException {
    writeLock();
    try {
    if (isInSafeMode()) {
      throw new SafeModeException("Checkpoint not created",
                                  safeMode);
    }
    LOG.info("Roll FSImage from " + Server.getRemoteAddress());
    getFSImage().rollFSImage(sig, true);
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
    writeLock();
    try {
    LOG.info("End checkpoint for " + registration.getAddress());
    getFSImage().endCheckpoint(sig, registration.getRole());
    } finally {
      writeUnlock();
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

  int getDistributedUpgradeVersion() {
    return upgradeManager.getUpgradeVersion();
  }

  UpgradeCommand getDistributedUpgradeCommand() throws IOException {
    return upgradeManager.getBroadcastCommand();
  }

  boolean getDistributedUpgradeState() {
    return upgradeManager.getUpgradeState();
  }

  short getDistributedUpgradeStatus() {
    return upgradeManager.getUpgradeStatus();
  }

  boolean startDistributedUpgradeIfNeeded() throws IOException {
    return upgradeManager.startUpgrade();
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

  private void checkSuperuserPrivilege() throws AccessControlException {
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
  public long getFilesTotal() {
    return this.dir.totalInodes();
  }

  @Override // FSNamesystemMBean
  public long getPendingReplicationBlocks() {
    return blockManager.pendingReplicationBlocksCount;
  }

  @Override // FSNamesystemMBean
  public long getUnderReplicatedBlocks() {
    return blockManager.underReplicatedBlocksCount;
  }

  /** Return number of under-replicated but not missing blocks */
  public long getUnderReplicatedNotMissingBlocks() {
    return blockManager.getUnderReplicatedNotMissingBlocks();
  }

  /** Returns number of blocks with corrupt replicas */
  public long getCorruptReplicaBlocks() {
    return blockManager.corruptReplicaBlocksCount;
  }

  @Override // FSNamesystemMBean
  public long getScheduledReplicationBlocks() {
    return blockManager.scheduledReplicationBlocksCount;
  }

  public long getPendingDeletionBlocks() {
    return blockManager.pendingDeletionBlocksCount;
  }

  public long getExcessBlocks() {
    return blockManager.excessBlocksCount;
  }
  
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
  void registerMBean(Configuration conf) {
    // We wrap to bypass standard mbean naming convention.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;
    try {
      myFSMetrics = new FSNamesystemMetrics(this, conf);
      bean = new StandardMBean(this,FSNamesystemMBean.class);
      mbeanName = MBeanUtil.registerMBean("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Exception in initializing StandardMBean as FSNamesystemMBean",
	  e);
    }

    LOG.info("Registered FSNamesystemStatusMBean");
  }

  /**
   * get FSNamesystemMetrics
   */
  public FSNamesystemMetrics getFSNamesystemMetrics() {
    return myFSMetrics;
  }

  /**
   * shutdown FSNamesystem
   */
  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }
  

  /**
   * Number of live data nodes
   * @return Number of live data nodes
   */
  @Override // FSNamesystemMBean
  public int getNumLiveDataNodes() {
    int numLive = 0;
    synchronized (datanodeMap) {   
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); 
                                                               it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        if (!isDatanodeDead(dn) ) {
          numLive++;
        }
      }
    }
    return numLive;
  }
  

  /**
   * Number of dead data nodes
   * @return Number of dead data nodes
   */
  @Override // FSNamesystemMBean
  public int getNumDeadDataNodes() {
    int numDead = 0;
    synchronized (datanodeMap) {   
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); 
                                                               it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        if (isDatanodeDead(dn) ) {
          numDead++;
        }
      }
    }
    return numDead;
  }

  /**
   * Sets the generation stamp for this filesystem
   */
  public void setGenerationStamp(long stamp) {
    generationStamp.setStamp(stamp);
  }

  /**
   * Gets the generation stamp for this filesystem
   */
  public long getGenerationStamp() {
    return generationStamp.getStamp();
  }

  /**
   * Increments, logs and then returns the stamp
   */
  long nextGenerationStamp() {
    long gs = generationStamp.nextStamp();
    getEditLog().logGenerationStamp(gs);
    return gs;
  }

  private INodeFileUnderConstruction checkUCBlock(ExtendedBlock block,
      String clientName) throws IOException {
    // check safe mode
    if (isInSafeMode())
      throw new SafeModeException("Cannot get a new generation stamp and an " +
                                "access token for block " + block, safeMode);
    
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
    writeLock();
    try {
    // check vadility of parameters
    checkUCBlock(block, clientName);
    
    // get a new generation stamp and an access token
    block.setGenerationStamp(nextGenerationStamp());
    LocatedBlock locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
    if (isBlockTokenEnabled) {
      locatedBlock.setBlockToken(blockTokenSecretManager.generateToken(
          block, EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
    }
    return locatedBlock;
    } finally {
      writeUnlock();
    }
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
    assert newBlock.getBlockId()==oldBlock.getBlockId() : newBlock + " and "
    + oldBlock + " has different block identifier";
    LOG.info("updatePipeline(block=" + oldBlock
        + ", newGenerationStamp=" + newBlock.getGenerationStamp()
        + ", newLength=" + newBlock.getNumBytes()
        + ", newNodes=" + Arrays.asList(newNodes)
        + ", clientName=" + clientName
        + ")");

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
    DatanodeDescriptor[] descriptors = null;
    if (newNodes.length > 0) {
      descriptors = new DatanodeDescriptor[newNodes.length];
      for(int i = 0; i < newNodes.length; i++) {
        descriptors[i] = getDatanode(newNodes[i]);
      }
    }
    blockinfo.setExpectedLocations(descriptors);

    // persist blocks only if append is supported
    String src = leaseManager.findPath(pendingFile);
    if (supportAppends) {
      dir.persistBlocks(src, pendingFile);
      getEditLog().logSync();
    }
    LOG.info("updatePipeline(" + oldBlock + ") successfully to " + newBlock);
    return;
    } finally {
      writeUnlock();
    }
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  //
  void changeLease(String src, String dst, HdfsFileStatus dinfo) {
    String overwrite;
    String replaceBy;

    boolean destinationExisted = true;
    if (dinfo == null) {
      destinationExisted = false;
    }

    if (destinationExisted && dinfo.isDir()) {
      Path spath = new Path(src);
      overwrite = spath.getParent().toString() + Path.SEPARATOR;
      replaceBy = dst + Path.SEPARATOR;
    } else {
      overwrite = src;
      replaceBy = dst;
    }

    leaseManager.changeLease(src, dst, overwrite, replaceBy);
  }
           
  /**
   * Serializes leases. 
   */
  void saveFilesUnderConstruction(DataOutputStream out) throws IOException {
    synchronized (leaseManager) {
      out.writeInt(leaseManager.countPath()); // write the size

      for (Lease lease : leaseManager.getSortedLeases()) {
        for(String path : lease.getPaths()) {
          // verify that path exists in namespace
          INode node;
          try {
            node = dir.getFileINode(path);
          } catch (UnresolvedLinkException e) {
            throw new AssertionError("Lease files should reside on this FS");
          }
          if (node == null) {
            throw new IOException("saveLeases found path " + path +
                                  " but no matching entry in namespace.");
          }
          if (!node.isUnderConstruction()) {
            throw new IOException("saveLeases found path " + path +
                                  " but is not under construction.");
          }
          INodeFileUnderConstruction cons = (INodeFileUnderConstruction) node;
          FSImageSerialization.writeINodeUnderConstruction(out, cons, path);
        }
      }
    }
  }

  /**
   * Register a name-node.
   * <p>
   * Registration is allowed if there is no ongoing streaming to
   * another backup node.
   * We currently allow only one backup node, but multiple chackpointers 
   * if there are no backups.
   * 
   * @param registration
   * @throws IOException
   */
  void registerBackupNode(NamenodeRegistration registration)
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
      boolean regAllowed = getEditLog().checkBackupRegistration(registration);
      if(!regAllowed)
        throw new IOException("Registration is not allowed. " +
                              "Another node is registered as a backup.");
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

  public int numCorruptReplicas(Block blk) {
    return blockManager.numCorruptReplicas(blk);
  }

  /** Get a datanode descriptor given corresponding storageID */
  DatanodeDescriptor getDatanode(String nodeID) {
    return datanodeMap.get(nodeID);
  }

  /**
   * Return a range of corrupt replica block ids. Up to numExpectedBlocks 
   * blocks starting at the next block after startingBlockId are returned
   * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId 
   * is null, up to numExpectedBlocks blocks are returned from the beginning.
   * If startingBlockId cannot be found, null is returned.
   *
   * @param numExpectedBlocks Number of block ids to return.
   *  0 <= numExpectedBlocks <= 100
   * @param startingBlockId Block id from which to start. If null, start at
   *  beginning.
   * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
   *
   */
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {  
    return blockManager.getCorruptReplicaBlockIds(numExpectedBlocks,
                                                  startingBlockId);
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
      String startBlockAfter) throws IOException {

    readLock();
    try {
    if (!isPopulatingReplQueues()) {
      throw new IOException("Cannot run listCorruptFileBlocks because " +
                            "replication queues have not been initialized.");
    }
    checkSuperuserPrivilege();
    long startBlockId = 0;
    // print a limited # of corrupt files per call
    int count = 0;
    ArrayList<CorruptFileBlockInfo> corruptFiles = new ArrayList<CorruptFileBlockInfo>();
    
    if (startBlockAfter != null) {
      startBlockId = Block.filename2id(startBlockAfter);
    }
    BlockIterator blkIterator = blockManager.getCorruptReplicaBlockIterator();
    while (blkIterator.hasNext()) {
      Block blk = blkIterator.next();
      INode inode = blockManager.getINode(blk);
      if (inode != null && blockManager.countNodes(blk).liveReplicas() == 0) {
        String src = FSDirectory.getFullPathName(inode);
        if (((startBlockAfter == null) || (blk.getBlockId() > startBlockId))
            && (src.startsWith(path))) {
          corruptFiles.add(new CorruptFileBlockInfo(src, blk));
          count++;
          if (count >= DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED)
            break;
        }
      }
    }
    LOG.info("list corrupt file blocks returned: " + count);
    return corruptFiles;
    } finally {
      readUnlock();
    }
  }
  
  /**
   * @return list of datanodes where decommissioning is in progress
   */
  public ArrayList<DatanodeDescriptor> getDecommissioningNodes() {
    readLock();
    try {
    ArrayList<DatanodeDescriptor> decommissioningNodes = 
        new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> results = 
        getDatanodeListForReport(DatanodeReportType.LIVE);
    for (Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (node.isDecommissionInProgress()) {
        decommissioningNodes.add(node);
      }
    }
    return decommissioningNodes;
    } finally {
      readUnlock();
    }
  }

  /**
   * Create delegation token secret manager
   */
  private DelegationTokenSecretManager createDelegationTokenSecretManager(
      Configuration conf) {
    return new DelegationTokenSecretManager(conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT),
        conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT),
        conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT),
        DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL, this);
  }

  /**
   * Returns the DelegationTokenSecretManager instance in the namesystem.
   * @return delegation token secret manager object
   */
  public DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return dtSecretManager;
  }

  /**
   * @param renewer
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot issue delegation token", safeMode);
    }
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be issued only with kerberos or web authentication");
    }
    
    if(dtSecretManager == null || !dtSecretManager.isRunning()) {
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
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
        dtId, dtSecretManager);
    long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
    logGetDelegationToken(dtId, expiryTime);
    return token;
  }

  /**
   * 
   * @param token
   * @return New expiryTime of the token
   * @throws InvalidToken
   * @throws IOException
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot renew delegation token", safeMode);
    }
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be renewed only with kerberos or web authentication");
    }
    String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
    long expiryTime = dtSecretManager.renewToken(token, renewer);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    id.readFields(in);
    logRenewDelegationToken(id, expiryTime);
    return expiryTime;
  }

  /**
   * 
   * @param token
   * @throws IOException
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot cancel delegation token", safeMode);
    }
    String canceller = UserGroupInformation.getCurrentUser().getUserName();
    DelegationTokenIdentifier id = dtSecretManager
        .cancelToken(token, canceller);
    logCancelDelegationToken(id);
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
   * Log the getDelegationToken operation to edit logs
   * 
   * @param id identifer of the new delegation token
   * @param expiryTime when delegation token expires
   */
  private void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) throws IOException {
    writeLock();
    try {
      getEditLog().logGetDelegationToken(id, expiryTime);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }

  /**
   * Log the renewDelegationToken operation to edit logs
   * 
   * @param id identifer of the delegation token being renewed
   * @param expiryTime when delegation token expires
   */
  private void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) throws IOException {
    writeLock();
    try {
      getEditLog().logRenewDelegationToken(id, expiryTime);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }

  
  /**
   * Log the cancelDelegationToken operation to edit logs
   * 
   * @param id identifer of the delegation token being cancelled
   */
  private void logCancelDelegationToken(DelegationTokenIdentifier id)
      throws IOException {
    writeLock();
    try {
      getEditLog().logCancelDelegationToken(id);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }

  /**
   * Log the updateMasterKey operation to edit logs
   * 
   * @param key new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) throws IOException {
    writeLock();
    try {
      getEditLog().logUpdateMasterKey(key);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
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
   * If the remote IP for namenode method invokation is null, then the
   * invocation is internal to the namenode. Client invoked methods are invoked
   * over RPC and always have address != null.
   */
  private boolean isExternalInvocation() {
    return Server.getRemoteIp() != null;
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
    // register MXBean
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    try {
      ObjectName mxbeanName = new ObjectName("HadoopInfo:type=NameNodeInfo");
      mbs.registerMBean(this, mxbeanName);
    } catch ( javax.management.InstanceAlreadyExistsException iaee ) {
      // in unit tests, we may run and restart the NN within the same JVM
      LOG.info("NameNode MXBean already registered");
    } catch ( javax.management.JMException e ) {
      LOG.warn("Failed to register NameNodeMXBean", e);
    }
  }

  /**
   * Class representing Namenode information for JMX interfaces
   */
  @Override // NameNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
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
    return getCapacityUsedNonDFS();
  }

  @Override // NameNodeMXBean
  public float getPercentUsed() {
    return getCapacityUsedPercent();
  }

  @Override // NameNodeMXBean
  public long getBlockPoolUsedSpace() {
    synchronized(heartbeats) {
      return blockPoolUsed;
    }
  }

  @Override // NameNodeMXBean
  public float getPercentBlockPoolUsed() {
    synchronized(heartbeats) {
      return DFSUtil.getPercentUsed(blockPoolUsed, capacityTotal);
    }
  }

  @Override // NameNodeMXBean
  public float getPercentRemaining() {
    return getCapacityRemainingPercent();
  }

  @Override // NameNodeMXBean
  public long getTotalBlocks() {
    return getBlocksTotal();
  }

  @Override // NameNodeMXBean
  public long getTotalFiles() {
    return getFilesTotal();
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
    final Map<String, Object> info = new HashMap<String, Object>();
    final ArrayList<DatanodeDescriptor> aliveNodeList =
      this.getDatanodeListForReport(DatanodeReportType.LIVE); 
    for (DatanodeDescriptor node : aliveNodeList) {
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
    final Map<String, Object> info = new HashMap<String, Object>();
    final ArrayList<DatanodeDescriptor> deadNodeList =
      this.getDatanodeListForReport(DatanodeReportType.DEAD); 
    for (DatanodeDescriptor node : deadNodeList) {
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
    final Map<String, Object> info = new HashMap<String, Object>();
    final ArrayList<DatanodeDescriptor> decomNodeList = 
      this.getDecommissioningNodes();
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
}
