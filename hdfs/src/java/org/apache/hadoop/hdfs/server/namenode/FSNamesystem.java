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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.AccessTokenHandler;
import org.apache.hadoop.hdfs.security.ExportedAccessKeys;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.Util;
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
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

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
public class FSNamesystem implements FSConstants, FSNamesystemMBean, FSClusterStats {
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

  static int BLOCK_DELETION_INCREMENT = 1000;
  private boolean isPermissionEnabled;
  private UserGroupInformation fsOwner;
  private String supergroup;
  private PermissionStatus defaultPermission;
  // FSNamesystemMetrics counter variables
  private FSNamesystemMetrics myFSMetrics;
  private long capacityTotal = 0L, capacityUsed = 0L, capacityRemaining = 0L;
  private int totalLoad = 0;
  boolean isAccessTokenEnabled;
  AccessTokenHandler accessTokenHandler;
  private long accessKeyUpdateInterval;
  private long accessTokenLifetime;
  
  // Scan interval is not configurable.
  private final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL = 3600000; // 1 hour
  private DelegationTokenSecretManager dtSecretManager;

  //
  // Stores the correct file name hierarchy
  //
  public FSDirectory dir;

  BlockManager blockManager;
    
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
  int blockInvalidateLimit = FSConstants.BLOCK_INVALIDATE_CHUNK;

  // precision of access times.
  private long accessTimePrecision = 0;

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
    this.hostsReader = new HostsFileReader(conf.get("dfs.hosts",""),
                        conf.get("dfs.hosts.exclude",""));
    if (isAccessTokenEnabled) {
      accessTokenHandler = new AccessTokenHandler(true,
          accessKeyUpdateInterval, accessTokenLifetime);
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
        conf.getInt("dfs.namenode.decommission.interval", 30),
        conf.getInt("dfs.namenode.decommission.nodes.per.interval", 5)));
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
  }

  public static Collection<URI> getNamespaceDirs(Configuration conf) {
    return getStorageDirs(conf, DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
  }

  public static Collection<URI> getStorageDirs(Configuration conf,
                                                String propertyName) {
    Collection<String> dirNames = conf.getStringCollection(propertyName);
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
      Collection<String> dirNames2 = cE.getStringCollection(propertyName);
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

  /**
   * dirs is a list of directories where the filesystem directory state 
   * is stored
   */
  FSNamesystem(FSImage fsImage, Configuration conf) throws IOException {
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
   * @param bnImage {@link BackupStorage}
   * @param conf configuration
   * @throws IOException
   */
  FSNamesystem(Configuration conf, BackupStorage bnImage) throws IOException {
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

    long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
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
        (short) conf.getInt("dfs.replication", DEFAULT_REPLICATION_FACTOR),
        conf.getInt("io.file.buffer.size", DEFAULT_FILE_BUFFER_SIZE));
    this.maxFsObjects = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY, 
                                     DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_DEFAULT);
    this.blockInvalidateLimit = Math.max(this.blockInvalidateLimit, 
                                         20*(int)(heartbeatInterval/1000));
    this.accessTimePrecision = conf.getLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 0);
    this.supportAppends = conf.getBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY,
                                      DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT);
    this.isAccessTokenEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    if (isAccessTokenEnabled) {
      this.accessKeyUpdateInterval = conf.getLong(
          DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 
          DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT) * 60 * 1000L; // 10 hrs
      this.accessTokenLifetime = conf.getLong(
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT) * 60 * 1000L; // 10 hrs
    }
    LOG.info("isAccessTokenEnabled=" + isAccessTokenEnabled
        + " accessKeyUpdateInterval=" + accessKeyUpdateInterval / (60 * 1000)
        + " min(s), accessTokenLifetime=" + accessTokenLifetime / (60 * 1000)
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
    return new NamespaceInfo(dir.fsImage.getNamespaceID(),
                             dir.fsImage.getCTime(),
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
  synchronized void metaSave(String filename) throws IOException {
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
  synchronized BlocksWithLocations getBlocks(DatanodeID datanode, long size)
      throws IOException {
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
  }

  /**
   * Get access keys
   * 
   * @return current access keys
   */
  ExportedAccessKeys getAccessKeys() {
    return isAccessTokenEnabled ? accessTokenHandler.exportKeys()
        : ExportedAccessKeys.DUMMY_KEYS;
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
  public synchronized void setPermission(String src, FsPermission permission)
      throws IOException, UnresolvedLinkException {
    if (isInSafeMode())
      throw new SafeModeException("Cannot set permission for " + src, safeMode);
    checkOwner(src);
    dir.setPermission(src, permission);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled()) {
      final HdfsFileStatus stat = dir.getFileInfo(src, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setPermission", src, null, stat);
    }
  }

  /**
   * Set owner for an existing file.
   * @throws IOException
   */
  public synchronized void setOwner(String src, String username, String group)
      throws IOException, UnresolvedLinkException {
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
    if (auditLog.isInfoEnabled()) {
      final HdfsFileStatus stat = dir.getFileInfo(src, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setOwner", src, null, stat);
    }
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String src,
      long offset, long length) throws IOException, UnresolvedLinkException {
    LocatedBlocks blocks = getBlockLocations(src, offset, length, true);
    if (blocks != null) {
      //sort the blocks
      DatanodeDescriptor client = host2DataNodeMap.getDatanodeByHost(
          clientMachine);
      for (LocatedBlock b : blocks.getLocatedBlocks()) {
        clusterMap.pseudoSortByDistance(client, b.getLocations());
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
      boolean doAccessTime) throws IOException, UnresolvedLinkException {
    if (isPermissionEnabled) {
      checkPathAccess(src, FsAction.READ);
    }

    if (offset < 0) {
      throw new IOException("Negative offset is not supported. File: " + src );
    }
    if (length < 0) {
      throw new IOException("Negative length is not supported. File: " + src );
    }
    final LocatedBlocks ret = getBlockLocationsInternal(src,
        offset, length, doAccessTime);  
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "open", src, null, null);
    }
    return ret;
  }

  private synchronized LocatedBlocks getBlockLocationsInternal(String src,
                                                       long offset, 
                                                       long length,
                                                       boolean doAccessTime)
      throws IOException, UnresolvedLinkException {
    INodeFile inode = dir.getFileINode(src);
    if (inode == null)
      throw new FileNotFoundException("File does not exist: " + src);
    assert !inode.isLink();
    if (doAccessTime && isAccessTimeSupported()) {
      dir.setTimes(src, inode, -1, now(), false);
    }
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

      if (!last.isComplete()) {
        return new LocatedBlocks(n, inode.isUnderConstruction(), locatedblocks,
            blockManager.getBlockLocation(last, n), false);
      }
      else {
        return new LocatedBlocks(n, inode.isUnderConstruction(), locatedblocks,
            blockManager.getBlockLocation(last, n-last.getNumBytes()), true);
      }
    }
  }

  /** Create a LocatedBlock. */
  LocatedBlock createLocatedBlock(final Block b, final DatanodeInfo[] locations,
      final long offset, final boolean corrupt) throws IOException {
    final LocatedBlock lb = new LocatedBlock(b, locations, offset, corrupt);
    if (isAccessTokenEnabled) {
      lb.setAccessToken(accessTokenHandler.generateToken(b.getBlockId(),
          EnumSet.of(AccessTokenHandler.AccessMode.READ)));
    }
    return lb;
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
    FSNamesystem.LOG.debug("concat " + Arrays.toString(srcs) + " to " + target);
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
    
    synchronized(this) {
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

      long blockSize = trgInode.preferredBlockSize;

      // check the end block to be full
      if(blockSize != trgInode.blocks[trgInode.blocks.length-1].getNumBytes()) {
        throw new IllegalArgumentException(target + " blocks size should be the same");
      }

      si.add(trgInode);
      short repl = trgInode.blockReplication;

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
        if(repl != srcInode.blockReplication) {
          throw new IllegalArgumentException(src + " and " + target + " " +
              "should have same replication: "
              + repl + " vs. " + srcInode.blockReplication);
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

      NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " + 
          Arrays.toString(srcs) + " to " + target);

      dir.concatInternal(target,srcs);
    }
    getEditLog().logSync();
   
    
    if (auditLog.isInfoEnabled()) {
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
  public synchronized void setTimes(String src, long mtime, long atime) 
    throws IOException, UnresolvedLinkException {
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. " +
                            " Please set dfs.support.accessTime configuration parameter.");
    }
    //
    // The caller needs to have write access to set access & modification times.
    if (isPermissionEnabled) {
      checkPathAccess(src, FsAction.WRITE);
    }
    INodeFile inode = dir.getFileINode(src);
    if (inode != null) {
      dir.setTimes(src, inode, mtime, atime, true);
      if (auditLog.isInfoEnabled()) {
        final HdfsFileStatus stat = dir.getFileInfo(src, false);
        logAuditEvent(UserGroupInformation.getCurrentUser(),
                      Server.getRemoteIp(),
                      "setTimes", src, null, stat);
      }
    } else {
      throw new FileNotFoundException("File " + src + " does not exist.");
    }
  }

  /**
   * Create a symbolic link.
   */
  public synchronized void createSymlink(String target, String link,
      PermissionStatus dirPerms, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    if (!createParent) {
      verifyParentDir(link);
    }
    createSymlinkInternal(target, link, dirPerms, createParent);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled()) {
      final HdfsFileStatus stat = dir.getFileInfo(link, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "createSymlink", link, target, stat);
    }
  }

  /**
   * Create a symbolic link.
   */
  private synchronized void createSymlinkInternal(String target, String link,
      PermissionStatus dirPerms, boolean createParent)
      throws IOException, UnresolvedLinkException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target=" + 
        target + " link=" + link);
    }

    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create symlink " + link, safeMode);
    }
    if (!DFSUtil.isValidName(link)) {
      throw new IOException("Invalid file name: " + link);
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
  public boolean setReplication(String src, short replication) 
    throws IOException, UnresolvedLinkException {
    boolean status = setReplicationInternal(src, replication);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setReplication", src, null, null);
    }
    return status;
  }

  private synchronized boolean setReplicationInternal(String src, short replication) 
    throws IOException, UnresolvedLinkException {
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
  private void verifyParentDir(String src) throws FileAlreadyExistsException,
      FileNotFoundException, UnresolvedLinkException {
    Path parent = new Path(src).getParent();
    if (parent != null) {
      INode[] pathINodes = dir.getExistingPathINodes(parent.toString());
      INode parentNode = pathINodes[pathINodes.length - 1];
      if (parentNode == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent.toString());
      } else if (!parentNode.isDirectory() && !parentNode.isLink()) {
        throw new FileAlreadyExistsException("Parent path is not a directory: "
            + parent.toString());
      }
    }
  }

  /**
   * Create a new file entry in the namespace.
   * 
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   * 
   * @throws IOException if file name is invalid
   *         {@link FSDirectory#isValidToCreate(String)}.
   */
  void startFile(String src, PermissionStatus permissions,
                 String holder, String clientMachine,
                 EnumSet<CreateFlag> flag, boolean createParent, 
                 short replication, long blockSize)
      throws IOException, UnresolvedLinkException {
    startFileInternal(src, permissions, holder, clientMachine, flag,
        createParent, replication, blockSize);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled()) {
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
  private synchronized LocatedBlock startFileInternal(String src,
                                              PermissionStatus permissions,
                                              String holder, 
                                              String clientMachine, 
                                              EnumSet<CreateFlag> flag,
                                              boolean createParent,
                                              short replication,
                                              long blockSize)
      throws IOException, UnresolvedLinkException {
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
      throw new IOException("Invalid file name: " + src);
    }

    // Verify that the destination does not exist as a directory already.
    boolean pathExists = dir.exists(src);
    if (pathExists && dir.isDir(src)) {
      throw new IOException("Cannot create file "+ src + "; already exists as a directory.");
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
          if(pendingFile.getLastBlock().getBlockUCState() ==
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

        if (lb != null && isAccessTokenEnabled) {
          lb.setAccessToken(accessTokenHandler.generateToken(lb.getBlock()
              .getBlockId(), EnumSet.of(AccessTokenHandler.AccessMode.WRITE)));
        }
        /*if (lb != null && isBlockTokenEnabled) {
          lb.setBlockToken(blockTokenSecretManager.generateToken(lb.getBlock(), 
              EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
        }*/
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
    return null;
  }

  /**
   * Append to an existing file in the namespace.
   */
  LocatedBlock appendFile(String src, String holder, String clientMachine)
    throws IOException, UnresolvedLinkException {
    if (supportAppends == false) {
      throw new IOException("Append to hdfs not supported." +
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

    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "append", src, null, null);
    }
    return lb;
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
                                         Block previous,
                                         HashMap<Node, Node> excludedNodes
                                         ) 
      throws IOException, UnresolvedLinkException {
    long fileLength, blockSize;
    int replication;
    DatanodeDescriptor clientNode = null;
    Block newBlock = null;

    NameNode.stateChangeLog.debug("BLOCK* NameSystem.getAdditionalBlock: file "
                                  +src+" for "+clientName);

    synchronized (this) {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot add block to " + src, safeMode);
      }

      // have we exceeded the configured limit of fs objects.
      checkFsObjectLimit();

      INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);

      // commit the last block and complete it if it has minimum replicas
      blockManager.commitOrCompleteLastBlock(pendingFile, previous);

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
    synchronized (this) {
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
    }
        
    // Create next block
    LocatedBlock b = new LocatedBlock(newBlock, targets, fileLength);
    if (isAccessTokenEnabled) {
      b.setAccessToken(accessTokenHandler.generateToken(b.getBlock()
          .getBlockId(), EnumSet.of(AccessTokenHandler.AccessMode.WRITE)));
    }
    return b;
  }

  /**
   * The client would like to let go of the given block
   */
  public synchronized boolean abandonBlock(Block b, String src, String holder)
    throws IOException, UnresolvedLinkException {
    //
    // Remove the block from the pending creates list
    //
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                                  +b+"of file "+src);
    INodeFileUnderConstruction file = checkLease(src, holder);
    dir.removeBlock(src, file, b);
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                                    + b
                                    + " is removed from pendingCreates");
    return true;
  }
  
  // make sure that we still have the lease on this file.
  private INodeFileUnderConstruction checkLease(String src, String holder) 
    throws IOException, UnresolvedLinkException {
    INodeFile file = dir.getFileINode(src);
    checkLease(src, holder, file);
    return (INodeFileUnderConstruction)file;
  }

  private void checkLease(String src, String holder, INode file) 
                                                     throws IOException {

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
   * The FSNamesystem will already know the blocks that make up the file.
   * Before we return, we make sure that all the file's blocks have 
   * been reported by datanodes and are replicated correctly.
   */
  
  enum CompleteFileStatus {
    OPERATION_FAILED,
    STILL_WAITING,
    COMPLETE_SUCCESS
  }
  
  public CompleteFileStatus completeFile(String src, String holder, Block last) 
    throws IOException, UnresolvedLinkException {
    CompleteFileStatus status = completeFileInternal(src, holder, last);
    getEditLog().logSync();
    return status;
  }

  private synchronized CompleteFileStatus completeFileInternal(String src, 
    String holder, Block last) throws IOException, UnresolvedLinkException {
    NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " + src + " for " + holder);
    if (isInSafeMode())
      throw new SafeModeException("Cannot complete file " + src, safeMode);
    INode iFile = dir.getFileINode(src);
    INodeFileUnderConstruction pendingFile = null;
    Block[] fileBlocks = null;

    if (iFile != null && iFile.isUnderConstruction()) {
      pendingFile = (INodeFileUnderConstruction) iFile;
      fileBlocks =  dir.getFileBlocks(src);
    }
    if (fileBlocks == null ) {    
      NameNode.stateChangeLog.warn("DIR* NameSystem.completeFile: "
                                   + "failed to complete " + src
                                   + " because dir.getFileBlocks() is null " + 
                                   " and pendingFile is " + 
                                   ((pendingFile == null) ? "null" : 
                                     ("from " + pendingFile.getClientMachine()))
                                  );                      
      return CompleteFileStatus.OPERATION_FAILED;
    } 

    // commit the last block and complete it if it has minimum replicas
    blockManager.commitOrCompleteLastBlock(pendingFile, last);

    if (!checkFileProgress(pendingFile, true)) {
      return CompleteFileStatus.STILL_WAITING;
    }

    finalizeINodeFileUnderConstruction(src, pendingFile);

    NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " + src
                                  + " is closed by " + holder);
    return CompleteFileStatus.COMPLETE_SUCCESS;
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
   */
  private Block allocateBlock(String src,
                              INode[] inodes,
                              DatanodeDescriptor targets[]) throws IOException {
    Block b = new Block(FSNamesystem.randBlockId.nextLong(), 0, 0); 
    while(isValidBlock(b)) {
      b.setBlockId(FSNamesystem.randBlockId.nextLong());
    }
    b.setGenerationStamp(getGenerationStamp());
    b = dir.addBlock(src, inodes, b, targets);
    NameNode.stateChangeLog.info("BLOCK* NameSystem.allocateBlock: "
                                 +src+ ". "+b);
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  synchronized boolean checkFileProgress(INodeFile v, boolean checkall) throws IOException {
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
  }


  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   */
  public synchronized void markBlockAsCorrupt(Block blk, DatanodeInfo dn)
    throws IOException {
    blockManager.findAndMarkBlockAsCorrupt(blk, dn);
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
    if (status && auditLog.isInfoEnabled()) {
      final HdfsFileStatus stat = dir.getFileInfo(dst, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "rename", src, dst, stat);
    }
    return status;
  }

  /** @deprecated See {@link #renameTo(String, String)} */
  @Deprecated
  private synchronized boolean renameToInternal(String src, String dst)
    throws IOException, UnresolvedLinkException {
    NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src + " to " + dst);
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
  }
  

  /** Rename src to dst */
  void renameTo(String src, String dst, Options.Rename... options)
      throws IOException, UnresolvedLinkException {
    renameToInternal(src, dst, options);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled()) {
      StringBuilder cmd = new StringBuilder("rename options=");
      for (Rename option : options) {
        cmd.append(option.value()).append(" ");
      }
      final HdfsFileStatus stat = dir.getFileInfo(dst, false);
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(),
                    cmd.toString(), src, dst, stat);
    }
  }

  private synchronized void renameToInternal(String src, String dst,
      Options.Rename... options) throws IOException, UnresolvedLinkException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: with options - "
          + src + " to " + dst);
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }
    if (isPermissionEnabled) {
      checkParentAccess(src, FsAction.WRITE);
      checkAncestorAccess(dst, FsAction.WRITE);
    }

    HdfsFileStatus dinfo = dir.getFileInfo(dst, false);
    dir.renameTo(src, dst, options);
    changeLease(src, dst, dinfo); // update lease with new filename
  }
  
  /**
   * Remove the indicated filename from namespace. If the filename 
   * is a directory (non empty) and recursive is set to false then throw exception.
   */
    public boolean delete(String src, boolean recursive) 
      throws IOException, UnresolvedLinkException {
      if ((!recursive) && (!dir.isDirEmpty(src))) {
        throw new IOException(src + " is non empty");
      }
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
      }
      boolean status = deleteInternal(src, true);
      if (status && auditLog.isInfoEnabled()) {
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
   */
  private boolean deleteInternal(String src, boolean enforcePermission) 
    throws IOException, UnresolvedLinkException {
    boolean deleteNow = false;
    ArrayList<Block> collectedBlocks = new ArrayList<Block>();
    synchronized(this) {
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
      synchronized(this) {
        for (int i=start; i<end; i++) {
          blockManager.removeBlock(blocks.get(i));
        }
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
   * @throws IOException if permission to access file is denied by the system 
   * @throws UnresolvedLinkException if a symlink is encountered.
   * @return object containing information regarding the file
   *         or null if file not found
   */
  HdfsFileStatus getFileInfo(String src, boolean resolveLink) 
    throws IOException, UnresolvedLinkException {
    if (!DFSUtil.isValidName(src)) {
      throw new IOException("Invalid file name: " + src);
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
    if (status && auditLog.isInfoEnabled()) {
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
  private synchronized boolean mkdirsInternal(String src,
      PermissionStatus permissions, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
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
      throw new IOException("Invalid directory name: " + src);
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
      throw new IOException("Invalid directory name: " + src);
    }
    return true;
  }

  ContentSummary getContentSummary(String src) 
      throws IOException, UnresolvedLinkException {
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
    synchronized (this) {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot fsync file " + src, safeMode);
      }
      INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);
      dir.persistBlocks(src, pendingFile);
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

  synchronized void commitBlockSynchronization(Block lastblock,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets)
      throws IOException, UnresolvedLinkException {
    LOG.info("commitBlockSynchronization(lastblock=" + lastblock
          + ", newgenerationstamp=" + newgenerationstamp
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets)
          + ", closeFile=" + closeFile
          + ", deleteBlock=" + deleteblock
          + ")");
    final BlockInfo storedBlock = blockManager.getStoredBlock(lastblock);
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
      pendingFile.removeLastBlock(lastblock);
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
    String src = leaseManager.findPath(pendingFile);
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
   * @return a partial listing starting after startAfter
   */
  public DirectoryListing getListing(String src, byte[] startAfter) 
    throws IOException, UnresolvedLinkException {
    if (isPermissionEnabled) {
      if (dir.isDir(src)) {
        checkPathAccess(src, FsAction.READ_EXECUTE);
      }
      else {
        checkTraverse(src);
      }
    }
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "listStatus", src, null, null);
    }
    return dir.getListing(src, startAfter);
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
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode#register()
   */
  public synchronized void registerDatanode(DatanodeRegistration nodeReg
                                            ) throws IOException {
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
    nodeReg.exportedKeys = getAccessKeys();
      
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
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.registerDatanode: "
                                      + "node restarted.");
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
      
      // resolve network location
      resolveNetworkLocation(nodeS);
      clusterMap.add(nodeS);
        
      // also treat the registration message as a heartbeat
      synchronized(heartbeats) {
        if( !heartbeats.contains(nodeS)) {
          heartbeats.add(nodeS);
          //update its timestamp
          nodeS.updateHeartbeat(0L, 0L, 0L, 0);
          nodeS.isAlive = true;
        }
      }
      return;
    } 

    // this is a new datanode serving a new data storage
    if (nodeReg.getStorageID().equals("")) {
      // this data storage has never been registered
      // it is either empty or was created by pre-storageID version of DFS
      nodeReg.storageID = newStorageID();
      NameNode.stateChangeLog.debug(
                                    "BLOCK* NameSystem.registerDatanode: "
                                    + "new storageID " + nodeReg.getStorageID() + " assigned.");
    }
    // register new datanode
    DatanodeDescriptor nodeDescr 
      = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK, hostName);
    resolveNetworkLocation(nodeDescr);
    unprotectedAddDatanode(nodeDescr);
    clusterMap.add(nodeDescr);
      
    // also treat the registration message as a heartbeat
    synchronized(heartbeats) {
      heartbeats.add(nodeDescr);
      nodeDescr.isAlive = true;
      // no need to update its timestamp
      // because its is done when the descriptor is created
    }
    return;
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
    return Storage.getRegistrationID(dir.fsImage);
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
      long capacity, long dfsUsed, long remaining,
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
        if (nodeinfo != null && shouldNodeShutdown(nodeinfo)) {
          setDatanodeDead(nodeinfo);
          throw new DisallowedDatanodeException(nodeinfo);
        }

        if (nodeinfo == null || !nodeinfo.isAlive) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }

        updateStats(nodeinfo, false);
        nodeinfo.updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
        updateStats(nodeinfo, true);
        
        //check lease recovery
        cmd = nodeinfo.getLeaseRecoveryCommand(Integer.MAX_VALUE);
        if (cmd != null) {
          return new DatanodeCommand[] {cmd};
        }
      
        ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>(3);
        //check pending replication
        cmd = nodeinfo.getReplicationCommand(
              blockManager.maxReplicationStreams - xmitsInProgress);
        if (cmd != null) {
          cmds.add(cmd);
        }
        //check block invalidation
        cmd = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
        if (cmd != null) {
          cmds.add(cmd);
        }
        // check access key update
        if (isAccessTokenEnabled && nodeinfo.needKeyUpdate) {
          cmds.add(new KeyUpdateCommand(accessTokenHandler.exportKeys()));
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
    //
    assert(Thread.holdsLock(heartbeats));
    if (isAdded) {
      capacityTotal += node.getCapacity();
      capacityUsed += node.getDfsUsed();
      capacityRemaining += node.getRemaining();
      totalLoad += node.getXceiverCount();
    } else {
      capacityTotal -= node.getCapacity();
      capacityUsed -= node.getDfsUsed();
      capacityRemaining -= node.getRemaining();
      totalLoad -= node.getXceiverCount();
    }
  }

  /**
   * Update access keys.
   */
  void updateAccessKey() throws IOException {
    this.accessTokenHandler.updateKeys();
    synchronized (heartbeats) {
      for (DatanodeDescriptor nodeInfo : heartbeats) {
        nodeInfo.needKeyUpdate = true;
      }
    }
  }

  /**
   * Periodically calls heartbeatCheck() and updateAccessKey()
   */
  class HeartbeatMonitor implements Runnable {
    private long lastHeartbeatCheck;
    private long lastAccessKeyUpdate;
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
          if (isAccessTokenEnabled && (lastAccessKeyUpdate + accessKeyUpdateInterval < now)) {
            updateAccessKey();
            lastAccessKeyUpdate = now;
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
    synchronized (this) {
      blockManager.updateState();
      blockManager.scheduledReplicationBlocksCount = workFound;
    }
    
    workFound += blockManager.computeInvalidateWork(nodesToProcess);
    return workFound;
  }

  public void setNodeReplicationLimit(int limit) {
    blockManager.maxReplicationStreams = limit;
  }

  /**
   * remove a datanode descriptor
   * @param nodeID datanode ID
   */
  synchronized public void removeDatanode(DatanodeID nodeID) 
    throws IOException {
    DatanodeDescriptor nodeInfo = getDatanode(nodeID);
    if (nodeInfo != null) {
      removeDatanode(nodeInfo);
    } else {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.removeDatanode: "
                                   + nodeID.getName() + " does not exist");
    }
  }
  
  /**
   * remove a datanode descriptor
   * @param nodeInfo datanode descriptor
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
  }

  void unprotectedRemoveDatanode(DatanodeDescriptor nodeDescr) {
    nodeDescr.resetBlocks();
    blockManager.removeFromInvalidates(nodeDescr.getStorageID());
    NameNode.stateChangeLog.debug(
                                  "BLOCK* NameSystem.unprotectedRemoveDatanode: "
                                  + nodeDescr.getName() + " is out of service now.");
  }
    
  void unprotectedAddDatanode(DatanodeDescriptor nodeDescr) {
    /* To keep host2DataNodeMap consistent with datanodeMap,
       remove  from host2DataNodeMap the datanodeDescriptor removed
       from datanodeMap before adding nodeDescr to host2DataNodeMap.
    */
    host2DataNodeMap.remove(
                            datanodeMap.put(nodeDescr.getStorageID(), nodeDescr));
    host2DataNodeMap.add(nodeDescr);
      
    NameNode.stateChangeLog.debug(
                                  "BLOCK* NameSystem.unprotectedAddDatanode: "
                                  + "node " + nodeDescr.getName() + " is added to datanodeMap.");
  }

  /**
   * Physically remove node from datanodeMap.
   * 
   * @param nodeID node
   */
  void wipeDatanode(DatanodeID nodeID) throws IOException {
    String key = nodeID.getStorageID();
    host2DataNodeMap.remove(datanodeMap.remove(key));
    NameNode.stateChangeLog.debug(
                                  "BLOCK* NameSystem.wipeDatanode: "
                                  + nodeID.getName() + " storage " + key 
                                  + " is removed from datanodeMap.");
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
        synchronized (this) {
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
        }
      }
      allAlive = !foundDead;
    }
  }
    
  /**
   * The given node is reporting all its blocks.  Use this info to 
   * update the (machine-->blocklist) and (block-->machinelist) tables.
   */
  public synchronized void processReport(DatanodeID nodeID, 
                                         BlockListAsLongs newReport
                                        ) throws IOException {
    long startTime = now();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: "
                             + "from " + nodeID.getName()+" " + 
                             newReport.getNumberOfBlocks()+" blocks");
    }
    DatanodeDescriptor node = getDatanode(nodeID);
    if (node == null) {
      throw new IOException("ProcessReport from unregisterted node: "
                            + nodeID.getName());
    }

    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(node)) {
      setDatanodeDead(node);
      throw new DisallowedDatanodeException(node);
    }
    
    blockManager.processReport(node, newReport);
    NameNode.getNameNodeMetrics().blockReport.inc((int) (now() - startTime));
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
  public synchronized void blockReceived(DatanodeID nodeID,  
                                         Block block,
                                         String delHint
                                         ) throws IOException {
    DatanodeDescriptor node = getDatanode(nodeID);
    if (node == null) {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: "
                                   + block + " is received from an unrecorded node " 
                                   + nodeID.getName());
      throw new IllegalArgumentException(
                                         "Unexpected exception.  Got blockReceived message from node " 
                                         + block + ", but there is no info for it");
    }
        
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockReceived: "
                                    +block+" is received from " + nodeID.getName());
    }

    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(node)) {
      setDatanodeDead(node);
      throw new DisallowedDatanodeException(node);
    }

    blockManager.addBlock(node, block, delHint);
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
                         getMissingBlocksCount()};
    }
  }

  /**
   * Total raw bytes including non-dfs used space.
   */
  public long getCapacityTotal() {
    return getStats()[0];
  }

  /**
   * Total used space by data nodes
   */
  public long getCapacityUsed() {
    return getStats()[1];
  }
  /**
   * Total used space by data nodes as percentage of total capacity
   */
  public float getCapacityUsedPercent() {
    synchronized(heartbeats){
      if (capacityTotal <= 0) {
        return 100;
      }

      return ((float)capacityUsed * 100.0f)/(float)capacityTotal;
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
    return getStats()[2];
  }

  /**
   * Total remaining space by data nodes as percentage of total capacity
   */
  public float getCapacityRemainingPercent() {
    synchronized(heartbeats){
      if (capacityTotal <= 0) {
        return 0;
      }

      return ((float)capacityRemaining * 100.0f)/(float)capacityTotal;
    }
  }
  /**
   * Total number of connections.
   */
  public int getTotalLoad() {
    synchronized (heartbeats) {
      return this.totalLoad;
    }
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    return getDatanodeListForReport(type).size(); 
  }

  private synchronized ArrayList<DatanodeDescriptor> getDatanodeListForReport(
                                                      DatanodeReportType type) {                  
    
    boolean listLiveNodes = type == DatanodeReportType.ALL ||
                            type == DatanodeReportType.LIVE;
    boolean listDeadNodes = type == DatanodeReportType.ALL ||
                            type == DatanodeReportType.DEAD;

    HashMap<String, String> mustList = new HashMap<String, String>();
    
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
  }

  public synchronized DatanodeInfo[] datanodeReport( DatanodeReportType type
      ) throws AccessControlException {
    checkSuperuserPrivilege();

    ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(type);
    DatanodeInfo[] arr = new DatanodeInfo[results.size()];
    for (int i=0; i<arr.length; i++) {
      arr[i] = new DatanodeInfo(results.get(i));
    }
    return arr;
  }

  /**
   * Save namespace image.
   * This will save current namespace into fsimage file and empty edits file.
   * Requires superuser privilege and safe mode.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   * @throws IOException if 
   */
  synchronized void saveNamespace() throws AccessControlException, IOException {
    checkSuperuserPrivilege();
    if(!isInSafeMode()) {
      throw new IOException("Safe mode should be turned ON " +
                            "in order to create namespace image.");
    }
    getFSImage().saveNamespace(true);
    LOG.info("New namespace image has been created.");
  }
  
  /**
   * Enables/Disables/Checks restoring failed storage replicas if the storage becomes available again.
   * Requires superuser privilege.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   */
  synchronized boolean restoreFailedStorage(String arg) throws AccessControlException {
    checkSuperuserPrivilege();
    
    // if it is disabled - enable it and vice versa.
    if(arg.equals("check"))
      return getFSImage().getRestoreFailedStorage();
    
    boolean val = arg.equals("true");  // false if not
    getFSImage().setRestoreFailedStorage(val);
    
    return val;
  }

  /**
   */
  public synchronized void DFSNodesStatus(ArrayList<DatanodeDescriptor> live, 
                                          ArrayList<DatanodeDescriptor> dead) {

    ArrayList<DatanodeDescriptor> results = 
                            getDatanodeListForReport(DatanodeReportType.ALL);    
    for(Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (isDatanodeDead(node))
        dead.add(node);
      else
        live.add(node);
    }
  }

  /**
   * Prints information about all datanodes.
   */
  private synchronized void datanodeDump(PrintWriter out) {
    synchronized (datanodeMap) {
      out.println("Metasave: Number of datanodes: " + datanodeMap.size());
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext();) {
        DatanodeDescriptor node = it.next();
        out.println(node.dumpDatanode());
      }
    }
  }

  /**
   * Start decommissioning the specified datanode. 
   */
  private void startDecommission (DatanodeDescriptor node) 
    throws IOException {

    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      LOG.info("Start Decommissioning node " + node.getName());
      node.startDecommission();
      node.decommissioningStatus.setStartTime(now());
      //
      // all the blocks that reside on this node have to be 
      // replicated.
      checkDecommissionStateInternal(node);
    }
  }

  /**
   * Stop decommissioning the specified datanodes.
   */
  public void stopDecommission (DatanodeDescriptor node) 
    throws IOException {
    LOG.info("Stop Decommissioning node " + node.getName());
    node.stopDecommission();
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
    if (node.isDecommissioned()) {
      return true;
    }
    return false;
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
    hostsReader.updateFileNames(conf.get("dfs.hosts",""), 
                                conf.get("dfs.hosts.exclude", ""));
    hostsReader.refresh();
    synchronized (this) {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
           it.hasNext();) {
        DatanodeDescriptor node = it.next();
        // Check if not include.
        if (!inHostsList(node, null)) {
          node.setDecommissioned();  // case 2.
        } else {
          if (inExcludedHostsList(node, null)) {
            if (!node.isDecommissionInProgress() && 
                !node.isDecommissioned()) {
              startDecommission(node);   // case 3.
            }
          } else {
            if (node.isDecommissionInProgress() || 
                node.isDecommissioned()) {
              stopDecommission(node);   // case 4.
            } 
          }
        }
      }
    } 
      
  }
    
  void finalizeUpgrade() throws IOException {
    checkSuperuserPrivilege();
    getFSImage().finalizeUpgrade();
  }

  /**
   * Checks if the node is not on the hosts list.  If it is not, then
   * it will be ignored.  If the node is in the hosts list, but is also 
   * on the exclude list, then it will be decommissioned.
   * Returns FALSE if node is rejected for registration. 
   * Returns TRUE if node is registered (including when it is on the 
   * exclude list and is being decommissioned). 
   */
  private synchronized boolean verifyNodeRegistration(DatanodeID nodeReg, String ipAddr) 
    throws IOException {
    if (!inHostsList(nodeReg, ipAddr)) {
      return false;    
    }
    if (inExcludedHostsList(nodeReg, ipAddr)) {
      DatanodeDescriptor node = getDatanode(nodeReg);
      if (node == null) {
        throw new IOException("verifyNodeRegistration: unknown datanode " +
                              nodeReg.getName());
      }
      if (!checkDecommissionStateInternal(node)) {
        startDecommission(node);
      }
    } 
    return true;
  }
    
  /**
   * Checks if the Admin state bit is DECOMMISSIONED.  If so, then 
   * we should shut it down. 
   * 
   * Returns true if the node should be shutdown.
   */
  private boolean shouldNodeShutdown(DatanodeDescriptor node) {
    return (node.isDecommissioned());
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
    /** Safe mode extension after the threshold. */
    private int extension;
    /** Min replication required by safe mode. */
    private int safeReplication;
      
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
    /** time of the last status printout */
    private long lastStatusReport = 0;
      
    /**
     * Creates SafeModeInfo when the name node enters
     * automatic safe mode at startup.
     *  
     * @param conf configuration
     */
    SafeModeInfo(Configuration conf) {
      this.threshold = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 0.95f);
      this.extension = conf.getInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
      this.safeReplication = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 
                                         DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
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
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
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
      // verify blocks replications
      blockManager.processMisReplicatedBlocks();
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
      return threshold != 0 && blockSafe < blockThreshold;
    }
      
    /**
     * Check and trigger safe mode if needed. 
     */
    private void checkMode() {
      if (needEnter()) {
        enter();
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
    }
      
    /**
     * Set total number of blocks.
     */
    synchronized void setBlockTotal(int total) {
      this.blockTotal = total;
      this.blockThreshold = (int) (blockTotal * threshold);
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
      
      String msg = null;
      if (reached == 0) {
        msg = String.format("The reported blocks %d needs additional %d"
            + " blocks to reach the threshold %.4f of total blocks %d. %s",
            blockSafe, (blockThreshold - blockSafe), threshold, blockTotal,
            leaveMsg);
      } else {
        msg = String.format("The reported blocks %d has reached the threshold"
            + " %.4f of total blocks %d. %s", blockSafe, threshold, 
            blockTotal, leaveMsg);
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
      // leave safe mode and stop the monitor
      try {
        leaveSafeMode(true);
      } catch(SafeModeException es) { // should never happen
        String msg = "SafeModeMonitor may not run during distributed upgrade.";
        assert false : msg;
        throw new RuntimeException(msg, es);
      }
      smmthread = null;
    }
  }
    
  /**
   * Current system time.
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
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
  boolean isInSafeMode() {
    if (safeMode == null)
      return false;
    return safeMode.isOn();
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
  synchronized void enterSafeMode() throws IOException {
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
  }

  /**
   * Leave safe mode.
   * @throws IOException
   */
  synchronized void leaveSafeMode(boolean checkForUpgrades) throws SafeModeException {
    if (!isInSafeMode()) {
      NameNode.stateChangeLog.info("STATE* Safe mode is already OFF."); 
      return;
    }
    if(getDistributedUpgradeState())
      throw new SafeModeException("Distributed upgrade is in progress",
                                  safeMode);
    safeMode.leave(checkForUpgrades);
  }
    
  synchronized String getSafeModeTip() {
    if (!isInSafeMode())
      return "";
    return safeMode.getTurnOffTip();
  }

  long getEditLogSize() throws IOException {
    return getEditLog().getEditLogSize();
  }

  synchronized CheckpointSignature rollEditLog() throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Checkpoint not created",
                                  safeMode);
    }
    LOG.info("Roll Edit Log from " + Server.getRemoteAddress());
    return getFSImage().rollEditLog();
  }

  synchronized void rollFSImage() throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Checkpoint not created",
                                  safeMode);
    }
    LOG.info("Roll FSImage from " + Server.getRemoteAddress());
    getFSImage().rollFSImage();
  }

  synchronized NamenodeCommand startCheckpoint(
                                NamenodeRegistration bnReg, // backup node
                                NamenodeRegistration nnReg) // active name-node
  throws IOException {
    LOG.info("Start checkpoint for " + bnReg.getAddress());
    NamenodeCommand cmd = getFSImage().startCheckpoint(bnReg, nnReg);
    getEditLog().logSync();
    return cmd;
  }

  synchronized void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    LOG.info("End checkpoint for " + registration.getAddress());
    getFSImage().endCheckpoint(sig, registration.getRole());
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
      pc.checkPermission(path, dir.rootDir, doCheckOwner,
          ancestorAccess, parentAccess, access, subAccess);
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

  public long getFilesTotal() {
    return this.dir.totalInodes();
  }

  public long getPendingReplicationBlocks() {
    return blockManager.pendingReplicationBlocksCount;
  }

  public long getUnderReplicatedBlocks() {
    return blockManager.underReplicatedBlocksCount;
  }

  /** Returns number of blocks with corrupt replicas */
  public long getCorruptReplicaBlocks() {
    return blockManager.corruptReplicaBlocksCount;
  }

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
      e.printStackTrace();
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

  private INodeFileUnderConstruction checkUCBlock(Block block, String clientName) 
  throws IOException {
    // check safe mode
    if (isInSafeMode())
      throw new SafeModeException("Cannot get a new generation stamp and an " +
                                "access token for block " + block, safeMode);
    
    // check stored block state
    BlockInfo storedBlock = blockManager.getStoredBlock(block);
    if (storedBlock == null || 
        storedBlock.getBlockUCState() != BlockUCState.UNDER_CONSTRUCTION) {
        throw new IOException(block + 
            " does not exist or is not under Construction" + storedBlock);
    }
    
    // check file inode
    INodeFile file = storedBlock.getINode();
    if (file==null || !file.isUnderConstruction()) {
      throw new IOException("The file " + storedBlock + 
          " is belonged to does not exist or it is not under construction.");
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
  synchronized LocatedBlock updateBlockForPipeline(Block block, 
      String clientName) throws IOException {
    // check vadility of parameters
    checkUCBlock(block, clientName);
    
    // get a new generation stamp and an access token
    block.setGenerationStamp(nextGenerationStamp());
    LocatedBlock locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
    if (isAccessTokenEnabled) {
      locatedBlock.setAccessToken(accessTokenHandler.generateToken(
          block.getBlockId(), EnumSet.of(AccessTokenHandler.AccessMode.WRITE)));
    }
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
  synchronized void updatePipeline(String clientName, Block oldBlock, 
      Block newBlock, DatanodeID[] newNodes)
      throws IOException {
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
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  //
  void changeLease(String src, String dst, HdfsFileStatus dinfo) 
                   throws IOException {
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
          FSImage.writeINodeUnderConstruction(out, cons, path);
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
  synchronized void registerBackupNode(NamenodeRegistration registration)
  throws IOException {
    if(getFSImage().getNamespaceID() != registration.getNamespaceID())
      throw new IOException("Incompatible namespaceIDs: " 
          + " Namenode namespaceID = " + getFSImage().getNamespaceID() 
          + "; " + registration.getRole() +
              " node namespaceID = " + registration.getNamespaceID());
    boolean regAllowed = getEditLog().checkBackupRegistration(registration);
    if(!regAllowed)
      throw new IOException("Registration is not allowed. " +
          "Another node is registered as a backup.");
  }

  /**
   * Release (unregister) backup node.
   * <p>
   * Find and remove the backup stream corresponding to the node.
   * @param registration
   * @throws IOException
   */
  synchronized void releaseBackupNode(NamenodeRegistration registration)
  throws IOException {
    if(getFSImage().getNamespaceID() != registration.getNamespaceID())
      throw new IOException("Incompatible namespaceIDs: " 
          + " Namenode namespaceID = " + getFSImage().getNamespaceID() 
          + "; " + registration.getRole() +
              " node namespaceID = " + registration.getNamespaceID());
    getEditLog().releaseBackupStream(registration);
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

  /**
   * @return Array of FileStatus objects representing files with 
   * corrupted blocks.
   * @throws AccessControlException
   * @throws IOException
   */
  synchronized FileStatus[] getCorruptFiles() 
    throws AccessControlException, IOException {
    
    checkSuperuserPrivilege();
    
    INode[] inodes = blockManager.getCorruptInodes();
    FileStatus[] ret = new FileStatus[inodes.length];
    
    int i = 0;
    for (INode inode: inodes) {
      String src = inode.getFullPathName();
      ret[i++] = new FileStatus(inode.computeContentSummary().getLength(), 
          inode.isDirectory(), 
          ((INodeFile)inode).getReplication(), 
          ((INodeFile)inode).getPreferredBlockSize(),
          inode.getModificationTime(),
          inode.getAccessTime(),
          inode.getFsPermission(),
          inode.getUserName(),
          inode.getGroupName(),
          new Path(src));
    }

    return ret;
  }
  
  public synchronized ArrayList<DatanodeDescriptor> getDecommissioningNodes() {
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
  }

  /*
   * Delegation Token
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
    String canceller = UserGroupInformation.getCurrentUser().getShortUserName();
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
    synchronized (this) {
      getEditLog().logGetDelegationToken(id, expiryTime);
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
    synchronized (this) {
      getEditLog().logRenewDelegationToken(id, expiryTime);
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
    synchronized (this) {
      getEditLog().logCancelDelegationToken(id);
    }
    getEditLog().logSync();
  }

  /**
   * Log the updateMasterKey operation to edit logs
   * 
   * @param key new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) throws IOException {
    synchronized (this) {
      getEditLog().logUpdateMasterKey(key);
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
}
