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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMetrics;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.AccessTokenHandler;
import org.apache.hadoop.security.ExportedAccessKeys;
import org.apache.hadoop.security.PermissionChecker;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.*;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.security.auth.login.LoginException;

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
public class FSNamesystem implements FSConstants, FSNamesystemMBean {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);
  public static final String AUDIT_FORMAT =
    "ugi=%s\t" +  // ugi
    "ip=%s\t" +   // remote IP
    "cmd=%s\t" +  // command
    "src=%s\t" +  // src path
    "dst=%s\t" +  // dst path (optional)
    "perm=%s";    // permissions (optional)

  private static final ThreadLocal<Formatter> auditFormatter =
    new ThreadLocal<Formatter>() {
      protected Formatter initialValue() {
        return new Formatter(new StringBuilder(AUDIT_FORMAT.length() * 4));
      }
  };

  private static final void logAuditEvent(UserGroupInformation ugi,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus stat) {
    final Formatter fmt = auditFormatter.get();
    ((StringBuilder)fmt.out()).setLength(0);
    auditLog.info(fmt.format(AUDIT_FORMAT, ugi, addr, cmd, src, dst,
                  (stat == null)
                    ? null
                    : stat.getOwner() + ':' + stat.getGroup() + ':' +
                      stat.getPermission()
          ).toString());

  }

  public static final Log auditLog = LogFactory.getLog(
      FSNamesystem.class.getName() + ".audit");

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
  // default block size of a file
  private long defaultBlockSize = 0;
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
  private void initialize(Configuration conf, FSImage fsImage) throws IOException {
    this.systemStart = now();
    this.blockManager = new BlockManager(this, conf);
    setConfigurationParameters(conf);
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
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
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

  public static Collection<File> getNamespaceDirs(Configuration conf) {
    return getStorageDirs(conf, "dfs.name.dir");
  }

  public static Collection<File> getStorageDirs(Configuration conf,
                                                String propertyName) {
    Collection<String> dirNames = conf.getStringCollection(propertyName);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if(startOpt == StartupOption.IMPORT) {
      // In case of IMPORT this will get rid of default directories 
      // but will retain directories specified in hdfs-site.xml
      // When importing image from a checkpoint, the name-node can
      // start with empty set of storage directories.
      Configuration cE = new Configuration(false);
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
      dirNames.add("/tmp/hadoop/dfs/name");
    Collection<File> dirs = new ArrayList<File>(dirNames.size());
    for(String name : dirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }

  public static Collection<File> getNamespaceEditsDirs(Configuration conf) {
    return getStorageDirs(conf, "dfs.name.edits.dir");
  }

  /**
   * dirs is a list of directories where the filesystem directory state 
   * is stored
   */
  FSNamesystem(FSImage fsImage, Configuration conf) throws IOException {
    this.blockManager = new BlockManager(this, conf);
    setConfigurationParameters(conf);
    this.dir = new FSDirectory(fsImage, this, conf);
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
    try {
      fsOwner = UnixUserGroupInformation.login(conf);
    } catch (LoginException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
    LOG.info("fsOwner=" + fsOwner);

    this.supergroup = conf.get("dfs.permissions.supergroup", "supergroup");
    this.isPermissionEnabled = conf.getBoolean("dfs.permissions", true);
    LOG.info("supergroup=" + supergroup);
    LOG.info("isPermissionEnabled=" + isPermissionEnabled);
    short filePermission = (short)conf.getInt("dfs.upgrade.permission", 00777);
    this.defaultPermission = PermissionStatus.createImmutable(
        fsOwner.getUserName(), supergroup, new FsPermission(filePermission));

    long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
    this.heartbeatRecheckInterval = conf.getInt(
        "heartbeat.recheck.interval", 5 * 60 * 1000); // 5 minutes
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval +
      10 * heartbeatInterval;
    this.replicationRecheckInterval = 
      conf.getInt("dfs.replication.interval", 3) * 1000L;
    this.defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    this.maxFsObjects = conf.getLong("dfs.max.objects", 0);
    this.blockInvalidateLimit = Math.max(this.blockInvalidateLimit, 
                                         20*(int)(heartbeatInterval/1000));
    this.accessTimePrecision = conf.getLong("dfs.access.time.precision", 0);
    this.supportAppends = conf.getBoolean("dfs.support.append", false);
    this.isAccessTokenEnabled = conf.getBoolean(
        AccessTokenHandler.STRING_ENABLE_ACCESS_TOKEN, false);
    if (isAccessTokenEnabled) {
      this.accessKeyUpdateInterval = conf.getLong(
          AccessTokenHandler.STRING_ACCESS_KEY_UPDATE_INTERVAL, 600) * 60 * 1000L; // 10 hrs
      this.accessTokenLifetime = conf.getLong(
          AccessTokenHandler.STRING_ACCESS_TOKEN_LIFETIME, 600) * 60 * 1000L; // 10 hrs
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
   * Test for a thread ref not being null or pointing to a dead thread
   * @param thread the thread to check
   * @return true if the thread is considered dead
   */
  private boolean isDead(Thread thread) {
      return thread == null || !thread.isAlive();
  }

  /**
   * Perform a cursory health check of the namesystem, particulary that it has
   * not been closed and that all threads are running.
   * @throws IOException for any health check
   */
  void ping() throws IOException {
    if (!fsRunning) {
      throw new IOException("Namesystem is not running");
    }
    boolean bad = false;
    StringBuilder sb = new StringBuilder();
    if (isDead(hbthread)) {
      bad = true;
      sb.append("[Heartbeat thread is dead]");
    }
    if (isDead(replthread)) {
      bad = true;
      sb.append("[Replication thread is dead]");
    }
    // this thread's liveness is only relevant in safe mode.
    if (safeMode!=null && isDead(smmthread)) {
      bad = true;
      sb.append("[SafeModeMonitor thread is dead while the name system is in safe mode]");
    }
    if (isDead(dnthread)) {
        bad = true;
        sb.append("[DecommissionedMonitor thread is dead]");
    }
    if (isDead(lmthread)) {
      bad = true;
      sb.append("[Lease monitor thread is dead]");
    }
    if (bad) {
      throw new IOException(sb.toString());
    }
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
    } catch (Exception e) {
      LOG.warn("Exception shutting down FSNamesystem", e);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        if (lmthread != null) {
          lmthread.interrupt();
          lmthread.join(3000);
        }
        if(dir != null) {
         dir.close();
         dir =  null;
        }
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
    File file = new File(System.getProperty("hadoop.log.dir"), 
                         filename);
    PrintWriter out = new PrintWriter(new BufferedWriter(
                                                         new FileWriter(file, true)));

    blockManager.metaSave(out);

    //
    // Dump all datanodes
    //
    datanodeDump(out);

    out.flush();
    out.close();
  }

  long getDefaultBlockSize() {
    return defaultBlockSize;
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
    Iterator<Block> iter = node.getBlockIterator();
    int startBlock = r.nextInt(numBlocks); // starting from a random block
    // skip blocks
    for(int i=0; i<startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    long totalSize = 0;
    while(totalSize<size && iter.hasNext()) {
      totalSize += addBlock(iter.next(), results);
    }
    if(totalSize<size) {
      iter = node.getBlockIterator(); // start from the beginning
      for(int i=0; i<startBlock&&totalSize<size; i++) {
        totalSize += addBlock(iter.next(), results);
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
    ArrayList<String> machineSet = blockManager.addBlock(block);
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
  public synchronized void setPermission(String src, FsPermission permission
      ) throws IOException {
    checkOwner(src);
    dir.setPermission(src, permission);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled()) {
      final FileStatus stat = dir.getFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "setPermission", src, null, stat);
    }
  }

  /**
   * Set owner for an existing file.
   * @throws IOException
   */
  public synchronized void setOwner(String src, String username, String group
      ) throws IOException {
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
      final FileStatus stat = dir.getFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "setOwner", src, null, stat);
    }
  }

  /**
   * Get block locations within the specified range.
   * 
   * @see #getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String src,
      long offset, long length) throws IOException {
    if (isPermissionEnabled) {
      checkPathAccess(src, FsAction.READ);
    }

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
   */
  public LocatedBlocks getBlockLocations(String src, long offset, long length
      ) throws IOException {
    return getBlockLocations(src, offset, length, false);
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @throws FileNotFoundException
   */
  public LocatedBlocks getBlockLocations(String src, long offset, long length,
      boolean doAccessTime) throws IOException {
    if (offset < 0) {
      throw new IOException("Negative offset is not supported. File: " + src );
    }
    if (length < 0) {
      throw new IOException("Negative length is not supported. File: " + src );
    }
    INodeFile inode = dir.getFileINode(src);
    if (inode == null)
      throw new FileNotFoundException();
    final LocatedBlocks ret = getBlockLocationsInternal(src, inode,
        offset, length, Integer.MAX_VALUE, doAccessTime);  
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "open", src, null, null);
    }
    return ret;
  }

  private synchronized LocatedBlocks getBlockLocationsInternal(String src,
                                                       INodeFile inode,
                                                       long offset, 
                                                       long length,
                                                       int nrBlocksToReturn,
                                                       boolean doAccessTime
                                                       ) throws IOException {
    if(inode == null) {
      return null;
    }
    if (doAccessTime && isAccessTimeSupported()) {
      dir.setTimes(src, inode, -1, now(), false);
    }
    Block[] blocks = inode.getBlocks();
    if (blocks == null) {
      return null;
    }
    if (blocks.length == 0) {
      return inode.createLocatedBlocks(new ArrayList<LocatedBlock>(blocks.length));
    }
    
    List<LocatedBlock> results = blockManager.getBlockLocations(blocks,
        offset, length, nrBlocksToReturn);
    return inode.createLocatedBlocks(results);
  }

  /**
   * stores the modification and access time for this inode. 
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  public synchronized void setTimes(String src, long mtime, long atime) throws IOException {
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
        final FileStatus stat = dir.getFileInfo(src);
        logAuditEvent(UserGroupInformation.getCurrentUGI(),
                      Server.getRemoteIp(),
                      "setTimes", src, null, stat);
      }
    } else {
      throw new FileNotFoundException("File " + src + " does not exist.");
    }
  }

  /**
   * Set replication for an existing file.
   * 
   * The NameNode sets new replication and schedules either replication of 
   * under-replicated data blocks or removal of the eccessive block copies 
   * if the blocks are over-replicated.
   * 
   * @see ClientProtocol#setReplication(String, short)
   * @param src file name
   * @param replication new replication
   * @return true if successful; 
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(String src, short replication) 
                                throws IOException {
    boolean status = setReplicationInternal(src, replication);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "setReplication", src, null, null);
    }
    return status;
  }

  private synchronized boolean setReplicationInternal(String src, 
                                             short replication
                                             ) throws IOException {
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
    
  long getPreferredBlockSize(String filename) throws IOException {
    if (isPermissionEnabled) {
      checkTraverse(filename);
    }
    return dir.getPreferredBlockSize(filename);
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
                 EnumSet<CreateFlag> flag, short replication, long blockSize
                ) throws IOException {
    startFileInternal(src, permissions, holder, clientMachine, flag,
        replication, blockSize);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled()) {
      final FileStatus stat = dir.getFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "create", src, null, stat);
    }
  }

  private synchronized void startFileInternal(String src,
                                              PermissionStatus permissions,
                                              String holder, 
                                              String clientMachine, 
                                              EnumSet<CreateFlag> flag,
                                              short replication,
                                              long blockSize
                                              ) throws IOException {
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean append = flag.contains(CreateFlag.APPEND);
    boolean create = flag.contains(CreateFlag.CREATE);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src
          + ", holder=" + holder
          + ", clientMachine=" + clientMachine
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

    try {
      INode myFile = dir.getFileINode(src);
      if (myFile != null && myFile.isUnderConstruction()) {
        INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) myFile;
        //
        // If the file is under construction , then it must be in our
        // leases. Find the appropriate lease record.
        //
        Lease lease = leaseManager.getLease(holder);
        //
        // We found the lease for this file. And surprisingly the original
        // holder is trying to recreate this file. This should never occur.
        //
        if (lease != null) {
          throw new AlreadyBeingCreatedException(
                                                 "failed to create file " + src + " for " + holder +
                                                 " on client " + clientMachine + 
                                                 " because current leaseholder is trying to recreate file.");
        }
        //
        // Find the original holder.
        //
        lease = leaseManager.getLease(pendingFile.clientName);
        if (lease == null) {
          throw new AlreadyBeingCreatedException(
                                                 "failed to create file " + src + " for " + holder +
                                                 " on client " + clientMachine + 
                                                 " because pendingCreates is non-null but no leases found.");
        }
        //
        // If the original holder has not renewed in the last SOFTLIMIT 
        // period, then start lease recovery.
        //
        if (lease.expiredSoftLimit()) {
          LOG.info("startFile: recover lease " + lease + ", src=" + src);
          internalReleaseLease(lease, src);
        }
        throw new AlreadyBeingCreatedException("failed to create file " + src + " for " + holder +
                                               " on client " + clientMachine + 
                                               ", because this file is already being created by " +
                                               pendingFile.getClientName() + 
                                               " on " + pendingFile.getClientMachine());
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
            this.startFileInternal(src, permissions, holder, clientMachine,
                EnumSet.of(CreateFlag.OVERWRITE), replication, blockSize);
            return;
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
        leaseManager.addLease(cons.clientName, src);

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
        leaseManager.addLease(newNode.clientName, src);
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
  }

  /**
   * Append to an existing file in the namespace.
   */
  LocatedBlock appendFile(String src, String holder, String clientMachine
      ) throws IOException {
    if (supportAppends == false) {
      throw new IOException("Append to hdfs not supported." +
                            " Please refer to dfs.support.append configuration parameter.");
    }
    startFileInternal(src, null, holder, clientMachine, EnumSet.of(CreateFlag.APPEND), 
                      (short)blockManager.maxReplication, (long)0);
    getEditLog().logSync();

    //
    // Create a LocatedBlock object for the last block of the file
    // to be returned to the client. Return null if the file does not
    // have a partial block at the end.
    //
    LocatedBlock lb = null;
    synchronized (this) {
      INodeFileUnderConstruction file = (INodeFileUnderConstruction)dir.getFileINode(src);

      Block[] blocks = file.getBlocks();
      if (blocks != null && blocks.length > 0) {
        Block last = blocks[blocks.length-1];
        BlockInfo storedBlock = blockManager.getStoredBlock(last);
        if (file.getPreferredBlockSize() > storedBlock.getNumBytes()) {
          long fileLength = file.computeContentSummary().getLength();
          DatanodeDescriptor[] targets = blockManager.getNodes(last);
          // remove the replica locations of this block from the node
          for (int i = 0; i < targets.length; i++) {
            targets[i].removeBlock(storedBlock);
          }
          // set the locations of the last block in the lease record
          file.setLastBlock(storedBlock, targets);

          lb = new LocatedBlock(last, targets, 
                                fileLength-storedBlock.getNumBytes());
          if (isAccessTokenEnabled) {
            lb.setAccessToken(accessTokenHandler.generateToken(lb.getBlock()
                .getBlockId(), EnumSet.of(AccessTokenHandler.AccessMode.WRITE)));
          }

          // Remove block from replication queue.
          blockManager.updateNeededReplications(last, 0, 0);

          // remove this block from the list of pending blocks to be deleted. 
          // This reduces the possibility of triggering HADOOP-1349.
          //
          for (DatanodeDescriptor dd : targets) {
            String datanodeId = dd.getStorageID();
            blockManager.removeFromInvalidates(datanodeId, last);
          }
        }
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

    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
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
                                         String clientName
                                         ) throws IOException {
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
        replication, clientNode, null, blockSize);
    if (targets.length < blockManager.minReplication) {
      throw new IOException("File " + src + " could only be replicated to " +
                            targets.length + " nodes, instead of " +
                            blockManager.minReplication
                            + ". ( there are currently " 
                            + heartbeats.size()
                            +" live data nodes in the cluster)");
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
      newBlock = allocateBlock(src, pathINodes);
      pendingFile.setTargets(targets);
      
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
  public synchronized boolean abandonBlock(Block b, String src, String holder
      ) throws IOException {
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
                                                      throws IOException {
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
  
  public CompleteFileStatus completeFile(String src, String holder) throws IOException {
    CompleteFileStatus status = completeFileInternal(src, holder);
    getEditLog().logSync();
    return status;
  }


  private synchronized CompleteFileStatus completeFileInternal(String src, 
                                                String holder) throws IOException {
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
    } else if (!checkFileProgress(pendingFile, true)) {
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
  private Block allocateBlock(String src, INode[] inodes) throws IOException {
    Block b = new Block(FSNamesystem.randBlockId.nextLong(), 0, 0); 
    while(isValidBlock(b)) {
      b.setBlockId(FSNamesystem.randBlockId.nextLong());
    }
    b.setGenerationStamp(getGenerationStamp());
    b = dir.addBlock(src, inodes, b);
    NameNode.stateChangeLog.info("BLOCK* NameSystem.allocateBlock: "
                                 +src+ ". "+b);
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  synchronized boolean checkFileProgress(INodeFile v, boolean checkall) {
    if (checkall) {
      //
      // check all blocks of the file.
      //
      for (Block block: v.getBlocks()) {
        if (!blockManager.checkMinReplication(block)) {
          return false;
        }
      }
    } else {
      //
      // check the penultimate block of this file
      //
      Block b = v.getPenultimateBlock();
      if (b != null && !blockManager.checkMinReplication(b)) {
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
    blockManager.markBlockAsCorrupt(blk, dn);
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

  /** Change the indicated filename. */
  public boolean renameTo(String src, String dst) throws IOException {
    boolean status = renameToInternal(src, dst);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled()) {
      final FileStatus stat = dir.getFileInfo(dst);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "rename", src, dst, stat);
    }
    return status;
  }

  private synchronized boolean renameToInternal(String src, String dst
      ) throws IOException {
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

    FileStatus dinfo = dir.getFileInfo(dst);
    if (dir.renameTo(src, dst)) {
      changeLease(src, dst, dinfo);     // update lease with new filename
      return true;
    }
    return false;
  }

  /**
   * Remove the indicated filename from namespace. If the filename 
   * is a directory (non empty) and recursive is set to false then throw exception.
   */
    public boolean delete(String src, boolean recursive) throws IOException {
      if ((!recursive) && (!dir.isDirEmpty(src))) {
        throw new IOException(src + " is non empty");
      }
      boolean status = deleteInternal(src, true);
      getEditLog().logSync();
      if (status && auditLog.isInfoEnabled()) {
        logAuditEvent(UserGroupInformation.getCurrentUGI(),
                      Server.getRemoteIp(),
                      "delete", src, null, null);
      }
      return status;
    }
    
  /**
   * Remove the indicated filename from the namespace.  This may
   * invalidate some blocks that make up the file.
   */
  synchronized boolean deleteInternal(String src, 
      boolean enforcePermission) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }
    if (isInSafeMode())
      throw new SafeModeException("Cannot delete " + src, safeMode);
    if (enforcePermission && isPermissionEnabled) {
      checkPermission(src, false, null, FsAction.WRITE, null, FsAction.ALL);
    }

    return dir.delete(src) != null;
  }

  void removePathAndBlocks(String src, List<Block> blocks) {
    leaseManager.removeLeaseWithPrefixPath(src);
    for(Block b : blocks) {
      blockManager.removeBlock(b);
    }
  }

  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @throws IOException if permission to access file is denied by the system 
   * @return object containing information regarding the file
   *         or null if file not found
   */
  FileStatus getFileInfo(String src) throws IOException {
    if (isPermissionEnabled) {
      checkTraverse(src);
    }
    return dir.getFileInfo(src);
  }

  /**
   * Create all the necessary directories
   */
  public boolean mkdirs(String src, PermissionStatus permissions
      ) throws IOException {
    boolean status = mkdirsInternal(src, permissions);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled()) {
      final FileStatus stat = dir.getFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "mkdirs", src, null, stat);
    }
    return status;
  }
    
  /**
   * Create all the necessary directories
   */
  private synchronized boolean mkdirsInternal(String src,
      PermissionStatus permissions) throws IOException {
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

    // validate that we have enough inodes. This is, at best, a 
    // heuristic because the mkdirs() operation migth need to 
    // create multiple inodes.
    checkFsObjectLimit();

    if (!dir.mkdirs(src, permissions, false, now())) {
      throw new IOException("Invalid directory name: " + src);
    }
    return true;
  }

  ContentSummary getContentSummary(String src) throws IOException {
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
  void setQuota(String path, long nsQuota, long dsQuota) throws IOException {
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
  void fsync(String src, String clientName) throws IOException {

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
   */
  void internalReleaseLease(Lease lease, String src) throws IOException {
    LOG.info("Recovering lease=" + lease + ", src=" + src);

    INodeFile iFile = dir.getFileINode(src);
    if (iFile == null) {
      final String message = "DIR* NameSystem.internalReleaseCreate: "
        + "attempt to release a create lock on "
        + src + " file does not exist.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
    if (!iFile.isUnderConstruction()) {
      final String message = "DIR* NameSystem.internalReleaseCreate: "
        + "attempt to release a create lock on "
        + src + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) iFile;

    // Initialize lease recovery for pendingFile. If there are no blocks 
    // associated with this file, then reap lease immediately. Otherwise 
    // renew the lease and trigger lease recovery.
    if (pendingFile.getTargets() == null ||
        pendingFile.getTargets().length == 0) {
      if (pendingFile.getBlocks().length == 0) {
        finalizeINodeFileUnderConstruction(src, pendingFile);
        NameNode.stateChangeLog.warn("BLOCK*"
          + " internalReleaseLease: No blocks found, lease removed.");
        return;
      }
      // setup the Inode.targets for the last block from the blockManager
      //
      Block[] blocks = pendingFile.getBlocks();
      Block last = blocks[blocks.length-1];
      DatanodeDescriptor[] targets = blockManager.getNodes(last);
      pendingFile.setTargets(targets);
    }
    // start lease recovery of the last block for this file.
    pendingFile.assignPrimaryDatanode();
    leaseManager.renewLease(lease);
  }

  private void finalizeINodeFileUnderConstruction(String src,
      INodeFileUnderConstruction pendingFile) throws IOException {
    leaseManager.removeLease(pendingFile.clientName, src);

    // The file is no longer pending.
    // Create permanent INode, update blockmap
    INodeFile newFile = pendingFile.convertToInodeFile();
    dir.replaceNode(src, pendingFile, newFile);

    // close file and persist block allocations for this file
    dir.closeFile(src, newFile);

    checkReplicationFactor(newFile);
  }

  synchronized void commitBlockSynchronization(Block lastblock,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
      ) throws IOException {
    LOG.info("commitBlockSynchronization(lastblock=" + lastblock
          + ", newgenerationstamp=" + newgenerationstamp
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets)
          + ", closeFile=" + closeFile
          + ", deleteBlock=" + deleteblock
          + ")");
    final BlockInfo oldblockinfo = blockManager.getStoredBlock(lastblock);
    if (oldblockinfo == null) {
      throw new IOException("Block (=" + lastblock + ") not found");
    }
    INodeFile iFile = oldblockinfo.getINode();
    if (!iFile.isUnderConstruction()) {
      throw new IOException("Unexpected block (=" + lastblock
          + ") since the file (=" + iFile.getLocalName()
          + ") is not under construction");
    }
    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)iFile;


    // Remove old block from blocks map. This always have to be done
    // because the generation stamp of this block is changing.
    blockManager.removeBlockFromMap(oldblockinfo);

    if (deleteblock) {
      pendingFile.removeBlock(lastblock);
    }
    else {
      // update last block, construct newblockinfo and add it to the blocks map
      lastblock.set(lastblock.getBlockId(), newlength, newgenerationstamp);
      final BlockInfo newblockinfo = blockManager.addINode(lastblock, pendingFile);

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
          descriptors[i].addBlock(newblockinfo);
        }
        pendingFile.setLastBlock(newblockinfo, null);
      } else {
        // add locations into the INodeUnderConstruction
        pendingFile.setLastBlock(newblockinfo, descriptors);
      }
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
   * Get a listing of all files at 'src'.  The Object[] array
   * exists so we can return file attributes (soon to be implemented)
   */
  public FileStatus[] getListing(String src) throws IOException {
    if (isPermissionEnabled) {
      if (dir.isDir(src)) {
        checkPathAccess(src, FsAction.READ_EXECUTE);
      }
      else {
        checkTraverse(src);
      }
    }
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "listStatus", src, null, null);
    }
    return dir.getListing(src);
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

    for (Iterator<Block> it = nodeInfo.getBlockIterator(); it.hasNext();) {
      blockManager.removeStoredBlock(it.next(), nodeInfo);
    }
    unprotectedRemoveDatanode(nodeInfo);
    clusterMap.remove(nodeInfo);
  }

  void unprotectedRemoveDatanode(DatanodeDescriptor nodeDescr) {
    nodeDescr.resetBlocks();
    blockManager.removeFromInvalidates(nodeDescr);
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
                              DatanodeDescriptor delNodeHint) {
    // first form a rack to datanodes map and
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
      long minSpace = Long.MAX_VALUE;

      // check if we can del delNodeHint
      if (firstOne && delNodeHint !=null && nonExcess.contains(delNodeHint) &&
            (priSet.contains(delNodeHint) || (addedNode != null && !priSet.contains(addedNode))) ) {
          cur = delNodeHint;
      } else { // regular excessive replica removal
        Iterator<DatanodeDescriptor> iter = 
          priSet.isEmpty() ? remains.iterator() : priSet.iterator();
          while( iter.hasNext() ) {
            DatanodeDescriptor node = iter.next();
            long free = node.getRemaining();

            if (minSpace > free) {
              minSpace = free;
              cur = node;
            }
          }
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
                         getCorruptReplicaBlocksCount(),
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
    getFSImage().saveFSImage();
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
      //
      // all the blocks that reside on this node have to be 
      // replicated.
      Iterator<Block> decommissionBlocks = node.getBlockIterator();
      while(decommissionBlocks.hasNext()) {
        Block block = decommissionBlocks.next();
        blockManager.updateNeededReplications(block, -1, 0);
      }
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
      conf = new Configuration();
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
      this.threshold = conf.getFloat("dfs.safemode.threshold.pct", 0.95f);
      this.extension = conf.getInt("dfs.safemode.extension", 0);
      this.safeReplication = conf.getInt("dfs.replication.min", 1);
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
    safeMode.setBlockTotal((int)getBlocksTotal());
  }

  /**
   * Get the total number of blocks in the system. 
   */
  public long getBlocksTotal() {
    return blockManager.getTotalBlocks();
  }

  /**
   * Enter safe mode manually.
   * @throws IOException
   */
  synchronized void enterSafeMode() throws IOException {
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

  NamenodeCommand startCheckpoint(NamenodeRegistration bnReg, // backup node
                                  NamenodeRegistration nnReg) // active name-node
  throws IOException {
    NamenodeCommand cmd;
    synchronized(this) {
      cmd = getFSImage().startCheckpoint(bnReg, nnReg);
    }
    LOG.info("Start checkpoint for " + bnReg.getAddress());
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
    return new PermissionStatus(fsOwner.getUserName(), supergroup, permission);
  }

  private FSPermissionChecker checkOwner(String path) throws AccessControlException {
    return checkPermission(path, true, null, null, null, null);
  }

  private FSPermissionChecker checkPathAccess(String path, FsAction access
      ) throws AccessControlException {
    return checkPermission(path, false, null, null, access, null);
  }

  private FSPermissionChecker checkParentAccess(String path, FsAction access
      ) throws AccessControlException {
    return checkPermission(path, false, null, access, null, null);
  }

  private FSPermissionChecker checkAncestorAccess(String path, FsAction access
      ) throws AccessControlException {
    return checkPermission(path, false, access, null, null, null);
  }

  private FSPermissionChecker checkTraverse(String path
      ) throws AccessControlException {
    return checkPermission(path, false, null, null, null, null);
  }

  private void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      PermissionChecker.checkSuperuserPrivilege(fsOwner, supergroup);
    }
  }

  /**
   * Check whether current user have permissions to access the path.
   * For more details of the parameters, see
   * {@link FSPermissionChecker#checkPermission(String, INodeDirectory, boolean, FsAction, FsAction, FsAction, FsAction)}.
   */
  private FSPermissionChecker checkPermission(String path, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess) throws AccessControlException {
    FSPermissionChecker pc = new FSPermissionChecker(
        fsOwner.getUserName(), supergroup);
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
  public long getCorruptReplicaBlocksCount() {
    return blockManager.corruptReplicaBlocksCount;
  }

  public long getScheduledReplicationBlocks() {
    return blockManager.scheduledReplicationBlocksCount;
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

  /**
   * Verifies that the block is associated with a file that has a lease.
   * Increments, logs and then returns the stamp
   */
  synchronized long nextGenerationStampForBlock(Block block) throws IOException {
    BlockInfo storedBlock = blockManager.getStoredBlock(block);
    if (storedBlock == null) {
      String msg = block + " is already commited, storedBlock == null.";
      LOG.info(msg);
      throw new IOException(msg);
    }
    INodeFile fileINode = storedBlock.getINode();
    if (!fileINode.isUnderConstruction()) {
      String msg = block + " is already commited, !fileINode.isUnderConstruction().";
      LOG.info(msg);
      throw new IOException(msg);
    }
    if (!((INodeFileUnderConstruction)fileINode).setLastRecoveryTime(now())) {
      String msg = block + " is beening recovered, ignoring this request.";
      LOG.info(msg);
      throw new IOException(msg);
    }
    return nextGenerationStamp();
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  //
  void changeLease(String src, String dst, FileStatus dinfo) 
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
          INode node = dir.getFileINode(path);
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
}
