/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.client.ServerConnectionManager;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HMaster is the "master server" for HBase. An HBase cluster has one active
 * master.  If many masters are started, all compete.  Whichever wins goes on to
 * run the cluster.  All others park themselves in their constructor until
 * master or cluster shutdown or until the active master loses its lease in
 * zookeeper.  Thereafter, all running master jostle to take over master role.
 * @see HMasterInterface
 * @see HMasterRegionInterface
 * @see Watcher
 */
public class HMaster extends Thread implements HConstants, HMasterInterface,
    HMasterRegionInterface, Watcher {
  // MASTER is name of the webapp and the attribute name used stuffing this
  //instance into web context.
  public static final String MASTER = "master";
  private static final Log LOG = LogFactory.getLog(HMaster.class.getName());

  // We start out with closed flag on.  Its set to off after construction.
  // Use AtomicBoolean rather than plain boolean because we want other threads
  // able to set shutdown flag.  Using AtomicBoolean can pass a reference
  // rather than have them have to know about the hosting Master class.
  final AtomicBoolean closed = new AtomicBoolean(true);
  // TODO: Is this separate flag necessary?
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  private final Configuration conf;
  private final Path rootdir;
  private InfoServer infoServer;
  private final int threadWakeFrequency;
  private final int numRetries;

  // Metrics is set when we call run.
  private final MasterMetrics metrics;

  final Lock splitLogLock = new ReentrantLock();

  // Our zk client.
  private ZooKeeperWrapper zooKeeperWrapper;
  // Watcher for master address and for cluster shutdown.
  private final ZKMasterAddressWatcher zkMasterAddressWatcher;
  // A Sleeper that sleeps for threadWakeFrequency; sleep if nothing todo.
  private final Sleeper sleeper;
  // Keep around for convenience.
  private final FileSystem fs;
  // Is the fileystem ok?
  private volatile boolean fsOk = true;
  // The Path to the old logs dir
  private final Path oldLogDir;

  private final HBaseServer rpcServer;
  private final HServerAddress address;

  private final ServerConnection connection;
  private final ServerManager serverManager;
  private final RegionManager regionManager;

  private long lastFragmentationQuery = -1L;
  private Map<String, Integer> fragmentation = null;
  private final RegionServerOperationQueue regionServerOperationQueue;

  /**
   * Constructor
   * @param conf configuration
   * @throws IOException
   */
  public HMaster(Configuration conf) throws IOException {
    this.conf = conf;
    // Set filesystem to be that of this.rootdir else we get complaints about
    // mismatched filesystems if hbase.rootdir is hdfs and fs.defaultFS is
    // default localfs.  Presumption is that rootdir is fully-qualified before
    // we get to here with appropriate fs scheme.
    this.rootdir = FSUtils.getRootDir(this.conf);
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    this.conf.set("fs.default.name", this.rootdir.toString());
    this.conf.set("fs.defaultFS", this.rootdir.toString());
    this.fs = FileSystem.get(this.conf);
    checkRootDir(this.rootdir, this.conf, this.fs);

    // Make sure the region servers can archive their old logs
    this.oldLogDir = new Path(this.rootdir, HREGION_OLDLOGDIR_NAME);
    if(!this.fs.exists(this.oldLogDir)) {
      this.fs.mkdirs(this.oldLogDir);
    }

    // Get my address and create an rpc server instance.  The rpc-server port
    // can be ephemeral...ensure we have the correct info
    HServerAddress a = new HServerAddress(getMyAddress(this.conf));
    this.rpcServer = HBaseRPC.getServer(this, a.getBindAddress(),
      a.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
      false, conf);
    this.address = new HServerAddress(this.rpcServer.getListenerAddress());

    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.threadWakeFrequency = conf.getInt(THREAD_WAKE_FREQUENCY, 10 * 1000);

    this.sleeper = new Sleeper(this.threadWakeFrequency, this.closed);
    this.connection = ServerConnectionManager.getConnection(conf);

    // Get our zookeeper wrapper and then try to write our address to zookeeper.
    // We'll succeed if we are only  master or if we win the race when many
    // masters.  Otherwise we park here inside in writeAddressToZooKeeper.
    // TODO: Bring up the UI to redirect to active Master.
    this.zooKeeperWrapper = new ZooKeeperWrapper(conf, this);
    this.zkMasterAddressWatcher =
      new ZKMasterAddressWatcher(this.zooKeeperWrapper, this.shutdownRequested);
    this.zkMasterAddressWatcher.writeAddressToZooKeeper(this.address, true);
    this.regionServerOperationQueue =
      new RegionServerOperationQueue(this.conf, this.closed);

    serverManager = new ServerManager(this);
    regionManager = new RegionManager(this);

    setName(MASTER);
    this.metrics = new MasterMetrics(MASTER);
    // We're almost open for business
    this.closed.set(false);
    LOG.info("HMaster initialized on " + this.address.toString());
  }

  /*
   * Get the rootdir.  Make sure its wholesome and exists before returning.
   * @param rd
   * @param conf
   * @param fs
   * @return hbase.rootdir (after checks for existence and bootstrapping if
   * needed populating the directory with necessary bootup files).
   * @throws IOException
   */
  private static Path checkRootDir(final Path rd, final Configuration c,
    final FileSystem fs)
  throws IOException {
    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, c.getInt(THREAD_WAKE_FREQUENCY, 10 * 1000));
    // Filesystem is good. Go ahead and check for hbase.rootdir.
    if (!fs.exists(rd)) {
      fs.mkdirs(rd);
      FSUtils.setVersion(fs, rd);
    } else {
      FSUtils.checkVersion(fs, rd, true);
    }
    // Make sure the root region directory exists!
    if (!FSUtils.rootRegionExists(fs, rd)) {
      bootstrap(rd, c);
    }
    return rd;
  }

  private static void bootstrap(final Path rd, final Configuration c)
  throws IOException {
    LOG.info("BOOTSTRAP: creating ROOT and first META regions");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo rootHRI = new HRegionInfo(HRegionInfo.ROOT_REGIONINFO);
      setInfoFamilyCaching(rootHRI, false);
      HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
      setInfoFamilyCaching(metaHRI, false);
      HRegion root = HRegion.createHRegion(rootHRI, rd, c);
      HRegion meta = HRegion.createHRegion(metaHRI, rd, c);
      setInfoFamilyCaching(rootHRI, true);
      setInfoFamilyCaching(metaHRI, true);
      // Add first region from the META table to the ROOT region.
      HRegion.addRegionToMETA(root, meta);
      root.close();
      root.getLog().closeAndDelete();
      meta.close();
      meta.getLog().closeAndDelete();
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  /*
   * @param hri Set all family block caching to <code>b</code>
   * @param b
   */
  private static void setInfoFamilyCaching(final HRegionInfo hri, final boolean b) {
    for (HColumnDescriptor hcd: hri.getTableDesc().families.values()) {
      if (Bytes.equals(hcd.getName(), HConstants.CATALOG_FAMILY)) {
        hcd.setBlockCacheEnabled(b);
        hcd.setInMemory(b);
      }
    }
  }

  /*
   * @return This masters' address.
   * @throws UnknownHostException
   */
  private static String getMyAddress(final Configuration c)
  throws UnknownHostException {
    // Find out our address up in DNS.
    String s = DNS.getDefaultHost(c.get("hbase.master.dns.interface","default"),
      c.get("hbase.master.dns.nameserver","default"));
    s += ":" + c.get(MASTER_PORT, Integer.toString(DEFAULT_MASTER_PORT));
    return s;
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  protected boolean checkFileSystem() {
    if (this.fsOk) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
      } catch (IOException e) {
        LOG.fatal("Shutting down HBase cluster: file system not available", e);
        this.closed.set(true);
        this.fsOk = false;
      }
    }
    return this.fsOk;
  }

  /** @return HServerAddress of the master server */
  public HServerAddress getMasterAddress() {
    return this.address;
  }

  public long getProtocolVersion(String protocol, long clientVersion) {
    return HBaseRPCProtocolVersion.versionID;
  }

  /** @return InfoServer object. Maybe null.*/
  public InfoServer getInfoServer() {
    return this.infoServer;
  }

  /**
   * @return HBase root dir.
   * @throws IOException
   */
  public Path getRootDir() {
    return this.rootdir;
  }

  public int getNumRetries() {
    return this.numRetries;
  }

  /**
   * @return Server metrics
   */
  public MasterMetrics getMetrics() {
    return this.metrics;
  }

  /**
   * @return Return configuration being used by this server.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  public ServerManager getServerManager() {
    return this.serverManager;
  }

  public RegionManager getRegionManager() {
    return this.regionManager;
  }

  int getThreadWakeFrequency() {
    return this.threadWakeFrequency;
  }

  FileSystem getFileSystem() {
    return this.fs;
  }

  AtomicBoolean getShutdownRequested() {
    return this.shutdownRequested;
  }

  AtomicBoolean getClosed() {
    return this.closed;
  }

  boolean isClosed() {
    return this.closed.get();
  }

  ServerConnection getServerConnection() {
    return this.connection;
  }

  /**
   * Get the ZK wrapper object
   * @return the zookeeper wrapper
   */
  public ZooKeeperWrapper getZooKeeperWrapper() {
    return this.zooKeeperWrapper;
  }

  // These methods are so don't have to pollute RegionManager with ServerManager.
  SortedMap<HServerLoad, Set<String>> getLoadToServers() {
    return this.serverManager.getLoadToServers();
  }

  int numServers() {
    return this.serverManager.numServers();
  }

  public double getAverageLoad() {
    return this.serverManager.getAverageLoad();
  }

  RegionServerOperationQueue getRegionServerOperationQueue () {
    return this.regionServerOperationQueue;
  }

  /**
   * Get the directory where old logs go
   * @return the dir
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /**
   * Add to the passed <code>m</code> servers that are loaded less than
   * <code>l</code>.
   * @param l
   * @param m
   */
  void getLightServers(final HServerLoad l,
      SortedMap<HServerLoad, Set<String>> m) {
    this.serverManager.getLightServers(l, m);
  }

  /** Main processing loop */
  @Override
  public void run() {
    joinCluster();
    startServiceThreads();
    /* Main processing loop */
    try {
      FINISHED: while (!this.closed.get()) {
        // check if we should be shutting down
        if (this.shutdownRequested.get()) {
          // The region servers won't all exit until we stop scanning the
          // meta regions
          this.regionManager.stopScanners();
          if (this.serverManager.numServers() == 0) {
            startShutdown();
            break;
          }
        }
        final HServerAddress root = this.regionManager.getRootRegionLocation();
        switch (this.regionServerOperationQueue.process(root)) {
        case FAILED:
            // If FAILED op processing, bad. Exit.
          break FINISHED;
        case REQUEUED_BUT_PROBLEM:
          if (!checkFileSystem())
              // If bad filesystem, exit.
            break FINISHED;
          default:
            // Continue run loop if conditions are PROCESSED, NOOP, REQUEUED
          break;
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
      this.closed.set(true);
    }

    // Wait for all the remaining region servers to report in.
    this.serverManager.letRegionServersShutdown();

    /*
     * Clean up and close up shop
     */
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    this.rpcServer.stop();
    this.regionManager.stop();
    this.zooKeeperWrapper.close();
    LOG.info("HMaster main thread exiting");
  }

  /*
   * Joins cluster.  Checks to see if this instance of HBase is fresh or the
   * master was started following a failover. In the second case, it inspects
   * the region server directory and gets their regions assignment.
   */
  private void joinCluster()  {
      LOG.debug("Checking cluster state...");
      HServerAddress rootLocation =
        this.zooKeeperWrapper.readRootRegionLocation();
      List<HServerAddress> addresses = this.zooKeeperWrapper.scanRSDirectory();
      // Check if this is a fresh start of the cluster
      if (addresses.isEmpty()) {
        LOG.debug("Master fresh start, proceeding with normal startup");
        splitLogAfterStartup();
        return;
      }
      // Failover case.
      LOG.info("Master failover, ZK inspection begins...");
      boolean isRootRegionAssigned = false;
      Map <byte[], HRegionInfo> assignedRegions =
        new HashMap<byte[], HRegionInfo>();
      // We must:
      // - contact every region server to add them to the regionservers list
      // - get their current regions assignment
      // TODO: Run in parallel?
      for (HServerAddress address : addresses) {
        HRegionInfo[] regions = null;
        try {
          HRegionInterface hri =
            this.connection.getHRegionConnection(address, false);
          HServerInfo info = hri.getHServerInfo();
          LOG.debug("Inspection found server " + info.getServerName());
          this.serverManager.recordNewServer(info, true);
          regions = hri.getRegionsAssignment();
        } catch (IOException e) {
          LOG.error("Failed contacting " + address.toString(), e);
          continue;
        }
        for (HRegionInfo r: regions) {
          if (r.isRootRegion()) {
            this.connection.setRootRegionLocation(new HRegionLocation(r, rootLocation));
            this.regionManager.setRootRegionLocation(rootLocation);
            // Undo the unassign work in the RegionManager constructor
            this.regionManager.removeRegion(r);
            isRootRegionAssigned = true;
          } else if (r.isMetaRegion()) {
            MetaRegion m = new MetaRegion(new HServerAddress(address), r);
            this.regionManager.addMetaRegionToScan(m);
          }
          assignedRegions.put(r.getRegionName(), r);
        }
      }
      LOG.info("Inspection found " + assignedRegions.size() + " regions, " +
        (isRootRegionAssigned ? "with -ROOT-" : "but -ROOT- was MIA"));
      splitLogAfterStartup();
  }

  /*
   * Inspect the log directory to recover any log file without
   * ad active region server.
   */
  private void splitLogAfterStartup() {
    Path logsDirPath =
      new Path(this.rootdir, HConstants.HREGION_LOGDIR_NAME);
    try {
      if (!this.fs.exists(logsDirPath)) return;
    } catch (IOException e) {
      throw new RuntimeException("Could exists for " + logsDirPath, e);
    }
    FileStatus[] logFolders;
    try {
      logFolders = this.fs.listStatus(logsDirPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed listing " + logsDirPath.toString(), e);
    }
    if (logFolders == null || logFolders.length == 0) {
      LOG.debug("No log files to split, proceeding...");
      return;
    }
    for (FileStatus status : logFolders) {
      String serverName = status.getPath().getName();
      LOG.info("Found log folder : " + serverName);
      if(this.serverManager.getServerInfo(serverName) == null) {
        LOG.info("Log folder doesn't belong " +
          "to a known region server, splitting");
        this.splitLogLock.lock();
        Path logDir =
          new Path(this.rootdir, HLog.getHLogDirectoryName(serverName));
        try {
          HLog.splitLog(this.rootdir, logDir, oldLogDir, this.fs, getConfiguration());
        } catch (IOException e) {
          LOG.error("Failed splitting " + logDir.toString(), e);
        } finally {
          this.splitLogLock.unlock();
        }
      } else {
        LOG.info("Log folder belongs to an existing region server");
      }
    }
  }

  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message.  This should be fine because
   * in general, we do not expect the master to get such unhandled exceptions
   *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
   *  need to install an unexpected exception handler.
   */
  private void startServiceThreads() {
    try {
      this.regionManager.start();
      // Put up info server.
      int port = this.conf.getInt("hbase.master.info.port", 60010);
      if (port >= 0) {
        String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
        this.infoServer = new InfoServer(MASTER, a, port, false);
        this.infoServer.setAttribute(MASTER, this);
        this.infoServer.start();
      }
      // Start the server so everything else is running before we start
      // receiving requests.
      this.rpcServer.start();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started service threads");
      }
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        try {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        } catch (IOException ex) {
          LOG.warn("thread start", ex);
        }
      }
      // Something happened during startup. Shut things down.
      this.closed.set(true);
      LOG.error("Failed startup", e);
    }
  }

  /*
   * Start shutting down the master
   */
  void startShutdown() {
    this.closed.set(true);
    this.regionManager.stopScanners();
    this.regionServerOperationQueue.shutdown();
    this.serverManager.notifyServers();
  }

  public MapWritable regionServerStartup(final HServerInfo serverInfo)
  throws IOException {
    // Set the ip into the passed in serverInfo.  Its ip is more than likely
    // not the ip that the master sees here.  See at end of this method where
    // we pass it back to the regionserver by setting "hbase.regionserver.address"
    String rsAddress = HBaseServer.getRemoteAddress();
    serverInfo.setServerAddress(new HServerAddress(rsAddress,
      serverInfo.getServerAddress().getPort()));
    // Register with server manager
    this.serverManager.regionServerStartup(serverInfo);
    // Send back some config info
    MapWritable mw = createConfigurationSubset();
     mw.put(new Text("hbase.regionserver.address"), new Text(rsAddress));
    return mw;
  }

  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected MapWritable createConfigurationSubset() {
    MapWritable mw = addConfig(new MapWritable(), HConstants.HBASE_DIR);
    return addConfig(mw, "fs.default.name");
  }

  private MapWritable addConfig(final MapWritable mw, final String key) {
    mw.put(new Text(key), new Text(this.conf.get(key)));
    return mw;
  }

  public HMsg [] regionServerReport(HServerInfo serverInfo, HMsg msgs[],
    HRegionInfo[] mostLoadedRegions)
  throws IOException {
    return adornRegionServerAnswer(serverInfo,
      this.serverManager.regionServerReport(serverInfo, msgs, mostLoadedRegions));
  }

  /**
   * Override if you'd add messages to return to regionserver <code>hsi</code>
   * @param messages Messages to add to
   * @return Messages to return to
   */
  protected HMsg [] adornRegionServerAnswer(final HServerInfo hsi,
      final HMsg [] msgs) {
    return msgs;
  }

  public boolean isMasterRunning() {
    return !this.closed.get();
  }

  public void shutdown() {
    LOG.info("Cluster shutdown requested. Starting to quiesce servers");
    this.shutdownRequested.set(true);
    this.zooKeeperWrapper.setClusterState(false);
  }

  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo [] newRegions = null;
    if(splitKeys == null || splitKeys.length == 0) {
      newRegions = new HRegionInfo [] { new HRegionInfo(desc, null, null) };
    } else {
      int numRegions = splitKeys.length + 1;
      newRegions = new HRegionInfo[numRegions];
      byte [] startKey = null;
      byte [] endKey = null;
      for(int i=0;i<numRegions;i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        newRegions[i] = new HRegionInfo(desc, startKey, endKey);
        startKey = endKey;
      }
    }
    for (int tries = 0; tries < this.numRetries; tries++) {
      try {
        // We can not create a table unless meta regions have already been
        // assigned and scanned.
        if (!this.regionManager.areAllMetaRegionsOnline()) {
          throw new NotAllMetaRegionsOnlineException();
        }
        if (!this.serverManager.canAssignUserRegions()) {
          throw new IOException("not enough servers to create table yet");
        }
        createTable(newRegions);
        LOG.info("created table " + desc.getNameAsString());
        break;
      } catch (TableExistsException e) {
        throw e;
      } catch (IOException e) {
        if (tries == this.numRetries - 1) {
          throw RemoteExceptionHandler.checkIOException(e);
        }
        this.sleeper.sleep();
      }
    }
  }

  private synchronized void createTable(final HRegionInfo [] newRegions)
  throws IOException {
    String tableName = newRegions[0].getTableDesc().getNameAsString();
    // 1. Check to see if table already exists. Get meta region where
    // table would sit should it exist. Open scanner on it. If a region
    // for the table we want to create already exists, then table already
    // created. Throw already-exists exception.
    MetaRegion m = regionManager.getFirstMetaRegionForRegion(newRegions[0]);    
    byte [] metaRegionName = m.getRegionName();
    HRegionInterface srvr = this.connection.getHRegionConnection(m.getServer());
    byte[] firstRowInTable = Bytes.toBytes(tableName + ",,");
    Scan scan = new Scan(firstRowInTable);
    scan.addColumn(CATALOG_FAMILY, REGIONINFO_QUALIFIER);
    long scannerid = srvr.openScanner(metaRegionName, scan);
    try {
      Result data = srvr.next(scannerid);
      if (data != null && data.size() > 0) {
        HRegionInfo info = Writables.getHRegionInfo(
          data.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER));
        if (info.getTableDesc().getNameAsString().equals(tableName)) {
          // A region for this table already exists. Ergo table exists.
          throw new TableExistsException(tableName);
        }
      }
    } finally {
      srvr.close(scannerid);
    }
    for(HRegionInfo newRegion : newRegions) {
      regionManager.createRegion(newRegion, srvr, metaRegionName);
    }
  }

  public void deleteTable(final byte [] tableName) throws IOException {
    if (Bytes.equals(tableName, ROOT_TABLE_NAME)) {
      throw new IOException("Can't delete root table");
    }
    new TableDelete(this, tableName).process();
    LOG.info("deleted table: " + Bytes.toString(tableName));
  }

  public void addColumn(byte [] tableName, HColumnDescriptor column)
  throws IOException {
    new AddColumn(this, tableName, column).process();
  }

  public void modifyColumn(byte [] tableName, byte [] columnName,
    HColumnDescriptor descriptor)
  throws IOException {
    new ModifyColumn(this, tableName, columnName, descriptor).process();
  }

  public void deleteColumn(final byte [] tableName, final byte [] c)
  throws IOException {
    new DeleteColumn(this, tableName, KeyValue.parseColumn(c)[0]).process();
  }

  public void enableTable(final byte [] tableName) throws IOException {
    if (Bytes.equals(tableName, ROOT_TABLE_NAME)) {
      throw new IOException("Can't enable root table");
    }
    new ChangeTableState(this, tableName, true).process();
  }

  public void disableTable(final byte [] tableName) throws IOException {
    if (Bytes.equals(tableName, ROOT_TABLE_NAME)) {
      throw new IOException("Can't disable root table");
    }
    new ChangeTableState(this, tableName, false).process();
  }

  // TODO: Redo so this method does not duplicate code with subsequent methods.
  private List<Pair<HRegionInfo,HServerAddress>> getTableRegions(
      final byte [] tableName)
  throws IOException {
    List<Pair<HRegionInfo,HServerAddress>> result =
      new ArrayList<Pair<HRegionInfo,HServerAddress>>();
    Set<MetaRegion> regions =
      this.regionManager.getMetaRegionsForTable(tableName);
    byte [] firstRowInTable = Bytes.toBytes(Bytes.toString(tableName) + ",,");
    for (MetaRegion m: regions) {
      byte [] metaRegionName = m.getRegionName();
      HRegionInterface srvr =
        this.connection.getHRegionConnection(m.getServer());
      Scan scan = new Scan(firstRowInTable);
      scan.addColumn(CATALOG_FAMILY, REGIONINFO_QUALIFIER);
      scan.addColumn(CATALOG_FAMILY, SERVER_QUALIFIER);
      // TODO: Use caching.
      long scannerid = srvr.openScanner(metaRegionName, scan);
      try {
        while (true) {
          Result data = srvr.next(scannerid);
          if (data == null || data.size() <= 0)
            break;
          HRegionInfo info = Writables.getHRegionInfo(
              data.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER));
          if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
            byte [] value = data.getValue(CATALOG_FAMILY, SERVER_QUALIFIER);
            if (value != null) {
              HServerAddress server = new HServerAddress(Bytes.toString(value));
              result.add(new Pair<HRegionInfo,HServerAddress>(info, server));
            }
          } else {
            break;
          }
        }
      } finally {
        srvr.close(scannerid);
      }
    }
    return result;
  }

  private Pair<HRegionInfo,HServerAddress> getTableRegionClosest(
      final byte [] tableName, final byte [] rowKey)
  throws IOException {
    Set<MetaRegion> regions =
      this.regionManager.getMetaRegionsForTable(tableName);
    for (MetaRegion m: regions) {
      byte [] firstRowInTable = Bytes.toBytes(Bytes.toString(tableName) + ",,");
      byte [] metaRegionName = m.getRegionName();
      HRegionInterface srvr = this.connection.getHRegionConnection(m.getServer());
      Scan scan = new Scan(firstRowInTable);
      scan.addColumn(CATALOG_FAMILY, REGIONINFO_QUALIFIER);
      scan.addColumn(CATALOG_FAMILY, SERVER_QUALIFIER);
      long scannerid = srvr.openScanner(metaRegionName, scan);
      try {
        while (true) {
          Result data = srvr.next(scannerid);
          if (data == null || data.size() <= 0)
            break;
          HRegionInfo info = Writables.getHRegionInfo(
              data.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER));
          if (Bytes.compareTo(info.getTableDesc().getName(), tableName) == 0) {
            if ((Bytes.compareTo(info.getStartKey(), rowKey) >= 0) &&
                (Bytes.compareTo(info.getEndKey(), rowKey) < 0)) {
                byte [] value = data.getValue(CATALOG_FAMILY, SERVER_QUALIFIER);
                if (value != null) {
                  HServerAddress server =
                    new HServerAddress(Bytes.toString(value));
                  return new Pair<HRegionInfo,HServerAddress>(info, server);
                }
            }
          } else {
            break;
          }
        }
      } finally {
        srvr.close(scannerid);
      }
    }
    return null;
  }

  private Pair<HRegionInfo,HServerAddress> getTableRegionFromName(
      final byte [] regionName)
  throws IOException {
    byte [] tableName = HRegionInfo.parseRegionName(regionName)[0];
    Set<MetaRegion> regions = regionManager.getMetaRegionsForTable(tableName);
    for (MetaRegion m: regions) {
      byte [] metaRegionName = m.getRegionName();
      HRegionInterface srvr = connection.getHRegionConnection(m.getServer());
      Get get = new Get(regionName);
      get.addColumn(CATALOG_FAMILY, REGIONINFO_QUALIFIER);
      get.addColumn(CATALOG_FAMILY, SERVER_QUALIFIER);
      Result data = srvr.get(metaRegionName, get);
      if(data == null || data.size() <= 0) continue;
      HRegionInfo info = Writables.getHRegionInfo(
          data.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER));
      byte [] value = data.getValue(CATALOG_FAMILY, SERVER_QUALIFIER);
      if(value != null) {
        HServerAddress server =
          new HServerAddress(Bytes.toString(value));
        return new Pair<HRegionInfo,HServerAddress>(info, server);
      }
    }
    return null;
  }

  /**
   * Get row from meta table.
   * @param row
   * @param family
   * @return Result
   * @throws IOException
   */
  protected Result getFromMETA(final byte [] row, final byte [] family)
  throws IOException {
    MetaRegion meta = this.regionManager.getMetaRegionForRow(row);
    HRegionInterface srvr = getMETAServer(meta);
    Get get = new Get(row);
    get.addFamily(family);
    return srvr.get(meta.getRegionName(), get);
  }

  /*
   * @param meta
   * @return Server connection to <code>meta</code> .META. region.
   * @throws IOException
   */
  private HRegionInterface getMETAServer(final MetaRegion meta)
  throws IOException {
    return this.connection.getHRegionConnection(meta.getServer());
  }

  public void modifyTable(final byte[] tableName, HConstants.Modify op,
      Writable[] args)
  throws IOException {
    switch (op) {
    case TABLE_SET_HTD:
      if (args == null || args.length < 1 ||
          !(args[0] instanceof HTableDescriptor))
        throw new IOException("SET_HTD request requires an HTableDescriptor");
      HTableDescriptor htd = (HTableDescriptor) args[0];
      LOG.info("modifyTable(SET_HTD): " + htd);
      new ModifyTableMeta(this, tableName, htd).process();
      break;

    case TABLE_SPLIT:
    case TABLE_COMPACT:
    case TABLE_MAJOR_COMPACT:
    case TABLE_FLUSH:
      if (args != null && args.length > 0) {
        if (!(args[0] instanceof ImmutableBytesWritable))
          throw new IOException(
            "request argument must be ImmutableBytesWritable");
        Pair<HRegionInfo,HServerAddress> pair = null;
        if(tableName == null) {
          byte [] regionName = ((ImmutableBytesWritable)args[0]).get();
          pair = getTableRegionFromName(regionName);
        } else {
          byte [] rowKey = ((ImmutableBytesWritable)args[0]).get();
          pair = getTableRegionClosest(tableName, rowKey);
        }
        if (pair != null) {
          this.regionManager.startAction(pair.getFirst().getRegionName(),
            pair.getFirst(), pair.getSecond(), op);
        }
      } else {
        for (Pair<HRegionInfo,HServerAddress> pair: getTableRegions(tableName))
          this.regionManager.startAction(pair.getFirst().getRegionName(),
            pair.getFirst(), pair.getSecond(), op);
      }
      break;

    case CLOSE_REGION:
      if (args == null || args.length < 1 || args.length > 2) {
        throw new IOException("Requires at least a region name; " +
          "or cannot have more than region name and servername");
      }
      // Arguments are regionname and an optional server name.
      byte [] regionname = ((ImmutableBytesWritable)args[0]).get();
      LOG.debug("Attempting to close region: " + Bytes.toStringBinary(regionname));
      String hostnameAndPort = null;
      if (args.length == 2) {
        hostnameAndPort = Bytes.toString(((ImmutableBytesWritable)args[1]).get());
      }
      // Need hri
      Result rr = getFromMETA(regionname, HConstants.CATALOG_FAMILY);
      HRegionInfo hri = getHRegionInfo(rr.getRow(), rr);
      if (hostnameAndPort == null) {
        // Get server from the .META. if it wasn't passed as argument
        hostnameAndPort = 
          Bytes.toString(rr.getValue(CATALOG_FAMILY, SERVER_QUALIFIER));
      }
      // Take region out of the intransistions in case it got stuck there doing
      // an open or whatever.
      this.regionManager.clearFromInTransition(regionname);
      // If hostnameAndPort is still null, then none, exit.
      if (hostnameAndPort == null) break;
      long startCode =
        Bytes.toLong(rr.getValue(CATALOG_FAMILY, STARTCODE_QUALIFIER));
      String name = HServerInfo.getServerName(hostnameAndPort, startCode);
      LOG.info("Marking " + hri.getRegionNameAsString() +
        " as closing on " + name + "; cleaning SERVER + STARTCODE; " +
          "master will tell regionserver to close region on next heartbeat");
      this.regionManager.setClosing(name, hri, hri.isOffline());
      MetaRegion meta = this.regionManager.getMetaRegionForRow(regionname);
      HRegionInterface srvr = getMETAServer(meta);
      HRegion.cleanRegionInMETA(srvr, meta.getRegionName(), hri);
      break;

    default:
      throw new IOException("unsupported modifyTable op " + op);
    }
  }

  /**
   * @return cluster status
   */
  public ClusterStatus getClusterStatus() {
    ClusterStatus status = new ClusterStatus();
    status.setHBaseVersion(VersionInfo.getVersion());
    status.setServerInfo(serverManager.getServersToServerInfo().values());
    status.setDeadServers(serverManager.getDeadServers());
    status.setRegionsInTransition(this.regionManager.getRegionsInTransition());
    return status;
  }

  // TODO ryan rework this function
  /*
   * Get HRegionInfo from passed META map of row values.
   * Returns null if none found (and logs fact that expected COL_REGIONINFO
   * was missing).  Utility method used by scanners of META tables.
   * @param row name of the row
   * @param map Map to do lookup in.
   * @return Null or found HRegionInfo.
   * @throws IOException
   */
  HRegionInfo getHRegionInfo(final byte [] row, final Result res)
  throws IOException {
    byte [] regioninfo = res.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER);
    if (regioninfo == null) {
      StringBuilder sb =  new StringBuilder();
      NavigableMap<byte[], byte[]> infoMap = res.getFamilyMap(CATALOG_FAMILY);
      for (byte [] e: infoMap.keySet()) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(Bytes.toString(CATALOG_FAMILY) + ":" + Bytes.toString(e));
      }
      LOG.warn(Bytes.toString(CATALOG_FAMILY) + ":" +
          Bytes.toString(REGIONINFO_QUALIFIER) + " is empty for row: " +
         Bytes.toString(row) + "; has keys: " + sb.toString());
      return null;
    }
    return Writables.getHRegionInfo(regioninfo);
  }

  /*
   * When we find rows in a meta region that has an empty HRegionInfo, we
   * clean them up here.
   *
   * @param s connection to server serving meta region
   * @param metaRegionName name of the meta region we scanned
   * @param emptyRows the row keys that had empty HRegionInfos
   */
  protected void deleteEmptyMetaRows(HRegionInterface s,
      byte [] metaRegionName,
      List<byte []> emptyRows) {
    for (byte [] regionName: emptyRows) {
      try {
        HRegion.removeRegionFromMETA(s, metaRegionName, regionName);
        LOG.warn("Removed region: " + Bytes.toString(regionName) +
          " from meta region: " +
          Bytes.toString(metaRegionName) + " because HRegionInfo was empty");
      } catch (IOException e) {
        LOG.error("deleting region: " + Bytes.toString(regionName) +
            " from meta region: " + Bytes.toString(metaRegionName), e);
      }
    }
  }

  /**
   * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  @Override
  public void process(WatchedEvent event) {
    LOG.debug(("Event " + event.getType() +  " with path " + event.getPath()));
    // Master should kill itself if its session expired or if its
    // znode was deleted manually (usually for testing purposes)
    if(event.getState() == KeeperState.Expired ||
      (event.getType().equals(EventType.NodeDeleted) &&
        event.getPath().equals(this.zooKeeperWrapper.getMasterElectionZNode())) &&
        !shutdownRequested.get()) {

      LOG.info("Master lost its znode, trying to get a new one");

      // Can we still be the master? If not, goodbye

      zooKeeperWrapper.close();
      try {
        zooKeeperWrapper = new ZooKeeperWrapper(conf, this);
        this.zkMasterAddressWatcher.setZookeeper(zooKeeperWrapper);
        if(!this.zkMasterAddressWatcher.
            writeAddressToZooKeeper(this.address,false)) {
          throw new Exception("Another Master is currently active");
        }

        // Verify the cluster to see if anything happened while we were away
        joinCluster();
      } catch (Exception e) {
        LOG.error("Killing master because of", e);
        System.exit(1);
      }
    }
  }

  private static void printUsageAndExit() {
    System.err.println("Usage: Master [opts] start|stop");
    System.err.println(" start  Start Master. If local mode, start Master and RegionServer in same JVM");
    System.err.println(" stop   Start cluster shutdown; Master signals RegionServer shutdown");
    System.err.println(" where [opts] are:");
    System.err.println("   --minServers=<servers>    Minimum RegionServers needed to host user tables.");
    System.exit(0);
  }

  /**
   * Utility for constructing an instance of the passed HMaster class.
   * @param masterClass
   * @param conf
   * @return HMaster instance.
   */
  public static HMaster constructMaster(Class<? extends HMaster> masterClass,
      final Configuration conf)  {
    try {
      Constructor<? extends HMaster> c =
        masterClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " +
        "Master: " + masterClass.toString() +
        ((e.getCause() != null)? e.getCause().getMessage(): ""), e);
    }
  }

  /*
   * Version of master that will shutdown the passed zk cluster on its way out.
   */
  static class LocalHMaster extends HMaster {
    private MiniZooKeeperCluster zkcluster = null;

    public LocalHMaster(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public void run() {
      super.run();
      if (this.zkcluster != null) {
        try {
          this.zkcluster.shutdown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    void setZKCluster(final MiniZooKeeperCluster zkcluster) {
      this.zkcluster = zkcluster;
    }
  }

  protected static void doMain(String [] args,
      Class<? extends HMaster> masterClass) {
    if (args.length < 1) {
      printUsageAndExit();
    }
    Configuration conf = HBaseConfiguration.create();
    // Process command-line args.
    for (String cmd: args) {

      if (cmd.startsWith("--minServers=")) {
        conf.setInt("hbase.regions.server.count.min",
          Integer.valueOf(cmd.substring(13)));
        continue;
      }

      if (cmd.equalsIgnoreCase("start")) {
        try {
          // Print out vm stats before starting up.
          RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
          if (runtime != null) {
            LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
              runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
            LOG.info("vmInputArguments=" + runtime.getInputArguments());
          }
          // If 'local', defer to LocalHBaseCluster instance.  Starts master
          // and regionserver both in the one JVM.
          if (LocalHBaseCluster.isLocal(conf)) {
            final MiniZooKeeperCluster zooKeeperCluster =
              new MiniZooKeeperCluster();
            File zkDataPath = new File(conf.get("hbase.zookeeper.property.dataDir"));
            int zkClientPort = conf.getInt("hbase.zookeeper.property.clientPort", 0);
            if (zkClientPort == 0) {
              throw new IOException("No config value for hbase.zookeeper.property.clientPort");
            }
            zooKeeperCluster.setTickTime(conf.getInt("hbase.zookeeper.property.tickTime", 3000));
            zooKeeperCluster.setClientPort(zkClientPort);
            int clientPort = zooKeeperCluster.startup(zkDataPath);
            if (clientPort != zkClientPort) {
              String errorMsg = "Couldnt start ZK at requested address of " +
                  zkClientPort + ", instead got: " + clientPort + ". Aborting. Why? " +
                  "Because clients (eg shell) wont be able to find this ZK quorum";
              System.err.println(errorMsg);
              throw new IOException(errorMsg);
            }
            conf.set("hbase.zookeeper.property.clientPort",
              Integer.toString(clientPort));
            // Need to have the zk cluster shutdown when master is shutdown.
            // Run a subclass that does the zk cluster shutdown on its way out.
            LocalHBaseCluster cluster = new LocalHBaseCluster(conf, 1,
              LocalHMaster.class, HRegionServer.class);
            ((LocalHMaster)cluster.getMaster()).setZKCluster(zooKeeperCluster);
            cluster.startup();
          } else {
            HMaster master = constructMaster(masterClass, conf);
            if (master.shutdownRequested.get()) {
              LOG.info("Won't bring the Master up as a shutdown is requested");
              return;
            }
            master.start();
          }
        } catch (Throwable t) {
          LOG.error("Failed to start master", t);
          System.exit(-1);
        }
        break;
      }

      if (cmd.equalsIgnoreCase("stop")) {
        HBaseAdmin adm = null;
        try {
          adm = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
          LOG.error("Master not running");
          System.exit(0);
        }
        try {
          adm.shutdown();
        } catch (Throwable t) {
          LOG.error("Failed to stop master", t);
          System.exit(-1);
        }
        break;
      }

      // Print out usage if we get to here.
      printUsageAndExit();
    }
  }

  public Map<String, Integer> getTableFragmentation() throws IOException {
    long now = System.currentTimeMillis();
    // only check every two minutes by default
    int check = this.conf.getInt("hbase.master.fragmentation.check.frequency", 2 * 60 * 1000);
    if (lastFragmentationQuery == -1 || now - lastFragmentationQuery > check) {
      fragmentation = FSUtils.getTableFragmentation(this);
      lastFragmentationQuery = now;
    }
    return fragmentation;
  }

  /**
   * Main program
   * @param args
   */
  public static void main(String [] args) {
    doMain(args, HMaster.class);
  }
}
