/**
 * Copyright 2009 The Apache Software Foundation
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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * HMaster is the "master server" for a HBase.
 * There is only one HMaster for a single HBase deployment.
 * 
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 */
public class HMaster extends Thread implements HConstants, HMasterInterface, 
  HMasterRegionInterface, Watcher {

  static final Log LOG = LogFactory.getLog(HMaster.class.getName());

  public long getProtocolVersion(String protocol, long clientVersion) {
    return HBaseRPCProtocolVersion.versionID;
  }

  // We start out with closed flag on.  Using AtomicBoolean rather than
  // plain boolean because want to pass a reference to supporting threads
  // started here in HMaster rather than have them have to know about the
  // hosting class
  volatile AtomicBoolean closed = new AtomicBoolean(true);
  volatile AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  volatile boolean fsOk = true;
  final Path rootdir;
  private final HBaseConfiguration conf;
  final FileSystem fs;
  final Random rand;
  final int threadWakeFrequency; 
  final int numRetries;
  final long maxRegionOpenTime;
  final int leaseTimeout;
  private final ZooKeeperWrapper zooKeeperWrapper;
  private final ZKMasterAddressWatcher zkMasterAddressWatcher;

  volatile DelayQueue<RegionServerOperation> delayedToDoQueue =
    new DelayQueue<RegionServerOperation>();
  volatile BlockingQueue<RegionServerOperation> toDoQueue =
    new PriorityBlockingQueue<RegionServerOperation>();

  private final HBaseServer server;
  private final HServerAddress address;

  final ServerConnection connection;

  final int metaRescanInterval;
  
  // A Sleeper that sleeps for threadWakeFrequency
  private final Sleeper sleeper;
  
  // Default access so accesible from unit tests. MASTER is name of the webapp
  // and the attribute name used stuffing this instance into web context.
  InfoServer infoServer;
  
  /** Name of master server */
  public static final String MASTER = "master";

  /** @return InfoServer object */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  ServerManager serverManager;
  RegionManager regionManager;
  
  private MasterMetrics metrics;

  /** 
   * Build the HMaster out of a raw configuration item.
   * @param conf configuration
   * 
   * @throws IOException
   */
  public HMaster(HBaseConfiguration conf) throws IOException {
    // find out our address up in DNS.
    String addressStr = DNS.getDefaultHost(
        conf.get("hbase.master.dns.interface","default"),
        conf.get("hbase.master.dns.nameserver","default"));
    addressStr += ":" + 
      conf.get(MASTER_PORT, Integer.toString(DEFAULT_MASTER_PORT));
    HServerAddress address = new HServerAddress(addressStr);
    LOG.info("My address is " + address);

    this.conf = conf;
    this.rootdir = new Path(conf.get(HBASE_DIR));
    try {
      FSUtils.validateRootPath(this.rootdir);
    } catch (IOException e) {
      LOG.fatal("Not starting HMaster because the root directory path '" +
          this.rootdir + "' is not valid. Check the setting of the" +
          " configuration parameter '" + HBASE_DIR + "'", e);
      throw e;
    }
    this.threadWakeFrequency = conf.getInt(THREAD_WAKE_FREQUENCY, 10 * 1000);
    // The filesystem hbase wants to use is probably not what is set into
    // fs.default.name; its value is probably the default.
    this.conf.set("fs.default.name", this.rootdir.toString());
    this.fs = FileSystem.get(conf);
    if (this.fs instanceof DistributedFileSystem) {
      // Make sure dfs is not in safe mode
      String message = "Waiting for dfs to exit safe mode...";
      while (((DistributedFileSystem) fs).setSafeMode(
          FSConstants.SafeModeAction.SAFEMODE_GET)) {
        LOG.info(message);
        try {
          Thread.sleep(this.threadWakeFrequency);
        } catch (InterruptedException e) {
          //continue
        }
      }
    }
    this.conf.set(HConstants.HBASE_DIR, this.rootdir.toString());
    this.rand = new Random();

    try {
      // Make sure the hbase root directory exists!
      if (!fs.exists(rootdir)) {
        fs.mkdirs(rootdir); 
        FSUtils.setVersion(fs, rootdir);
      } else {
        FSUtils.checkVersion(fs, rootdir, true);
      }

      // Make sure the root region directory exists!
      if (!FSUtils.rootRegionExists(fs, rootdir)) {
        bootstrap();
      }
    } catch (IOException e) {
      LOG.fatal("Not starting HMaster because:", e);
      throw e;
    }

    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.maxRegionOpenTime =
      conf.getLong("hbase.hbasemaster.maxregionopen", 120 * 1000);
    this.leaseTimeout = conf.getInt("hbase.master.lease.period", 120 * 1000);
    this.server = HBaseRPC.getServer(this, address.getBindAddress(),
        address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);
         
    //  The rpc-server port can be ephemeral... ensure we have the correct info
    this.address = new HServerAddress(server.getListenerAddress());

    // dont retry too much
    conf.setInt("hbase.client.retries.number", 3);
    this.connection = ServerConnectionManager.getConnection(conf);

    this.metaRescanInterval =
      conf.getInt("hbase.master.meta.thread.rescanfrequency", 60 * 1000);

    this.sleeper = new Sleeper(this.threadWakeFrequency, this.closed);
    
    zooKeeperWrapper = new ZooKeeperWrapper(conf, this);
    zkMasterAddressWatcher = new ZKMasterAddressWatcher(this);
    serverManager = new ServerManager(this);
    regionManager = new RegionManager(this);
    
    writeAddressToZooKeeper();
    
    // We're almost open for business
    this.closed.set(false);
    LOG.info("HMaster initialized on " + this.address.toString());
  }

  private void writeAddressToZooKeeper() {
    while (true) {
      zkMasterAddressWatcher.waitForMasterAddressAvailability();
      // Check if we need to shutdown instead of taking control
      if(this.shutdownRequested.get())
      {
        return;
      } else if(zooKeeperWrapper.writeMasterAddress(address)) {
        zooKeeperWrapper.setClusterState(true);
        // Watch our own node
        zooKeeperWrapper.readMasterAddress(this);
        return;
      }
    }
  }

  private void bootstrap() throws IOException {
    LOG.info("BOOTSTRAP: creating ROOT and first META regions");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      setBlockCaching(HRegionInfo.ROOT_REGIONINFO, false);
      setBlockCaching(HRegionInfo.FIRST_META_REGIONINFO, false);
      HRegion root = HRegion.createHRegion(HRegionInfo.ROOT_REGIONINFO,
        this.rootdir, this.conf);
      HRegion meta = HRegion.createHRegion(HRegionInfo.FIRST_META_REGIONINFO,
        this.rootdir, this.conf);
      // Add first region from the META table to the ROOT region.
      HRegion.addRegionToMETA(root, meta);
      root.close();
      root.getLog().closeAndDelete();
      meta.close();
      meta.getLog().closeAndDelete();
      setBlockCaching(HRegionInfo.ROOT_REGIONINFO, true);
      setBlockCaching(HRegionInfo.FIRST_META_REGIONINFO, true);
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
  private void setBlockCaching(final HRegionInfo hri, final boolean b) {
    for (HColumnDescriptor hcd: hri.getTableDesc().families.values()) {
      hcd.setBlockCacheEnabled(b);
    }
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  protected boolean checkFileSystem() {
    if (fsOk) {
      try {
        FSUtils.checkFileSystemAvailable(fs);
      } catch (IOException e) {
        LOG.fatal("Shutting down HBase cluster: file system not available", e);
        closed.set(true);
        fsOk = false;
      }
    }
    return fsOk;
  }

  /** @return HServerAddress of the master server */
  public HServerAddress getMasterAddress() {
    return address;
  }
  
  /**
   * @return Hbase root dir.
   */
  public Path getRootDir() {
    return this.rootdir;
  }

  /**
   * @return Read-only map of servers to serverinfo.
   */
  public Map<String, HServerInfo> getServersToServerInfo() {
    return serverManager.getServersToServerInfo();
  }

  public Map<HServerAddress, HServerInfo> getServerAddressToServerInfo() {
    return serverManager.getServerAddressToServerInfo();
  }

  /**
   * @return Read-only map of servers to load.
   */
  public Map<String, HServerLoad> getServersToLoad() {
    return serverManager.getServersToLoad();
  }
  
  /** @return The average load */
  public double getAverageLoad() {
    return serverManager.getAverageLoad();
  }
  
  /** @return the number of regions on filesystem */
  public int countRegionsOnFS() {
    try {
      return regionManager.countRegionsOnFS();
    } catch (IOException e) {
      LOG.warn("Get count of Regions on FileSystem error : " + 
          StringUtils.stringifyException(e));
    }
    return -1;
  }

  /**
   * @return Location of the <code>-ROOT-</code> region.
   */
  public HServerAddress getRootRegionLocation() {
    HServerAddress rootServer = null;
    if (!shutdownRequested.get() && !closed.get()) {
      rootServer = regionManager.getRootRegionLocation();
    }
    return rootServer;
  }
  
  /**
   * Wait until root region is available
   */
  public void waitForRootRegionLocation() {
    regionManager.waitForRootRegionLocation();
  }
  
  /**
   * @return Read-only map of online regions.
   */
  public Map<byte [], MetaRegion> getOnlineMetaRegions() {
    return regionManager.getOnlineMetaRegions();
  }

  /** Main processing loop */
  @Override
  public void run() {
    final String threadName = "HMaster";
    Thread.currentThread().setName(threadName);
    verifyClusterState();
    startServiceThreads();
    /* Main processing loop */
    try {
      while (!closed.get()) {
        // check if we should be shutting down
        if (shutdownRequested.get()) {
          // The region servers won't all exit until we stop scanning the
          // meta regions
          regionManager.stopScanners();
          if (serverManager.numServers() == 0) {
            startShutdown();
            break;
          }
        }
        // work on the TodoQueue. If that fails, we should shut down.
        if (!processToDoQueue()) {
          break;
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
      closed.set(true);
    }
    
    // Wait for all the remaining region servers to report in.
    serverManager.letRegionServersShutdown();

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
    server.stop();                      // Stop server
    regionManager.stop();

    zooKeeperWrapper.close();

    // Join up with all threads
    LOG.info("HMaster main thread exiting");
  }
  
  /**
   * Try to get an operation off of the todo queue and perform it.
   */ 
  private boolean processToDoQueue() {
    RegionServerOperation op = null;

    // block until the root region is online
    if (regionManager.getRootRegionLocation() != null) {
      // We can't process server shutdowns unless the root region is online
      op = delayedToDoQueue.poll();
    }
    
    // if there aren't any todo items in the queue, sleep for a bit.
    if (op == null ) {
      try {
        op = toDoQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // continue
      }
    }
    
    // at this point, if there's still no todo operation, or we're supposed to
    // be closed, return.
    if (op == null || closed.get()) {
      return true;
    }
    
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing todo: " + op.toString());
      }
      
      // perform the operation. 
      if (!op.process()) {
        // Operation would have blocked because not all meta regions are
        // online. This could cause a deadlock, because this thread is waiting
        // for the missing meta region(s) to come back online, but since it
        // is waiting, it cannot process the meta region online operation it
        // is waiting for. So put this operation back on the queue for now.
        if (toDoQueue.size() == 0) {
          // The queue is currently empty so wait for a while to see if what
          // we need comes in first
          sleeper.sleep();
        }
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Put " + op.toString() + " back on queue");
          }
          toDoQueue.put(op);
        } catch (InterruptedException e) {
          throw new RuntimeException(
            "Putting into toDoQueue was interrupted.", e);
        }
      }
    } catch (Exception ex) {
      // There was an exception performing the operation.
      if (ex instanceof RemoteException) {
        try {
          ex = RemoteExceptionHandler.decodeRemoteException(
            (RemoteException)ex);
        } catch (IOException e) {
          ex = e;
          LOG.warn("main processing loop: " + op.toString(), e);
        }
      }
      // make sure the filesystem is still ok. otherwise, we're toast.
      if (!checkFileSystem()) {
        return false;
      }
      LOG.warn("Processing pending operations: " + op.toString(), ex);
      delayedToDoQueue.put(op);
    }
    return true;
  }
  
  /*
   * Verifies if this instance of HBase is fresh or the master was started
   * following a failover. In the second case, it inspects the region server
   * directory and gets their regions assignment. 
   */
  private void verifyClusterState()  {
    try {
      LOG.debug("Checking cluster state...");
      HServerAddress rootLocation = zooKeeperWrapper.readRootRegionLocation();
      List<HServerAddress> addresses =  zooKeeperWrapper.scanRSDirectory();
      
      // Check if this is a fresh start of the cluster
      if(addresses.size() == 0) {
        LOG.debug("This is a fresh start, proceeding with normal startup");
        splitLogAfterStartup();
        return;
      }
      LOG.info("This is a failover, ZK inspection begins...");
      boolean isRootRegionAssigned = false;
      Map<byte[], HRegionInfo> assignedRegions = 
        new HashMap<byte[], HRegionInfo>();
      // This is a failover case. We must:
      // - contact every region server to add them to the regionservers list
      // - get their current regions assignment 
      for (HServerAddress address : addresses) {
        HRegionInterface hri = 
          this.connection.getHRegionConnection(address, false);
        HServerInfo info = hri.getHServerInfo();
        LOG.debug("Inspection found server " + info.getName());
        serverManager.recordNewServer(info, true);
        HRegionInfo[] regions = hri.getRegionsAssignment();
        for (HRegionInfo region : regions) {
          if(region.isRootRegion()) {
            connection.setRootRegionLocation(
                new HRegionLocation(region, rootLocation));
            regionManager.setRootRegionLocation(rootLocation);
            // Undo the unassign work in the RegionManager constructor
            regionManager.removeRegion(region);
            isRootRegionAssigned = true;
          }
          else if(region.isMetaRegion()) {
            MetaRegion m =
              new MetaRegion(new HServerAddress(address),
                  region);
            regionManager.addMetaRegionToScan(m);
          }
          assignedRegions.put(region.getRegionName(), region);
        }
      }
      LOG.info("Inspection found " + assignedRegions.size() + " regions, " + 
          (isRootRegionAssigned ? "with -ROOT-" : "but -ROOT- was MIA"));
      splitLogAfterStartup();
    } catch(IOException ex) {
      ex.printStackTrace();
    }
  }
  
  /**
   * Inspect the log directory to recover any log file without
   * and active region server.
   * @throws IOException
   */
  private void splitLogAfterStartup() throws IOException {
    Path logsDirPath =
      new Path(this.rootdir, HConstants.HREGION_LOGDIR_NAME);
    FileStatus [] logFolders = this.fs.listStatus(logsDirPath);
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
        this.regionManager.splitLogLock.lock();
        Path logDir =
          new Path(this.rootdir, HLog.getHLogDirectoryName(serverName));
        try {
          HLog.splitLog(this.rootdir, logDir, this.fs,
              getConfiguration());
        } finally {
          this.regionManager.splitLogLock.unlock();
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
    // Do after main thread name has been set
    this.metrics = new MasterMetrics();
    try {
      regionManager.start();
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
      this.server.start();
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started service threads");
    }
  }

  /*
   * Start shutting down the master
   */
  void startShutdown() {
    closed.set(true);
    regionManager.stopScanners();
    synchronized(toDoQueue) {
      toDoQueue.clear();                         // Empty the queue
      delayedToDoQueue.clear();                  // Empty shut down queue
      toDoQueue.notifyAll();                     // Wake main thread
    }
    serverManager.notifyServers();
  }

  /*
   * HMasterRegionInterface
   */
  public MapWritable regionServerStartup(final HServerInfo serverInfo)
  throws IOException {
    // Set the address for now even tho it will not be persisted on HRS side
    // If the address given is not the default one,
    // use the IP given by the user.
    if (serverInfo.getServerAddress().getBindAddress().equals(
        DEFAULT_HOST)) {
      String rsAddress = HBaseServer.getRemoteAddress();
      serverInfo.setServerAddress(new HServerAddress(rsAddress,
        serverInfo.getServerAddress().getPort()));
    }
    // Register with server manager
    this.serverManager.regionServerStartup(serverInfo);
    // Send back some config info
    return createConfigurationSubset();
  }
  
  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected MapWritable createConfigurationSubset() {
    MapWritable mw = addConfig(new MapWritable(), HConstants.HBASE_DIR);
    // Get the real address of the HRS.
    String rsAddress = HBaseServer.getRemoteAddress();
    if (rsAddress != null) {
      mw.put(new Text("hbase.regionserver.address"), new Text(rsAddress));
    }
    
    return addConfig(mw, "fs.default.name");
  }

  private MapWritable addConfig(final MapWritable mw, final String key) {
    mw.put(new Text(key), new Text(this.conf.get(key)));
    return mw;
  }

  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[], 
    HRegionInfo[] mostLoadedRegions)
  throws IOException {
    return serverManager.regionServerReport(serverInfo, msgs,
      mostLoadedRegions);
  }

  /*
   * HMasterInterface
   */

  public boolean isMasterRunning() {
    return !closed.get();
  }

  public void shutdown() {
    LOG.info("Cluster shutdown requested. Starting to quiesce servers");
    this.shutdownRequested.set(true);
    this.zooKeeperWrapper.setClusterState(false);
  }

  public void createTable(HTableDescriptor desc)
  throws IOException {    
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo newRegion = new HRegionInfo(desc, null, null);

    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // We can not create a table unless meta regions have already been
        // assigned and scanned.
        if (!regionManager.areAllMetaRegionsOnline()) {
          throw new NotAllMetaRegionsOnlineException();
        }
        createTable(newRegion);
        LOG.info("created table " + desc.getNameAsString());
        break;
      } catch (TableExistsException e) {
        throw e;
      } catch (IOException e) {
        if (tries == numRetries - 1) {
          throw RemoteExceptionHandler.checkIOException(e);
        }
        sleeper.sleep();
      }
    }
  }

  private synchronized void createTable(final HRegionInfo newRegion) 
  throws IOException {
    String tableName = newRegion.getTableDesc().getNameAsString();
    // 1. Check to see if table already exists. Get meta region where
    // table would sit should it exist. Open scanner on it. If a region
    // for the table we want to create already exists, then table already
    // created. Throw already-exists exception.
    MetaRegion m = regionManager.getFirstMetaRegionForRegion(newRegion);
        
    byte [] metaRegionName = m.getRegionName();
    HRegionInterface srvr = connection.getHRegionConnection(m.getServer());
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
    regionManager.createRegion(newRegion, srvr, metaRegionName);
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

  private List<Pair<HRegionInfo,HServerAddress>>
  getTableRegions(final byte [] tableName) throws IOException {
    List<Pair<HRegionInfo,HServerAddress>> result =
        new ArrayList<Pair<HRegionInfo,HServerAddress>>();
    Set<MetaRegion> regions = regionManager.getMetaRegionsForTable(tableName);
    byte [] firstRowInTable = Bytes.toBytes(Bytes.toString(tableName) + ",,");
    for (MetaRegion m: regions) {
      byte [] metaRegionName = m.getRegionName();
      HRegionInterface srvr = connection.getHRegionConnection(m.getServer());
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
            byte [] value = data.getValue(CATALOG_FAMILY, SERVER_QUALIFIER);
            if (value != null) {
              HServerAddress server =
                new HServerAddress(Bytes.toString(value));
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

  private Pair<HRegionInfo,HServerAddress>
  getTableRegionClosest(final byte [] tableName, final byte [] rowKey)
    throws IOException {
    Set<MetaRegion> regions = regionManager.getMetaRegionsForTable(tableName);
    for (MetaRegion m: regions) {
      byte [] firstRowInTable = Bytes.toBytes(Bytes.toString(tableName) + ",,");
      byte [] metaRegionName = m.getRegionName();
      HRegionInterface srvr = connection.getHRegionConnection(m.getServer());
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

  private Pair<HRegionInfo,HServerAddress>
  getTableRegionFromName(final byte [] regionName)
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
      String servername = null;
      if (args.length == 2) {
        servername = Bytes.toString(((ImmutableBytesWritable)args[1]).get());
      }
      // Need hri 
      Result rr = getFromMETA(regionname, HConstants.CATALOG_FAMILY);
      HRegionInfo hri = getHRegionInfo(rr.getRow(), rr);
      if (servername == null) {
        // Get server from the .META. if it wasn't passed as argument
        servername = 
          Bytes.toString(rr.getValue(CATALOG_FAMILY, SERVER_QUALIFIER));
      }
      // Take region out of the intransistions in case it got stuck there doing
      // an open or whatever.
      this.regionManager.clearFromInTransition(regionname);
      // If servername is still null, then none, exit.
      if (servername == null) break;
      // Need to make up a HServerInfo 'servername' for that is how
      // items are keyed in regionmanager Maps.
      HServerAddress addr = new HServerAddress(servername);
      long startCode =
        Bytes.toLong(rr.getValue(CATALOG_FAMILY, STARTCODE_QUALIFIER));
      String name = HServerInfo.getServerName(addr, startCode);
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
    status.setServerInfo(serverManager.serversToServerInfo.values());
    status.setDeadServers(serverManager.deadServers);
    status.setRegionsInTransition(this.regionManager.getRegionsInTransition());
    return status;
  }

  /**
   * @return Server metrics
   */
  public MasterMetrics getMetrics() {
    return this.metrics;
  }

  /*
   * Managing leases
   */

  /**
   * @return Return configuration being used by this server.
   */
  public HBaseConfiguration getConfiguration() {
    return this.conf;
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
   * Get the ZK wrapper object
   * @return the zookeeper wrapper
   */
  public ZooKeeperWrapper getZooKeeperWrapper() {
    return zooKeeperWrapper;
  }
  
  /**
   * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  @Override
  public void process(WatchedEvent event) {
    LOG.debug(("Got event " + event.getType() + 
        " with path " + event.getPath()));
    // Master should kill itself if its session expired or if its 
    // znode was deleted manually (usually for testing purposes)
    if(event.getState() == KeeperState.Expired || 
        (event.getType().equals(EventType.NodeDeleted) && 
            event.getPath().equals(
                this.zooKeeperWrapper.getMasterElectionZNode())) 
                && !shutdownRequested.get()) {
      LOG.error("Master lost its znode, killing itself now");
      System.exit(1);
    }
  }
   
  /*
   * Main program
   */

  private static void printUsageAndExit() {
    System.err.println("Usage: java org.apache.hbase.HMaster start|stop");
    System.exit(0);
  }

  protected static void doMain(String [] args,
      Class<? extends HMaster> masterClass) {

    if (args.length < 1) {
      printUsageAndExit();
    }

    HBaseConfiguration conf = new HBaseConfiguration();

    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).
    for (String cmd: args) {

      if (cmd.equals("start")) {
        try {
          RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
          if (runtime != null) {
            LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
              runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
            LOG.info("vmInputArguments=" + runtime.getInputArguments());
          }
          // If 'local', defer to LocalHBaseCluster instance.
          if (LocalHBaseCluster.isLocal(conf)) {
            (new LocalHBaseCluster(conf)).startup();
          } else {
            Constructor<? extends HMaster> c =
              masterClass.getConstructor(HBaseConfiguration.class);
            HMaster master = c.newInstance(conf);
            if(master.shutdownRequested.get()) {
              LOG.info("Won't bring the Master up as a shutdown is requested");
              return;
            }
            master.start();
          }
        } catch (Throwable t) {
          LOG.error( "Can not start master", t);
          System.exit(-1);
        }
        break;
      }

      if (cmd.equals("stop")) {
        HBaseAdmin adm = null;
        try {
          adm = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
          LOG.error("master is not running");
          System.exit(0);
        }
        try {
          adm.shutdown();
        } catch (Throwable t) {
          LOG.error( "Can not stop master", t);
          System.exit(-1);
        }
        break;
      }

      // Print out usage if we get to here.
      printUsageAndExit();
    }
  }
  
  /**
   * Main program
   * @param args
   */
  public static void main(String [] args) {
    doMain(args, HMaster.class);
  }

}
