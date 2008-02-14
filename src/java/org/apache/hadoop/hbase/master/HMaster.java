/**
 * Copyright 2007 The Apache Software Foundation
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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.ipc.HbaseRPC;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HConnection;
import org.apache.hadoop.hbase.HConnectionManager;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionInterface;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HRegion;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.LeaseListener;

/**
 * HMaster is the "master server" for a HBase.
 * There is only one HMaster for a single HBase deployment.
 */
public class HMaster extends Thread implements HConstants, HMasterInterface, 
  HMasterRegionInterface {
  
  static final Log LOG = LogFactory.getLog(HMaster.class.getName());
  static final Long ZERO_L = Long.valueOf(0L);

  /** {@inheritDoc} */
  public long getProtocolVersion(String protocol,
      @SuppressWarnings("unused") long clientVersion)
  throws IOException {
    if (protocol.equals(HMasterInterface.class.getName())) {
      return HMasterInterface.versionID; 
    } else if (protocol.equals(HMasterRegionInterface.class.getName())) {
      return HMasterRegionInterface.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  // We start out with closed flag on.  Using AtomicBoolean rather than
  // plain boolean because want to pass a reference to supporting threads
  // started here in HMaster rather than have them have to know about the
  // hosting class
  volatile AtomicBoolean closed = new AtomicBoolean(true);
  volatile boolean shutdownRequested = false;
  volatile AtomicInteger quiescedMetaServers = new AtomicInteger(0);
  volatile boolean fsOk = true;
  final Path rootdir;
  final HBaseConfiguration conf;
  final FileSystem fs;
  final Random rand;
  final int threadWakeFrequency; 
  final int numRetries;
  final long maxRegionOpenTime;

  volatile DelayQueue<RegionServerOperation> delayedToDoQueue =
    new DelayQueue<RegionServerOperation>();
  volatile BlockingQueue<RegionServerOperation> toDoQueue =
    new LinkedBlockingQueue<RegionServerOperation>();

  final int leaseTimeout;
  private final Leases serverLeases;
  private final Server server;
  private final HServerAddress address;

  final HConnection connection;

  final int metaRescanInterval;

  volatile AtomicReference<HServerAddress> rootRegionLocation =
    new AtomicReference<HServerAddress>(null);
  
  final Lock splitLogLock = new ReentrantLock();
  
  // A Sleeper that sleeps for threadWakeFrequency
  protected final Sleeper sleeper;
  
  // Default access so accesible from unit tests. MASTER is name of the webapp
  // and the attribute name used stuffing this instance into web context.
  InfoServer infoServer;
  
  /** Name of master server */
  public static final String MASTER = "master";

  public InfoServer getInfoServer() {
    return infoServer;
  }

  volatile boolean rootScanned = false;

  private final RootScanner rootScannerThread;
  final Integer rootScannerLock = new Integer(0);

  /** Set by root scanner to indicate the number of meta regions */
  volatile AtomicInteger numberOfMetaRegions = new AtomicInteger();

  /** Work for the meta scanner is queued up here */
  volatile BlockingQueue<MetaRegion> metaRegionsToScan =
    new LinkedBlockingQueue<MetaRegion>();

  /** These are the online meta regions */
  volatile SortedMap<Text, MetaRegion> onlineMetaRegions =
    Collections.synchronizedSortedMap(new TreeMap<Text, MetaRegion>());

  /** Set by meta scanner after initial scan */
  volatile boolean initialMetaScanComplete = false;

  final MetaScanner metaScannerThread;
  final Integer metaScannerLock = new Integer(0);

  /** The map of known server names to server info */
  volatile Map<String, HServerInfo> serversToServerInfo =
    new ConcurrentHashMap<String, HServerInfo>();
  
  /** Set of known dead servers */
  volatile Set<String> deadServers =
    Collections.synchronizedSet(new HashSet<String>());

  /** SortedMap server load -> Set of server names */
  volatile SortedMap<HServerLoad, Set<String>> loadToServers =
    Collections.synchronizedSortedMap(new TreeMap<HServerLoad, Set<String>>());

  /** Map of server names -> server load */
  volatile Map<String, HServerLoad> serversToLoad =
    new ConcurrentHashMap<String, HServerLoad>();

  /**
   * The 'unassignedRegions' table maps from a HRegionInfo to a timestamp that
   * indicates the last time we *tried* to assign the region to a RegionServer.
   * If the timestamp is out of date, then we can try to reassign it. 
   * 
   * We fill 'unassignedRecords' by scanning ROOT and META tables, learning the
   * set of all known valid regions.
   * 
   * <p>Items are removed from this list when a region server reports in that
   * the region has been deployed.
   */
  volatile SortedMap<HRegionInfo, Long> unassignedRegions =
    Collections.synchronizedSortedMap(new TreeMap<HRegionInfo, Long>());

  /**
   * Regions that have been assigned, and the server has reported that it has
   * started serving it, but that we have not yet recorded in the meta table.
   */
  volatile Set<Text> pendingRegions =
    Collections.synchronizedSet(new HashSet<Text>());

  /**
   * The 'killList' is a list of regions that are going to be closed, but not
   * reopened.
   */
  volatile Map<String, HashMap<Text, HRegionInfo>> killList =
    new ConcurrentHashMap<String, HashMap<Text, HRegionInfo>>();

  /** 'killedRegions' contains regions that are in the process of being closed */
  volatile Set<Text> killedRegions =
    Collections.synchronizedSet(new HashSet<Text>());

  /**
   * 'regionsToDelete' contains regions that need to be deleted, but cannot be
   * until the region server closes it
   */
  volatile Set<Text> regionsToDelete =
    Collections.synchronizedSet(new HashSet<Text>());

  /** Set of tables currently in creation. */
  private volatile Set<Text> tableInCreation = 
    Collections.synchronizedSet(new HashSet<Text>());

  /** Build the HMaster out of a raw configuration item.
   * 
   * @param conf - Configuration object
   * @throws IOException
   */
  public HMaster(HBaseConfiguration conf) throws IOException {
    this(new Path(conf.get(HBASE_DIR)),
        new HServerAddress(conf.get(MASTER_ADDRESS, DEFAULT_MASTER_ADDRESS)),
        conf);
  }

  /** 
   * Build the HMaster
   * @param rd base directory of this HBase instance.  Must be fully
   * qualified so includes filesystem to use.
   * @param address server address and port number
   * @param conf configuration
   * 
   * @throws IOException
   */
  public HMaster(Path rd, HServerAddress address, HBaseConfiguration conf)
  throws IOException {
    this.conf = conf;
    this.rootdir = rd;
    // The filesystem hbase wants to use is probably not what is set into
    // fs.default.name; its value is probably the default.
    this.conf.set("fs.default.name", this.rootdir.toString());
    this.fs = FileSystem.get(conf);
    this.conf.set(HConstants.HBASE_DIR, this.rootdir.toString());
    this.rand = new Random();
    Path rootRegionDir =
      HRegion.getRegionDir(rootdir, HRegionInfo.rootRegionInfo);
    LOG.info("Root region dir: " + rootRegionDir.toString());

    try {
      // Make sure the root directory exists!
      if(! fs.exists(rootdir)) {
        fs.mkdirs(rootdir);
        FSUtils.setVersion(fs, rootdir);
      } else if (!FSUtils.checkVersion(fs, rootdir)) {
        throw new IOException("File system needs upgrade. Run " +
          "the '${HBASE_HOME}/bin/hbase migrate' script");
      }

      if (!fs.exists(rootRegionDir)) {
        LOG.info("bootstrap: creating ROOT and first META regions");
        try {
          HRegion root = HRegion.createHRegion(HRegionInfo.rootRegionInfo,
              this.rootdir, this.conf);
          HRegion meta = HRegion.createHRegion(HRegionInfo.firstMetaRegionInfo,
            this.rootdir, this.conf);

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
    } catch (IOException e) {
      LOG.fatal("Not starting HMaster because:", e);
      throw e;
    }

    this.threadWakeFrequency = conf.getInt(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.maxRegionOpenTime =
      conf.getLong("hbase.hbasemaster.maxregionopen", 30 * 1000);

    this.leaseTimeout = conf.getInt("hbase.master.lease.period", 30 * 1000);
    this.serverLeases = new Leases(this.leaseTimeout, 
        conf.getInt("hbase.master.lease.thread.wakefrequency", 15 * 1000));
    
    this.server = HbaseRPC.getServer(this, address.getBindAddress(),
        address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);

    //  The rpc-server port can be ephemeral... ensure we have the correct info
    this.address = new HServerAddress(server.getListenerAddress());
    conf.set(MASTER_ADDRESS, address.toString());

    this.connection = HConnectionManager.getConnection(conf);

    this.metaRescanInterval =
      conf.getInt("hbase.master.meta.thread.rescanfrequency", 60 * 1000);

    // The root region
    this.rootScannerThread = new RootScanner(this);

    // Scans the meta table
    this.metaScannerThread = new MetaScanner(this);
    
    unassignRootRegion();

    this.sleeper = new Sleeper(this.threadWakeFrequency, this.closed);
    
    // We're almost open for business
    this.closed.set(false);
    LOG.info("HMaster initialized on " + this.address.toString());
  }
  
  /*
   * Unassign the root region.
   * This method would be used in case where root region server had died
   * without reporting in.  Currently, we just flounder and never recover.  We
   * could 'notice' dead region server in root scanner -- if we failed access
   * multiple times -- but reassigning root is catastrophic.
   * 
   */
  void unassignRootRegion() {
    this.rootRegionLocation.set(null);
    if (!this.shutdownRequested) {
      this.unassignedRegions.put(HRegionInfo.rootRegionInfo, ZERO_L);
    }
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  protected boolean checkFileSystem() {
    if (fsOk) {
      if (!FSUtils.isFileSystemAvailable(fs)) {
        LOG.fatal("Shutting down HBase cluster: file system not available");
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
    return Collections.unmodifiableMap(this.serversToServerInfo);
  }

  /**
   * @return Read-only map of servers to load.
   */
  public Map<String, HServerLoad> getServersToLoad() {
    return Collections.unmodifiableMap(this.serversToLoad);
  }

  /**
   * @return Location of the <code>-ROOT-</code> region.
   */
  public HServerAddress getRootRegionLocation() {
    HServerAddress rootServer = null;
    if (!shutdownRequested && !closed.get()) {
      rootServer = this.rootRegionLocation.get();
    }
    return rootServer;
  }
  
  /**
   * @return Read-only map of online regions.
   */
  public Map<Text, MetaRegion> getOnlineMetaRegions() {
    return Collections.unmodifiableSortedMap(this.onlineMetaRegions);
  }

  /** Main processing loop */
  @Override
  public void run() {
    final String threadName = "HMaster";
    Thread.currentThread().setName(threadName);
    startServiceThreads();
    /* Main processing loop */
    try {
      while (!closed.get()) {
        RegionServerOperation op = null;
        if (shutdownRequested && serversToServerInfo.size() == 0) {
          startShutdown();
          break;
        }
        if (rootRegionLocation.get() != null) {
          // We can't process server shutdowns unless the root region is online 
          op = this.delayedToDoQueue.poll();
        }
        if (op == null ) {
          try {
            op = toDoQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            // continue
          }
        }
        if (op == null || closed.get()) {
          continue;
        }
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Main processing loop: " + op.toString());
          }

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
          if (ex instanceof RemoteException) {
            try {
              ex = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException)ex);
            } catch (IOException e) {
              ex = e;
              LOG.warn("main processing loop: " + op.toString(), e);
            }
          }
          if (!checkFileSystem()) {
            break;
          }
          LOG.warn("Processing pending operations: " + op.toString(), ex);
          try {
            toDoQueue.put(op);
          } catch (InterruptedException e) {
            throw new RuntimeException(
                "Putting into toDoQueue was interrupted.", e);
          } catch (Exception e) {
            LOG.error("main processing loop: " + op.toString(), e);
          }
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
      this.closed.set(true);
    }
    // The region servers won't all exit until we stop scanning the meta regions
    stopScanners();
    
    // Wait for all the remaining region servers to report in.
    letRegionServersShutdown();

    /*
     * Clean up and close up shop
     */
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    server.stop();                      // Stop server
    serverLeases.close();               // Turn off the lease monitor

    // Join up with all threads
    try {
      if (rootScannerThread.isAlive()) {
        rootScannerThread.join();       // Wait for the root scanner to finish.
      }
    } catch (Exception iex) {
      LOG.warn("root scanner", iex);
    }
    try {
      if (metaScannerThread.isAlive()) {
        metaScannerThread.join();       // Wait for meta scanner to finish.
      }
    } catch(Exception iex) {
      LOG.warn("meta scanner", iex);
    }
    LOG.info("HMaster main thread exiting");
  }
  
  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message.  This should be fine because
   * in general, we do not expect the master to get such unhandled exceptions
   *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
   *  need to install an unexpected exception handler.
   */
  private void startServiceThreads() {
    String threadName = Thread.currentThread().getName();
    try {
      Threads.setDaemonThreadRunning(this.rootScannerThread,
        threadName + ".rootScanner");
      Threads.setDaemonThreadRunning(this.metaScannerThread,
        threadName + ".metaScanner");
      // Leases are not the same as Chore threads. Set name differently.
      this.serverLeases.setName(threadName + ".leaseChecker");
      this.serverLeases.start();
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started service threads");
    }
  }

  /*
   * Start shutting down the master
   */
  private void startShutdown() {
    closed.set(true);
    stopScanners();
    synchronized(toDoQueue) {
      toDoQueue.clear();                         // Empty the queue
      delayedToDoQueue.clear();                  // Empty shut down queue
      toDoQueue.notifyAll();                     // Wake main thread
    }
    synchronized (serversToServerInfo) {
      serversToServerInfo.notifyAll();
    }
  }

  /*
   * Stop the root and meta scanners so that the region servers serving meta
   * regions can shut down.
   */
  private void stopScanners() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("telling root scanner to stop");
    }
    synchronized(rootScannerLock) {
      if (rootScannerThread.isAlive()) {
        rootScannerThread.interrupt();  // Wake root scanner
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("telling meta scanner to stop");
    }
    synchronized(metaScannerLock) {
      if (metaScannerThread.isAlive()) {
        metaScannerThread.interrupt();  // Wake meta scanner
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("meta and root scanners notified");
    }
  }

  /*
   * Wait on regionservers to report in
   * with {@link #regionServerReport(HServerInfo, HMsg[])} so they get notice
   * the master is going down.  Waits until all region servers come back with
   * a MSG_REGIONSERVER_STOP which will cancel their lease or until leases held
   * by remote region servers have expired.
   */
  private void letRegionServersShutdown() {
    if (!fsOk) {
      // Forget waiting for the region servers if the file system has gone
      // away. Just exit as quickly as possible.
      return;
    }
    synchronized (serversToServerInfo) {
      while (this.serversToServerInfo.size() > 0) {
        LOG.info("Waiting on following regionserver(s) to go down (or " +
            "region server lease expiration, whichever happens first): " +
            this.serversToServerInfo.values());
        try {
          serversToServerInfo.wait(threadWakeFrequency);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }

  /*
   * HMasterRegionInterface
   */

  /** {@inheritDoc} */
  @SuppressWarnings("unused")
  public HbaseMapWritable regionServerStartup(HServerInfo serverInfo)
    throws IOException {

    String s = serverInfo.getServerAddress().toString().trim();
    LOG.info("received start message from: " + s);

    HServerLoad load = serversToLoad.remove(s);
    if (load != null) {
      // The startup message was from a known server.
      // Remove stale information about the server's load.
      Set<String> servers = loadToServers.get(load);
      if (servers != null) {
        servers.remove(s);
        loadToServers.put(load, servers);
      }
    }

    HServerInfo storedInfo = serversToServerInfo.remove(s);
    if (storedInfo != null && !closed.get()) {
      // The startup message was from a known server with the same name.
      // Timeout the old one right away.
      HServerAddress root = rootRegionLocation.get();
      if (root != null && root.equals(storedInfo.getServerAddress())) {
        unassignRootRegion();
      }
      delayedToDoQueue.put(new ProcessServerShutdown(this, storedInfo));
    }

    // record new server

    load = new HServerLoad();
    serverInfo.setLoad(load);
    serversToServerInfo.put(s, serverInfo);
    serversToLoad.put(s, load);
    Set<String> servers = loadToServers.get(load);
    if (servers == null) {
      servers = new HashSet<String>();
    }
    servers.add(s);
    loadToServers.put(load, servers);

    if (!closed.get()) {
      serverLeases.createLease(s, new ServerExpirer(s));
    }
    
    return createConfigurationSubset();
  }
  
  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected HbaseMapWritable createConfigurationSubset() {
    HbaseMapWritable mw = addConfig(new HbaseMapWritable(), HConstants.HBASE_DIR);
    return addConfig(mw, "fs.default.name");
  }

  private HbaseMapWritable addConfig(final HbaseMapWritable mw, final String key) {
    mw.put(new Text(key), new Text(this.conf.get(key)));
    return mw;
  }

  /** {@inheritDoc} */
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[])
  throws IOException {
    String serverName = serverInfo.getServerAddress().toString().trim();
    if (msgs.length > 0) {
      if (msgs[0].getMsg() == HMsg.MSG_REPORT_EXITING) {
        synchronized (serversToServerInfo) {
          try {
            // HRegionServer is shutting down. Cancel the server's lease.
            // Note that canceling the server's lease takes care of updating
            // serversToServerInfo, etc.
            if (LOG.isDebugEnabled()) {
              LOG.debug("Region server " + serverName +
              ": MSG_REPORT_EXITING -- cancelling lease");
            }

            if (cancelLease(serverName)) {
              // Only process the exit message if the server still has a lease.
              // Otherwise we could end up processing the server exit twice.
              LOG.info("Region server " + serverName +
              ": MSG_REPORT_EXITING -- lease cancelled");
              // Get all the regions the server was serving reassigned
              // (if we are not shutting down).
              if (!closed.get()) {
                for (int i = 1; i < msgs.length; i++) {
                  HRegionInfo info = msgs[i].getRegionInfo();
                  if (info.isRootRegion()) {
                    rootRegionLocation.set(null);
                  } else if (info.isMetaTable()) {
                    onlineMetaRegions.remove(info.getStartKey());
                  }

                  this.unassignedRegions.put(info, ZERO_L);
                }
              }
            }

            // We don't need to return anything to the server because it isn't
            // going to do any more work.
            return new HMsg[0];
          } finally {
            serversToServerInfo.notifyAll();
          }
        }
      } else if (msgs[0].getMsg() == HMsg.MSG_REPORT_QUIESCED) {
        LOG.info("Region server " + serverName + " quiesced");
        quiescedMetaServers.incrementAndGet();
      }
    }

    if(quiescedMetaServers.get() >= serversToServerInfo.size()) {
      // If the only servers we know about are meta servers, then we can
      // proceed with shutdown
      LOG.info("All user tables quiesced. Proceeding with shutdown");
      startShutdown();
    }

    if (shutdownRequested && !closed.get()) {
      // Tell the server to stop serving any user regions
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_QUIESCE)};
    }

    if (closed.get()) {
      // Tell server to shut down if we are shutting down.  This should
      // happen after check of MSG_REPORT_EXITING above, since region server
      // will send us one of these messages after it gets MSG_REGIONSERVER_STOP
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_STOP)};
    }

    HServerInfo storedInfo = serversToServerInfo.get(serverName);
    if (storedInfo == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("received server report from unknown server: " + serverName);
      }

      // The HBaseMaster may have been restarted.
      // Tell the RegionServer to start over and call regionServerStartup()

      return new HMsg[]{new HMsg(HMsg.MSG_CALL_SERVER_STARTUP)};

    } else if (storedInfo.getStartCode() != serverInfo.getStartCode()) {

      // This state is reachable if:
      //
      // 1) RegionServer A started
      // 2) RegionServer B started on the same machine, then 
      //    clobbered A in regionServerStartup.
      // 3) RegionServer A returns, expecting to work as usual.
      //
      // The answer is to ask A to shut down for good.

      if (LOG.isDebugEnabled()) {
        LOG.debug("region server race condition detected: " + serverName);
      }

      synchronized (serversToServerInfo) {
        cancelLease(serverName);
        serversToServerInfo.notifyAll();
      }
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_STOP)};

    } else {

      // All's well.  Renew the server's lease.
      // This will always succeed; otherwise, the fetch of serversToServerInfo
      // would have failed above.

      serverLeases.renewLease(serverName);

      // Refresh the info object and the load information

      serversToServerInfo.put(serverName, serverInfo);

      HServerLoad load = serversToLoad.get(serverName);
      if (load != null && !load.equals(serverInfo.getLoad())) {
        // We have previous information about the load on this server
        // and the load on this server has changed

        Set<String> servers = loadToServers.get(load);

        // Note that servers should never be null because loadToServers
        // and serversToLoad are manipulated in pairs

        servers.remove(serverName);
        loadToServers.put(load, servers);
      }

      // Set the current load information

      load = serverInfo.getLoad();
      serversToLoad.put(serverName, load);
      Set<String> servers = loadToServers.get(load);
      if (servers == null) {
        servers = new HashSet<String>();
      }
      servers.add(serverName);
      loadToServers.put(load, servers);

      // Next, process messages for this server
      return processMsgs(serverInfo, msgs);
    }
  }

  /** Cancel a server's lease and update its load information */
  private boolean cancelLease(final String serverName) {
    boolean leaseCancelled = false;
    HServerInfo info = serversToServerInfo.remove(serverName);
    if (info != null) {
      // Only cancel lease and update load information once.
      // This method can be called a couple of times during shutdown.
      if (rootRegionLocation.get() != null &&
          info.getServerAddress().equals(rootRegionLocation.get())) {
        unassignRootRegion();
      }
      LOG.info("Cancelling lease for " + serverName);
      serverLeases.cancelLease(serverName);
      leaseCancelled = true;

      // update load information
      HServerLoad load = serversToLoad.remove(serverName);
      if (load != null) {
        Set<String> servers = loadToServers.get(load);
        if (servers != null) {
          servers.remove(serverName);
          loadToServers.put(load, servers);
        }
      }
    }
    return leaseCancelled;
  }

  /** 
   * Process all the incoming messages from a server that's contacted us.
   * 
   * Note that we never need to update the server's load information because
   * that has already been done in regionServerReport.
   */
  private HMsg[] processMsgs(HServerInfo info, HMsg incomingMsgs[])
  throws IOException {
    
    ArrayList<HMsg> returnMsgs = new ArrayList<HMsg>();
    String serverName = info.getServerAddress().toString();
    HashMap<Text, HRegionInfo> regionsToKill = null;
    regionsToKill = killList.remove(serverName);

    // Get reports on what the RegionServer did.

    for (int i = 0; i < incomingMsgs.length; i++) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received " + incomingMsgs[i].toString() + " from " +
            serverName);
      }
      HRegionInfo region = incomingMsgs[i].getRegionInfo();

      switch (incomingMsgs[i].getMsg()) {

      case HMsg.MSG_REPORT_PROCESS_OPEN:
        synchronized (unassignedRegions) {
          // Region server has acknowledged request to open region.
          // Extend region open time by max region open time.
          unassignedRegions.put(region,
              System.currentTimeMillis() + this.maxRegionOpenTime);
        }
        break;
        
      case HMsg.MSG_REPORT_OPEN:
        boolean duplicateAssignment = false;
        synchronized (unassignedRegions) {
          if (unassignedRegions.remove(region) == null) {
            if (region.getRegionName().compareTo(
                HRegionInfo.rootRegionInfo.getRegionName()) == 0) {
              // Root region
              HServerAddress rootServer = rootRegionLocation.get();
              if (rootServer != null) {
                if (rootServer.toString().compareTo(serverName) == 0) {
                  // A duplicate open report from the correct server
                  break;
                }
                // We received an open report on the root region, but it is
                // assigned to a different server
                duplicateAssignment = true;
              }
            } else {
              // Not root region. If it is not a pending region, then we are
              // going to treat it as a duplicate assignment
              if (pendingRegions.contains(region.getRegionName())) {
                // A duplicate report from the correct server
                break;
              }
              // Although we can't tell for certain if this is a duplicate
              // report from the correct server, we are going to treat it
              // as such
              duplicateAssignment = true;
            }
          }
          if (duplicateAssignment) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("region server " + info.getServerAddress().toString()
                  + " should not have opened region " + region.getRegionName());
            }

            // This Region should not have been opened.
            // Ask the server to shut it down, but don't report it as closed.  
            // Otherwise the HMaster will think the Region was closed on purpose, 
            // and then try to reopen it elsewhere; that's not what we want.

            returnMsgs.add(
                new HMsg(HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT, region)); 

          } else {
            LOG.info(info.getServerAddress().toString() + " serving " +
                region.getRegionName());

            if (region.getRegionName().compareTo(
                HRegionInfo.rootRegionInfo.getRegionName()) == 0) {
              // Store the Root Region location (in memory)
              synchronized (rootRegionLocation) {
                this.rootRegionLocation.set(
                    new HServerAddress(info.getServerAddress()));
                this.rootRegionLocation.notifyAll();
              }
            } else {
              // Note that the table has been assigned and is waiting for the
              // meta table to be updated.

              pendingRegions.add(region.getRegionName());

              // Queue up an update to note the region location.

              try {
                toDoQueue.put(new ProcessRegionOpen(this, info, region));
              } catch (InterruptedException e) {
                throw new RuntimeException(
                    "Putting into toDoQueue was interrupted.", e);
              }
            } 
          }
        }
        break;

      case HMsg.MSG_REPORT_CLOSE:
        LOG.info(info.getServerAddress().toString() + " no longer serving " +
            region.getRegionName());

        if (region.getRegionName().compareTo(
            HRegionInfo.rootRegionInfo.getRegionName()) == 0) {

          // Root region

          if (region.isOffline()) {
            // Can't proceed without root region. Shutdown.
            LOG.fatal("root region is marked offline");
            shutdown();
          }
          unassignRootRegion();

        } else {
          boolean reassignRegion = !region.isOffline();
          boolean deleteRegion = false;

          if (killedRegions.remove(region.getRegionName())) {
            reassignRegion = false;
          }

          if (regionsToDelete.remove(region.getRegionName())) {
            reassignRegion = false;
            deleteRegion = true;
          }

          if (region.isMetaTable()) {
            // Region is part of the meta table. Remove it from onlineMetaRegions
            onlineMetaRegions.remove(region.getStartKey());
          }

          // NOTE: we cannot put the region into unassignedRegions as that
          //       could create a race with the pending close if it gets 
          //       reassigned before the close is processed.

          unassignedRegions.remove(region);

          try {
            toDoQueue.put(new ProcessRegionClose(this, region, reassignRegion,
                deleteRegion));

          } catch (InterruptedException e) {
            throw new RuntimeException(
                "Putting into toDoQueue was interrupted.", e);
          }
        }
        break;

      case HMsg.MSG_REPORT_SPLIT:
        // A region has split.

        HRegionInfo newRegionA = incomingMsgs[++i].getRegionInfo();
        unassignedRegions.put(newRegionA, ZERO_L);

        HRegionInfo newRegionB = incomingMsgs[++i].getRegionInfo();
        unassignedRegions.put(newRegionB, ZERO_L);

        LOG.info("region " + region.getRegionName() +
            " split. New regions are: " + newRegionA.getRegionName() + ", " +
            newRegionB.getRegionName());

        if (region.isMetaTable()) {
          // A meta region has split.

          onlineMetaRegions.remove(region.getStartKey());
          numberOfMetaRegions.incrementAndGet();
        }
        break;

      default:
        throw new IOException(
            "Impossible state during msg processing.  Instruction: " +
            incomingMsgs[i].getMsg());
      }
    }

    // Process the kill list

    if (regionsToKill != null) {
      for (HRegionInfo i: regionsToKill.values()) {
        returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE, i));
        killedRegions.add(i.getRegionName());
      }
    }

    // Figure out what the RegionServer ought to do, and write back.
    assignRegions(info, serverName, returnMsgs);
    return returnMsgs.toArray(new HMsg[returnMsgs.size()]);
  }
  
  /*
   * Assigns regions to region servers attempting to balance the load across
   * all region servers
   * 
   * @param info
   * @param serverName
   * @param returnMsgs
   */
  private void assignRegions(HServerInfo info, String serverName,
      ArrayList<HMsg> returnMsgs) {
    
    synchronized (this.unassignedRegions) {
      
      // We need to hold a lock on assign attempts while we figure out what to
      // do so that multiple threads do not execute this method in parallel
      // resulting in assigning the same region to multiple servers.
      
      long now = System.currentTimeMillis();
      Set<HRegionInfo> regionsToAssign = new HashSet<HRegionInfo>();
      for (Map.Entry<HRegionInfo, Long> e: this.unassignedRegions.entrySet()) {
      HRegionInfo i = e.getKey();
        if (numberOfMetaRegions.get() != onlineMetaRegions.size() &&
            !i.isMetaRegion()) {
          // Can't assign user regions until all meta regions have been assigned
          // and are on-line
          continue;
        }
        long diff = now - e.getValue().longValue();
        if (diff > this.maxRegionOpenTime) {
          regionsToAssign.add(e.getKey());
        }
      }
      int nRegionsToAssign = regionsToAssign.size();
      if (nRegionsToAssign <= 0) {
        // No regions to assign.  Return.
        return;
      }

      if (this.serversToServerInfo.size() == 1) {
        assignRegionsToOneServer(regionsToAssign, serverName, returnMsgs);
        // Finished.  Return.
        return;
      }

      // Multiple servers in play.
      // We need to allocate regions only to most lightly loaded servers.
      HServerLoad thisServersLoad = info.getLoad();
      int nregions = regionsPerServer(nRegionsToAssign, thisServersLoad);
      nRegionsToAssign -= nregions;
      if (nRegionsToAssign > 0) {
        // We still have more regions to assign. See how many we can assign
        // before this server becomes more heavily loaded than the next
        // most heavily loaded server.
        SortedMap<HServerLoad, Set<String>> heavyServers =
          new TreeMap<HServerLoad, Set<String>>();
        synchronized (this.loadToServers) {
          heavyServers.putAll(this.loadToServers.tailMap(thisServersLoad));
        }
        int nservers = 0;
        HServerLoad heavierLoad = null;
        for (Map.Entry<HServerLoad, Set<String>> e : heavyServers.entrySet()) {
          Set<String> servers = e.getValue();
          nservers += servers.size();
          if (e.getKey().compareTo(thisServersLoad) == 0) {
            // This is the load factor of the server we are considering
            nservers -= 1;
            continue;
          }

          // If we get here, we are at the first load entry that is a
          // heavier load than the server we are considering
          heavierLoad = e.getKey();
          break;
        }

        nregions = 0;
        if (heavierLoad != null) {
          // There is a more heavily loaded server
          for (HServerLoad load =
            new HServerLoad(thisServersLoad.getNumberOfRequests(),
                thisServersLoad.getNumberOfRegions());
          load.compareTo(heavierLoad) <= 0 && nregions < nRegionsToAssign;
          load.setNumberOfRegions(load.getNumberOfRegions() + 1), nregions++) {
            // continue;
          }
        }

        if (nregions < nRegionsToAssign) {
          // There are some more heavily loaded servers
          // but we can't assign all the regions to this server.
          if (nservers > 0) {
            // There are other servers that can share the load.
            // Split regions that need assignment across the servers.
            nregions = (int) Math.ceil((1.0 * nRegionsToAssign)
                / (1.0 * nservers));
          } else {
            // No other servers with same load.
            // Split regions over all available servers
            nregions = (int) Math.ceil((1.0 * nRegionsToAssign)
                / (1.0 * serversToServerInfo.size()));
          }
        } else {
          // Assign all regions to this server
          nregions = nRegionsToAssign;
        }

        now = System.currentTimeMillis();
        for (HRegionInfo regionInfo: regionsToAssign) {
          LOG.info("assigning region " + regionInfo.getRegionName() +
              " to server " + serverName);
          this.unassignedRegions.put(regionInfo, Long.valueOf(now));
          returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));
          if (--nregions <= 0) {
            break;
          }
        }
      }
    }
  }
  
  /*
   * @param nRegionsToAssign
   * @param thisServersLoad
   * @return How many regions we can assign to more lightly loaded servers
   */
  private int regionsPerServer(final int nRegionsToAssign,
      final HServerLoad thisServersLoad) {
    
    SortedMap<HServerLoad, Set<String>> lightServers =
      new TreeMap<HServerLoad, Set<String>>();
    
    synchronized (this.loadToServers) {
      lightServers.putAll(this.loadToServers.headMap(thisServersLoad));
    }

    int nRegions = 0;
    for (Map.Entry<HServerLoad, Set<String>> e : lightServers.entrySet()) {
      HServerLoad lightLoad = new HServerLoad(e.getKey().getNumberOfRequests(),
          e.getKey().getNumberOfRegions());
      do {
        lightLoad.setNumberOfRegions(lightLoad.getNumberOfRegions() + 1);
        nRegions += 1;
      } while (lightLoad.compareTo(thisServersLoad) <= 0
          && nRegions < nRegionsToAssign);

      nRegions *= e.getValue().size();
      if (nRegions >= nRegionsToAssign) {
        break;
      }
    }
    return nRegions;
  }
  
  /*
   * Assign all to the only server. An unlikely case but still possible.
   * @param regionsToAssign
   * @param serverName
   * @param returnMsgs
   */
  private void assignRegionsToOneServer(final Set<HRegionInfo> regionsToAssign,
      final String serverName, final ArrayList<HMsg> returnMsgs) {
    long now = System.currentTimeMillis();
    for (HRegionInfo regionInfo: regionsToAssign) {
      LOG.info("assigning region " + regionInfo.getRegionName() +
          " to the only server " + serverName);
      this.unassignedRegions.put(regionInfo, Long.valueOf(now));
      returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));
    }
  }

  /*
   * HMasterInterface
   */

  /** {@inheritDoc} */
  public boolean isMasterRunning() {
    return !closed.get();
  }

  /** {@inheritDoc} */
  public void shutdown() {
    LOG.info("Cluster shutdown requested. Starting to quiesce servers");
    this.shutdownRequested = true;
  }

  /** {@inheritDoc} */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo newRegion = new HRegionInfo(desc, null, null);

    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // We can not access meta regions if they have not already been
        // assigned and scanned.  If we timeout waiting, just shutdown.
        if (this.metaScannerThread.waitForMetaRegionsOrClose()) {
          break;
        }
        createTable(newRegion);
        LOG.info("created table " + desc.getName());
        break;
      
      } catch (IOException e) {
        if (tries == numRetries - 1) {
          throw RemoteExceptionHandler.checkIOException(e);
        }
      }
    }
  }

  private void createTable(final HRegionInfo newRegion) throws IOException {
    Text tableName = newRegion.getTableDesc().getName();
    if (tableInCreation.contains(tableName)) {
      throw new TableExistsException("Table " + tableName + " in process "
          + "of being created");
    }
    tableInCreation.add(tableName);
    try {
      // 1. Check to see if table already exists. Get meta region where
      // table would sit should it exist. Open scanner on it. If a region
      // for the table we want to create already exists, then table already
      // created. Throw already-exists exception.
      
      MetaRegion m = null;
      synchronized (onlineMetaRegions) {
        m = (onlineMetaRegions.size() == 1 ?
            onlineMetaRegions.get(onlineMetaRegions.firstKey()) : 
              (onlineMetaRegions.containsKey(newRegion.getRegionName()) ?
                  onlineMetaRegions.get(newRegion.getRegionName()) :
                    onlineMetaRegions.get(onlineMetaRegions.headMap(
                        newRegion.getTableDesc().getName()).lastKey())));
      }
          
      Text metaRegionName = m.getRegionName();
      HRegionInterface srvr = connection.getHRegionConnection(m.getServer());
      long scannerid = srvr.openScanner(metaRegionName, COL_REGIONINFO_ARRAY,
          tableName, System.currentTimeMillis(), null);
      try {
        HbaseMapWritable data = srvr.next(scannerid);
            
        // Test data and that the row for the data is for our table. If table
        // does not exist, scanner will return row after where our table would
        // be inserted if it exists so look for exact match on table name.
            
        if (data != null && data.size() > 0) {
          for (Writable k: data.keySet()) {
            if (HRegionInfo.getTableNameFromRegionName(
                ((HStoreKey) k).getRow()).equals(tableName)) {
          
              // Then a region for this table already exists. Ergo table exists.
                  
              throw new TableExistsException(tableName.toString());
            }
          }
        }
            
      } finally {
        srvr.close(scannerid);
      }

      // 2. Create the HRegion
          
      HRegion region =
        HRegion.createHRegion(newRegion, this.rootdir, this.conf);

      // 3. Insert into meta
          
      HRegionInfo info = region.getRegionInfo();
      Text regionName = region.getRegionName();
      BatchUpdate b = new BatchUpdate(regionName);
      b.put(COL_REGIONINFO, Writables.getBytes(info));
      srvr.batchUpdate(metaRegionName, b);
      
      // 4. Close the new region to flush it to disk.  Close its log file too.
      
      region.close();
      region.getLog().closeAndDelete();

      // 5. Get it assigned to a server

      this.unassignedRegions.put(info, ZERO_L);

    } finally {
      tableInCreation.remove(newRegion.getTableDesc().getName());
    }
  }

  /** {@inheritDoc} */
  public void deleteTable(Text tableName) throws IOException {
    new TableDelete(this, tableName).process();
    LOG.info("deleted table: " + tableName);
  }

  /** {@inheritDoc} */
  public void addColumn(Text tableName, HColumnDescriptor column)
  throws IOException {    
    new AddColumn(this, tableName, column).process();
  }

  /** {@inheritDoc} */
  public void modifyColumn(Text tableName, Text columnName, 
    HColumnDescriptor descriptor)
  throws IOException {
    new ModifyColumn(this, tableName, columnName, descriptor).process();
  }

  /** {@inheritDoc} */
  public void deleteColumn(Text tableName, Text columnName) throws IOException {
    new DeleteColumn(this, tableName, 
      HStoreKey.extractFamily(columnName)).process();
  }

  /** {@inheritDoc} */
  public void enableTable(Text tableName) throws IOException {
    new ChangeTableState(this, tableName, true).process();
  }

  /** {@inheritDoc} */
  public void disableTable(Text tableName) throws IOException {
    new ChangeTableState(this, tableName, false).process();
  }

  /** {@inheritDoc} */
  public HServerAddress findRootRegion() {
    return rootRegionLocation.get();
  }

  /*
   * Managing leases
   */

  /** Instantiated to monitor the health of a region server */
  private class ServerExpirer implements LeaseListener {
    @SuppressWarnings("hiding")
    private String server;

    ServerExpirer(String server) {
      this.server = server;
    }

    /** {@inheritDoc} */
    public void leaseExpired() {
      LOG.info(server + " lease expired");
      // Remove the server from the known servers list and update load info
      HServerInfo info = serversToServerInfo.remove(server);
      if (info != null) {
        HServerAddress root = rootRegionLocation.get();
        if (root != null && root.equals(info.getServerAddress())) {
          unassignRootRegion();
        }
        String serverName = info.getServerAddress().toString();
        HServerLoad load = serversToLoad.remove(serverName);
        if (load != null) {
          Set<String> servers = loadToServers.get(load);
          if (servers != null) {
            servers.remove(serverName);
            loadToServers.put(load, servers);
          }
        }
        deadServers.add(server);
      }
      synchronized (serversToServerInfo) {
        serversToServerInfo.notifyAll();
      }

      // NOTE: If the server was serving the root region, we cannot reassign it
      // here because the new server will start serving the root region before
      // the ProcessServerShutdown operation has a chance to split the log file.
      if (info != null) {
        delayedToDoQueue.put(new ProcessServerShutdown(HMaster.this, info));
      }
    }
  }

  /**
   * @return Return configuration being used by this server.
   */
  public HBaseConfiguration getConfiguration() {
    return this.conf;
  }
    
  /*
   * Get HRegionInfo from passed META map of row values.
   * Returns null if none found (and logs fact that expected COL_REGIONINFO
   * was missing).  Utility method used by scanners of META tables.
   * @param map Map to do lookup in.
   * @return Null or found HRegionInfo.
   * @throws IOException
   */
  HRegionInfo getHRegionInfo(final Map<Text, byte[]> map)
  throws IOException {
    byte [] bytes = map.get(COL_REGIONINFO);
    if (bytes == null) {
      LOG.warn(COL_REGIONINFO.toString() + " is empty; has keys: " +
        map.keySet().toString());
      return null;
    }
    return (HRegionInfo)Writables.getWritable(bytes, new HRegionInfo());
  }

  /*
   * Main program
   */

  private static void printUsageAndExit() {
    System.err.println("Usage: java org.apache.hbase.HMaster " +
    "[--bind=hostname:port] start|stop");
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

    final String addressArgKey = "--bind=";
    for (String cmd: args) {
      if (cmd.startsWith(addressArgKey)) {
        conf.set(MASTER_ADDRESS, cmd.substring(addressArgKey.length()));
        continue;
      }

      if (cmd.equals("start")) {
        try {
          // If 'local', defer to LocalHBaseCluster instance.
          if (LocalHBaseCluster.isLocal(conf)) {
            (new LocalHBaseCluster(conf)).startup();
          } else {
            Constructor<? extends HMaster> c =
              masterClass.getConstructor(HBaseConfiguration.class);
            HMaster master = c.newInstance(conf);
            master.start();
          }
        } catch (Throwable t) {
          LOG.error( "Can not start master", t);
          System.exit(-1);
        }
        break;
      }

      if (cmd.equals("stop")) {
        try {
          if (LocalHBaseCluster.isLocal(conf)) {
            LocalHBaseCluster.doLocal(conf);
          }
          HBaseAdmin adm = new HBaseAdmin(conf);
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
