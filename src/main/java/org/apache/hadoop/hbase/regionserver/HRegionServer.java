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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterAddressTracker;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.RootLocationEditor;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.MultiPutResponse;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.regionserver.handler.CloseMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRootHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRootHandler;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALObserver;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
public class HRegionServer implements HRegionInterface, HBaseRPCErrorHandler,
    Runnable, RegionServerServices, Server {
  public static final Log LOG = LogFactory.getLog(HRegionServer.class);

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  protected volatile boolean stopped = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;

  private volatile boolean killed = false;

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;

  protected HServerInfo serverInfo;
  protected final Configuration conf;

  private final HConnection connection;
  protected final AtomicBoolean haveRootRegion = new AtomicBoolean(false);
  private FileSystem fs;
  private Path rootDir;
  private final Random rand = new Random();

  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.
   */
  protected final Map<String, HRegion> onlineRegions =
    new ConcurrentHashMap<String, HRegion>();

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final LinkedBlockingQueue<HMsg> outboundMsgs = new LinkedBlockingQueue<HMsg>();

  final int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;

  protected final int numRegionsToReport;

  private final long maxScannerResultSize;

  // Remote HMaster
  private HMasterRegionInterface hbaseMaster;

  // Server to handle client requests. Default access so can be accessed by
  // unit tests.
  HBaseServer server;

  // Leases
  private Leases leases;

  // Request counter
  private volatile AtomicInteger requestCount = new AtomicInteger();

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  InfoServer infoServer;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  /*
   * Space is reserved in HRS constructor and then released when aborting to
   * recover from an OOME. See HBASE-706. TODO: Make this percentage of the heap
   * or a minimum.
   */
  private final LinkedList<byte[]> reservedSpace = new LinkedList<byte[]>();

  private RegionServerMetrics metrics;

  // Compactions
  CompactSplitThread compactSplitThread;

  // Cache flushing
  MemStoreFlusher cacheFlusher;

  /*
   * Check for major compactions.
   */
  Chore majorCompactionChecker;

  // HLog and HLog roller. log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  protected volatile HLog hlog;
  LogRoller hlogRoller;

  // flag set after we're done setting up server threads (used for testing)
  protected volatile boolean isOnline;

  final Map<String, InternalScanner> scanners = new ConcurrentHashMap<String, InternalScanner>();

  // zookeeper connection and watcher
  private ZooKeeperWatcher zooKeeper;

  // master address manager and watcher
  private MasterAddressTracker masterAddressManager;

  // catalog tracker
  private CatalogTracker catalogTracker;

  // Cluster Status Tracker
  private ClusterStatusTracker clusterStatusTracker;

  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  private final long rpcTimeout;

  // Address passed in to constructor. This is not always the address we run
  // with. For example, if passed port is 0, then we are to pick a port. The
  // actual address we run with is in the #serverInfo data member.
  private final HServerAddress address;

  // The main region server thread.
  @SuppressWarnings("unused")
  private Thread regionServerThread;

  private final String machineName;

  // Instance of the hbase executor service.
  private ExecutorService service;

  // Replication services. If no replication, this handler will be null.
  private Replication replicationHandler;

  /**
   * Starts a HRegionServer at the default location
   *
   * @param conf
   * @throws IOException
   * @throws InterruptedException 
   */
  public HRegionServer(Configuration conf) throws IOException, InterruptedException {
    this.machineName = DNS.getDefaultHost(conf.get(
        "hbase.regionserver.dns.interface", "default"), conf.get(
        "hbase.regionserver.dns.nameserver", "default"));
    String addressStr = machineName
        + ":"
        + conf.get(HConstants.REGIONSERVER_PORT, Integer
            .toString(HConstants.DEFAULT_REGIONSERVER_PORT));
    // This is not necessarily the address we will run with. The address we
    // use will be in #serverInfo data member. For example, we may have been
    // passed a port of 0 which means we should pick some ephemeral port to bind
    // to.
    this.address = new HServerAddress(addressStr);

    this.fsOk = true;
    this.conf = conf;
    this.connection = HConnectionManager.getConnection(conf);
    this.isOnline = false;

    // Config'ed params
    this.numRetries = conf.getInt("hbase.client.retries.number", 2);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 1 * 1000);

    sleeper = new Sleeper(this.msgInterval, this);

    this.maxScannerResultSize = conf.getLong(
        HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);

    this.numRegionsToReport = conf.getInt(
        "hbase.regionserver.numregionstoreport", 10);

    this.rpcTimeout = conf.getLong(
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
        HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD);

    initialize();
  }

  private static final int NORMAL_QOS = 0;
  private static final int QOS_THRESHOLD = 10;  // the line between low and high qos
  private static final int HIGH_QOS = 100;

  class QosFunction implements Function<Writable,Integer> {
    public boolean isMetaRegion(byte[] regionName) {
      HRegion region;
      try {
        region = getRegion(regionName);
      } catch (NotServingRegionException ignored) {
        return false;
      }
      return region.getRegionInfo().isMetaRegion();
    }

    @Override
    public Integer apply(Writable from) {
      if (from instanceof HBaseRPC.Invocation) {
        HBaseRPC.Invocation inv = (HBaseRPC.Invocation) from;

        String methodName = inv.getMethodName();

        // scanner methods...
        if (methodName.equals("next") || methodName.equals("close")) {
          // translate!
          Long scannerId;
          try {
            scannerId = (Long) inv.getParameters()[0];
          } catch (ClassCastException ignored) {
            //LOG.debug("Low priority: " + from);
            return NORMAL_QOS; // doh.
          }
          String scannerIdString = Long.toString(scannerId);
          InternalScanner scanner = scanners.get(scannerIdString);
          if (scanner instanceof HRegion.RegionScanner) {
            HRegion.RegionScanner rs = (HRegion.RegionScanner) scanner;
            HRegionInfo regionName = rs.getRegionName();
            if (regionName.isMetaRegion()) {
              //LOG.debug("High priority scanner request: " + scannerId);
              return HIGH_QOS;
            }
          }
        }
        else if (methodName.equals("getHServerInfo") ||
            methodName.equals("getRegionsAssignment") ||
            methodName.equals("unlockRow") ||
            methodName.equals("getProtocolVersion") ||
            methodName.equals("getClosestRowBefore")) {
          //LOG.debug("High priority method: " + methodName);
          return HIGH_QOS;
        }
        else if (inv.getParameterClasses()[0] == byte[].class) {
          // first arg is byte array, so assume this is a regionname:
          if (isMetaRegion((byte[]) inv.getParameters()[0])) {
            //LOG.debug("High priority with method: " + methodName + " and region: "
            //    + Bytes.toString((byte[]) inv.getParameters()[0]));
            return HIGH_QOS;
          }
        }
        else if (inv.getParameterClasses()[0] == MultiAction.class) {
          MultiAction ma = (MultiAction) inv.getParameters()[0];
          Set<byte[]> regions = ma.getRegions();
          // ok this sucks, but if any single of the actions touches a meta, the whole
          // thing gets pingged high priority.  This is a dangerous hack because people
          // can get their multi action tagged high QOS by tossing a Get(.META.) AND this
          // regionserver hosts META/-ROOT-
          for (byte[] region: regions) {
            if (isMetaRegion(region)) {
              //LOG.debug("High priority multi with region: " + Bytes.toString(region));
              return HIGH_QOS; // short circuit for the win.
            }
          }
        }
      }
      //LOG.debug("Low priority: " + from.toString());
      return NORMAL_QOS;
    }
  }

  /**
   * Creates all of the state that needs to be reconstructed in case we are
   * doing a restart. This is shared between the constructor and restart(). Both
   * call it.
   *
   * @throws IOException
   * @throws InterruptedException 
   */
  private void initialize() throws IOException, InterruptedException {
    this.abortRequested = false;
    this.stopped = false;

    // Server to handle client requests
    this.server = HBaseRPC.getServer(this,
        new Class<?>[]{HRegionInterface.class, HBaseRPCErrorHandler.class,
        OnlineRegions.class},
        address.getBindAddress(),
      address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        conf.getInt("hbase.regionserver.metahandler.count", 10),
        false, conf, QOS_THRESHOLD);
    this.server.setErrorHandler(this);
    this.server.setQosFunction(new QosFunction());

    // Address is giving a default IP for the moment. Will be changed after
    // calling the master.
    this.serverInfo = new HServerInfo(new HServerAddress(new InetSocketAddress(
        address.getBindAddress(), this.server.getListenerAddress().getPort())),
        System.currentTimeMillis(), this.conf.getInt(
            "hbase.regionserver.info.port", 60030), machineName);
    if (this.serverInfo.getServerAddress() == null) {
      throw new NullPointerException("Server address cannot be null; "
          + "hbase-958 debugging");
    }
    initializeZooKeeper();
    initializeThreads();
    int nbBlocks = conf.getInt("hbase.regionserver.nbreservationblocks", 4);
    for (int i = 0; i < nbBlocks; i++) {
      reservedSpace.add(new byte[HConstants.DEFAULT_SIZE_RESERVATION_BLOCK]);
    }
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this
   * cluster and then after that, wait until cluster 'up' flag has been set.
   * This is the order in which master does things.
   * Finally put up a catalog tracker.
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZooKeeper() throws IOException, InterruptedException {
    // Open connection to zookeeper and set primary watcher
    zooKeeper = new ZooKeeperWatcher(conf, REGIONSERVER +
      serverInfo.getServerAddress().getPort(), this);

    // Create the master address manager, register with zk, and start it.  Then
    // block until a master is available.  No point in starting up if no master
    // running.
    this.masterAddressManager = new MasterAddressTracker(zooKeeper, this);
    this.masterAddressManager.start();
    this.masterAddressManager.blockUntilAvailable();

    // Wait on cluster being up.  Master will set this flag up in zookeeper
    // when ready.
    this.clusterStatusTracker = new ClusterStatusTracker(this.zooKeeper, this);
    this.clusterStatusTracker.start();
    this.clusterStatusTracker.blockUntilAvailable();

    // Create the catalog tracker and start it; 
    this.catalogTracker = new CatalogTracker(this.zooKeeper, this.connection,
      this, this.conf.getInt("hbase.regionserver.catalog.timeout", Integer.MAX_VALUE));
    catalogTracker.start();
  }

  /**
   * @return True if cluster shutdown in progress
   */
  private boolean isClusterUp() {
    return this.clusterStatusTracker.isClusterUp();
  }

  private void initializeThreads() throws IOException {

    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplitThread(this);

    // Background thread to check for major compactions; needed if region
    // has not gotten updates in a while. Make it run at a lesser frequency.
    int multiplier = this.conf.getInt(HConstants.THREAD_WAKE_FREQUENCY
        + ".multiplier", 1000);
    this.majorCompactionChecker = new MajorCompactionChecker(this,
        this.threadWakeFrequency * multiplier, this);

    this.leases = new Leases((int) conf.getLong(
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
        HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD),
        this.threadWakeFrequency);
  }

  /**
   * The HRegionServer sticks in this loop until closed. It repeatedly checks in
   * with the HMaster, sending heartbeats & reports, and receiving HRegion
   * load/unload instructions.
   */
  public void run() {
    this.regionServerThread = Thread.currentThread();
    boolean calledCloseUserRegions = false;
    try {
      while (!this.stopped) {
        if (tryReportForDuty()) break;
      }
      long lastMsg = 0;
      List<HMsg> outboundMessages = new ArrayList<HMsg>();
      // The main run loop.
      for (int tries = 0; !this.stopped && isHealthy();) {
        if (!isClusterUp()) {
          if (this.onlineRegions.isEmpty()) {
            stop("Exiting; cluster shutdown set and not carrying any regions");
          } else if (!calledCloseUserRegions) {
            closeUserRegions(this.abortRequested);
            calledCloseUserRegions = true;
          }
        }
        // Try to get the root region location from zookeeper.
        this.catalogTracker.waitForRoot();
        long now = System.currentTimeMillis();
        // Drop into the send loop if msgInterval has elapsed or if something
        // to send. If we fail talking to the master, then we'll sleep below
        // on poll of the outboundMsgs blockingqueue.
        if ((now - lastMsg) >= msgInterval || !outboundMessages.isEmpty()) {
          try {
            doMetrics();
            tryRegionServerReport(outboundMessages);
            lastMsg = System.currentTimeMillis();
            // Reset tries count if we had a successful transaction.
            tries = 0;
            if (this.stopped) continue;
          } catch (Exception e) { // FindBugs REC_CATCH_EXCEPTION
            // Two special exceptions could be printed out here,
            // PleaseHoldException and YouAreDeadException
            if (e instanceof IOException) {
              e = RemoteExceptionHandler.checkIOException((IOException) e);
            }
            if (e instanceof YouAreDeadException) {
              // This will be caught and handled as a fatal error below
              throw e;
            }
            tries++;
            if (tries > 0 && (tries % this.numRetries) == 0) {
              // Check filesystem every so often.
              checkFileSystem();
            }
            if (this.stopped) {
              continue;
            }
            LOG.warn("Attempt=" + tries, e);
            // No point retrying immediately; this is probably connection to
            // master issue. Doing below will cause us to sleep.
            lastMsg = System.currentTimeMillis();
          }
        }
        now = System.currentTimeMillis();
        HMsg msg = this.outboundMsgs.poll((msgInterval - (now - lastMsg)), TimeUnit.MILLISECONDS);
        if (msg != null) outboundMessages.add(msg);
      } // for
    } catch (Throwable t) {
      if (!checkOOME(t)) {
        abort("Unhandled exception", t);
      }
    }
    this.leases.closeAfterLeasesExpire();
    this.server.stop();
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // Send cache a shutdown.
    LruBlockCache c = (LruBlockCache) StoreFile.getBlockCache(this.conf);
    if (c != null) {
      c.shutdown();
    }

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive? If OOME could have exited already
    cacheFlusher.interruptIfNecessary();
    compactSplitThread.interruptIfNecessary();
    hlogRoller.interruptIfNecessary();
    this.majorCompactionChecker.interrupt();

    if (killed) {
      // Just skip out w/o closing regions.
    } else if (abortRequested) {
      if (this.fsOk) {
        closeAllRegions(abortRequested); // Don't leave any open file handles
        closeWAL(false);
      }
      LOG.info("aborting server at: " + this.serverInfo.getServerName());
    } else {
      closeAllRegions(abortRequested);
      closeWAL(true);
      closeAllScanners();
      LOG.info("stopping server at: " + this.serverInfo.getServerName());
    }
    waitOnAllRegionsToClose();

    // Make sure the proxy is down.
    if (this.hbaseMaster != null) {
      HBaseRPC.stopProxy(this.hbaseMaster);
      this.hbaseMaster = null;
    }
    this.leases.close();
    HConnectionManager.deleteConnection(conf, true);
    this.zooKeeper.close();
    if (!killed) {
      join();
    }
    LOG.info(Thread.currentThread().getName() + " exiting");
  }

  /**
   * Wait on regions close.
   */
  private void waitOnAllRegionsToClose() {
    // Wait till all regions are closed before going out.
    int lastCount = -1;
    while (!this.onlineRegions.isEmpty()) {
      int count = this.onlineRegions.size();
      // Only print a message if the count of regions has changed.
      if (count != lastCount) {
        lastCount = count;
        LOG.info("Waiting on " + count + " regions to close");
        // Only print out regions still closing if a small number else will
        // swamp the log.
        if (count < 10) {
          LOG.debug(this.onlineRegions);
        }
      }
      Threads.sleep(1000);
    }
  }

  List<HMsg> tryRegionServerReport(final List<HMsg> outboundMessages)
  throws IOException {
    this.serverInfo.setLoad(buildServerLoad());
    this.requestCount.set(0);
    addOutboundMsgs(outboundMessages);
    HMsg [] msgs = this.hbaseMaster.regionServerReport(this.serverInfo,
      outboundMessages.toArray(HMsg.EMPTY_HMSG_ARRAY),
      getMostLoadedRegions());
    updateOutboundMsgs(outboundMessages);
    outboundMessages.clear();

    for (int i = 0; !this.stopped && msgs != null && i < msgs.length; i++) {
      LOG.info(msgs[i].toString());
      // Intercept stop regionserver messages
      if (msgs[i].getType().equals(HMsg.Type.STOP_REGIONSERVER)) {
        stop("Received " + msgs[i]);
        continue;
      }
      LOG.warn("NOT PROCESSING " + msgs[i] + " -- WHY IS MASTER SENDING IT TO US?");
    }
    return outboundMessages;
  }

  private HServerLoad buildServerLoad() {
    MemoryUsage memory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    HServerLoad hsl = new HServerLoad(requestCount.get(),
      (int)(memory.getUsed() / 1024 / 1024),
      (int) (memory.getMax() / 1024 / 1024));
    for (HRegion r : this.onlineRegions.values()) {
      hsl.addRegionInfo(createRegionLoad(r));
    }
    return hsl;
  }

  private void closeWAL(final boolean delete) {
    try {
      if (this.hlog != null) {
        if (delete) {
          hlog.closeAndDelete();
        } else {
          hlog.close();
        }
      }
    } catch (Throwable e) {
      LOG.error("Close and delete failed", RemoteExceptionHandler.checkThrowable(e));
    }
  }

  private void closeAllScanners() {
    // Close any outstanding scanners. Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, InternalScanner> e : this.scanners.entrySet()) {
      try {
        e.getValue().close();
      } catch (IOException ioe) {
        LOG.warn("Closing scanner " + e.getKey(), ioe);
      }
    }
  }

  /*
   * Add to the passed <code>msgs</code> messages to pass to the master.
   *
   * @param msgs Current outboundMsgs array; we'll add messages to this List.
   */
  private void addOutboundMsgs(final List<HMsg> msgs) {
    if (msgs.isEmpty()) {
      this.outboundMsgs.drainTo(msgs);
      return;
    }
    OUTER: for (HMsg m : this.outboundMsgs) {
      for (HMsg mm : msgs) {
        // Be careful don't add duplicates.
        if (mm.equals(m)) {
          continue OUTER;
        }
      }
      msgs.add(m);
    }
  }

  /*
   * Remove from this.outboundMsgs those messsages we sent the master.
   *
   * @param msgs Messages we sent the master.
   */
  private void updateOutboundMsgs(final List<HMsg> msgs) {
    if (msgs.isEmpty()) {
      return;
    }
    for (HMsg m : this.outboundMsgs) {
      for (HMsg mm : msgs) {
        if (mm.equals(m)) {
          this.outboundMsgs.remove(m);
          break;
        }
      }
    }
  }

  /*
   * Run init. Sets up hlog and starts up all server threads.
   *
   * @param c Extra configuration.
   */
  protected void handleReportForDutyResponse(final MapWritable c) throws IOException {
    try {
      for (Map.Entry<Writable, Writable> e : c.entrySet()) {
        String key = e.getKey().toString();
        String value = e.getValue().toString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Config from master: " + key + "=" + value);
        }
        this.conf.set(key, value);
      }
      // Master may have sent us a new address with the other configs.
      // Update our address in this case. See HBASE-719
      String hra = conf.get("hbase.regionserver.address");
      // TODO: The below used to be this.address != null. Was broken by what
      // looks like a mistake in:
      //
      // HBASE-1215 migration; metautils scan of meta region was broken;
      // wouldn't see first row
      // ------------------------------------------------------------------------
      // r796326 | stack | 2009-07-21 07:40:34 -0700 (Tue, 21 Jul 2009) | 38
      // lines
      if (hra != null) {
        HServerAddress hsa = new HServerAddress(hra, this.serverInfo
            .getServerAddress().getPort());
        LOG.info("Master passed us address to use. Was="
            + this.serverInfo.getServerAddress() + ", Now=" + hsa.toString());
        this.serverInfo.setServerAddress(hsa);
      }

      // hack! Maps DFSClient => RegionServer for logs.  HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapred.task.id") == null) {
        this.conf.set("mapred.task.id", 
            "hb_rs_" + this.serverInfo.getServerName() + "_" +
            System.currentTimeMillis());
      }

      // Master sent us hbase.rootdir to use. Should be fully qualified
      // path with file system specification included. Set 'fs.defaultFS'
      // to match the filesystem on hbase.rootdir else underlying hadoop hdfs
      // accessors will be going against wrong filesystem (unless all is set
      // to defaults).
      this.conf.set("fs.defaultFS", this.conf.get("hbase.rootdir"));
      // Get fs instance used by this RS
      this.fs = FileSystem.get(this.conf);
      this.rootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
      this.hlog = setupWALAndReplication();
      // Init in here rather than in constructor after thread name has been set
      this.metrics = new RegionServerMetrics();
      startServiceThreads();
      LOG.info("Serving as " + this.serverInfo.getServerName() +
        ", sessionid=0x" +
        Long.toHexString(this.zooKeeper.getZooKeeper().getSessionId()));
      isOnline = true;
    } catch (Throwable e) {
      this.isOnline = false;
      stop("Failed initialization");
      throw convertThrowableToIOE(cleanup(e, "Failed init"),
          "Region server startup failed");
    }
  }

  /*
   * @param r Region to get RegionLoad for.
   *
   * @return RegionLoad instance.
   *
   * @throws IOException
   */
  private HServerLoad.RegionLoad createRegionLoad(final HRegion r) {
    byte[] name = r.getRegionName();
    int stores = 0;
    int storefiles = 0;
    int storefileSizeMB = 0;
    int memstoreSizeMB = (int) (r.memstoreSize.get() / 1024 / 1024);
    int storefileIndexSizeMB = 0;
    synchronized (r.stores) {
      stores += r.stores.size();
      for (Store store : r.stores.values()) {
        storefiles += store.getStorefilesCount();
        storefileSizeMB += (int) (store.getStorefilesSize() / 1024 / 1024);
        storefileIndexSizeMB += (int) (store.getStorefilesIndexSize() / 1024 / 1024);
      }
    }
    return new HServerLoad.RegionLoad(name, stores, storefiles,
        storefileSizeMB, memstoreSizeMB, storefileIndexSizeMB);
  }

  /**
   * @param encodedRegionName
   * @return An instance of RegionLoad.
   * @throws IOException
   */
  public HServerLoad.RegionLoad createRegionLoad(final String encodedRegionName) {
    return createRegionLoad(this.onlineRegions.get(encodedRegionName));
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t) {
    return cleanup(t, null);
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @param msg Message to log in error. Can be null.
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t, final String msg) {
    // Don't log as error if NSRE; NSRE is 'normal' operation.
    if (t instanceof NotServingRegionException) {
      LOG.debug("NotServingRegionException; " +  t.getMessage());
      return t;
    }
    if (msg == null) {
      LOG.error("", RemoteExceptionHandler.checkThrowable(t));
    } else {
      LOG.error(msg, RemoteExceptionHandler.checkThrowable(t));
    }
    if (!checkOOME(t)) {
      checkFileSystem();
    }
    return t;
  }

  /*
   * @param t
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t) {
    return convertThrowableToIOE(t, null);
  }

  /*
   * @param t
   *
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t, final String msg) {
    return (t instanceof IOException ? (IOException) t : msg == null
        || msg.length() == 0 ? new IOException(t) : new IOException(msg, t));
  }

  /*
   * Check if an OOME and if so, call abort.
   *
   * @param e
   *
   * @return True if we OOME'd and are aborting.
   */
  public boolean checkOOME(final Throwable e) {
    boolean stop = false;
    if (e instanceof OutOfMemoryError
        || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
        || (e.getMessage() != null && e.getMessage().contains(
            "java.lang.OutOfMemoryError"))) {
      abort("OutOfMemoryError, aborting", e);
      stop = true;
    }
    return stop;
  }

  /**
   * Checks to see if the file system is still accessible. If not, sets
   * abortRequested and stopRequested
   *
   * @return false if file system is not available
   */
  protected boolean checkFileSystem() {
    if (this.fsOk && this.fs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
      } catch (IOException e) {
        abort("File System not available", e);
        this.fsOk = false;
      }
    }
    return this.fsOk;
  }

  /*
   * Inner class that runs on a long period checking if regions need major
   * compaction.
   */
  private static class MajorCompactionChecker extends Chore {
    private final HRegionServer instance;

    MajorCompactionChecker(final HRegionServer h, final int sleepTime,
        final Stoppable stopper) {
      super("MajorCompactionChecker", sleepTime, h);
      this.instance = h;
      LOG.info("Runs every " + sleepTime + "ms");
    }

    @Override
    protected void chore() {
      for (HRegion r : this.instance.onlineRegions.values()) {
        try {
          if (r != null && r.isMajorCompaction()) {
            // Queue a compaction. Will recognize if major is needed.
            this.instance.compactSplitThread.requestCompaction(r, getName()
                + " requests major compaction");
          }
        } catch (IOException e) {
          LOG.warn("Failed major compaction check on " + r, e);
        }
      }
    }
  }

  /**
   * Report the status of the server. A server is online once all the startup is
   * completed (setting up filesystem, starting service threads, etc.). This
   * method is designed mostly to be useful in tests.
   *
   * @return true if online, false if not.
   */
  public boolean isOnline() {
    return isOnline;
  }

  /**
   * Setup WAL log and replication if enabled.
   * Replication setup is done in here because it wants to be hooked up to WAL.
   * @return A WAL instance.
   * @throws IOException
   */
  private HLog setupWALAndReplication() throws IOException {
    final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path logdir = new Path(rootDir, HLog.getHLogDirectoryName(this.serverInfo));
    if (LOG.isDebugEnabled()) {
      LOG.debug("logdir=" + logdir);
    }
    if (this.fs.exists(logdir)) {
      throw new RegionServerRunningException("Region server already "
          + "running at " + this.serverInfo.getServerName()
          + " because logdir " + logdir.toString() + " exists");
    }

    // Instantiate replication manager if replication enabled.  Pass it the
    // log directories.
    try {
      this.replicationHandler = Replication.isReplication(this.conf)?
        new Replication(this, this.fs, logdir, oldLogDir): null;
    } catch (KeeperException e) {
      throw new IOException("Failed replication handler create", e);
    }
    return instantiateHLog(logdir, oldLogDir);
  }

  /**
   * Called by {@link #setupWALAndReplication()} creating WAL instance.
   * @param logdir
   * @param oldLogDir
   * @return WAL instance.
   * @throws IOException
   */
  protected HLog instantiateHLog(Path logdir, Path oldLogDir) throws IOException {
    return new HLog(this.fs, logdir, oldLogDir, this.conf,
      getWALActionListeners(), this.serverInfo.getServerAddress().toString());
  }

  /**
   * Called by {@link #instantiateHLog(Path, Path)} setting up WAL instance.
   * Add any {@link WALObserver}s you want inserted before WAL startup.
   * @return List of WALActionsListener that will be passed in to
   * {@link HLog} on construction.
   */
  protected List<WALObserver> getWALActionListeners() {
    List<WALObserver> listeners = new ArrayList<WALObserver>();
    // Log roller.
    this.hlogRoller = new LogRoller(this, this);
    listeners.add(this.hlogRoller);
    if (this.replicationHandler != null) {
      listeners = new ArrayList<WALObserver>();
      // Replication handler is an implementation of WALActionsListener.
      listeners.add(this.replicationHandler);
    }
    return listeners;
  }

  protected LogRoller getLogRoller() {
    return hlogRoller;
  }

  /*
   * @param interval Interval since last time metrics were called.
   */
  protected void doMetrics() {
    try {
      metrics();
    } catch (Throwable e) {
      LOG.warn("Failed metrics", e);
    }
  }

  protected void metrics() {
    this.metrics.regions.set(this.onlineRegions.size());
    this.metrics.incrementRequests(this.requestCount.get());
    // Is this too expensive every three seconds getting a lock on onlineRegions
    // and then per store carried? Can I make metrics be sloppier and avoid
    // the synchronizations?
    int stores = 0;
    int storefiles = 0;
    long memstoreSize = 0;
    long storefileIndexSize = 0;
    synchronized (this.onlineRegions) {
      for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
        HRegion r = e.getValue();
        memstoreSize += r.memstoreSize.get();
        synchronized (r.stores) {
          stores += r.stores.size();
          for (Map.Entry<byte[], Store> ee : r.stores.entrySet()) {
            Store store = ee.getValue();
            storefiles += store.getStorefilesCount();
            storefileIndexSize += store.getStorefilesIndexSize();
          }
        }
      }
    }
    this.metrics.stores.set(stores);
    this.metrics.storefiles.set(storefiles);
    this.metrics.memstoreSizeMB.set((int) (memstoreSize / (1024 * 1024)));
    this.metrics.storefileIndexSizeMB
        .set((int) (storefileIndexSize / (1024 * 1024)));
    this.metrics.compactionQueueSize.set(compactSplitThread
        .getCompactionQueueSize());

    LruBlockCache lruBlockCache = (LruBlockCache) StoreFile.getBlockCache(conf);
    if (lruBlockCache != null) {
      this.metrics.blockCacheCount.set(lruBlockCache.size());
      this.metrics.blockCacheFree.set(lruBlockCache.getFreeSize());
      this.metrics.blockCacheSize.set(lruBlockCache.getCurrentSize());
      double ratio = lruBlockCache.getStats().getHitRatio();
      int percent = (int) (ratio * 100);
      this.metrics.blockCacheHitRatio.set(percent);
    }
  }

  /**
   * @return Region server metrics instance.
   */
  public RegionServerMetrics getMetrics() {
    return this.metrics;
  }

  /*
   * Start maintanence Threads, Server, Worker and lease checker threads.
   * Install an UncaughtExceptionHandler that calls abort of RegionServer if we
   * get an unhandled exception. We cannot set the handler on all threads.
   * Server's internal Listener thread is off limits. For Server, if an OOME, it
   * waits a while then retries. Meantime, a flush or a compaction that tries to
   * run should trigger same critical condition and the shutdown will run. On
   * its way out, this server will shut down Server. Leases are sort of
   * inbetween. It has an internal thread that while it inherits from Chore, it
   * keeps its own internal stop mechanism so needs to be stopped by this
   * hosting server. Worker logs the exception and exits.
   */
  private void startServiceThreads() throws IOException {
    String n = Thread.currentThread().getName();
    UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        abort("Uncaught exception in service thread " + t.getName(), e);
      }
    };

    // Start executor services
    this.service = new ExecutorService(getServerName());
    this.service.startExecutorService(ExecutorType.RS_OPEN_REGION,
      conf.getInt("hbase.regionserver.executor.openregion.threads", 5));
    this.service.startExecutorService(ExecutorType.RS_OPEN_ROOT,
      conf.getInt("hbase.regionserver.executor.openroot.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_OPEN_META,
      conf.getInt("hbase.regionserver.executor.openmeta.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_REGION,
      conf.getInt("hbase.regionserver.executor.closeregion.threads", 5));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_ROOT,
      conf.getInt("hbase.regionserver.executor.closeroot.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_META,
      conf.getInt("hbase.regionserver.executor.closemeta.threads", 1));

    Threads.setDaemonThreadRunning(this.hlogRoller, n + ".logRoller", handler);
    Threads.setDaemonThreadRunning(this.cacheFlusher, n + ".cacheFlusher",
        handler);
    Threads.setDaemonThreadRunning(this.compactSplitThread, n + ".compactor",
        handler);
    Threads.setDaemonThreadRunning(this.majorCompactionChecker, n
        + ".majorCompactionChecker", handler);

    // Leases is not a Thread. Internally it runs a daemon thread. If it gets
    // an unhandled exception, it will just exit.
    this.leases.setName(n + ".leaseChecker");
    this.leases.start();
    // Put up info server.
    int port = this.conf.getInt("hbase.regionserver.info.port", 60030);
    // -1 is for disabling info server
    if (port >= 0) {
      String addr = this.conf.get("hbase.regionserver.info.bindAddress",
          "0.0.0.0");
      // check if auto port bind enabled
      boolean auto = this.conf.getBoolean("hbase.regionserver.info.port.auto",
          false);
      while (true) {
        try {
          this.infoServer = new InfoServer("regionserver", addr, port, false);
          this.infoServer.setAttribute("regionserver", this);
          this.infoServer.start();
          break;
        } catch (BindException e) {
          if (!auto) {
            // auto bind disabled throw BindException
            throw e;
          }
          // auto bind enabled, try to use another port
          LOG.info("Failed binding http info server to port: " + port);
          port++;
          // update HRS server info port.
          this.serverInfo = new HServerInfo(this.serverInfo.getServerAddress(),
            this.serverInfo.getStartCode(), port,
            this.serverInfo.getHostname());
        }
      }
    }

    if (this.replicationHandler != null) {
      this.replicationHandler.startReplicationServices();
    }

    // Start Server.  This service is like leases in that it internally runs
    // a thread.
    this.server.start();
  }

  /*
   * Verify that server is healthy
   */
  private boolean isHealthy() {
    if (!fsOk) {
      // File system problem
      return false;
    }
    // Verify that all threads are alive
    if (!(leases.isAlive() && compactSplitThread.isAlive()
        && cacheFlusher.isAlive() && hlogRoller.isAlive()
        && this.majorCompactionChecker.isAlive())) {
      stop("One or more threads are no longer alive -- stop");
      return false;
    }
    return true;
  }

  /** @return the HLog */
  public HLog getWAL() {
    return this.hlog;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return this.catalogTracker;
  }

  @Override
  public void stop(final String msg) {
    this.stopped = true;
    LOG.info("STOPPED: " + msg);
    synchronized (this) {
      // Wakes run() if it is sleeping
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }
  }

  @Override
  public void postOpenDeployTasks(final HRegion r, final CatalogTracker ct,
      final boolean daughter)
  throws KeeperException, IOException {
    // Do checks to see if we need to compact (references or too many files)
    if (r.hasReferences() || r.hasTooManyStoreFiles()) {
      getCompactionRequester().requestCompaction(r,
        r.hasReferences()? "Region has references on open" :
          "Region has too many store files");
    }
    // Add to online regions
    addToOnlineRegions(r);
    // Update ZK, ROOT or META
    if (r.getRegionInfo().isRootRegion()) {
      RootLocationEditor.setRootLocation(getZooKeeper(),
        getServerInfo().getServerAddress());
    } else if (r.getRegionInfo().isMetaRegion()) {
      MetaEditor.updateMetaLocation(ct, r.getRegionInfo(), getServerInfo());
    } else {
      if (daughter) {
        // If daughter of a split, update whole row, not just location.
        MetaEditor.addDaughter(ct, r.getRegionInfo(), getServerInfo());
      } else {
        MetaEditor.updateRegionLocation(ct, r.getRegionInfo(), getServerInfo());
      }
    }
  }

  /**
   * Cause the server to exit without closing the regions it is serving, the log
   * it is using and without notifying the master. Used unit testing and on
   * catastrophic events such as HDFS is yanked out from under hbase or we OOME.
   *
   * @param reason
   *          the reason we are aborting
   * @param cause
   *          the exception that caused the abort, or null
   */
  public void abort(String reason, Throwable cause) {
    if (cause != null) {
      LOG.fatal("Aborting region server " + this + ": " + reason, cause);
    } else {
      LOG.fatal("Aborting region server " + this + ": " + reason);
    }
    this.abortRequested = true;
    this.reservedSpace.clear();
    if (this.metrics != null) {
      LOG.info("Dump of metrics: " + this.metrics);
    }
    stop(reason);
  }

  /**
   * @see HRegionServer#abort(String, Throwable)
   */
  public void abort(String reason) {
    abort(reason, null);
  }

  /*
   * Simulate a kill -9 of this server. Exits w/o closing regions or cleaninup
   * logs but it does close socket in case want to bring up server on old
   * hostname+port immediately.
   */
  protected void kill() {
    this.killed = true;
    abort("Simulated kill");
  }

  /**
   * Wait on all threads to finish. Presumption is that all closes and stops
   * have already been called.
   */
  protected void join() {
    Threads.shutdown(this.majorCompactionChecker);
    Threads.shutdown(this.cacheFlusher);
    Threads.shutdown(this.compactSplitThread);
    Threads.shutdown(this.hlogRoller);
    this.service.shutdown();
    if (this.replicationHandler != null) {
      this.replicationHandler.join();
    }
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it.
   *
   * Method will block until a master is available. You can break from this
   * block by requesting the server stop.
   *
   * @return
   */
  private boolean getMaster() {
    HServerAddress masterAddress = null;
    while ((masterAddress = masterAddressManager.getMasterAddress()) == null) {
      if (stopped) {
        return false;
      }
      LOG.debug("No master found, will retry");
      sleeper.sleep();
    }
    LOG.info("Telling master at " + masterAddress + " that we are up");
    HMasterRegionInterface master = null;
    while (!stopped && master == null) {
      try {
        // Do initial RPC setup. The final argument indicates that the RPC
        // should retry indefinitely.
        master = (HMasterRegionInterface) HBaseRPC.waitForProxy(
            HMasterRegionInterface.class, HBaseRPCProtocolVersion.versionID,
            masterAddress.getInetSocketAddress(), this.conf, -1,
            this.rpcTimeout);
      } catch (IOException e) {
        LOG.warn("Unable to connect to master. Retrying. Error was:", e);
        sleeper.sleep();
      }
    }
    this.hbaseMaster = master;
    return true;
  }

  /**
   * @return True if successfully invoked {@link #reportForDuty()}
   * @throws IOException
   */
  private boolean tryReportForDuty() throws IOException {
    MapWritable w = reportForDuty();
    if (w != null) {
      handleReportForDutyResponse(w);
      return true;
    }
    sleeper.sleep();
    LOG.warn("No response on reportForDuty. Sleeping and then retrying.");
    return false;
  }

  /*
   * Let the master know we're here Run initialization using parameters passed
   * us by the master.
   */
  private MapWritable reportForDuty() {
    while (!stopped && !getMaster()) {
      sleeper.sleep();
      LOG.warn("Unable to get master for initialization");
    }

    MapWritable result = null;
    long lastMsg = 0;
    while (!stopped) {
      try {
        this.requestCount.set(0);
        lastMsg = System.currentTimeMillis();
        ZKUtil.setAddressAndWatch(zooKeeper, ZKUtil.joinZNode(
            zooKeeper.rsZNode, ZKUtil.getNodeName(serverInfo)), address);
        this.serverInfo.setLoad(buildServerLoad());
        result = this.hbaseMaster.regionServerStartup(this.serverInfo);
        break;
      } catch (IOException e) {
        LOG.warn("error telling master we are up", e);
      } catch (KeeperException e) {
        LOG.warn("error putting up ephemeral node in zookeeper", e);
      }
      sleeper.sleep(lastMsg);
    }
    return result;
  }

  /**
   * Add to the outbound message buffer
   *
   * When a region splits, we need to tell the master that there are two new
   * regions that need to be assigned.
   *
   * We do not need to inform the master about the old region, because we've
   * updated the meta or root regions, and the master will pick that up on its
   * next rescan of the root or meta tables.
   */
  void reportSplit(HRegionInfo oldRegion, HRegionInfo newRegionA,
      HRegionInfo newRegionB) {
    this.outboundMsgs.add(new HMsg(
        HMsg.Type.REGION_SPLIT, oldRegion, newRegionA,
        newRegionB, Bytes.toBytes("Daughters; "
            + newRegionA.getRegionNameAsString() + ", "
            + newRegionB.getRegionNameAsString())));
  }

  /**
   * Closes all regions.  Called on our way out.
   * Assumes that its not possible for new regions to be added to onlineRegions
   * while this method runs.
   */
  protected void closeAllRegions(final boolean abort) {
    closeUserRegions(abort);
    // Only root and meta should remain.  Are we carrying root or meta?
    HRegion meta = null;
    HRegion root = null;
    this.lock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e: onlineRegions.entrySet()) {
        HRegionInfo hri = e.getValue().getRegionInfo();
        if (hri.isRootRegion()) {
          root = e.getValue();
        } else if (hri.isMetaRegion()) {
          meta = e.getValue();
        }
        if (meta != null && root != null) break;
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    if (meta != null) closeRegion(meta.getRegionInfo(), abort, false);
    if (root != null) closeRegion(root.getRegionInfo(), abort, false);
  }

  /**
   * Schedule closes on all user regions.
   * @param abort Whether we're running an abort.
   */
  void closeUserRegions(final boolean abort) {
    this.lock.writeLock().lock();
    try {
      synchronized (this.onlineRegions) {
        for (Map.Entry<String, HRegion> e: this.onlineRegions.entrySet()) {
          HRegion r = e.getValue();
          if (!r.getRegionInfo().isMetaRegion()) {
            // Don't update zk with this close transition; pass false.
            closeRegion(r.getRegionInfo(), abort, false);
          }
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  @Override
  public HRegionInfo getRegionInfo(final byte[] regionName)
  throws NotServingRegionException {
    requestCount.incrementAndGet();
    return getRegion(regionName).getRegionInfo();
  }

  public Result getClosestRowBefore(final byte[] regionName, final byte[] row,
      final byte[] family) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      // locate the region we're operating on
      HRegion region = getRegion(regionName);
      // ask the region for all the data

      Result r = region.getClosestRowBefore(row, family);
      return r;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  /** {@inheritDoc} */
  public Result get(byte[] regionName, Get get) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      return region.get(get, getLockFromId(get.getLockId()));
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public boolean exists(byte[] regionName, Get get) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      Result r = region.get(get, getLockFromId(get.getLockId()));
      return r != null && !r.isEmpty();
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public void put(final byte[] regionName, final Put put) throws IOException {
    if (put.getRow() == null) {
      throw new IllegalArgumentException("update has null row");
    }

    checkOpen();
    this.requestCount.incrementAndGet();
    HRegion region = getRegion(regionName);
    try {
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      boolean writeToWAL = put.getWriteToWAL();
      region.put(put, getLockFromId(put.getLockId()), writeToWAL);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public int put(final byte[] regionName, final List<Put> puts)
      throws IOException {
    checkOpen();
    HRegion region = null;
    try {
      region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }

      @SuppressWarnings("unchecked")
      Pair<Put, Integer>[] putsWithLocks = new Pair[puts.size()];

      int i = 0;
      for (Put p : puts) {
        Integer lock = getLockFromId(p.getLockId());
        putsWithLocks[i++] = new Pair<Put, Integer>(p, lock);
      }

      this.requestCount.addAndGet(puts.size());
      OperationStatusCode[] codes = region.put(putsWithLocks);
      for (i = 0; i < codes.length; i++) {
        if (codes[i] != OperationStatusCode.SUCCESS) {
          return i;
        }
      }
      return -1;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  private boolean checkAndMutate(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final byte[] value,
      final Writable w, Integer lock) throws IOException {
    checkOpen();
    this.requestCount.incrementAndGet();
    HRegion region = getRegion(regionName);
    try {
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      return region
          .checkAndMutate(row, family, qualifier, value, w, lock, true);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  /**
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param value
   *          the expected value
   * @param put
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndPut(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final byte[] value,
      final Put put) throws IOException {
    return checkAndMutate(regionName, row, family, qualifier, value, put,
        getLockFromId(put.getLockId()));
  }

  /**
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param value
   *          the expected value
   * @param delete
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndDelete(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final byte[] value,
      final Delete delete) throws IOException {
    return checkAndMutate(regionName, row, family, qualifier, value, delete,
        getLockFromId(delete.getLockId()));
  }

  //
  // remote scanner interface
  //

  public long openScanner(byte[] regionName, Scan scan) throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (scan == null) {
      npe = new NullPointerException("scan is null");
    }
    if (npe != null) {
      throw new IOException("Invalid arguments to openScanner", npe);
    }
    requestCount.incrementAndGet();
    try {
      HRegion r = getRegion(regionName);
      return addScanner(r.getScanner(scan));
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t, "Failed openScanner"));
    }
  }

  protected long addScanner(InternalScanner s) throws LeaseStillHeldException {
    long scannerId = -1L;
    scannerId = rand.nextLong();
    String scannerName = String.valueOf(scannerId);
    scanners.put(scannerName, s);
    this.leases.createLease(scannerName, new ScannerListener(scannerName));
    return scannerId;
  }

  public Result next(final long scannerId) throws IOException {
    Result[] res = next(scannerId, 1);
    if (res == null || res.length == 0) {
      return null;
    }
    return res[0];
  }

  public Result[] next(final long scannerId, int nbRows) throws IOException {
    try {
      String scannerName = String.valueOf(scannerId);
      InternalScanner s = this.scanners.get(scannerName);
      if (s == null) {
        throw new UnknownScannerException("Name: " + scannerName);
      }
      try {
        checkOpen();
      } catch (IOException e) {
        // If checkOpen failed, server not running or filesystem gone,
        // cancel this lease; filesystem is gone or we're closing or something.
        this.leases.cancelLease(scannerName);
        throw e;
      }
      this.leases.renewLease(scannerName);
      List<Result> results = new ArrayList<Result>(nbRows);
      long currentScanResultSize = 0;
      List<KeyValue> values = new ArrayList<KeyValue>();
      for (int i = 0; i < nbRows
          && currentScanResultSize < maxScannerResultSize; i++) {
        requestCount.incrementAndGet();
        // Collect values to be returned here
        boolean moreRows = s.next(values);
        if (!values.isEmpty()) {
          for (KeyValue kv : values) {
            currentScanResultSize += kv.heapSize();
          }
          results.add(new Result(values));
        }
        if (!moreRows) {
          break;
        }
        values.clear();
      }
      // Below is an ugly hack where we cast the InternalScanner to be a
      // HRegion.RegionScanner. The alternative is to change InternalScanner
      // interface but its used everywhere whereas we just need a bit of info
      // from HRegion.RegionScanner, IF its filter if any is done with the scan
      // and wants to tell the client to stop the scan. This is done by passing
      // a null result.
      return ((HRegion.RegionScanner) s).isFilterDone() && results.isEmpty() ? null
          : results.toArray(new Result[0]);
    } catch (Throwable t) {
      if (t instanceof NotServingRegionException) {
        String scannerName = String.valueOf(scannerId);
        this.scanners.remove(scannerName);
      }
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public void close(final long scannerId) throws IOException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      String scannerName = String.valueOf(scannerId);
      InternalScanner s = scanners.remove(scannerName);
      if (s != null) {
        s.close();
        this.leases.cancelLease(scannerName);
      }
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  /**
   * Instantiated as a scanner lease. If the lease times out, the scanner is
   * closed
   */
  private class ScannerListener implements LeaseListener {
    private final String scannerName;

    ScannerListener(final String n) {
      this.scannerName = n;
    }

    public void leaseExpired() {
      LOG.info("Scanner " + this.scannerName + " lease expired");
      InternalScanner s = scanners.remove(this.scannerName);
      if (s != null) {
        try {
          s.close();
        } catch (IOException e) {
          LOG.error("Closing scanner", e);
        }
      }
    }
  }

  //
  // Methods that do the actual work for the remote API
  //
  public void delete(final byte[] regionName, final Delete delete)
      throws IOException {
    checkOpen();
    try {
      boolean writeToWAL = true;
      this.requestCount.incrementAndGet();
      HRegion region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      Integer lid = getLockFromId(delete.getLockId());
      region.delete(delete, lid, writeToWAL);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public int delete(final byte[] regionName, final List<Delete> deletes)
      throws IOException {
    // Count of Deletes processed.
    int i = 0;
    checkOpen();
    HRegion region = null;
    try {
      boolean writeToWAL = true;
      region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      int size = deletes.size();
      Integer[] locks = new Integer[size];
      for (Delete delete : deletes) {
        this.requestCount.incrementAndGet();
        locks[i] = getLockFromId(delete.getLockId());
        region.delete(delete, locks[i], writeToWAL);
        i++;
      }
    } catch (WrongRegionException ex) {
      LOG.debug("Batch deletes: " + i, ex);
      return i;
    } catch (NotServingRegionException ex) {
      return i;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
    return -1;
  }

  public long lockRow(byte[] regionName, byte[] row) throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (row == null) {
      npe = new NullPointerException("row to lock is null");
    }
    if (npe != null) {
      IOException io = new IOException("Invalid arguments to lockRow");
      io.initCause(npe);
      throw io;
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      Integer r = region.obtainRowLock(row);
      long lockId = addRowLock(r, region);
      LOG.debug("Row lock " + lockId + " explicitly acquired by client");
      return lockId;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t, "Error obtaining row lock (fsOk: "
          + this.fsOk + ")"));
    }
  }

  protected long addRowLock(Integer r, HRegion region)
      throws LeaseStillHeldException {
    long lockId = -1L;
    lockId = rand.nextLong();
    String lockName = String.valueOf(lockId);
    rowlocks.put(lockName, r);
    this.leases.createLease(lockName, new RowLockListener(lockName, region));
    return lockId;
  }

  /**
   * Method to get the Integer lock identifier used internally from the long
   * lock identifier used by the client.
   *
   * @param lockId
   *          long row lock identifier from client
   * @return intId Integer row lock used internally in HRegion
   * @throws IOException
   *           Thrown if this is not a valid client lock id.
   */
  Integer getLockFromId(long lockId) throws IOException {
    if (lockId == -1L) {
      return null;
    }
    String lockName = String.valueOf(lockId);
    Integer rl = rowlocks.get(lockName);
    if (rl == null) {
      throw new IOException("Invalid row lock");
    }
    this.leases.renewLease(lockName);
    return rl;
  }

  public void unlockRow(byte[] regionName, long lockId) throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (lockId == -1L) {
      npe = new NullPointerException("lockId is null");
    }
    if (npe != null) {
      IOException io = new IOException("Invalid arguments to unlockRow");
      io.initCause(npe);
      throw io;
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      String lockName = String.valueOf(lockId);
      Integer r = rowlocks.remove(lockName);
      if (r == null) {
        throw new UnknownRowLockException(lockName);
      }
      region.releaseRowLock(r);
      this.leases.cancelLease(lockName);
      LOG.debug("Row lock " + lockId
          + " has been explicitly released by client");
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public void bulkLoadHFile(String hfilePath, byte[] regionName,
      byte[] familyName) throws IOException {
    HRegion region = getRegion(regionName);
    region.bulkLoadHFile(hfilePath, familyName);
  }

  Map<String, Integer> rowlocks = new ConcurrentHashMap<String, Integer>();

  /**
   * Instantiated as a row lock lease. If the lease times out, the row lock is
   * released
   */
  private class RowLockListener implements LeaseListener {
    private final String lockName;
    private final HRegion region;

    RowLockListener(final String lockName, final HRegion region) {
      this.lockName = lockName;
      this.region = region;
    }

    public void leaseExpired() {
      LOG.info("Row Lock " + this.lockName + " lease expired");
      Integer r = rowlocks.remove(this.lockName);
      if (r != null) {
        region.releaseRowLock(r);
      }
    }
  }

  // Region open/close direct RPCs

  @Override
  public void openRegion(HRegionInfo region) {
    LOG.info("Received request to open region: " +
      region.getRegionNameAsString());
    if (region.isRootRegion()) {
      this.service.submit(new OpenRootHandler(this, this, region));
    } else if(region.isMetaRegion()) {
      this.service.submit(new OpenMetaHandler(this, this, region));
    } else {
      this.service.submit(new OpenRegionHandler(this, this, region));
    }
  }

  @Override
  public boolean closeRegion(HRegionInfo region)
  throws NotServingRegionException {
    LOG.info("Received close region: " + region.getRegionNameAsString());
    // TODO: Need to check if this is being served here but currently undergoing
    // a split (so master needs to retry close after split is complete)
    if (!onlineRegions.containsKey(region.getEncodedName())) {
      LOG.warn("Received close for region we are not serving; " +
        region.getEncodedName());
      throw new NotServingRegionException("Received close for "
          + region.getRegionNameAsString() + " but we are not serving it");
    }
    return closeRegion(region, false, true);
  }

  /**
   * @param region Region to close
   * @param abort True if we are aborting
   * @param zk True if we are to update zk about the region close; if the close
   * was orchestrated by master, then update zk.  If the close is being run by
   * the regionserver because its going down, don't update zk.
   * @return
   */
  protected boolean closeRegion(HRegionInfo region, final boolean abort,
      final boolean zk) {
    CloseRegionHandler crh = null;
    if (region.isRootRegion()) {
      crh = new CloseRootHandler(this, this, region, abort, zk);
    } else if (region.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, region, abort, zk);
    } else {
      crh = new CloseRegionHandler(this, this, region, abort, zk);
    }
    this.service.submit(crh);
    return true;
  }

  // Manual remote region administration RPCs

  @Override
  public void flushRegion(HRegionInfo regionInfo)
      throws NotServingRegionException, IOException {
    LOG.info("Flushing " + regionInfo.getRegionNameAsString());
    HRegion region = getRegion(regionInfo.getRegionName());
    region.flushcache();
  }

  @Override
  public void splitRegion(HRegionInfo regionInfo)
      throws NotServingRegionException, IOException {
    HRegion region = getRegion(regionInfo.getRegionName());
    region.flushcache();
    region.shouldSplit(true);
    // force a compaction, split will be side-effect
    // TODO: flush/compact/split refactor will make it trivial to do this
    // sync/async (and won't require us to do a compaction to split!)
    compactSplitThread.requestCompaction(region, "User-triggered split");
  }

  @Override
  public void compactRegion(HRegionInfo regionInfo, boolean major)
      throws NotServingRegionException, IOException {
    HRegion region = getRegion(regionInfo.getRegionName());
    region.flushcache();
    region.shouldSplit(true);
    compactSplitThread.requestCompaction(region, major, "User-triggered "
        + (major ? "major " : "") + "compaction");
  }

  /** @return the info server */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   *
   * @return the configuration
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /** @return the write lock for the server */
  ReentrantReadWriteLock.WriteLock getWriteLock() {
    return lock.writeLock();
  }

  @Override
  public NavigableSet<HRegionInfo> getOnlineRegions() {
    NavigableSet<HRegionInfo> sortedset = new TreeSet<HRegionInfo>();
    synchronized(this.onlineRegions) {
      for (Map.Entry<String,HRegion> e: this.onlineRegions.entrySet()) {
        sortedset.add(e.getValue().getRegionInfo());
      }
    }
    return sortedset;
  }

  public int getNumberOfOnlineRegions() {
    return onlineRegions.size();
  }

  /**
   * For tests and web ui.
   * This method will only work if HRegionServer is in the same JVM as client;
   * HRegion cannot be serialized to cross an rpc.
   * @see #getOnlineRegions()
   */
  public Collection<HRegion> getOnlineRegionsLocalContext() {
    return Collections.unmodifiableCollection(this.onlineRegions.values());
  }

  @Override
  public void addToOnlineRegions(HRegion region) {
    lock.writeLock().lock();
    try {
      onlineRegions.put(region.getRegionInfo().getEncodedName(), region);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean removeFromOnlineRegions(final String encodedName) {
    this.lock.writeLock().lock();
    HRegion toReturn = null;
    try {
      toReturn = onlineRegions.remove(encodedName);
    } finally {
      this.lock.writeLock().unlock();
    }
    return toReturn != null;
  }

  /**
   * @return A new Map of online regions sorted by region size with the first
   *         entry being the biggest.
   */
  public SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<Long, HRegion>(
        new Comparator<Long>() {
          public int compare(Long a, Long b) {
            return -1 * a.compareTo(b);
          }
        });
    // Copy over all regions. Regions are sorted by size with biggest first.
    synchronized (this.onlineRegions) {
      for (HRegion region : this.onlineRegions.values()) {
        sortedRegions.put(Long.valueOf(region.memstoreSize.get()), region);
      }
    }
    return sortedRegions;
  }

  @Override
  public HRegion getFromOnlineRegions(final String encodedRegionName) {
    return onlineRegions.get(encodedRegionName);
  }

  /**
   * @param regionName
   * @return HRegion for the passed binary <code>regionName</code> or null if
   *         named region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    return getFromOnlineRegions(HRegionInfo.encodeRegionName(regionName));
  }

  /** @return the request count */
  public AtomicInteger getRequestCount() {
    return this.requestCount;
  }

  /** @return reference to FlushRequester */
  public FlushRequester getFlushRequester() {
    return this.cacheFlusher;
  }

  /**
   * Protected utility method for safely obtaining an HRegion handle.
   *
   * @param regionName
   *          Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final byte[] regionName)
      throws NotServingRegionException {
    HRegion region = null;
    this.lock.readLock().lock();
    try {
      region = getOnlineRegion(regionName);
      if (region == null) {
        throw new NotServingRegionException("Region is not online: " +
          Bytes.toStringBinary(regionName));
      }
      return region;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Get the top N most loaded regions this server is serving so we can tell the
   * master which regions it can reallocate if we're overloaded. TODO: actually
   * calculate which regions are most loaded. (Right now, we're just grabbing
   * the first N regions being served regardless of load.)
   */
  protected HRegionInfo[] getMostLoadedRegions() {
    ArrayList<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    synchronized (onlineRegions) {
      for (HRegion r : onlineRegions.values()) {
        if (r.isClosed() || r.isClosing()) {
          continue;
        }
        if (regions.size() < numRegionsToReport) {
          regions.add(r.getRegionInfo());
        } else {
          break;
        }
      }
    }
    return regions.toArray(new HRegionInfo[regions.size()]);
  }

  /**
   * Called to verify that this server is up and running.
   *
   * @throws IOException
   */
  protected void checkOpen() throws IOException {
    if (this.stopped || this.abortRequested) {
      throw new IOException("Server not running"
          + (this.abortRequested ? ", aborting" : ""));
    }
    if (!fsOk) {
      throw new IOException("File system not available");
    }
  }

  /**
   * @return Returns list of non-closed regions hosted on this server. If no
   *         regions to check, returns an empty list.
   */
  protected Set<HRegion> getRegionsToCheck() {
    HashSet<HRegion> regionsToCheck = new HashSet<HRegion>();
    // TODO: is this locking necessary?
    lock.readLock().lock();
    try {
      regionsToCheck.addAll(this.onlineRegions.values());
    } finally {
      lock.readLock().unlock();
    }
    // Purge closed regions.
    for (final Iterator<HRegion> i = regionsToCheck.iterator(); i.hasNext();) {
      HRegion r = i.next();
      if (r.isClosed()) {
        i.remove();
      }
    }
    return regionsToCheck;
  }

  public long getProtocolVersion(final String protocol, final long clientVersion)
      throws IOException {
    if (protocol.equals(HRegionInterface.class.getName())) {
      return HBaseRPCProtocolVersion.versionID;
    }
    throw new IOException("Unknown protocol to name node: " + protocol);
  }

  /**
   * @return Queue to which you can add outbound messages.
   */
  protected LinkedBlockingQueue<HMsg> getOutboundMsgs() {
    return this.outboundMsgs;
  }

  /**
   * Return the total size of all memstores in every region.
   *
   * @return memstore size in bytes
   */
  public long getGlobalMemStoreSize() {
    long total = 0;
    synchronized (onlineRegions) {
      for (HRegion region : onlineRegions.values()) {
        total += region.memstoreSize.get();
      }
    }
    return total;
  }

  /**
   * @return Return the leases.
   */
  protected Leases getLeases() {
    return leases;
  }

  /**
   * @return Return the rootDir.
   */
  protected Path getRootDir() {
    return rootDir;
  }

  /**
   * @return Return the fs.
   */
  protected FileSystem getFileSystem() {
    return fs;
  }

  /**
   * @return Info on port this server has bound to, etc.
   */
  public HServerInfo getServerInfo() {
    return this.serverInfo;
  }

  /** {@inheritDoc} */
  public long incrementColumnValue(byte[] regionName, byte[] row,
      byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
      throws IOException {
    checkOpen();

    if (regionName == null) {
      throw new IOException("Invalid arguments to incrementColumnValue "
          + "regionName is null");
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      long retval = region.incrementColumnValue(row, family, qualifier, amount,
          writeToWAL);

      return retval;
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc} */
  public HRegionInfo[] getRegionsAssignment() throws IOException {
    HRegionInfo[] regions = new HRegionInfo[onlineRegions.size()];
    Iterator<HRegion> ite = onlineRegions.values().iterator();
    for (int i = 0; ite.hasNext(); i++) {
      regions[i] = ite.next().getRegionInfo();
    }
    return regions;
  }

  /** {@inheritDoc} */
  public HServerInfo getHServerInfo() throws IOException {
    return serverInfo;
  }

  @Override
  public MultiResponse multi(MultiAction multi) throws IOException {
    MultiResponse response = new MultiResponse();
    for (Map.Entry<byte[], List<Action>> e : multi.actions.entrySet()) {
      byte[] regionName = e.getKey();
      List<Action> actionsForRegion = e.getValue();
      // sort based on the row id - this helps in the case where we reach the
      // end of a region, so that we don't have to try the rest of the
      // actions in the list.
      Collections.sort(actionsForRegion);
      Row action = null;
      try {
        for (Action a : actionsForRegion) {
          action = a.getAction();
          if (action instanceof Delete) {
            delete(regionName, (Delete) action);
            response.add(regionName, new Pair<Integer, Result>(
                a.getOriginalIndex(), new Result()));
          } else if (action instanceof Get) {
            response.add(regionName, new Pair<Integer, Result>(
                a.getOriginalIndex(), get(regionName, (Get) action)));
          } else if (action instanceof Put) {
            put(regionName, (Put) action);
            response.add(regionName, new Pair<Integer, Result>(
                a.getOriginalIndex(), new Result()));
          } else {
            LOG.debug("Error: invalid Action, row must be a Get, Delete or Put.");
            throw new IllegalArgumentException("Invalid Action, row must be a Get, Delete or Put.");
          }
        }
      } catch (IOException ioe) {
        if (multi.size() == 1) throw ioe;
        LOG.debug("Exception processing " +
          org.apache.commons.lang.StringUtils.abbreviate(action.toString(), 64) +
          "; " + ioe.getMessage());
        response.add(regionName,null);
        // stop processing on this region, continue to the next.
      }
    }
    return response;
  }

  /**
   * @deprecated Use HRegionServer.multi( MultiAction action) instead
   */
  @Override
  public MultiPutResponse multiPut(MultiPut puts) throws IOException {
    MultiPutResponse resp = new MultiPutResponse();

    // do each region as it's own.
    for (Map.Entry<byte[], List<Put>> e : puts.puts.entrySet()) {
      int result = put(e.getKey(), e.getValue());
      resp.addResult(e.getKey(), result);

      e.getValue().clear(); // clear some RAM
    }

    return resp;
  }

  public String toString() {
    return this.serverInfo.toString();
  }

  /**
   * Interval at which threads should run
   *
   * @return the interval
   */
  public int getThreadWakeFrequency() {
    return threadWakeFrequency;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public String getServerName() {
    return serverInfo.getServerName();
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    return this.compactSplitThread;
  }
  
  //
  // Main program and support routines
  //

  /**
   * @param hrs
   * @return Thread the RegionServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startRegionServer(final HRegionServer hrs)
      throws IOException {
    return startRegionServer(hrs, "regionserver"
        + hrs.getServerInfo().getServerAddress().getPort());
  }

  /**
   * @param hrs
   * @param name
   * @return Thread the RegionServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startRegionServer(final HRegionServer hrs,
      final String name) throws IOException {
    Thread t = new Thread(hrs);
    t.setName(name);
    t.start();
    // Install shutdown hook that will catch signals and run an orderly shutdown
    // of the hrs.
    ShutdownHook.install(hrs.getConfiguration(), FileSystem.get(hrs
        .getConfiguration()), hrs, t);
    return t;
  }

  /**
   * Utility for constructing an instance of the passed HRegionServer class.
   *
   * @param regionServerClass
   * @param conf2
   * @return HRegionServer instance.
   */
  public static HRegionServer constructRegionServer(
      Class<? extends HRegionServer> regionServerClass,
      final Configuration conf2) {
    try {
      Constructor<? extends HRegionServer> c = regionServerClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf2);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Master: "
          + regionServerClass.toString(), e);
    }
  }

  @Override
  public void replicateLogEntries(final HLog.Entry[] entries)
  throws IOException {
    if (this.replicationHandler == null) return;
    this.replicationHandler.replicateLogEntries(entries);
  }


  /**
   * @see org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HRegionServer> regionServerClass = (Class<? extends HRegionServer>) conf
        .getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);

    new HRegionServerCommandLine(regionServerClass).doMain(args);
  }

}
