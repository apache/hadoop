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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HMsg.Type;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LeaseListener;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.MultiPutResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.client.ServerConnectionManager;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
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
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * HRegionServer makes a set of HRegions available to clients.  It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
public class HRegionServer implements HConstants, HRegionInterface,
    HBaseRPCErrorHandler, Runnable, Watcher {
  public static final Log LOG = LogFactory.getLog(HRegionServer.class);
  private static final HMsg REPORT_EXITING = new HMsg(Type.MSG_REPORT_EXITING);
  private static final HMsg REPORT_QUIESCED = new HMsg(Type.MSG_REPORT_QUIESCED);
  private static final HMsg [] EMPTY_HMSG_ARRAY = new HMsg [] {};

  // Set when a report to the master comes back with a message asking us to
  // shutdown.  Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation. We use AtomicBoolean rather than
  // plain boolean so we can pass a reference to Chore threads.  Otherwise,
  // Chore threads need to know about the hosting class.
  protected final AtomicBoolean stopRequested = new AtomicBoolean(false);

  protected final AtomicBoolean quiesced = new AtomicBoolean(false);

  // Go down hard.  Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;

  private volatile boolean killed = false;

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;

  protected HServerInfo serverInfo;
  protected final Configuration conf;

  private final ServerConnection connection;
  protected final AtomicBoolean haveRootRegion = new AtomicBoolean(false);
  private FileSystem fs;
  private Path rootDir;
  private final Random rand = new Random();

  // Key is Bytes.hashCode of region name byte array and the value is HRegion
  // in both of the maps below.  Use Bytes.mapKey(byte []) generating key for
  // below maps.
  protected final Map<Integer, HRegion> onlineRegions =
    new ConcurrentHashMap<Integer, HRegion>();

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final LinkedBlockingQueue<HMsg> outboundMsgs =
    new LinkedBlockingQueue<HMsg>();

  final int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;

  protected final int numRegionsToReport;

  private final long maxScannerResultSize;

  // Remote HMaster
  private HMasterRegionInterface hbaseMaster;

  // Server to handle client requests.  Default access so can be accessed by
  // unit tests.
  HBaseServer server;

  // Leases
  private Leases leases;

  // Request counter
  private volatile AtomicInteger requestCount = new AtomicInteger();

  // Info server.  Default access so can be used by unit tests.  REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  InfoServer infoServer;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  /*
   * Space is reserved in HRS constructor and then released when aborting
   * to recover from an OOME. See HBASE-706.  TODO: Make this percentage of the
   * heap or a minimum.
   */
  private final LinkedList<byte[]> reservedSpace = new LinkedList<byte []>();

  private RegionServerMetrics metrics;

  // Compactions
  CompactSplitThread compactSplitThread;

  // Cache flushing
  MemStoreFlusher cacheFlusher;

  /* Check for major compactions.
   */
  Chore majorCompactionChecker;

  // HLog and HLog roller.  log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  protected volatile HLog hlog;
  LogRoller hlogRoller;

  // flag set after we're done setting up server threads (used for testing)
  protected volatile boolean isOnline;

  final Map<String, InternalScanner> scanners =
    new ConcurrentHashMap<String, InternalScanner>();

  private ZooKeeperWrapper zooKeeperWrapper;

  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  private final long rpcTimeout;

  // Address passed in to constructor.  This is not always the address we run
  // with.  For example, if passed port is 0, then we are to pick a port.  The
  // actual address we run with is in the #serverInfo data member.
  private final HServerAddress address;

  // The main region server thread.
  private Thread regionServerThread;

  private final String machineName;

  /**
   * Starts a HRegionServer at the default location
   * @param conf
   * @throws IOException
   */
  public HRegionServer(Configuration conf) throws IOException {
    machineName = DNS.getDefaultHost(
        conf.get("hbase.regionserver.dns.interface","default"),
        conf.get("hbase.regionserver.dns.nameserver","default"));
    String addressStr = machineName + ":" +
      conf.get(REGIONSERVER_PORT, Integer.toString(DEFAULT_REGIONSERVER_PORT));
    // This is not necessarily the address we will run with.  The address we
    // use will be in #serverInfo data member.  For example, we may have been
    // passed a port of 0 which means we should pick some ephemeral port to bind
    // to.
    address = new HServerAddress(addressStr);
    LOG.info("My address is " + address);

    this.abortRequested = false;
    this.fsOk = true;
    this.conf = conf;
    this.connection = ServerConnectionManager.getConnection(conf);

    this.isOnline = false;

    // Config'ed params
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.threadWakeFrequency = conf.getInt(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 1 * 1000);

    sleeper = new Sleeper(this.msgInterval, this.stopRequested);

    this.maxScannerResultSize = conf.getLong(
            HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
            HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);

    // Task thread to process requests from Master
    this.worker = new Worker();

    this.numRegionsToReport =
      conf.getInt("hbase.regionserver.numregionstoreport", 10);

    this.rpcTimeout = conf.getLong(HBASE_REGIONSERVER_LEASE_PERIOD_KEY, DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD);

    reinitialize();
  }

  /**
   * Creates all of the state that needs to be reconstructed in case we are
   * doing a restart. This is shared between the constructor and restart().
   * Both call it.
   * @throws IOException
   */
  private void reinitialize() throws IOException {
    this.abortRequested = false;
    this.stopRequested.set(false);

    // Server to handle client requests
    this.server = HBaseRPC.getServer(this, address.getBindAddress(),
      address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
      false, conf);
    this.server.setErrorHandler(this);
    // Address is giving a default IP for the moment. Will be changed after
    // calling the master.
    this.serverInfo = new HServerInfo(new HServerAddress(
      new InetSocketAddress(address.getBindAddress(),
      this.server.getListenerAddress().getPort())), System.currentTimeMillis(),
      this.conf.getInt("hbase.regionserver.info.port", 60030), machineName);
    if (this.serverInfo.getServerAddress() == null) {
      throw new NullPointerException("Server address cannot be null; " +
        "hbase-958 debugging");
    }
    reinitializeThreads();
    reinitializeZooKeeper();
    int nbBlocks = conf.getInt("hbase.regionserver.nbreservationblocks", 4);
    for(int i = 0; i < nbBlocks; i++)  {
      reservedSpace.add(new byte[DEFAULT_SIZE_RESERVATION_BLOCK]);
    }
  }

  private void reinitializeZooKeeper() throws IOException {
    zooKeeperWrapper = new ZooKeeperWrapper(conf, this);
    watchMasterAddress();
  }

  private void reinitializeThreads() {
    this.workerThread = new Thread(worker);

    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplitThread(this);

    // Log rolling thread
    this.hlogRoller = new LogRoller(this);

    // Background thread to check for major compactions; needed if region
    // has not gotten updates in a while.  Make it run at a lesser frequency.
    int multiplier = this.conf.getInt(THREAD_WAKE_FREQUENCY +
        ".multiplier", 1000);
    this.majorCompactionChecker = new MajorCompactionChecker(this,
      this.threadWakeFrequency * multiplier,  this.stopRequested);

    this.leases = new Leases(
        (int) conf.getLong(HBASE_REGIONSERVER_LEASE_PERIOD_KEY, DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD),
        this.threadWakeFrequency);
  }

  /**
   * We register ourselves as a watcher on the master address ZNode. This is
   * called by ZooKeeper when we get an event on that ZNode. When this method
   * is called it means either our master has died, or a new one has come up.
   * Either way we need to update our knowledge of the master.
   * @param event WatchedEvent from ZooKeeper.
   */
  public void process(WatchedEvent event) {
    EventType type = event.getType();
    KeeperState state = event.getState();
    LOG.info("Got ZooKeeper event, state: " + state + ", type: " +
      type + ", path: " + event.getPath());

    // Ignore events if we're shutting down.
    if (this.stopRequested.get()) {
      LOG.debug("Ignoring ZooKeeper event while shutting down");
      return;
    }

    if (state == KeeperState.Expired) {
      LOG.error("ZooKeeper session expired");
      boolean restart =
        this.conf.getBoolean("hbase.regionserver.restart.on.zk.expire", false);
      if (restart) {
        restart();
      } else {
        abort();
      }
    } else if (type == EventType.NodeDeleted) {
      watchMasterAddress();
    } else if (type == EventType.NodeCreated) {
      getMaster();

      // ZooKeeper watches are one time only, so we need to re-register our watch.
      watchMasterAddress();
    }
  }

  private void watchMasterAddress() {
    while (!stopRequested.get() && !zooKeeperWrapper.watchMasterAddress(this)) {
      LOG.warn("Unable to set watcher on ZooKeeper master address. Retrying.");
      sleeper.sleep();
    }
  }

  private void restart() {
    LOG.info("Restarting Region Server");
    abort();
    Threads.shutdown(regionServerThread);
    boolean done = false;
    while (!done) {
      try {
        reinitialize();
        done = true;
      } catch (IOException e) {
        LOG.debug("Error trying to reinitialize ZooKeeper", e);
      }
    }
    Thread t = new Thread(this);
    String name = regionServerThread.getName();
    t.setName(name);
    t.start();
  }

  /** @return ZooKeeperWrapper used by RegionServer. */
  public ZooKeeperWrapper getZooKeeperWrapper() {
    return zooKeeperWrapper;
  }

  /**
   * The HRegionServer sticks in this loop until closed. It repeatedly checks
   * in with the HMaster, sending heartbeats & reports, and receiving HRegion
   * load/unload instructions.
   */
  public void run() {
    regionServerThread = Thread.currentThread();
    boolean quiesceRequested = false;
    try {
      MapWritable w = null;
      while (!stopRequested.get()) {
        w = reportForDuty();
        if (w != null) {
          init(w);
          break;
        }
        sleeper.sleep();
        LOG.warn("No response from master on reportForDuty. Sleeping and " +
          "then trying again.");
      }
      List<HMsg> outboundMessages = new ArrayList<HMsg>();
      long lastMsg = 0;
      // Now ask master what it wants us to do and tell it what we have done
      for (int tries = 0; !stopRequested.get() && isHealthy();) {
        // Try to get the root region location from the master.
        if (!haveRootRegion.get()) {
          HServerAddress rootServer = zooKeeperWrapper.readRootRegionLocation();
          if (rootServer != null) {
            // By setting the root region location, we bypass the wait imposed on
            // HTable for all regions being assigned.
            this.connection.setRootRegionLocation(
                new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, rootServer));
            haveRootRegion.set(true);
          }
        }
        long now = System.currentTimeMillis();
        // Drop into the send loop if msgInterval has elapsed or if something
        // to send.  If we fail talking to the master, then we'll sleep below
        // on poll of the outboundMsgs blockingqueue.
        if ((now - lastMsg) >= msgInterval || !outboundMessages.isEmpty()) {
          try {
            doMetrics();
            MemoryUsage memory =
              ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            HServerLoad hsl = new HServerLoad(requestCount.get(),
              (int)(memory.getUsed()/1024/1024),
              (int)(memory.getMax()/1024/1024));
            for (HRegion r: onlineRegions.values()) {
              hsl.addRegionInfo(createRegionLoad(r));
            }
            this.serverInfo.setLoad(hsl);
            this.requestCount.set(0);
            addOutboundMsgs(outboundMessages);
            HMsg msgs[] = this.hbaseMaster.regionServerReport(
              serverInfo, outboundMessages.toArray(EMPTY_HMSG_ARRAY),
              getMostLoadedRegions());
            lastMsg = System.currentTimeMillis();
            updateOutboundMsgs(outboundMessages);
            outboundMessages.clear();
            if (this.quiesced.get() && onlineRegions.size() == 0) {
              // We've just told the master we're exiting because we aren't
              // serving any regions. So set the stop bit and exit.
              LOG.info("Server quiesced and not serving any regions. " +
                "Starting shutdown");
              stopRequested.set(true);
              this.outboundMsgs.clear();
              continue;
            }

            // Queue up the HMaster's instruction stream for processing
            boolean restart = false;
            for(int i = 0;
                !restart && !stopRequested.get() && i < msgs.length;
                i++) {
              LOG.info(msgs[i].toString());
              this.connection.unsetRootRegionLocation();
              switch(msgs[i].getType()) {

              case MSG_REGIONSERVER_STOP:
                stopRequested.set(true);
                break;

              case MSG_REGIONSERVER_QUIESCE:
                if (!quiesceRequested) {
                  try {
                    toDo.put(new ToDoEntry(msgs[i]));
                  } catch (InterruptedException e) {
                    throw new RuntimeException("Putting into msgQueue was " +
                        "interrupted.", e);
                  }
                  quiesceRequested = true;
                }
                break;

              default:
                if (fsOk) {
                  try {
                    toDo.put(new ToDoEntry(msgs[i]));
                  } catch (InterruptedException e) {
                    throw new RuntimeException("Putting into msgQueue was " +
                        "interrupted.", e);
                  }
                }
              }
            }
            // Reset tries count if we had a successful transaction.
            tries = 0;

            if (restart || this.stopRequested.get()) {
              toDo.clear();
              continue;
            }
          } catch (Exception e) { // FindBugs REC_CATCH_EXCEPTION
            if (e instanceof IOException) {
              e = RemoteExceptionHandler.checkIOException((IOException) e);
            }
            tries++;
            if (tries > 0 && (tries % this.numRetries) == 0) {
              // Check filesystem every so often.
              checkFileSystem();
            }
            if (this.stopRequested.get()) {
              LOG.info("Stop requested, clearing toDo despite exception");
              toDo.clear();
              continue;
            }
            LOG.warn("Attempt=" + tries, e);
            // No point retrying immediately; this is probably connection to
            // master issue.  Doing below will cause us to sleep.
            lastMsg = System.currentTimeMillis();
          }
        }
        now = System.currentTimeMillis();
        HMsg msg = this.outboundMsgs.poll((msgInterval - (now - lastMsg)),
          TimeUnit.MILLISECONDS);
        // If we got something, add it to list of things to send.
        if (msg != null) outboundMessages.add(msg);
        // Do some housekeeping before going back around
        housekeeping();
      } // for
    } catch (Throwable t) {
      if (!checkOOME(t)) {
        LOG.fatal("Unhandled exception. Aborting...", t);
        abort();
      }
    }
    this.leases.closeAfterLeasesExpire();
    this.worker.stop();
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
    LruBlockCache c = (LruBlockCache)StoreFile.getBlockCache(this.conf);
    if (c != null) c.shutdown();

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive?  If OOME could have exited already
    cacheFlusher.interruptIfNecessary();
    compactSplitThread.interruptIfNecessary();
    hlogRoller.interruptIfNecessary();
    this.majorCompactionChecker.interrupt();

    if (killed) {
      // Just skip out w/o closing regions.
    } else if (abortRequested) {
      if (this.fsOk) {
        // Only try to clean up if the file system is available
        try {
          if (this.hlog != null) {
            this.hlog.close();
            LOG.info("On abort, closed hlog");
          }
        } catch (Throwable e) {
          LOG.error("Unable to close log in abort",
            RemoteExceptionHandler.checkThrowable(e));
        }
        closeAllRegions(); // Don't leave any open file handles
      }
      LOG.info("aborting server at: " + this.serverInfo.getServerName());
    } else {
      ArrayList<HRegion> closedRegions = closeAllRegions();
      try {
        if (this.hlog != null) {
          hlog.closeAndDelete();
        }
      } catch (Throwable e) {
        LOG.error("Close and delete failed",
          RemoteExceptionHandler.checkThrowable(e));
      }
      try {
        HMsg[] exitMsg = new HMsg[closedRegions.size() + 1];
        exitMsg[0] = REPORT_EXITING;
        // Tell the master what regions we are/were serving
        int i = 1;
        for (HRegion region: closedRegions) {
          exitMsg[i++] = new HMsg(HMsg.Type.MSG_REPORT_CLOSE,
              region.getRegionInfo());
        }

        LOG.info("telling master that region server is shutting down at: " +
            serverInfo.getServerName());
        hbaseMaster.regionServerReport(serverInfo, exitMsg, (HRegionInfo[])null);
      } catch (Throwable e) {
        LOG.warn("Failed to send exiting message to master: ",
          RemoteExceptionHandler.checkThrowable(e));
      }
      LOG.info("stopping server at: " + this.serverInfo.getServerName());
    }

    // Make sure the proxy is down.
    if (this.hbaseMaster != null) {
      HBaseRPC.stopProxy(this.hbaseMaster);
      this.hbaseMaster = null;
    }

    if (!killed) {
      this.zooKeeperWrapper.close();
      join();
    }
    LOG.info(Thread.currentThread().getName() + " exiting");
  }

  /*
   * Add to the passed <code>msgs</code> messages to pass to the master.
   * @param msgs Current outboundMsgs array; we'll add messages to this List.
   */
  private void addOutboundMsgs(final List<HMsg> msgs) {
    if (msgs.isEmpty()) {
      this.outboundMsgs.drainTo(msgs);
      return;
    }
    OUTER: for (HMsg m: this.outboundMsgs) {
      for (HMsg mm: msgs) {
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
   * @param msgs Messages we sent the master.
   */
  private void updateOutboundMsgs(final List<HMsg> msgs) {
    if (msgs.isEmpty()) return;
    for (HMsg m: this.outboundMsgs) {
      for (HMsg mm: msgs) {
        if (mm.equals(m)) {
          this.outboundMsgs.remove(m);
          break;
        }
      }
    }
  }

  /*
   * Run init. Sets up hlog and starts up all server threads.
   * @param c Extra configuration.
   */
  protected void init(final MapWritable c) throws IOException {
    try {
      for (Map.Entry<Writable, Writable> e: c.entrySet()) {
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
      // TODO: The below used to be this.address != null.  Was broken by what
      // looks like a mistake in:
      //
      // HBASE-1215 migration; metautils scan of meta region was broken; wouldn't see first row
      // ------------------------------------------------------------------------
      // r796326 | stack | 2009-07-21 07:40:34 -0700 (Tue, 21 Jul 2009) | 38 lines
      if (hra != null) {
        HServerAddress hsa = new HServerAddress (hra,
          this.serverInfo.getServerAddress().getPort());
        LOG.info("Master passed us address to use. Was=" +
          this.serverInfo.getServerAddress() + ", Now=" + hra);
        this.serverInfo.setServerAddress(hsa);
      }
      // Master sent us hbase.rootdir to use. Should be fully qualified
      // path with file system specification included.  Set 'fs.defaultFS'
      // to match the filesystem on hbase.rootdir else underlying hadoop hdfs
      // accessors will be going against wrong filesystem (unless all is set
      // to defaults).
      this.conf.set("fs.defaultFS", this.conf.get("hbase.rootdir"));
      this.conf.setBoolean("fs.automatic.close", false);
      this.fs = FileSystem.get(this.conf);
      this.rootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
      this.hlog = setupHLog();
      // Init in here rather than in constructor after thread name has been set
      this.metrics = new RegionServerMetrics();
      startServiceThreads();
      isOnline = true;
    } catch (Throwable e) {
      this.isOnline = false;
      this.stopRequested.set(true);
      throw convertThrowableToIOE(cleanup(e, "Failed init"),
        "Region server startup failed");
    }
  }

  /*
   * @param r Region to get RegionLoad for.
   * @return RegionLoad instance.
   * @throws IOException
   */
  private HServerLoad.RegionLoad createRegionLoad(final HRegion r) {
    byte[] name = r.getRegionName();
    int stores = 0;
    int storefiles = 0;
    int storefileSizeMB = 0;
    int memstoreSizeMB = (int)(r.memstoreSize.get()/1024/1024);
    int storefileIndexSizeMB = 0;
    synchronized (r.stores) {
      stores += r.stores.size();
      for (Store store: r.stores.values()) {
        storefiles += store.getStorefilesCount();
        storefileSizeMB +=
          (int)(store.getStorefilesSize()/1024/1024);
        storefileIndexSizeMB +=
          (int)(store.getStorefilesIndexSize()/1024/1024);
      }
    }
    return new HServerLoad.RegionLoad(name, stores, storefiles,
      storefileSizeMB, memstoreSizeMB, storefileIndexSizeMB);
  }

  /**
   * @param regionName
   * @return An instance of RegionLoad.
   * @throws IOException
   */
  public HServerLoad.RegionLoad createRegionLoad(final byte [] regionName) {
    return createRegionLoad(this.onlineRegions.get(Bytes.mapKey(regionName)));
  }

  /*
   * Cleanup after Throwable caught invoking method.  Converts <code>t</code>
   * to IOE if it isn't already.
   * @param t Throwable
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t) {
    return cleanup(t, null);
  }

  /*
   * Cleanup after Throwable caught invoking method.  Converts <code>t</code>
   * to IOE if it isn't already.
   * @param t Throwable
   * @param msg Message to log in error.  Can be null.
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t, final String msg) {
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
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t) {
    return convertThrowableToIOE(t, null);
  }

  /*
   * @param t
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t,
      final String msg) {
    return (t instanceof IOException? (IOException)t:
      msg == null || msg.length() == 0?
        new IOException(t): new IOException(msg, t));
  }
  /*
   * Check if an OOME and if so, call abort.
   * @param e
   * @return True if we OOME'd and are aborting.
   */
  public boolean checkOOME(final Throwable e) {
    boolean stop = false;
    if (e instanceof OutOfMemoryError ||
      (e.getCause() != null && e.getCause() instanceof OutOfMemoryError) ||
      (e.getMessage() != null &&
        e.getMessage().contains("java.lang.OutOfMemoryError"))) {
      LOG.fatal("OutOfMemoryError, aborting.", e);
      abort();
      stop = true;
    }
    return stop;
  }


  /**
   * Checks to see if the file system is still accessible.
   * If not, sets abortRequested and stopRequested
   *
   * @return false if file system is not available
   */
  protected boolean checkFileSystem() {
    if (this.fsOk && this.fs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
      } catch (IOException e) {
        LOG.fatal("Shutting down HRegionServer: file system not available", e);
        abort();
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

    MajorCompactionChecker(final HRegionServer h,
        final int sleepTime, final AtomicBoolean stopper) {
      super(sleepTime, stopper);
      this.instance = h;
      LOG.info("Runs every " + sleepTime + "ms");
    }

    @Override
    protected void chore() {
      Set<Integer> keys = this.instance.onlineRegions.keySet();
      for (Integer i: keys) {
        HRegion r = this.instance.onlineRegions.get(i);
        try {
          if (r != null && r.isMajorCompaction()) {
            // Queue a compaction.  Will recognize if major is needed.
            this.instance.compactSplitThread.
              compactionRequested(r, getName() + " requests major compaction");
          }
        } catch (IOException e) {
          LOG.warn("Failed major compaction check on " + r, e);
        }
      }
    }
  }

  /**
   * Report the status of the server. A server is online once all the startup
   * is completed (setting up filesystem, starting service threads, etc.). This
   * method is designed mostly to be useful in tests.
   * @return true if online, false if not.
   */
  public boolean isOnline() {
    return isOnline;
  }

  private HLog setupHLog() throws IOException {
    Path oldLogDir = new Path(rootDir, HREGION_OLDLOGDIR_NAME);
    Path logdir = new Path(rootDir, HLog.getHLogDirectoryName(this.serverInfo));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Log dir " + logdir);
    }
    if (fs.exists(logdir)) {
      throw new RegionServerRunningException("region server already " +
        "running at " + this.serverInfo.getServerName() +
        " because logdir " + logdir.toString() + " exists");
    }
    HLog newlog = instantiateHLog(logdir, oldLogDir);
    return newlog;
  }

  // instantiate
  protected HLog instantiateHLog(Path logdir, Path oldLogDir) throws IOException {
    HLog newlog = new HLog(fs, logdir, oldLogDir, conf, hlogRoller, null,
        serverInfo.getServerAddress().toString());
    return newlog;
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
    // and then per store carried?  Can I make metrics be sloppier and avoid
    // the synchronizations?
    int stores = 0;
    int storefiles = 0;
    long memstoreSize = 0;
    long storefileIndexSize = 0;
    synchronized (this.onlineRegions) {
      for (Map.Entry<Integer, HRegion> e: this.onlineRegions.entrySet()) {
        HRegion r = e.getValue();
        memstoreSize += r.memstoreSize.get();
        synchronized (r.stores) {
          stores += r.stores.size();
          for(Map.Entry<byte [], Store> ee: r.stores.entrySet()) {
            Store store = ee.getValue();
            storefiles += store.getStorefilesCount();
            storefileIndexSize += store.getStorefilesIndexSize();
          }
        }
      }
    }
    this.metrics.stores.set(stores);
    this.metrics.storefiles.set(storefiles);
    this.metrics.memstoreSizeMB.set((int)(memstoreSize/(1024*1024)));
    this.metrics.storefileIndexSizeMB.set((int)(storefileIndexSize/(1024*1024)));
    this.metrics.compactionQueueSize.set(compactSplitThread.
      getCompactionQueueSize());

    LruBlockCache lruBlockCache = (LruBlockCache)StoreFile.getBlockCache(conf);
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
   * get an unhandled exception.  We cannot set the handler on all threads.
   * Server's internal Listener thread is off limits.  For Server, if an OOME,
   * it waits a while then retries.  Meantime, a flush or a compaction that
   * tries to run should trigger same critical condition and the shutdown will
   * run.  On its way out, this server will shut down Server.  Leases are sort
   * of inbetween. It has an internal thread that while it inherits from
   * Chore, it keeps its own internal stop mechanism so needs to be stopped
   * by this hosting server.  Worker logs the exception and exits.
   */
  private void startServiceThreads() throws IOException {
    String n = Thread.currentThread().getName();
    UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        abort();
        LOG.fatal("Set stop flag in " + t.getName(), e);
      }
    };
    Threads.setDaemonThreadRunning(this.hlogRoller, n + ".logRoller",
        handler);
    Threads.setDaemonThreadRunning(this.cacheFlusher, n + ".cacheFlusher",
      handler);
    Threads.setDaemonThreadRunning(this.compactSplitThread, n + ".compactor",
        handler);
    Threads.setDaemonThreadRunning(this.workerThread, n + ".worker", handler);
    Threads.setDaemonThreadRunning(this.majorCompactionChecker,
        n + ".majorCompactionChecker", handler);

    // Leases is not a Thread. Internally it runs a daemon thread.  If it gets
    // an unhandled exception, it will just exit.
    this.leases.setName(n + ".leaseChecker");
    this.leases.start();
    // Put up info server.
    int port = this.conf.getInt("hbase.regionserver.info.port", 60030);
    // -1 is for disabling info server
    if (port >= 0) {
      String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");
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
          if (!auto){
            // auto bind disabled throw BindException
            throw e;
          }
          // auto bind enabled, try to use another port
          LOG.info("Failed binding http info server to port: " + port);
          port++;
          // update HRS server info port.
          this.serverInfo = new HServerInfo(this.serverInfo.getServerAddress(),
            this.serverInfo.getStartCode(),  port,
            this.serverInfo.getHostname());
        }
      }
    }

    // Start Server.  This service is like leases in that it internally runs
    // a thread.
    this.server.start();
    LOG.info("HRegionServer started at: " +
      this.serverInfo.getServerAddress().toString());
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
    if (!(leases.isAlive() && compactSplitThread.isAlive() &&
        cacheFlusher.isAlive() && hlogRoller.isAlive() &&
        workerThread.isAlive() && this.majorCompactionChecker.isAlive())) {
      // One or more threads are no longer alive - shut down
      stop();
      return false;
    }
    return true;
  }

  /*
   * Run some housekeeping tasks.
   */
  private void housekeeping() {
    // If the todo list has > 0 messages, iterate looking for open region
    // messages. Send the master a message that we're working on its
    // processing so it doesn't assign the region elsewhere.
    if (this.toDo.isEmpty()) {
      return;
    }
    // This iterator isn't safe if elements are gone and HRS.Worker could
    // remove them (it already checks for null there). Goes from oldest.
    for (ToDoEntry e: this.toDo) {
      if(e == null) {
        LOG.warn("toDo gave a null entry during iteration");
        break;
      }
      HMsg msg = e.msg;
      if (msg != null) {
        if (msg.isType(HMsg.Type.MSG_REGION_OPEN)) {
          addProcessingMessage(msg.getRegionInfo());
        }
      } else {
        LOG.warn("Message is empty: " + e);
      }
    }
  }

  /** @return the HLog */
  public HLog getLog() {
    return this.hlog;
  }

  /**
   * Sets a flag that will cause all the HRegionServer threads to shut down
   * in an orderly fashion.  Used by unit tests.
   */
  public void stop() {
    this.stopRequested.set(true);
    synchronized(this) {
      // Wakes run() if it is sleeping
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }
  }

  /**
   * Cause the server to exit without closing the regions it is serving, the
   * log it is using and without notifying the master.
   * Used unit testing and on catastrophic events such as HDFS is yanked out
   * from under hbase or we OOME.
   */
  public void abort() {
    this.abortRequested = true;
    this.reservedSpace.clear();
    LOG.info("Dump of metrics: " + this.metrics.toString());
    stop();
  }

  /*
   * Simulate a kill -9 of this server.
   * Exits w/o closing regions or cleaninup logs but it does close socket in
   * case want to bring up server on old hostname+port immediately.
   */
  protected void kill() {
    this.killed = true;
    abort();
  }

  /**
   * Wait on all threads to finish.
   * Presumption is that all closes and stops have already been called.
   */
  protected void join() {
    Threads.shutdown(this.majorCompactionChecker);
    Threads.shutdown(this.workerThread);
    Threads.shutdown(this.cacheFlusher);
    Threads.shutdown(this.compactSplitThread);
    Threads.shutdown(this.hlogRoller);
  }

  private boolean getMaster() {
    HServerAddress masterAddress = null;
    while (masterAddress == null) {
      if (stopRequested.get()) {
        return false;
      }
      try {
        masterAddress = zooKeeperWrapper.readMasterAddressOrThrow();
      } catch (IOException e) {
        LOG.warn("Unable to read master address from ZooKeeper. Retrying." +
                 " Error was:", e);
        sleeper.sleep();
      }
    }

    LOG.info("Telling master at " + masterAddress + " that we are up");
    HMasterRegionInterface master = null;
    while (!stopRequested.get() && master == null) {
      try {
        // Do initial RPC setup.  The final argument indicates that the RPC
        // should retry indefinitely.
        master = (HMasterRegionInterface)HBaseRPC.waitForProxy(
          HMasterRegionInterface.class, HBaseRPCProtocolVersion.versionID,
          masterAddress.getInetSocketAddress(), this.conf, -1, this.rpcTimeout);
      } catch (IOException e) {
        LOG.warn("Unable to connect to master. Retrying. Error was:", e);
        sleeper.sleep();
      }
    }
    this.hbaseMaster = master;
    return true;
  }

  /*
   * Let the master know we're here
   * Run initialization using parameters passed us by the master.
   */
  private MapWritable reportForDuty() {
    while (!stopRequested.get() && !getMaster()) {
      sleeper.sleep();
      LOG.warn("Unable to get master for initialization");
    }

    MapWritable result = null;
    long lastMsg = 0;
    while(!stopRequested.get()) {
      try {
        this.requestCount.set(0);
        MemoryUsage memory =
          ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        HServerLoad hsl = new HServerLoad(0, (int)memory.getUsed()/1024/1024,
          (int)memory.getMax()/1024/1024);
        this.serverInfo.setLoad(hsl);
        if (LOG.isDebugEnabled())
          LOG.debug("sending initial server load: " + hsl);
        lastMsg = System.currentTimeMillis();
        boolean startCodeOk = false;
        while(!startCodeOk) {
          this.serverInfo = createServerInfoWithNewStartCode(this.serverInfo);
          startCodeOk = zooKeeperWrapper.writeRSLocation(this.serverInfo);
          if(!startCodeOk) {
           LOG.debug("Start code already taken, trying another one");
          }
        }
        result = this.hbaseMaster.regionServerStartup(this.serverInfo);
        break;
      } catch (IOException e) {
        LOG.warn("error telling master we are up", e);
      }
      sleeper.sleep(lastMsg);
    }
    return result;
  }

  private HServerInfo createServerInfoWithNewStartCode(final HServerInfo hsi) {
    return new HServerInfo(hsi.getServerAddress(), hsi.getInfoPort(),
      hsi.getHostname());
  }

  /* Add to the outbound message buffer */
  private void reportOpen(HRegionInfo region) {
    this.outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_OPEN, region));
  }

  /* Add to the outbound message buffer */
  private void reportClose(HRegionInfo region) {
    reportClose(region, null);
  }

  /* Add to the outbound message buffer */
  private void reportClose(final HRegionInfo region, final byte[] message) {
    this.outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_CLOSE, region, message));
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
    this.outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS,
      oldRegion, newRegionA, newRegionB,
      Bytes.toBytes("Daughters; " +
          newRegionA.getRegionNameAsString() + ", " +
          newRegionB.getRegionNameAsString())));
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMaster-given operations
  //////////////////////////////////////////////////////////////////////////////

  /*
   * Data structure to hold a HMsg and retries count.
   */
  private static final class ToDoEntry {
    protected final AtomicInteger tries = new AtomicInteger(0);
    protected final HMsg msg;

    ToDoEntry(final HMsg msg) {
      this.msg = msg;
    }
  }

  final BlockingQueue<ToDoEntry> toDo = new LinkedBlockingQueue<ToDoEntry>();
  private Worker worker;
  private Thread workerThread;

  /** Thread that performs long running requests from the master */
  class Worker implements Runnable {
    void stop() {
      synchronized(toDo) {
        toDo.notifyAll();
      }
    }

    public void run() {
      try {
        while(!stopRequested.get()) {
          ToDoEntry e = null;
          try {
            e = toDo.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
            if(e == null || stopRequested.get()) {
              continue;
            }
            LOG.info("Worker: " + e.msg);
            HRegion region = null;
            HRegionInfo info = e.msg.getRegionInfo();
            switch(e.msg.getType()) {

            case MSG_REGIONSERVER_QUIESCE:
              closeUserRegions();
              break;

            case MSG_REGION_OPEN:
              // Open a region
              if (!haveRootRegion.get() && !info.isRootRegion()) {
                // root region is not online yet. requeue this task
                LOG.info("putting region open request back into queue because" +
                    " root region is not yet available");
                try {
                  toDo.put(e);
                } catch (InterruptedException ex) {
                  LOG.warn("insertion into toDo queue was interrupted", ex);
                  break;
                }
              }
              openRegion(info);
              break;

            case MSG_REGION_CLOSE:
              // Close a region
              closeRegion(e.msg.getRegionInfo(), true);
              break;

            case MSG_REGION_CLOSE_WITHOUT_REPORT:
              // Close a region, don't reply
              closeRegion(e.msg.getRegionInfo(), false);
              break;

            case MSG_REGION_SPLIT:
              region = getRegion(info.getRegionName());
              region.flushcache();
              region.shouldSplit(true);
              // force a compaction; split will be side-effect.
              compactSplitThread.compactionRequested(region,
                e.msg.getType().name());
              break;

            case MSG_REGION_MAJOR_COMPACT:
            case MSG_REGION_COMPACT:
              // Compact a region
              region = getRegion(info.getRegionName());
              compactSplitThread.compactionRequested(region,
                e.msg.isType(Type.MSG_REGION_MAJOR_COMPACT),
                e.msg.getType().name());
              break;

            case MSG_REGION_FLUSH:
              region = getRegion(info.getRegionName());
              region.flushcache();
              break;

            case TESTING_MSG_BLOCK_RS:
              while (!stopRequested.get()) {
                Threads.sleep(1000);
                LOG.info("Regionserver blocked by " +
                  HMsg.Type.TESTING_MSG_BLOCK_RS + "; " + stopRequested.get());
              }
              break;

            default:
              throw new AssertionError(
                  "Impossible state during msg processing.  Instruction: "
                  + e.msg.toString());
            }
          } catch (InterruptedException ex) {
            LOG.warn("Processing Worker queue", ex);
          } catch (Exception ex) {
            if (ex instanceof IOException) {
              ex = RemoteExceptionHandler.checkIOException((IOException) ex);
            }
            if(e != null && e.tries.get() < numRetries) {
              LOG.warn(ex);
              e.tries.incrementAndGet();
              try {
                toDo.put(e);
              } catch (InterruptedException ie) {
                throw new RuntimeException("Putting into msgQueue was " +
                    "interrupted.", ex);
              }
            } else {
              LOG.error("unable to process message" +
                  (e != null ? (": " + e.msg.toString()) : ""), ex);
              if (!checkFileSystem()) {
                break;
              }
            }
          }
        }
      } catch(Throwable t) {
        if (!checkOOME(t)) {
          LOG.fatal("Unhandled exception", t);
        }
      } finally {
        LOG.info("worker thread exiting");
      }
    }
  }

  void openRegion(final HRegionInfo regionInfo) {
    Integer mapKey = Bytes.mapKey(regionInfo.getRegionName());
    HRegion region = this.onlineRegions.get(mapKey);
    if (region == null) {
      try {
        region = instantiateRegion(regionInfo);
        // Startup a compaction early if one is needed, if region has references
        // or if a store has too many store files
        if (region.hasReferences() || region.hasTooManyStoreFiles()) {
          this.compactSplitThread.compactionRequested(region,
            region.hasReferences() ? "Region has references on open" :
                                     "Region has too many store files");
        }
      } catch (Throwable e) {
        Throwable t = cleanup(e,
          "Error opening " + regionInfo.getRegionNameAsString());
        // TODO: add an extra field in HRegionInfo to indicate that there is
        // an error. We can't do that now because that would be an incompatible
        // change that would require a migration
        reportClose(regionInfo, StringUtils.stringifyException(t).getBytes());
        return;
      }
      this.lock.writeLock().lock();
      try {
        this.hlog.setSequenceNumber(region.getMinSequenceId());
        this.onlineRegions.put(mapKey, region);
      } finally {
        this.lock.writeLock().unlock();
      }
    }
    reportOpen(regionInfo);
  }

  protected HRegion instantiateRegion(final HRegionInfo regionInfo)
      throws IOException {
    HRegion r = HRegion.newHRegion(HTableDescriptor.getTableDir(rootDir, regionInfo
        .getTableDesc().getName()), this.hlog, this.fs, conf, regionInfo,
        this.cacheFlusher);
    r.initialize(null,  new Progressable() {
      public void progress() {
        addProcessingMessage(regionInfo);
      }
    });
    return r;
  }

  /**
   * Add a MSG_REPORT_PROCESS_OPEN to the outbound queue.
   * This method is called while region is in the queue of regions to process
   * and then while the region is being opened, it is called from the Worker
   * thread that is running the region open.
   * @param hri Region to add the message for
   */
  public void addProcessingMessage(final HRegionInfo hri) {
    getOutboundMsgs().add(new HMsg(HMsg.Type.MSG_REPORT_PROCESS_OPEN, hri));
  }

  protected void closeRegion(final HRegionInfo hri, final boolean reportWhenCompleted)
  throws IOException {
    HRegion region = this.removeFromOnlineRegions(hri);
    if (region != null) {
      region.close();
      if(reportWhenCompleted) {
        reportClose(hri);
      }
    }
  }

  /** Called either when the master tells us to restart or from stop() */
  ArrayList<HRegion> closeAllRegions() {
    ArrayList<HRegion> regionsToClose = new ArrayList<HRegion>();
    this.lock.writeLock().lock();
    try {
      regionsToClose.addAll(onlineRegions.values());
      onlineRegions.clear();
    } finally {
      this.lock.writeLock().unlock();
    }
    // Close any outstanding scanners.  Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, InternalScanner> e: this.scanners.entrySet()) {
      try {
        e.getValue().close();
      } catch (IOException ioe) {
        LOG.warn("Closing scanner " + e.getKey(), ioe);
      }
    }
    for (HRegion region: regionsToClose) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("closing region " + Bytes.toString(region.getRegionName()));
      }
      try {
        region.close(abortRequested);
      } catch (Throwable e) {
        cleanup(e, "Error closing " + Bytes.toString(region.getRegionName()));
      }
    }
    return regionsToClose;
  }

  /*
   * Thread to run close of a region.
   */
  private static class RegionCloserThread extends Thread {
    private final HRegion r;

    protected RegionCloserThread(final HRegion r) {
      super(Thread.currentThread().getName() + ".regionCloser." + r.toString());
      this.r = r;
    }

    @Override
    public void run() {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closing region " + r.toString());
        }
        r.close();
      } catch (Throwable e) {
        LOG.error("Error closing region " + r.toString(),
          RemoteExceptionHandler.checkThrowable(e));
      }
    }
  }

  /** Called as the first stage of cluster shutdown. */
  void closeUserRegions() {
    ArrayList<HRegion> regionsToClose = new ArrayList<HRegion>();
    this.lock.writeLock().lock();
    try {
      synchronized (onlineRegions) {
        for (Iterator<Map.Entry<Integer, HRegion>> i =
            onlineRegions.entrySet().iterator(); i.hasNext();) {
          Map.Entry<Integer, HRegion> e = i.next();
          HRegion r = e.getValue();
          if (!r.getRegionInfo().isMetaRegion()) {
            regionsToClose.add(r);
            i.remove();
          }
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    // Run region closes in parallel.
    Set<Thread> threads = new HashSet<Thread>();
    try {
      for (final HRegion r : regionsToClose) {
        RegionCloserThread t = new RegionCloserThread(r);
        t.start();
        threads.add(t);
      }
    } finally {
      for (Thread t : threads) {
        while (t.isAlive()) {
          try {
            t.join();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }
    this.quiesced.set(true);
    if (onlineRegions.size() == 0) {
      outboundMsgs.add(REPORT_EXITING);
    } else {
      outboundMsgs.add(REPORT_QUIESCED);
    }
  }

  //
  // HRegionInterface
  //

  public HRegionInfo getRegionInfo(final byte [] regionName)
  throws NotServingRegionException {
    requestCount.incrementAndGet();
    return getRegion(regionName).getRegionInfo();
  }


  public Result getClosestRowBefore(final byte [] regionName,
    final byte [] row, final byte [] family)
  throws IOException {
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
  public Result get(byte [] regionName, Get get) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      return region.get(get, getLockFromId(get.getLockId()));
    } catch(Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public boolean exists(byte [] regionName, Get get) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      Result r = region.get(get, getLockFromId(get.getLockId()));
      return r != null && !r.isEmpty();
    } catch(Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public void put(final byte [] regionName, final Put put)
  throws IOException {
    if (put.getRow() == null)
      throw new IllegalArgumentException("update has null row");

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

  public int put(final byte[] regionName, final Put [] puts)
  throws IOException {
    // Count of Puts processed.
    int i = 0;
    checkOpen();
    HRegion region = null;
    boolean writeToWAL = true;
    try {
      region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      for (i = 0; i < puts.length; i++) {
        this.requestCount.incrementAndGet();
        Integer lock = getLockFromId(puts[i].getLockId());
        writeToWAL &= puts[i].getWriteToWAL();
        region.put(puts[i], lock);
      }

    } catch (WrongRegionException ex) {
      LOG.debug("Batch puts: " + i, ex);
      return i;
    } catch (NotServingRegionException ex) {
      LOG.debug("Batch puts interrupted at index=" + i + " because:" +
        ex.getMessage());
      return i;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
    return -1;
  }

  /**
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param value the expected value
   * @param put
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndPut(final byte[] regionName, final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put) throws IOException{
    //Getting actual value
    Get get = new Get(row);
    get.addColumn(family, qualifier);

    checkOpen();
    this.requestCount.incrementAndGet();
    HRegion region = getRegion(regionName);
    try {
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      boolean retval = region.checkAndPut(row, family, qualifier, value, put,
        getLockFromId(put.getLockId()), true);
      return retval;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  //
  // remote scanner interface
  //

  public long openScanner(byte [] regionName, Scan scan)
  throws IOException {
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
    this.leases.
      createLease(scannerName, new ScannerListener(scannerName));
    return scannerId;
  }

  public Result next(final long scannerId) throws IOException {
    Result [] res = next(scannerId, 1);
    if(res == null || res.length == 0) {
      return null;
    }
    return res[0];
  }

  public Result [] next(final long scannerId, int nbRows) throws IOException {
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
      for (int i = 0; i < nbRows && currentScanResultSize < maxScannerResultSize; i++) {
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
      // HRegion.RegionScanner.  The alternative is to change InternalScanner
      // interface but its used everywhere whereas we just need a bit of info
      // from HRegion.RegionScanner, IF its filter if any is done with the scan
      // and wants to tell the client to stop the scan.  This is done by passing
      // a null result.
      return ((HRegion.RegionScanner)s).isFilterDone() && results.isEmpty()?
        null: results.toArray(new Result[0]);
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
   * Instantiated as a scanner lease.
   * If the lease times out, the scanner is closed
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
  public void delete(final byte [] regionName, final Delete delete)
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

  public int delete(final byte[] regionName, final Delete [] deletes)
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
      Integer[] locks = new Integer[deletes.length];
      for (i = 0; i < deletes.length; i++) {
        this.requestCount.incrementAndGet();
        locks[i] = getLockFromId(deletes[i].getLockId());
        region.delete(deletes[i], locks[i], writeToWAL);
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

  public long lockRow(byte [] regionName, byte [] row)
  throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if(regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if(row == null) {
      npe = new NullPointerException("row to lock is null");
    }
    if(npe != null) {
      IOException io = new IOException("Invalid arguments to lockRow");
      io.initCause(npe);
      throw io;
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      Integer r = region.obtainRowLock(row);
      long lockId = addRowLock(r,region);
      LOG.debug("Row lock " + lockId + " explicitly acquired by client");
      return lockId;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t,
        "Error obtaining row lock (fsOk: " + this.fsOk + ")"));
    }
  }

  protected long addRowLock(Integer r, HRegion region) throws LeaseStillHeldException {
    long lockId = -1L;
    lockId = rand.nextLong();
    String lockName = String.valueOf(lockId);
    rowlocks.put(lockName, r);
    this.leases.
      createLease(lockName, new RowLockListener(lockName, region));
    return lockId;
  }

  /**
   * Method to get the Integer lock identifier used internally
   * from the long lock identifier used by the client.
   * @param lockId long row lock identifier from client
   * @return intId Integer row lock used internally in HRegion
   * @throws IOException Thrown if this is not a valid client lock id.
   */
  Integer getLockFromId(long lockId)
  throws IOException {
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

  public void unlockRow(byte [] regionName, long lockId)
  throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if(regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if(lockId == -1L) {
      npe = new NullPointerException("lockId is null");
    }
    if(npe != null) {
      IOException io = new IOException("Invalid arguments to unlockRow");
      io.initCause(npe);
      throw io;
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      String lockName = String.valueOf(lockId);
      Integer r = rowlocks.remove(lockName);
      if(r == null) {
        throw new UnknownRowLockException(lockName);
      }
      region.releaseRowLock(r);
      this.leases.cancelLease(lockName);
      LOG.debug("Row lock " + lockId + " has been explicitly released by client");
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public void bulkLoadHFile(
      String hfilePath, byte[] regionName, byte[] familyName)
  throws IOException {
    HRegion region = getRegion(regionName);
    region.bulkLoadHFile(hfilePath, familyName);
  }
  
  Map<String, Integer> rowlocks =
    new ConcurrentHashMap<String, Integer>();

  /**
   * Instantiated as a row lock lease.
   * If the lease times out, the row lock is released
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
      if(r != null) {
        region.releaseRowLock(r);
      }
    }
  }

  /** @return the info server */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopRequested() {
    return this.stopRequested.get();
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

  /**
   * @return Immutable list of this servers regions.
   */
  public Collection<HRegion> getOnlineRegions() {
    return Collections.unmodifiableCollection(onlineRegions.values());
  }

  public HRegion [] getOnlineRegionsAsArray() {
    return getOnlineRegions().toArray(new HRegion[0]);
  }

  /**
   * @return The HRegionInfos from online regions sorted
   */
  public SortedSet<HRegionInfo> getSortedOnlineRegionInfos() {
    SortedSet<HRegionInfo> result = new TreeSet<HRegionInfo>();
    synchronized(this.onlineRegions) {
      for (HRegion r: this.onlineRegions.values()) {
        result.add(r.getRegionInfo());
      }
    }
    return result;
  }

  /**
   * This method removes HRegion corresponding to hri from the Map of onlineRegions.
   *
   * @param hri the HRegionInfo corresponding to the HRegion to-be-removed.
   * @return the removed HRegion, or null if the HRegion was not in onlineRegions.
   */
  HRegion removeFromOnlineRegions(HRegionInfo hri) {
    this.lock.writeLock().lock();
    HRegion toReturn = null;
    try {
      toReturn = onlineRegions.remove(Bytes.mapKey(hri.getRegionName()));
    } finally {
      this.lock.writeLock().unlock();
    }
    return toReturn;
  }

  /**
   * @return A new Map of online regions sorted by region size with the first
   * entry being the biggest.
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

  /**
   * @param regionName
   * @return HRegion for the passed <code>regionName</code> or null if named
   * region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte [] regionName) {
    return onlineRegions.get(Bytes.mapKey(regionName));
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
   * @param regionName Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final byte [] regionName)
  throws NotServingRegionException {
    HRegion region = null;
    this.lock.readLock().lock();
    try {
      region = onlineRegions.get(Integer.valueOf(Bytes.hashCode(regionName)));
      if (region == null) {
        throw new NotServingRegionException(regionName);
      }
      return region;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Get the top N most loaded regions this server is serving so we can
   * tell the master which regions it can reallocate if we're overloaded.
   * TODO: actually calculate which regions are most loaded. (Right now, we're
   * just grabbing the first N regions being served regardless of load.)
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
    if (this.stopRequested.get() || this.abortRequested) {
      throw new IOException("Server not running" +
        (this.abortRequested? ", aborting": ""));
    }
    if (!fsOk) {
      throw new IOException("File system not available");
    }
  }

  /**
   * @return Returns list of non-closed regions hosted on this server.  If no
   * regions to check, returns an empty list.
   */
  protected Set<HRegion> getRegionsToCheck() {
    HashSet<HRegion> regionsToCheck = new HashSet<HRegion>();
    //TODO: is this locking necessary?
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

  public long getProtocolVersion(final String protocol,
      final long clientVersion)
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
  public HServerInfo getServerInfo() { return this.serverInfo; }

  /** {@inheritDoc} */
  public long incrementColumnValue(byte [] regionName, byte [] row,
      byte [] family, byte [] qualifier, long amount, boolean writeToWAL)
  throws IOException {
    checkOpen();

    if (regionName == null) {
      throw new IOException("Invalid arguments to incrementColumnValue " +
      "regionName is null");
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
    for(int i = 0; ite.hasNext(); i++) {
      regions[i] = ite.next().getRegionInfo();
    }
    return regions;
  }

  /** {@inheritDoc} */
  public HServerInfo getHServerInfo() throws IOException {
    return serverInfo;
  }

  @Override
  public MultiPutResponse multiPut(MultiPut puts) throws IOException {
    MultiPutResponse resp = new MultiPutResponse();

    // do each region as it's own.
    for( Map.Entry<byte[],List<Put>> e: puts.puts.entrySet()) {
      int result = put(e.getKey(), e.getValue().toArray(new Put[]{}));
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
   * @return the interval
   */
  public int getThreadWakeFrequency() {
    return threadWakeFrequency;
  }

  //
  // Main program and support routines
  //

  /**
   * @param hrs
   * @return Thread the RegionServer is running in correctly named.
   */
  public static Thread startRegionServer(final HRegionServer hrs) {
    return startRegionServer(hrs,
      "regionserver" + hrs.getServerInfo().getServerAddress().getPort());
  }

  /**
   * @param hrs
   * @param name
   * @return Thread the RegionServer is running in correctly named.
   */
  public static Thread startRegionServer(final HRegionServer hrs,
      final String name) {
    Thread t = new Thread(hrs);
    t.setName(name);
    t.start();
    return t;
  }

  private static void printUsageAndExit() {
    printUsageAndExit(null);
  }

  private static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.err.println("Usage: java org.apache.hbase.HRegionServer start|stop");
    System.exit(0);
  }

  /**
   * Utility for constructing an instance of the passed HRegionServer class.
   * @param regionServerClass
   * @param conf2
   * @return HRegionServer instance.
   */
  public static HRegionServer constructRegionServer(Class<? extends HRegionServer> regionServerClass,
      final Configuration conf2)  {
    try {
      Constructor<? extends HRegionServer> c =
        regionServerClass.getConstructor(Configuration.class);
      return c.newInstance(conf2);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " +
        "Master: " + regionServerClass.toString(), e);
    }
  }

  /**
   * Do class main.
   * @param args
   * @param regionServerClass HRegionServer to instantiate.
   */
  protected static void doMain(final String [] args,
      final Class<? extends HRegionServer> regionServerClass) {
    if (args.length < 1) {
      printUsageAndExit();
    }
    Configuration conf = HBaseConfiguration.create();

    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).
    for (String cmd: args) {
      if (cmd.equals("start")) {
        try {
          // If 'local', don't start a region server here.  Defer to
          // LocalHBaseCluster.  It manages 'local' clusters.
          if (LocalHBaseCluster.isLocal(conf)) {
            LOG.warn("Not starting a distinct region server because " +
              HConstants.CLUSTER_DISTRIBUTED + " is false");
          } else {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            if (runtime != null) {
              LOG.info("vmInputArguments=" + runtime.getInputArguments());
            }
            HRegionServer hrs = constructRegionServer(regionServerClass, conf);
            startRegionServer(hrs);
          }
        } catch (Throwable t) {
          LOG.error( "Can not start region server because "+
              StringUtils.stringifyException(t) );
        }
        break;
      }

      if (cmd.equals("stop")) {
        printUsageAndExit("To shutdown the regionserver run " +
          "bin/hbase-daemon.sh stop regionserver or send a kill signal to" +
          "the regionserver pid");
      }

      // Print out usage if we get to here.
      printUsageAndExit();
    }
  }

  /**
   * @param args
   */
  public static void main(String [] args) {
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HRegionServer> regionServerClass =
      (Class<? extends HRegionServer>) conf.getClass(HConstants.REGION_SERVER_IMPL,
        HRegionServer.class);
    doMain(args, regionServerClass);
  }

}
