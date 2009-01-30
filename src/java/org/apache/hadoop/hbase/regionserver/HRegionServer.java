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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LeaseListener;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionHistorian;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.ValueOverMaxLengthException;
import org.apache.hadoop.hbase.HMsg.Type;
import org.apache.hadoop.hbase.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.client.ServerConnectionManager;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
 * HRegionServer makes a set of HRegions available to clients.  It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
public class HRegionServer implements HConstants, HRegionInterface, Runnable {
  static final Log LOG = LogFactory.getLog(HRegionServer.class);
  private static final HMsg REPORT_EXITING = new HMsg(Type.MSG_REPORT_EXITING);
  private static final HMsg REPORT_QUIESCED = new HMsg(Type.MSG_REPORT_QUIESCED);
  
  // Set when a report to the master comes back with a message asking us to
  // shutdown.  Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation. We use AtomicBoolean rather than
  // plain boolean so we can pass a reference to Chore threads.  Otherwise,
  // Chore threads need to know about the hosting class.
  protected final AtomicBoolean stopRequested = new AtomicBoolean(false);
  
  protected final AtomicBoolean quiesced = new AtomicBoolean(false);

  protected final AtomicBoolean safeMode = new AtomicBoolean(true);

  // Go down hard.  Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;
  
  // If false, the file system has become unavailable
  protected volatile boolean fsOk;
  
  protected final HServerInfo serverInfo;
  protected final HBaseConfiguration conf;

  private final ServerConnection connection;
  private final AtomicBoolean haveRootRegion = new AtomicBoolean(false);
  private FileSystem fs;
  private Path rootDir;
  private final Random rand = new Random();

  // Key is Bytes.hashCode of region name byte array and the value is HRegion
  // in both of the maps below.  Use Bytes.mapKey(byte []) generating key for
  // below maps.
  protected final Map<Integer, HRegion> onlineRegions =
    new ConcurrentHashMap<Integer, HRegion>();

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final List<HMsg> outboundMsgs =
    Collections.synchronizedList(new ArrayList<HMsg>());

  final int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;
  private final int serverLeaseTimeout;

  protected final int numRegionsToReport;
  
  // Remote HMaster
  private HMasterRegionInterface hbaseMaster;

  // Server to handle client requests.  Default access so can be accessed by
  // unit tests.
  final HBaseServer server;
  
  // Leases
  private final Leases leases;
  
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
  final CompactSplitThread compactSplitThread;

  // Cache flushing  
  final MemcacheFlusher cacheFlusher;
  
  /* Check for major compactions.
   */
  final Chore majorCompactionChecker;

  // HLog and HLog roller.  log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  protected volatile HLog log;
  final LogRoller logRoller;
  final LogFlusher logFlusher;
  
  // limit compactions while starting up
  CompactionLimitThread compactionLimitThread;

  // flag set after we're done setting up server threads (used for testing)
  protected volatile boolean isOnline;

  final Map<String, InternalScanner> scanners =
    new ConcurrentHashMap<String, InternalScanner>();

  private final ZooKeeperWrapper zooKeeperWrapper;

  /**
   * Starts a HRegionServer at the default location
   * @param conf
   * @throws IOException
   */
  public HRegionServer(HBaseConfiguration conf) throws IOException {
    this(new HServerAddress(conf.get(REGIONSERVER_ADDRESS,
        DEFAULT_REGIONSERVER_ADDRESS)), conf);
  }
  
  /**
   * Starts a HRegionServer at the specified location
   * @param address
   * @param conf
   * @throws IOException
   */
  public HRegionServer(HServerAddress address, HBaseConfiguration conf)
  throws IOException {  
    this.abortRequested = false;
    this.fsOk = true;
    this.conf = conf;
    this.connection = ServerConnectionManager.getConnection(conf);

    this.isOnline = false;
    
    // Config'ed params
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.threadWakeFrequency = conf.getInt(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);
    this.serverLeaseTimeout =
      conf.getInt("hbase.master.lease.period", 120 * 1000);

    // Cache flushing thread.
    this.cacheFlusher = new MemcacheFlusher(conf, this);
    
    // Compaction thread
    this.compactSplitThread = new CompactSplitThread(this);
    
    // Log rolling thread
    this.logRoller = new LogRoller(this);
    
    // Log flushing thread
    this.logFlusher =
      new LogFlusher(this.threadWakeFrequency, this.stopRequested);
    
    // Background thread to check for major compactions; needed if region
    // has not gotten updates in a while.  Make it run at a lesser frequency.
    int multiplier = this.conf.getInt(THREAD_WAKE_FREQUENCY +
        ".multiplier", 1000);
    this.majorCompactionChecker = new MajorCompactionChecker(this,
      this.threadWakeFrequency * multiplier,  this.stopRequested);

    // Task thread to process requests from Master
    this.worker = new Worker();
    this.workerThread = new Thread(worker);

    // Server to handle client requests
    this.server = HBaseRPC.getServer(this, address.getBindAddress(), 
      address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
      false, conf);
    // Address is givin a default IP for the moment. Will be changed after
    // calling the master.
    this.serverInfo = new HServerInfo(new HServerAddress(
      new InetSocketAddress(DEFAULT_HOST,
      this.server.getListenerAddress().getPort())), System.currentTimeMillis(),
      this.conf.getInt("hbase.regionserver.info.port", 60030));
    if (this.serverInfo.getServerAddress() == null) {
      throw new NullPointerException("Server address cannot be null; " +
        "hbase-958 debugging");
    }
    this.numRegionsToReport =                                        
      conf.getInt("hbase.regionserver.numregionstoreport", 10);      
      
    this.leases = new Leases(
      conf.getInt("hbase.regionserver.lease.period", 60 * 1000),
      this.threadWakeFrequency);
    
    int nbBlocks = conf.getInt("hbase.regionserver.nbreservationblocks", 4);
    for(int i = 0; i < nbBlocks; i++)  {
      reservedSpace.add(new byte[DEFAULT_SIZE_RESERVATION_BLOCK]);
    }

    this.zooKeeperWrapper = new ZooKeeperWrapper(conf);
  }

  /**
   * The HRegionServer sticks in this loop until closed. It repeatedly checks
   * in with the HMaster, sending heartbeats & reports, and receiving HRegion 
   * load/unload instructions.
   */
  public void run() {
    boolean quiesceRequested = false;
    // A sleeper that sleeps for msgInterval.
    Sleeper sleeper = new Sleeper(this.msgInterval, this.stopRequested);
    try {
      init(reportForDuty(sleeper));
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
        if (lastMsg != 0 && (now - lastMsg) >= serverLeaseTimeout) {
          // It has been way too long since we last reported to the master.
          LOG.warn("unable to report to master for " + (now - lastMsg) +
            " milliseconds - retrying");
        }
        if ((now - lastMsg) >= msgInterval) {
          HMsg outboundArray[] = null;
          synchronized(this.outboundMsgs) {
            outboundArray =
              this.outboundMsgs.toArray(new HMsg[outboundMsgs.size()]);
            this.outboundMsgs.clear();
          }
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
            HMsg msgs[] = hbaseMaster.regionServerReport(
              serverInfo, outboundArray, getMostLoadedRegions());
            lastMsg = System.currentTimeMillis();
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
              if (safeMode.get()) {
                if (!msgs[i].isInSafeMode()) {
                  this.connection.unsetRootRegionLocation();
                  synchronized (safeMode) {
                    safeMode.set(false);
                    safeMode.notifyAll();
                  }
                }
              }
              switch(msgs[i].getType()) {
              case MSG_CALL_SERVER_STARTUP:
                // We the MSG_CALL_SERVER_STARTUP on startup but we can also
                // get it when the master is panicing because for instance
                // the HDFS has been yanked out from under it.  Be wary of
                // this message.
                if (checkFileSystem()) {
                  closeAllRegions();
                  try {
                    log.closeAndDelete();
                  } catch (Exception e) {
                    LOG.error("error closing and deleting HLog", e);
                  }
                  try {
                    serverInfo.setStartCode(System.currentTimeMillis());
                    log = setupHLog();
                    this.logFlusher.setHLog(log);
                  } catch (IOException e) {
                    this.abortRequested = true;
                    this.stopRequested.set(true);
                    e = RemoteExceptionHandler.checkIOException(e); 
                    LOG.fatal("error restarting server", e);
                    break;
                  }
                  reportForDuty(sleeper);
                  restart = true;
                } else {
                  LOG.fatal("file system available check failed. " +
                  "Shutting down server.");
                }
                break;

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
          } catch (Exception e) {
            if (e instanceof IOException) {
              e = RemoteExceptionHandler.checkIOException((IOException) e);
            }
            if (tries < this.numRetries) {
              LOG.warn("Processing message (Retry: " + tries + ")", e);
              tries++;
            } else {
              LOG.error("Exceeded max retries: " + this.numRetries, e);
              checkFileSystem();
            }
            if (this.stopRequested.get()) {
                LOG.info("Stop was requested, clearing the toDo " +
                        "despite of the exception");
                toDo.clear();
                continue;
            }
          }
        }
        // Do some housekeeping before going to sleep
        housekeeping();
        sleeper.sleep(lastMsg);
      } // for
    } catch (Throwable t) {
      if (!checkOOME(t)) {
        LOG.fatal("Unhandled exception. Aborting...", t);
        abort();
      }
    }
    RegionHistorian.getInstance().offline();
    this.leases.closeAfterLeasesExpire();
    this.worker.stop();
    this.server.stop();
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive?  If OOME could have exited already
    cacheFlusher.interruptIfNecessary();
    logFlusher.interrupt();
    compactSplitThread.interruptIfNecessary();
    logRoller.interruptIfNecessary();
    this.majorCompactionChecker.interrupt();

    if (abortRequested) {
      if (this.fsOk) {
        // Only try to clean up if the file system is available
        try {
          if (this.log != null) {
            this.log.close();
            LOG.info("On abort, closed hlog");
          }
        } catch (Throwable e) {
          LOG.error("Unable to close log in abort",
            RemoteExceptionHandler.checkThrowable(e));
        }
        closeAllRegions(); // Don't leave any open file handles
      }
      LOG.info("aborting server at: " +
        serverInfo.getServerAddress().toString());
    } else {
      ArrayList<HRegion> closedRegions = closeAllRegions();
      try {
        log.closeAndDelete();
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
            serverInfo.getServerAddress().toString());
        hbaseMaster.regionServerReport(serverInfo, exitMsg, (HRegionInfo[])null);
      } catch (Throwable e) {
        LOG.warn("Failed to send exiting message to master: ",
          RemoteExceptionHandler.checkThrowable(e));
      }
      LOG.info("stopping server at: " +
        serverInfo.getServerAddress().toString());
    }
    if (this.hbaseMaster != null) {
      HBaseRPC.stopProxy(this.hbaseMaster);
      this.hbaseMaster = null;
    }
    join();

    runThread(this.hdfsShutdownThread,
      this.conf.getLong("hbase.dfs.shutdown.wait", 30000));
    LOG.info(Thread.currentThread().getName() + " exiting");
  }

  /**
   * Run and wait on passed thread in HRS context.
   * @param t
   * @param dfsShutdownWait
   */
  public void runThread(final Thread t, final long dfsShutdownWait) {
    if (t ==  null) {
      return;
    }
    t.start();
    Threads.shutdown(t, dfsShutdownWait);
  }

  /**
   * Set the hdfs shutdown thread to run on exit.  Pass null to disable
   * running of the shutdown test.  Needed by tests.
   * @param t Thread to run.  Pass null to disable tests.
   * @return Previous occupant of the shutdown thread position.
   */
  public Thread setHDFSShutdownThreadOnExit(final Thread t) {
    Thread old = this.hdfsShutdownThread;
    this.hdfsShutdownThread = t;
    return old;
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
      if(conf.get("hbase.regionserver.address") != null)
        serverInfo.setServerAddress(new HServerAddress
            (conf.get("hbase.regionserver.address"), 
            serverInfo.getServerAddress().getPort()));
      // Master sent us hbase.rootdir to use. Should be fully qualified
      // path with file system specification included.  Set 'fs.default.name'
      // to match the filesystem on hbase.rootdir else underlying hadoop hdfs
      // accessors will be going against wrong filesystem (unless all is set
      // to defaults).
      this.conf.set("fs.default.name", this.conf.get("hbase.rootdir"));
      this.fs = FileSystem.get(this.conf);

      // Register shutdown hook for HRegionServer, runs an orderly shutdown
      // when a kill signal is recieved
      Runtime.getRuntime().addShutdownHook(new ShutdownThread(this,
          Thread.currentThread()));
      this.hdfsShutdownThread = suppressHdfsShutdownHook();

      this.rootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
      this.log = setupHLog();
      this.logFlusher.setHLog(log);
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
  private HServerLoad.RegionLoad createRegionLoad(final HRegion r)
  throws IOException {
    byte[] name = r.getRegionName();
    int stores = 0;
    int storefiles = 0;
    int memcacheSizeMB = (int)(r.memcacheSize.get()/1024/1024);
    int storefileIndexSizeMB = 0;
    synchronized (r.stores) {
      stores += r.stores.size();
      for (HStore store: r.stores.values()) {
        storefiles += store.getStorefilesCount();
        storefileIndexSizeMB += 
          (int)(store.getStorefilesIndexSize()/1024/1024);
      }
    }
    return new HServerLoad.RegionLoad(name, stores, storefiles, memcacheSizeMB,
      storefileIndexSizeMB);
  }
 
  /**
   * @param regionName
   * @return An instance of RegionLoad.
   * @throws IOException
   */
  public HServerLoad.RegionLoad createRegionLoad(final byte [] regionName)
  throws IOException {
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
      LOG.error(RemoteExceptionHandler.checkThrowable(t));
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
  private boolean checkOOME(final Throwable e) {
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

  /**
   * Thread for toggling safemode after some configurable interval.
   */
  private class CompactionLimitThread extends Thread {
    @Override
    public void run() {
      // First wait until we exit safe mode
      synchronized (safeMode) {
        while(safeMode.get()) {
          LOG.debug("Waiting to exit safe mode");
          try {
            safeMode.wait();
          } catch (InterruptedException e) {
            // ignore
          }
        }
      }

      // now that safemode is off, slowly increase the per-cycle compaction
      // limit, finally setting it to unlimited (-1)

      int compactionCheckInterval = 
        conf.getInt("hbase.regionserver.thread.splitcompactcheckfrequency",
            20 * 1000);
      final int limitSteps[] = {
        1, 1, 1, 1,
        2, 2, 2, 2, 2, 2,
        3, 3, 3, 3, 3, 3, 3, 3, 
        4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
        -1
      };
      for (int i = 0; i < limitSteps.length; i++) {
        // Just log changes.
        if (compactSplitThread.getLimit() != limitSteps[i] &&
            LOG.isDebugEnabled()) {
          LOG.debug("setting compaction limit to " + limitSteps[i]);
        }
        compactSplitThread.setLimit(limitSteps[i]);
        try {
          Thread.sleep(compactionCheckInterval);
        } catch (InterruptedException ex) {
          // unlimit compactions before exiting
          compactSplitThread.setLimit(-1);
          if (LOG.isDebugEnabled()) {
            LOG.debug(this.getName() + " exiting on interrupt");
          }
          return;
        }
      }
      LOG.info("compactions no longer limited");
    }
  }

  /*
   * Thread to shutdown the region server in an orderly manner.  This thread
   * is registered as a shutdown hook in the HRegionServer constructor and is
   * only called when the HRegionServer receives a kill signal.
   */
  private static class ShutdownThread extends Thread {
    private final HRegionServer instance;
    private final Thread mainThread;
    
    /**
     * @param instance
     * @param mainThread
     */
    public ShutdownThread(HRegionServer instance, Thread mainThread) {
      this.instance = instance;
      this.mainThread = mainThread;
    }

    @Override
    public void run() {
      LOG.info("Starting shutdown thread.");
      
      // tell the region server to stop
      instance.stop();

      // Wait for main thread to exit.
      Threads.shutdown(mainThread);

      LOG.info("Shutdown thread complete");
    }    
  }

  // We need to call HDFS shutdown when we are done shutting down
  private Thread hdfsShutdownThread;

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
   * So, HDFS caches FileSystems so when you call FileSystem.get it's fast. In
   * order to make sure things are cleaned up, it also creates a shutdown hook
   * so that all filesystems can be closed when the process is terminated. This
   * conveniently runs concurrently with our own shutdown handler, and
   * therefore causes all the filesystems to be closed before the server can do
   * all its necessary cleanup.
   *
   * The crazy dirty reflection in this method sneaks into the FileSystem cache
   * and grabs the shutdown hook, removes it from the list of active shutdown
   * hooks, and hangs onto it until later. Then, after we're properly done with
   * our graceful shutdown, we can execute the hdfs hook manually to make sure
   * loose ends are tied up.
   *
   * This seems quite fragile and susceptible to breaking if Hadoop changes
   * anything about the way this cleanup is managed. Keep an eye on things.
   */
  private Thread suppressHdfsShutdownHook() {
    try {
      Field field = FileSystem.class.getDeclaredField ("clientFinalizer");
      field.setAccessible(true);
      Thread hdfsClientFinalizer = (Thread)field.get(null);
      if (hdfsClientFinalizer == null) {
        throw new RuntimeException("client finalizer is null, can't suppress!");
      }
      Runtime.getRuntime().removeShutdownHook(hdfsClientFinalizer);
      return hdfsClientFinalizer;
      
    } catch (NoSuchFieldException nsfe) {
      LOG.fatal("Couldn't find field 'clientFinalizer' in FileSystem!", nsfe);
      throw new RuntimeException("Failed to suppress HDFS shutdown hook");
    } catch (IllegalAccessException iae) {
      LOG.fatal("Couldn't access field 'clientFinalizer' in FileSystem!", iae);
      throw new RuntimeException("Failed to suppress HDFS shutdown hook");
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
    
  private HLog setupHLog() throws RegionServerRunningException,
    IOException {
    
    Path logdir = new Path(rootDir, HLog.getHLogDirectoryName(serverInfo));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Log dir " + logdir);
    }
    if (fs.exists(logdir)) {
      throw new RegionServerRunningException("region server already " +
        "running at " + this.serverInfo.getServerAddress().toString() +
        " because logdir " + logdir.toString() + " exists");
    }
    HLog newlog = new HLog(fs, logdir, conf, logRoller);
    return newlog;
  }
  
  /*
   * @param interval Interval since last time metrics were called.
   */
  protected void doMetrics() {
    this.metrics.regions.set(this.onlineRegions.size());
    this.metrics.incrementRequests(this.requestCount.get());
    // Is this too expensive every three seconds getting a lock on onlineRegions
    // and then per store carried?  Can I make metrics be sloppier and avoid
    // the synchronizations?
    int stores = 0;
    int storefiles = 0;
    long memcacheSize = 0;
    long storefileIndexSize = 0;
    synchronized (this.onlineRegions) {
      for (Map.Entry<Integer, HRegion> e: this.onlineRegions.entrySet()) {
        HRegion r = e.getValue();
        memcacheSize += r.memcacheSize.get();
        synchronized (r.stores) {
          stores += r.stores.size();
          for(Map.Entry<Integer, HStore> ee: r.stores.entrySet()) {
            HStore store = ee.getValue(); 
            storefiles += store.getStorefilesCount();
            try {
              storefileIndexSize += store.getStorefilesIndexSize();
            } catch (IOException ex) {
              LOG.warn("error getting store file index size for " + store +
                ": " + StringUtils.stringifyException(ex));  
            }
          }
        }
      }
    }
    this.metrics.stores.set(stores);
    this.metrics.storefiles.set(storefiles);
    this.metrics.memcacheSizeMB.set((int)(memcacheSize/(1024*1024)));
    this.metrics.storefileIndexSizeMB.set((int)(storefileIndexSize/(1024*1024)));
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
    Threads.setDaemonThreadRunning(this.logRoller, n + ".logRoller",
        handler);
    Threads.setDaemonThreadRunning(this.logFlusher, n + ".logFlusher",
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
    if (port >= 0) {
      String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
      this.infoServer = new InfoServer("regionserver", a, port, false);
      this.infoServer.setAttribute("regionserver", this);
      this.infoServer.start();
    }

    // Set up the safe mode handler if safe mode has been configured.
    if (!conf.getBoolean("hbase.regionserver.safemode", true)) {
      safeMode.set(false);
      compactSplitThread.setLimit(-1);
      LOG.debug("skipping safe mode");
    } else {
      this.compactionLimitThread = new CompactionLimitThread();
      Threads.setDaemonThreadRunning(this.compactionLimitThread, n + ".safeMode",
        handler);
    }

    // Start Server.  This service is like leases in that it internally runs
    // a thread.
    this.server.start();
    LOG.info("HRegionServer started at: " +
        serverInfo.getServerAddress().toString());
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
        cacheFlusher.isAlive() && logRoller.isAlive() &&
        workerThread.isAlive() && this.majorCompactionChecker.isAlive())) {
      // One or more threads are no longer alive - shut down
      stop();
      return false;
    }
    return true;
  }
  /*
   * Run some housekeeping tasks before we go into 'hibernation' sleeping at
   * the end of the main HRegionServer run loop.
   */
  private void housekeeping() {
    // If the todo list has > 0 messages, iterate looking for open region
    // messages. Send the master a message that we're working on its
    // processing so it doesn't assign the region elsewhere.
    if (this.toDo.size() <= 0) {
      return;
    }
    // This iterator is 'safe'.  We are guaranteed a view on state of the
    // queue at time iterator was taken out.  Apparently goes from oldest.
    for (ToDoEntry e: this.toDo) {
      if (e.msg.isType(HMsg.Type.MSG_REGION_OPEN)) {
        addProcessingMessage(e.msg.getRegionInfo());
      }
    }
  }

  /** @return the HLog */
  HLog getLog() {
    return this.log;
  }

  /**
   * Sets a flag that will cause all the HRegionServer threads to shut down
   * in an orderly fashion.  Used by unit tests.
   */
  public void stop() {
    this.stopRequested.set(true);
    synchronized(this) {
      notifyAll(); // Wakes run() if it is sleeping
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

  /** 
   * Wait on all threads to finish.
   * Presumption is that all closes and stops have already been called.
   */
  void join() {
    Threads.shutdown(this.majorCompactionChecker);
    Threads.shutdown(this.workerThread);
    Threads.shutdown(this.cacheFlusher);
    Threads.shutdown(this.compactSplitThread);
    Threads.shutdown(this.logRoller);
  }
  
  /*
   * Let the master know we're here
   * Run initialization using parameters passed us by the master.
   */
  private MapWritable reportForDuty(final Sleeper sleeper) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Telling master at " +
        conf.get(MASTER_ADDRESS) + " that we are up");
    }
    HMasterRegionInterface master = null;
    while (!stopRequested.get() && master == null) {
      try {
        // Do initial RPC setup.  The final argument indicates that the RPC
        // should retry indefinitely.
        master = (HMasterRegionInterface)HBaseRPC.waitForProxy(
            HMasterRegionInterface.class, HBaseRPCProtocolVersion.versionID,
            new HServerAddress(conf.get(MASTER_ADDRESS)).getInetSocketAddress(),
            this.conf, -1);
      } catch (IOException e) {
        LOG.warn("Unable to connect to master. Retrying. Error was:", e);
        sleeper.sleep();
      }
    }
    this.hbaseMaster = master;
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
        result = this.hbaseMaster.regionServerStartup(serverInfo);
        break;
      } catch (Leases.LeaseStillHeldException e) {
        LOG.info("Lease " + e.getName() + " already held on master. Check " +
          "DNS configuration so that all region servers are" +
          "reporting their true IPs and not 127.0.0.1. Otherwise, this" +
          "problem should resolve itself after the lease period of " +
          this.conf.get("hbase.master.lease.period")
          + " seconds expires over on the master");
      } catch (IOException e) {
        LOG.warn("error telling master we are up", e);
      }
      sleeper.sleep(lastMsg);
    }
    return result;
  }

  /* Add to the outbound message buffer */
  private void reportOpen(HRegionInfo region) {
    outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_OPEN, region));
  }

  /* Add to the outbound message buffer */
  private void reportClose(HRegionInfo region) {
    reportClose(region, null);
  }

  /* Add to the outbound message buffer */
  private void reportClose(final HRegionInfo region, final byte[] message) {
    outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_CLOSE, region, message));
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
    outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_SPLIT, oldRegion,
      (oldRegion.getRegionNameAsString() + " split; daughters: " +
        newRegionA.getRegionNameAsString() + ", " +
        newRegionB.getRegionNameAsString()).getBytes()));
    outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_OPEN, newRegionA));
    outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_OPEN, newRegionB));
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMaster-given operations
  //////////////////////////////////////////////////////////////////////////////

  /*
   * Data structure to hold a HMsg and retries count.
   */
  private static class ToDoEntry {
    private int tries;
    private final HMsg msg;
    ToDoEntry(HMsg msg) {
      this.tries = 0;
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
              region.regionInfo.shouldSplit(true);
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
              
            default:
              throw new AssertionError(
                  "Impossible state during msg processing.  Instruction: "
                  + e.msg.toString());
            }
          } catch (InterruptedException ex) {
            // continue
          } catch (Exception ex) {
            if (ex instanceof IOException) {
              ex = RemoteExceptionHandler.checkIOException((IOException) ex);
            }
            if(e != null && e.tries < numRetries) {
              LOG.warn(ex);
              e.tries++;
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
    // If historian is not online and this is not a meta region, online it.
    if (!regionInfo.isMetaRegion() &&
        !RegionHistorian.getInstance().isOnline()) {
      RegionHistorian.getInstance().online(this.conf);
    }
    Integer mapKey = Bytes.mapKey(regionInfo.getRegionName());
    HRegion region = this.onlineRegions.get(mapKey);
    if (region == null) {
      try {
        region = instantiateRegion(regionInfo);
        // Startup a compaction early if one is needed.
        this.compactSplitThread.
          compactionRequested(region, "Region open check");
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
        this.log.setSequenceNumber(region.getMinSequenceId());
        this.onlineRegions.put(mapKey, region);
      } finally {
        this.lock.writeLock().unlock();
      }
    }
    reportOpen(regionInfo); 
  }
  
  protected HRegion instantiateRegion(final HRegionInfo regionInfo)
      throws IOException {
    HRegion r = new HRegion(HTableDescriptor.getTableDir(rootDir, regionInfo
        .getTableDesc().getName()), this.log, this.fs, conf, regionInfo,
        this.cacheFlusher);
    r.initialize(null,  new Progressable() {
      public void progress() {
        addProcessingMessage(regionInfo);
      }
    });
    return r; 
  }
  
  /*
   * Add a MSG_REPORT_PROCESS_OPEN to the outbound queue.
   * This method is called while region is in the queue of regions to process
   * and then while the region is being opened, it is called from the Worker
   * thread that is running the region open.
   * @param hri Region to add the message for
   */
  protected void addProcessingMessage(final HRegionInfo hri) {
    getOutboundMsgs().add(new HMsg(HMsg.Type.MSG_REPORT_PROCESS_OPEN, hri));
  }

  void closeRegion(final HRegionInfo hri, final boolean reportWhenCompleted)
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
    for(HRegion region: regionsToClose) {
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

  public Cell[] get(final byte [] regionName, final byte [] row,
    final byte [] column, final long timestamp, final int numVersions) 
  throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      return getRegion(regionName).get(row, column, timestamp, numVersions);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public RowResult getRow(final byte [] regionName, final byte [] row, 
    final byte [][] columns, final long ts,
    final int numVersions, final long lockId)
  throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      // convert the columns array into a set so it's easy to check later.
      Set<byte []> columnSet = null;
      if (columns != null) {
        columnSet = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
        columnSet.addAll(Arrays.asList(columns));
      }
      
      HRegion region = getRegion(regionName);
      HbaseMapWritable<byte [], Cell> result =
        region.getFull(row, columnSet, 
          ts, numVersions, getLockFromId(lockId));
      if (result == null || result.isEmpty())
        return null;
      return new RowResult(row, result);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public RowResult getClosestRowBefore(final byte [] regionName, 
    final byte [] row, final byte [] columnFamily)
  throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      // locate the region we're operating on
      HRegion region = getRegion(regionName);
      // ask the region for all the data 
      RowResult rr = region.getClosestRowBefore(row, columnFamily);
      return rr;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }
  
  public RowResult next(final long scannerId) throws IOException {
    RowResult[] rrs = next(scannerId, 1);
    return rrs.length == 0 ? null : rrs[0];
  }

  public RowResult[] next(final long scannerId, int nbRows) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    ArrayList<RowResult> resultSets = new ArrayList<RowResult>();
    try {
      String scannerName = String.valueOf(scannerId);
      InternalScanner s = scanners.get(scannerName);
      if (s == null) {
        throw new UnknownScannerException("Name: " + scannerName);
      }
      this.leases.renewLease(scannerName);
      for(int i = 0; i < nbRows; i++) {
        // Collect values to be returned here
        HbaseMapWritable<byte [], Cell> values
          = new HbaseMapWritable<byte [], Cell>();
        HStoreKey key = new HStoreKey();
        while (s.next(key, values)) {
          if (values.size() > 0) {
            // Row has something in it. Return the value.
            resultSets.add(new RowResult(key.getRow(), values));
            break;
          }
        }
      }
      return resultSets.toArray(new RowResult[resultSets.size()]);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public void batchUpdate(final byte [] regionName, BatchUpdate b,
      long lockId) throws IOException {
    if (b.getRow() == null)
      throw new IllegalArgumentException("update has null row");
    
    checkOpen();
    this.requestCount.incrementAndGet();
    HRegion region = getRegion(regionName);
    validateValuesLength(b, region);
    try {
      cacheFlusher.reclaimMemcacheMemory();
      region.batchUpdate(b, getLockFromId(b.getRowLock()));
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }
  
  public int batchUpdates(final byte[] regionName, final BatchUpdate[] b)
  throws IOException {
    int i = 0;
    checkOpen();
    try {
      HRegion region = getRegion(regionName);
      this.cacheFlusher.reclaimMemcacheMemory();
      Integer[] locks = new Integer[b.length];
      for (i = 0; i < b.length; i++) {
        this.requestCount.incrementAndGet();
        validateValuesLength(b[i], region);
        locks[i] = getLockFromId(b[i].getRowLock());
        region.batchUpdate(b[i], locks[i]);
      }
    } catch(WrongRegionException ex) {
      return i;
    } catch (NotServingRegionException ex) {
      return i;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
    return -1;
  }
  
  public boolean checkAndSave(final byte [] regionName, final BatchUpdate b,
      final HbaseMapWritable<byte[],byte[]> expectedValues)
  throws IOException {
    if (b.getRow() == null)
      throw new IllegalArgumentException("update has null row");
    checkOpen();
    this.requestCount.incrementAndGet();
    HRegion region = getRegion(regionName);
    validateValuesLength(b, region);
    try {
      cacheFlusher.reclaimMemcacheMemory();
      boolean result = region.checkAndSave(b,
        expectedValues,getLockFromId(b.getRowLock()), false);
      return result;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }
  
  
  /**
   * Utility method to verify values length
   * @param batchUpdate The update to verify
   * @throws IOException Thrown if a value is too long
   */
  private void validateValuesLength(BatchUpdate batchUpdate, 
      HRegion region) throws IOException {
    HTableDescriptor desc = region.getTableDesc();
    for (Iterator<BatchOperation> iter = 
      batchUpdate.iterator(); iter.hasNext();) {
      BatchOperation operation = iter.next();
      if (operation.getValue() != null) {
        HColumnDescriptor fam = 
          desc.getFamily(HStoreKey.getFamily(operation.getColumn()));
        if (fam != null) {
          int maxLength = fam.getMaxValueLength();
          if (operation.getValue().length > maxLength) {
            throw new ValueOverMaxLengthException("Value in column "
                + Bytes.toString(operation.getColumn()) + " is too long. "
                + operation.getValue().length + " instead of " + maxLength);
          }
        }
      }
    }
  }
  
  //
  // remote scanner interface
  //

  public long openScanner(byte [] regionName, byte [][] cols, byte [] firstRow,
    final long timestamp, final RowFilterInterface filter)
  throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (cols == null) {
      npe = new NullPointerException("columns to scan is null");
    } else if (firstRow == null) {
      npe = new NullPointerException("firstRow for scanner is null");
    }
    if (npe != null) {
      throw new IOException("Invalid arguments to openScanner", npe);
    }
    requestCount.incrementAndGet();
    try {
      HRegion r = getRegion(regionName);
      InternalScanner s =
        r.getScanner(cols, firstRow, timestamp, filter);
      long scannerId = addScanner(s);
      return scannerId;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t, "Failed openScanner"));
    }
  }
  
  protected long addScanner(InternalScanner s) throws LeaseStillHeldException {
    long scannerId = -1L;
    scannerId = rand.nextLong();
    String scannerName = String.valueOf(scannerId);
    synchronized(scanners) {
      scanners.put(scannerName, s);
    }
    this.leases.
      createLease(scannerName, new ScannerListener(scannerName));
    return scannerId;
  }
  
  public void close(final long scannerId) throws IOException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      String scannerName = String.valueOf(scannerId);
      InternalScanner s = null;
      synchronized(scanners) {
        s = scanners.remove(scannerName);
      }
      if(s == null) {
        throw new UnknownScannerException(scannerName);
      }
      s.close();
      this.leases.cancelLease(scannerName);
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
      InternalScanner s = null;
      synchronized(scanners) {
        s = scanners.remove(this.scannerName);
      }
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
  
  public void deleteAll(final byte [] regionName, final byte [] row,
      final byte [] column, final long timestamp, final long lockId) 
  throws IOException {
    HRegion region = getRegion(regionName);
    region.deleteAll(row, column, timestamp, getLockFromId(lockId));
  }

  public void deleteAll(final byte [] regionName, final byte [] row,
      final long timestamp, final long lockId) 
  throws IOException {
    HRegion region = getRegion(regionName);
    region.deleteAll(row, timestamp, getLockFromId(lockId));
  }

  public void deleteAllByRegex(byte[] regionName, byte[] row, String colRegex,
      long timestamp, long lockId) throws IOException {
    getRegion(regionName).deleteAllByRegex(row, colRegex, timestamp, 
        getLockFromId(lockId));
  }

  public void deleteFamily(byte [] regionName, byte [] row, byte [] family, 
    long timestamp, final long lockId)
  throws IOException{
    getRegion(regionName).deleteFamily(row, family, timestamp,
        getLockFromId(lockId));
  }

  public void deleteFamilyByRegex(byte[] regionName, byte[] row, String familyRegex,
      long timestamp, long lockId) throws IOException {
    getRegion(regionName).deleteFamilyByRegex(row, familyRegex, timestamp, 
        getLockFromId(lockId));
  }

  public boolean exists(byte[] regionName, byte[] row, byte[] column,
      long timestamp, long lockId)
  throws IOException {
    return getRegion(regionName).exists(row, column, timestamp, 
      getLockFromId(lockId));
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
    synchronized(rowlocks) {
      rowlocks.put(lockName, r);
    }
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
  private Integer getLockFromId(long lockId)
  throws IOException {
    if(lockId == -1L) {
      return null;
    }
    String lockName = String.valueOf(lockId);
    Integer rl = null;
    synchronized(rowlocks) {
      rl = rowlocks.get(lockName);
    }
    if(rl == null) {
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
      Integer r = null;
      synchronized(rowlocks) {
        r = rowlocks.remove(lockName);
      }
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
      Integer r = null;
      synchronized(rowlocks) {
        r = rowlocks.remove(this.lockName);
      }
      if(r != null) {
        region.releaseRowLock(r);
      }
    }
  }

  /**
   * @return Info on this server.
   */
  public HServerInfo getServerInfo() {
    return this.serverInfo;
  }

  /** @return the info server */
  public InfoServer getInfoServer() {
    return infoServer;
  }
  
  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopRequested() {
    return stopRequested.get();
  }

  /**
   * @return true if the region server is in safe mode
   */
  public boolean isInSafeMode() {
    return safeMode.get();
  }

  /**
   * 
   * @return the configuration
   */
  public HBaseConfiguration getConfiguration() {
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
        sortedRegions.put(Long.valueOf(region.memcacheSize.get()), region);
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
      Integer key = Integer.valueOf(Bytes.hashCode(regionName));
      region = onlineRegions.get(key);
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
  protected List<HMsg> getOutboundMsgs() {
    return this.outboundMsgs;
  }

  /**
   * Return the total size of all memcaches in every region.
   * @return memcache size in bytes
   */
  public long getGlobalMemcacheSize() {
    long total = 0;
    synchronized (onlineRegions) {
      for (HRegion region : onlineRegions.values()) {
        total += region.memcacheSize.get();
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

  //
  // Main program and support routines
  //

  private static void printUsageAndExit() {
    printUsageAndExit(null);
  }
  
  private static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.err.println("Usage: java " +
        "org.apache.hbase.HRegionServer [--bind=hostname:port] start");
    System.exit(0);
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
    Configuration conf = new HBaseConfiguration();
    
    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).
    final String addressArgKey = "--bind=";
    for (String cmd: args) {
      if (cmd.startsWith(addressArgKey)) {
        conf.set(REGIONSERVER_ADDRESS, cmd.substring(addressArgKey.length()));
        continue;
      }
      
      if (cmd.equals("start")) {
        try {
          // If 'local', don't start a region server here.  Defer to
          // LocalHBaseCluster.  It manages 'local' clusters.
          if (LocalHBaseCluster.isLocal(conf)) {
            LOG.warn("Not starting a distinct region server because " +
              "hbase.master is set to 'local' mode");
          } else {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            if (runtime != null) {
              LOG.info("vmInputArguments=" + runtime.getInputArguments());
            }
            Constructor<? extends HRegionServer> c =
              regionServerClass.getConstructor(HBaseConfiguration.class);
            HRegionServer hrs = c.newInstance(conf);
            Thread t = new Thread(hrs);
            t.setName("regionserver" + hrs.server.getListenerAddress());
            t.start();
          }
        } catch (Throwable t) {
          LOG.error( "Can not start region server because "+
              StringUtils.stringifyException(t) );
          System.exit(-1);
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
    Configuration conf = new HBaseConfiguration();
    @SuppressWarnings("unchecked")
    Class<? extends HRegionServer> regionServerClass = (Class<? extends HRegionServer>) conf
        .getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);
    doMain(args, regionServerClass);
  }
}
