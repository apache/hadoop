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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
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
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;

/**
 * HRegionServer makes a set of HRegions available to clients.  It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
public class HRegionServer implements HConstants, HRegionInterface, Runnable {
  static final Log LOG = LogFactory.getLog(HRegionServer.class);
  
  public long getProtocolVersion(final String protocol, 
      @SuppressWarnings("unused") final long clientVersion)
  throws IOException {  
    if (protocol.equals(HRegionInterface.class.getName())) {
      return HRegionInterface.versionID;
    }
    throw new IOException("Unknown protocol to name node: " + protocol);
  }
  
  // Set when a report to the master comes back with a message asking us to
  // shutdown.  Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation. We use AtomicBoolean rather than
  // plain boolean so we can pass a reference to Chore threads.  Otherwise,
  // Chore threads need to know about the hosting class.
  protected AtomicBoolean stopRequested = new AtomicBoolean(false);
  
  // Go down hard.  Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;
  
  // If false, the file system has become unavailable
  protected volatile boolean fsOk;
  
  final Path rootDir;
  protected final HServerInfo serverInfo;
  protected final Configuration conf;
  private final Random rand;
  
  // region name -> HRegion
  protected final SortedMap<Text, HRegion> onlineRegions;
  protected final Map<Text, HRegion> retiringRegions =
    new HashMap<Text, HRegion>();
 
  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Vector<HMsg> outboundMsgs;

  int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;
  
  // File paths
  private FileSystem fs;

  // Remote HMaster
  private HMasterRegionInterface hbaseMaster;

  // Server to handle client requests.  Default access so can be accessed by
  // unit tests.
  Server server;
  
  // Leases
  private Leases leases;
  
  // Request counter
  private AtomicInteger requestCount;
  
  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  // Check to see if regions should be split
  private final Thread splitOrCompactCheckerThread;
  // Needed at shutdown. On way out, if can get this lock then we are not in
  // middle of a split or compaction: i.e. splits/compactions cannot be
  // interrupted.
  protected final Integer splitOrCompactLock = new Integer(0);
  
  /**
   * Runs periodically to determine if regions need to be compacted or split
   */
  class SplitOrCompactChecker extends Chore
  implements RegionUnavailableListener {
    private HTable root = null;
    private HTable meta = null;

    public SplitOrCompactChecker(final AtomicBoolean stop) {
      super(conf.getInt("hbase.regionserver.thread.splitcompactcheckfrequency",
        30 * 1000), stop);
    }

    public void closing(final Text regionName) {
      lock.writeLock().lock();
      try {
        // Remove region from regions Map and add it to the Map of retiring
        // regions.
        retiringRegions.put(regionName, onlineRegions.remove(regionName));
        if (LOG.isDebugEnabled()) {
          LOG.debug(regionName.toString() + " closing (" +
            "Adding to retiringRegions)");
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
    
    public void closed(final Text regionName) {
      lock.writeLock().lock();
      try {
        retiringRegions.remove(regionName);
        if (LOG.isDebugEnabled()) {
          LOG.debug(regionName.toString() + " closed");
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
    
    /**
     * Scan for splits or compactions to run.  Run any we find.
     */
    protected void chore() {
      // Don't interrupt us while we're working
      synchronized (splitOrCompactLock) {
        checkForSplitsOrCompactions();
      }
    }
    
    private void checkForSplitsOrCompactions() {
      // Grab a list of regions to check
      List<HRegion> nonClosedRegionsToCheck = getRegionsToCheck();
      for(HRegion cur: nonClosedRegionsToCheck) {
        try {
          if (cur.needsCompaction()) {
            cur.compactStores();
          }
          // After compaction, it probably needs splitting.  May also need
          // splitting just because one of the memcache flushes was big.
          Text midKey = new Text();
          if (cur.needsSplit(midKey)) {
            split(cur, midKey);
          }
        } catch(IOException e) {
          //TODO: What happens if this fails? Are we toast?
          LOG.error("Split or compaction failed", e);
          if (!checkFileSystem()) {
            break;
          }
        }
      }
    }
    
    private void split(final HRegion region, final Text midKey)
    throws IOException {
      final HRegionInfo oldRegionInfo = region.getRegionInfo();
      final HRegion[] newRegions = region.closeAndSplit(midKey, this);
      
      // When a region is split, the META table needs to updated if we're
      // splitting a 'normal' region, and the ROOT table needs to be
      // updated if we are splitting a META region.
      HTable t = null;
      if (region.getRegionInfo().tableDesc.getName().equals(META_TABLE_NAME)) {
        // We need to update the root region
        if (this.root == null) {
          this.root = new HTable(conf, ROOT_TABLE_NAME);
        }
        t = root;
      } else {
        // For normal regions we need to update the meta region
        if (meta == null) {
          meta = new HTable(conf, META_TABLE_NAME);
        }
        t = meta;
      }
      LOG.info("Updating " + t.getTableName() + " with region split info");

      // Mark old region as offline and split in META.
      // NOTE: there is no need for retry logic here. HTable does it for us.
      long lockid = t.startUpdate(oldRegionInfo.getRegionName());
      oldRegionInfo.offLine = true;
      oldRegionInfo.split = true;
      t.put(lockid, COL_REGIONINFO, Writables.getBytes(oldRegionInfo));
      t.put(lockid, COL_SPLITA, Writables.getBytes(
        newRegions[0].getRegionInfo()));
      t.put(lockid, COL_SPLITB, Writables.getBytes(
        newRegions[1].getRegionInfo()));
      t.commit(lockid);
      
      // Add new regions to META
      for (int i = 0; i < newRegions.length; i++) {
        lockid = t.startUpdate(newRegions[i].getRegionName());
        t.put(lockid, COL_REGIONINFO, Writables.getBytes(
          newRegions[i].getRegionInfo()));
        t.commit(lockid);
      }
          
      // Now tell the master about the new regions
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reporting region split to master");
      }
      reportSplit(oldRegionInfo, newRegions[0].getRegionInfo(),
        newRegions[1].getRegionInfo());
      LOG.info("region split, META update, and report to master all" +
        " successful. Old region=" + oldRegionInfo.getRegionName() +
        ", new regions: " + newRegions[0].getRegionName() + ", " +
        newRegions[1].getRegionName());
      
      // Do not serve the new regions. Let the Master assign them.
    }
  }

  // Cache flushing  
  private final Thread cacheFlusherThread;
  // Needed during shutdown so we send an interrupt after completion of a
  // flush, not in the midst.
  protected final Integer cacheFlusherLock = new Integer(0);
  
  /* Runs periodically to flush memcache.
   */
  class Flusher extends Chore {
    public Flusher(final int period, final AtomicBoolean stop) {
      super(period, stop);
    }
    
    protected void chore() {
      synchronized(cacheFlusherLock) {
        checkForFlushesToRun();
      }
    }
    
    private void checkForFlushesToRun() {
      // Grab a list of items to flush
      List<HRegion> nonClosedRegionsToFlush = getRegionsToCheck();
      // Flush them, if necessary
      for(HRegion cur: nonClosedRegionsToFlush) {
        try {
          cur.optionallyFlush();
        } catch (IOException iex) {
          LOG.error("Cache flush failed",
            RemoteExceptionHandler.checkIOException(iex));
          if (!checkFileSystem()) {
            break;
          }
        }
      }
    }
  }
  
  // HLog and HLog roller.
  protected final HLog log;
  private final Thread logRollerThread;
  protected final Integer logRollerLock = new Integer(0);
  
  /** Runs periodically to determine if the HLog should be rolled */
  class LogRoller extends Chore {
    private int MAXLOGENTRIES =
      conf.getInt("hbase.regionserver.maxlogentries", 30 * 1000);
    
    public LogRoller(final int period, final AtomicBoolean stop) {
      super(period, stop);
    }
 
    protected void chore() {
      synchronized(logRollerLock) {
        checkForLogRoll();
      }
    }

    private void checkForLogRoll() {
      // If the number of log entries is high enough, roll the log.  This
      // is a very fast operation, but should not be done too frequently.
      int nEntries = log.getNumEntries();
      if(nEntries > this.MAXLOGENTRIES) {
        try {
          LOG.info("Rolling hlog. Number of entries: " + nEntries);
          log.rollWriter();
        } catch (IOException iex) {
          LOG.error("Log rolling failed",
            RemoteExceptionHandler.checkIOException(iex));
          checkFileSystem();
        }
      }
    }
  }

  /**
   * Starts a HRegionServer at the default location
   * @param conf
   * @throws IOException
   */
  public HRegionServer(Configuration conf) throws IOException {
    this(new Path(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR)),
        new HServerAddress(conf.get(REGIONSERVER_ADDRESS,
          DEFAULT_REGIONSERVER_ADDRESS)),
        conf);
  }
  
  /**
   * Starts a HRegionServer at the specified location
   * @param rootDir
   * @param address
   * @param conf
   * @throws IOException
   */
  public HRegionServer(Path rootDir, HServerAddress address,
      Configuration conf)
  throws IOException {  
    this.abortRequested = false;
    this.fsOk = true;
    this.rootDir = rootDir;
    this.conf = conf;
    this.rand = new Random();
    this.onlineRegions =
      Collections.synchronizedSortedMap(new TreeMap<Text, HRegion>());
    
    this.outboundMsgs = new Vector<HMsg>();
    this.requestCount = new AtomicInteger();

    // Config'ed params
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.threadWakeFrequency = conf.getInt(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);

    // Cache flushing chore thread.
    this.cacheFlusherThread =
      new Flusher(this.threadWakeFrequency, stopRequested);
    
    // Check regions to see if they need to be split or compacted chore thread
    this.splitOrCompactCheckerThread =
      new SplitOrCompactChecker(this.stopRequested);
    
    // Task thread to process requests from Master
    this.toDo = new LinkedBlockingQueue<ToDoEntry>();
    this.worker = new Worker();
    this.workerThread = new Thread(worker);
    this.sleeper = new Sleeper(this.msgInterval, this.stopRequested);

    try {
      // Server to handle client requests
      this.server = RPC.getServer(this, address.getBindAddress(), 
        address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);

      // Use interface to get the 'real' IP for this host.
      // 'serverInfo' is sent to master.  Should have the real IP of this host
      // rather than 'localhost' or 0.0.0.0 or 127.0.0.1 in it.
      String realIP = DNS.getDefaultIP(
        conf.get("dfs.datanode.dns.interface","default"));
      this.serverInfo = new HServerInfo(new HServerAddress(
        new InetSocketAddress(realIP, server.getListenerAddress().getPort())),
        this.rand.nextLong());
      Path logdir = new Path(rootDir, "log" + "_" + realIP + "_" +
        this.serverInfo.getServerAddress().getPort());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Log dir " + logdir);
      }
      
      // Logging
      this.fs = FileSystem.get(conf);
      if(fs.exists(logdir)) {
        throw new RegionServerRunningException("region server already " +
          "running at " + this.serverInfo.getServerAddress().toString() +
          " because logdir " + logdir.toString() + " exists");
      }
      
      this.log = new HLog(fs, logdir, conf);
      this.logRollerThread =
        new LogRoller(this.threadWakeFrequency, stopRequested);

      // Remote HMaster
      this.hbaseMaster = (HMasterRegionInterface)RPC.waitForProxy(
          HMasterRegionInterface.class, HMasterRegionInterface.versionID,
          new HServerAddress(conf.get(MASTER_ADDRESS)).getInetSocketAddress(),
          conf);
    } catch (IOException e) {
      this.stopRequested.set(true);
      throw RemoteExceptionHandler.checkIOException(e);
    }
  }

  /** @return the HLog */
  HLog getLog() {
    return log;
  }

  /**
   * Sets a flag that will cause all the HRegionServer threads to shut down
   * in an orderly fashion.
   * <p>FOR DEBUGGING ONLY
   */
  synchronized void stop() {
    stopRequested.set(true);
    notifyAll();                        // Wakes run() if it is sleeping
  }
  
  /**
   * Cause the server to exit without closing the regions it is serving, the
   * log it is using and without notifying the master.
   * Used unit testing and on catastrophic events such as HDFS is yanked out
   * from under hbase or we OOME.
   */
  synchronized void abort() {
    abortRequested = true;
    stop();
  }

  /** 
   * Wait on all threads to finish.
   * Presumption is that all closes and stops have already been called.
   */
  void join() {
    try {
      this.workerThread.join();
    } catch(InterruptedException iex) {
      // continue
    }
    try {
      this.logRollerThread.join();
    } catch(InterruptedException iex) {
      // continue
    }
    try {
      this.cacheFlusherThread.join();
    } catch(InterruptedException iex) {
      // continue
    }
    try {
      this.splitOrCompactCheckerThread.join();
    } catch(InterruptedException iex) {
      // continue
    }
    try {
      this.server.join();
    } catch(InterruptedException iex) {
      // continue
    }
    LOG.info("HRegionServer stopped at: " +
      serverInfo.getServerAddress().toString());
  }

  /**
   * The HRegionServer sticks in this loop until closed. It repeatedly checks
   * in with the HMaster, sending heartbeats & reports, and receiving HRegion 
   * load/unload instructions.
   */
  public void run() {
    startAllServices();
    
    // Set below if HMaster asked us stop.
    boolean masterRequestedStop = false;
    
    try {
      while(!stopRequested.get()) {
        long lastMsg = 0;
        try {
          reportForDuty();
        } catch(IOException e) {
          this.sleeper.sleep(lastMsg);
          continue;
        }

        // Now ask master what it wants us to do and tell it what we have done
        for (int tries = 0; !stopRequested.get();) {
          if ((System.currentTimeMillis() - lastMsg) >= msgInterval) {
            HMsg outboundArray[] = null;
            synchronized(outboundMsgs) {
              outboundArray =
                this.outboundMsgs.toArray(new HMsg[outboundMsgs.size()]);
              this.outboundMsgs.clear();
            }

            try {
              this.serverInfo.setLoad(new HServerLoad(requestCount.get(),
                  onlineRegions.size()));
              this.requestCount.set(0);
              HMsg msgs[] =
                this.hbaseMaster.regionServerReport(serverInfo, outboundArray);
              lastMsg = System.currentTimeMillis();
              // Queue up the HMaster's instruction stream for processing
              boolean restart = false;
              for(int i = 0; i < msgs.length && !stopRequested.get() &&
                  !restart; i++) {
                switch(msgs[i].getMsg()) {
                
                case HMsg.MSG_CALL_SERVER_STARTUP:
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Got call server startup message");
                  }
                  // We the MSG_CALL_SERVER_STARTUP on startup but we can also
                  // get it when the master is panicing because for instance
                  // the HDFS has been yanked out from under it.  Be wary of
                  // this message.
                  if (checkFileSystem()) {
                    closeAllRegions();
                    restart = true;
                  }
                  
                  break;

                case HMsg.MSG_REGIONSERVER_STOP:
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Got regionserver stop message");
                  }
                  masterRequestedStop = true;
                  stopRequested.set(true);
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
              if (restart || this.stopRequested.get()) {
                toDo.clear();
                break;
              }
              // Reset tries count if we had a successful transaction.
              tries = 0;
            } catch (IOException e) {
              e = RemoteExceptionHandler.checkIOException(e);
              if(tries < this.numRetries) {
                LOG.warn("", e);
                tries++;
              } else {
                LOG.error("Exceeded max retries: " + this.numRetries, e);
                if (!checkFileSystem()) {
                  continue;
                }
                // Something seriously wrong. Shutdown.
                stop();
              }
            }
          } // while (!stopRequested.get())
          this.sleeper.sleep(lastMsg);
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Aborting...", t);
      abort();
    }
    leases.closeAfterLeasesExpire();
    this.worker.stop();
    this.server.stop();
    
    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive?  If OOME could have exited already
    synchronized(logRollerLock) {
      this.logRollerThread.interrupt();
    }
    synchronized(cacheFlusherLock) {
      this.cacheFlusherThread.interrupt();
    }
    synchronized(splitOrCompactLock) {
      this.splitOrCompactCheckerThread.interrupt();
    }

    if (abortRequested) {
      if (this.fsOk) {
        // Only try to clean up if the file system is available
        try {
          log.close();
          LOG.info("On abort, closed hlog");
        } catch (IOException e) {
          LOG.error("Unable to close log in abort",
              RemoteExceptionHandler.checkIOException(e));
        }
        closeAllRegions(); // Don't leave any open file handles
      }
      LOG.info("aborting server at: " +
        serverInfo.getServerAddress().toString());
    } else {
      ArrayList<HRegion> closedRegions = closeAllRegions();
      try {
        log.closeAndDelete();
      } catch (IOException e) {
        LOG.error("", RemoteExceptionHandler.checkIOException(e));
      }
      try {
        if (!masterRequestedStop && closedRegions != null) {
          HMsg[] exitMsg = new HMsg[closedRegions.size() + 1];
          exitMsg[0] = new HMsg(HMsg.MSG_REPORT_EXITING);
          // Tell the master what regions we are/were serving
          int i = 1;
          for(HRegion region: closedRegions) {
            exitMsg[i++] = new HMsg(HMsg.MSG_REPORT_CLOSE,
                region.getRegionInfo());
          }

          LOG.info("telling master that region server is shutting down at: " +
              serverInfo.getServerAddress().toString());
          hbaseMaster.regionServerReport(serverInfo, exitMsg);
        }
      } catch (IOException e) {
        LOG.warn("", RemoteExceptionHandler.checkIOException(e));
      }
      LOG.info("stopping server at: " +
        serverInfo.getServerAddress().toString());
    }

    join(); 
    LOG.info("main thread exiting");
  }

  /*
   * Start Chore Threads, Server, Worker and lease checker threads. Install an
   * UncaughtExceptionHandler that calls abort of RegionServer if we get
   * an unhandled exception.  We cannot set the handler on all threads.
   * Server's internal Listener thread is off limits.  For Server, if an OOME,
   * it waits a while then retries.  Meantime, a flush or a compaction that
   * tries to run should trigger same critical condition and the shutdown will
   * run.  On its way out, this server will shut down Server.  Leases are sort
   * of inbetween. It has an internal thread that while it inherits from
   * Chore, it keeps its own internal stop mechanism so needs to be stopped
   * by this hosting server.
   */
  private void startAllServices() {
    String n = Thread.currentThread().getName();
    UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        abort();
        LOG.fatal("Set stop flag in " + t.getName(), e);
      }
    };
    Threads.setDaemonThreadRunning(this.cacheFlusherThread, n + ".cacheFlusher",
      handler);
    Threads.setDaemonThreadRunning(this.splitOrCompactCheckerThread,
      n + ".splitOrCompactChecker", handler);
    Threads.setDaemonThreadRunning(this.logRollerThread, n + ".logRoller",
      handler);
    // Worker is not the same as the above threads in that it does not
    // inherit from Chore.  Set an UncaughtExceptionHandler on it in case its
    // the one to see an OOME, etc., first.  The handler will set the stop
    // flag.
    Threads.setDaemonThreadRunning(this.workerThread, n + ".worker", handler);
    // Leases is not a Thread. Internally it runs a daemon thread.  If it gets
    // an unhandled exception, it will just exit.
    this.leases = new Leases(
      conf.getInt("hbase.regionserver.lease.period", 3 * 60 * 1000),
      this.threadWakeFrequency);
    this.leases.setName(n + ".leaseChecker");
    this.leases.start();
    // Start Server.  This service is like leases in that it internally runs
    // a thread.
    try {
      this.server.start();
      LOG.info("HRegionServer started at: " +
        serverInfo.getServerAddress().toString());
    } catch(IOException e) {
      this.stopRequested.set(true);
      LOG.fatal("Failed start Server",
        RemoteExceptionHandler.checkIOException(e));
    }
  }
  
  /*
   * Let the master know we're here
   * @throws IOException
   */
  private void reportForDuty() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Telling master we are up");
    }
    this.requestCount.set(0);
    this.serverInfo.setLoad(new HServerLoad(0, onlineRegions.size()));
    this.hbaseMaster.regionServerStartup(serverInfo);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Done telling master we are up");
    }
  }

  /** Add to the outbound message buffer */
  private void reportOpen(HRegion region) {
    synchronized(outboundMsgs) {
      outboundMsgs.add(new HMsg(HMsg.MSG_REPORT_OPEN, region.getRegionInfo()));
    }
  }

  /** Add to the outbound message buffer */
  private void reportClose(HRegion region) {
    synchronized(outboundMsgs) {
      outboundMsgs.add(new HMsg(HMsg.MSG_REPORT_CLOSE, region.getRegionInfo()));
    }
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
    synchronized(outboundMsgs) {
      outboundMsgs.add(new HMsg(HMsg.MSG_REPORT_SPLIT, oldRegion));
      outboundMsgs.add(new HMsg(HMsg.MSG_REPORT_OPEN, newRegionA));
      outboundMsgs.add(new HMsg(HMsg.MSG_REPORT_OPEN, newRegionB));
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMaster-given operations
  //////////////////////////////////////////////////////////////////////////////

  private static class ToDoEntry {
    int tries;
    HMsg msg;
    ToDoEntry(HMsg msg) {
      this.tries = 0;
      this.msg = msg;
    }
  }
  BlockingQueue<ToDoEntry> toDo;
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
      for(ToDoEntry e = null; !stopRequested.get(); ) {
        try {
          e = toDo.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
          // continue
        }
        if(e == null || stopRequested.get()) {
          continue;
        }
        try {
          LOG.info(e.msg.toString());
          
          switch(e.msg.getMsg()) {

          case HMsg.MSG_REGION_OPEN:
            // Open a region
            openRegion(e.msg.getRegionInfo());
            break;

          case HMsg.MSG_REGION_CLOSE:
            // Close a region
            closeRegion(e.msg.getRegionInfo(), true);
            break;

          case HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT:
            // Close a region, don't reply
            closeRegion(e.msg.getRegionInfo(), false);
            break;

          default:
            throw new AssertionError(
                "Impossible state during msg processing.  Instruction: "
                + e.msg.toString());
          }
        } catch (IOException ie) {
          ie = RemoteExceptionHandler.checkIOException(ie);
          if(e.tries < numRetries) {
            LOG.warn(ie);
            e.tries++;
            try {
              toDo.put(e);
            } catch (InterruptedException ex) {
              throw new RuntimeException("Putting into msgQueue was interrupted.", ex);
            }
          } else {
            LOG.error("unable to process message: " + e.msg.toString(), ie);
            if (!checkFileSystem()) {
              break;
            }
          }
        }
      }
      } catch(Throwable t) {
        LOG.fatal("Unhandled exception", t);
      } finally {
        LOG.info("worker thread exiting");
      }
    }
  }
  
  void openRegion(HRegionInfo regionInfo) throws IOException {
    HRegion region = onlineRegions.get(regionInfo.regionName);
    if(region == null) {
      region = new HRegion(rootDir, log, fs, conf, regionInfo, null);
      this.lock.writeLock().lock();
      try {
        this.log.setSequenceNumber(region.getMaxSequenceId());
        this.onlineRegions.put(region.getRegionName(), region);
      } finally {
        this.lock.writeLock().unlock();
      }
    }
    reportOpen(region); 
  }

  void closeRegion(final HRegionInfo hri, final boolean reportWhenCompleted)
  throws IOException {  
    this.lock.writeLock().lock();
    HRegion region = null;
    try {
      region = onlineRegions.remove(hri.regionName);
    } finally {
      this.lock.writeLock().unlock();
    }
      
    if(region != null) {
      region.close();
      if(reportWhenCompleted) {
        reportClose(region);
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
        LOG.debug("closing region " + region.getRegionName());
      }
      try {
        region.close(abortRequested);
      } catch (IOException e) {
        LOG.error("error closing region " + region.getRegionName(),
          RemoteExceptionHandler.checkIOException(e));
      }
    }
    return regionsToClose;
  }

  //
  // HRegionInterface
  //

  /** {@inheritDoc} */
  public HRegionInfo getRegionInfo(final Text regionName)
    throws NotServingRegionException {
    
    requestCount.incrementAndGet();
    return getRegion(regionName).getRegionInfo();
  }

  /** {@inheritDoc} */
  public byte [] get(final Text regionName, final Text row,
      final Text column) throws IOException {
    
    requestCount.incrementAndGet();
    try {
      return getRegion(regionName).get(row, column);
      
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc} */
  public byte [][] get(final Text regionName, final Text row,
      final Text column, final int numVersions) throws IOException {
    
    requestCount.incrementAndGet();
    try {
      return getRegion(regionName).get(row, column, numVersions);
      
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc} */
  public byte [][] get(final Text regionName, final Text row, final Text column, 
      final long timestamp, final int numVersions) throws IOException {
    
    requestCount.incrementAndGet();
    try {
      return getRegion(regionName).get(row, column, timestamp, numVersions);
      
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc} */
  public MapWritable getRow(final Text regionName, final Text row)
    throws IOException {
    
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      MapWritable result = new MapWritable();
      TreeMap<Text, byte[]> map = region.getFull(row);
      for (Map.Entry<Text, byte []> es: map.entrySet()) {
        result.put(new HStoreKey(row, es.getKey()),
            new ImmutableBytesWritable(es.getValue()));
      }
      return result;
      
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc} */
  public MapWritable next(final long scannerId) throws IOException {
    
    requestCount.incrementAndGet();
    try {
      String scannerName = String.valueOf(scannerId);
      HInternalScannerInterface s = scanners.get(scannerName);
      if (s == null) {
        throw new UnknownScannerException("Name: " + scannerName);
      }
      leases.renewLease(scannerId, scannerId);

      // Collect values to be returned here
      MapWritable values = new MapWritable();
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte []> results = new TreeMap<Text, byte []>();
      while (s.next(key, results)) {
        for(Map.Entry<Text, byte []> e: results.entrySet()) {
          values.put(new HStoreKey(key.getRow(), e.getKey(), key.getTimestamp()),
            new ImmutableBytesWritable(e.getValue()));
        }

        if(values.size() > 0) {
          // Row has something in it. Return the value.
          break;
        }

        // No data for this row, go get another.
        results.clear();
      }
      return values;
      
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc} */
  public void batchUpdate(Text regionName, long timestamp, BatchUpdate b)
  throws IOException {  
    requestCount.incrementAndGet();
    // If timestamp == LATEST_TIMESTAMP and we have deletes, then they need
    // special treatment.  For these we need to first find the latest cell so
    // when we write the delete, we write it with the latest cells' timestamp
    // so the delete record overshadows.  This means deletes and puts do not
    // happen within the same row lock.
    List<Text> deletes = null;
    try {
      long lockid = startUpdate(regionName, b.getRow());
      for(BatchOperation op: b) {
        switch(op.getOp()) {
        case PUT:
          put(regionName, lockid, op.getColumn(), op.getValue());
          break;

        case DELETE:
          if (timestamp == LATEST_TIMESTAMP) {
            // Save off these deletes.
            if (deletes == null) {
              deletes = new ArrayList<Text>();
            }
            deletes.add(op.getColumn());
          } else {
            delete(regionName, lockid, op.getColumn());
          }
          break;
        }
      }
      commit(regionName, lockid,
        (timestamp == LATEST_TIMESTAMP)? System.currentTimeMillis(): timestamp);
      
      if (deletes != null && deletes.size() > 0) {
        // We have some LATEST_TIMESTAMP deletes to run.
        HRegion r = getRegion(regionName);
        for (Text column: deletes) {
          r.deleteMultiple(b.getRow(), column, LATEST_TIMESTAMP, 1);
        }
      }
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }
  
  //
  // remote scanner interface
  //

  /** {@inheritDoc} */
  public long openScanner(Text regionName, Text[] cols, Text firstRow,
      final long timestamp, final RowFilterInterface filter)
    throws IOException {
    
    requestCount.incrementAndGet();
    try {
      HRegion r = getRegion(regionName);
      long scannerId = -1L;
      HInternalScannerInterface s =
        r.getScanner(cols, firstRow, timestamp, filter);
      scannerId = rand.nextLong();
      String scannerName = String.valueOf(scannerId);
      synchronized(scanners) {
        scanners.put(scannerName, s);
      }
      leases.createLease(scannerId, scannerId, new ScannerListener(scannerName));
      return scannerId;
    } catch (IOException e) {
      LOG.error("", RemoteExceptionHandler.checkIOException(e));
      checkFileSystem();
      throw e;
    }
  }
  
  /** {@inheritDoc} */
  public void close(final long scannerId) throws IOException {
    requestCount.incrementAndGet();
    try {
      String scannerName = String.valueOf(scannerId);
      HInternalScannerInterface s = null;
      synchronized(scanners) {
        s = scanners.remove(scannerName);
      }
      if(s == null) {
        throw new UnknownScannerException(scannerName);
      }
      s.close();
      leases.cancelLease(scannerId, scannerId);
      
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  Map<String, HInternalScannerInterface> scanners =
    Collections.synchronizedMap(new HashMap<String,
      HInternalScannerInterface>());

  /** 
   * Instantiated as a scanner lease.
   * If the lease times out, the scanner is closed
   */
  private class ScannerListener implements LeaseListener {
    private final String scannerName;
    
    ScannerListener(final String n) {
      this.scannerName = n;
    }
    
    /** {@inheritDoc} */
    public void leaseExpired() {
      LOG.info("Scanner " + this.scannerName + " lease expired");
      HInternalScannerInterface s = null;
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
  
  protected long startUpdate(Text regionName, Text row) throws IOException {
    
    HRegion region = getRegion(regionName);
    return region.startUpdate(row);
  }

  protected void put(final Text regionName, final long lockid,
      final Text column, final byte [] val) throws IOException {

    HRegion region = getRegion(regionName, true);
    region.put(lockid, column, val);
  }

  protected void delete(Text regionName, long lockid, Text column) 
    throws IOException {
    HRegion region = getRegion(regionName);
    region.delete(lockid, column);
  }
  
  /** {@inheritDoc} */
  public void deleteAll(final Text regionName, final Text row,
      final Text column, final long timestamp) 
  throws IOException {
    HRegion region = getRegion(regionName);
    region.deleteAll(row, column, timestamp);
  }

  protected void commit(Text regionName, final long lockid,
      final long timestamp) throws IOException {

    HRegion region = getRegion(regionName, true);
    region.commit(lockid, timestamp);
  }

  /** 
   * Protected utility method for safely obtaining an HRegion handle.
   * @param regionName Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final Text regionName)
    throws NotServingRegionException {
    
    return getRegion(regionName, false);
  }
  
  /** 
   * Protected utility method for safely obtaining an HRegion handle.
   * @param regionName Name of online {@link HRegion} to return
   * @param checkRetiringRegions Set true if we're to check retiring regions
   * as well as online regions.
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final Text regionName,
      final boolean checkRetiringRegions) throws NotServingRegionException {
    
    HRegion region = null;
    this.lock.readLock().lock();
    try {
      region = onlineRegions.get(regionName);
      if (region == null && checkRetiringRegions) {
        region = this.retiringRegions.get(regionName);
        if (LOG.isDebugEnabled()) {
          if (region != null) {
            LOG.debug("Found region " + regionName + " in retiringRegions");
          }
        }
      }

      if (region == null) {
        throw new NotServingRegionException(regionName.toString());
      }
      
      return region;
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /**
   * Checks to see if the file system is still accessible.
   * If not, sets abortRequested and stopRequested
   * 
   * @return false if file system is not available
   */
  protected synchronized boolean checkFileSystem() {
    if (this.fsOk) {
      if (!FSUtils.isFileSystemAvailable(fs)) {
        LOG.fatal("Shutting down HRegionServer: file system not available");
        this.abortRequested = true;
        this.stopRequested.set(true);
        fsOk = false;
      }
    }
    return this.fsOk;
  }
 
  /**
   * @return Returns list of non-closed regions hosted on this server.  If no
   * regions to check, returns an empty list.
   */
  protected List<HRegion> getRegionsToCheck() {
    ArrayList<HRegion> regionsToCheck = new ArrayList<HRegion>();
    lock.readLock().lock();
    try {
      regionsToCheck.addAll(this.onlineRegions.values());
    } finally {
      lock.readLock().unlock();
    }
    // Purge closed regions.
    for (final ListIterator<HRegion> i = regionsToCheck.listIterator();
        i.hasNext();) {
      HRegion r = i.next();
      if (r.isClosed()) {
        i.remove();
      }
    }
    return regionsToCheck;
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
          
          Constructor<? extends HRegionServer> c =
            regionServerClass.getConstructor(Configuration.class);
          HRegionServer hrs = c.newInstance(conf);
          Thread t = new Thread(hrs);
          t.setName("regionserver" + hrs.server.getListenerAddress());
          t.start();
        } catch (Throwable t) {
          LOG.error( "Can not start region server because "+
              StringUtils.stringifyException(t) );
          System.exit(-1);
        }
        break;
      }
      
      if (cmd.equals("stop")) {
        printUsageAndExit("There is no regionserver stop mechanism. To stop " +
          "regionservers, shutdown the hbase master");
      }
      
      // Print out usage if we get to here.
      printUsageAndExit();
    }
  }
  
  /**
   * @param args
   */
  public static void main(String [] args) {
    doMain(args, HRegionServer.class);
  }
}