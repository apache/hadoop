/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;

/*******************************************************************************
 * HRegionServer makes a set of HRegions available to clients.  It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 ******************************************************************************/
public class HRegionServer implements HConstants, HRegionInterface, Runnable {
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.ipc.VersionedProtocol#getProtocolVersion(java.lang.String, long)
   */
  public long getProtocolVersion(final String protocol, 
      @SuppressWarnings("unused") final long clientVersion)
  throws IOException { 
    if (protocol.equals(HRegionInterface.class.getName())) {
      return HRegionInterface.versionID;
    }
    throw new IOException("Unknown protocol to name node: " + protocol);
  }

  static final Log LOG = LogFactory.getLog(HRegionServer.class);
  
  protected volatile boolean stopRequested;
  protected volatile boolean abortRequested;
  private final Path rootDir;
  protected final HServerInfo serverInfo;
  protected final Configuration conf;
  private final Random rand;
  
  // region name -> HRegion
  protected final SortedMap<Text, HRegion> onlineRegions;
  protected final Map<Text, HRegion> retiringRegions = new HashMap<Text, HRegion>();
  
  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Vector<HMsg> outboundMsgs;

  int numRetries;
  protected final long threadWakeFrequency;
  private final long msgInterval;
  
  // Check to see if regions should be split
  protected final long splitOrCompactCheckFrequency;
  private final SplitOrCompactChecker splitOrCompactChecker;
  private final Thread splitOrCompactCheckerThread;
  protected final Integer splitOrCompactLock = new Integer(0);
  
  /**
   * Interface used by the {@link org.apache.hadoop.io.retry} mechanism.
   */
  public interface UpdateMetaInterface {
    /**
     * @return True if succeeded.
     * @throws IOException
     */
   public boolean update() throws IOException;
  }

  /** Runs periodically to determine if regions need to be compacted or split */
  class SplitOrCompactChecker implements Runnable, RegionUnavailableListener {
    HClient client = new HClient(conf);
  
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.RegionUnavailableListener#closing(org.apache.hadoop.io.Text)
     */
    public void closing(final Text regionName) {
      lock.writeLock().lock();
      try {
        // Remove region from regions Map and add it to the Map of retiring
        // regions.
        retiringRegions.put(regionName, onlineRegions.remove(regionName));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding " + regionName + " to retiringRegions");
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.RegionUnavailableListener#closed(org.apache.hadoop.io.Text)
     */
    public void closed(final Text regionName) {
      lock.writeLock().lock();
      try {
        retiringRegions.remove(regionName);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing " + regionName + " from retiringRegions");
        }
      } finally {
        lock.writeLock().unlock();
      }
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {
      while(! stopRequested) {
        long startTime = System.currentTimeMillis();
        synchronized(splitOrCompactLock) { // Don't interrupt us while we're working
          // Grab a list of regions to check
          Vector<HRegion> regionsToCheck = new Vector<HRegion>();
          lock.readLock().lock();
          try {
            regionsToCheck.addAll(onlineRegions.values());
          } finally {
            lock.readLock().unlock();
          }

          try {
            for(HRegion cur: regionsToCheck) {
              if(cur.isClosed()) {
                continue;                               // Skip if closed
              }

              if(cur.needsCompaction()) {
                // Best time to split a region is right after compaction
                if(cur.compactStores()) {
                  Text midKey = new Text();
                  if(cur.needsSplit(midKey)) {
                    split(cur, midKey);
                  }
                }
              }
            }
          } catch(IOException e) {
            //TODO: What happens if this fails? Are we toast?
            LOG.error("What happens if this fails? Are we toast?", e);
          }
        }
        
        if (stopRequested) {
          continue;
        }

        // Sleep
        long waitTime = splitOrCompactCheckFrequency -
          (System.currentTimeMillis() - startTime);
        if (waitTime > 0) {
          try {
            Thread.sleep(waitTime);
          } catch(InterruptedException iex) {
            // continue
          }
        }
      }
      LOG.info("splitOrCompactChecker exiting");
    }
    
    private void split(final HRegion region, final Text midKey)
    throws IOException {
      final Text oldRegion = region.getRegionName();
      final HRegion[] newRegions = region.closeAndSplit(midKey, this);

      // When a region is split, the META table needs to updated if we're
      // splitting a 'normal' region, and the ROOT table needs to be
      // updated if we are splitting a META region.
      final Text tableToUpdate =
        region.getRegionInfo().tableDesc.getName().equals(META_TABLE_NAME) ?
            ROOT_TABLE_NAME : META_TABLE_NAME;
      if(LOG.isDebugEnabled()) {
        LOG.debug("Updating " + tableToUpdate + " with region split info");
      }
      
      // Wrap the update of META region with an org.apache.hadoop.io.retry.
      UpdateMetaInterface implementation = new UpdateMetaInterface() {
        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.HRegionServer.UpdateMetaInterface#update()
         */
        public boolean update() throws IOException {
          HRegion.removeRegionFromMETA(client, tableToUpdate,
            region.getRegionName());
          for (int i = 0; i < newRegions.length; i++) {
            HRegion.addRegionToMETA(client, tableToUpdate, newRegions[i],
              serverInfo.getServerAddress(), serverInfo.getStartCode());
          }
          
          // Now tell the master about the new regions
          if (LOG.isDebugEnabled()) {
            LOG.debug("Reporting region split to master");
          }
          reportSplit(newRegions[0].getRegionInfo(), newRegions[1].
            getRegionInfo());
          LOG.info("region split, META update, and report to master all" +
            " successful. Old region=" + oldRegion + ", new regions: " +
            newRegions[0].getRegionName() + ", " +
            newRegions[1].getRegionName());

          // Finally, start serving the new regions
          lock.writeLock().lock();
          try {
            onlineRegions.put(newRegions[0].getRegionName(), newRegions[0]);
            onlineRegions.put(newRegions[1].getRegionName(), newRegions[1]);
          } finally {
            lock.writeLock().unlock();
          }
          return true;
        }
      };
      
      // Get retry proxy wrapper around 'implementation'.
      UpdateMetaInterface retryProxy = (UpdateMetaInterface)RetryProxy.
        create(UpdateMetaInterface.class, implementation,
        client.getRetryPolicy());
      // Run retry.
      retryProxy.update();
    }
  }

  // Cache flushing  
  private final Flusher cacheFlusher;
  private final Thread cacheFlusherThread;
  protected final Integer cacheFlusherLock = new Integer(0);
  /** Runs periodically to flush the memcache */
  class Flusher implements Runnable {
    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {
      while(! stopRequested) {
        long startTime = System.currentTimeMillis();

        synchronized(cacheFlusherLock) {

          // Grab a list of items to flush

          Vector<HRegion> toFlush = new Vector<HRegion>();
          lock.readLock().lock();
          try {
            toFlush.addAll(onlineRegions.values());
          } finally {
            lock.readLock().unlock();
          }

          // Flush them, if necessary

          for(HRegion cur: toFlush) {
            if(cur.isClosed()) {                // Skip if closed
              continue;
            }

            try {
              cur.optionallyFlush();
            } catch(IOException iex) {
              LOG.error(iex);
            }
          }
        }
        
        // Sleep
        long waitTime = stopRequested ? 0
            : threadWakeFrequency - (System.currentTimeMillis() - startTime);
        
        if(waitTime > 0) {
          try {
            Thread.sleep(waitTime);
          } catch(InterruptedException iex) {
            // continue
          }
        }
      }
      LOG.info("cacheFlusher exiting");
    }
  }
  
  // File paths
  
  private FileSystem fs;
  
  // Logging
  
  protected final HLog log;
  private final LogRoller logRoller;
  private final Thread logRollerThread;
  protected final Integer logRollerLock = new Integer(0);
  
  /** Runs periodically to determine if the log should be rolled */
  class LogRoller implements Runnable {
    private int maxLogEntries =
      conf.getInt("hbase.regionserver.maxlogentries", 30 * 1000);
    
    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {
      while(! stopRequested) {
        synchronized(logRollerLock) {
          // If the number of log entries is high enough, roll the log.  This is a
          // very fast operation, but should not be done too frequently.
          int nEntries = log.getNumEntries();
          if(nEntries > this.maxLogEntries) {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Rolling log. Number of entries is: " + nEntries);
              }
              log.rollWriter();
            } catch(IOException iex) {
              // continue
            }
          }
        }
        
        if(!stopRequested) {
          try {
            Thread.sleep(threadWakeFrequency);
          } catch(InterruptedException iex) {
            // continue
          }
        }
      }
      LOG.info("logRoller exiting");
    }
  }
  
  // Remote HMaster

  private HMasterRegionInterface hbaseMaster;

  // Server
  
  private Server server;
  
  // Leases
  private Leases leases;

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
      Configuration conf) throws IOException {
    
    // Basic setup
    this.stopRequested = false;
    this.abortRequested = false;
    this.rootDir = rootDir;
    this.conf = conf;
    this.rand = new Random();
    this.onlineRegions =
      Collections.synchronizedSortedMap(new TreeMap<Text, HRegion>());
    
    this.outboundMsgs = new Vector<HMsg>();

    // Config'ed params
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.msgInterval = conf.getLong("hbase.regionserver.msginterval",
      15 * 1000);
    this.splitOrCompactCheckFrequency =
      conf.getLong("hbase.regionserver.thread.splitcompactcheckfrequency",
      60 * 1000);

    // Cache flushing
    this.cacheFlusher = new Flusher();
    this.cacheFlusherThread = new Thread(cacheFlusher);
    
    // Check regions to see if they need to be split
    this.splitOrCompactChecker = new SplitOrCompactChecker();
    this.splitOrCompactCheckerThread = new Thread(splitOrCompactChecker);
    
    // Process requests from Master
    this.toDo = new LinkedList<ToDoEntry>();
    this.worker = new Worker();
    this.workerThread = new Thread(worker);

    try {
      // Server to handle client requests
      this.server = RPC.getServer(this, address.getBindAddress(), 
        address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);

       // Use configured nameserver & interface to get local hostname.
       // 'serverInfo' is sent to master.  Should have name of this host rather than
       // 'localhost' or 0.0.0.0 or 127.0.0.1 in it.
       String localHostname = DNS.getDefaultHost(
         conf.get("dfs.datanode.dns.interface","default"),
         conf.get("dfs.datanode.dns.nameserver","default"));
       InetSocketAddress hostnameAddress = new InetSocketAddress(localHostname,
         server.getListenerAddress().getPort());
       this.serverInfo = new HServerInfo(new HServerAddress(hostnameAddress),
         this.rand.nextLong());

      // Local file paths
      String serverName = localHostname + "_" +
        this.serverInfo.getServerAddress().getPort();
      
      Path logdir = new Path(rootDir, "log" + "_" + serverName);

      // Logging
      this.fs = FileSystem.get(conf);
      if(fs.exists(logdir)) {
        throw new RegionServerRunningException("region server already running at " +
          this.serverInfo.getServerAddress().toString() + " because logdir " +
          logdir.toString() + " exists");
      }
      
      this.log = new HLog(fs, logdir, conf);
      this.logRoller = new LogRoller();
      this.logRollerThread = new Thread(logRoller);

      // Remote HMaster
      this.hbaseMaster = (HMasterRegionInterface)RPC.waitForProxy(
          HMasterRegionInterface.class, HMasterRegionInterface.versionID,
          new HServerAddress(conf.get(MASTER_ADDRESS)).getInetSocketAddress(),
          conf);
    } catch(IOException e) {
      this.stopRequested = true;
      throw e;
    }
  }

  /**
   * Sets a flag that will cause all the HRegionServer threads to shut down
   * in an orderly fashion.
   */
  public synchronized void stop() {
    stopRequested = true;
    notifyAll();                        // Wakes run() if it is sleeping
  }
  
  /**
   * Cause the server to exit without closing the regions it is serving, the
   * log it is using and without notifying the master.
   * 
   * FOR DEBUGGING ONLY
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
    
    // Threads
    
    String threadName = Thread.currentThread().getName();

    workerThread.setName(threadName + ".worker");
    workerThread.start();
    cacheFlusherThread.setName(threadName + ".cacheFlusher");
    cacheFlusherThread.start();
    splitOrCompactCheckerThread.setName(threadName + ".splitOrCompactChecker");
    splitOrCompactCheckerThread.start();
    logRollerThread.setName(threadName + ".logRoller");
    logRollerThread.start();
    leases = new Leases(conf.getLong("hbase.regionserver.lease.period", 
        3 * 60 * 1000), threadWakeFrequency);
    
    // Server

    try {
      this.server.start();
      LOG.info("HRegionServer started at: " + serverInfo.getServerAddress().toString());
    } catch(IOException e) {
      LOG.error(e);
      stopRequested = true;
    }

    while(! stopRequested) {
      long lastMsg = 0;
      long waitTime;

      // Let the master know we're here
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Telling master we are up");
        }
        
        hbaseMaster.regionServerStartup(serverInfo);
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("Done telling master we are up");
        }
      } catch(IOException e) {
        waitTime = stopRequested ? 0
            : msgInterval - (System.currentTimeMillis() - lastMsg);
        
        if(waitTime > 0) {
          synchronized (this) {
            try {
              wait(waitTime);
            } catch (InterruptedException e1) {
              // Go back up to the while test if stop has been requested.
            }
          }
        }
        continue;
      }
      
      // Now ask master what it wants us to do and tell it what we have done.
      while (!stopRequested) {
        if ((System.currentTimeMillis() - lastMsg) >= msgInterval) {

          HMsg outboundArray[] = null;
          synchronized(outboundMsgs) {
            outboundArray = outboundMsgs.toArray(new HMsg[outboundMsgs.size()]);
            outboundMsgs.clear();
          }

          try {
            HMsg msgs[] = hbaseMaster.regionServerReport(serverInfo, outboundArray);
            lastMsg = System.currentTimeMillis();

            // Queue up the HMaster's instruction stream for processing

            synchronized(toDo) {
              boolean restart = false;
              for(int i = 0; i < msgs.length && !stopRequested && !restart; i++) {
                switch(msgs[i].getMsg()) {
                
                case HMsg.MSG_CALL_SERVER_STARTUP:
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Got call server startup message");
                  }
                  closeAllRegions();
                  restart = true;
                  break;
                
                case HMsg.MSG_REGIONSERVER_STOP:
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Got regionserver stop message");
                  }
                  stopRequested = true;
                  break;
                  
                default:
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Got default message");
                  }
                  toDo.addLast(new ToDoEntry(msgs[i]));
                }
              }
              
              if(restart || stopRequested) {
                toDo.clear();
                break;
              }
              
              if(toDo.size() > 0) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("notify on todo");
                }
                toDo.notifyAll();
              }
            }

          } catch(IOException e) {
            LOG.error(e);
          }
        }

        waitTime = stopRequested ? 0
            : msgInterval - (System.currentTimeMillis() - lastMsg);
        if (waitTime > 0) {
          synchronized (this) {
            try {
              wait(waitTime);
            } catch(InterruptedException iex) {
              // On interrupt we go around to the while test of stopRequested
            }
          }
        }
      }
    }
    this.worker.stop();
    this.server.stop();
    leases.close();
    
    // Send interrupts to wake up threads if sleeping so they notice shutdown.

    synchronized(logRollerLock) {
      this.logRollerThread.interrupt();
    }

    synchronized(cacheFlusherLock) {
      this.cacheFlusherThread.interrupt();
    }

    synchronized(splitOrCompactLock) {
      this.splitOrCompactCheckerThread.interrupt();
    }

    if(abortRequested) {
      try {
        log.rollWriter();
        
      } catch(IOException e) {
        LOG.warn(e);
      }
      LOG.info("aborting server at: " + serverInfo.getServerAddress().toString());
      
    } else {
      Vector<HRegion> closedRegions = closeAllRegions();
      try {
        log.closeAndDelete();

      } catch(IOException e) {
        LOG.error(e);
      }
      try {
        HMsg[] exitMsg = new HMsg[closedRegions.size() + 1];
        exitMsg[0] = new HMsg(HMsg.MSG_REPORT_EXITING);

        // Tell the master what regions we are/were serving

        int i = 1;
        for(HRegion region: closedRegions) {
          exitMsg[i++] = new HMsg(HMsg.MSG_REPORT_CLOSE, region.getRegionInfo());
        }

        LOG.info("telling master that region server is shutting down at: "
            + serverInfo.getServerAddress().toString());
        
        hbaseMaster.regionServerReport(serverInfo, exitMsg);

      } catch(IOException e) {
        LOG.warn(e);
      }
      LOG.info("stopping server at: " + serverInfo.getServerAddress().toString());
    }

    join();
      
    if(LOG.isDebugEnabled()) {
      LOG.debug("main thread exiting");
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
  void reportSplit(HRegionInfo newRegionA, HRegionInfo newRegionB) {
    synchronized(outboundMsgs) {
      outboundMsgs.add(new HMsg(HMsg.MSG_NEW_REGION, newRegionA));
      outboundMsgs.add(new HMsg(HMsg.MSG_NEW_REGION, newRegionB));
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
  LinkedList<ToDoEntry> toDo;
  private Worker worker;
  private Thread workerThread;
  /** Thread that performs long running requests from the master */
  class Worker implements Runnable {
    void stop() {
      synchronized(toDo) {
        toDo.notifyAll();
      }
    }
    
    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {
      for(ToDoEntry e = null; !stopRequested; ) {
        synchronized(toDo) {
          while(toDo.size() == 0 && !stopRequested) {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Wait on todo");
              }
              toDo.wait(threadWakeFrequency);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Wake on todo");
              }
            } catch(InterruptedException ex) {
              // continue
            }
          }
          if(stopRequested) {
            continue;
          }
          e = toDo.removeFirst();
        }
        
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug(e.msg.toString());
          }
          
          switch(e.msg.getMsg()) {

          case HMsg.MSG_REGION_OPEN:                    // Open a region
            openRegion(e.msg.getRegionInfo());
            break;

          case HMsg.MSG_REGION_CLOSE:                   // Close a region
            closeRegion(e.msg.getRegionInfo(), true);
            break;

          case HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT:    // Close a region, don't reply
            closeRegion(e.msg.getRegionInfo(), false);
            break;

          default:
            throw new AssertionError(
                "Impossible state during msg processing.  Instruction: "
                + e.msg.toString());
          }
        } catch(IOException ie) {
          if(e.tries < numRetries) {
            LOG.warn(ie);
            e.tries++;
            synchronized(toDo) {
              toDo.addLast(e);
            }
          } else {
            LOG.error("unable to process message: " + e.msg.toString(), ie);
          }
        }
      }
      LOG.info("worker thread exiting");
    }
  }
  
  void openRegion(HRegionInfo regionInfo) throws IOException {
    HRegion region = onlineRegions.get(regionInfo.regionName);
    if(region == null) {
      region = new HRegion(rootDir, log, fs, conf, regionInfo, null);

      this.lock.writeLock().lock();
      try {
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
  Vector<HRegion> closeAllRegions() {
    Vector<HRegion> regionsToClose = new Vector<HRegion>();
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
        region.close();
        LOG.debug("region closed " + region.getRegionName());
      } catch(IOException e) {
        LOG.error("error closing region " + region.getRegionName(), e);
      }
    }
    return regionsToClose;
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegionInterface
  //////////////////////////////////////////////////////////////////////////////

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#getRegionInfo(org.apache.hadoop.io.Text)
   */
  public HRegionInfo getRegionInfo(final Text regionName)
  throws NotServingRegionException {
    return getRegion(regionName).getRegionInfo();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#get(org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text)
   */
  public byte [] get(final Text regionName, final Text row,
      final Text column)
  throws IOException {
    return getRegion(regionName).get(row, column);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#get(org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, int)
   */
  public byte [][] get(final Text regionName, final Text row,
      final Text column, final int numVersions)
  throws IOException {  
    return getRegion(regionName).get(row, column, numVersions);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#get(org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, long, int)
   */
  public byte [][] get(final Text regionName, final Text row, final Text column, 
      final long timestamp, final int numVersions) throws IOException {
    return getRegion(regionName).get(row, column, timestamp, numVersions);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#getRow(org.apache.hadoop.io.Text, org.apache.hadoop.io.Text)
   */
  public KeyedData[] getRow(final Text regionName, final Text row) throws IOException {
    HRegion region = getRegion(regionName);
    TreeMap<Text, byte[]> map = region.getFull(row);
    KeyedData result[] = new KeyedData[map.size()];
    int counter = 0;
    for (Map.Entry<Text, byte []> es: map.entrySet()) {
      result[counter++] =
        new KeyedData(new HStoreKey(row, es.getKey()), es.getValue());
    }
    return result;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#next(long)
   */
  public KeyedData[] next(final long scannerId)
  throws IOException {
    String scannerName = String.valueOf(scannerId);
    HInternalScannerInterface s = scanners.get(scannerName);
    if (s == null) {
      throw new UnknownScannerException("Name: " + scannerName);
    }
    leases.renewLease(scannerId, scannerId);
    
    // Collect values to be returned here
    
    ArrayList<KeyedData> values = new ArrayList<KeyedData>();
    
    TreeMap<Text, byte []> results = new TreeMap<Text, byte []>();
    
    // Keep getting rows until we find one that has at least one non-deleted column value
    
    HStoreKey key = new HStoreKey();
    while (s.next(key, results)) {
      for(Map.Entry<Text, byte []> e: results.entrySet()) {
        HStoreKey k = new HStoreKey(key.getRow(), e.getKey(), key.getTimestamp());
        byte [] val = e.getValue();
        if (DELETE_BYTES.compareTo(val) == 0) {
          // Column value is deleted. Don't return it.
          if (LOG.isDebugEnabled()) {
            LOG.debug("skipping deleted value for key: " + k.toString());
          }
          continue;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("adding value for key: " + k.toString());
        }
        values.add(new KeyedData(k, val));
      }
      if(values.size() > 0) {
        // Row has something in it. Return the value.
        break;
      }
      
      // No data for this row, go get another.
      
      results.clear();
    }
    return values.toArray(new KeyedData[values.size()]);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#startUpdate(org.apache.hadoop.io.Text, long, org.apache.hadoop.io.Text)
   */
  public long startUpdate(Text regionName, long clientid, Text row) 
      throws IOException {
    HRegion region = getRegion(regionName);
    long lockid = region.startUpdate(row);
    this.leases.createLease(clientid, lockid,
      new RegionListener(region, lockid));
    return lockid;
  }

  /** Create a lease for an update. If it times out, the update is aborted */
  private static class RegionListener implements LeaseListener {
    private HRegion localRegion;
    private long localLockId;
    
    RegionListener(HRegion region, long lockId) {
      this.localRegion = region;
      this.localLockId = lockId;
    }
    
    public void leaseExpired() {
      try {
        localRegion.abort(localLockId);
        
      } catch(IOException iex) {
        LOG.error(iex);
      }
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#put(org.apache.hadoop.io.Text, long, long, org.apache.hadoop.io.Text, org.apache.hadoop.io.BytesWritable)
   */
  public void put(final Text regionName, final long clientid,
      final long lockid, final Text column, final byte [] val)
  throws IOException {
    HRegion region = getRegion(regionName, true);
    leases.renewLease(clientid, lockid);
    region.put(lockid, column, val);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#delete(org.apache.hadoop.io.Text, long, long, org.apache.hadoop.io.Text)
   */
  public void delete(Text regionName, long clientid, long lockid, Text column) 
  throws IOException {
    HRegion region = getRegion(regionName);
    leases.renewLease(clientid, lockid);
    region.delete(lockid, column);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#abort(org.apache.hadoop.io.Text, long, long)
   */
  public void abort(Text regionName, long clientid, long lockid) 
  throws IOException {
    HRegion region = getRegion(regionName, true);
    leases.cancelLease(clientid, lockid);
    region.abort(lockid);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#commit(org.apache.hadoop.io.Text, long, long)
   */
  public void commit(Text regionName, long clientid, long lockid) 
  throws IOException {
    HRegion region = getRegion(regionName, true);
    leases.cancelLease(clientid, lockid);
    region.commit(lockid);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#renewLease(long, long)
   */
  public void renewLease(long lockid, long clientid) throws IOException {
    leases.renewLease(clientid, lockid);
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
      final boolean checkRetiringRegions)
  throws NotServingRegionException {
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

  //////////////////////////////////////////////////////////////////////////////
  // remote scanner interface
  //////////////////////////////////////////////////////////////////////////////

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
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.LeaseListener#leaseExpired()
     */
    public void leaseExpired() {
      LOG.info("Scanner " + this.scannerName + " lease expired");
      HInternalScannerInterface s = null;
      synchronized(scanners) {
        s = scanners.remove(this.scannerName);
      }
      if (s != null) {
        s.close();
      }
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#openScanner(org.apache.hadoop.io.Text, org.apache.hadoop.io.Text[], org.apache.hadoop.io.Text)
   */
  public long openScanner(Text regionName, Text[] cols, Text firstRow)
  throws IOException {
    HRegion r = getRegion(regionName);
    long scannerId = -1L;
    try {
      HInternalScannerInterface s = r.getScanner(cols, firstRow);
      scannerId = rand.nextLong();
      String scannerName = String.valueOf(scannerId);
      synchronized(scanners) {
        scanners.put(scannerName, s);
      }
      leases.createLease(scannerId, scannerId,
        new ScannerListener(scannerName));
    } catch(IOException e) {
      LOG.error(e);
      throw e;
    }
    return scannerId;
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HRegionInterface#close(long)
   */
  public void close(final long scannerId) throws IOException {
    String scannerName = String.valueOf(scannerId);
    HInternalScannerInterface s = null;
    synchronized(scanners) {
      s = scanners.remove(scannerName);
    }
    if(s == null) {
      throw new UnknownScannerException(scannerName.toString());
    }
    s.close();
    leases.cancelLease(scannerId, scannerId);
  }

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
   * @param args
   */
  public static void main(String [] args) {
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
          (new Thread(new HRegionServer(conf))).start();
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
}
