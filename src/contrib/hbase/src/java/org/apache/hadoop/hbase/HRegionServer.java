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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.StringUtils;

/*******************************************************************************
 * HRegionServer makes a set of HRegions available to clients.  It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 ******************************************************************************/
public class HRegionServer
    implements HConstants, HRegionInterface, Runnable {
  
  public long getProtocolVersion(final String protocol, 
      @SuppressWarnings("unused") final long clientVersion)
  throws IOException { 
    if (protocol.equals(HRegionInterface.class.getName())) {
      return HRegionInterface.versionID;
    }
    throw new IOException("Unknown protocol to name node: " + protocol);
  }

  static final Log LOG = LogFactory.getLog(HRegionServer.class);
  
  volatile boolean stopRequested;
  private Path regionDir;
  HServerInfo info;
  Configuration conf;
  private Random rand;
  
  // region name -> HRegion
  TreeMap<Text, HRegion> onlineRegions = new TreeMap<Text, HRegion>();
  Map<Text, HRegion> retiringRegions = new HashMap<Text, HRegion>();
  
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Vector<HMsg> outboundMsgs;

  long threadWakeFrequency;
  private long msgInterval;
  
  // Check to see if regions should be split
  long splitOrCompactCheckFrequency;
  private SplitOrCompactChecker splitOrCompactChecker;
  private Thread splitOrCompactCheckerThread;
  Integer splitOrCompactLock = Integer.valueOf(0);
  
  /*
   * Interface used by the {@link org.apache.hadoop.io.retry} mechanism.
   */
  interface UpdateMetaInterface {
    /*
     * @return True if succeeded.
     * @throws IOException
     */
   boolean update() throws IOException;
  }
  
  class SplitOrCompactChecker implements Runnable, RegionUnavailableListener {
    HClient client = new HClient(conf);
  
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
        (oldRegion.find(META_TABLE_NAME.toString()) == 0) ?
            ROOT_TABLE_NAME : META_TABLE_NAME;
      if(LOG.isDebugEnabled()) {
        LOG.debug("Updating " + tableToUpdate + " with region split info");
      }
      
      // Wrap the update of META region with an org.apache.hadoop.io.retry.
      UpdateMetaInterface implementation = new UpdateMetaInterface() {
        public boolean update() throws IOException {
          HRegion.removeRegionFromMETA(client, tableToUpdate,
            region.getRegionName());
          for (int i = 0; i < newRegions.length; i++) {
            HRegion.addRegionToMETA(client, tableToUpdate, newRegions[i],
              info.getServerAddress(), info.getStartCode());
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
  private Flusher cacheFlusher;
  private Thread cacheFlusherThread;
  Integer cacheFlusherLock = Integer.valueOf(0);
  class Flusher implements Runnable {
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
  private Path oldlogfile;
  
  // Logging
  HLog log;
  private LogRoller logRoller;
  private Thread logRollerThread;
  Integer logRollerLock = Integer.valueOf(0);
  
  /**
   * Log rolling Runnable.
   */
  class LogRoller implements Runnable {
    private int maxLogEntries =
      conf.getInt("hbase.regionserver.maxlogentries", 30 * 1000);
    
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

  /** Start a HRegionServer at the default location */
  public HRegionServer(Configuration conf) throws IOException {
    this(new Path(conf.get(HREGION_DIR, DEFAULT_HREGION_DIR)),
        new HServerAddress(conf.get(REGIONSERVER_ADDRESS, "localhost:0")),
        conf);
  }
  
  /** Start a HRegionServer at an indicated location */
  public HRegionServer(Path regionDir, HServerAddress address,
      Configuration conf) throws IOException {
    
    // Basic setup
    this.stopRequested = false;
    this.regionDir = regionDir;
    this.conf = conf;
    this.rand = new Random();
    this.outboundMsgs = new Vector<HMsg>();
    this.scanners =
      Collections.synchronizedMap(new TreeMap<Text, HInternalScannerInterface>());

    // Config'ed params
    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.msgInterval = conf.getLong("hbase.regionserver.msginterval",
      15 * 1000);
    this.splitOrCompactCheckFrequency =
      conf.getLong("hbase.regionserver.thread.splitcompactcheckfrequency",
      60 * 1000);
    
    // Cache flushing
    this.cacheFlusher = new Flusher();
    this.cacheFlusherThread =
      new Thread(cacheFlusher, "HRegionServer.cacheFlusher");
    
    // Check regions to see if they need to be split
    this.splitOrCompactChecker = new SplitOrCompactChecker();
    this.splitOrCompactCheckerThread =
      new Thread(splitOrCompactChecker, "HRegionServer.splitOrCompactChecker");
    
    // Process requests from Master
    this.toDo = new Vector<HMsg>();
    this.worker = new Worker();
    this.workerThread = new Thread(worker, "HRegionServer.worker");

    try {
      // Server to handle client requests
      
      this.server = RPC.getServer(this, address.getBindAddress(), 
        address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);

      this.info = new HServerInfo(new HServerAddress(server.getListenerAddress()),
          this.rand.nextLong());

      // Local file paths
      
      String serverName =
        this.info.getServerAddress().getBindAddress() + "_"
        + this.info.getServerAddress().getPort();
      
      Path newlogdir = new Path(regionDir, "log" + "_" + serverName);
      this.oldlogfile = new Path(regionDir, "oldlogfile" + "_" + serverName);

      // Logging
      
      this.fs = FileSystem.get(conf);
      HLog.consolidateOldLog(newlogdir, oldlogfile, fs, conf);
      // TODO: Now we have a consolidated log for all regions, sort and
      // then split result by region passing the splits as reconstruction
      // logs to HRegions on start. Or, rather than consolidate, split logs
      // into per region files.
      this.log = new HLog(fs, newlogdir, conf);
      this.logRoller = new LogRoller();
      this.logRollerThread = new Thread(logRoller, "HRegionServer.logRoller");

      // Remote HMaster
      
      this.hbaseMaster = (HMasterRegionInterface)RPC.waitForProxy(
          HMasterRegionInterface.class, HMasterRegionInterface.versionID,
          new HServerAddress(conf.get(MASTER_ADDRESS)).getInetSocketAddress(),
          conf);

      // Threads
      
      this.workerThread.start();
      this.cacheFlusherThread.start();
      this.splitOrCompactCheckerThread.start();
      this.logRollerThread.start();
      this.leases = new Leases(conf.getLong("hbase.regionserver.lease.period", 
          3 * 60 * 1000), threadWakeFrequency);
      
      // Server

      this.server.start();

    } catch(IOException e) {
      this.stopRequested = true;
      throw e;
    }
    
    LOG.info("HRegionServer started at: " + address.toString());
  }

  /**
   * Set a flag that will cause all the HRegionServer threads to shut down
   * in an orderly fashion.
   */
  public synchronized void stop() {
    stopRequested = true;
    notifyAll();                        // Wakes run() if it is sleeping
  }

  /** Wait on all threads to finish.
   * Presumption is that all closes and stops have already been called.
   */
  public void join() {
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
      info.getServerAddress().toString());
  }
  
  /**
   * The HRegionServer sticks in this loop until closed. It repeatedly checks
   * in with the HMaster, sending heartbeats & reports, and receiving HRegion 
   * load/unload instructions.
   */
  public void run() {
    while(! stopRequested) {
      long lastMsg = 0;
      long waitTime;

      // Let the master know we're here
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Telling master we are up");
        }
        
        hbaseMaster.regionServerStartup(info);
        
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
            HMsg msgs[] = hbaseMaster.regionServerReport(info, outboundArray);
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
                  toDo.clear();
                  closeAllRegions();
                  restart = true;
                  break;
                
                case HMsg.MSG_REGIONSERVER_STOP:
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Got regionserver stop message");
                  }
                  toDo.clear();
                  closeAllRegions();
                  stopRequested = true;
                  break;
                  
                default:
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Got default message");
                  }
                  toDo.add(msgs[i]);
                }
              }
              
              if(toDo.size() > 0) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("notify on todo");
                }
                toDo.notifyAll();
              }
              if(restart || stopRequested) {
                break;
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
    try {
      HMsg[] exitMsg = { new HMsg(HMsg.MSG_REPORT_EXITING) };
      hbaseMaster.regionServerReport(info, exitMsg);
      
    } catch(IOException e) {
      LOG.warn(e);
    }
    try {
      LOG.info("stopping server at: " + info.getServerAddress().toString());

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
      
      this.worker.stop();
      this.server.stop();

      closeAllRegions();
      log.close();
      leases.close();
      join();
      
    } catch(IOException e) {
      LOG.error(e);
    }
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

  Vector<HMsg> toDo;
  private Worker worker;
  private Thread workerThread;
  class Worker implements Runnable {
    public void stop() {
      synchronized(toDo) {
        toDo.notifyAll();
      }
    }
    
    public void run() {
      for(HMsg msg = null; !stopRequested; ) {
        synchronized(toDo) {
          while(toDo.size() == 0 && !stopRequested) {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Wait on todo");
              }
              toDo.wait();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Wake on todo");
              }
            } catch(InterruptedException e) {
              // continue
            }
          }
          if(stopRequested) {
            continue;
          }
          msg = toDo.remove(0);
        }
        
        try {
          switch(msg.getMsg()) {

          case HMsg.MSG_REGION_OPEN:                    // Open a region
            if (LOG.isDebugEnabled()) {
              LOG.debug("MSG_REGION_OPEN");
            }
            openRegion(msg.getRegionInfo());
            break;

          case HMsg.MSG_REGION_CLOSE:                   // Close a region
            if (LOG.isDebugEnabled()) {
              LOG.debug("MSG_REGION_CLOSE");
            }
            closeRegion(msg.getRegionInfo(), true);
            break;

          case HMsg.MSG_REGION_MERGE:                   // Merge two regions
            if (LOG.isDebugEnabled()) {
              LOG.debug("MSG_REGION_MERGE");
            }
            //TODO ???
            throw new IOException("TODO: need to figure out merge");
            //break;

          case HMsg.MSG_CALL_SERVER_STARTUP:            // Close regions, restart
            if (LOG.isDebugEnabled()) {
              LOG.debug("MSG_CALL_SERVER_STARTUP");
            }
            closeAllRegions();
            continue;

          case HMsg.MSG_REGIONSERVER_STOP:              // Go away
            if (LOG.isDebugEnabled()) {
              LOG.debug("MSG_REGIONSERVER_STOP");
            }
            stopRequested = true;
            continue;

          case HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT:    // Close a region, don't reply
            if (LOG.isDebugEnabled()) {
              LOG.debug("MSG_REGION_CLOSE_WITHOUT_REPORT");
            }
            closeRegion(msg.getRegionInfo(), false);
            break;

          default:
            throw new IOException("Impossible state during msg processing.  Instruction: " + msg);
          }
        } catch(IOException e) {
          LOG.error(e);
        }
      }
      LOG.info("worker thread exiting");
    }
  }
  
  void openRegion(HRegionInfo regionInfo) throws IOException {
    this.lock.writeLock().lock();
    try {
      HRegion region =
        new HRegion(regionDir, log, fs, conf, regionInfo, null, oldlogfile);
      this.onlineRegions.put(region.getRegionName(), region);
      reportOpen(region); 
    } finally {
      this.lock.writeLock().unlock();
    }
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
  void closeAllRegions() {
    Vector<HRegion> regionsToClose = new Vector<HRegion>();
    this.lock.writeLock().lock();
    try {
      regionsToClose.addAll(onlineRegions.values());
      onlineRegions.clear();
    } finally {
      this.lock.writeLock().unlock();
    }
    for(Iterator<HRegion> it = regionsToClose.iterator(); it.hasNext(); ) {
      HRegion region = it.next();
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
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegionInterface
  //////////////////////////////////////////////////////////////////////////////

  /** Obtain a table descriptor for the given region */
  public HRegionInfo getRegionInfo(Text regionName)
  throws NotServingRegionException {
    return getRegion(regionName).getRegionInfo();
  }

  /** Get the indicated row/column */
  public BytesWritable get(Text regionName, Text row, Text column)
  throws IOException {
    return getRegion(regionName).get(row, column);
  }

  /** Get multiple versions of the indicated row/col */
  public BytesWritable[] get(Text regionName, Text row, Text column, 
      int numVersions)
  throws IOException {
    return getRegion(regionName).get(row, column, numVersions);
  }

  /** Get multiple timestamped versions of the indicated row/col */
  public BytesWritable[] get(Text regionName, Text row, Text column, 
      long timestamp, int numVersions)
  throws IOException {
    return getRegion(regionName).get(row, column, timestamp, numVersions);
  }

  /** Get all the columns (along with their names) for a given row. */
  public LabelledData[] getRow(Text regionName, Text row) throws IOException {
    HRegion region = getRegion(regionName);
    TreeMap<Text, BytesWritable> map = region.getFull(row);
    LabelledData result[] = new LabelledData[map.size()];
    int counter = 0;
    for (Map.Entry<Text, BytesWritable> es: map.entrySet()) {
      result[counter++] = new LabelledData(es.getKey(), es.getValue());
    }
    return result;
  }

  /**
   * Start an update to the HBase.  This also creates a lease associated with
   * the caller.
   */
  private static class RegionListener extends LeaseListener {
    private HRegion localRegion;
    private long localLockId;
    
    public RegionListener(HRegion region, long lockId) {
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
  
  public LabelledData[] next(final long scannerId, final HStoreKey key)
  throws IOException {
    Text scannerName = new Text(String.valueOf(scannerId));
    HInternalScannerInterface s = scanners.get(scannerName);
    if (s == null) {
      throw new UnknownScannerException("Name: " + scannerName + ", key " +
        key);
    }
    leases.renewLease(scannerName, scannerName);
    TreeMap<Text, BytesWritable> results = new TreeMap<Text, BytesWritable>();
    ArrayList<LabelledData> values = new ArrayList<LabelledData>();
    // Keep getting rows till we find one that has at least one non-deleted
    // column value.
    while (s.next(key, results)) {
      for(Map.Entry<Text, BytesWritable> e: results.entrySet()) {
        BytesWritable val = e.getValue();
        if(val.getSize() == DELETE_BYTES.getSize()
            && val.compareTo(DELETE_BYTES) == 0) {
          // Column value is deleted. Don't return it.
          continue;
        }
        values.add(new LabelledData(e.getKey(), val));
      }
      if (values.size() > 0) {
        // Row has something in it. Let it out. Else go get another row.
        break;
      }
      // Need to clear results before we go back up and call 'next' again.
      results.clear();
    }
    return values.toArray(new LabelledData[values.size()]);
  }

  public long startUpdate(Text regionName, long clientid, Text row) 
      throws IOException {
    HRegion region = getRegion(regionName);
    long lockid = region.startUpdate(row);
    this.leases.createLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)), 
        new RegionListener(region, lockid));
    
    return lockid;
  }

  /** Add something to the HBase. */
  public void put(Text regionName, long clientid, long lockid, Text column, 
      BytesWritable val) throws IOException {
    HRegion region = getRegion(regionName, true);
    leases.renewLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
    region.put(lockid, column, val);
  }

  /** Remove a cell from the HBase. */
  public void delete(Text regionName, long clientid, long lockid, Text column) 
      throws IOException {
    HRegion region = getRegion(regionName);
    leases.renewLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
    region.delete(lockid, column);
  }

  /** Abandon the transaction */
  public void abort(Text regionName, long clientid, long lockid) 
      throws IOException {
    HRegion region = getRegion(regionName, true);
    leases.cancelLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
    region.abort(lockid);
  }

  /** Confirm the transaction */
  public void commit(Text regionName, long clientid, long lockid) 
  throws IOException {
    HRegion region = getRegion(regionName, true);
    leases.cancelLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
    region.commit(lockid);
  }

  /** Don't let the client's lease expire just yet...  */
  public void renewLease(long lockid, long clientid) throws IOException {
    leases.renewLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
  }

  /** Private utility method for safely obtaining an HRegion handle.
   * @param regionName Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  private HRegion getRegion(final Text regionName)
  throws NotServingRegionException {
    return getRegion(regionName, false);
  }
  
  /** Private utility method for safely obtaining an HRegion handle.
   * @param regionName Name of online {@link HRegion} to return
   * @param checkRetiringRegions Set true if we're to check retiring regions
   * as well as online regions.
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  private HRegion getRegion(final Text regionName,
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

  Map<Text, HInternalScannerInterface> scanners;
  
  private class ScannerListener extends LeaseListener {
    private Text scannerName;
    
    public ScannerListener(Text scannerName) {
      this.scannerName = scannerName;
    }
    
    public void leaseExpired() {
      LOG.info("Scanner " + scannerName + " lease expired");
      HInternalScannerInterface s = null;
      synchronized(scanners) {
        s = scanners.remove(scannerName);
      }
      if(s != null) {
        s.close();
      }
    }
  }
  
  /** Start a scanner for a given HRegion. */
  public long openScanner(Text regionName, Text[] cols, Text firstRow)
  throws IOException {
    HRegion r = getRegion(regionName);
    long scannerId = -1L;
    try {
      HInternalScannerInterface s = r.getScanner(cols, firstRow);
      scannerId = rand.nextLong();
      Text scannerName = new Text(String.valueOf(scannerId));
      synchronized(scanners) {
        scanners.put(scannerName, s);
      }
      leases.createLease(scannerName, scannerName,
        new ScannerListener(scannerName));
    } catch(IOException e) {
      LOG.error(e);
      throw e;
    }
    return scannerId;
  }
  
  public void close(long scannerId) throws IOException {
    Text scannerName = new Text(String.valueOf(scannerId));
    HInternalScannerInterface s = null;
    synchronized(scanners) {
      s = scanners.remove(scannerName);
    }
    if(s == null) {
      throw new UnknownScannerException(scannerName.toString());
    }
    s.close();
    leases.cancelLease(scannerName, scannerName);
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
          LOG.error( "Can not start master because "+
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
