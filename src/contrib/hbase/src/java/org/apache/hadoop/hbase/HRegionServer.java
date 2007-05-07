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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*******************************************************************************
 * HRegionServer makes a set of HRegions available to clients.  It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 ******************************************************************************/
public class HRegionServer
    implements HConstants, HRegionInterface, Runnable {
  
  public long getProtocolVersion(String protocol, 
      long clientVersion) throws IOException { 
    if (protocol.equals(HRegionInterface.class.getName())) {
      return HRegionInterface.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  private static final Log LOG = LogFactory.getLog(HRegionServer.class);
  
  private volatile boolean stopRequested;
  private Path regionDir;
  private HServerAddress address;
  private Configuration conf;
  private Random rand;
  private TreeMap<Text, HRegion> regions;               // region name -> HRegion
  private ReadWriteLock locker;
  private Vector<HMsg> outboundMsgs;

  private long threadWakeFrequency;
  private int maxLogEntries;
  private long msgInterval;
  
  // Check to see if regions should be split
  
  private long splitCheckFrequency;
  private SplitChecker splitChecker;
  private Thread splitCheckerThread;
  
  private class SplitChecker implements Runnable {
    private HClient client = new HClient(conf);
  
    private class SplitRegion {
      public HRegion region;
      public Text midKey;
      
      SplitRegion(HRegion region, Text midKey) {
        this.region = region;
        this.midKey = midKey;
      }
    }
    
    public void run() {
      while(! stopRequested) {
        long startTime = System.currentTimeMillis();

        // Grab a list of regions to check
        Vector<HRegion> checkSplit = new Vector<HRegion>();
        locker.readLock().lock();
        try {
          checkSplit.addAll(regions.values());
        } finally {
          locker.readLock().unlock();
        }

        // Check to see if they need splitting

        Vector<SplitRegion> toSplit = new Vector<SplitRegion>();
        for(Iterator<HRegion> it = checkSplit.iterator(); it.hasNext(); ) {
          HRegion cur = it.next();
          Text midKey = new Text();
          
          try {
            if(cur.needsSplit(midKey)) {
              toSplit.add(new SplitRegion(cur, midKey));
            }
            
          } catch(IOException iex) {
            iex.printStackTrace();
          }
        }

        for(Iterator<SplitRegion> it = toSplit.iterator(); it.hasNext(); ) {
          SplitRegion r = it.next();
          
          locker.writeLock().lock();
          regions.remove(r.region.getRegionName());
          locker.writeLock().unlock();
 
          HRegion[] newRegions = null;
          try {
            Text oldRegion = r.region.getRegionName();
            
            LOG.info("splitting region: " + oldRegion);
            
            newRegions = r.region.closeAndSplit(r.midKey);

            // When a region is split, the META table needs to updated if we're
            // splitting a 'normal' region, and the ROOT table needs to be
            // updated if we are splitting a META region.

            Text tableToUpdate
              = (oldRegion.find(META_TABLE_NAME.toString()) == 0)
                ? ROOT_TABLE_NAME : META_TABLE_NAME;

            if(LOG.isDebugEnabled()) {
              LOG.debug("region split complete. updating meta");
            }
            
            client.openTable(tableToUpdate);
            long lockid = client.startUpdate(oldRegion);
            client.delete(lockid, COL_REGIONINFO);
            client.delete(lockid, COL_SERVER);
            client.delete(lockid, COL_STARTCODE);
            client.commit(lockid);
            
            for(int i = 0; i < newRegions.length; i++) {
              ByteArrayOutputStream bytes = new ByteArrayOutputStream();
              DataOutputStream out = new DataOutputStream(bytes);
              newRegions[i].getRegionInfo().write(out);
              
              lockid = client.startUpdate(newRegions[i].getRegionName());
              client.put(lockid, COL_REGIONINFO, bytes.toByteArray());
              client.commit(lockid);
            }
            
            // Now tell the master about the new regions
            
            if(LOG.isDebugEnabled()) {
              LOG.debug("reporting region split to master");
            }
            
            reportSplit(newRegions[0].getRegionInfo(), newRegions[1].getRegionInfo());
            
            LOG.info("region split successful. old region=" + oldRegion
                + ", new regions: " + newRegions[0].getRegionName() + ", "
                + newRegions[1].getRegionName());
            
            newRegions[0].close();
            newRegions[1].close();
            
          } catch(IOException e) {
            //TODO: What happens if this fails? Are we toast?
            e.printStackTrace();
            continue;
          }
        }
        
        // Sleep
        long waitTime = stopRequested ? 0
            : splitCheckFrequency - (System.currentTimeMillis() - startTime);
        if (waitTime > 0) {
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Sleep splitChecker");
            }
            Thread.sleep(waitTime);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Wake splitChecker");
            }
          } catch(InterruptedException iex) {
          }
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("splitChecker exiting");
      }
    }
  }
  
  // Cache flushing
  
  private Flusher cacheFlusher;
  private Thread cacheFlusherThread;
  private class Flusher implements Runnable {
    public void run() {
      while(! stopRequested) {
        long startTime = System.currentTimeMillis();

        // Grab a list of items to flush

        Vector<HRegion> toFlush = new Vector<HRegion>();
        locker.readLock().lock();
        try {
          toFlush.addAll(regions.values());
          
        } finally {
          locker.readLock().unlock();
        }

        // Flush them, if necessary

        for(Iterator<HRegion> it = toFlush.iterator(); it.hasNext(); ) {
          HRegion cur = it.next();
          
          try {
            cur.optionallyFlush();
            
          } catch(IOException iex) {
            iex.printStackTrace();
          }
        }

        // Sleep
        long waitTime = stopRequested ? 0
            : threadWakeFrequency - (System.currentTimeMillis() - startTime);
        
        if(waitTime > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Sleep cacheFlusher");
          }
          try {
            Thread.sleep(waitTime);
          } catch(InterruptedException iex) {
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Wake cacheFlusher");
          }
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("cacheFlusher exiting");
      }
    }
  }
  
  // File paths
  
  private FileSystem fs;
  private Path oldlogfile;
  
  // Logging
  
  private HLog log;
  private LogRoller logRoller;
  private Thread logRollerThread;
  private class LogRoller implements Runnable {
    public void run() {
      while(! stopRequested) {
        // If the number of log entries is high enough, roll the log.  This is a
        // very fast operation, but should not be done too frequently.
        if(log.getNumEntries() > maxLogEntries) {
          try {
            log.rollWriter();
          } catch(IOException iex) {
          }
        }

        if(!stopRequested) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Sleep logRoller");
          }
          try {
            Thread.sleep(threadWakeFrequency);
          } catch(InterruptedException iex) {
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Wake logRoller");
          }
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("logRoller exiting");
      }
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
      Configuration conf) 
  throws IOException {
    
    // Basic setup
    this.stopRequested = false;
    this.regionDir = regionDir;
    this.conf = conf;
    this.rand = new Random();
    this.regions = new TreeMap<Text, HRegion>();
    this.locker = new ReentrantReadWriteLock();
    this.outboundMsgs = new Vector<HMsg>();
    this.scanners =
      Collections.synchronizedMap(new TreeMap<Text, HInternalScannerInterface>());

    // Config'ed params
    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.maxLogEntries = conf.getInt("hbase.regionserver.maxlogentries", 30 * 1000);
    this.msgInterval = conf.getLong("hbase.regionserver.msginterval",
        15 * 1000);
    this.splitCheckFrequency =
      conf.getLong("hbase.regionserver.thread.splitcheckfrequency", 60 * 1000);
    
    // Cache flushing
    this.cacheFlusher = new Flusher();
    this.cacheFlusherThread =
      new Thread(cacheFlusher, "HRegionServer.cacheFlusher");
    
    // Check regions to see if they need to be split
    this.splitChecker = new SplitChecker();
    this.splitCheckerThread =
      new Thread(splitChecker, "HRegionServer.splitChecker");
    
    // Process requests from Master
    this.toDo = new Vector<HMsg>();
    this.worker = new Worker();
    this.workerThread = new Thread(worker, "HRegionServer.worker");

    try {
      // Server to handle client requests
      this.server = RPC.getServer(this, address.getBindAddress().toString(), 
        address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);

      this.address = new HServerAddress(server.getListenerAddress());

      // Local file paths
      String serverName =
        this.address.getBindAddress() + "_" + this.address.getPort();
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
      this.hbaseMaster = (HMasterRegionInterface)RPC.
        waitForProxy(HMasterRegionInterface.class,
        HMasterRegionInterface.versionID,
        new HServerAddress(conf.get(MASTER_ADDRESS)).getInetSocketAddress(),
        conf);

      // Threads
      this.workerThread.start();
      this.cacheFlusherThread.start();
      this.splitCheckerThread.start();
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
  public synchronized void stop() throws IOException {
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
    }
    try {
      this.logRollerThread.join();
    } catch(InterruptedException iex) {
    }
    try {
      this.cacheFlusherThread.join();
    } catch(InterruptedException iex) {
    }
    try {
      this.splitCheckerThread.join();
    } catch(InterruptedException iex) {
    }
    try {
      this.server.join();
    } catch(InterruptedException iex) {
    }
    LOG.info("server stopped at: " + address.toString());
  }
  
  /**
   * The HRegionServer sticks in this loop until closed. It repeatedly checks
   * in with the HMaster, sending heartbeats & reports, and receiving HRegion 
   * load/unload instructions.
   */
  public void run() {
    while(! stopRequested) {
      HServerInfo info = new HServerInfo(address, rand.nextLong());
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
          if (LOG.isDebugEnabled()) {
            LOG.debug("Sleep");
          }
          synchronized(this) {
            try {
              Thread.sleep(waitTime);
            } catch(InterruptedException iex) {
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Wake");
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
            e.printStackTrace();
          }
        }

        waitTime = stopRequested ? 0
            : msgInterval - (System.currentTimeMillis() - lastMsg);
        if (waitTime > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Sleep");
          }
          synchronized(this) {
            try {
              Thread.sleep(waitTime);
            } catch(InterruptedException iex) {
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Wake");
          }
        }
      }
    }
    try {
      LOG.info("stopping server at: " + address.toString());

      // Send interrupts to wake up threads if sleeping so they notice shutdown.

      this.logRollerThread.interrupt();
      this.cacheFlusherThread.interrupt();
      this.splitCheckerThread.interrupt();
      
      this.worker.stop();
      this.server.stop();

      closeAllRegions();
      log.close();
      leases.close();
      join();
      
    } catch(IOException e) {
      e.printStackTrace();
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
  private void reportSplit(HRegionInfo newRegionA, HRegionInfo newRegionB) {
    synchronized(outboundMsgs) {
      outboundMsgs.add(new HMsg(HMsg.MSG_NEW_REGION, newRegionA));
      outboundMsgs.add(new HMsg(HMsg.MSG_NEW_REGION, newRegionB));
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMaster-given operations
  //////////////////////////////////////////////////////////////////////////////

  private Vector<HMsg> toDo;
  private Worker worker;
  private Thread workerThread;
  private class Worker implements Runnable {
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

          case HMsg.MSG_REGION_CLOSE_AND_DELETE:
            if (LOG.isDebugEnabled()) {
              LOG.debug("MSG_REGION_CLOSE_AND_DELETE");
            }
            closeAndDeleteRegion(msg.getRegionInfo());
            break;

          default:
            throw new IOException("Impossible state during msg processing.  Instruction: " + msg);
          }
        } catch(IOException e) {
          e.printStackTrace();
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("worker thread exiting");
      }
    }
  }
  
  private void openRegion(HRegionInfo regionInfo) throws IOException {
    this.locker.writeLock().lock();
    try {
      HRegion region =
        new HRegion(regionDir, log, fs, conf, regionInfo, null, oldlogfile);
      regions.put(region.getRegionName(), region);
      reportOpen(region);
      
    } finally {
      this.locker.writeLock().unlock();
    }
  }

  private void closeRegion(HRegionInfo info, boolean reportWhenCompleted)
      throws IOException {
    
    this.locker.writeLock().lock();
    try {
      HRegion region = regions.remove(info.regionName);
      
      if(region != null) {
        region.close();
        
        if(reportWhenCompleted) {
          reportClose(region);
        }
      }
      
    } finally {
      this.locker.writeLock().unlock();
    }
  }

  private void closeAndDeleteRegion(HRegionInfo info) throws IOException {

    this.locker.writeLock().lock();
    try {
      HRegion region = regions.remove(info.regionName);
  
      if(region != null) {
        region.closeAndDelete();
      }
  
    } finally {
      this.locker.writeLock().unlock();
    }
  }

  /** Called either when the master tells us to restart or from stop() */
  private void closeAllRegions() {
    this.locker.writeLock().lock();
    try {
      for(Iterator<HRegion> it = regions.values().iterator(); it.hasNext(); ) {
        HRegion region = it.next();
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing region " + region.getRegionName());
        }
        try {
          region.close();
          
        } catch(IOException e) {
          e.printStackTrace();
        }
      }
      regions.clear();
      
    } finally {
      this.locker.writeLock().unlock();
    }
  }

  /*****************************************************************************
   * TODO - Figure out how the master is to determine when regions should be
   *        merged. It once it makes this determination, it needs to ensure that
   *        the regions to be merged are first being served by the same
   *        HRegionServer and if not, move them so they are.
   *        
   *        For now, we do not do merging. Splits are driven by the HRegionServer.
   ****************************************************************************/
/*
  private void mergeRegions(Text regionNameA, Text regionNameB) throws IOException {
    locking.obtainWriteLock();
    try {
      HRegion srcA = regions.remove(regionNameA);
      HRegion srcB = regions.remove(regionNameB);
      HRegion newRegion = HRegion.closeAndMerge(srcA, srcB);
      regions.put(newRegion.getRegionName(), newRegion);

      reportClose(srcA);
      reportClose(srcB);
      reportOpen(newRegion);
      
    } finally {
      locking.releaseWriteLock();
    }
  }
*/

  //////////////////////////////////////////////////////////////////////////////
  // HRegionInterface
  //////////////////////////////////////////////////////////////////////////////

  /** Obtain a table descriptor for the given region */
  public HRegionInfo getRegionInfo(Text regionName) {
    HRegion region = getRegion(regionName);
    if(region == null) {
      return null;
    }
    return region.getRegionInfo();
  }

  /** Get the indicated row/column */
  public BytesWritable get(Text regionName, Text row, Text column) throws IOException {
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("get " + row.toString() + ", " + column.toString());
    }
    BytesWritable results = region.get(row, column);
    if(results != null) {
      return results;
    }
    return null;
  }

  /** Get multiple versions of the indicated row/col */
  public BytesWritable[] get(Text regionName, Text row, Text column, 
      int numVersions) throws IOException {
    
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    BytesWritable[] results = region.get(row, column, numVersions);
    if(results != null) {
      return results;
    }
    return null;
  }

  /** Get multiple timestamped versions of the indicated row/col */
  public BytesWritable[] get(Text regionName, Text row, Text column, 
      long timestamp, int numVersions) throws IOException {
    
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    BytesWritable[] results = region.get(row, column, timestamp, numVersions);
    if(results != null) {
      return results;
    }
    return null;
  }

  /** Get all the columns (along with their names) for a given row. */
  public LabelledData[] getRow(Text regionName, Text row) throws IOException {
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    TreeMap<Text, BytesWritable> map = region.getFull(row);
    LabelledData result[] = new LabelledData[map.size()];
    int counter = 0;
    for(Iterator<Text> it = map.keySet().iterator(); it.hasNext(); ) {
      Text colname = it.next();
      BytesWritable val = map.get(colname);
      result[counter++] = new LabelledData(colname, val);
    }
    return result;
  }

  /**
   * Start an update to the HBase.  This also creates a lease associated with
   * the caller.
   */
  private class RegionListener extends LeaseListener {
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
        iex.printStackTrace();
      }
    }
  }
  
  public long startUpdate(Text regionName, long clientid, Text row) 
      throws IOException {
    
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    long lockid = region.startUpdate(row);
    leases.createLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)), 
        new RegionListener(region, lockid));
    
    return lockid;
  }

  /** Add something to the HBase. */
  public void put(Text regionName, long clientid, long lockid, Text column, 
      BytesWritable val) throws IOException {
    
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    leases.renewLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
    
    region.put(lockid, column, val);
  }

  /** Remove a cell from the HBase. */
  public void delete(Text regionName, long clientid, long lockid, Text column) 
      throws IOException {
    
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    leases.renewLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
    
    region.delete(lockid, column);
  }

  /** Abandon the transaction */
  public void abort(Text regionName, long clientid, long lockid) 
      throws IOException {
    
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    leases.cancelLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
    
    region.abort(lockid);
  }

  /** Confirm the transaction */
  public void commit(Text regionName, long clientid, long lockid) 
      throws IOException {
    
    HRegion region = getRegion(regionName);
    if(region == null) {
      throw new IOException("Not serving region " + regionName);
    }
    
    leases.cancelLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
    
    region.commit(lockid);
  }

  /** Don't let the client's lease expire just yet...  */
  public void renewLease(long lockid, long clientid) throws IOException {
    leases.renewLease(new Text(String.valueOf(clientid)), 
        new Text(String.valueOf(lockid)));
  }

  /** Private utility method for safely obtaining an HRegion handle. */
  private HRegion getRegion(Text regionName) {
    this.locker.readLock().lock();
    try {
      return regions.get(regionName);
      
    } finally {
      this.locker.readLock().unlock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // remote scanner interface
  //////////////////////////////////////////////////////////////////////////////

  private Map<Text, HInternalScannerInterface> scanners;
  private class ScannerListener extends LeaseListener {
    private Text scannerName;
    
    public ScannerListener(Text scannerName) {
      this.scannerName = scannerName;
    }
    
    public void leaseExpired() {
      HInternalScannerInterface s = scanners.remove(scannerName);
      if(s != null) {
        try {
          s.close();
          
        } catch(IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  /** Start a scanner for a given HRegion. */
  public long openScanner(Text regionName, Text[] cols, Text firstRow)
      throws IOException {

    HRegion r = getRegion(regionName);
    if(r == null) {
      throw new IOException("Not serving region " + regionName);
    }

    long scannerId = -1L;
    try {
      HInternalScannerInterface s = r.getScanner(cols, firstRow);
      scannerId = rand.nextLong();
      Text scannerName = new Text(String.valueOf(scannerId));
      scanners.put(scannerName, s);
      leases.createLease(scannerName, scannerName, new ScannerListener(scannerName));
    
    } catch(IOException e) {
      e.printStackTrace();
      throw e;
    }
    return scannerId;
  }
  
  public LabelledData[] next(long scannerId, HStoreKey key) throws IOException {
    
    Text scannerName = new Text(String.valueOf(scannerId));
    HInternalScannerInterface s = scanners.get(scannerName);
    if(s == null) {
      throw new IOException("unknown scanner");
    }
    leases.renewLease(scannerName, scannerName);
    TreeMap<Text, BytesWritable> results = new TreeMap<Text, BytesWritable>();
    ArrayList<LabelledData> values = new ArrayList<LabelledData>();
    if(s.next(key, results)) {
      for(Iterator<Map.Entry<Text, BytesWritable>> it
          = results.entrySet().iterator();
          it.hasNext(); ) {
        
        Map.Entry<Text, BytesWritable> e = it.next();
        BytesWritable val = e.getValue();
        if(val.getSize() == DELETE_BYTES.getSize()
            && val.compareTo(DELETE_BYTES) == 0) {
            
          // Value is deleted. Don't return a value
          
          continue;

        } else {
          values.add(new LabelledData(e.getKey(), val));
        }
      }
    }
    return values.toArray(new LabelledData[values.size()]);
  }
  
  public void close(long scannerId) throws IOException {
    Text scannerName = new Text(String.valueOf(scannerId));
    HInternalScannerInterface s = scanners.remove(scannerName);
    if(s == null) {
      throw new IOException("unknown scanner");
    }
    try {
      s.close();
        
    } catch(IOException ex) {
      ex.printStackTrace();
    }
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
  
  public static void main(String [] args) throws IOException {
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