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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.KeyedData;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.StringUtils;


/**
 * HMaster is the "master server" for a HBase.
 * There is only one HMaster for a single HBase deployment.
 */
public class HMaster implements HConstants, HMasterInterface, 
    HMasterRegionInterface, Runnable {

  /**
   * {@inheritDoc}
   */
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

  static final Log LOG =
    LogFactory.getLog(org.apache.hadoop.hbase.HMaster.class.getName());
  
  volatile boolean closed;
  Path dir;
  Configuration conf;
  FileSystem fs;
  Random rand;
  long threadWakeFrequency; 
  int numRetries;
  long maxRegionOpenTime;
  
  BlockingQueue<PendingOperation> msgQueue;
  
  private Leases serverLeases;
  private Server server;
  private HServerAddress address;
  
  HClient client;
 
  long metaRescanInterval;
  
  HServerAddress rootRegionLocation;
  
  /**
   * Columns in the 'meta' ROOT and META tables.
   */
  static final Text METACOLUMNS[] = {
      COLUMN_FAMILY
  };
  

  boolean rootScanned;
  int numMetaRegions;

  /**
   * Base HRegion scanner class. Holds utilty common to <code>ROOT</code> and
   * <code>META</code> HRegion scanners.
   * 
   * <p>How do we know if all regions are assigned? After the initial scan of
   * the <code>ROOT</code> and <code>META</code> regions, all regions known at
   * that time will have been or are in the process of being assigned.</p>
   * 
   * <p>When a region is split the region server notifies the master of the
   * split and the new regions are assigned. But suppose the master loses the
   * split message? We need to periodically rescan the <code>ROOT</code> and
   * <code>META</code> regions.
   *    <ul>
   *    <li>If we rescan, any regions that are new but not assigned will have
   *    no server info. Any regions that are not being served by the same
   *    server will get re-assigned.</li>
   *      
   *    <li>Thus a periodic rescan of the root region will find any new
   *    <code>META</code> regions where we missed the <code>META</code> split
   *    message or we failed to detect a server death and consequently need to
   *    assign the region to a new server.</li>
   *        
   *    <li>if we keep track of all the known <code>META</code> regions, then
   *    we can rescan them periodically. If we do this then we can detect any
   *    regions for which we missed a region split message.</li>
   *    </ul>
   *    
   * Thus just keeping track of all the <code>META</code> regions permits
   * periodic rescanning which will detect unassigned regions (new or
   * otherwise) without the need to keep track of every region.</p>
   * 
   * <p>So the <code>ROOT</code> region scanner needs to wake up:
   * <ol>
   * <li>when the master receives notification that the <code>ROOT</code>
   * region has been opened.</li>
   * <li>periodically after the first scan</li>
   * </ol>
   * 
   * The <code>META</code>  scanner needs to wake up:
   * <ol>
   * <li>when a <code>META</code> region comes on line</li>
   * </li>periodically to rescan the known <code>META</code> regions</li>
   * </ol>
   * 
   * <p>A <code>META</code> region is not 'known' until it has been scanned
   * once.
   */
  abstract class BaseScanner implements Runnable {
    private final Text FIRST_ROW = new Text();
    
    /**
     * @param region Region to scan
     * @return True if scan completed.
     * @throws IOException
     */
    protected boolean scanRegion(final MetaRegion region)
    throws IOException {
      boolean scannedRegion = false;
      HRegionInterface regionServer = null;
      long scannerId = -1L;
      if (LOG.isDebugEnabled()) {
        LOG.debug(Thread.currentThread().getName() + " scanning meta region " +
          region.regionName);
      }

      try {
        regionServer = client.getHRegionConnection(region.server);
        scannerId = regionServer.openScanner(region.regionName, METACOLUMNS,
          FIRST_ROW, System.currentTimeMillis(), null);

        while (true) {
          TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          KeyedData[] values = regionServer.next(scannerId);
          if (values.length == 0) {
            break;
          }

          for (int i = 0; i < values.length; i++) {
            results.put(values[i].getKey().getColumn(), values[i].getData());
          }

          HRegionInfo info = HRegion.getRegionInfo(results);
          String serverName = HRegion.getServerName(results);
          long startCode = HRegion.getStartCode(results);

          if(LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + " scanner: " +
                Long.valueOf(scannerId) + " regioninfo: {" + info.toString() +
                "}, server: " + serverName + ", startCode: " + startCode);
          }

          // Note Region has been assigned.
          checkAssigned(info, serverName, startCode);
          scannedRegion = true;
        }

      } catch (UnknownScannerException e) {
        // Reset scannerId so we do not try closing a scanner the other side
        // has lost account of: prevents duplicated stack trace out of the 
        // below close in the finally.
        scannerId = -1L;

      } finally {
        try {
          if (scannerId != -1L) {
            if (regionServer != null) {
              regionServer.close(scannerId);
            }
          }
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          LOG.error(e);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(Thread.currentThread().getName() + " scan of meta region " +
          region.regionName + " complete");
      }
      return scannedRegion;
    }
    
    protected void checkAssigned(final HRegionInfo info,
        final String serverName, final long startCode) {
      // Skip region - if ...
      if(info.offLine                                           // offline
          || killedRegions.contains(info.regionName)            // queued for offline
          || regionsToDelete.contains(info.regionName)) {       // queued for delete

        unassignedRegions.remove(info.regionName);
        assignAttempts.remove(info.regionName);
        
        if(LOG.isDebugEnabled()) {
          LOG.debug("not assigning region: " + info.regionName);
        }
        return;
      }
      
      HServerInfo storedInfo = null;
      if(serverName != null) {
        TreeMap<Text, HRegionInfo> regionsToKill = killList.get(serverName);
        if(regionsToKill != null && regionsToKill.containsKey(info.regionName)) {
          // Skip if region is on kill list
          
          if(LOG.isDebugEnabled()) {
            LOG.debug("not assigning region (on kill list): " + info.regionName);
          }
          return;
        }
        storedInfo = serversToServerInfo.get(serverName);
      }
      if( !(
          unassignedRegions.containsKey(info.regionName)
          || pendingRegions.contains(info.regionName)
          )
          && (storedInfo == null
              || storedInfo.getStartCode() != startCode)) {
                  
        if(LOG.isDebugEnabled()) {
          LOG.debug("region unassigned: " + info.regionName
              + " serverName: " + serverName
              + (storedInfo == null ? " storedInfo == null"
                  : (" startCode=" + startCode + ", storedStartCode="
                      + storedInfo.getStartCode())));
        }

        // The current assignment is no good; load the region.
        
        unassignedRegions.put(info.regionName, info);
        assignAttempts.put(info.regionName, Long.valueOf(0L));
      }
    }
  }
  
  /**
   * Scanner for the <code>ROOT</code> HRegion.
   */
  class RootScanner extends BaseScanner {
    /**
     * {@inheritDoc}
     */
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running ROOT scanner");
      }
      int tries = 0;
      while(!closed && tries < numRetries) {
        try {
          // rootRegionLocation will be filled in when we get an 'open region'
          // regionServerReport message from the HRegionServer that has been
          // allocated the ROOT region below.  If we get back false, then
          // HMaster has closed.
          if (waitForRootRegionOrClose()) {
            continue;
          }
          synchronized(rootScannerLock) { // Don't interrupt us while we're working
            rootScanned = false;
            // Make a MetaRegion instance for ROOT region to pass scanRegion.
            MetaRegion mr = new MetaRegion();
            mr.regionName = HGlobals.rootRegionInfo.regionName;
            mr.server = HMaster.this.rootRegionLocation;
            mr.startKey = null;
            if (scanRegion(mr)) {
              numMetaRegions += 1;
            }
            rootScanned = true;
          }
          tries = 0;

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            try {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
              
            } catch (IOException ex) {
              LOG.warn(ex);
            }
          }
          tries++;
          if(tries < numRetries) {
            LOG.warn("ROOT scanner", e);
            
          } else {
            LOG.error("ROOT scanner", e);
            closed = true;
            break;
          }
        }
        try {
          Thread.sleep(metaRescanInterval);
        } catch(InterruptedException e) {
          // Catch and go around again. If interrupt, its spurious or we're
          // being shutdown.  Go back up to the while test.
        }
      }
      LOG.info("ROOT scanner exiting");
    }
  }
  
  private RootScanner rootScanner;
  private Thread rootScannerThread;
  Integer rootScannerLock = new Integer(0);

  @SuppressWarnings("unchecked")
  static class MetaRegion implements Comparable {
    HServerAddress server;
    Text regionName;
    Text startKey;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
      return this.compareTo(o) == 0;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      int result = this.regionName.hashCode();
      result ^= this.startKey.hashCode();
      return result;
    }

    // Comparable

    /**
     * {@inheritDoc}
     */
    public int compareTo(Object o) {
      MetaRegion other = (MetaRegion)o;
      
      int result = this.regionName.compareTo(other.regionName);
      if(result == 0) {
        result = this.startKey.compareTo(other.startKey);
      }
      return result;
    }
    
  }
  
  /** Work for the meta scanner is queued up here */
  Vector<MetaRegion> metaRegionsToScan;

  SortedMap<Text, MetaRegion> knownMetaRegions;
  
  boolean allMetaRegionsScanned;
  
  /**
   * MetaScanner <code>META</code> table.
   * 
   * When a <code>META</code> server comes on line, a MetaRegion object is
   * queued up by regionServerReport() and this thread wakes up.
   *
   * It's important to do this work in a separate thread, or else the blocking 
   * action would prevent other work from getting done.
   */
  class MetaScanner extends BaseScanner {
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("null")
    public void run() {
      while (!closed) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running META scanner");
        }
        MetaRegion region = null;
        while (region == null && !closed) {
          synchronized (metaRegionsToScan) {
            if (metaRegionsToScan.size() != 0) {
              region = metaRegionsToScan.remove(0);
            }
            if (region == null) {
              try {
                metaRegionsToScan.wait(threadWakeFrequency);
              } catch (InterruptedException e) {
                // Catch and go around again.  We've been woken because there
                // are new meta regions available or because we are being
                // shut down.
              }
            }
          }
        }
        if (closed) {
          continue;
        }
        try {
          synchronized(metaScannerLock) { // Don't interrupt us while we're working
            scanRegion(region);
            knownMetaRegions.put(region.startKey, region);
            if (rootScanned && knownMetaRegions.size() == numMetaRegions) {
              if(LOG.isDebugEnabled()) {
                LOG.debug("all meta regions scanned");
              }
              allMetaRegionsScanned = true;
              metaRegionsScanned();
            }
          }

          int tries = 0;
          do {
            try {
              Thread.sleep(metaRescanInterval);
            } catch(InterruptedException ex) {
              // Catch and go around again.
            }
            if(!allMetaRegionsScanned         // A meta region must have split
                || closed) {                  // We're shutting down
              break;
            }

            try {
              
              // Rescan the known meta regions every so often
              synchronized(metaScannerLock) { // Don't interrupt us while we're working
                Vector<MetaRegion> v = new Vector<MetaRegion>();
                v.addAll(knownMetaRegions.values());
                for(Iterator<MetaRegion> i = v.iterator(); i.hasNext(); ) {
                  scanRegion(i.next());
                }
              }
              tries = 0;
              
            } catch (IOException e) {
              if (e instanceof RemoteException) {
                e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
              }
              tries++;
              if(tries < numRetries) {
                LOG.warn("META scanner", e);
                
              } else {
                throw e;
              }
            }
          } while(true);

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            try {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
              
            } catch (IOException ex) {
              LOG.warn(ex);
            }
          }
          LOG.error("META scanner", e);
          closed = true;
        }
      }
      LOG.info("META scanner exiting");
    }

    /**
     * Called by the meta scanner when it has completed scanning all meta 
     * regions. This wakes up any threads that were waiting for this to happen.
     */
    private synchronized void metaRegionsScanned() {
      notifyAll();
    }
    
    /**
     * Other threads call this method to wait until all the meta regions have
     * been scanned.
     */
    synchronized boolean waitForMetaScanOrClose() {
      while(!closed && !allMetaRegionsScanned) {
        try {
          wait(threadWakeFrequency);
        } catch(InterruptedException e) {
          // continue
        }
      }
      return closed;
    }
  }

  MetaScanner metaScanner;
  private Thread metaScannerThread;
  Integer metaScannerLock = new Integer(0);

  /**
   * The 'unassignedRegions' table maps from a region name to a HRegionInfo 
   * record, which includes the region's table, its id, and its start/end keys.
   * 
   * We fill 'unassignedRecords' by scanning ROOT and META tables, learning the
   * set of all known valid regions.
   */
  SortedMap<Text, HRegionInfo> unassignedRegions;

  /**
   * The 'assignAttempts' table maps from regions to a timestamp that indicates
   * the last time we *tried* to assign the region to a RegionServer. If the 
   * timestamp is out of date, then we can try to reassign it.
   */
  SortedMap<Text, Long> assignAttempts;

  /**
   * Regions that have been assigned, and the server has reported that it has
   * started serving it, but that we have not yet recorded in the meta table.
   */
  SortedSet<Text> pendingRegions;
  
  /**
   * The 'killList' is a list of regions that are going to be closed, but not
   * reopened.
   */
  SortedMap<String, TreeMap<Text, HRegionInfo>> killList;
  
  /** 'killedRegions' contains regions that are in the process of being closed */
  SortedSet<Text> killedRegions;

  /**
   * 'regionsToDelete' contains regions that need to be deleted, but cannot be
   * until the region server closes it
   */
  SortedSet<Text> regionsToDelete;
  
  /** The map of known server names to server info */
  SortedMap<String, HServerInfo> serversToServerInfo =
    Collections.synchronizedSortedMap(new TreeMap<String, HServerInfo>());

  /** Build the HMaster out of a raw configuration item.
   * 
   * @param conf - Configuration object
   * @throws IOException
   */
  public HMaster(Configuration conf) throws IOException {
    this(new Path(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR)),
        new HServerAddress(conf.get(MASTER_ADDRESS, DEFAULT_MASTER_ADDRESS)),
        conf);
  }

  /** 
   * Build the HMaster
   * @param dir         - base directory
   * @param address     - server address and port number
   * @param conf        - configuration
   * 
   * @throws IOException
   */
  public HMaster(Path dir, HServerAddress address, Configuration conf)
  throws IOException {
    this.closed = true;
    this.dir = dir;
    this.conf = conf;
    this.fs = FileSystem.get(conf);
    this.rand = new Random();

    // Make sure the root directory exists!
    
    if(! fs.exists(dir)) {
      fs.mkdirs(dir);
    }

    Path rootRegionDir =
      HStoreFile.getHRegionDir(dir, HGlobals.rootRegionInfo.regionName);
    LOG.info("Root region dir: " + rootRegionDir.toString());
    if(! fs.exists(rootRegionDir)) {
      LOG.info("bootstrap: creating ROOT and first META regions");
      try {
        HRegion root = HRegion.createHRegion(0L, HGlobals.rootTableDesc,
          this.dir, this.conf);
        HRegion meta = HRegion.createHRegion(1L, HGlobals.metaTableDesc,
            this.dir, this.conf);
        // Add first region from the META table to the ROOT region.
        HRegion.addRegionToMETA(root, meta);
        root.close();
        root.getLog().closeAndDelete();
        meta.close();
        meta.getLog().closeAndDelete();
      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        LOG.error(e);
      }
    }

    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.maxRegionOpenTime = conf.getLong("hbase.hbasemaster.maxregionopen", 30 * 1000);
    this.msgQueue = new LinkedBlockingQueue<PendingOperation>();
    this.serverLeases = new Leases(
      conf.getLong("hbase.master.lease.period", 30 * 1000), 
      conf.getLong("hbase.master.lease.thread.wakefrequency", 15 * 1000));
    this.server = RPC.getServer(this, address.getBindAddress(),
      address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
      false, conf);

    //  The rpc-server port can be ephemeral... ensure we have the correct info
    this.address = new HServerAddress(server.getListenerAddress());
    conf.set(MASTER_ADDRESS, address.toString());
    
    this.client = new HClient(conf);
    
    this.metaRescanInterval
      = conf.getLong("hbase.master.meta.thread.rescanfrequency", 60 * 1000);

    // The root region
    
    this.rootRegionLocation = null;
    this.rootScanned = false;
    this.rootScanner = new RootScanner();
    this.rootScannerThread = new Thread(rootScanner, "HMaster.rootScanner");
    
    // Scans the meta table

    this.numMetaRegions = 0;
    this.metaRegionsToScan = new Vector<MetaRegion>();
    
    this.knownMetaRegions = 
      Collections.synchronizedSortedMap(new TreeMap<Text, MetaRegion>());
    
    this.allMetaRegionsScanned = false;

    this.metaScanner = new MetaScanner();
    this.metaScannerThread = new Thread(metaScanner, "HMaster.metaScanner");

    this.unassignedRegions = 
      Collections.synchronizedSortedMap(new TreeMap<Text, HRegionInfo>());
    
    this.unassignedRegions.put(HGlobals.rootRegionInfo.regionName, HGlobals.rootRegionInfo);
    
    this.assignAttempts = 
      Collections.synchronizedSortedMap(new TreeMap<Text, Long>());
    
    this.pendingRegions =
      Collections.synchronizedSortedSet(new TreeSet<Text>());
    
    this.assignAttempts.put(HGlobals.rootRegionInfo.regionName,
      Long.valueOf(0L));

    this.killList = 
      Collections.synchronizedSortedMap(
          new TreeMap<String, TreeMap<Text, HRegionInfo>>());
    
    this.killedRegions =
      Collections.synchronizedSortedSet(new TreeSet<Text>());
    
    this.regionsToDelete =
      Collections.synchronizedSortedSet(new TreeSet<Text>());
    
    // We're almost open for business
    this.closed = false;
    LOG.info("HMaster initialized on " + this.address.toString());
  }
  
  /** 
   * @return HServerAddress of the master server
   */
  public HServerAddress getMasterAddress() {
    return address;
  }

  /** Main processing loop */
  public void run() {
    Thread.currentThread().setName("HMaster");
    try { 
      // Start things up
      this.rootScannerThread.start();
      this.metaScannerThread.start();

      // Start the server last so everything else is running before we start
      // receiving requests
      this.server.start();
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        try {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          
        } catch (IOException ex) {
          LOG.warn(ex);
        }
      }
      // Something happened during startup. Shut things down.
      this.closed = true;
      LOG.error(e);
    }

    // Main processing loop
    for (PendingOperation op = null; !closed; ) {
      try {
        op = msgQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // continue
      }
      if (op == null || closed) {
        continue;
      }
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Processing " + op.toString());
        }
        op.process();
        
      } catch (Exception ex) {
        if (ex instanceof RemoteException) {
          try {
            ex = RemoteExceptionHandler.decodeRemoteException((RemoteException) ex);
            
          } catch (IOException e) {
            LOG.warn(e);
          }
        }
        LOG.warn(ex);
        try {
          msgQueue.put(op);
        } catch (InterruptedException e) {
          throw new RuntimeException("Putting into msgQueue was interrupted.", e);
        }
      }
    }
    letRegionServersShutdown();
    
    /*
     * Clean up and close up shop
     */

    // Wake other threads so they notice the close

    synchronized(rootScannerLock) {
      rootScannerThread.interrupt();
    }
    synchronized(metaScannerLock) {
      metaScannerThread.interrupt();
    }
    server.stop();                              // Stop server
    serverLeases.close();                       // Turn off the lease monitor
    
    // Join up with all threads
    
    try {
      // Wait for the root scanner to finish.
      rootScannerThread.join();
    } catch (Exception iex) {
      // Print if ever there is an interrupt (Just for kicks. Remove if it
      // ever happens).
      LOG.warn(iex);
    }
    try {
      // Join the thread till it finishes.
      metaScannerThread.join();
    } catch(Exception iex) {
      // Print if ever there is an interrupt (Just for kicks. Remove if it
      // ever happens).
      LOG.warn(iex);
    }
    try {
      // Join until its finished.  TODO: Maybe do in parallel in its own thread
      // as is done in TaskTracker if its taking a long time to go down.
      server.join();
    } catch(InterruptedException iex) {
      // Print if ever there is an interrupt (Just for kicks. Remove if it
      // ever happens).
      LOG.warn(iex);
    }
    
    LOG.info("HMaster main thread exiting");
  }
  
  /*
   * Wait on regionservers to report in
   * with {@link #regionServerReport(HServerInfo, HMsg[])} so they get notice
   * the master is going down.  Waits until all region servers come back with
   * a MSG_REGIONSERVER_STOP which will cancel their lease or until leases held
   * by remote region servers have expired.
   */
  private void letRegionServersShutdown() {
    while (this.serversToServerInfo.size() > 0) {
      LOG.info("Waiting on following regionserver(s) to go down (or " +
        "region server lease expiration, whichever happens first): " +
        this.serversToServerInfo.values());
      try {
        Thread.sleep(threadWakeFrequency);
      } catch (InterruptedException e) {
        // continue
      }
    }
  }
  
  /**
   * Wait until <code>rootRegionLocation</code> has been set or until the
   * <code>closed</code> flag has been set.
   * @return True if <code>rootRegionLocation</code> was populated.
   */
  synchronized boolean waitForRootRegionOrClose() {
    while (!closed && rootRegionLocation == null) {
      try {
        wait(threadWakeFrequency);
      } catch(InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wake from wait for root region (or close) (IE)");
        }
      }
    }
    return this.rootRegionLocation == null;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // HMasterRegionInterface
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unused")
  public void regionServerStartup(HServerInfo serverInfo)
  throws IOException {
    String s = serverInfo.getServerAddress().toString().trim();
    HServerInfo storedInfo = null;
    LOG.info("received start message from: " + s);
    
    // If we get the startup message but there's an old server by that
    // name, then we can timeout the old one right away and register
    // the new one.
    storedInfo = serversToServerInfo.remove(s);
    if (storedInfo != null && !closed) {
      try {
        msgQueue.put(new PendingServerShutdown(storedInfo));
      } catch (InterruptedException e) {
        throw new RuntimeException("Putting into msgQueue was interrupted.", e);
      }
    }

    // Either way, record the new server
    serversToServerInfo.put(s, serverInfo);
    if(!closed) {
      long serverLabel = getServerLabel(s);
      LOG.debug("Created lease for " + serverLabel);
      serverLeases.createLease(serverLabel, serverLabel, new ServerExpirer(s));
    }
  }
  
  private long getServerLabel(final String s) {
    return s.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[])
  throws IOException {
    String s = serverInfo.getServerAddress().toString().trim();
    long serverLabel = getServerLabel(s);
    
    if (msgs.length > 0 && msgs[0].getMsg() == HMsg.MSG_REPORT_EXITING) {
      // HRegionServer is shutting down. Cancel the server's lease.
      LOG.debug("Region server " + s + ": MSG_REPORT_EXITING");
      cancelLease(s, serverLabel);
      
      // Get all the regions the server was serving reassigned (if we
      // are not shutting down).
      if (!closed) {
        for (int i = 1; i < msgs.length; i++) {
          HRegionInfo info = msgs[i].getRegionInfo();
          if (info.tableDesc.getName().equals(ROOT_TABLE_NAME)) {
            rootRegionLocation = null;
          } else if (info.tableDesc.getName().equals(META_TABLE_NAME)) {
            allMetaRegionsScanned = false;
          }
          unassignedRegions.put(info.regionName, info);
          assignAttempts.put(info.regionName, Long.valueOf(0L));
        }
      }
      
      // We don't need to return anything to the server because it isn't
      // going to do any more work.
      return new HMsg[0];
    }
    
    if (closed) {
      // Tell server to shut down if we are shutting down.  This should
      // happen after check of MSG_REPORT_EXITING above, since region server
      // will send us one of these messages after it gets MSG_REGIONSERVER_STOP
      return HMsg.MSG_REGIONSERVER_STOP_IN_ARRAY;
    }

    HServerInfo storedInfo = serversToServerInfo.get(s);
    if(storedInfo == null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("received server report from unknown server: " + s);
      }

      // The HBaseMaster may have been restarted.
      // Tell the RegionServer to start over and call regionServerStartup()
      HMsg returnMsgs[] = new HMsg[1];
      returnMsgs[0] = new HMsg(HMsg.MSG_CALL_SERVER_STARTUP);
      return returnMsgs;
    } else if(storedInfo.getStartCode() != serverInfo.getStartCode()) {

      // This state is reachable if:
      //
      // 1) RegionServer A started
      // 2) RegionServer B started on the same machine, then 
      //    clobbered A in regionServerStartup.
      // 3) RegionServer A returns, expecting to work as usual.
      //
      // The answer is to ask A to shut down for good.

      if(LOG.isDebugEnabled()) {
        LOG.debug("region server race condition detected: " + s);
      }

      return HMsg.MSG_REGIONSERVER_STOP_IN_ARRAY;
    } else {

      // All's well.  Renew the server's lease.
      // This will always succeed; otherwise, the fetch of serversToServerInfo
      // would have failed above.
      
      serverLeases.renewLease(serverLabel, serverLabel);

      // Refresh the info object
      serversToServerInfo.put(s, serverInfo);

      // Next, process messages for this server
      return processMsgs(serverInfo, msgs);
    }
  }

  /** cancel a server's lease */
  private void cancelLease(final String serverName, final long serverLabel)
  throws IOException {
    if (serversToServerInfo.remove(serverName) != null) {
      // Only cancel lease once.
      // This method can be called a couple of times during shutdown.
      LOG.debug("Cancelling lease for " + serverName);
      serverLeases.cancelLease(serverLabel, serverLabel);
    }
  }
  
  /** Process all the incoming messages from a server that's contacted us. */
  private HMsg[] processMsgs(HServerInfo info, HMsg incomingMsgs[]) throws IOException {
    Vector<HMsg> returnMsgs = new Vector<HMsg>();
    
    TreeMap<Text, HRegionInfo> regionsToKill =
      killList.remove(info.getServerAddress().toString());
    
    // Get reports on what the RegionServer did.
    
    for(int i = 0; i < incomingMsgs.length; i++) {
      HRegionInfo region = incomingMsgs[i].getRegionInfo();

      switch(incomingMsgs[i].getMsg()) {

      case HMsg.MSG_REPORT_OPEN:
        HRegionInfo regionInfo = unassignedRegions.get(region.regionName);

        if(regionInfo == null) {

          if(LOG.isDebugEnabled()) {
            LOG.debug("region server " + info.getServerAddress().toString()
                + " should not have opened region " + region.regionName);
          }

          // This Region should not have been opened.
          // Ask the server to shut it down, but don't report it as closed.  
          // Otherwise the HMaster will think the Region was closed on purpose, 
          // and then try to reopen it elsewhere; that's not what we want.

          returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT, region)); 

        } else {

          if(LOG.isDebugEnabled()) {
            LOG.debug(info.getServerAddress().toString() + " serving "
                + region.regionName);
          }

          // Note that it has been assigned and is waiting for the meta table
          // to be updated.
          
          pendingRegions.add(region.regionName);
          
          // Remove from unassigned list so we don't assign it to someone else

          unassignedRegions.remove(region.regionName);
          assignAttempts.remove(region.regionName);

          if(region.regionName.compareTo(HGlobals.rootRegionInfo.regionName) == 0) {

            // Store the Root Region location (in memory)

            rootRegionLocation = new HServerAddress(info.getServerAddress());

            // Wake up threads waiting for the root server

            rootRegionIsAvailable();
            break;

          } else if(region.tableDesc.getName().equals(META_TABLE_NAME)) {

            // It's a meta region. Put it on the queue to be scanned.

            MetaRegion r = new MetaRegion();
            r.server = info.getServerAddress();
            r.regionName = region.regionName;
            r.startKey = region.startKey;

            synchronized(metaRegionsToScan) {
              metaRegionsToScan.add(r);
              metaRegionsToScan.notifyAll();
            }
          }

          // Queue up an update to note the region location.

          try {
            msgQueue.put(new PendingOpenReport(info, region));
          } catch (InterruptedException e) {
            throw new RuntimeException("Putting into msgQueue was interrupted.", e);
          }
        }
        break;

      case HMsg.MSG_REPORT_CLOSE:
        if(LOG.isDebugEnabled()) {
          LOG.debug(info.getServerAddress().toString() + " no longer serving "
              + region.regionName);
        }

        if(region.regionName.compareTo(HGlobals.rootRegionInfo.regionName) == 0) { // Root region
          rootRegionLocation = null;
          unassignedRegions.put(region.regionName, region);
          assignAttempts.put(region.regionName, Long.valueOf(0L));

        } else {
          boolean reassignRegion = true;
          boolean deleteRegion = false;

          if(killedRegions.remove(region.regionName)) {
            reassignRegion = false;
          }
            
          if(regionsToDelete.remove(region.regionName)) {
            reassignRegion = false;
            deleteRegion = true;
          }
          unassignedRegions.remove(region.regionName);
          assignAttempts.remove(region.regionName);

          try {
            msgQueue.put(new PendingCloseReport(region, reassignRegion, deleteRegion));
          } catch (InterruptedException e) {
            throw new RuntimeException("Putting into msgQueue was interrupted.", e);
          }

          // NOTE: we cannot put the region into unassignedRegions as that
          //       could create a race with the pending close if it gets 
          //       reassigned before the close is processed.

        }
        break;

      case HMsg.MSG_NEW_REGION:
        if(LOG.isDebugEnabled()) {
          LOG.debug("new region " + region.regionName);
        }
        
        // A region has split and the old server is serving the two new regions.

        if(region.tableDesc.getName().equals(META_TABLE_NAME)) {
          // A meta region has split.

          allMetaRegionsScanned = false;
        }
        
        break;

      default:
        throw new IOException("Impossible state during msg processing.  Instruction: "
            + incomingMsgs[i].getMsg());
      }
    }

    // Process the kill list
    
    if(regionsToKill != null) {
      for(HRegionInfo i: regionsToKill.values()) {
        returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE, i));
        killedRegions.add(i.regionName);
      }
    }

    // Figure out what the RegionServer ought to do, and write back.
    if(unassignedRegions.size() > 0) {
      // Open new regions as necessary
      int targetForServer = (int) Math.ceil(unassignedRegions.size()
          / (1.0 * serversToServerInfo.size()));

      int counter = 0;
      long now = System.currentTimeMillis();
      for (Text curRegionName: unassignedRegions.keySet()) {
        HRegionInfo regionInfo = unassignedRegions.get(curRegionName);
        long assignedTime = assignAttempts.get(curRegionName);
        if (now - assignedTime > maxRegionOpenTime) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("assigning region " + regionInfo.regionName + " to server "
                + info.getServerAddress().toString());
          }

          returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));

          assignAttempts.put(curRegionName, Long.valueOf(now));
          counter++;
        }

        if(counter >= targetForServer) {
          break;
        }
      }
    }
    return returnMsgs.toArray(new HMsg[returnMsgs.size()]);
  }
  
  /**
   * Called when the master has received a report from a region server that it
   * is now serving the root region. Causes any threads waiting for the root
   * region to be available to be woken up.
   */
  private synchronized void rootRegionIsAvailable() {
    notifyAll();
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Some internal classes to manage msg-passing and client operations
  //////////////////////////////////////////////////////////////////////////////
  
  private abstract class PendingOperation {
    protected final Text[] columns = {
        COLUMN_FAMILY
    };
    protected final Text startRow = new Text();
    protected long clientId;

    PendingOperation() {
      this.clientId = rand.nextLong();
    }
    
    abstract void process() throws IOException;
  }

  /** 
   * Instantiated when a server's lease has expired, meaning it has crashed.
   * The region server's log file needs to be split up for each region it was
   * serving, and the regions need to get reassigned.
   */
  private class PendingServerShutdown extends PendingOperation {
    private HServerAddress deadServer;
    private String deadServerName;
    private long oldStartCode;
    
    private class ToDoEntry {
      boolean deleteRegion;
      boolean regionOffline;
      Text row;
      HRegionInfo info;
      
      ToDoEntry(Text row, HRegionInfo info) {
        this.deleteRegion = false;
        this.regionOffline = false;
        this.row = row;
        this.info = info;
      }
    }
    
    PendingServerShutdown(HServerInfo serverInfo) {
      super();
      this.deadServer = serverInfo.getServerAddress();
      this.deadServerName = this.deadServer.toString();
      this.oldStartCode = serverInfo.getStartCode();
    }
    
    /** Finds regions that the dead region server was serving */
    private void scanMetaRegion(HRegionInterface server, long scannerId,
        Text regionName) throws IOException {

      Vector<ToDoEntry> toDoList = new Vector<ToDoEntry>();
      TreeMap<Text, HRegionInfo> regions = new TreeMap<Text, HRegionInfo>();

      DataInputBuffer inbuf = new DataInputBuffer();
      try {
        while(true) {
          KeyedData[] values = null;
          
          try {
            values = server.next(scannerId);
            
          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
              
            }
            LOG.error(e);
            break;
          }
          
          if(values == null || values.length == 0) {
            break;
          }

          TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          Text row = null;
          for(int i = 0; i < values.length; i++) {
            if(row == null) {
              row = values[i].getKey().getRow();
              
            } else {
              if(!row.equals(values[i].getKey().getRow())) {
                LOG.error("Multiple rows in same scanner result set. firstRow="
                    + row + ", currentRow=" + values[i].getKey().getRow());
              }
            }
            results.put(values[i].getKey().getColumn(), values[i].getData());
          }
          
          byte [] bytes = results.get(COL_SERVER); 
          String serverName = null;
          if(bytes == null || bytes.length == 0) {
            // No server
            continue;
          }
          try {
            serverName = new String(bytes, UTF8_ENCODING);
            
          } catch(UnsupportedEncodingException e) {
            LOG.error(e);
            break;
          }

          if(deadServerName.compareTo(serverName) != 0) {
            // This isn't the server you're looking for - move along
            continue;
          }

          bytes = results.get(COL_STARTCODE);
          if(bytes == null || bytes.length == 0) {
            // No start code
            continue;
          }
          long startCode = -1L;
          try {
            startCode =
              Long.valueOf(new String(bytes, UTF8_ENCODING)).longValue();
          } catch(UnsupportedEncodingException e) {
            LOG.error(e);
            break;
          }

          if(oldStartCode != startCode) {
            // Close but no cigar
            continue;
          }

          // Bingo! Found it.

          bytes = results.get(COL_REGIONINFO);
          if(bytes == null || bytes.length == 0) {
            throw new IOException("no value for " + COL_REGIONINFO);
          }
          inbuf.reset(bytes, bytes.length);
          HRegionInfo info = new HRegionInfo();
          try {
            info.readFields(inbuf);
            
          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            LOG.error(e);
            break;
          }

          if(LOG.isDebugEnabled()) {
            LOG.debug(serverName + " was serving " + info.toString());
          }

          if(info.tableDesc.getName().equals(META_TABLE_NAME)) {
            allMetaRegionsScanned = false;
          }
          
          ToDoEntry todo = new ToDoEntry(row, info);
          toDoList.add(todo);
          
          if(killList.containsKey(deadServerName)) {
            TreeMap<Text, HRegionInfo> regionsToKill = killList.get(deadServerName);
            if(regionsToKill.containsKey(info.regionName)) {
              regionsToKill.remove(info.regionName);
              killList.put(deadServerName, regionsToKill);
              unassignedRegions.remove(info.regionName);
              assignAttempts.remove(info.regionName);
              
              if(regionsToDelete.contains(info.regionName)) {
                // Delete this region
                
                regionsToDelete.remove(info.regionName);
                todo.deleteRegion = true;
                
              } else {
                // Mark region offline
                
                todo.regionOffline = true;
              }
            }
          } else {
            // Get region reassigned

            regions.put(info.regionName, info);
          }
        }

      } finally {
        if(scannerId != -1L) {
          try {
            server.close(scannerId);
            
          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            LOG.error(e);
          }
        }
      }

      // Remove server from root/meta entries
      for(int i = 0; i < toDoList.size(); i++) {
        ToDoEntry e = toDoList.get(i);
        long lockid = server.startUpdate(regionName, clientId, e.row);
        if(e.deleteRegion) {
          server.delete(regionName, clientId, lockid, COL_REGIONINFO);
        } else if(e.regionOffline) {
          e.info.offLine = true;
          ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
          DataOutputStream s = new DataOutputStream(byteValue);
          e.info.write(s);
          server.put(regionName, clientId, lockid, COL_REGIONINFO,
            byteValue.toByteArray());
        }
        server.delete(regionName, clientId, lockid, COL_SERVER);
        server.delete(regionName, clientId, lockid, COL_STARTCODE);
        server.commit(regionName, clientId, lockid, System.currentTimeMillis());
      }

      // Get regions reassigned

      for(Map.Entry<Text, HRegionInfo> e: regions.entrySet()) {
        Text region = e.getKey();
        HRegionInfo regionInfo = e.getValue();

        unassignedRegions.put(region, regionInfo);
        assignAttempts.put(region, Long.valueOf(0L));
      }
    }

    @Override
    void process() throws IOException {
      if(LOG.isDebugEnabled()) {
        LOG.debug("server shutdown: " + deadServerName);
      }
      
      // Process the old log file
      
      HLog.splitLog(dir, new Path(dir, "log" + "_" + deadServer.getBindAddress()
          + "_" + deadServer.getPort()), fs, conf);

      if(rootRegionLocation != null
          && deadServerName.equals(rootRegionLocation.toString())) {
        
        rootRegionLocation = null;
        unassignedRegions.put(HGlobals.rootRegionInfo.regionName,
            HGlobals.rootRegionInfo);
        assignAttempts.put(HGlobals.rootRegionInfo.regionName,
          Long.valueOf(0L));
      }
      
      // Scan the ROOT region

      HRegionInterface server = null;
      long scannerId = -1L;
      for(int tries = 0; tries < numRetries; tries ++) {
        if(waitForRootRegionOrClose()) {// Wait until the root region is available
          return;                       // We're shutting down. Forget it.
        }
        server = client.getHRegionConnection(rootRegionLocation);
        scannerId = -1L;
        
        try {
          LOG.debug("scanning root region");
          scannerId = server.openScanner(HGlobals.rootRegionInfo.regionName,
              columns, startRow, System.currentTimeMillis(), null);
          scanMetaRegion(server, scannerId, HGlobals.rootRegionInfo.regionName);
          break;
          
        } catch (IOException e) {
          if (tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            throw e;
          }
        }
      }

      // We can not scan every meta region if they have not already been assigned
      // and scanned.

      for(int tries = 0; tries < numRetries; tries ++) {
        try {
          if(metaScanner.waitForMetaScanOrClose()) {
            return;                     // We're shutting down. Forget it.
          }
      
          for(Iterator<MetaRegion> i = knownMetaRegions.values().iterator();
              i.hasNext(); ) {
          
            server = null;
            scannerId = -1L;
            MetaRegion r = i.next();

            server = client.getHRegionConnection(r.server);
          
            scannerId = server.openScanner(r.regionName, columns, startRow,
                System.currentTimeMillis(), null);
            scanMetaRegion(server, scannerId, r.regionName);
            
          }
          break;
            
        } catch (IOException e) {
          if (tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            throw e;
          }
        }
      }
    }
  }
  
  /**
   * PendingCloseReport is instantiated when a region server reports that it
   * has closed a region.
   */
  private class PendingCloseReport extends PendingOperation {
    private HRegionInfo regionInfo;
    private boolean reassignRegion;
    private boolean deleteRegion;
    private boolean rootRegion;
    
    PendingCloseReport(HRegionInfo regionInfo, boolean reassignRegion,
        boolean deleteRegion) {
      
      super();

      this.regionInfo = regionInfo;
      this.reassignRegion = reassignRegion;
      this.deleteRegion = deleteRegion;

      // If the region closing down is a meta region then we need to update
      // the ROOT table
      
      if(this.regionInfo.tableDesc.getName().equals(META_TABLE_NAME)) {
        this.rootRegion = true;
        
      } else {
        this.rootRegion = false;
      }
    }
    
    @Override
    void process() throws IOException {
      for(int tries = 0; tries < numRetries; tries ++) {

        // We can not access any meta region if they have not already been assigned
        // and scanned.

        if(metaScanner.waitForMetaScanOrClose()) {
          return;                       // We're shutting down. Forget it.
        }

        if(LOG.isDebugEnabled()) {
          LOG.debug("region closed: " + regionInfo.regionName);
        }

        // Mark the Region as unavailable in the appropriate meta table

        Text metaRegionName;
        HRegionInterface server;
        if (rootRegion) {
          metaRegionName = HGlobals.rootRegionInfo.regionName;
          if(waitForRootRegionOrClose()) {// Make sure root region available
            return;                     // We're shutting down. Forget it.
          }
          server = client.getHRegionConnection(rootRegionLocation);

        } else {
          MetaRegion r = null;
          if(knownMetaRegions.containsKey(regionInfo.regionName)) {
            r = knownMetaRegions.get(regionInfo.regionName);

          } else {
            r = knownMetaRegions.get(
                knownMetaRegions.headMap(regionInfo.regionName).lastKey());
          }
          metaRegionName = r.regionName;
          server = client.getHRegionConnection(r.server);
        }

        try {
          long lockid = server.startUpdate(metaRegionName, clientId, regionInfo.regionName);
          if(deleteRegion) {
            server.delete(metaRegionName, clientId, lockid, COL_REGIONINFO);
            
          } else if(!reassignRegion ) {
            regionInfo.offLine = true;
            ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
            DataOutputStream s = new DataOutputStream(byteValue);
            regionInfo.write(s);

            server.put(metaRegionName, clientId, lockid, COL_REGIONINFO,
              byteValue.toByteArray());
          }
          server.delete(metaRegionName, clientId, lockid, COL_SERVER);
          server.delete(metaRegionName, clientId, lockid, COL_STARTCODE);
          server.commit(metaRegionName, clientId, lockid,
              System.currentTimeMillis());
          
          break;

        } catch (IOException e) {
          if (tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            throw e;
          }
          continue;
        }
      }

      if(reassignRegion) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("reassign region: " + regionInfo.regionName);
        }
        
        unassignedRegions.put(regionInfo.regionName, regionInfo);
        assignAttempts.put(regionInfo.regionName, Long.valueOf(0L));
        
      } else if(deleteRegion) {
        try {
          HRegion.deleteRegion(fs, dir, regionInfo.regionName);

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          LOG.error("failed to delete region " + regionInfo.regionName);
          LOG.error(e);
          throw e;
        }
      }
    }
  }

  /** 
   * PendingOpenReport is instantiated when a region server reports that it is
   * serving a region. This applies to all meta and user regions except the 
   * root region which is handled specially.
   */
  private class PendingOpenReport extends PendingOperation {
    private boolean rootRegion;
    private Text regionName;
    private byte [] serverAddress;
    private byte [] startCode;
    
    PendingOpenReport(HServerInfo info, HRegionInfo region) {
      if (region.tableDesc.getName().equals(META_TABLE_NAME)) {
        // The region which just came on-line is a META region.
        // We need to look in the ROOT region for its information.
        this.rootRegion = true;
      } else {
        // Just an ordinary region. Look for it in the META table.
        this.rootRegion = false;
      }
      this.regionName = region.regionName;
      try {
        this.serverAddress = info.getServerAddress().toString().
          getBytes(UTF8_ENCODING);
        this.startCode = String.valueOf(info.getStartCode()).
          getBytes(UTF8_ENCODING);
      } catch(UnsupportedEncodingException e) {
        LOG.error(e);
      }
    }
    
    @Override
    void process() throws IOException {
      for(int tries = 0; tries < numRetries; tries ++) {

        // We can not access any meta region if they have not already been assigned
        // and scanned.

        if(metaScanner.waitForMetaScanOrClose()) {
          return;                       // We're shutting down. Forget it.
        }

        if(LOG.isDebugEnabled()) {
          LOG.debug(regionName + " open on "
              + new String(this.serverAddress, UTF8_ENCODING));
        }

        // Register the newly-available Region's location.

        Text metaRegionName;
        HRegionInterface server;
        if(rootRegion) {
          metaRegionName = HGlobals.rootRegionInfo.regionName;
          if(waitForRootRegionOrClose()) {// Make sure root region available
            return;                     // We're shutting down. Forget it.
          }
          server = client.getHRegionConnection(rootRegionLocation);

        } else {
          MetaRegion r = null;
          if(knownMetaRegions.containsKey(regionName)) {
            r = knownMetaRegions.get(regionName);

          } else {
            r = knownMetaRegions.get(
                knownMetaRegions.headMap(regionName).lastKey());
          }
          metaRegionName = r.regionName;
          server = client.getHRegionConnection(r.server);
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("updating row " + regionName + " in table " + metaRegionName);
        }
        try {
          long lockid = server.startUpdate(metaRegionName, clientId, regionName);
          server.put(metaRegionName, clientId, lockid, COL_SERVER, serverAddress);
          server.put(metaRegionName, clientId, lockid, COL_STARTCODE, startCode);
          server.commit(metaRegionName, clientId, lockid,
              System.currentTimeMillis());
          
          break;
          
        } catch (IOException e) {
          if(tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            throw e;
          }
        }
        pendingRegions.remove(regionName);
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMasterInterface
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * {@inheritDoc}
   */
  public boolean isMasterRunning() {
    return !closed;
  }

  /**
   * {@inheritDoc}
   */
  public void shutdown() {
    TimerTask tt = new TimerTask() {
      @Override
      public void run() {
        closed = true;
        synchronized(msgQueue) {
          msgQueue.clear();                         // Empty the queue
          msgQueue.notifyAll();                     // Wake main thread
        }
      }
    };
    Timer t = new Timer("Shutdown");
    t.schedule(tt, 10);
  }

  /**
   * {@inheritDoc}
   */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo newRegion = new HRegionInfo(rand.nextLong(), desc, null, null);

    for(int tries = 0; tries < numRetries; tries++) {
      try {
        // We can not access meta regions if they have not already been
        // assigned and scanned.  If we timeout waiting, just shutdown.
        if (metaScanner.waitForMetaScanOrClose()) {
          return;
        }
         createTable(newRegion);
        break;
      } catch (IOException e) {
        if(tries == numRetries - 1) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
      }
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("created table " + desc.getName());
    }
  }
  
  /*
   * Set of tables currently in creation. Access needs to be synchronized.
   */
  private Set<Text> tableInCreation = new HashSet<Text>();
  
  private void createTable(final HRegionInfo newRegion) throws IOException {
    Text tableName = newRegion.tableDesc.getName();
    synchronized (tableInCreation) {
      if (tableInCreation.contains(tableName)) {
        throw new TableExistsException("Table " + tableName + " in process "
            + "of being created");
      }
      tableInCreation.add(tableName);
    }
    try {
      // 1. Check to see if table already exists. Get meta region where
      // table would sit should it exist. Open scanner on it. If a region
      // for the table we want to create already exists, then table already
      // created. Throw already-exists exception.
      MetaRegion m = (knownMetaRegions.containsKey(newRegion.regionName))?
          knownMetaRegions.get(newRegion.regionName):
            knownMetaRegions.get(knownMetaRegions.
              headMap(newRegion.getTableDesc().getName()).lastKey());
      Text metaRegionName = m.regionName;
      HRegionInterface connection = client.getHRegionConnection(m.server);
      long scannerid = connection.openScanner(metaRegionName,
        new Text[] { COL_REGIONINFO }, tableName, System.currentTimeMillis(),
        null);
      try {
        KeyedData[] data = connection.next(scannerid);
        // Test data and that the row for the data is for our table. If
        // table does not exist, scanner will return row after where our table
        // would be inserted if it exists so look for exact match on table
        // name.
        if (data != null && data.length > 0 &&
          HRegionInfo.getTableNameFromRegionName(data[0].getKey().getRow()).
            equals(tableName)) {
          // Then a region for this table already exists. Ergo table exists.
          throw new TableExistsException(tableName.toString());
        }
      } finally {
        connection.close(scannerid);
      }

      // 2. Create the HRegion
      HRegion r = HRegion.createHRegion(newRegion.regionId, newRegion.
        getTableDesc(), this.dir, this.conf);

      // 3. Insert into meta
      HRegionInfo info = r.getRegionInfo();
      Text regionName = r.getRegionName();
      ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteValue);
      info.write(s);
      long clientId = rand.nextLong();
      long lockid = connection.
        startUpdate(metaRegionName, clientId, regionName);
      connection.put(metaRegionName, clientId, lockid, COL_REGIONINFO,
        byteValue.toByteArray());
      connection.commit(metaRegionName, clientId, lockid,
        System.currentTimeMillis());

      // 4. Close the new region to flush it to disk.  Close its log file too.
      r.close();
      r.getLog().closeAndDelete();

      // 5. Get it assigned to a server
      unassignedRegions.put(regionName, info);
      assignAttempts.put(regionName, Long.valueOf(0L));
    } finally {
      synchronized (tableInCreation) {
        tableInCreation.remove(newRegion.getTableDesc().getName());
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public void deleteTable(Text tableName) throws IOException {
    new TableDelete(tableName).process();
    if(LOG.isDebugEnabled()) {
      LOG.debug("deleted table: " + tableName);
    }
  }
  
  /**
   * {@inheritDoc}
   */
  public void addColumn(Text tableName, HColumnDescriptor column) throws IOException {
    new AddColumn(tableName, column).process();
  }
  
  /**
   * {@inheritDoc}
   */
  public void deleteColumn(Text tableName, Text columnName) throws IOException {
    new DeleteColumn(tableName, HStoreKey.extractFamily(columnName)).process();
  }
  
  /**
   * {@inheritDoc}
   */
  public void enableTable(Text tableName) throws IOException {
    new ChangeTableState(tableName, true).process();
  }

  /**
   * {@inheritDoc}
   */
  public void disableTable(Text tableName) throws IOException {
    new ChangeTableState(tableName, false).process();
  }
  
  /**
   * {@inheritDoc}
   */
  public HServerAddress findRootRegion() {
    return rootRegionLocation;
  }
  
  // Helper classes for HMasterInterface

  private abstract class TableOperation {
    private SortedSet<MetaRegion> metaRegions;
    protected Text tableName;
    
    protected TreeSet<HRegionInfo> unservedRegions;
    
    protected TableOperation(Text tableName) throws IOException {
      if (!isMasterRunning()) {
        throw new MasterNotRunningException();
      }
      this.metaRegions = new TreeSet<MetaRegion>();
      this.tableName = tableName;
      this.unservedRegions = new TreeSet<HRegionInfo>();

      // We can not access any meta region if they have not already been
      // assigned and scanned.

      if(metaScanner.waitForMetaScanOrClose()) {
        return;                         // We're shutting down. Forget it.
      }

      Text firstMetaRegion = null;
      if(knownMetaRegions.size() == 1) {
        firstMetaRegion = knownMetaRegions.firstKey();

      } else if(knownMetaRegions.containsKey(tableName)) {
        firstMetaRegion = tableName;

      } else {
        firstMetaRegion = knownMetaRegions.headMap(tableName).lastKey();
      }

      this.metaRegions.addAll(knownMetaRegions.tailMap(firstMetaRegion).values());
    }
    
    void process() throws IOException {
      for(int tries = 0; tries < numRetries; tries++) {
        boolean tableExists = false;
        try {
          synchronized(metaScannerLock) {     // Prevent meta scanner from running
            for(MetaRegion m: metaRegions) {

              // Get a connection to a meta server

              HRegionInterface server = client.getHRegionConnection(m.server);

              // Open a scanner on the meta region
              
              long scannerId =
                server.openScanner(m.regionName, METACOLUMNS, tableName,
                    System.currentTimeMillis(), null);
              
              try {
                DataInputBuffer inbuf = new DataInputBuffer();
                while(true) {
                  HRegionInfo info = new HRegionInfo();
                  String serverName = null;
                  long startCode = -1L;
                  
                  KeyedData[] values = null;
                  values = server.next(scannerId);
                  if(values == null || values.length == 0) {
                    break;
                  }
                  boolean haveRegionInfo = false;
                  for(int i = 0; i < values.length; i++) {
                    if(values[i].getData().length == 0) {
                      break;
                    }
                    Text column = values[i].getKey().getColumn();
                    if(column.equals(COL_REGIONINFO)) {
                      haveRegionInfo = true;
                      inbuf.reset(values[i].getData(),
                        values[i].getData().length);
                      info.readFields(inbuf);
                    } else if(column.equals(COL_SERVER)) {
                      try {
                        serverName =
                          new String(values[i].getData(), UTF8_ENCODING);
                      } catch(UnsupportedEncodingException e) {
                        assert(false);
                      }
                    } else if(column.equals(COL_STARTCODE)) {
                      try {
                        startCode = Long.valueOf(new String(values[i].getData(),
                          UTF8_ENCODING)).longValue();
                      } catch(UnsupportedEncodingException e) {
                        assert(false);
                      }
                    }
                  }
                  
                  if(!haveRegionInfo) {
                    throw new IOException(COL_REGIONINFO + " not found");
                  }
                  
                  if(info.tableDesc.getName().compareTo(tableName) > 0) {
                    break;               // Beyond any more entries for this table
                  }
                  
                  tableExists = true;
                  if(!isBeingServed(serverName, startCode)) {
                    unservedRegions.add(info);
                  }
                  processScanItem(serverName, startCode, info);

                } // while(true)
                
              } finally {
                if(scannerId != -1L) {
                  try {
                    server.close(scannerId);

                  } catch (IOException e) {
                    if (e instanceof RemoteException) {
                      e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
                    }
                    LOG.error(e);
                  }
                }
                scannerId = -1L;
              }
              
              if(!tableExists) {
                throw new IOException(tableName + " does not exist");
              }
              
              postProcessMeta(m, server);
              unservedRegions.clear();
              
            } // for(MetaRegion m:)
          } // synchronized(metaScannerLock)
          
        } catch (IOException e) {
          if(tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            throw e;
          }
          continue;
        }
        break;
      } // for(tries...)
    }
    
    protected boolean isBeingServed(String serverName, long startCode) {
      boolean result = false;
      if(serverName != null && startCode != -1L) {
        HServerInfo s = serversToServerInfo.get(serverName);
        result = s != null && s.getStartCode() == startCode;
      }
      return result;
    }
    
    protected boolean isEnabled(HRegionInfo info) {
      return !info.offLine;
    }
    
    protected abstract void processScanItem(String serverName, long startCode,
        HRegionInfo info) throws IOException;
    
    protected abstract void postProcessMeta(MetaRegion m,
        HRegionInterface server)
    throws IOException;
  }

  /** Instantiated to enable or disable a table */
  private class ChangeTableState extends TableOperation {
    private boolean online;
    
    protected TreeMap<String, TreeSet<HRegionInfo>> servedRegions =
      new TreeMap<String, TreeSet<HRegionInfo>>();
    protected long lockid;
    protected long clientId;
    
    ChangeTableState(Text tableName, boolean onLine) throws IOException {
      super(tableName);
      this.online = onLine;
    }
    
    @Override
    protected void processScanItem(String serverName, long startCode,
        HRegionInfo info) {
      if (isBeingServed(serverName, startCode)) {
        TreeSet<HRegionInfo> regions = servedRegions.get(serverName);
        if (regions == null) {
          regions = new TreeSet<HRegionInfo>();
        }
        regions.add(info);
        servedRegions.put(serverName, regions);
      }
    }
    
    @Override
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
        throws IOException {
      // Process regions not being served
      if(LOG.isDebugEnabled()) {
        LOG.debug("processing unserved regions");
      }
      for(HRegionInfo i: unservedRegions) {
        // Update meta table
        if(LOG.isDebugEnabled()) {
          LOG.debug("updating columns in row: " + i.regionName);
        }

        lockid = -1L;
        clientId = rand.nextLong();
        try {
          lockid = server.startUpdate(m.regionName, clientId, i.regionName);
          updateRegionInfo(server, m.regionName, i);
          server.delete(m.regionName, clientId, lockid, COL_SERVER);
          server.delete(m.regionName, clientId, lockid, COL_STARTCODE);
          server.commit(m.regionName, clientId, lockid,
              System.currentTimeMillis());
          
          lockid = -1L;

          if(LOG.isDebugEnabled()) {
            LOG.debug("updated columns in row: " + i.regionName);
          }

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          LOG.error("column update failed in row: " + i.regionName);
          LOG.error(e);

        } finally {
          try {
            if(lockid != -1L) {
              server.abort(m.regionName, clientId, lockid);
            }

          } catch (IOException iex) {
            if (iex instanceof RemoteException) {
              iex = RemoteExceptionHandler.decodeRemoteException((RemoteException) iex);
            }
            LOG.error(iex);
          }
        }

        if(online) {                            // Bring offline regions on-line
          if(!unassignedRegions.containsKey(i.regionName)) {
            unassignedRegions.put(i.regionName, i);
            assignAttempts.put(i.regionName, 0L);
          }
          
        } else {                                // Prevent region from getting assigned.
          unassignedRegions.remove(i.regionName);
          assignAttempts.remove(i.regionName);
        }
      }
      
      // Process regions currently being served
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("processing regions currently being served");
      }
      for(Map.Entry<String, TreeSet<HRegionInfo>> e: servedRegions.entrySet()) {
        String serverName = e.getKey();
        if (online) {
          LOG.debug("Already online");
          continue;                             // Already being served
        }
        
        // Cause regions being served to be taken off-line and disabled
        TreeMap<Text, HRegionInfo> localKillList = killList.get(serverName);
        if(localKillList == null) {
          localKillList = new TreeMap<Text, HRegionInfo>();
        }
        for(HRegionInfo i: e.getValue()) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("adding region " + i.regionName + " to local kill list");
          }
          localKillList.put(i.regionName, i);
        }
        if(localKillList.size() > 0) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("inserted local kill list into kill list for server " +
              serverName);
          }
          killList.put(serverName, localKillList);
        }
      }
      servedRegions.clear();
    }
    
    protected void updateRegionInfo(HRegionInterface server, Text regionName,
        HRegionInfo i) throws IOException {
      
      i.offLine = !online;
      
      ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteValue);
      i.write(s);

      server.put(regionName, clientId, lockid, COL_REGIONINFO,
        byteValue.toByteArray());
    }
  }

  /** 
   * Instantiated to delete a table
   * Note that it extends ChangeTableState, which takes care of disabling
   * the table.
   */
  private class TableDelete extends ChangeTableState {
    
    TableDelete(Text tableName) throws IOException {
      super(tableName, false);
    }
    
    @Override
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
        throws IOException {
      // For regions that are being served, mark them for deletion      
      for (TreeSet<HRegionInfo> s: servedRegions.values()) {
        for (HRegionInfo i: s) {
          regionsToDelete.add(i.regionName);
        }
      }

      // Unserved regions we can delete now
      for (HRegionInfo i: unservedRegions) {
        // Delete the region
        try {
          HRegion.deleteRegion(fs, dir, i.regionName);
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          LOG.error("failed to delete region " + i.regionName);
          LOG.error(e);
        }
      }
      super.postProcessMeta(m, server);
    }
    
    @Override
    protected void updateRegionInfo(
      @SuppressWarnings("hiding") HRegionInterface server, Text regionName,
      @SuppressWarnings("unused") HRegionInfo i)
    throws IOException {
      server.delete(regionName, clientId, lockid, COL_REGIONINFO);
    }
  }
  
  private abstract class ColumnOperation extends TableOperation {
    protected ColumnOperation(Text tableName) throws IOException {
      super(tableName);
    }

    @Override
    protected void processScanItem(
      @SuppressWarnings("unused") String serverName,
      @SuppressWarnings("unused") long startCode,
      final HRegionInfo info)
    throws IOException {  
      if(isEnabled(info)) {
        throw new TableNotDisabledException(tableName.toString());
      }
    }

    protected void updateRegionInfo(HRegionInterface server, Text regionName,
        HRegionInfo i)
    throws IOException {  
      ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteValue);
      i.write(s);
      long lockid = -1L;
      long clientId = rand.nextLong();
      try {
        lockid = server.startUpdate(regionName, clientId, i.regionName);
        server.put(regionName, clientId, lockid, COL_REGIONINFO,
          byteValue.toByteArray());
        server.commit(regionName, clientId, lockid, System.currentTimeMillis());
        lockid = -1L;
        if(LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: " + i.regionName);
        }
      } catch (Exception e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        LOG.error("column update failed in row: " + i.regionName);
        LOG.error(e);

      } finally {
        if(lockid != -1L) {
          try {
            server.abort(regionName, clientId, lockid);
            
          } catch (IOException iex) {
            if (iex instanceof RemoteException) {
              iex = RemoteExceptionHandler.decodeRemoteException((RemoteException) iex);
            }
            LOG.error(iex);
          }
        }
      }
    }
  }

  /** Instantiated to remove a column family from a table */
  private class DeleteColumn extends ColumnOperation {
    private Text columnName;
    
    DeleteColumn(Text tableName, Text columnName) throws IOException {
      super(tableName);
      this.columnName = columnName;
    }
    
    @Override
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
    throws IOException {

      for(HRegionInfo i: unservedRegions) {
        i.tableDesc.families().remove(columnName);
        updateRegionInfo(server, m.regionName, i);
        
        // Delete the directories used by the column

        try {
          fs.delete(HStoreFile.getMapDir(dir, i.regionName, columnName));
          
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          LOG.error(e);
        }
        
        try {
          fs.delete(HStoreFile.getInfoDir(dir, i.regionName, columnName));
          
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          LOG.error(e);
        }
        
      }
    }
  }

  /** Instantiated to add a column family to a table */
  private class AddColumn extends ColumnOperation {
    private HColumnDescriptor newColumn;
    
    AddColumn(Text tableName, HColumnDescriptor newColumn)
        throws IOException {
      
      super(tableName);
      this.newColumn = newColumn;
    }
   
    @Override
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
        throws IOException {

      for(HRegionInfo i: unservedRegions) {
        
        // All we need to do to add a column is add it to the table descriptor.
        // When the region is brought on-line, it will find the column missing
        // and create it.
        
        i.tableDesc.addFamily(newColumn);
        updateRegionInfo(server, m.regionName, i);
      }
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Managing leases
  //////////////////////////////////////////////////////////////////////////////

  /** Instantiated to monitor the health of a region server */
  private class ServerExpirer implements LeaseListener {
    @SuppressWarnings("hiding")
    private String server;
    
    ServerExpirer(String server) {
      this.server = server;
    }
    
    /**
     * {@inheritDoc}
     */
    public void leaseExpired() {
      LOG.info(server + " lease expired");
      HServerInfo storedInfo = serversToServerInfo.remove(server);
      if(rootRegionLocation != null
          && rootRegionLocation.toString().equals(
              storedInfo.getServerAddress().toString())) {
        
        rootRegionLocation = null;
        unassignedRegions.put(HGlobals.rootRegionInfo.regionName,
            HGlobals.rootRegionInfo);
        assignAttempts.put(HGlobals.rootRegionInfo.regionName, 0L);
      }
      try {
        msgQueue.put(new PendingServerShutdown(storedInfo));
      } catch (InterruptedException e) {
        throw new RuntimeException("Putting into msgQueue was interrupted.", e);
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Main program
  //////////////////////////////////////////////////////////////////////////////
  
  private static void printUsageAndExit() {
    System.err.println("Usage: java org.apache.hbase.HMaster " +
        "[--bind=hostname:port] start|stop");
    System.exit(0);
  }
  
  /**
   * Main program
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
        conf.set(MASTER_ADDRESS, cmd.substring(addressArgKey.length()));
        continue;
      }
      
      if (cmd.equals("start")) {
        try {
          (new Thread(new HMaster(conf))).start();
        } catch (Throwable t) {
          LOG.error( "Can not start master because "+
              StringUtils.stringifyException(t) );
          System.exit(-1);
        }
        break;
      }
      
      if (cmd.equals("stop")) {
        try {
          HClient client = new HClient(conf);
          client.shutdown();
        } catch (Throwable t) {
          LOG.error( "Can not stop master because " +
              StringUtils.stringifyException(t) );
          System.exit(-1);
        }
        break;
      }
      
      // Print out usage if we get to here.
      printUsageAndExit();
    }
  }
}
