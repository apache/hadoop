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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.MapWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;

import org.apache.hadoop.hbase.io.BatchUpdate;


/**
 * HMaster is the "master server" for a HBase.
 * There is only one HMaster for a single HBase deployment.
 */
public class HMaster implements HConstants, HMasterInterface, 
HMasterRegionInterface, Runnable {

  /** {@inheritDoc} */
  public long getProtocolVersion(String protocol,
      @SuppressWarnings("unused") long clientVersion) throws IOException {

    if (protocol.equals(HMasterInterface.class.getName())) {
      return HMasterInterface.versionID; 

    } else if (protocol.equals(HMasterRegionInterface.class.getName())) {
      return HMasterRegionInterface.versionID;

    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  static final Log LOG = LogFactory.getLog(HMaster.class.getName());

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

  HConnection connection;

  long metaRescanInterval;

  final AtomicReference<HServerAddress> rootRegionLocation;

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
   * </li>periodically to rescan the online <code>META</code> regions</li>
   * </ol>
   * 
   * <p>A <code>META</code> region is not 'online' until it has been scanned
   * once.
   */
  abstract class BaseScanner implements Runnable {
    protected boolean rootRegion;
    protected final Text tableName;

    protected abstract void initialScan();
    protected abstract void maintenanceScan();

    BaseScanner(final Text tableName) {
      super();
      this.tableName = tableName;
      this.rootRegion = tableName.equals(ROOT_TABLE_NAME);
    }

    /** {@inheritDoc} */
    public void run() {
      initialScan();
      while (!closed) {
        try {
          Thread.sleep(metaRescanInterval);
        } catch (InterruptedException e) {
          continue;
        }
        maintenanceScan();
      }
      LOG.info(this.getClass().getSimpleName() + " exiting");
    }

    /**
     * @param region Region to scan
     * @throws IOException
     */
    protected void scanRegion(final MetaRegion region) throws IOException {
      HRegionInterface regionServer = null;
      long scannerId = -1L;
      LOG.info(Thread.currentThread().getName() + " scanning meta region " +
          region.regionName + " on " + region.server.toString());

      // Array to hold list of split parents found.  Scan adds to list.  After
      // scan we go check if parents can be removed.

      Map<HRegionInfo, SortedMap<Text, byte[]>> splitParents =
        new HashMap<HRegionInfo, SortedMap<Text, byte[]>>();
      try {
        regionServer = connection.getHRegionConnection(region.server);
        scannerId =
          regionServer.openScanner(region.regionName, COLUMN_FAMILY_ARRAY,
              EMPTY_START_ROW, System.currentTimeMillis(), null);

        int numberOfRegionsFound = 0;
        while (true) {
          SortedMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          MapWritable values = regionServer.next(scannerId);
          if (values == null || values.size() == 0) {
            break;
          }

          for (Map.Entry<WritableComparable, Writable> e: values.entrySet()) {
            HStoreKey key = (HStoreKey) e.getKey();
            results.put(key.getColumn(),
                ((ImmutableBytesWritable) e.getValue()).get());
          }

          HRegionInfo info = (HRegionInfo) Writables.getWritable(
              results.get(COL_REGIONINFO), new HRegionInfo());

          String serverName = Writables.bytesToString(results.get(COL_SERVER));
          long startCode = Writables.bytesToLong(results.get(COL_STARTCODE));

          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + " scanner: " +
                Long.valueOf(scannerId) + " regioninfo: {" + info.toString() +
                "}, server: " + serverName + ", startCode: " + startCode);
          }

          // Note Region has been assigned.
          checkAssigned(info, serverName, startCode);

          if (isSplitParent(info)) {
            splitParents.put(info, results);
          }
          numberOfRegionsFound += 1;
        }
        if (rootRegion) {
          numberOfMetaRegions.set(numberOfRegionsFound);
        }

      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);

          if (e instanceof UnknownScannerException) {
            // Reset scannerId so we do not try closing a scanner the other side
            // has lost account of: prevents duplicated stack trace out of the 
            // below close in the finally.
            scannerId = -1L;
          }
        }
        throw e;

      } finally {
        try {
          if (scannerId != -1L && regionServer != null) {
            regionServer.close(scannerId);
          }
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          LOG.error("Closing scanner", e);
        }
      }

      // Scan is finished.  Take a look at split parents to see if any we can clean up.

      if (splitParents.size() > 0) {
        for (Map.Entry<HRegionInfo, SortedMap<Text, byte[]>> e:
          splitParents.entrySet()) {
          
          SortedMap<Text, byte[]> results = e.getValue();
          cleanupSplits(region.regionName, regionServer, e.getKey(), 
              (HRegionInfo) Writables.getWritable(results.get(COL_SPLITA),
                  new HRegionInfo()),
              (HRegionInfo) Writables.getWritable(results.get(COL_SPLITB),
                  new HRegionInfo()));
        }
      }
      LOG.info(Thread.currentThread().getName() + " scan of meta region " +
          region.regionName + " complete");
    }

    private boolean isSplitParent(final HRegionInfo info) {
      boolean result = false;

      // Skip if not a split region.
      
      if (!info.isSplit()) {
        return result;
      }
      if (!info.isOffline()) {
        LOG.warn("Region is split but not offline: " + info.regionName);
      }
      return true;
    }

    /**
     * @param metaRegionName
     * @param server HRegionInterface of meta server to talk to 
     * @param info HRegionInfo of split parent
     * @param splitA low key range child region 
     * @param splitB upper key range child region
     * @return True if we removed <code>info</code> and this region has
     * been cleaned up.
     * @throws IOException
     */
    private boolean cleanupSplits(final Text metaRegionName, 
        final HRegionInterface server, final HRegionInfo info,
        final HRegionInfo splitA, final HRegionInfo splitB) throws IOException {
    
      boolean result = false;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking " + info.getRegionName() + " to see if daughter " +
        "splits still hold references");
      }
      boolean noReferencesA = splitA == null;
      boolean noReferencesB = splitB == null;
      
      if (!noReferencesA) {
        noReferencesA = hasReferences(metaRegionName, server,
          info.getRegionName(), splitA, COL_SPLITA);
      }
      if (!noReferencesB) {
        noReferencesB = hasReferences(metaRegionName, server,
          info.getRegionName(), splitB, COL_SPLITB);
      }
      if (!noReferencesA && !noReferencesB) {
        // No references.  Remove this item from table and deleted region on
        // disk.
        LOG.info("Deleting region " + info.getRegionName() +
        " because daughter splits no longer hold references");
        
        if (!HRegion.deleteRegion(fs, dir, info.getRegionName())) {
          LOG.warn("Deletion of " + info.getRegionName() + " failed");
        }
        
        BatchUpdate b = new BatchUpdate();
        long lockid = b.startUpdate(info.getRegionName());
        b.delete(lockid, COL_REGIONINFO);
        b.delete(lockid, COL_SERVER);
        b.delete(lockid, COL_STARTCODE);
        server.batchUpdate(metaRegionName, System.currentTimeMillis(), b);
        result = true;
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Done checking " + info.getRegionName() + ": splitA: " +
            noReferencesA + ", splitB: "+ noReferencesB);
      }
      return result;
    }

    protected boolean hasReferences(final Text metaRegionName, 
        final HRegionInterface server, final Text regionName,
        final HRegionInfo split, final Text column) throws IOException {
      
      boolean result = false;
      for (Text family: split.getTableDesc().families().keySet()) {
        Path p = HStoreFile.getMapDir(fs.makeQualified(dir),
            split.getRegionName(), HStoreKey.extractFamily(family));
        
        // Look for reference files.
        
        Path [] ps = fs.listPaths(p,
            new PathFilter () {
              public boolean accept(Path p) {
                return HStoreFile.isReference(p);
              }
            }
        );
        
        if (ps != null && ps.length > 0) {
          result = true;
          break;
        }
      }
      
      if (result) {
        return result;
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug(split.getRegionName().toString()
            +" no longer has references to " + regionName.toString());
      }
      
      BatchUpdate b = new BatchUpdate();
      long lockid = b.startUpdate(regionName);
      b.delete(lockid, column);
      server.batchUpdate(metaRegionName, System.currentTimeMillis(), b);
        
      return result;
    }

    protected void checkAssigned(final HRegionInfo info,
        final String serverName, final long startCode) {
      
      // Skip region - if ...
      if(info.offLine                                     // offline
          || killedRegions.contains(info.regionName)      // queued for offline
          || regionsToDelete.contains(info.regionName)) { // queued for delete
        unassignedRegions.remove(info.regionName);
        assignAttempts.remove(info.regionName);
        return;
      }

      HServerInfo storedInfo = null;
      if (serverName.length() != 0) {
        Map<Text, HRegionInfo> regionsToKill = killList.get(serverName);
        if (regionsToKill != null &&
            regionsToKill.containsKey(info.regionName)) {
          
          // Skip if region is on kill list
          if(LOG.isDebugEnabled()) {
            LOG.debug("not assigning region (on kill list): " + info.regionName);
          }
          return;
        }
        synchronized (serversToServerInfo) {
          storedInfo = serversToServerInfo.get(serverName);
        }
      }
      if (!(unassignedRegions.containsKey(info.regionName) ||
          pendingRegions.contains(info.regionName))
          && (storedInfo == null || storedInfo.getStartCode() != startCode)) {
        
        // The current assignment is no good; load the region.
        unassignedRegions.put(info.regionName, info);
        assignAttempts.put(info.regionName, Long.valueOf(0L));
      }
    }
  }

  volatile boolean rootScanned;

  /** Scanner for the <code>ROOT</code> HRegion. */
  class RootScanner extends BaseScanner {
    /** Constructor */
    public RootScanner() {
      super(HConstants.ROOT_TABLE_NAME);
    }

    private void scanRoot() {
      int tries = 0;
      while (!closed && tries < numRetries) {
        synchronized (rootRegionLocation) {
          while(!closed && rootRegionLocation.get() == null) {
            // rootRegionLocation will be filled in when we get an 'open region'
            // regionServerReport message from the HRegionServer that has been
            // allocated the ROOT region below.
            
            try {
              rootRegionLocation.wait();
            } catch (InterruptedException e) {
              // continue
            }
          }
        }
        if (closed) {
          continue;
        }

        try {
          // Don't interrupt us while we're working
          
          synchronized(rootScannerLock) {
            scanRegion(new MetaRegion(rootRegionLocation.get(),
                HGlobals.rootRegionInfo.regionName, null));
          }
          break;

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            try {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);

            } catch (IOException ex) {
              e = ex;
            }
          }
          tries += 1;
          if (tries == 1) {
            LOG.warn("Scan ROOT region", e);
          } else {
            LOG.error("Scan ROOT region", e);
          }
        }
        if (!closed) {
          // sleep before retry

          try {
            Thread.sleep(threadWakeFrequency);
          } catch (InterruptedException e) {
            // continue
          }
        }
      }      
    }

    @Override
    protected void initialScan() {
      scanRoot();
      rootScanned = true;
    }

    @Override
    protected void maintenanceScan() {
      scanRoot();
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

    MetaRegion(HServerAddress server, Text regionName, Text startKey) {
      this.server = server;
      this.regionName = regionName;
      this.startKey = startKey;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      return this.compareTo(o) == 0;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      int result = this.regionName.hashCode();
      result ^= this.startKey.hashCode();
      return result;
    }

    // Comparable

    /** {@inheritDoc} */
    public int compareTo(Object o) {
      MetaRegion other = (MetaRegion)o;

      int result = this.regionName.compareTo(other.regionName);
      if(result == 0) {
        result = this.startKey.compareTo(other.startKey);
      }
      return result;
    }
  }

  /** Set by root scanner to indicate the number of meta regions */
  AtomicInteger numberOfMetaRegions;

  /** Work for the meta scanner is queued up here */
  BlockingQueue<MetaRegion> metaRegionsToScan;

  /** These are the online meta regions */
  SortedMap<Text, MetaRegion> onlineMetaRegions;

  /** Set by meta scanner after initial scan */
  volatile boolean initialMetaScanComplete;

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
    /** Constructor */
    public MetaScanner() {
      super(HConstants.META_TABLE_NAME);
    }

    private void scanOneMetaRegion(MetaRegion region) {
      int tries = 0;
      while (!closed && tries < numRetries) {
        while (!closed && !rootScanned && rootRegionLocation.get() == null) {
          try {
            Thread.sleep(threadWakeFrequency);
          } catch (InterruptedException e) {
            // continue
          }
        }
        if (closed) {
          continue;
        }

        try {
          // Don't interrupt us while we're working

          synchronized (metaScannerLock) {
            scanRegion(region);
            onlineMetaRegions.put(region.startKey, region);
          }
          break;

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            try {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);

            } catch (IOException ex) {
              e = ex;
            }
          }
          tries += 1;
          if (tries == 1) {
            LOG.warn("Scan one META region", e);
          } else {
            LOG.error("Scan one META region", e);
          }
        }
        if (!closed) {
          // sleep before retry

          try {
            Thread.sleep(threadWakeFrequency);                  
          } catch (InterruptedException e) {
            //continue
          }
        }
      }
    }

    @Override
    protected void initialScan() {
      MetaRegion region = null;
      while (!closed && region == null && !metaRegionsScanned()) {
        try {
          region =
            metaRegionsToScan.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // continue
        }

        if (region != null) {
          scanOneMetaRegion(region);
        }
      }
      initialMetaScanComplete = true;
    }

    @Override
    protected void maintenanceScan() {
      ArrayList<MetaRegion> regions = new ArrayList<MetaRegion>();
      regions.addAll(onlineMetaRegions.values());
      for (MetaRegion r: regions) {
        scanOneMetaRegion(r);
      }
      metaRegionsScanned();
    }

    /**
     * Called by the meta scanner when it has completed scanning all meta 
     * regions. This wakes up any threads that were waiting for this to happen.
     */
    private synchronized boolean metaRegionsScanned() {
      if (!rootScanned || 
          numberOfMetaRegions.get() != onlineMetaRegions.size()) {
        
        return false;
      }
      LOG.info("all meta regions scanned");
      notifyAll();
      return true;
    }

    /**
     * Other threads call this method to wait until all the meta regions have
     * been scanned.
     */
    synchronized boolean waitForMetaRegionsOrClose() {
      while (!closed) {
        if (rootScanned &&
            numberOfMetaRegions.get() == onlineMetaRegions.size()) {

          break;
        }

        try {
          wait(threadWakeFrequency);
        } catch (InterruptedException e) {
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
  Map<Text, Long> assignAttempts;

  /**
   * Regions that have been assigned, and the server has reported that it has
   * started serving it, but that we have not yet recorded in the meta table.
   */
  Set<Text> pendingRegions;

  /**
   * The 'killList' is a list of regions that are going to be closed, but not
   * reopened.
   */
  Map<String, HashMap<Text, HRegionInfo>> killList;

  /** 'killedRegions' contains regions that are in the process of being closed */
  Set<Text> killedRegions;

  /**
   * 'regionsToDelete' contains regions that need to be deleted, but cannot be
   * until the region server closes it
   */
  Set<Text> regionsToDelete;

  /** 
   * The map of known server names to server info
   * 
   * Access to this map and loadToServers and serversToLoad must be synchronized
   * on this object
   */
  Map<String, HServerInfo> serversToServerInfo;

  /** SortedMap server load -> Set of server names */
  SortedMap<HServerLoad, Set<String>> loadToServers;

  /** Map of server names -> server load */
  Map<String, HServerLoad> serversToLoad;

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
      HRegion.getRegionDir(dir, HGlobals.rootRegionInfo.regionName);
    LOG.info("Root region dir: " + rootRegionDir.toString());

    if (!fs.exists(rootRegionDir)) {
      LOG.info("bootstrap: creating ROOT and first META regions");
      try {
        HRegion root = HRegion.createHRegion(HGlobals.rootRegionInfo, this.dir,
            this.conf, null);
        
        HRegion meta =
          HRegion.createHRegion(new HRegionInfo(1L, HGlobals.metaTableDesc,
              null, null), this.dir, this.conf, null);
        
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
        LOG.error("bootstrap", e);
      }
    }

    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.maxRegionOpenTime =
      conf.getLong("hbase.hbasemaster.maxregionopen", 30 * 1000);
    
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

    this.connection = HConnectionManager.getConnection(conf);

    this.metaRescanInterval =
      conf.getLong("hbase.master.meta.thread.rescanfrequency", 60 * 1000);

    // The root region

    this.rootRegionLocation = new AtomicReference<HServerAddress>();
    this.rootScanned = false;
    this.rootScanner = new RootScanner();
    this.rootScannerThread = new Thread(rootScanner, "HMaster.rootScanner");

    // Scans the meta table

    this.numberOfMetaRegions = new AtomicInteger();
    this.metaRegionsToScan = new LinkedBlockingQueue<MetaRegion>();

    this.onlineMetaRegions = 
      Collections.synchronizedSortedMap(new TreeMap<Text, MetaRegion>());

    this.initialMetaScanComplete = false;

    this.metaScanner = new MetaScanner();
    this.metaScannerThread = new Thread(metaScanner, "HMaster.metaScanner");

    this.unassignedRegions = 
      Collections.synchronizedSortedMap(new TreeMap<Text, HRegionInfo>());

    this.unassignedRegions.put(HGlobals.rootRegionInfo.regionName,
        HGlobals.rootRegionInfo);

    this.assignAttempts = 
      Collections.synchronizedMap(new HashMap<Text, Long>());

    this.assignAttempts.put(HGlobals.rootRegionInfo.regionName,
        Long.valueOf(0L));

    this.pendingRegions =
      Collections.synchronizedSet(new HashSet<Text>());

    this.killList = 
      Collections.synchronizedMap(
          new HashMap<String, HashMap<Text, HRegionInfo>>());

    this.killedRegions =
      Collections.synchronizedSet(new HashSet<Text>());

    this.regionsToDelete =
      Collections.synchronizedSet(new HashSet<Text>());

    this.serversToServerInfo = new HashMap<String, HServerInfo>();
    this.loadToServers = new TreeMap<HServerLoad, Set<String>>();
    this.serversToLoad = new HashMap<String, HServerLoad>();

    // We're almost open for business
    this.closed = false;
    LOG.info("HMaster initialized on " + this.address.toString());
  }

  /** @return HServerAddress of the master server */
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
          LOG.warn("thread start", ex);
        }
      }

      // Something happened during startup. Shut things down.
      
      this.closed = true;
      LOG.error("Failed startup", e);
    }

    /*
     * Main processing loop
     */
     
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
          LOG.debug("Main processing loop: " + op.toString());
        }
      
        if (!op.process()) {
          // Operation would have blocked because not all meta regions are
          // online. This could cause a deadlock, because this thread is waiting
          // for the missing meta region(s) to come back online, but since it
          // is waiting, it cannot process the meta region online operation it
          // is waiting for. So put this operation back on the queue for now.

          if (msgQueue.size() == 0) {
            // The queue is currently empty so wait for a while to see if what
            // we need comes in first

            try {
              Thread.sleep(threadWakeFrequency);
            } catch (InterruptedException e) {
              // continue
            }
          }
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Put " + op.toString() + " back on queue");
            }
            msgQueue.put(op);
          } catch (InterruptedException e) {
            throw new RuntimeException("Putting into msgQueue was interrupted.", e);
          }
        }

      } catch (Exception ex) {
        if (ex instanceof RemoteException) {
          try {
            ex = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) ex);

          } catch (IOException e) {
            LOG.warn("main processing loop: " + op.toString(), e);
          }
        }
        LOG.warn("Processing pending operations: " + op.toString(), ex);
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

    synchronized(rootScannerLock) {
      rootScannerThread.interrupt();    // Wake root scanner
    }
    synchronized(metaScannerLock) {
      metaScannerThread.interrupt();    // Wake meta scanner
    }
    server.stop();                      // Stop server
    serverLeases.close();               // Turn off the lease monitor

    // Join up with all threads

    try {
      rootScannerThread.join();         // Wait for the root scanner to finish.
    } catch (Exception iex) {
      LOG.warn("root scanner", iex);
    }
    try {
      metaScannerThread.join();         // Wait for meta scanner to finish.
    } catch(Exception iex) {
      LOG.warn("meta scanner", iex);
    }
    try {
      // TODO: Maybe do in parallel in its own thread as is done in TaskTracker
      // if its taking a long time to go down.
      
      server.join();                    // Wait for server to finish.
    } catch(InterruptedException iex) {
      LOG.warn("server", iex);
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
  public void regionServerStartup(HServerInfo serverInfo) throws IOException {
    String s = serverInfo.getServerAddress().toString().trim();
    HServerInfo storedInfo = null;
    LOG.info("received start message from: " + s);

    // If we get the startup message but there's an old server by that
    // name, then we can timeout the old one right away and register
    // the new one.

    synchronized (serversToServerInfo) {
      storedInfo = serversToServerInfo.remove(s);
      HServerLoad load = serversToLoad.remove(s);
    
      if (load != null) {
        Set<String> servers = loadToServers.get(load);
        if (servers != null) {
          servers.remove(s);
          loadToServers.put(load, servers);
        }
      }
      serversToServerInfo.notifyAll();
    }
    if (storedInfo != null && !closed) {
      try {
        msgQueue.put(new PendingServerShutdown(storedInfo));
      } catch (InterruptedException e) {
        throw new RuntimeException("Putting into msgQueue was interrupted.", e);
      }
    }

    // Either way, record the new server

    synchronized (serversToServerInfo) {
      HServerLoad load = new HServerLoad();
      serverInfo.setLoad(load);
      serversToServerInfo.put(s, serverInfo);
      serversToLoad.put(s, load);
      Set<String> servers = loadToServers.get(load);
      if (servers == null) {
        servers = new HashSet<String>();
      }
      servers.add(s);
      loadToServers.put(load, servers);
    }

    if (!closed) {
      long serverLabel = getServerLabel(s);
      serverLeases.createLease(serverLabel, serverLabel, new ServerExpirer(s));
    }
  }

  private long getServerLabel(final String s) {
    return s.hashCode();
  }

  /** {@inheritDoc} */
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[])
  throws IOException {
    
    String serverName = serverInfo.getServerAddress().toString().trim();
    long serverLabel = getServerLabel(serverName);

    if (msgs.length > 0 && msgs[0].getMsg() == HMsg.MSG_REPORT_EXITING) {

      // HRegionServer is shutting down. Cancel the server's lease.
      // Note that cancelling the server's lease takes care of updating
      // serversToServerInfo, etc.

      if (cancelLease(serverName, serverLabel)) {
        // Only process the exit message if the server still has a lease.
        // Otherwise we could end up processing the server exit twice.

        LOG.info("Region server " + serverName + ": MSG_REPORT_EXITING");

        // Get all the regions the server was serving reassigned
        // (if we are not shutting down).

        if (!closed) {
          for (int i = 1; i < msgs.length; i++) {
            HRegionInfo info = msgs[i].getRegionInfo();

            if (info.tableDesc.getName().equals(ROOT_TABLE_NAME)) {
              rootRegionLocation.set(null);

            } else if (info.tableDesc.getName().equals(META_TABLE_NAME)) {
              onlineMetaRegions.remove(info.getStartKey());
            }

            unassignedRegions.put(info.regionName, info);
            assignAttempts.put(info.regionName, Long.valueOf(0L));
          }
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
    
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_STOP)};
    }

    HServerInfo storedInfo;
    synchronized (serversToServerInfo) {
      storedInfo = serversToServerInfo.get(serverName);
    }
    if(storedInfo == null) {
      if(LOG.isDebugEnabled()) {
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

      cancelLease(serverName, serverLabel);
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_STOP)};

    } else {

      // All's well.  Renew the server's lease.
      // This will always succeed; otherwise, the fetch of serversToServerInfo
      // would have failed above.

      serverLeases.renewLease(serverLabel, serverLabel);

      // Refresh the info object and the load information

      synchronized (serversToServerInfo) {
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
      }

      // Next, process messages for this server
      return processMsgs(serverInfo, msgs);
    }
  }

  /** Cancel a server's lease and update its load information */
  private boolean cancelLease(final String serverName, final long serverLabel) {
    boolean leaseCancelled = false;
    synchronized (serversToServerInfo) {
      HServerInfo info = serversToServerInfo.remove(serverName);
      if (info != null) {
        // Only cancel lease and update load information once.
        // This method can be called a couple of times during shutdown.

        LOG.info("Cancelling lease for " + serverName);
        serverLeases.cancelLease(serverLabel, serverLabel);
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
      serversToServerInfo.notifyAll();
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
    HashMap<Text, HRegionInfo> regionsToKill = killList.remove(serverName);

    // Get reports on what the RegionServer did.

    for (int i = 0; i < incomingMsgs.length; i++) {
      HRegionInfo region = incomingMsgs[i].getRegionInfo();

      switch (incomingMsgs[i].getMsg()) {

      case HMsg.MSG_REPORT_OPEN:
        HRegionInfo regionInfo = unassignedRegions.get(region.regionName);

        if (regionInfo == null) {

          if (LOG.isDebugEnabled()) {
            LOG.debug("region server " + info.getServerAddress().toString()
                + " should not have opened region " + region.regionName);
          }

          // This Region should not have been opened.
          // Ask the server to shut it down, but don't report it as closed.  
          // Otherwise the HMaster will think the Region was closed on purpose, 
          // and then try to reopen it elsewhere; that's not what we want.

          returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT, region)); 

        } else {
          LOG.info(info.getServerAddress().toString() + " serving " +
              region.regionName);

          // Remove from unassigned list so we don't assign it to someone else

          unassignedRegions.remove(region.regionName);
          assignAttempts.remove(region.regionName);

          if (region.regionName.compareTo(
              HGlobals.rootRegionInfo.regionName) == 0) {

            // Store the Root Region location (in memory)

            synchronized (rootRegionLocation) {
              rootRegionLocation.set(new HServerAddress(info.getServerAddress()));
              rootRegionLocation.notifyAll();
            }
            break;
          }

          // Note that the table has been assigned and is waiting for the meta
          // table to be updated.

          pendingRegions.add(region.regionName);

          // Queue up an update to note the region location.

          try {
            msgQueue.put(new PendingOpenReport(info, region));
          } catch (InterruptedException e) {
            throw new RuntimeException("Putting into msgQueue was interrupted.", e);
          }
        }
        break;

      case HMsg.MSG_REPORT_CLOSE:
        LOG.info(info.getServerAddress().toString() + " no longer serving " +
            region.regionName);

        if (region.regionName.compareTo(
            HGlobals.rootRegionInfo.regionName) == 0) {
          
          // Root region
          
          rootRegionLocation.set(null);
          unassignedRegions.put(region.regionName, region);
          assignAttempts.put(region.regionName, Long.valueOf(0L));

        } else {
          boolean reassignRegion = true;
          boolean deleteRegion = false;

          if (killedRegions.remove(region.regionName)) {
            reassignRegion = false;
          }

          if (regionsToDelete.remove(region.regionName)) {
            reassignRegion = false;
            deleteRegion = true;
          }

          // NOTE: we cannot put the region into unassignedRegions as that
          //       could create a race with the pending close if it gets 
          //       reassigned before the close is processed.

          unassignedRegions.remove(region.regionName);
          assignAttempts.remove(region.regionName);

          try {
            msgQueue.put(new PendingCloseReport(region, reassignRegion,
                deleteRegion));
            
          } catch (InterruptedException e) {
            throw new RuntimeException("Putting into msgQueue was interrupted.", e);
          }
        }
        break;

      case HMsg.MSG_REPORT_SPLIT:
        // A region has split.
        
        HRegionInfo newRegionA = incomingMsgs[++i].getRegionInfo();
        unassignedRegions.put(newRegionA.getRegionName(), newRegionA);
        assignAttempts.put(newRegionA.getRegionName(), Long.valueOf(0L));

        HRegionInfo newRegionB = incomingMsgs[++i].getRegionInfo();
        unassignedRegions.put(newRegionB.getRegionName(), newRegionB);
        assignAttempts.put(newRegionB.getRegionName(), Long.valueOf(0L));

        LOG.info("region " + region.regionName + " split. New regions are: "
            + newRegionA.regionName + ", " + newRegionB.regionName);

        if (region.tableDesc.getName().equals(META_TABLE_NAME)) {
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
        killedRegions.add(i.regionName);
      }
    }

    // Figure out what the RegionServer ought to do, and write back.

    assignRegions(info, serverName, returnMsgs);
    return returnMsgs.toArray(new HMsg[returnMsgs.size()]);
  }

  /**
   * Assigns regions to region servers attempting to balance the load across
   * all region servers
   * 
   * @param info
   * @param serverName
   * @param returnMsgs
   */
  private void assignRegions(HServerInfo info, String serverName,
      ArrayList<HMsg> returnMsgs) {

    long now = System.currentTimeMillis();
    TreeSet<Text> regionsToAssign = new TreeSet<Text>();
    for (Map.Entry<Text, Long> e: assignAttempts.entrySet()) {
      if (now - e.getValue() > maxRegionOpenTime) {
        regionsToAssign.add(e.getKey());
      }
    }
    int nRegionsToAssign = regionsToAssign.size();

    if (nRegionsToAssign > 0) {
      if (serversToServerInfo.size() == 1) {
        // Only one server. An unlikely case but still possible.
        // Assign all unassigned regions to it.

        for (Text regionName: regionsToAssign) {
          HRegionInfo regionInfo = unassignedRegions.get(regionName);
          LOG.info("assigning region " + regionName + " to server " +
              serverName);

          assignAttempts.put(regionName, Long.valueOf(now));
          returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));
        }

      } else {
        // Multiple servers in play.
        // We need to allocate regions only to most lightly loaded servers.

        HServerLoad thisServersLoad = info.getLoad();

        synchronized (serversToServerInfo) {
          SortedMap<HServerLoad, Set<String>> lightServers =
            loadToServers.headMap(thisServersLoad);

          // How many regions we can assign to more lightly loaded servers?

          int nregions = 0;
          for (Map.Entry<HServerLoad, Set<String>> e: lightServers.entrySet()) {
            HServerLoad lightLoad =
              new HServerLoad(e.getKey().getNumberOfRequests(),
                  e.getKey().getNumberOfRegions());

            do {
              lightLoad.setNumberOfRegions(lightLoad.getNumberOfRegions() + 1);
              nregions += 1;

            } while (lightLoad.compareTo(thisServersLoad) <= 0 &&
                nregions < nRegionsToAssign);

            nregions *= e.getValue().size();

            if (nregions >= nRegionsToAssign) {
              break;
            }
          }

          nRegionsToAssign -= nregions;
          if (nRegionsToAssign > 0) {
            // We still have more regions to assign. See how many we can assign
            // before this server becomes more heavily loaded than the next
            // most heavily loaded server.

            SortedMap<HServerLoad, Set<String>> heavyServers =
              loadToServers.tailMap(thisServersLoad);
            int nservers = 0;
            HServerLoad heavierLoad = null;
            for (Map.Entry<HServerLoad, Set<String>> e:
              heavyServers.entrySet()) {

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
              load.setNumberOfRegions(load.getNumberOfRegions() + 1),
              nregions++) {
              }
            }

            if (nregions < nRegionsToAssign) {
              // There are some more heavily loaded servers
              // but we can't assign all the regions to this server.

              if (nservers > 0) {
                // There are other servers that can share the load.
                // Split regions that need assignment across the servers.

                nregions =
                  (int) Math.ceil((1.0 * nRegionsToAssign) / (1.0 * nservers));

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

            for (Map.Entry<Text, HRegionInfo> e: unassignedRegions.entrySet()) {
              Text regionName = e.getKey();
              HRegionInfo regionInfo = e.getValue();
              LOG.info("assigning region " + regionName + " to server " +
                  serverName);

              assignAttempts.put(regionName, Long.valueOf(now));
              returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));

              if (--nregions <= 0) {
                break;
              }
            }
          }
        }
      }
    }
  }

  /*
   * Some internal classes to manage msg-passing and client operations
   */

  private abstract class PendingOperation {
    PendingOperation() {
      super();
    }

    abstract boolean process() throws IOException;
  }

  /** 
   * Instantiated when a server's lease has expired, meaning it has crashed.
   * The region server's log file needs to be split up for each region it was
   * serving, and the regions need to get reassigned.
   */
  private class PendingServerShutdown extends PendingOperation {
    private HServerAddress deadServer;
    private String deadServerName;
    private transient boolean logSplit;
    private transient boolean rootChecked;
    private transient boolean rootRescanned;

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
      this.logSplit = false;
      this.rootChecked = false;
      this.rootRescanned = false;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "PendingServerShutdown of " + this.deadServer.toString();
    }

    /** Finds regions that the dead region server was serving */
    private void scanMetaRegion(HRegionInterface server, long scannerId,
        Text regionName) throws IOException {

      ArrayList<ToDoEntry> toDoList = new ArrayList<ToDoEntry>();
      TreeMap<Text, HRegionInfo> regions = new TreeMap<Text, HRegionInfo>();

      try {
        while (true) {
          MapWritable values = null;

          try {
            values = server.next(scannerId);

          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);

            }
            LOG.error("Shutdown scanning of meta region", e);
            break;
          }

          if (values == null || values.size() == 0) {
            break;
          }

          SortedMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          Text row = null;
          for (Map.Entry<WritableComparable, Writable> e: values.entrySet()) {
            HStoreKey key = (HStoreKey) e.getKey();
            Text thisRow = key.getRow();
            if (row == null) {
              row = thisRow;
            } else {
              if (!row.equals(thisRow)) {
                LOG.error("Multiple rows in same scanner result set. firstRow="
                    + row + ", currentRow=" + thisRow);
              }
            }
            results.put(key.getColumn(),
                ((ImmutableBytesWritable) e.getValue()).get());
          }

          if (LOG.isDebugEnabled() && row != null) {
            LOG.debug("shutdown scanner looking at " + row.toString());
          }

          // Check server name.  If null, be conservative and treat as though
          // region had been on shutdown server (could be null because we
          // missed edits in hlog because hdfs does not do write-append).

          String serverName;
          try {
            serverName = Writables.bytesToString(results.get(COL_SERVER));

          } catch(UnsupportedEncodingException e) {
            LOG.error("Server name", e);
            break;
          }
          if (serverName.length() > 0 &&
              deadServerName.compareTo(serverName) != 0) {
          
            // This isn't the server you're looking for - move along
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("Server name " + serverName + " is not same as " +
                  deadServerName + ": Passing");
            }
            continue;
          }

          // Bingo! Found it.
          
          HRegionInfo info = null;
          try {
            info = (HRegionInfo) Writables.getWritable(
                results.get(COL_REGIONINFO), new HRegionInfo());
            
          } catch (IOException e) {
            LOG.error("Read fields", e);
            break;
          }
          LOG.info(info.getRegionName() + " was on shutdown server <" +
              serverName + "> (or server is null). Marking unassigned if " +
          "meta and clearing pendingRegions");

          if (info.tableDesc.getName().equals(META_TABLE_NAME)) {
            onlineMetaRegions.remove(info.getStartKey());
          }

          ToDoEntry todo = new ToDoEntry(row, info);
          toDoList.add(todo);

          if (killList.containsKey(deadServerName)) {
            HashMap<Text, HRegionInfo> regionsToKill =
              killList.get(deadServerName);

            if (regionsToKill.containsKey(info.regionName)) {
              regionsToKill.remove(info.regionName);
              killList.put(deadServerName, regionsToKill);
              unassignedRegions.remove(info.regionName);
              assignAttempts.remove(info.regionName);
            
              if (regionsToDelete.contains(info.regionName)) {
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
           
            // If it was pending, remove.
            // Otherwise will obstruct its getting reassigned.
            
            pendingRegions.remove(info.getRegionName());
          }
        }

      } finally {
        if(scannerId != -1L) {
          try {
            server.close(scannerId);

          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);
            }
            LOG.error("Closing scanner", e);
          }
        }
      }

      // Remove server from root/meta entries
      
      for (ToDoEntry e: toDoList) {
        BatchUpdate b = new BatchUpdate();
        long lockid = b.startUpdate(e.row);
      
        if (e.deleteRegion) {
          b.delete(lockid, COL_REGIONINFO);
        
        } else if (e.regionOffline) {
          e.info.offLine = true;
          b.put(lockid, COL_REGIONINFO, Writables.getBytes(e.info));
        }
        b.delete(lockid, COL_SERVER);
        b.delete(lockid, COL_STARTCODE);
        server.batchUpdate(regionName, System.currentTimeMillis(), b);
      }

      // Get regions reassigned

      for (Map.Entry<Text, HRegionInfo> e: regions.entrySet()) {
        Text region = e.getKey();
        HRegionInfo regionInfo = e.getValue();

        unassignedRegions.put(region, regionInfo);
        assignAttempts.put(region, Long.valueOf(0L));
      }
    }

    @Override
    boolean process() throws IOException {
      LOG.info("process shutdown of server " + deadServer + ": logSplit: " +
          this.logSplit + ", rootChecked: " + this.rootChecked +
          ", rootRescanned: " + this.rootRescanned + ", numberOfMetaRegions: " +
          numberOfMetaRegions.get() + ", onlineMetaRegions.size(): " +
          onlineMetaRegions.size());

      if (!logSplit) {
        // Process the old log file

        HLog.splitLog(dir, new Path(dir, "log" + "_" +
            deadServer.getBindAddress() + "_" + deadServer.getPort()), fs, conf);

        logSplit = true;
      }

      if (!rootChecked) {
        if (rootRegionLocation.get() != null &&
            deadServer.equals(rootRegionLocation.get())) {

          rootRegionLocation.set(null);
          unassignedRegions.put(HGlobals.rootRegionInfo.regionName,
              HGlobals.rootRegionInfo);

          assignAttempts.put(HGlobals.rootRegionInfo.regionName,
              Long.valueOf(0L));
        }
        rootChecked = true;
      }

      if (!rootRescanned) {
        // Scan the ROOT region

        HRegionInterface server = null;
        long scannerId = -1L;
        for (int tries = 0; tries < numRetries; tries ++) {
          if (closed) {
            return true;
          }
          if (rootRegionLocation.get() == null || !rootScanned) {
            // We can't proceed until the root region is online and has been scanned
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("process server shutdown scanning root region " +
              "cancelled because rootRegionLocation is null");
            }
            return false;
          }
          server = connection.getHRegionConnection(rootRegionLocation.get());
          scannerId = -1L;

          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("process server shutdown scanning root region on " +
                  rootRegionLocation.get().getBindAddress());
            }
            scannerId = server.openScanner(HGlobals.rootRegionInfo.regionName,
                COLUMN_FAMILY_ARRAY, EMPTY_START_ROW,
                System.currentTimeMillis(), null);
            
            scanMetaRegion(server, scannerId, HGlobals.rootRegionInfo.regionName);
            break;

          } catch (IOException e) {
            if (tries == numRetries - 1) {
              if (e instanceof RemoteException) {
                e = RemoteExceptionHandler.decodeRemoteException(
                    (RemoteException) e);
              }
              throw e;
            }
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("process server shutdown scanning root region on " +
              rootRegionLocation.get().getBindAddress() + " finished " +
              Thread.currentThread().getName());
        }
        rootRescanned = true;
      }

      for (int tries = 0; tries < numRetries; tries++) {
        try {
          if (closed) {
            return true;
          }
          if (!rootScanned ||
              numberOfMetaRegions.get() != onlineMetaRegions.size()) {
            // We can't proceed because not all of the meta regions are online.
            // We can't block either because that would prevent the meta region
            // online message from being processed. So return false to have this
            // operation requeued.
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("Requeuing shutdown because rootScanned: " +
                  rootScanned + ", numberOfMetaRegions: " +
                  numberOfMetaRegions.get() + ", onlineMetaRegions.size(): " +
                  onlineMetaRegions.size());
            }
            return false;
          }

          for (MetaRegion r: onlineMetaRegions.values()) {

            HRegionInterface server = null;
            long scannerId = -1L;

            if (LOG.isDebugEnabled()) {
              LOG.debug("process server shutdown scanning " + r.regionName +
                  " on " + r.server + " " + Thread.currentThread().getName());
            }
            server = connection.getHRegionConnection(r.server);

            scannerId = server.openScanner(r.regionName, COLUMN_FAMILY_ARRAY,
                EMPTY_START_ROW, System.currentTimeMillis(), null);
            
            scanMetaRegion(server, scannerId, r.regionName);
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("process server shutdown finished scanning " +
                  r.regionName + " on " + r.server + " " +
                  Thread.currentThread().getName());
            }
          }
          break;

        } catch (IOException e) {
          if (tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);
            }
            throw e;
          }
        }
      }
      return true;
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

      if (this.regionInfo.tableDesc.getName().equals(META_TABLE_NAME)) {
        this.rootRegion = true;

      } else {
        this.rootRegion = false;
      }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "PendingCloseReport of " + this.regionInfo.getRegionName();
    }

    @Override
    boolean process() throws IOException {
      for (int tries = 0; tries < numRetries; tries++) {
        if (closed) {
          return true;
        }
        LOG.info("region closed: " + regionInfo.regionName);

        // Mark the Region as unavailable in the appropriate meta table

        Text metaRegionName;
        HRegionInterface server;
        if (rootRegion) {
          if (rootRegionLocation.get() == null || !rootScanned) {
            // We can't proceed until the root region is online and has been
            // scanned
            return false;
          }
          metaRegionName = HGlobals.rootRegionInfo.regionName;
          server = connection.getHRegionConnection(rootRegionLocation.get());
          onlineMetaRegions.remove(regionInfo.getStartKey());

        } else {
          if (!rootScanned ||
              numberOfMetaRegions.get() != onlineMetaRegions.size()) {
            
            // We can't proceed because not all of the meta regions are online.
            // We can't block either because that would prevent the meta region
            // online message from being processed. So return false to have this
            // operation requeued.
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("Requeuing close because rootScanned=" +
                  rootScanned + ", numberOfMetaRegions=" +
                  numberOfMetaRegions.get() + ", onlineMetaRegions.size()=" +
                  onlineMetaRegions.size());
            }
            return false;
          }

          MetaRegion r = null;
          if (onlineMetaRegions.containsKey(regionInfo.getRegionName())) {
            r = onlineMetaRegions.get(regionInfo.getRegionName());

          } else {
            r = onlineMetaRegions.get(onlineMetaRegions.headMap(
                regionInfo.getRegionName()).lastKey());
          }
          metaRegionName = r.regionName;
          server = connection.getHRegionConnection(r.server);
        }

        try {
          BatchUpdate b = new BatchUpdate();
          long lockid = b.startUpdate(regionInfo.regionName);

          if (deleteRegion) {
            b.delete(lockid, COL_REGIONINFO);

          } else if (!reassignRegion ) {
            regionInfo.offLine = true;
            b.put(lockid, COL_REGIONINFO, Writables.getBytes(regionInfo));
          }
          b.delete(lockid, COL_SERVER);
          b.delete(lockid, COL_STARTCODE);
          server.batchUpdate(metaRegionName, System.currentTimeMillis(), b);

          break;

        } catch (IOException e) {
          if (tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);
            }
            throw e;
          }
          continue;
        }
      }

      if (reassignRegion) {
        LOG.info("reassign region: " + regionInfo.regionName);

        unassignedRegions.put(regionInfo.regionName, regionInfo);
        assignAttempts.put(regionInfo.regionName, Long.valueOf(0L));

      } else if (deleteRegion) {
        try {
          HRegion.deleteRegion(fs, dir, regionInfo.regionName);

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) e);
          }
          LOG.error("failed delete region " + regionInfo.regionName, e);
          throw e;
        }
      }
      return true;
    }
  }

  /** 
   * PendingOpenReport is instantiated when a region server reports that it is
   * serving a region. This applies to all meta and user regions except the 
   * root region which is handled specially.
   */
  private class PendingOpenReport extends PendingOperation {
    private boolean rootRegion;
    private HRegionInfo region;
    private HServerAddress serverAddress;
    private byte [] startCode;

    PendingOpenReport(HServerInfo info, HRegionInfo region) throws IOException {
      if (region.tableDesc.getName().equals(META_TABLE_NAME)) {
        // The region which just came on-line is a META region.
        // We need to look in the ROOT region for its information.
        
        this.rootRegion = true;
      
      } else {
        // Just an ordinary region. Look for it in the META table.
      
        this.rootRegion = false;
      }
      this.region = region;
      this.serverAddress = info.getServerAddress();
      this.startCode = Writables.longToBytes(info.getStartCode());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "PendingOpenOperation from " + serverAddress.toString();
    }

    @Override
    boolean process() throws IOException {
      for (int tries = 0; tries < numRetries; tries++) {
        if (closed) {
          return true;
        }
        LOG.info(region.getRegionName() + " open on " + 
            this.serverAddress.toString());

        // Register the newly-available Region's location.

        Text metaRegionName;
        HRegionInterface server;
        if (rootRegion) {
          if (rootRegionLocation.get() == null || !rootScanned) {
            // We can't proceed until the root region is online and has been scanned
            if (LOG.isDebugEnabled()) {
              LOG.debug("root region: " + 
                ((rootRegionLocation != null)?
                  rootRegionLocation.toString(): "null") +
                ", rootScanned: " + rootScanned);
            }
            return false;
          }
          metaRegionName = HGlobals.rootRegionInfo.regionName;
          server = connection.getHRegionConnection(rootRegionLocation.get());

        } else {
          if (!rootScanned ||
              numberOfMetaRegions.get() != onlineMetaRegions.size()) {

            // We can't proceed because not all of the meta regions are online.
            // We can't block either because that would prevent the meta region
            // online message from being processed. So return false to have this
            // operation requeued.

            if (LOG.isDebugEnabled()) {
              LOG.debug("Requeuing open because rootScanned: " +
                  rootScanned + ", numberOfMetaRegions: " +
                  numberOfMetaRegions.get() + ", onlineMetaRegions.size(): " +
                  onlineMetaRegions.size());
            }
            return false;
          }

          MetaRegion r = null;
          if (onlineMetaRegions.containsKey(region.getRegionName())) {
            r = onlineMetaRegions.get(region.getRegionName());

          } else {
            r = onlineMetaRegions.get(onlineMetaRegions.headMap(
                region.getRegionName()).lastKey());
          }
          metaRegionName = r.regionName;
          server = connection.getHRegionConnection(r.server);
        }
        LOG.info("updating row " + region.getRegionName() + " in table " +
            metaRegionName);

        try {
          BatchUpdate b = new BatchUpdate();
          long lockid = b.startUpdate(region.getRegionName());
          
          b.put(lockid, COL_SERVER,
              Writables.stringToBytes(serverAddress.toString()));
          
          b.put(lockid, COL_STARTCODE, startCode);
          server.batchUpdate(metaRegionName, System.currentTimeMillis(), b);

          if (region.tableDesc.getName().equals(META_TABLE_NAME)) {
            // It's a meta region.

            MetaRegion m =
              new MetaRegion(serverAddress, region.regionName, region.startKey);

            if (!initialMetaScanComplete) {
              // Put it on the queue to be scanned for the first time.

              try {
                metaRegionsToScan.put(m);
              
              } catch (InterruptedException e) {
                throw new RuntimeException(
                    "Putting into metaRegionsToScan was interrupted.", e);
              }
            } else {
              // Add it to the online meta regions

              onlineMetaRegions.put(region.startKey, m);
            }
          }
          // If updated successfully, remove from pending list.
          
          pendingRegions.remove(region.getRegionName());
          break;

        } catch (IOException e) {
          if (tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);
            }
            throw e;
          }
        }
      }
      return true;
    }
  }

  /*
   * HMasterInterface
   */

  /** {@inheritDoc} */
  public boolean isMasterRunning() {
    return !closed;
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo newRegion = new HRegionInfo(rand.nextLong(), desc, null, null);

    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // We can not access meta regions if they have not already been
        // assigned and scanned.  If we timeout waiting, just shutdown.
    
        if (metaScanner.waitForMetaRegionsOrClose()) {
          break;
        }
        createTable(newRegion);
        LOG.info("created table " + desc.getName());
        break;
      
      } catch (IOException e) {
        if (tries == numRetries - 1) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) e);
          }
          throw e;
        }
      }
    }
  }

  /* Set of tables currently in creation. Access needs to be synchronized. */
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
      
      MetaRegion m = (onlineMetaRegions.containsKey(newRegion.regionName) ?
          onlineMetaRegions.get(newRegion.regionName) :
            onlineMetaRegions.get(onlineMetaRegions.headMap(
                newRegion.getTableDesc().getName()).lastKey()));
          
      Text metaRegionName = m.regionName;
      HRegionInterface server = connection.getHRegionConnection(m.server);
      long scannerid = server.openScanner(metaRegionName, COL_REGIONINFO_ARRAY,
          tableName, System.currentTimeMillis(), null);
      try {
        MapWritable data = server.next(scannerid);
            
        // Test data and that the row for the data is for our table. If table
        // does not exist, scanner will return row after where our table would
        // be inserted if it exists so look for exact match on table name.
            
        if (data != null && data.size() > 0) {
          for (WritableComparable k: data.keySet()) {
            if (HRegionInfo.getTableNameFromRegionName(
                ((HStoreKey) k).getRow()).equals(tableName)) {
          
              // Then a region for this table already exists. Ergo table exists.
                  
              throw new TableExistsException(tableName.toString());
            }
          }
        }
            
      } finally {
        server.close(scannerid);
      }

      // 2. Create the HRegion
          
      HRegion region =
        HRegion.createHRegion(newRegion, this.dir, this.conf, null);

      // 3. Insert into meta
          
      HRegionInfo info = region.getRegionInfo();
      Text regionName = region.getRegionName();
      BatchUpdate b = new BatchUpdate();
      long lockid = b.startUpdate(regionName);
      b.put(lockid, COL_REGIONINFO, Writables.getBytes(info));
      server.batchUpdate(metaRegionName, System.currentTimeMillis(), b);

      // 4. Close the new region to flush it to disk.  Close its log file too.
      
      region.close();
      region.getLog().closeAndDelete();

      // 5. Get it assigned to a server
      
      unassignedRegions.put(regionName, info);
      assignAttempts.put(regionName, Long.valueOf(0L));

    } finally {
      synchronized (tableInCreation) {
        tableInCreation.remove(newRegion.getTableDesc().getName());
      }
    }
  }

  /** {@inheritDoc} */
  public void deleteTable(Text tableName) throws IOException {
    new TableDelete(tableName).process();
    LOG.info("deleted table: " + tableName);
  }

  /** {@inheritDoc} */
  public void addColumn(Text tableName, HColumnDescriptor column)
  throws IOException {
    
    new AddColumn(tableName, column).process();
  }

  /** {@inheritDoc} */
  public void deleteColumn(Text tableName, Text columnName) throws IOException {
    new DeleteColumn(tableName, HStoreKey.extractFamily(columnName)).process();
  }

  /** {@inheritDoc} */
  public void enableTable(Text tableName) throws IOException {
    new ChangeTableState(tableName, true).process();
  }

  /** {@inheritDoc} */
  public void disableTable(Text tableName) throws IOException {
    new ChangeTableState(tableName, false).process();
  }

  /** {@inheritDoc} */
  public HServerAddress findRootRegion() {
    return rootRegionLocation.get();
  }

  /*
   * Helper classes for HMasterInterface
   */

  private abstract class TableOperation {
    private Set<MetaRegion> metaRegions;
    protected Text tableName;
    protected Set<HRegionInfo> unservedRegions;

    protected TableOperation(Text tableName) throws IOException {
      if (!isMasterRunning()) {
        throw new MasterNotRunningException();
      }

      this.metaRegions = new HashSet<MetaRegion>();
      this.tableName = tableName;
      this.unservedRegions = new HashSet<HRegionInfo>();

      // We can not access any meta region if they have not already been
      // assigned and scanned.

      if (metaScanner.waitForMetaRegionsOrClose()) {
        throw new MasterNotRunningException(); // We're shutting down. Forget it.
      }

      Text firstMetaRegion = null;
      if (onlineMetaRegions.size() == 1) {
        firstMetaRegion = onlineMetaRegions.firstKey();

      } else if (onlineMetaRegions.containsKey(tableName)) {
        firstMetaRegion = tableName;

      } else {
        firstMetaRegion = onlineMetaRegions.headMap(tableName).lastKey();
      }

      this.metaRegions.addAll(onlineMetaRegions.tailMap(firstMetaRegion).values());
    }

    void process() throws IOException {
      for (int tries = 0; tries < numRetries; tries++) {
        boolean tableExists = false;
        try {
          synchronized(metaScannerLock) {     // Prevent meta scanner from running
            for (MetaRegion m: metaRegions) {

              // Get a connection to a meta server

              HRegionInterface server = connection.getHRegionConnection(m.server);

              // Open a scanner on the meta region

              long scannerId =
                server.openScanner(m.regionName, COLUMN_FAMILY_ARRAY, tableName,
                    System.currentTimeMillis(), null);

              try {
                while (true) {
                  HRegionInfo info = new HRegionInfo();
                  String serverName = null;
                  long startCode = -1L;

                  MapWritable values = server.next(scannerId);
                  if(values == null || values.size() == 0) {
                    break;
                  }
                  boolean haveRegionInfo = false;
                  for (Map.Entry<WritableComparable, Writable> e:
                    values.entrySet()) {

                    byte[] value = ((ImmutableBytesWritable) e.getValue()).get();
                    if (value == null || value.length == 0) {
                      break;
                    }
                    HStoreKey key = (HStoreKey) e.getKey();
                    Text column = key.getColumn();
                    if (column.equals(COL_REGIONINFO)) {
                      haveRegionInfo = true;
                      info = (HRegionInfo) Writables.getWritable(value, info);
                    
                    } else if (column.equals(COL_SERVER)) {
                      try {
                        serverName =
                          Writables.bytesToString(value);
                    
                      } catch (UnsupportedEncodingException ex) {
                        assert(false);
                      }
                    
                    } else if (column.equals(COL_STARTCODE)) {
                      try {
                        startCode = Writables.bytesToLong(value);
                      
                      } catch (UnsupportedEncodingException ex) {
                        assert(false);
                      }
                    }
                  }

                  if (!haveRegionInfo) {
                    throw new IOException(COL_REGIONINFO + " not found");
                  }

                  if (info.tableDesc.getName().compareTo(tableName) > 0) {
                    break;               // Beyond any more entries for this table
                  }

                  tableExists = true;
                  if (!isBeingServed(serverName, startCode)) {
                    unservedRegions.add(info);
                  }
                  processScanItem(serverName, startCode, info);

                } // while(true)

              } finally {
                if (scannerId != -1L) {
                  try {
                    server.close(scannerId);

                  } catch (IOException e) {
                    if (e instanceof RemoteException) {
                      e = RemoteExceptionHandler.decodeRemoteException(
                          (RemoteException) e);
                    }
                    LOG.error("", e);
                  }
                }
                scannerId = -1L;
              }

              if (!tableExists) {
                throw new IOException(tableName + " does not exist");
              }

              postProcessMeta(m, server);
              unservedRegions.clear();

            } // for(MetaRegion m:)
          } // synchronized(metaScannerLock)

        } catch (IOException e) {
          if (tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);
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
      if (serverName != null && serverName.length() > 0 && startCode != -1L) {
        HServerInfo s;
        synchronized (serversToServerInfo) {
          s = serversToServerInfo.get(serverName);
        }
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
        HRegionInterface server) throws IOException;
  }

  /** Instantiated to enable or disable a table */
  private class ChangeTableState extends TableOperation {
    private boolean online;

    protected Map<String, HashSet<HRegionInfo>> servedRegions =
      new HashMap<String, HashSet<HRegionInfo>>();
    
    protected long lockid;

    ChangeTableState(Text tableName, boolean onLine) throws IOException {
      super(tableName);
      this.online = onLine;
    }

    @Override
    protected void processScanItem(String serverName, long startCode,
        HRegionInfo info) {
    
      if (isBeingServed(serverName, startCode)) {
        HashSet<HRegionInfo> regions = servedRegions.get(serverName);
        if (regions == null) {
          regions = new HashSet<HRegionInfo>();
        }
        regions.add(info);
        servedRegions.put(serverName, regions);
      }
    }

    @Override
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
    throws IOException {
      
      // Process regions not being served
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("processing unserved regions");
      }
      for (HRegionInfo i: unservedRegions) {
        if (i.offLine && i.isSplit()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping region " + i.toString() + " because it is " +
                "offline because it has been split");
          }
          continue;
        }
        
        // Update meta table
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("updating columns in row: " + i.regionName);
        }

        BatchUpdate b = new BatchUpdate();
        lockid = b.startUpdate(i.regionName);
        updateRegionInfo(b, i);
        b.delete(lockid, COL_SERVER);
        b.delete(lockid, COL_STARTCODE);

        for (int tries = 0; tries < numRetries; tries++) {
          try {
            server.batchUpdate(m.regionName, System.currentTimeMillis(), b);
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("updated columns in row: " + i.regionName);
            }
            break;

          } catch (IOException e) {
            if (tries == numRetries - 1) {
              if (e instanceof RemoteException) {
                e = RemoteExceptionHandler.decodeRemoteException(
                    (RemoteException) e);
              }
              LOG.error("column update failed in row: " + i.regionName, e);
              break;
            }
          }
          try {
            Thread.sleep(threadWakeFrequency);

          } catch (InterruptedException e) {
            // continue
          }
        }

        if (online) {                           // Bring offline regions on-line
          if (!unassignedRegions.containsKey(i.regionName)) {
            unassignedRegions.put(i.regionName, i);
            assignAttempts.put(i.regionName, Long.valueOf(0L));
          }

        } else {                                // Prevent region from getting assigned.
          unassignedRegions.remove(i.regionName);
          assignAttempts.remove(i.regionName);
        }
      }

      // Process regions currently being served

      if (LOG.isDebugEnabled()) {
        LOG.debug("processing regions currently being served");
      }
      for (Map.Entry<String, HashSet<HRegionInfo>> e: servedRegions.entrySet()) {
        String serverName = e.getKey();
        if (online) {
          LOG.debug("Already online");
          continue;                             // Already being served
        }

        // Cause regions being served to be taken off-line and disabled

        HashMap<Text, HRegionInfo> localKillList = killList.get(serverName);
        if (localKillList == null) {
          localKillList = new HashMap<Text, HRegionInfo>();
        }
        for (HRegionInfo i: e.getValue()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("adding region " + i.regionName + " to local kill list");
          }
          localKillList.put(i.regionName, i);
        }
        if (localKillList.size() > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("inserted local kill list into kill list for server " +
                serverName);
          }
          killList.put(serverName, localKillList);
        }
      }
      servedRegions.clear();
    }

    protected void updateRegionInfo(final BatchUpdate b, final HRegionInfo i)
    throws IOException {
      
      i.offLine = !online;
      b.put(lockid, COL_REGIONINFO, Writables.getBytes(i));
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
      
      for (HashSet<HRegionInfo> s: servedRegions.values()) {
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
            e = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) e);
          }
          LOG.error("failed to delete region " + i.regionName, e);
        }
      }
      super.postProcessMeta(m, server);
    }

    @Override
    protected void updateRegionInfo(BatchUpdate b,
        @SuppressWarnings("unused") HRegionInfo i) {
      
      b.delete(lockid, COL_REGIONINFO);
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
        final HRegionInfo info) throws IOException {
      
      if (isEnabled(info)) {
        throw new TableNotDisabledException(tableName.toString());
      }
    }

    protected void updateRegionInfo(HRegionInterface server, Text regionName,
        HRegionInfo i) throws IOException {

      BatchUpdate b = new BatchUpdate();
      long lockid = b.startUpdate(i.regionName);
      b.put(lockid, COL_REGIONINFO, Writables.getBytes(i));
      
      for (int tries = 0; tries < numRetries; tries++) {
        try {
          server.batchUpdate(regionName, System.currentTimeMillis(), b);
        
          if (LOG.isDebugEnabled()) {
            LOG.debug("updated columns in row: " + i.regionName);
          }
          break;
        
        } catch (IOException e) {
          if (tries == numRetries - 1) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            LOG.error("column update failed in row: " + i.regionName, e);
            break;
          }
        }
        try {
          Thread.sleep(threadWakeFrequency);
          
        } catch (InterruptedException e) {
          // continue
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

      for (HRegionInfo i: unservedRegions) {
        i.tableDesc.families().remove(columnName);
        updateRegionInfo(server, m.regionName, i);

        // Delete the directories used by the column

        try {
          fs.delete(HStoreFile.getMapDir(dir, i.regionName, columnName));

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) e);
          }
          LOG.error("", e);
        }

        try {
          fs.delete(HStoreFile.getInfoDir(dir, i.regionName, columnName));

        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) e);
          }
          LOG.error("", e);
        }
      }
    }
  }

  /** Instantiated to add a column family to a table */
  private class AddColumn extends ColumnOperation {
    private HColumnDescriptor newColumn;

    AddColumn(Text tableName, HColumnDescriptor newColumn) throws IOException {
      super(tableName);
      this.newColumn = newColumn;
    }

    @Override
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
    throws IOException {

      for (HRegionInfo i: unservedRegions) {

        // All we need to do to add a column is add it to the table descriptor.
        // When the region is brought on-line, it will find the column missing
        // and create it.

        i.tableDesc.addFamily(newColumn);
        updateRegionInfo(server, m.regionName, i);
      }
    }
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

      HServerInfo info;
      synchronized (serversToServerInfo) {
        info = serversToServerInfo.remove(server);
        
        if (info != null) {
          String serverName = info.getServerAddress().toString();
          HServerLoad load = serversToLoad.remove(serverName);
          if (load != null) {
            Set<String> servers = loadToServers.get(load);
            if (servers != null) {
              servers.remove(serverName);
              loadToServers.put(load, servers);
            }
          }
        }
        serversToServerInfo.notifyAll();
      }

      // NOTE: If the server was serving the root region, we cannot reassign it
      // here because the new server will start serving the root region before
      // the PendingServerShutdown operation has a chance to split the log file.

      try {
        msgQueue.put(new PendingServerShutdown(info));
      } catch (InterruptedException e) {
        throw new RuntimeException("Putting into msgQueue was interrupted.", e);
      }
    }
  }

  /*
   * Main program
   */

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
          LOG.error( "Can not start master", t);
          System.exit(-1);
        }
        break;
      }

      if (cmd.equals("stop")) {
        try {
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
}
