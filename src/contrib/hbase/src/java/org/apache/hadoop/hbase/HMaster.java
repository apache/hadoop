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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
  abstract class BaseScanner extends Chore {
    protected boolean rootRegion;

    protected abstract boolean initialScan();
    protected abstract void maintenanceScan();

    BaseScanner(final boolean rootRegion, final int period,
        final AtomicBoolean stop) {
      super(period, stop);
      this.rootRegion = rootRegion;
    }
    
    @Override
    protected boolean initialChore() {
      return initialScan();
    }
    
    @Override
    protected void chore() {
      maintenanceScan();
    }

    /**
     * @param region Region to scan
     * @throws IOException
     */
    protected void scanRegion(final MetaRegion region) throws IOException {
      HRegionInterface regionServer = null;
      long scannerId = -1L;
      LOG.info(Thread.currentThread().getName() + " scanning meta region " +
        region.toString());

      // Array to hold list of split parents found.  Scan adds to list.  After
      // scan we go check if parents can be removed.
      Map<HRegionInfo, SortedMap<Text, byte[]>> splitParents =
        new HashMap<HRegionInfo, SortedMap<Text, byte[]>>();
      try {
        regionServer = connection.getHRegionConnection(region.getServer());
        scannerId =
          regionServer.openScanner(region.getRegionName(), COLUMN_FAMILY_ARRAY,
              EMPTY_START_ROW, System.currentTimeMillis(), null);

        int numberOfRegionsFound = 0;
        while (true) {
          HbaseMapWritable values = regionServer.next(scannerId);
          if (values == null || values.size() == 0) {
            break;
          }

          // TODO: Why does this have to be a sorted map?
          SortedMap<Text, byte[]> results = toRowMap(values).getMap();
          
          HRegionInfo info = getHRegionInfo(results);
          if (info == null) {
            continue;
          }

          String serverName = Writables.bytesToString(results.get(COL_SERVER));
          long startCode = Writables.bytesToLong(results.get(COL_STARTCODE));
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + " regioninfo: {" +
              info.toString() + "}, server: " + serverName + ", startCode: " +
              startCode);
          }

          // Note Region has been assigned.
          checkAssigned(info, serverName, startCode);
          if (isSplitParent(info)) {
            splitParents.put(info, results);
          }
          numberOfRegionsFound += 1;
        }
        if (this.rootRegion) {
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
          LOG.error("Closing scanner",
            RemoteExceptionHandler.checkIOException(e));
        }
      }

      // Scan is finished.  Take a look at split parents to see if any we can
      // clean up.
      if (splitParents.size() > 0) {
        for (Map.Entry<HRegionInfo, SortedMap<Text, byte[]>> e:
            splitParents.entrySet()) {
          HRegionInfo hri = e.getKey();
          cleanupSplits(region.getRegionName(), regionServer, hri, e.getValue());
        }
      }
      LOG.info(Thread.currentThread().getName() + " scan of meta region " +
        region.toString() + " complete");
    }

    /*
     * @param info Region to check.
     * @return True if this is a split parent.
     */
    private boolean isSplitParent(final HRegionInfo info) {
      if (!info.isSplit()) {
        return false;
      }
      if (!info.isOffline()) {
        LOG.warn("Region is split but not offline: " + info.getRegionName());
      }
      return true;
    }

    /*
     * If daughters no longer hold reference to the parents, delete the parent.
     * @param metaRegionName Meta region name.
     * @param server HRegionInterface of meta server to talk to 
     * @param parent HRegionInfo of split parent
     * @param rowContent Content of <code>parent</code> row in
     * <code>metaRegionName</code>
     * @return True if we removed <code>parent</code> from meta table and from
     * the filesystem.
     * @throws IOException
     */
    private boolean cleanupSplits(final Text metaRegionName, 
        final HRegionInterface srvr, final HRegionInfo parent,
        SortedMap<Text, byte[]> rowContent)
    throws IOException {
      boolean result = false;

      boolean hasReferencesA = hasReferences(metaRegionName, srvr,
          parent.getRegionName(), rowContent, COL_SPLITA);
      boolean hasReferencesB = hasReferences(metaRegionName, srvr,
          parent.getRegionName(), rowContent, COL_SPLITB);
      
      if (!hasReferencesA && !hasReferencesB) {
        LOG.info("Deleting region " + parent.getRegionName() +
          " because daughter splits no longer hold references");
        if (!HRegion.deleteRegion(fs, rootdir, parent)) {
          LOG.warn("Deletion of " + parent.getRegionName() + " failed");
        }
        
        HRegion.removeRegionFromMETA(srvr, metaRegionName,
          parent.getRegionName());
        result = true;
      } else if (LOG.isDebugEnabled()) {
        // If debug, note we checked and current state of daughters.
        LOG.debug("Checked " + parent.getRegionName() +
          " for references: splitA: " + hasReferencesA + ", splitB: "+
          hasReferencesB);
      }
      return result;
    }
    
    /* 
     * Checks if a daughter region -- either splitA or splitB -- still holds
     * references to parent.  If not, removes reference to the split from
     * the parent meta region row.
     * @param metaRegionName Name of meta region to look in.
     * @param srvr Where region resides.
     * @param parent Parent region name. 
     * @param rowContent Keyed content of the parent row in meta region.
     * @param splitColumn Column name of daughter split to examine
     * @return True if still has references to parent.
     * @throws IOException
     */
    protected boolean hasReferences(final Text metaRegionName, 
      final HRegionInterface srvr, final Text parent,
      SortedMap<Text, byte[]> rowContent, final Text splitColumn)
    throws IOException {
      boolean result = false;
      HRegionInfo split =
        Writables.getHRegionInfoOrNull(rowContent.get(splitColumn));
      if (split == null) {
        return result;
      }
      Path tabledir =
        HTableDescriptor.getTableDir(rootdir, split.getTableDesc().getName());
      for (HColumnDescriptor family: split.getTableDesc().families().values()) {
        Path p = HStoreFile.getMapDir(tabledir, split.getEncodedName(),
            family.getFamilyName());

        // Look for reference files.  Call listPaths with an anonymous
        // instance of PathFilter.

        Path [] ps = fs.listPaths(p,
            new PathFilter () {
              public boolean accept(Path path) {
                return HStore.isReference(path);
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
            +" no longer has references to " + parent.toString());
      }
      
      BatchUpdate b = new BatchUpdate(rand.nextLong());
      long lockid = b.startUpdate(parent);
      b.delete(lockid, splitColumn);
      srvr.batchUpdate(metaRegionName, System.currentTimeMillis(), b);
        
      return result;
    }

    protected void checkAssigned(final HRegionInfo info,
      final String serverName, final long startCode) throws IOException {
      
      // Skip region - if ...
      if(info.isOffline()                                 // offline
          || killedRegions.contains(info.getRegionName()) // queued for offline
          || regionsToDelete.contains(info.getRegionName())) { // queued for delete

        unassignedRegions.remove(info);
        return;
      }
      HServerInfo storedInfo = null;
      boolean deadServer = false;
      if (serverName.length() != 0) {
        synchronized (killList) {
          Map<Text, HRegionInfo> regionsToKill = killList.get(serverName);
          if (regionsToKill != null &&
              regionsToKill.containsKey(info.getRegionName())) {

            // Skip if region is on kill list
            if(LOG.isDebugEnabled()) {
              LOG.debug("not assigning region (on kill list): " +
                  info.getRegionName());
            }
            return;
          }
        }
        storedInfo = serversToServerInfo.get(serverName);
        deadServer = deadServers.contains(serverName);
      }

      /*
       * If the server is not dead and either:
       *   the stored info is not null and the start code does not match
       * or:
       *   the stored info is null and the region is neither unassigned nor pending
       * then:
       */ 
      if (!deadServer &&
          ((storedInfo != null && storedInfo.getStartCode() != startCode) ||
              (storedInfo == null &&
                  !unassignedRegions.containsKey(info) &&
                  !pendingRegions.contains(info.getRegionName())
              )
          )
        ) {

        // The current assignment is invalid
        if (LOG.isDebugEnabled()) {
          LOG.debug("Current assignment of " + info.getRegionName() +
            " is not valid: storedInfo: " + storedInfo + ", startCode: " +
            startCode + ", storedInfo.startCode: " +
            ((storedInfo != null)? storedInfo.getStartCode(): -1) +
            ", unassignedRegions: " + unassignedRegions.containsKey(info) +
            ", pendingRegions: " +
            pendingRegions.contains(info.getRegionName()));
        }
        // Recover the region server's log if there is one.
        // This is only done from here if we are restarting and there is stale
        // data in the meta region. Once we are on-line, dead server log
        // recovery is handled by lease expiration and ProcessServerShutdown
        if (!initialMetaScanComplete && serverName.length() != 0) {
          StringBuilder dirName = new StringBuilder("log_");
          dirName.append(serverName.replace(":", "_"));
          Path logDir = new Path(rootdir, dirName.toString());
          try {
            if (fs.exists(logDir)) {
              splitLogLock.lock();
              try {
                HLog.splitLog(rootdir, logDir, fs, conf);
              } finally {
                splitLogLock.unlock();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Split " + logDir.toString());
            }
          } catch (IOException e) {
            LOG.warn("unable to split region server log because: ", e);
            throw e;
          }
        }
        // Now get the region assigned
        unassignedRegions.put(info, ZERO_L);
      }
    }
  }

  volatile boolean rootScanned = false;

  /** Scanner for the <code>ROOT</code> HRegion. */
  class RootScanner extends BaseScanner {
    /** Constructor */
    public RootScanner() {
      super(true, metaRescanInterval, closed);
    }

    private boolean scanRoot() {
      // Don't retry if we get an error while scanning. Errors are most often
      // caused by the server going away. Wait until next rescan interval when
      // things should be back to normal
      boolean scanSuccessful = false;
      synchronized (rootRegionLocation) {
        while(!closed.get() && rootRegionLocation.get() == null) {
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
      if (closed.get()) {
        return scanSuccessful;
      }

      try {
        // Don't interrupt us while we're working
        synchronized(rootScannerLock) {
          scanRegion(new MetaRegion(rootRegionLocation.get(),
              HRegionInfo.rootRegionInfo.getRegionName(), null));
        }
        scanSuccessful = true;
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.warn("Scan ROOT region", e);
        // Make sure the file system is still available
        checkFileSystem();
      } catch (Exception e) {
        // If for some reason we get some other kind of exception, 
        // at least log it rather than go out silently.
        LOG.error("Unexpected exception", e);
      }
      return scanSuccessful;
    }

    @Override
    protected boolean initialScan() {
      rootScanned = scanRoot();
      return rootScanned;
    }

    @Override
    protected void maintenanceScan() {
      scanRoot();
    }
  }

  private final RootScanner rootScannerThread;
  final Integer rootScannerLock = new Integer(0);

  /** Describes a meta region and its server */
  @SuppressWarnings("unchecked")
  public static class MetaRegion implements Comparable {
    private HServerAddress server;
    private Text regionName;
    private Text startKey;

    MetaRegion(HServerAddress server, Text regionName, Text startKey) {
      if (server == null) {
        throw new IllegalArgumentException("server cannot be null");
      }
      this.server = server;
      
      if (regionName == null) {
        throw new IllegalArgumentException("regionName cannot be null");
      }
      this.regionName = new Text(regionName);
      
      this.startKey = new Text();
      if (startKey != null) {
        this.startKey.set(startKey);
      }
    }
    
    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "{regionname: " + this.regionName.toString() + ", startKey: <" +
        this.startKey.toString() + ">, server: " + this.server.toString() + "}";
    }

    /** @return the regionName */
    public Text getRegionName() {
      return regionName;
    }

    /** @return the server */
    public HServerAddress getServer() {
      return server;
    }

    /** @return the startKey */
    public Text getStartKey() {
      return startKey;
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
      int result = this.regionName.compareTo(other.getRegionName());
      if(result == 0) {
        result = this.startKey.compareTo(other.getStartKey());
        if (result == 0) {
          // Might be on different host?
          result = this.server.compareTo(other.server);
        }
      }
      return result;
    }
  }

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
    private final List<MetaRegion> metaRegionsToRescan =
      new ArrayList<MetaRegion>();
    
    /** Constructor */
    public MetaScanner() {
      super(false, metaRescanInterval, closed);
    }

    private boolean scanOneMetaRegion(MetaRegion region) {
      // Don't retry if we get an error while scanning. Errors are most often
      // caused by the server going away. Wait until next rescan interval when
      // things should be back to normal
      boolean scanSuccessful = false;
      while (!closed.get() && !rootScanned &&
          rootRegionLocation.get() == null) {
        sleeper.sleep();
      }
      if (closed.get()) {
        return scanSuccessful;
      }

      try {
        // Don't interrupt us while we're working
        synchronized (metaScannerLock) {
          scanRegion(region);
          onlineMetaRegions.put(region.getStartKey(), region);
        }
        scanSuccessful = true;
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.warn("Scan one META region: " + region.toString(), e);
        // The region may have moved (TestRegionServerAbort, etc.).  If
        // so, either it won't be in the onlineMetaRegions list or its host
        // address has changed and the containsValue will fail. If not
        // found, best thing to do here is probably return.
        if (!onlineMetaRegions.containsValue(region.getStartKey())) {
          LOG.debug("Scanned region is no longer in map of online " +
          "regions or its value has changed");
          return scanSuccessful;
        }
        // Make sure the file system is still available
        checkFileSystem();
      } catch (Exception e) {
        // If for some reason we get some other kind of exception, 
        // at least log it rather than go out silently.
        LOG.error("Unexpected exception", e);
      }
      return scanSuccessful;
    }

    @Override
    protected boolean initialScan() {
      MetaRegion region = null;
      while (!closed.get() && region == null && !metaRegionsScanned()) {
        try {
          region =
            metaRegionsToScan.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // continue
        }
        if (region == null && metaRegionsToRescan.size() != 0) {
          region = metaRegionsToRescan.remove(0);
        }
        if (region != null) {
          if (!scanOneMetaRegion(region)) {
            metaRegionsToRescan.add(region);
          }
        }
      }
      initialMetaScanComplete = true;
      return true;
    }

    @Override
    protected void maintenanceScan() {
      ArrayList<MetaRegion> regions = new ArrayList<MetaRegion>();
      synchronized (onlineMetaRegions) {
        regions.addAll(onlineMetaRegions.values());
      }
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
      while (!closed.get()) {
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
      return closed.get();
    }
  }

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
    this(new Path(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR)),
        new HServerAddress(conf.get(MASTER_ADDRESS, DEFAULT_MASTER_ADDRESS)),
        conf);
  }

  /** 
   * Build the HMaster
   * @param rootdir base directory of this HBase instance
   * @param address server address and port number
   * @param conf configuration
   * 
   * @throws IOException
   */
  public HMaster(Path rootdir, HServerAddress address, HBaseConfiguration conf)
    throws IOException {
    
    this.conf = conf;
    this.fs = FileSystem.get(conf);
    this.rootdir = fs.makeQualified(rootdir);
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
    this.rootScannerThread = new RootScanner();

    // Scans the meta table
    this.metaScannerThread = new MetaScanner();
    
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
      delayedToDoQueue.put(new ProcessServerShutdown(storedInfo));
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
      long serverLabel = getServerLabel(s);
      serverLeases.createLease(serverLabel, serverLabel, new ServerExpirer(s));
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

  private long getServerLabel(final String s) {
    return s.hashCode();
  }

  /** {@inheritDoc} */
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[])
  throws IOException {
    String serverName = serverInfo.getServerAddress().toString().trim();
    long serverLabel = getServerLabel(serverName);
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

            if (cancelLease(serverName, serverLabel)) {
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
        cancelLease(serverName, serverLabel);
        serversToServerInfo.notifyAll();
      }
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_STOP)};

    } else {

      // All's well.  Renew the server's lease.
      // This will always succeed; otherwise, the fetch of serversToServerInfo
      // would have failed above.

      serverLeases.renewLease(serverLabel, serverLabel);

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
  private boolean cancelLease(final String serverName, final long serverLabel) {
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
                toDoQueue.put(new ProcessRegionOpen(info, region));
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
            toDoQueue.put(new ProcessRegionClose(region, reassignRegion,
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
   * Some internal classes to manage msg-passing and region server operations
   */

  private abstract class RegionServerOperation implements Delayed {
    private long expire;

    protected RegionServerOperation() {
      // Set the future time at which we expect to be released from the
      // DelayQueue we're inserted in on lease expiration.
      this.expire = System.currentTimeMillis() + leaseTimeout / 2;
    }
    
    /** {@inheritDoc} */
    public long getDelay(TimeUnit unit) {
      return unit.convert(this.expire - System.currentTimeMillis(),
        TimeUnit.MILLISECONDS);
    }
    
    /** {@inheritDoc} */
    public int compareTo(Delayed o) {
      return Long.valueOf(getDelay(TimeUnit.MILLISECONDS)
          - o.getDelay(TimeUnit.MILLISECONDS)).intValue();
    }
    
    protected void requeue() {
      this.expire = System.currentTimeMillis() + leaseTimeout / 2;
      delayedToDoQueue.put(this);
    }
    
    protected boolean rootAvailable() {
      boolean available = true;
      if (rootRegionLocation.get() == null) {
        available = false;
        requeue();
      }
      return available;
    }

    protected boolean metaTableAvailable() {
      boolean available = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("numberOfMetaRegions: " + numberOfMetaRegions.get() +
            ", onlineMetaRegions.size(): " + onlineMetaRegions.size());
      }
      if (numberOfMetaRegions.get() != onlineMetaRegions.size()) {
        // We can't proceed because not all of the meta regions are online.
        // We can't block either because that would prevent the meta region
        // online message from being processed. In order to prevent spinning
        // in the run queue, put this request on the delay queue to give
        // other threads the opportunity to get the meta regions on-line.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Requeuing because not all meta regions are online");
        }
        available = false;
        requeue();
      }
      return available;
    }
    
    protected abstract boolean process() throws IOException;
  }

  /** 
   * Instantiated when a server's lease has expired, meaning it has crashed.
   * The region server's log file needs to be split up for each region it was
   * serving, and the regions need to get reassigned.
   */
  private class ProcessServerShutdown extends RegionServerOperation {
    private HServerAddress deadServer;
    private String deadServerName;
    private Path oldLogDir;
    private boolean logSplit;
    private boolean rootRescanned;

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

    /**
     * @param serverInfo
     */
    public ProcessServerShutdown(HServerInfo serverInfo) {
      super();
      this.deadServer = serverInfo.getServerAddress();
      this.deadServerName = this.deadServer.toString();
      this.logSplit = false;
      this.rootRescanned = false;
      StringBuilder dirName = new StringBuilder("log_");
      dirName.append(deadServer.getBindAddress());
      dirName.append("_");
      dirName.append(serverInfo.getStartCode());
      dirName.append("_");
      dirName.append(deadServer.getPort());
      this.oldLogDir = new Path(rootdir, dirName.toString());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "ProcessServerShutdown of " + this.deadServer.toString();
    }

    /** Finds regions that the dead region server was serving */
    private void scanMetaRegion(HRegionInterface server, long scannerId,
        Text regionName) throws IOException {

      ArrayList<ToDoEntry> toDoList = new ArrayList<ToDoEntry>();
      HashSet<HRegionInfo> regions = new HashSet<HRegionInfo>();

      try {
        while (true) {
          HbaseMapWritable values = null;
          try {
            values = server.next(scannerId);
          } catch (IOException e) {
            LOG.error("Shutdown scanning of meta region",
              RemoteExceptionHandler.checkIOException(e));
            break;
          }
          if (values == null || values.size() == 0) {
            break;
          }
          // TODO: Why does this have to be a sorted map?
          RowMap rm = toRowMap(values);
          Text row = rm.getRow();
          SortedMap<Text, byte[]> map = rm.getMap();
          if (LOG.isDebugEnabled() && row != null) {
            LOG.debug("shutdown scanner looking at " + row.toString());
          }

          // Check server name.  If null, be conservative and treat as though
          // region had been on shutdown server (could be null because we
          // missed edits in hlog because hdfs does not do write-append).
          String serverName;
          try {
            serverName = Writables.bytesToString(map.get(COL_SERVER));
          } catch (UnsupportedEncodingException e) {
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
          HRegionInfo info = getHRegionInfo(map);
          if (info == null) {
            continue;
          }
          LOG.info(info.getRegionName() + " was on shutdown server <" +
              serverName + "> (or server is null). Marking unassigned in " +
          "meta and clearing pendingRegions");

          if (info.isMetaTable()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("removing meta region " + info.getRegionName() +
                  " from online meta regions");
            }
            onlineMetaRegions.remove(info.getStartKey());
          }

          ToDoEntry todo = new ToDoEntry(row, info);
          toDoList.add(todo);

          if (killList.containsKey(deadServerName)) {
            HashMap<Text, HRegionInfo> regionsToKill =
              new HashMap<Text, HRegionInfo>();
            synchronized (killList) {
              regionsToKill.putAll(killList.get(deadServerName));
            }

            if (regionsToKill.containsKey(info.getRegionName())) {
              regionsToKill.remove(info.getRegionName());
              killList.put(deadServerName, regionsToKill);
              unassignedRegions.remove(info);
              synchronized (regionsToDelete) {
                if (regionsToDelete.contains(info.getRegionName())) {
                  // Delete this region
                  regionsToDelete.remove(info.getRegionName());
                  todo.deleteRegion = true;
                } else {
                  // Mark region offline
                  todo.regionOffline = true;
                }
              }
            }

          } else {
            // Get region reassigned
            regions.add(info);

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
            LOG.error("Closing scanner",
              RemoteExceptionHandler.checkIOException(e));
          }
        }
      }

      // Update server in root/meta entries
      for (ToDoEntry e: toDoList) {
        if (e.deleteRegion) {
          HRegion.removeRegionFromMETA(server, regionName, e.row);
        } else if (e.regionOffline) {
          HRegion.offlineRegionInMETA(server, regionName, e.info);
        }
      }

      // Get regions reassigned
      for (HRegionInfo info: regions) {
        unassignedRegions.put(info, ZERO_L);
      }
    }

    @Override
    protected boolean process() throws IOException {
      LOG.info("process shutdown of server " + deadServer + ": logSplit: " +
          this.logSplit + ", rootRescanned: " + this.rootRescanned +
          ", numberOfMetaRegions: " + numberOfMetaRegions.get() +
          ", onlineMetaRegions.size(): " + onlineMetaRegions.size());

      if (!logSplit) {
        // Process the old log file
        if (fs.exists(oldLogDir)) {
          if (!splitLogLock.tryLock()) {
            return false;
          }
          try {
            HLog.splitLog(rootdir, oldLogDir, fs, conf);
          } finally {
            splitLogLock.unlock();
          }
        }
        logSplit = true;
      }

      if (!rootAvailable()) {
        // Return true so that worker does not put this request back on the
        // toDoQueue.
        // rootAvailable() has already put it on the delayedToDoQueue
        return true;
      }

      if (!rootRescanned) {
        // Scan the ROOT region

        HRegionInterface server = null;
        long scannerId = -1L;
        for (int tries = 0; tries < numRetries; tries ++) {
          if (closed.get()) {
            return true;
          }
          server = connection.getHRegionConnection(rootRegionLocation.get());
          scannerId = -1L;

          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("process server shutdown scanning root region on " +
                  rootRegionLocation.get().getBindAddress());
            }
            scannerId =
              server.openScanner(HRegionInfo.rootRegionInfo.getRegionName(),
                  COLUMN_FAMILY_ARRAY, EMPTY_START_ROW,
                  System.currentTimeMillis(), null);
            
            scanMetaRegion(server, scannerId,
                HRegionInfo.rootRegionInfo.getRegionName());
            break;

          } catch (IOException e) {
            if (tries == numRetries - 1) {
              throw RemoteExceptionHandler.checkIOException(e);
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
      
      if (!metaTableAvailable()) {
        // We can't proceed because not all meta regions are online.
        // metaAvailable() has put this request on the delayedToDoQueue
        // Return true so that worker does not put this on the toDoQueue
        return true;
      }

      for (int tries = 0; tries < numRetries; tries++) {
        try {
          if (closed.get()) {
            return true;
          }
          List<MetaRegion> regions = new ArrayList<MetaRegion>();
          synchronized (onlineMetaRegions) {
            regions.addAll(onlineMetaRegions.values());
          }
          for (MetaRegion r: regions) {
            HRegionInterface server = null;
            long scannerId = -1L;

            if (LOG.isDebugEnabled()) {
              LOG.debug("process server shutdown scanning " +
                  r.getRegionName() + " on " + r.getServer() + " " +
                  Thread.currentThread().getName());
            }
            server = connection.getHRegionConnection(r.getServer());

            scannerId =
              server.openScanner(r.getRegionName(), COLUMN_FAMILY_ARRAY,
                  EMPTY_START_ROW, System.currentTimeMillis(), null);

            scanMetaRegion(server, scannerId, r.getRegionName());

            if (LOG.isDebugEnabled()) {
              LOG.debug("process server shutdown finished scanning " +
                  r.getRegionName() + " on " + r.getServer() + " " +
                  Thread.currentThread().getName());
            }
          }
          deadServers.remove(deadServerName);
          break;

        } catch (IOException e) {
          if (tries == numRetries - 1) {
            throw RemoteExceptionHandler.checkIOException(e);
          }
        }
      }
      return true;
    }
  }

  /**
   * Abstract class that performs common operations for 
   * @see #ProcessRegionClose and @see #ProcessRegionOpen
   */
  private abstract class ProcessRegionStatusChange
    extends RegionServerOperation {

    protected final boolean isMetaTable;
    protected final HRegionInfo regionInfo;
    private MetaRegion metaRegion;
    protected Text metaRegionName;
    
    /**
     * @param regionInfo
     */
    public ProcessRegionStatusChange(HRegionInfo regionInfo) {
      super();
      this.regionInfo = regionInfo;
      this.isMetaTable = regionInfo.isMetaTable();
      this.metaRegion = null;
      this.metaRegionName = null;
    }
    
    protected boolean metaRegionAvailable() {
      boolean available = true;
      if (isMetaTable) {
        // This operation is for the meta table
        if (!rootAvailable()) {
          // But we can't proceed unless the root region is available
          available = false;
        }
      } else {
        if (!rootScanned || !metaTableAvailable()) {
          // The root region has not been scanned or the meta table is not
          // available so we can't proceed.
          // Put the operation on the delayedToDoQueue
          requeue();
          available = false;
        }
      }
      return available;
    }
    
    protected HRegionInterface getMetaServer() throws IOException {
      if (this.isMetaTable) {
        this.metaRegionName = HRegionInfo.rootRegionInfo.getRegionName();
      } else {
        if (this.metaRegion == null) {
          synchronized (onlineMetaRegions) {
            metaRegion = onlineMetaRegions.size() == 1 ? 
                onlineMetaRegions.get(onlineMetaRegions.firstKey()) :
                  onlineMetaRegions.containsKey(regionInfo.getRegionName()) ?
                      onlineMetaRegions.get(regionInfo.getRegionName()) :
                        onlineMetaRegions.get(onlineMetaRegions.headMap(
                            regionInfo.getRegionName()).lastKey());
          }
          this.metaRegionName = metaRegion.getRegionName();
        }
      }

      HServerAddress server = null;
      if (isMetaTable) {
        server = rootRegionLocation.get();
        
      } else {
        server = metaRegion.getServer();
      }
      return connection.getHRegionConnection(server);
    }
    
  }
  /**
   * ProcessRegionClose is instantiated when a region server reports that it
   * has closed a region.
   */
  private class ProcessRegionClose extends ProcessRegionStatusChange {
    private boolean reassignRegion;
    private boolean deleteRegion;

    /**
     * @param regionInfo
     * @param reassignRegion
     * @param deleteRegion
     */
    public ProcessRegionClose(HRegionInfo regionInfo, boolean reassignRegion,
        boolean deleteRegion) {

      super(regionInfo);
      this.reassignRegion = reassignRegion;
      this.deleteRegion = deleteRegion;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "ProcessRegionClose of " + this.regionInfo.getRegionName();
    }

    @Override
    protected boolean process() throws IOException {
      for (int tries = 0; tries < numRetries; tries++) {
        if (closed.get()) {
          return true;
        }
        LOG.info("region closed: " + regionInfo.getRegionName());

        // Mark the Region as unavailable in the appropriate meta table

        if (!metaRegionAvailable()) {
          // We can't proceed unless the meta region we are going to update
          // is online. metaRegionAvailable() has put this operation on the
          // delayedToDoQueue, so return true so the operation is not put 
          // back on the toDoQueue
          return true;
        }

        try {
          if (deleteRegion) {
            HRegion.removeRegionFromMETA(getMetaServer(), metaRegionName,
              regionInfo.getRegionName());
          } else {
            HRegion.offlineRegionInMETA(getMetaServer(), metaRegionName,
              regionInfo);
          }
          break;

        } catch (IOException e) {
          if (tries == numRetries - 1) {
            throw RemoteExceptionHandler.checkIOException(e);
          }
          continue;
        }
      }

      if (reassignRegion) {
        LOG.info("reassign region: " + regionInfo.getRegionName());

        unassignedRegions.put(regionInfo, ZERO_L);

      } else if (deleteRegion) {
        try {
          HRegion.deleteRegion(fs, rootdir, regionInfo);
        } catch (IOException e) {
          e = RemoteExceptionHandler.checkIOException(e);
          LOG.error("failed delete region " + regionInfo.getRegionName(), e);
          throw e;
        }
      }
      return true;
    }
  }

  /** 
   * ProcessRegionOpen is instantiated when a region server reports that it is
   * serving a region. This applies to all meta and user regions except the 
   * root region which is handled specially.
   */
  private class ProcessRegionOpen extends ProcessRegionStatusChange {
    private final HServerAddress serverAddress;
    private final byte [] startCode;

    /**
     * @param info
     * @param regionInfo
     * @throws IOException
     */
    public ProcessRegionOpen(HServerInfo info, HRegionInfo regionInfo)
    throws IOException {
      super(regionInfo);
      this.serverAddress = info.getServerAddress();
      this.startCode = Writables.longToBytes(info.getStartCode());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "PendingOpenOperation from " + serverAddress.toString();
    }

    @Override
    protected boolean process() throws IOException {
      for (int tries = 0; tries < numRetries; tries++) {
        if (closed.get()) {
          return true;
        }
        LOG.info(regionInfo.toString() + " open on " + 
            this.serverAddress.toString());

        if (!metaRegionAvailable()) {
          // We can't proceed unless the meta region we are going to update
          // is online. metaRegionAvailable() has put this operation on the
          // delayedToDoQueue, so return true so the operation is not put 
          // back on the toDoQueue
          return true;
        }

        // Register the newly-available Region's location.
        
        HRegionInterface server = getMetaServer();
        LOG.info("updating row " + regionInfo.getRegionName() + " in table " +
          metaRegionName + " with startcode " +
          Writables.bytesToLong(this.startCode) + " and server "+
          serverAddress.toString());
        try {
          BatchUpdate b = new BatchUpdate(rand.nextLong());
          long lockid = b.startUpdate(regionInfo.getRegionName());
          b.put(lockid, COL_SERVER,
            Writables.stringToBytes(serverAddress.toString()));
          b.put(lockid, COL_STARTCODE, startCode);
          server.batchUpdate(metaRegionName, System.currentTimeMillis(), b);
          if (isMetaTable) {
            // It's a meta region.
            MetaRegion m = new MetaRegion(this.serverAddress,
              this.regionInfo.getRegionName(), this.regionInfo.getStartKey());
            if (!initialMetaScanComplete) {
              // Put it on the queue to be scanned for the first time.
              try {
                LOG.debug("Adding " + m.toString() + " to regions to scan");
                metaRegionsToScan.put(m);
              } catch (InterruptedException e) {
                throw new RuntimeException(
                    "Putting into metaRegionsToScan was interrupted.", e);
              }
            } else {
              // Add it to the online meta regions
              LOG.debug("Adding to onlineMetaRegions: " + m.toString());
              onlineMetaRegions.put(this.regionInfo.getStartKey(), m);
            }
          }
          // If updated successfully, remove from pending list.
          pendingRegions.remove(regionInfo.getRegionName());
          break;
        } catch (IOException e) {
          if (tries == numRetries - 1) {
            throw RemoteExceptionHandler.checkIOException(e);
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
      HRegionInterface server = connection.getHRegionConnection(m.getServer());
      long scannerid = server.openScanner(metaRegionName, COL_REGIONINFO_ARRAY,
          tableName, System.currentTimeMillis(), null);
      try {
        HbaseMapWritable data = server.next(scannerid);
            
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
        server.close(scannerid);
      }

      // 2. Create the HRegion
          
      HRegion region =
        HRegion.createHRegion(newRegion, this.rootdir, this.conf);

      // 3. Insert into meta
          
      HRegionInfo info = region.getRegionInfo();
      Text regionName = region.getRegionName();
      BatchUpdate b = new BatchUpdate(rand.nextLong());
      long lockid = b.startUpdate(regionName);
      b.put(lockid, COL_REGIONINFO, Writables.getBytes(info));
      server.batchUpdate(metaRegionName, System.currentTimeMillis(), b);

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
    new TableDelete(tableName).process();
    LOG.info("deleted table: " + tableName);
  }

  /** {@inheritDoc} */
  public void addColumn(Text tableName, HColumnDescriptor column)
  throws IOException {
    
    new AddColumn(tableName, column).process();
  }

  /** {@inheritDoc} */
  public void modifyColumn(Text tableName, Text columnName, 
    HColumnDescriptor descriptor)
  throws IOException {
    new ModifyColumn(tableName, columnName, descriptor).process();
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

      if (metaScannerThread.waitForMetaRegionsOrClose()) {
        throw new MasterNotRunningException(); // We're shutting down. Forget it.
      }

      Text firstMetaRegion = null;
      synchronized (onlineMetaRegions) {
        if (onlineMetaRegions.size() == 1) {
          firstMetaRegion = onlineMetaRegions.firstKey();

        } else if (onlineMetaRegions.containsKey(tableName)) {
          firstMetaRegion = tableName;

        } else {
          firstMetaRegion = onlineMetaRegions.headMap(tableName).lastKey();
        }
        this.metaRegions.addAll(onlineMetaRegions.tailMap(
            firstMetaRegion).values());
      }
    }

    void process() throws IOException {
      for (int tries = 0; tries < numRetries; tries++) {
        boolean tableExists = false;
        try {
          synchronized(metaScannerLock) {     // Prevent meta scanner from running
            for (MetaRegion m: metaRegions) {

              // Get a connection to a meta server

              HRegionInterface server =
                connection.getHRegionConnection(m.getServer());

              // Open a scanner on the meta region

              long scannerId =
                server.openScanner(m.getRegionName(), COLUMN_FAMILY_ARRAY,
                    tableName, System.currentTimeMillis(), null);

              try {
                while (true) {
                  HbaseMapWritable values = server.next(scannerId);
                  if(values == null || values.size() == 0) {
                    break;
                  }
                  RowMap rm = toRowMap(values);
                  SortedMap<Text, byte[]> map = rm.getMap();
                  HRegionInfo info = getHRegionInfo(map);
                  if (info == null) {
                    throw new IOException(COL_REGIONINFO + " not found on " +
                      rm.getRow());
                  }
                  String serverName = Writables.bytesToString(map.get(COL_SERVER));
                  long startCode = Writables.bytesToLong(map.get(COL_STARTCODE));
                  if (info.getTableDesc().getName().compareTo(tableName) > 0) {
                    break; // Beyond any more entries for this table
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
                    e = RemoteExceptionHandler.checkIOException(e);
                    LOG.error("closing scanner", e);
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
            // No retries left
            checkFileSystem();
            throw RemoteExceptionHandler.checkIOException(e);
          }
          continue;
        }
        break;
      } // for(tries...)
    }

    protected boolean isBeingServed(String serverName, long startCode) {
      boolean result = false;
      if (serverName != null && serverName.length() > 0 && startCode != -1L) {
        HServerInfo s = serversToServerInfo.get(serverName);
        result = s != null && s.getStartCode() == startCode;
      }
      return result;
    }

    protected boolean isEnabled(HRegionInfo info) {
      return !info.isOffline();
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
        if (i.isOffline() && i.isSplit()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping region " + i.toString() + " because it is " +
                "offline because it has been split");
          }
          continue;
        }
        
        // Update meta table
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("updating columns in row: " + i.getRegionName());
        }

        BatchUpdate b = new BatchUpdate(rand.nextLong());
        lockid = b.startUpdate(i.getRegionName());
        updateRegionInfo(b, i);
        b.delete(lockid, COL_SERVER);
        b.delete(lockid, COL_STARTCODE);
        server.batchUpdate(m.getRegionName(), System.currentTimeMillis(), b);
        if (LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: " + i.getRegionName());
        }

        if (online) {                         // Bring offline regions on-line
          if (!unassignedRegions.containsKey(i)) {
            unassignedRegions.put(i, ZERO_L);
          }

        } else {                              // Prevent region from getting assigned.
          unassignedRegions.remove(i);
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

        HashMap<Text, HRegionInfo> localKillList =
          new HashMap<Text, HRegionInfo>();
        
        synchronized (killList) {
          HashMap<Text, HRegionInfo> killedRegions = killList.get(serverName);
          if (killedRegions != null) {
            localKillList.putAll(killedRegions);
          }
        }
        for (HRegionInfo i: e.getValue()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("adding region " + i.getRegionName() +
                " to local kill list");
          }
          localKillList.put(i.getRegionName(), i);
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
      
      i.setOffline(!online);
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
          regionsToDelete.add(i.getRegionName());
        }
      }

      // Unserved regions we can delete now
      
      for (HRegionInfo i: unservedRegions) {
        // Delete the region
      
        try {
          HRegion.deleteRegion(fs, rootdir, i);
        
        } catch (IOException e) {
          LOG.error("failed to delete region " + i.getRegionName(),
            RemoteExceptionHandler.checkIOException(e));
        }
      }
      super.postProcessMeta(m, server);
    }

    @Override
    protected void updateRegionInfo(BatchUpdate b,
        @SuppressWarnings("unused") HRegionInfo info) {
      for (int i = 0; i < ALL_META_COLUMNS.length; i++) {
        // Be sure to clean all cells
        b.delete(lockid, ALL_META_COLUMNS[i]);
      }
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

      BatchUpdate b = new BatchUpdate(rand.nextLong());
      long lockid = b.startUpdate(i.getRegionName());
      b.put(lockid, COL_REGIONINFO, Writables.getBytes(i));
      server.batchUpdate(regionName, System.currentTimeMillis(), b);
      if (LOG.isDebugEnabled()) {
        LOG.debug("updated columns in row: " + i.getRegionName());
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

      Path tabledir = new Path(rootdir, tableName.toString());
      for (HRegionInfo i: unservedRegions) {
        i.getTableDesc().families().remove(columnName);
        updateRegionInfo(server, m.getRegionName(), i);

        // Delete the directories used by the column

        String encodedName = i.getEncodedName();
        fs.delete(HStoreFile.getMapDir(tabledir, encodedName, columnName));
        fs.delete(HStoreFile.getInfoDir(tabledir, encodedName, columnName));
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

        i.getTableDesc().addFamily(newColumn);
        updateRegionInfo(server, m.getRegionName(), i);
      }
    }
  }

  /** Instantiated to modify an existing column family on a table */
  private class ModifyColumn extends ColumnOperation {
    private HColumnDescriptor descriptor;
    private Text columnName;
    
    ModifyColumn(Text tableName, Text columnName, HColumnDescriptor _descriptor) 
      throws IOException {
      super(tableName);
      this.descriptor = _descriptor;
      this.columnName = columnName;
    }

    @Override
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
      throws IOException {

      for (HRegionInfo i: unservedRegions) {
        // get the column families map from the table descriptor
        Map<Text, HColumnDescriptor> families = i.getTableDesc().families();
        
        // if the table already has this column, then put the new descriptor 
        // version.
        if (families.get(columnName) != null){
          families.put(columnName, descriptor);
          updateRegionInfo(server, m.getRegionName(), i);          
        }
        else{ // otherwise, we have an error.
          throw new IOException("Column family '" + columnName + 
            "' doesn't exist, so cannot be modified.");
        }
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
        delayedToDoQueue.put(new ProcessServerShutdown(info));
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
   * Data structure used to return results out of the toRowMap method.
   */
  private class RowMap {
    final Text row;
    final SortedMap<Text, byte[]> map;
    
    private RowMap(final Text r, final SortedMap<Text, byte[]> m) {
      this.row = r;
      this.map = m;
    }

    private Text getRow() {
      return this.row;
    }

    private SortedMap<Text, byte[]> getMap() {
      return this.map;
    }
  }
  
  /*
   * Convert an HbaseMapWritable to a Map keyed by column.
   * Utility method used scanning meta regions
   * @param mw The MapWritable to convert.  Cannot be null.
   * @return Returns a SortedMap currently.  TODO: This looks like it could
   * be a plain Map.
   */
  protected RowMap toRowMap(final HbaseMapWritable mw) {
    if (mw == null) {
      throw new IllegalArgumentException("Passed MapWritable cannot be null");
    }
    SortedMap<Text, byte[]> m = new TreeMap<Text, byte[]>();
    Text row = null;
    for (Map.Entry<Writable, Writable> e: mw.entrySet()) {
      HStoreKey key = (HStoreKey) e.getKey();
      Text thisRow = key.getRow();
      if (row == null) {
        row = thisRow;
      } else {
        if (!row.equals(thisRow)) {
          LOG.error("Multiple rows in same scanner result set. firstRow=" +
            row + ", currentRow=" + thisRow);
        }
      }
      m.put(key.getColumn(), ((ImmutableBytesWritable) e.getValue()).get());
    }
    return new RowMap(row, m);
  }
  
  /*
   * Get HRegionInfo from passed META map of row values.
   * Returns null if none found (and logs fact that expected COL_REGIONINFO
   * was missing).  Utility method used by scanners of META tables.
   * @param map Map to do lookup in.
   * @return Null or found HRegionInfo.
   * @throws IOException
   */
  protected HRegionInfo getHRegionInfo(final Map<Text, byte[]> map)
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
