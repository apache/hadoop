/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.ipc.RemoteException;


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
abstract class BaseScanner extends Chore implements HConstants {
  static final Log LOG = LogFactory.getLog(BaseScanner.class.getName());
    
  private final boolean rootRegion;
  protected final HMaster master;
  
  protected boolean initialScanComplete;
  
  protected abstract boolean initialScan();
  protected abstract void maintenanceScan();
  
  // will use this variable to synchronize and make sure we aren't interrupted 
  // mid-scan
  final Object scannerLock = new Object();
  
  BaseScanner(final HMaster master, final boolean rootRegion, final int period,
      final AtomicBoolean stop) {
    super(period, stop);
    this.rootRegion = rootRegion;
    this.master = master;
    this.initialScanComplete = false;
  }
  
  /** @return true if initial scan completed successfully */
  public boolean isInitialScanComplete() {
    return initialScanComplete;
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
    Map<HRegionInfo, Result> splitParents =
      new HashMap<HRegionInfo, Result>();
    List<byte []> emptyRows = new ArrayList<byte []>();
    int rows = 0;
    try {
      regionServer = master.connection.getHRegionConnection(region.getServer());
      Scan s = new Scan().addFamily(HConstants.CATALOG_FAMILY);
      // Make this scan do a row at a time otherwise, data can be stale.
      s.setCaching(1);
      scannerId = regionServer.openScanner(region.getRegionName(), s);
      while (true) {
        Result values = regionServer.next(scannerId);
        if (values == null || values.size() == 0) {
          break;
        }
        HRegionInfo info = master.getHRegionInfo(values.getRow(), values);
        if (info == null) {
          emptyRows.add(values.getRow());
          continue;
        }
        String serverAddress = getServerAddress(values);
        long startCode = getStartCode(values);

        // Note Region has been assigned.
        checkAssigned(regionServer, region, info, serverAddress, startCode);
        if (isSplitParent(info)) {
          splitParents.put(info, values);
        }
        rows += 1;
      }
      if (rootRegion) {
        this.master.regionManager.setNumMetaRegions(rows);
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

    // Scan is finished.
    
    // First clean up any meta region rows which had null HRegionInfos
    if (emptyRows.size() > 0) {
      LOG.warn("Found " + emptyRows.size() + " rows with empty HRegionInfo " +
        "while scanning meta region " + Bytes.toString(region.getRegionName()));
      this.master.deleteEmptyMetaRows(regionServer, region.getRegionName(),
          emptyRows);
    }

    // Take a look at split parents to see if any we can clean up.
    
    if (splitParents.size() > 0) {
      for (Map.Entry<HRegionInfo, Result> e : splitParents.entrySet()) {
        HRegionInfo hri = e.getKey();
        cleanupSplits(region.getRegionName(), regionServer, hri, e.getValue());
      }
    }
    LOG.info(Thread.currentThread().getName() + " scan of " + rows +
      " row(s) of meta region " + region.toString() + " complete");
  }

  /*
   * @param r
   * @return Empty String or server address found in <code>r</code>
   */
  private String getServerAddress(final Result r) {
    byte [] val = r.getValue(CATALOG_FAMILY, SERVER_QUALIFIER);
    return val == null || val.length <= 0? "": Bytes.toString(val);
  }

  /*
   * @param r
   * @return Return 0L or server startcode found in <code>r</code>
   */
  private long getStartCode(final Result r) {
    byte [] val = r.getValue(CATALOG_FAMILY, STARTCODE_QUALIFIER);
    return val == null || val.length <= 0? 0L: Bytes.toLong(val);
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
      LOG.warn("Region is split but not offline: " +
        info.getRegionNameAsString());
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
  private boolean cleanupSplits(final byte [] metaRegionName, 
    final HRegionInterface srvr, final HRegionInfo parent,
    Result rowContent)
  throws IOException {
    boolean result = false;
    boolean hasReferencesA = hasReferences(metaRegionName, srvr,
        parent.getRegionName(), rowContent, CATALOG_FAMILY, SPLITA_QUALIFIER);
    boolean hasReferencesB = hasReferences(metaRegionName, srvr,
        parent.getRegionName(), rowContent, CATALOG_FAMILY, SPLITB_QUALIFIER);
    if (!hasReferencesA && !hasReferencesB) {
      LOG.info("Deleting region " + parent.getRegionNameAsString() +
        " (encoded=" + parent.getEncodedName() +
        ") because daughter splits no longer hold references");
      HRegion.deleteRegion(this.master.fs, this.master.rootdir, parent);
      HRegion.removeRegionFromMETA(srvr, metaRegionName,
        parent.getRegionName());
      result = true;
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
  private boolean hasReferences(final byte [] metaRegionName, 
    final HRegionInterface srvr, final byte [] parent,
    Result rowContent, final byte [] splitFamily, byte [] splitQualifier)
  throws IOException {
    boolean result = false;
    HRegionInfo split =
      Writables.getHRegionInfoOrNull(rowContent.getValue(splitFamily, splitQualifier));
    if (split == null) {
      return result;
    }
    Path tabledir =
      new Path(this.master.rootdir, split.getTableDesc().getNameAsString());
    for (HColumnDescriptor family: split.getTableDesc().getFamilies()) {
      Path p = Store.getStoreHomedir(tabledir, split.getEncodedName(),
        family.getName());
      // Look for reference files.  Call listStatus with an anonymous
      // instance of PathFilter.
      FileStatus [] ps = this.master.fs.listStatus(p,
          new PathFilter () {
            public boolean accept(Path path) {
              return StoreFile.isReference(path);
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
      LOG.debug(split.getRegionNameAsString() + "/" + split.getEncodedName()
          + " no longer has references to " + Bytes.toStringBinary(parent));
    }
    
    Delete delete = new Delete(parent);
    delete.deleteColumns(splitFamily, splitQualifier);
    srvr.delete(metaRegionName, delete);
    
    return result;
  }

  /*
   * Check the passed region is assigned.  If not, add to unassigned.
   * @param regionServer
   * @param meta
   * @param info
   * @param serverAddress
   * @param startCode
   * @throws IOException
   */
  protected void checkAssigned(final HRegionInterface regionServer,
    final MetaRegion meta, final HRegionInfo info,
    final String serverAddress, final long startCode) 
  throws IOException {
    String serverName = null;
    String sa = serverAddress;
    long sc = startCode;
    if (sa == null || sa.length() <= 0) {
      // Scans are sloppy.  They don't respect row locks and they get and 
      // cache a row internally so may have data that is a little stale.  Make
      // sure that for sure this serverAddress is null.  We are trying to
      // avoid double-assignments.  See hbase-1784.  Will have to wait till
      // 0.21 hbase where we use zk to mediate state transitions to do better.
      Get g = new Get(info.getRegionName());
      g.addFamily(HConstants.CATALOG_FAMILY);
      Result r = regionServer.get(meta.getRegionName(), g);
      if (r != null && !r.isEmpty()) {
        sa = getServerAddress(r);
        if (sa != null && sa.length() > 0) {
          // Reget startcode in case its changed in the meantime too.
          sc = getStartCode(r);
          LOG.debug("GET got values when meta found none: serverAddress=" + sa
              + ", startCode=" + sc);
        }
      }
    }
    if (sa != null && sa.length() > 0) {
      serverName = HServerInfo.getServerName(sa, sc);
    }
    HServerInfo storedInfo = null;
    synchronized (this.master.regionManager) {
      /* We don't assign regions that are offline, in transition or were on
       * a dead server. Regions that were on a dead server will get reassigned
       * by ProcessServerShutdown
       */
      if (info.isOffline() ||
        this.master.regionManager.regionIsInTransition(info.getRegionNameAsString()) ||
          (serverName != null && this.master.serverManager.isDead(serverName))) {
        return;
      }
      if (serverName != null) {
        storedInfo = this.master.serverManager.getServerInfo(serverName);
      }

      // If we can't find the HServerInfo, then add it to the list of
      //  unassigned regions.
      if (storedInfo == null) {
        // The current assignment is invalid
        if (LOG.isDebugEnabled()) {
          LOG.debug("Current assignment of " + info.getRegionNameAsString() +
            " is not valid; " + " serverAddress=" + sa +
            ", startCode=" + sc + " unknown.");
        }
        // Now get the region assigned
        this.master.regionManager.setUnassigned(info, true);
      }
    }
  }

  /**
   * Notify the thread to die at the end of its next run
   */
  public void interruptIfAlive() {
    synchronized(scannerLock){
      if (isAlive()) {
        super.interrupt();
      }
    }
  }
}
