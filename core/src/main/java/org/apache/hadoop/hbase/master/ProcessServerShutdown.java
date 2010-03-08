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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 
 * Instantiated when a server's lease has expired, meaning it has crashed.
 * The region server's log file needs to be split up for each region it was
 * serving, and the regions need to get reassigned.
 */
class ProcessServerShutdown extends RegionServerOperation {
  private final String deadServer;
  private boolean isRootServer;
  private List<MetaRegion> metaRegions;

  private Path rsLogDir;
  private boolean logSplit;
  private boolean rootRescanned;
  private HServerAddress deadServerAddress;


  private static class ToDoEntry {
    boolean regionOffline;
    final HRegionInfo info;

    ToDoEntry(final HRegionInfo info) {
      this.regionOffline = false;
      this.info = info;
    }
  }

  /**
   * @param master
   * @param serverInfo
   */
  public ProcessServerShutdown(HMaster master, HServerInfo serverInfo) {
    super(master);
    this.deadServer = HServerInfo.getServerName(serverInfo);
    this.deadServerAddress = serverInfo.getServerAddress();
    this.logSplit = false;
    this.rootRescanned = false;
    this.rsLogDir =
      new Path(master.getRootDir(), HLog.getHLogDirectoryName(serverInfo));

    // check to see if I am responsible for either ROOT or any of the META tables.

    closeMetaRegions();
  }

  private void closeMetaRegions() {
    isRootServer = master.getRegionManager().isRootServer(deadServerAddress);
    if (isRootServer) {
      master.getRegionManager().unsetRootRegion();
    }
    else {
        //HBASE-1928: Check whether this server has been transitioning the ROOT table
        isRootServer = master.getRegionManager().isRootServerCandidate (deadServer);
        if (isRootServer) {
          master.getRegionManager().unsetRootRegion();
        }
    }

    List<byte[]> metaStarts = master.getRegionManager().listMetaRegionsForServer(deadServerAddress);

    metaRegions = new ArrayList<MetaRegion>();
    for (byte [] region : metaStarts) {
      MetaRegion r = master.getRegionManager().offlineMetaRegion(region);
      metaRegions.add(r);
    }

    //HBASE-1928: Check whether this server has been transitioning the META table
    HRegionInfo metaServerRegionInfo = master.getRegionManager().getMetaServerRegionInfo (deadServer);
    if (metaServerRegionInfo != null) {
      metaRegions.add (new MetaRegion (deadServerAddress, metaServerRegionInfo));
    }
  }

  @Override
  public String toString() {
    return "ProcessServerShutdown of " + this.deadServer;
  }

  /** Finds regions that the dead region server was serving
   */
  protected void scanMetaRegion(HRegionInterface server, long scannerId,
    byte [] regionName)
  throws IOException {
    List<ToDoEntry> toDoList = new ArrayList<ToDoEntry>();
    Set<HRegionInfo> regions = new HashSet<HRegionInfo>();
    List<byte []> emptyRows = new ArrayList<byte []>();
    try {
      while (true) {
        Result values = null;
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
        byte [] row = values.getRow();
        // Check server name.  If null, skip (We used to consider it was on
        // shutdown server but that would mean that we'd reassign regions that
        // were already out being assigned, ones that were product of a split
        // that happened while the shutdown was being processed.
        String serverAddress = 
          Bytes.toString(values.getValue(CATALOG_FAMILY, SERVER_QUALIFIER));
        long startCode =
          Bytes.toLong(values.getValue(CATALOG_FAMILY, STARTCODE_QUALIFIER));
        String serverName = null;
        if (serverAddress != null && serverAddress.length() > 0) {
          serverName = HServerInfo.getServerName(serverAddress, startCode);
        }
        if (serverName == null || !deadServer.equals(serverName)) {
          // This isn't the server you're looking for - move along
          continue;
        }

        if (LOG.isDebugEnabled() && row != null) {
          LOG.debug("Shutdown scanner for " + serverName + " processing " +
            Bytes.toString(row));
        }

//        HRegionInfo info = master.getHRegionInfo(row, values.rowResult());
        HRegionInfo info = master.getHRegionInfo(row, values);
        if (info == null) {
          emptyRows.add(row);
          continue;
        }

        synchronized (master.getRegionManager()) {
          if (info.isMetaTable()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("removing meta region " +
                  Bytes.toString(info.getRegionName()) +
              " from online meta regions");
            }
            master.getRegionManager().offlineMetaRegion(info.getStartKey());
          }

          ToDoEntry todo = new ToDoEntry(info);
          toDoList.add(todo);

          if (master.getRegionManager().isOfflined(info.getRegionNameAsString()) ||
              info.isOffline()) {
            master.getRegionManager().removeRegion(info);
            // Mark region offline
            if (!info.isOffline()) {
              todo.regionOffline = true;
            }
          } else {
            if (!info.isOffline() && !info.isSplit()) {
              // Get region reassigned
              regions.add(info);
            }
          }
        }
      }
    } finally {
      if (scannerId != -1L) {
        try {
          server.close(scannerId);
        } catch (IOException e) {
          LOG.error("Closing scanner",
            RemoteExceptionHandler.checkIOException(e));
        }
      }
    }

    // Scan complete. Remove any rows which had empty HRegionInfos
    
    if (emptyRows.size() > 0) {
      LOG.warn("Found " + emptyRows.size() +
        " rows with empty HRegionInfo while scanning meta region " +
        Bytes.toString(regionName));
      master.deleteEmptyMetaRows(server, regionName, emptyRows);
    }
    // Update server in root/meta entries
    for (ToDoEntry e: toDoList) {
      if (e.regionOffline) {
        HRegion.offlineRegionInMETA(server, regionName, e.info);
      }
    }

    // Get regions reassigned
    for (HRegionInfo info: regions) {
      master.getRegionManager().setUnassigned(info, true);
    }
  }

  private class ScanRootRegion extends RetryableMetaOperation<Boolean> {
    ScanRootRegion(MetaRegion m, HMaster master) {
      super(m, master);
    }

    public Boolean call() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process server shutdown scanning root region on " +
            master.getRegionManager().getRootRegionLocation().getBindAddress());
      }
      Scan scan = new Scan();
      scan.addFamily(CATALOG_FAMILY);
      long scannerId = server.openScanner(
          HRegionInfo.ROOT_REGIONINFO.getRegionName(), scan);
      scanMetaRegion(server, scannerId,
          HRegionInfo.ROOT_REGIONINFO.getRegionName());
      return true;
    }
  }

  private class ScanMetaRegions extends RetryableMetaOperation<Boolean> {
    ScanMetaRegions(MetaRegion m, HMaster master) {
      super(m, master);
    }
    
    public Boolean call() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process server shutdown scanning " +
          Bytes.toString(m.getRegionName()) + " on " + m.getServer());
      }
      Scan scan = new Scan();
      scan.addFamily(CATALOG_FAMILY);
      long scannerId = server.openScanner(
          m.getRegionName(), scan);
      scanMetaRegion(server, scannerId, m.getRegionName());
      return true;
    }
  }

  @Override
  protected boolean process() throws IOException {
    LOG.info("process shutdown of server " + this.deadServer +
      ": logSplit: " +
      logSplit + ", rootRescanned: " + rootRescanned +
      ", numberOfMetaRegions: " + 
      master.getRegionManager().numMetaRegions() +
      ", onlineMetaRegions.size(): " + 
      master.getRegionManager().numOnlineMetaRegions());
    if (!logSplit) {
      // Process the old log file
      if (this.master.getFileSystem().exists(rsLogDir)) {
        if (!master.getRegionManager().splitLogLock.tryLock()) {
          return false;
        }
        try {
          HLog.splitLog(master.getRootDir(), rsLogDir,
              this.master.getOldLogDir(), this.master.getFileSystem(),
            this.master.getConfiguration());
        } finally {
          master.getRegionManager().splitLogLock.unlock();
        }
      }
      logSplit = true;
    }

    LOG.info("Log split complete, meta reassignment and scanning:");

    if (this.isRootServer) {
      LOG.info("ProcessServerShutdown reassigning ROOT region");
      master.getRegionManager().reassignRootRegion();

      isRootServer = false;  // prevent double reassignment... heh.
    }

    for (MetaRegion metaRegion : metaRegions) {
      LOG.info("ProcessServerShutdown setting to unassigned: " + metaRegion.toString());
      master.getRegionManager().setUnassigned(metaRegion.getRegionInfo(), true);
    }
    // one the meta regions are online, "forget" about them.  Since there are explicit
    // checks below to make sure meta/root are online, this is likely to occur.
    metaRegions.clear();

    if (!rootAvailable()) {
      // Return true so that worker does not put this request back on the
      // toDoQueue.
      // rootAvailable() has already put it on the delayedToDoQueue
      return true;
    }

    if (!rootRescanned) {
      // Scan the ROOT region
      Boolean result = new ScanRootRegion(
          new MetaRegion(master.getRegionManager().getRootRegionLocation(),
              HRegionInfo.ROOT_REGIONINFO), this.master).doWithRetries();
      if (result == null) {
        // Master is closing - give up
        return true;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("process server shutdown scanning root region on " +
          master.getRegionManager().getRootRegionLocation().getBindAddress() + 
          " finished " + Thread.currentThread().getName());
      }
      rootRescanned = true;
    }
    if (!metaTableAvailable()) {
      // We can't proceed because not all meta regions are online.
      // metaAvailable() has put this request on the delayedToDoQueue
      // Return true so that worker does not put this on the toDoQueue
      return true;
    }

    List<MetaRegion> regions = master.getRegionManager().getListOfOnlineMetaRegions();
    for (MetaRegion r: regions) {
      Boolean result = new ScanMetaRegions(r, this.master).doWithRetries();
      if (result == null) {
        break;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("process server shutdown finished scanning " +
          Bytes.toString(r.getRegionName()) + " on " + r.getServer());
      }
    }
    // Remove this server from dead servers list.  Finished splitting logs.
    this.master.getServerManager().removeDeadServer(deadServer);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removed " + deadServer + " from deadservers Map");
    }
    return true;
  }

  @Override
  protected int getPriority() {
    return 2; // high but not highest priority
  }
}
