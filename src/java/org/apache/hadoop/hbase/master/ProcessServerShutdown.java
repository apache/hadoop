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
import java.util.HashSet;
import java.util.HashMap;
import java.util.SortedMap;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionInterface;
import org.apache.hadoop.hbase.HRegion;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.HLog;

/** 
 * Instantiated when a server's lease has expired, meaning it has crashed.
 * The region server's log file needs to be split up for each region it was
 * serving, and the regions need to get reassigned.
 */
class ProcessServerShutdown extends RegionServerOperation {
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
  public ProcessServerShutdown(HMaster master, HServerInfo serverInfo) {
    super(master);
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
    this.oldLogDir = new Path(master.rootdir, dirName.toString());
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
        RowMap rm = RowMap.fromHbaseMapWritable(values);
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
        HRegionInfo info = master.getHRegionInfo(map);
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
          master.regionManager.offlineMetaRegion(info.getStartKey());
        }

        ToDoEntry todo = new ToDoEntry(row, info);
        toDoList.add(todo);

        if (master.regionManager.isMarkedClosedNoReopen(deadServerName, info.getRegionName())) {
          master.regionManager.noLongerMarkedClosedNoReopen(deadServerName, info.getRegionName());
          master.regionManager.noLongerUnassigned(info);
          if (master.regionManager.isMarkedForDeletion(info.getRegionName())) {
            // Delete this region
            master.regionManager.regionDeleted(info.getRegionName());
            todo.deleteRegion = true;
          } else {
            // Mark region offline
            todo.regionOffline = true;
          }
        } else {
          // Get region reassigned
          regions.add(info);

          // If it was pending, remove.
          // Otherwise will obstruct its getting reassigned.
          master.regionManager.noLongerPending(info.getRegionName());
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
      master.regionManager.setUnassigned(info);
    }
  }

  @Override
  protected boolean process() throws IOException {
    LOG.info("process shutdown of server " + deadServer + ": logSplit: " +
      this.logSplit + ", rootRescanned: " + rootRescanned +
      ", numberOfMetaRegions: " + 
      master.regionManager.numMetaRegions() +
      ", onlineMetaRegions.size(): " + 
      master.regionManager.numOnlineMetaRegions());

    if (!logSplit) {
      // Process the old log file
      if (master.fs.exists(oldLogDir)) {
        if (!master.regionManager.splitLogLock.tryLock()) {
          return false;
        }
        try {
          HLog.splitLog(master.rootdir, oldLogDir, master.fs, master.conf);
        } finally {
          master.regionManager.splitLogLock.unlock();
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
        if (master.closed.get()) {
          return true;
        }
        server = master.connection.getHRegionConnection(
          master.getRootRegionLocation());
        scannerId = -1L;

        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("process server shutdown scanning root region on " +
              master.getRootRegionLocation().getBindAddress());
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
          master.getRootRegionLocation().getBindAddress() + 
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

    for (int tries = 0; tries < numRetries; tries++) {
      try {
        if (master.closed.get()) {
          return true;
        }
        List<MetaRegion> regions = master.regionManager.getListOfOnlineMetaRegions();
        for (MetaRegion r: regions) {
          HRegionInterface server = null;
          long scannerId = -1L;

          if (LOG.isDebugEnabled()) {
            LOG.debug("process server shutdown scanning " +
              r.getRegionName() + " on " + r.getServer() + " " +
              Thread.currentThread().getName());
          }
          server = master.connection.getHRegionConnection(r.getServer());

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
        master.serverManager.removeDeadServer(deadServerName);
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