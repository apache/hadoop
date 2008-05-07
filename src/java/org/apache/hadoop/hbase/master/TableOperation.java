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
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Sleeper;

/**
 * Abstract base class for operations that need to examine all HRegionInfo 
 * objects that make up a table. (For a table, operate on each of its rows
 * in .META.) To gain the 
 */
abstract class TableOperation implements HConstants {
  static final Long ZERO_L = Long.valueOf(0L);
  
  protected static final Log LOG = LogFactory.getLog(TableOperation.class);
  
  protected Set<MetaRegion> metaRegions;
  protected Text tableName;
  protected Set<HRegionInfo> unservedRegions;
  protected HMaster master;
  protected final int numRetries;
  protected final Sleeper sleeper;
  
  protected TableOperation(final HMaster master, final Text tableName) 
  throws IOException {
    this.sleeper = master.sleeper;
    this.numRetries = master.numRetries;
    
    this.master = master;
    
    if (!this.master.isMasterRunning()) {
      throw new MasterNotRunningException();
    }

    this.tableName = tableName;
    this.unservedRegions = new HashSet<HRegionInfo>();

    // We can not access any meta region if they have not already been
    // assigned and scanned.

    if (master.regionManager.metaScannerThread.waitForMetaRegionsOrClose()) {
      // We're shutting down. Forget it.
      throw new MasterNotRunningException(); 
    }

    this.metaRegions = master.regionManager.getMetaRegionsForTable(tableName);
  }

  private class ProcessTableOperation extends RetryableMetaOperation<Boolean> {
    ProcessTableOperation(MetaRegion m, HMaster master) {
      super(m, master);
    }

    /** {@inheritDoc} */
    public Boolean call() throws IOException {
      boolean tableExists = false;

      // Open a scanner on the meta region
      long scannerId = server.openScanner(m.getRegionName(),
          COLUMN_FAMILY_ARRAY, tableName, HConstants.LATEST_TIMESTAMP, null);

      List<Text> emptyRows = new ArrayList<Text>();
      try {
        while (true) {
          RowResult values = server.next(scannerId);
          if(values == null || values.size() == 0) {
            break;
          }
          HRegionInfo info = this.master.getHRegionInfo(values.getRow(), values);
          if (info == null) {
            emptyRows.add(values.getRow());
            LOG.error(COL_REGIONINFO + " not found on " + values.getRow());
            continue;
          }
          String serverName = Writables.cellToString(values.get(COL_SERVER));
          long startCode = Writables.cellToLong(values.get(COL_STARTCODE));
          if (info.getTableDesc().getName().compareTo(tableName) > 0) {
            break; // Beyond any more entries for this table
          }

          tableExists = true;
          if (!isBeingServed(serverName, startCode)) {
            unservedRegions.add(info);
          }
          processScanItem(serverName, startCode, info);
        }
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

      // Get rid of any rows that have a null HRegionInfo

      if (emptyRows.size() > 0) {
        LOG.warn("Found " + emptyRows.size() +
            " rows with empty HRegionInfo while scanning meta region " +
            m.getRegionName());
        master.deleteEmptyMetaRows(server, m.getRegionName(), emptyRows);
      }

      if (!tableExists) {
        throw new TableNotFoundException(tableName + " does not exist");
      }

      postProcessMeta(m, server);
      unservedRegions.clear();

      return true;
    }
  }

  void process() throws IOException {
    // Prevent meta scanner from running
    synchronized(master.regionManager.metaScannerThread.scannerLock) {
      for (MetaRegion m: metaRegions) {
        new ProcessTableOperation(m, master).doWithRetries();
      }
    }
  }
  
  protected boolean isBeingServed(String serverName, long startCode) {
    boolean result = false;
    if (serverName != null && serverName.length() > 0 && startCode != -1L) {
      HServerInfo s = master.serverManager.getServerInfo(serverName);
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
