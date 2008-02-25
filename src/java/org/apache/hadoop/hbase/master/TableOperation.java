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
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

/**
 * Abstract base class for operations that need to examine all HRegionInfo 
 * objects that make up a table. (For a table, operate on each of its rows
 * in .META.) To gain the 
 */
abstract class TableOperation implements HConstants {
  static final Long ZERO_L = Long.valueOf(0L);
  
  protected static final Log LOG = 
    LogFactory.getLog(TableOperation.class.getName());
  
  protected Set<MetaRegion> metaRegions;
  protected Text tableName;
  protected Set<HRegionInfo> unservedRegions;
  protected HMaster master;
  protected final int numRetries;
  
  protected TableOperation(final HMaster master, final Text tableName) 
  throws IOException {
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

  void process() throws IOException {
    for (int tries = 0; tries < numRetries; tries++) {
      boolean tableExists = false;
      try {
        // Prevent meta scanner from running
        synchronized(master.regionManager.metaScannerThread.scannerLock) {
          for (MetaRegion m: metaRegions) {
            // Get a connection to a meta server
            HRegionInterface server =
              master.connection.getHRegionConnection(m.getServer());

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
                RowMap rm = RowMap.fromHbaseMapWritable(values);
                SortedMap<Text, byte[]> map = rm.getMap();
                HRegionInfo info = this.master.getHRegionInfo(map);
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
          this.master.checkFileSystem();
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
