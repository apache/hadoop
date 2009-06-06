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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Writables;

/** Instantiated to enable or disable a table */
class ChangeTableState extends TableOperation {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private boolean online;

  protected final Map<String, HashSet<HRegionInfo>> servedRegions =
    new HashMap<String, HashSet<HRegionInfo>>();
  
  protected long lockid;

  ChangeTableState(final HMaster master, final byte [] tableName, 
    final boolean onLine) 
  throws IOException {
    super(master, tableName);
    this.online = onLine;
  }

  @Override
  protected void processScanItem(String serverName, HRegionInfo info) {
      
    if (isBeingServed(serverName)) {
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
          LOG.debug("Skipping region " + i.toString() +
              " because it is offline because it has been split");
        }
        continue;
      }

      // Update meta table
      Put put = updateRegionInfo(i);
      server.put(m.getRegionName(), put);
      
      Delete delete = new Delete(i.getRegionName());
      delete.deleteColumns(CATALOG_FAMILY, SERVER_QUALIFIER);
      delete.deleteColumns(CATALOG_FAMILY, STARTCODE_QUALIFIER);
      server.delete(m.getRegionName(), delete);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated columns in row: " + i.getRegionNameAsString());
      }

      synchronized (master.regionManager) {
        if (online) {
          // Bring offline regions on-line
          if (!master.regionManager.regionIsOpening(i.getRegionNameAsString())) {
            master.regionManager.setUnassigned(i, false);
          }
        } else {
          // Prevent region from getting assigned.
          master.regionManager.removeRegion(i);
        }
      }
    }

    // Process regions currently being served
    if (LOG.isDebugEnabled()) {
      LOG.debug("processing regions currently being served");
    }
    synchronized (master.regionManager) {
      for (Map.Entry<String, HashSet<HRegionInfo>> e: servedRegions.entrySet()) {
        String serverName = e.getKey();
        if (online) {
          LOG.debug("Already online");
          continue;                             // Already being served
        }

        // Cause regions being served to be taken off-line and disabled
        for (HRegionInfo i: e.getValue()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("adding region " + i.getRegionNameAsString() + " to kill list");
          }
          // this marks the regions to be closed
          master.regionManager.setClosing(serverName, i, true);
        }
      }
    }
    servedRegions.clear();
  }

  protected Put updateRegionInfo(final HRegionInfo i)
  throws IOException {
    i.setOffline(!online);
    Put put = new Put(i.getRegionName());
    put.add(CATALOG_FAMILY, REGIONINFO_QUALIFIER, Writables.getBytes(i));
    return put;
  }
}
