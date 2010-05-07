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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Writables;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

/**
 * Instantiated to enable or disable a table
 */
class ChangeTableState extends TableOperation {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private boolean online;
  // Do in order.
  protected final Map<String, HashSet<HRegionInfo>> servedRegions =
    new TreeMap<String, HashSet<HRegionInfo>>();
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
      HashSet<HRegionInfo> regions = this.servedRegions.get(serverName);
      if (regions == null) {
        regions = new HashSet<HRegionInfo>();
      }
      regions.add(info);
      this.servedRegions.put(serverName, regions);
    }
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    // Process regions not being served
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing unserved regions");
    }
    for (HRegionInfo i: this.unservedRegions) {
      if (i.isOffline() && i.isSplit()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping region " + i.toString() +
            " because it is offline and split");
        }
        continue;
      }

      if(!this.online && this.master.getRegionManager().
          isPendingOpen(i.getRegionNameAsString())) {
        LOG.debug("Skipping region " + i.toString() +
          " because it is pending open, will tell it to close later");
        continue;
      }

      // If it's already offline then don't set it a second/third time, skip
      // Same for online, don't set again if already online
      if (!(i.isOffline() && !online) && !(!i.isOffline() && online)) {
        // Update meta table
        Put put = updateRegionInfo(i);
        server.put(m.getRegionName(), put);
        Delete delete = new Delete(i.getRegionName());
        delete.deleteColumns(CATALOG_FAMILY, SERVER_QUALIFIER);
        delete.deleteColumns(CATALOG_FAMILY, STARTCODE_QUALIFIER);
        server.delete(m.getRegionName(), delete);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removed server and startcode from row and set online=" +
          this.online + ": " + i.getRegionNameAsString());
      }
      synchronized (master.getRegionManager()) {
        if (this.online) {
          // Bring offline regions on-line
          if (!this.master.getRegionManager().regionIsOpening(i.getRegionNameAsString())) {
            this.master.getRegionManager().setUnassigned(i, false);
          }
        } else {
          // Prevent region from getting assigned.
          this.master.getRegionManager().removeRegion(i);
        }
      }
    }

    // Process regions currently being served
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing regions currently being served");
    }
    synchronized (this.master.getRegionManager()) {
      for (Map.Entry<String, HashSet<HRegionInfo>> e:
          this.servedRegions.entrySet()) {
        String serverName = e.getKey();
        if (this.online) {
          LOG.debug("Already online");
          continue;                             // Already being served
        }

        // Cause regions being served to be taken off-line and disabled
        for (HRegionInfo i: e.getValue()) {
          // The scan we did could be totally staled, get the freshest data
          Get get = new Get(i.getRegionName());
          get.addColumn(CATALOG_FAMILY, SERVER_QUALIFIER);
          Result values = server.get(m.getRegionName(), get);
          String serverAddress = BaseScanner.getServerAddress(values);
          // If this region is unassigned, skip!
          if(serverAddress.length() == 0) {
            continue;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding region " + i.getRegionNameAsString() +
              " to setClosing list");
          }
          // this marks the regions to be closed
          this.master.getRegionManager().setClosing(serverName, i, true);
        }
      }
    }
    this.servedRegions.clear();
  }

  protected Put updateRegionInfo(final HRegionInfo i)
  throws IOException {
    i.setOffline(!online);
    Put put = new Put(i.getRegionName());
    put.add(CATALOG_FAMILY, REGIONINFO_QUALIFIER, Writables.getBytes(i));
    return put;
  }
}
