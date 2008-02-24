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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

/** Instantiated to enable or disable a table */
class ChangeTableState extends TableOperation {
  private boolean online;

  protected final Map<String, HashSet<HRegionInfo>> servedRegions =
    new HashMap<String, HashSet<HRegionInfo>>();
  
  protected long lockid;

  ChangeTableState(final HMaster master, final Text tableName, 
    final boolean onLine) 
  throws IOException {
    super(master, tableName);
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

      BatchUpdate b = new BatchUpdate(i.getRegionName());
      updateRegionInfo(b, i);
      b.delete(COL_SERVER);
      b.delete(COL_STARTCODE);
      server.batchUpdate(m.getRegionName(), b);
      if (LOG.isDebugEnabled()) {
        LOG.debug("updated columns in row: " + i.getRegionName());
      }

      if (online) {
        // Bring offline regions on-line
        if (!master.regionManager.isUnassigned(i)) {
          master.regionManager.setUnassigned(i);
        }
      } else {
        // Prevent region from getting assigned.
        master.regionManager.noLongerUnassigned(i);
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
        
      Map<Text, HRegionInfo> killedRegions = 
        master.regionManager.getMarkedClosedNoReopen(serverName);
      if (killedRegions != null) {
        localKillList.putAll(killedRegions);
      }
      
      for (HRegionInfo i: e.getValue()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("adding region " + i.getRegionName() +
              " to kill list");
        }
        localKillList.put(i.getRegionName(), i);
      }
      if (localKillList.size() > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("inserted local kill list into kill list for server " +
              serverName);
        }
        master.regionManager.markClosedNoReopenBulk(serverName, localKillList);
      }
    }
    servedRegions.clear();
  }

  protected void updateRegionInfo(final BatchUpdate b, final HRegionInfo i)
  throws IOException {
    i.setOffline(!online);
    b.put(COL_REGIONINFO, Writables.getBytes(i));
  }
}