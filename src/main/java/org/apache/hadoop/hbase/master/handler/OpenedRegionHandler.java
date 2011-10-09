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
package org.apache.hadoop.hbase.master.handler;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Handles OPENED region event on Master.
 */
public class OpenedRegionHandler extends EventHandler implements TotesHRegionInfo {
  private static final Log LOG = LogFactory.getLog(OpenedRegionHandler.class);
  private final AssignmentManager assignmentManager;
  private final HRegionInfo regionInfo;
  private final ServerName sn;
  private final OpenedPriority priority;
  private final int expectedVersion;

  private enum OpenedPriority {
    ROOT (1),
    META (2),
    USER (3);

    private final int value;
    OpenedPriority(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  };

  public OpenedRegionHandler(Server server,
      AssignmentManager assignmentManager, HRegionInfo regionInfo,
      ServerName sn, int expectedVersion) {
    super(server, EventType.RS_ZK_REGION_OPENED);
    this.assignmentManager = assignmentManager;
    this.regionInfo = regionInfo;
    this.sn = sn;
    this.expectedVersion = expectedVersion;
    if(regionInfo.isRootRegion()) {
      priority = OpenedPriority.ROOT;
    } else if(regionInfo.isMetaRegion()) {
      priority = OpenedPriority.META;
    } else {
      priority = OpenedPriority.USER;
    }
  }

  @Override
  public int getPriority() {
    return priority.getValue();
  }

  @Override
  public HRegionInfo getHRegionInfo() {
    return this.regionInfo;
  }
  
  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid();
  }

  @Override
  public void process() {
    // Code to defend against case where we get SPLIT before region open
    // processing completes; temporary till we make SPLITs go via zk -- 0.92.
    RegionState regionState = this.assignmentManager.isRegionInTransition(regionInfo);
    if (regionState != null
        && regionState.getState().equals(RegionState.State.OPEN)) {
      if (deleteOpenedNode(expectedVersion)) {
        // Remove region from in-memory transition and unassigned node from ZK
        this.assignmentManager.regionOnline(regionInfo, this.sn);
        debugLog(regionInfo, "The master has opened the region " +
            regionInfo.getRegionNameAsString() + " that was online on " +
            this.sn.toString());
      } else {
        LOG.error("The znode of region " + regionInfo.getRegionNameAsString() +
          " could not be deleted.");
      }
    } else {
      LOG.warn("Skipping the onlining of " + regionInfo.getRegionNameAsString() +
        " because regions is NOT in RIT -- presuming this is because it SPLIT");
    }
    if (this.assignmentManager.getZKTable().isDisablingOrDisabledTable(
        regionInfo.getTableNameAsString())) {
      debugLog(regionInfo,
          "Opened region " + regionInfo.getRegionNameAsString() + " but "
          + "this table is disabled, triggering close of region");
      assignmentManager.unassign(regionInfo);
    }
  }

  private boolean deleteOpenedNode(int expectedVersion) {
    debugLog(regionInfo, "Handling OPENED event for " +
      this.regionInfo.getRegionNameAsString() + " from " + this.sn.toString() +
      "; deleting unassigned node");
    try {
      // delete the opened znode only if the version matches.
      return ZKAssign.deleteNode(server.getZooKeeper(),
          regionInfo.getEncodedName(), EventType.RS_ZK_REGION_OPENED, expectedVersion);
    } catch(KeeperException.NoNodeException e){
      // Getting no node exception here means that already the region has been opened.
      LOG.warn("The znode of the region " + regionInfo.getRegionNameAsString() +
        " would have already been deleted");
      return false;
    } catch (KeeperException e) {
      server.abort("Error deleting OPENED node in ZK (" +
        regionInfo.getRegionNameAsString() + ")", e);
    }
    return false;
  }

  private void debugLog(HRegionInfo region, String string) {
    if (region.isMetaTable() || region.isRootRegion()) {
      LOG.info(string);
    } else {
      LOG.debug(string);
    }
  }
}
