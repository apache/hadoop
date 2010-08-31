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
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Handles CLOSED region event on Master.
 * <p>
 * If table is being disabled, deletes ZK unassigned node and removes from
 * regions in transition.
 * <p>
 * Otherwise, assigns the region to another server.
 */
public class ClosedRegionHandler extends EventHandler implements TotesHRegionInfo {
  private static final Log LOG = LogFactory.getLog(ClosedRegionHandler.class);

  private final AssignmentManager assignmentManager;
  private final RegionTransitionData data;
  private final HRegionInfo regionInfo;

  private final ClosedPriority priority;

  private enum ClosedPriority {
    ROOT (1),
    META (2),
    USER (3);

    private final int value;
    ClosedPriority(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  };

  public ClosedRegionHandler(Server server,
      AssignmentManager assignmentManager, RegionTransitionData data,
      HRegionInfo regionInfo) {
    super(server, EventType.RS2ZK_REGION_CLOSED);
    this.assignmentManager = assignmentManager;
    this.data = data;
    this.regionInfo = regionInfo;
    if(regionInfo.isRootRegion()) {
      priority = ClosedPriority.ROOT;
    } else if(regionInfo.isMetaRegion()) {
      priority = ClosedPriority.META;
    } else {
      priority = ClosedPriority.USER;
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
  public void process() {
    LOG.debug("Handling CLOSED event");
    // Check if this table is being disabled or not
    if (assignmentManager.isTableOfRegionDisabled(regionInfo.getRegionName())) {
      // Disabling so should not be reassigned, just delete the CLOSED node
      LOG.debug("Table being disabled so deleting ZK node and removing from " +
          "regions in transition, skipping assignment");
      try {
        ZKAssign.deleteClosedNode(server.getZooKeeper(),
            regionInfo.getEncodedName());
      } catch (KeeperException.NoNodeException nne) {
        LOG.warn("Tried to delete closed node for " + data + " but it does " +
            "not exist");
        return;
      } catch (KeeperException e) {
        server.abort("Error deleting CLOSED node in ZK", e);
      }
      assignmentManager.regionOffline(regionInfo);
      return;
    }
    // ZK Node is in CLOSED state, assign it.
    assignmentManager.setOffline(regionInfo);
    assignmentManager.assign(regionInfo);
  }
}