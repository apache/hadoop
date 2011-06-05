/**
 * Copyright 2011 The Apache Software Foundation
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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Handles SPLIT region event on Master.
 */
public class SplitRegionHandler extends EventHandler implements TotesHRegionInfo {
  private static final Log LOG = LogFactory.getLog(SplitRegionHandler.class);
  private final AssignmentManager assignmentManager;
  private final HRegionInfo parent;
  private final ServerName sn;
  private final List<HRegionInfo> daughters;
  /**
   * For testing only!  Set to true to skip handling of split.
   */
  public static boolean TEST_SKIP = false;

  public SplitRegionHandler(Server server,
      AssignmentManager assignmentManager, HRegionInfo regionInfo,
      ServerName sn, final List<HRegionInfo> daughters) {
    super(server, EventType.RS_ZK_REGION_SPLIT);
    this.assignmentManager = assignmentManager;
    this.parent = regionInfo;
    this.sn = sn;
    this.daughters = daughters;
  }

  @Override
  public HRegionInfo getHRegionInfo() {
    return this.parent;
  }
  
  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    String parentRegion = "UnknownRegion";
    if(parent != null) {
      parentRegion = parent.getRegionNameAsString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" + parentRegion;
  }

  @Override
  public void process() {
    LOG.debug("Handling SPLIT event for " + this.parent.getEncodedName() +
      "; deleting node");
    // The below is for testing ONLY!  We can't do fault injection easily, so
    // resort to this kinda uglyness -- St.Ack 02/25/2011.
    if (TEST_SKIP) {
      LOG.warn("Skipping split message, TEST_SKIP is set");
      return;
    }
    this.assignmentManager.handleSplitReport(this.sn, this.parent,
      this.daughters.get(0), this.daughters.get(1));
    // Remove region from ZK
    try {
      ZKAssign.deleteNode(this.server.getZooKeeper(),
        this.parent.getEncodedName(),
        EventHandler.EventType.RS_ZK_REGION_SPLIT);
    } catch (KeeperException e) {
      server.abort("Error deleting SPLIT node in ZK for transition ZK node (" +
        parent.getEncodedName() + ")", e);
    }
    LOG.info("Handled SPLIT report); parent=" +
      this.parent.getRegionNameAsString() +
      " daughter a=" + this.daughters.get(0).getRegionNameAsString() +
      "daughter b=" + this.daughters.get(1).getRegionNameAsString());
  }
}