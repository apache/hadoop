/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;
import org.apache.hadoop.util.Timer;

/**
 * Disabled tracker for slow peers. To be used when dfs.datanode.peer.stats.enabled is disabled.
 */
@InterfaceAudience.Private
public class SlowPeerDisabledTracker extends SlowPeerTracker {

  private static final Logger LOG = LoggerFactory.getLogger(SlowPeerDisabledTracker.class);

  public SlowPeerDisabledTracker(Configuration conf, Timer timer) {
    super(conf, timer);
  }

  @Override
  public boolean isSlowPeerTrackerEnabled() {
    return false;
  }

  @Override
  public void addReport(String slowNode, String reportingNode, OutlierMetrics slowNodeMetrics) {
    LOG.trace("Adding slow peer report is disabled. To enable it, please enable config {}.",
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
  }

  @Override
  public Set<SlowPeerLatencyWithReportingNode> getReportsForNode(String slowNode) {
    LOG.trace("Retrieval of slow peer report is disabled. To enable it, please enable config {}.",
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
    return ImmutableSet.of();
  }

  @Override
  public Map<String, SortedSet<SlowPeerLatencyWithReportingNode>> getReportsForAllDataNodes() {
    LOG.trace("Retrieval of slow peer report for all nodes is disabled. "
            + "To enable it, please enable config {}.",
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
    return ImmutableMap.of();
  }

  @Override
  public String getJson() {
    LOG.trace("Retrieval of slow peer reports as json string is disabled. "
            + "To enable it, please enable config {}.",
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
    return null;
  }

  @Override
  public List<String> getSlowNodes(int numNodes) {
    return ImmutableList.of();
  }

}
