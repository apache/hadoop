/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework.ProportionalCapacityPreemptionPolicyMockFramework.parseResourceFromString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MockSchedulerNodes {
  private static final Logger LOG = LoggerFactory.getLogger(MockSchedulerNodes.class);
  private String config;
  private Map<NodeId, FiCaSchedulerNode> nodeIdToSchedulerNodes = new HashMap<>();

  MockSchedulerNodes(String config) {
    this.config = config;
    init();
  }

  /**
   * Format is:
   * host1=partition[ res=resource];
   * host2=partition[ res=resource];
   */
  private void init() {
    String[] nodesConfigStrArray = config.split(";");
    for (String p : nodesConfigStrArray) {
      String[] arr = p.split(" ");

      NodeId nodeId = NodeId.newInstance(arr[0].substring(0, arr[0].indexOf("=")), 1);
      String partition = arr[0].substring(arr[0].indexOf("=") + 1);

      FiCaSchedulerNode sn = mock(FiCaSchedulerNode.class);
      when(sn.getNodeID()).thenReturn(nodeId);
      when(sn.getPartition()).thenReturn(partition);

      Resource totalRes = Resources.createResource(0);
      if (arr.length > 1) {
        String res = arr[1];
        if (res.contains("res=")) {
          String resString = res.substring(
              res.indexOf("res=") + "res=".length());
          totalRes = parseResourceFromString(resString);
        }
      }
      when(sn.getTotalResource()).thenReturn(totalRes);
      when(sn.getUnallocatedResource()).thenReturn(Resources.clone(totalRes));

      // TODO, add settings of killable resources when necessary
      when(sn.getTotalKillableResources()).thenReturn(Resources.none());

      List<RMContainer> liveContainers = new ArrayList<>();
      when(sn.getCopiedListOfRunningContainers()).thenReturn(liveContainers);

      nodeIdToSchedulerNodes.put(nodeId, sn);

      LOG.debug("add scheduler node, id=" + nodeId + ", partition=" + partition);
    }
  }

  Map<NodeId, FiCaSchedulerNode> getNodeIdToSchedulerNodes() {
    return nodeIdToSchedulerNodes;
  }
}
