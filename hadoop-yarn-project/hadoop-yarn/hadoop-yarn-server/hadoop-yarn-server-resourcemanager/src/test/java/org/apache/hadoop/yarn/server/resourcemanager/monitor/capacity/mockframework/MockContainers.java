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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Map;

class MockContainers {
  private MockApplication mockApp;
  private Map<String, CSQueue> nameToCSQueues;
  private Map<NodeId, FiCaSchedulerNode> nodeIdToSchedulerNodes;

  MockContainers(MockApplication mockApp,
      Map<String, CSQueue> nameToCSQueues,
      Map<NodeId, FiCaSchedulerNode> nodeIdToSchedulerNodes) {
    this.mockApp = mockApp;
    this.nameToCSQueues = nameToCSQueues;
    this.nodeIdToSchedulerNodes = nodeIdToSchedulerNodes;
    init();
  }

  private void init() {
    String containersConfig = mockApp.containersConfig;
    int start = containersConfig.indexOf("=") + 1;
    int end = -1;
    int containerId = 1;

    while (start < containersConfig.length()) {
      while (start < containersConfig.length()
          && containersConfig.charAt(start) != '(') {
        start++;
      }
      if (start >= containersConfig.length()) {
        throw new IllegalArgumentException(
            "Error in containers specification, line=" + containersConfig);
      }
      end = start + 1;
      while (end < containersConfig.length()
          && containersConfig.charAt(end) != ')') {
        end++;
      }
      if (end >= containersConfig.length()) {
        throw new IllegalArgumentException(
            "Error in containers specification, line=" + containersConfig);
      }

      // now we found start/end, get container values
      String[] values = containersConfig.substring(start + 1, end).split(",");
      if (values.length < 6 || values.length > 8) {
        throw new IllegalArgumentException("Format to define container is:"
            + "(priority,resource,host,label expression,repeat,reserved, pending)");
      }

      ContainerSpecification.Builder builder = ContainerSpecification.Builder.create()
          .withPriority(values[0])
          .withResource(values[1])
          .withHostname(values[2])
          .withLabel(values[3])
          .withRepeat(values[4])
          .withReserved(values[5]);

      if (values.length >= 7) {
        builder.withPendingResource(values[6]);
      }
      if (values.length == 8) {
        builder.withUsername(values[7]);
      }
      ContainerSpecification containerSpec = builder.build();

      Resource usedResources = Resource.newInstance(0, 0);
      for (int i = 0; i < containerSpec.repeat; i++) {
        Resources.addTo(usedResources, containerSpec.resource);
        MockContainer mockContainer = new MockContainer(containerSpec, containerId, mockApp);
        FiCaSchedulerNode schedulerNode =
            nodeIdToSchedulerNodes.get(containerSpec.nodeId);
        LeafQueue queue = (LeafQueue) nameToCSQueues.get(mockApp.queueName);
        mockApp.addMockContainer(mockContainer, schedulerNode, queue);
        containerId++;
      }
      mockApp.addAggregatedContainerData(containerSpec, usedResources);
      start = end + 1;
    }
  }
}
