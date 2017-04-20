/*
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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.common.tools.SliderUtils;

/**
 * Provides allocation services to a cluster -both random and placed.
 *
 * Important: container allocations need an app attempt ID put into the
 * container ID
 */
public class Allocator {

  private final MockYarnCluster cluster;
  /**
   * Rolling index into the cluster used for the next "random" assignment.
   */
  private int rollingIndex = 0;

  Allocator(MockYarnCluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Allocate a node using the list of nodes in the container as the
   * hints.
   * @param request request
   * @return the allocated container -or null for none
   */
  MockContainer allocate(AMRMClient.ContainerRequest request) {
    MockYarnCluster.MockYarnClusterNode node = null;
    MockYarnCluster.MockYarnClusterContainer allocated = null;
    if (SliderUtils.isNotEmpty(request.getNodes())) {
      for (String host : request.getNodes()) {
        node = cluster.lookup(host);
        allocated = node.allocate();
        if (allocated != null) {
          break;
        }
      }
    }

    if (allocated != null) {
      return createContainerRecord(request, allocated, node);
    } else {
      if (request.getRelaxLocality() || request.getNodes().isEmpty()) {
        // fallback to anywhere
        return allocateRandom(request);
      } else {
        //no match and locality can't be requested
        return null;
      }
    }
  }

  /**
   * Allocate a node without any positioning -use whatever policy this allocator
   * chooses.
   * @param request request
   * @return the allocated container -or null for none
   */
  MockContainer allocateRandom(AMRMClient.ContainerRequest request) {
    int start = rollingIndex;
    MockYarnCluster.MockYarnClusterNode node = cluster.nodeAt(rollingIndex);
    MockYarnCluster.MockYarnClusterContainer allocated = node.allocate();
    // if there is no space, try again -but stop when all the nodes
    // have failed
    while (allocated == null && start != nextIndex()) {
      node = cluster.nodeAt(rollingIndex);
      allocated = node.allocate();
    }

    //here the allocation is set, so create the response
    return createContainerRecord(request, allocated, node);
  }

  /**
   * Create a container record -if one was allocated.
   * @param allocated allocation -may be null
   * @param node node with the container
   * @return a container record, or null if there was no allocation
   */
  public MockContainer createContainerRecord(
      AMRMClient.ContainerRequest request,
      MockYarnCluster.MockYarnClusterContainer allocated,
      MockYarnCluster.MockYarnClusterNode node) {
    if (allocated == null) {
      // no space
      return null;
    }
    MockContainer container = new MockContainer();
    container.setId(new MockContainerId(allocated.getCid()));
    container.setNodeId(node.getNodeId());
    container.setNodeHttpAddress(node.httpAddress());
    container.setPriority(request.getPriority());
    container.setResource(request.getCapability());
    return container;
  }

  public int nextIndex() {
    rollingIndex = (rollingIndex + 1) % cluster.getClusterSize();
    return rollingIndex;
  }

}
