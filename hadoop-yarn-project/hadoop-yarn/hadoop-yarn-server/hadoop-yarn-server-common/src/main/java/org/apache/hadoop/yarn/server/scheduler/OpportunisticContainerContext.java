/**
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

package org.apache.hadoop.yarn.server.scheduler;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator.AllocationParams;
import static org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator.ContainerIdGenerator;

/**
 * This encapsulates application specific information used by the
 * Opportunistic Container Allocator to allocate containers.
 */
public class OpportunisticContainerContext {

  private static final Logger LOG = LoggerFactory
      .getLogger(OpportunisticContainerContext.class);

  // Currently just used to keep track of allocated containers.
  // Can be used for reporting stats later.
  private Set<ContainerId> containersAllocated = new HashSet<>();
  private AllocationParams appParams =
      new AllocationParams();
  private ContainerIdGenerator containerIdGenerator =
      new ContainerIdGenerator();

  private volatile List<RemoteNode> nodeList = new LinkedList<>();
  private final Map<String, RemoteNode> nodeMap = new LinkedHashMap<>();

  private final Set<String> blacklist = new HashSet<>();

  // This maintains a map of outstanding OPPORTUNISTIC Reqs. Key-ed by Priority,
  // Resource Name (host/rack/any) and capability. This mapping is required
  // to match a received Container to an outstanding OPPORTUNISTIC
  // ResourceRequest (ask).
  private final TreeMap<Priority, Map<Resource, ResourceRequest>>
      outstandingOpReqs = new TreeMap<>();

  public Set<ContainerId> getContainersAllocated() {
    return containersAllocated;
  }

  public AllocationParams getAppParams() {
    return appParams;
  }

  public ContainerIdGenerator getContainerIdGenerator() {
    return containerIdGenerator;
  }

  public void setContainerIdGenerator(
      ContainerIdGenerator containerIdGenerator) {
    this.containerIdGenerator = containerIdGenerator;
  }

  public Map<String, RemoteNode> getNodeMap() {
    return Collections.unmodifiableMap(nodeMap);
  }

  public synchronized void updateNodeList(List<RemoteNode> newNodeList) {
    // This is an optimization for centralized placement. The
    // OppContainerAllocatorAMService has a cached list of nodes which it sets
    // here. The nodeMap needs to be updated only if the backing node list is
    // modified.
    if (newNodeList != nodeList) {
      nodeList = newNodeList;
      nodeMap.clear();
      for (RemoteNode n : nodeList) {
        nodeMap.put(n.getNodeId().getHost(), n);
      }
    }
  }

  public void updateAllocationParams(Resource minResource, Resource maxResource,
      Resource incrResource, int containerTokenExpiryInterval) {
    appParams.setMinResource(minResource);
    appParams.setMaxResource(maxResource);
    appParams.setIncrementResource(incrResource);
    appParams.setContainerTokenExpiryInterval(containerTokenExpiryInterval);
  }

  public Set<String> getBlacklist() {
    return blacklist;
  }

  public TreeMap<Priority, Map<Resource, ResourceRequest>>
      getOutstandingOpReqs() {
    return outstandingOpReqs;
  }

  public void updateCompletedContainers(AllocateResponse allocateResponse) {
    for (ContainerStatus cs :
        allocateResponse.getCompletedContainersStatuses()) {
      if (cs.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
        containersAllocated.remove(cs.getContainerId());
      }
    }
  }

  /**
   * Takes a list of ResourceRequests (asks), extracts the key information viz.
   * (Priority, ResourceName, Capability) and adds to the outstanding
   * OPPORTUNISTIC outstandingOpReqs map. The nested map is required to enforce
   * the current YARN constraint that only a single ResourceRequest can exist at
   * a give Priority and Capability.
   *
   * @param resourceAsks the list with the {@link ResourceRequest}s
   */
  public void addToOutstandingReqs(List<ResourceRequest> resourceAsks) {
    for (ResourceRequest request : resourceAsks) {
      Priority priority = request.getPriority();

      // TODO: Extend for Node/Rack locality. We only handle ANY requests now
      if (!ResourceRequest.isAnyLocation(request.getResourceName())) {
        continue;
      }

      if (request.getNumContainers() == 0) {
        continue;
      }

      Map<Resource, ResourceRequest> reqMap =
          outstandingOpReqs.get(priority);
      if (reqMap == null) {
        reqMap = new HashMap<>();
        outstandingOpReqs.put(priority, reqMap);
      }

      ResourceRequest resourceRequest = reqMap.get(request.getCapability());
      if (resourceRequest == null) {
        resourceRequest = request;
        reqMap.put(request.getCapability(), request);
      } else {
        resourceRequest.setNumContainers(
            resourceRequest.getNumContainers() + request.getNumContainers());
      }
      if (ResourceRequest.isAnyLocation(request.getResourceName())) {
        LOG.info("# of outstandingOpReqs in ANY (at priority = " + priority
            + ", with capability = " + request.getCapability() + " ) : "
            + resourceRequest.getNumContainers());
      }
    }
  }

  /**
   * This method matches a returned list of Container Allocations to any
   * outstanding OPPORTUNISTIC ResourceRequest.
   * @param capability Capability
   * @param allocatedContainers Allocated Containers
   */
  public void matchAllocationToOutstandingRequest(Resource capability,
      List<Container> allocatedContainers) {
    for (Container c : allocatedContainers) {
      containersAllocated.add(c.getId());
      Map<Resource, ResourceRequest> asks =
          outstandingOpReqs.get(c.getPriority());

      if (asks == null) {
        continue;
      }

      ResourceRequest rr = asks.get(capability);
      if (rr != null) {
        rr.setNumContainers(rr.getNumContainers() - 1);
        if (rr.getNumContainers() == 0) {
          asks.remove(capability);
        }
      }
    }
  }
}
