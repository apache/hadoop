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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResoureRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ApplicationSchedulingConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is an implementation of the {@link AppPlacementAllocator} that takes
 * into account locality preferences (node, rack, any) when allocating
 * containers.
 */
public class LocalityAppPlacementAllocator <N extends SchedulerNode>
    extends AppPlacementAllocator<N> {
  private static final Logger LOG =
      LoggerFactory.getLogger(LocalityAppPlacementAllocator.class);

  private final Map<String, ResourceRequest> resourceRequestMap =
      new ConcurrentHashMap<>();
  private volatile String primaryRequestedPartition =
      RMNodeLabelsManager.NO_LABEL;
  private MultiNodeSortingManager<N> multiNodeSortingManager = null;
  private String multiNodeSortPolicyName;

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  public LocalityAppPlacementAllocator() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initialize(AppSchedulingInfo appSchedulingInfo,
      SchedulerRequestKey schedulerRequestKey, RMContext rmContext) {
    super.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    multiNodeSortPolicyName = appSchedulingInfo
        .getApplicationSchedulingEnvs().get(
            ApplicationSchedulingConfig.ENV_MULTI_NODE_SORTING_POLICY_CLASS);
    multiNodeSortingManager = (MultiNodeSortingManager<N>) rmContext
        .getMultiNodeSortingManager();
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "nodeLookupPolicy used for " + appSchedulingInfo
              .getApplicationId()
              + " is " + ((multiNodeSortPolicyName != null) ?
              multiNodeSortPolicyName :
              ""));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<N> getPreferredNodeIterator(
      CandidateNodeSet<N> candidateNodeSet) {
    // Now only handle the case that single node in the candidateNodeSet
    // TODO, Add support to multi-hosts inside candidateNodeSet which is passed
    // in.

    N singleNode = CandidateNodeSetUtils.getSingleNode(candidateNodeSet);
    if (singleNode != null) {
      return IteratorUtils.singletonIterator(singleNode);
    }

    // singleNode will be null if Multi-node placement lookup is enabled, and
    // hence could consider sorting policies.
    return multiNodeSortingManager.getMultiNodeSortIterator(
        candidateNodeSet.getAllNodes().values(),
        candidateNodeSet.getPartition(),
        multiNodeSortPolicyName);
  }

  private boolean hasRequestLabelChanged(ResourceRequest requestOne,
      ResourceRequest requestTwo) {
    String requestOneLabelExp = requestOne.getNodeLabelExpression();
    String requestTwoLabelExp = requestTwo.getNodeLabelExpression();
    // First request label expression can be null and second request
    // is not null then we have to consider it as changed.
    if ((null == requestOneLabelExp) && (null != requestTwoLabelExp)) {
      return true;
    }
    // If the label is not matching between both request when
    // requestOneLabelExp is not null.
    return ((null != requestOneLabelExp) && !(requestOneLabelExp
        .equals(requestTwoLabelExp)));
  }

  private void updateNodeLabels(ResourceRequest request) {
    String resourceName = request.getResourceName();
    if (resourceName.equals(ResourceRequest.ANY)) {
      ResourceRequest previousAnyRequest =
          getResourceRequest(resourceName);

      // When there is change in ANY request label expression, we should
      // update label for all resource requests already added of same
      // priority as ANY resource request.
      if ((null == previousAnyRequest) || hasRequestLabelChanged(
          previousAnyRequest, request)) {
        for (ResourceRequest r : resourceRequestMap.values()) {
          if (!r.getResourceName().equals(ResourceRequest.ANY)) {
            r.setNodeLabelExpression(request.getNodeLabelExpression());
          }
        }
      }
    } else{
      ResourceRequest anyRequest = getResourceRequest(ResourceRequest.ANY);
      if (anyRequest != null) {
        request.setNodeLabelExpression(anyRequest.getNodeLabelExpression());
      }
    }
  }

  @Override
  public PendingAskUpdateResult updatePendingAsk(
      Collection<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer) {

    this.writeLock.lock();
    try {
      PendingAskUpdateResult updateResult = null;

      // Update resource requests
      for (ResourceRequest request : requests) {
        String resourceName = request.getResourceName();

        // Update node labels if required
        updateNodeLabels(request);

        // Increment number of containers if recovering preempted resources
        ResourceRequest lastRequest = resourceRequestMap.get(resourceName);
        if (recoverPreemptedRequestForAContainer && lastRequest != null) {
          request.setNumContainers(lastRequest.getNumContainers() + 1);
        }

        // Update asks
        resourceRequestMap.put(resourceName, request);

        if (resourceName.equals(ResourceRequest.ANY)) {
          String partition = request.getNodeLabelExpression() == null ?
              RMNodeLabelsManager.NO_LABEL :
              request.getNodeLabelExpression();

          this.primaryRequestedPartition = partition;

          //update the applications requested labels set
          appSchedulingInfo.addRequestedPartition(partition);

          PendingAsk lastPendingAsk =
              lastRequest == null ? null : new PendingAsk(
                  lastRequest.getCapability(), lastRequest.getNumContainers());
          String lastRequestedNodePartition =
              lastRequest == null ? null : lastRequest.getNodeLabelExpression();

          updateResult = new PendingAskUpdateResult(lastPendingAsk,
              new PendingAsk(request.getCapability(),
                  request.getNumContainers()), lastRequestedNodePartition,
              request.getNodeLabelExpression());
        }
      }
      return updateResult;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public PendingAskUpdateResult updatePendingAsk(
      SchedulerRequestKey schedulerRequestKey,
      SchedulingRequest schedulingRequest,
      boolean recoverPreemptedRequestForAContainer)
      throws SchedulerInvalidResoureRequestException {
    throw new SchedulerInvalidResoureRequestException(this.getClass().getName()
        + " not be able to handle SchedulingRequest, there exists a "
        + "ResourceRequest with the same scheduler key=" + schedulerRequestKey
        + ", please send SchedulingRequest with a different allocationId and "
        + "priority");
  }

  @Override
  public Map<String, ResourceRequest> getResourceRequests() {
    return resourceRequestMap;
  }

  private ResourceRequest getResourceRequest(String resourceName) {
    return resourceRequestMap.get(resourceName);
  }

  @Override
  public PendingAsk getPendingAsk(String resourceName) {
    readLock.lock();
    try {
      ResourceRequest request = getResourceRequest(resourceName);
      if (null == request) {
        return PendingAsk.ZERO;
      } else{
        return new PendingAsk(request.getCapability(),
            request.getNumContainers());
      }
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public int getOutstandingAsksCount(String resourceName) {
    readLock.lock();
    try {
      ResourceRequest request = getResourceRequest(resourceName);
      if (null == request) {
        return 0;
      } else{
        return request.getNumContainers();
      }
    } finally {
      readLock.unlock();
    }

  }

  private void decrementOutstanding(SchedulerRequestKey schedulerRequestKey,
      ResourceRequest offSwitchRequest) {
    int numOffSwitchContainers = offSwitchRequest.getNumContainers() - 1;
    offSwitchRequest.setNumContainers(numOffSwitchContainers);

    // Do we have any outstanding requests?
    // If there is nothing, we need to deactivate this application
    if (numOffSwitchContainers == 0) {
      appSchedulingInfo.getSchedulerKeys().remove(schedulerRequestKey);
      appSchedulingInfo.checkForDeactivation();
      resourceRequestMap.remove(ResourceRequest.ANY);
      if (resourceRequestMap.isEmpty()) {
        appSchedulingInfo.removeAppPlacement(schedulerRequestKey);
      }
    }

    appSchedulingInfo.decPendingResource(
        offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
  }

  public ResourceRequest cloneResourceRequest(ResourceRequest request) {
    ResourceRequest newRequest = ResourceRequest.clone(request);
    newRequest.setNumContainers(1);
    return newRequest;
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateRackLocal(SchedulerRequestKey schedulerKey,
      SchedulerNode node, ResourceRequest rackLocalRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getRackName(), rackLocalRequest);

    ResourceRequest offRackRequest = resourceRequestMap.get(
        ResourceRequest.ANY);
    decrementOutstanding(schedulerKey, offRackRequest);

    // Update cloned RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateOffSwitch(SchedulerRequestKey schedulerKey,
      ResourceRequest offSwitchRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decrementOutstanding(schedulerKey, offSwitchRequest);
    // Update cloned OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(offSwitchRequest));
  }


  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateNodeLocal(SchedulerRequestKey schedulerKey,
      SchedulerNode node, ResourceRequest nodeLocalRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getNodeName(), nodeLocalRequest);

    ResourceRequest rackLocalRequest = resourceRequestMap.get(
        node.getRackName());
    decResourceRequest(node.getRackName(), rackLocalRequest);

    ResourceRequest offRackRequest = resourceRequestMap.get(
        ResourceRequest.ANY);
    decrementOutstanding(schedulerKey, offRackRequest);

    // Update cloned NodeLocal, RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(nodeLocalRequest));
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  private void decResourceRequest(String resourceName,
      ResourceRequest request) {
    request.setNumContainers(request.getNumContainers() - 1);
    if (request.getNumContainers() == 0) {
      resourceRequestMap.remove(resourceName);
    }
  }

  @Override
  public boolean canAllocate(NodeType type, SchedulerNode node) {
    readLock.lock();
    try {
      ResourceRequest r = resourceRequestMap.get(
          ResourceRequest.ANY);
      if (r == null || r.getNumContainers() <= 0) {
        return false;
      }
      if (type == NodeType.RACK_LOCAL || type == NodeType.NODE_LOCAL) {
        r = resourceRequestMap.get(node.getRackName());
        if (r == null || r.getNumContainers() <= 0) {
          return false;
        }
        if (type == NodeType.NODE_LOCAL) {
          r = resourceRequestMap.get(node.getNodeName());
          if (r == null || r.getNumContainers() <= 0) {
            return false;
          }
        }
      }

      return true;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean canDelayTo(String resourceName) {
    readLock.lock();
    try {
      ResourceRequest request = getResourceRequest(resourceName);
      return request == null || request.getRelaxLocality();
    } finally {
      readLock.unlock();
    }

  }


  @Override
  public boolean precheckNode(SchedulerNode schedulerNode,
      SchedulingMode schedulingMode,
      Optional<DiagnosticsCollector> dcOpt) {
    // We will only look at node label = nodeLabelToLookAt according to
    // schedulingMode and partition of node.
    LOG.debug("precheckNode is invoked for {},{}", schedulerNode.getNodeID(),
        schedulingMode);
    String nodePartitionToLookAt;
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      nodePartitionToLookAt = schedulerNode.getPartition();
    } else {
      nodePartitionToLookAt = RMNodeLabelsManager.NO_LABEL;
    }

    boolean rst = primaryRequestedPartition.equals(nodePartitionToLookAt);
    if (!rst && dcOpt.isPresent()) {
      dcOpt.get().collectPartitionDiagnostics(primaryRequestedPartition,
          nodePartitionToLookAt);
    }
    return rst;
  }

  @Override
  public boolean precheckNode(SchedulerNode schedulerNode,
      SchedulingMode schedulingMode) {
    return precheckNode(schedulerNode, schedulingMode, Optional.empty());
  }

  @Override
  public String getPrimaryRequestedNodePartition() {
    return primaryRequestedPartition;
  }

  @Override
  public int getUniqueLocationAsks() {
    return resourceRequestMap.size();
  }

  @Override
  public void showRequests() {
    for (ResourceRequest request : resourceRequestMap.values()) {
      if (request.getNumContainers() > 0) {
        LOG.debug("\tRequest=" + request);
      }
    }
  }

  @Override
  public ContainerRequest allocate(SchedulerRequestKey schedulerKey,
      NodeType type, SchedulerNode node) {
    writeLock.lock();
    try {

      List<ResourceRequest> resourceRequests = new ArrayList<>();

      ResourceRequest request;
      if (type == NodeType.NODE_LOCAL) {
        request = resourceRequestMap.get(node.getNodeName());
      } else if (type == NodeType.RACK_LOCAL) {
        request = resourceRequestMap.get(node.getRackName());
      } else{
        request = resourceRequestMap.get(ResourceRequest.ANY);
      }

      if (type == NodeType.NODE_LOCAL) {
        allocateNodeLocal(schedulerKey, node, request, resourceRequests);
      } else if (type == NodeType.RACK_LOCAL) {
        allocateRackLocal(schedulerKey, node, request, resourceRequests);
      } else{
        allocateOffSwitch(schedulerKey, request, resourceRequests);
      }

      return new ContainerRequest(resourceRequests);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public SchedulingRequest getSchedulingRequest() {
    return null;
  }
}
