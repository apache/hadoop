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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer
    .RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Class encapsulates all outstanding container increase and decrease
 * requests for an application.
 */
public class ContainerUpdateContext {

  public static final ContainerId UNDEFINED =
      ContainerId.newContainerId(ApplicationAttemptId.newInstance(
              ApplicationId.newInstance(-1, -1), -1), -1);
  protected static final RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  // Keep track of containers that are undergoing promotion
  private final Map<SchedulerRequestKey, Map<Resource,
      Map<NodeId, Set<ContainerId>>>> outstandingIncreases = new HashMap<>();

  private final Map<ContainerId, Resource> outstandingDecreases =
      new HashMap<>();
  private final AppSchedulingInfo appSchedulingInfo;

  ContainerUpdateContext(AppSchedulingInfo appSchedulingInfo) {
    this.appSchedulingInfo = appSchedulingInfo;
  }

  /**
   * Add the container to outstanding decreases.
   * @param updateReq UpdateContainerRequest.
   * @param schedulerNode SchedulerNode.
   * @param container Container.
   * @return If it was possible to decrease the container.
   */
  public synchronized boolean checkAndAddToOutstandingDecreases(
      UpdateContainerRequest updateReq, SchedulerNode schedulerNode,
      Container container) {
    if (outstandingDecreases.containsKey(container.getId())) {
      return false;
    }
    if (ContainerUpdateType.DECREASE_RESOURCE ==
        updateReq.getContainerUpdateType()) {
      SchedulerRequestKey updateKey = new SchedulerRequestKey
          (container.getPriority(),
              container.getAllocationRequestId(), container.getId());
      cancelPreviousRequest(schedulerNode, updateKey);
      outstandingDecreases.put(container.getId(), updateReq.getCapability());
    } else {
      outstandingDecreases.put(container.getId(), container.getResource());
    }
    return true;
  }

  /**
   * Add the container to outstanding increases.
   * @param rmContainer RMContainer.
   * @param schedulerNode SchedulerNode.
   * @param updateRequest UpdateContainerRequest.
   * @return true if updated to outstanding increases was successful.
   */
  public synchronized boolean checkAndAddToOutstandingIncreases(
      RMContainer rmContainer, SchedulerNode schedulerNode,
      UpdateContainerRequest updateRequest) {
    Container container = rmContainer.getContainer();
    SchedulerRequestKey schedulerKey =
        SchedulerRequestKey.create(updateRequest,
            rmContainer.getAllocatedSchedulerKey());
    Map<Resource, Map<NodeId, Set<ContainerId>>> resourceMap =
        outstandingIncreases.get(schedulerKey);
    if (resourceMap == null) {
      resourceMap = new HashMap<>();
      outstandingIncreases.put(schedulerKey, resourceMap);
    } else {
      // Updating Resource for and existing increase container
      if (ContainerUpdateType.INCREASE_RESOURCE ==
          updateRequest.getContainerUpdateType()) {
        cancelPreviousRequest(schedulerNode, schedulerKey);
      } else {
        return false;
      }
    }
    Resource resToIncrease = getResourceToIncrease(updateRequest, rmContainer);
    Map<NodeId, Set<ContainerId>> locationMap =
        resourceMap.get(resToIncrease);
    if (locationMap == null) {
      locationMap = new HashMap<>();
      resourceMap.put(resToIncrease, locationMap);
    }
    Set<ContainerId> containerIds = locationMap.get(container.getNodeId());
    if (containerIds == null) {
      containerIds = new HashSet<>();
      locationMap.put(container.getNodeId(), containerIds);
    }
    if (outstandingDecreases.containsKey(container.getId())) {
      return false;
    }

    containerIds.add(container.getId());
    if (!Resources.isNone(resToIncrease)) {
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> updateResReqs =
          new HashMap<>();
      Map<String, ResourceRequest> resMap =
          createResourceRequests(rmContainer, schedulerNode,
              schedulerKey, resToIncrease);
      updateResReqs.put(schedulerKey, resMap);
      appSchedulingInfo.updateResourceRequests(updateResReqs, false);
    }
    return true;
  }

  private void cancelPreviousRequest(SchedulerNode schedulerNode,
      SchedulerRequestKey schedulerKey) {
    AppPlacementAllocator<SchedulerNode> appPlacementAllocator =
        appSchedulingInfo.getAppPlacementAllocator(schedulerKey);
    if (appPlacementAllocator != null) {
      PendingAsk pendingAsk = appPlacementAllocator.getPendingAsk(
          ResourceRequest.ANY);
      // Decrement the pending using a dummy RR with
      // resource = prev update req capability
      if (pendingAsk != null && pendingAsk.getCount() > 0) {
        appSchedulingInfo.allocate(NodeType.OFF_SWITCH, schedulerNode,
            schedulerKey, Container.newInstance(UNDEFINED,
                schedulerNode.getNodeID(), "host:port",
                pendingAsk.getPerAllocationResource(),
                schedulerKey.getPriority(), null));
      }
    }
  }

  private Map<String, ResourceRequest> createResourceRequests(
      RMContainer rmContainer, SchedulerNode schedulerNode,
      SchedulerRequestKey schedulerKey, Resource resToIncrease) {
    Map<String, ResourceRequest> resMap = new HashMap<>();
    resMap.put(rmContainer.getContainer().getNodeId().getHost(),
        createResourceReqForIncrease(schedulerKey, resToIncrease,
            RECORD_FACTORY.newRecordInstance(ResourceRequest.class),
            rmContainer, rmContainer.getContainer().getNodeId().getHost()));
    resMap.put(schedulerNode.getRackName(),
        createResourceReqForIncrease(schedulerKey, resToIncrease,
            RECORD_FACTORY.newRecordInstance(ResourceRequest.class),
            rmContainer, schedulerNode.getRackName()));
    resMap.put(ResourceRequest.ANY,
        createResourceReqForIncrease(schedulerKey, resToIncrease,
            RECORD_FACTORY.newRecordInstance(ResourceRequest.class),
            rmContainer, ResourceRequest.ANY));
    return resMap;
  }

  private Resource getResourceToIncrease(UpdateContainerRequest updateReq,
      RMContainer rmContainer) {
    if (updateReq.getContainerUpdateType() ==
        ContainerUpdateType.PROMOTE_EXECUTION_TYPE) {
      return rmContainer.getContainer().getResource();
    }
    if (updateReq.getContainerUpdateType() ==
        ContainerUpdateType.INCREASE_RESOURCE) {
      //       This has to equal the Resources in excess of fitsIn()
      //       for container increase and is equal to the container total
      //       resource for Promotion.
      Resource maxCap = Resources.componentwiseMax(updateReq.getCapability(),
          rmContainer.getContainer().getResource());
      return Resources.add(maxCap,
          Resources.negate(rmContainer.getContainer().getResource()));
    }
    return null;
  }

  private static ResourceRequest createResourceReqForIncrease(
      SchedulerRequestKey schedulerRequestKey, Resource resToIncrease,
      ResourceRequest rr, RMContainer rmContainer, String resourceName) {
    rr.setResourceName(resourceName);
    rr.setNumContainers(1);
    rr.setRelaxLocality(false);
    rr.setPriority(rmContainer.getContainer().getPriority());
    rr.setAllocationRequestId(schedulerRequestKey.getAllocationRequestId());
    rr.setCapability(resToIncrease);
    rr.setNodeLabelExpression(rmContainer.getNodeLabelExpression());
    rr.setExecutionTypeRequest(ExecutionTypeRequest.newInstance(
        ExecutionType.GUARANTEED, true));
    return rr;
  }

  /**
   * Remove Container from outstanding increases / decreases. Calling this
   * method essentially completes the update process.
   * @param schedulerKey SchedulerRequestKey.
   * @param container Container.
   */
  public synchronized void removeFromOutstandingUpdate(
      SchedulerRequestKey schedulerKey, Container container) {
    Map<Resource, Map<NodeId, Set<ContainerId>>> resourceMap =
        outstandingIncreases.get(schedulerKey);
    if (resourceMap != null) {
      Map<NodeId, Set<ContainerId>> locationMap =
          resourceMap.get(container.getResource());
      if (locationMap != null) {
        Set<ContainerId> containerIds = locationMap.get(container.getNodeId());
        if (containerIds != null && !containerIds.isEmpty()) {
          containerIds.remove(container.getId());
          if (containerIds.isEmpty()) {
            locationMap.remove(container.getNodeId());
          }
        }
        if (locationMap.isEmpty()) {
          resourceMap.remove(container.getResource());
        }
      }
      if (resourceMap.isEmpty()) {
        outstandingIncreases.remove(schedulerKey);
      }
    }
    outstandingDecreases.remove(container.getId());
  }

  /**
   * Check if a new container is to be matched up against an outstanding
   * Container increase request.
   * @param node SchedulerNode.
   * @param schedulerKey SchedulerRequestKey.
   * @param rmContainer RMContainer.
   * @return ContainerId.
   */
  public ContainerId matchContainerToOutstandingIncreaseReq(
      SchedulerNode node, SchedulerRequestKey schedulerKey,
      RMContainer rmContainer) {
    ContainerId retVal = null;
    Container container = rmContainer.getContainer();
    Map<Resource, Map<NodeId, Set<ContainerId>>> resourceMap =
        outstandingIncreases.get(schedulerKey);
    if (resourceMap != null) {
      Map<NodeId, Set<ContainerId>> locationMap =
          resourceMap.get(container.getResource());
      if (locationMap != null) {
        Set<ContainerId> containerIds = locationMap.get(container.getNodeId());
        if (containerIds != null && !containerIds.isEmpty()) {
          retVal = containerIds.iterator().next();
        }
      }
    }
    // Allocation happened on NM on the same host, but not on the NM
    // we need.. We need to signal that this container has to be released.
    // We also need to add these requests back.. to be reallocated.
    if (resourceMap != null && retVal == null) {
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> reqsToUpdate =
          new HashMap<>();
      Map<String, ResourceRequest> resMap = createResourceRequests
          (rmContainer, node, schedulerKey,
          rmContainer.getContainer().getResource());
      reqsToUpdate.put(schedulerKey, resMap);
      appSchedulingInfo.updateResourceRequests(reqsToUpdate, true);
      return UNDEFINED;
    }
    return retVal;
  }

  /**
   * Swaps the existing RMContainer's and the temp RMContainers internal
   * container references after adjusting the resources in each.
   * @param tempRMContainer Temp RMContainer.
   * @param existingRMContainer Existing RMContainer.
   * @param updateType Update Type.
   * @return Existing RMContainer after swapping the container references.
   */
  public RMContainer swapContainer(RMContainer tempRMContainer,
      RMContainer existingRMContainer, ContainerUpdateType updateType) {
    ContainerId matchedContainerId = existingRMContainer.getContainerId();
    // Swap updated container with the existing container
    Container tempContainer = tempRMContainer.getContainer();

    Resource updatedResource = createUpdatedResource(
        tempContainer, existingRMContainer.getContainer(), updateType);
    Resource resourceToRelease = createResourceToRelease(
        existingRMContainer.getContainer(), updateType);
    Container newContainer = Container.newInstance(matchedContainerId,
        existingRMContainer.getContainer().getNodeId(),
        existingRMContainer.getContainer().getNodeHttpAddress(),
        updatedResource,
        existingRMContainer.getContainer().getPriority(), null,
        tempContainer.getExecutionType());
    newContainer.setAllocationRequestId(
        existingRMContainer.getContainer().getAllocationRequestId());
    newContainer.setVersion(existingRMContainer.getContainer().getVersion());

    tempRMContainer.getContainer().setResource(resourceToRelease);
    tempRMContainer.getContainer().setExecutionType(
        existingRMContainer.getContainer().getExecutionType());

    ((RMContainerImpl)existingRMContainer).setContainer(newContainer);
    return existingRMContainer;
  }

  /**
   * Returns the resource that the container will finally be assigned with
   * at the end of the update operation.
   * @param tempContainer Temporary Container created for the operation.
   * @param existingContainer Existing Container.
   * @param updateType Update Type.
   * @return Final Resource.
   */
  private Resource createUpdatedResource(Container tempContainer,
      Container existingContainer, ContainerUpdateType updateType) {
    if (ContainerUpdateType.INCREASE_RESOURCE == updateType) {
      return Resources.add(existingContainer.getResource(),
          tempContainer.getResource());
    } else if (ContainerUpdateType.DECREASE_RESOURCE == updateType) {
      return outstandingDecreases.get(existingContainer.getId());
    } else {
      return existingContainer.getResource();
    }
  }

  /**
   * Returns the resources that need to be released at the end of the update
   * operation.
   * @param existingContainer Existing Container.
   * @param updateType Updated type.
   * @return Resources to be released.
   */
  private Resource createResourceToRelease(Container existingContainer,
      ContainerUpdateType updateType) {
    if (ContainerUpdateType.INCREASE_RESOURCE == updateType) {
      return Resources.none();
    } else if (ContainerUpdateType.DECREASE_RESOURCE == updateType){
      return Resources.add(existingContainer.getResource(),
          Resources.negate(
              outstandingDecreases.get(existingContainer.getId())));
    } else {
      return existingContainer.getResource();
    }
  }
}
