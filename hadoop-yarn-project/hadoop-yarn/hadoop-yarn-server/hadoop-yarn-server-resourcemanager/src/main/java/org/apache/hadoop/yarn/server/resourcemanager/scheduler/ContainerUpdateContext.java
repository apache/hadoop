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
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

  private final Set<ContainerId> outstandingDecreases = new HashSet<>();
  private final AppSchedulingInfo appSchedulingInfo;

  ContainerUpdateContext(AppSchedulingInfo appSchedulingInfo) {
    this.appSchedulingInfo = appSchedulingInfo;
  }

  private synchronized boolean isBeingIncreased(Container container) {
    Map<Resource, Map<NodeId, Set<ContainerId>>> resourceMap =
        outstandingIncreases.get(
            new SchedulerRequestKey(container.getPriority(),
                container.getAllocationRequestId(), container.getId()));
    if (resourceMap != null) {
      Map<NodeId, Set<ContainerId>> locationMap =
          resourceMap.get(container.getResource());
      if (locationMap != null) {
        Set<ContainerId> containerIds = locationMap.get(container.getNodeId());
        if (containerIds != null && !containerIds.isEmpty()) {
          return containerIds.contains(container.getId());
        }
      }
    }
    return false;
  }

  /**
   * Add the container to outstanding decreases.
   * @param container Container.
   * @return true if updated to outstanding decreases was successful.
   */
  public synchronized boolean checkAndAddToOutstandingDecreases(
      Container container) {
    if (isBeingIncreased(container)
        || outstandingDecreases.contains(container.getId())) {
      return false;
    }
    outstandingDecreases.add(container.getId());
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
    }
    Map<NodeId, Set<ContainerId>> locationMap =
        resourceMap.get(container.getResource());
    if (locationMap == null) {
      locationMap = new HashMap<>();
      resourceMap.put(container.getResource(), locationMap);
    }
    Set<ContainerId> containerIds = locationMap.get(container.getNodeId());
    if (containerIds == null) {
      containerIds = new HashSet<>();
      locationMap.put(container.getNodeId(), containerIds);
    }
    if (containerIds.contains(container.getId())
        || outstandingDecreases.contains(container.getId())) {
      return false;
    }
    containerIds.add(container.getId());

    Map<SchedulerRequestKey, Map<String, ResourceRequest>> updateResReqs =
        new HashMap<>();
    Resource resToIncrease = getResourceToIncrease(updateRequest, rmContainer);
    Map<String, ResourceRequest> resMap =
        createResourceRequests(rmContainer, schedulerNode,
            schedulerKey, resToIncrease);
    updateResReqs.put(schedulerKey, resMap);
    appSchedulingInfo.addToPlacementSets(false, updateResReqs);
    return true;
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
    // TODO: Fix this for container increase..
    //       This has to equal the Resources in excess of fitsIn()
    //       for container increase and is equal to the container total
    //       resource for Promotion.
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
      appSchedulingInfo.addToPlacementSets(true, reqsToUpdate);
      return UNDEFINED;
    }
    return retVal;
  }
}
