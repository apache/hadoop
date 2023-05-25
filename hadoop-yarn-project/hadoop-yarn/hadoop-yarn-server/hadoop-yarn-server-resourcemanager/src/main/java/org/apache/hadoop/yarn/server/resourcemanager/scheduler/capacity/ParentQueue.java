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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getACLsForFlexibleAutoCreatedParentQueue;

@Private
@Evolving
public class ParentQueue extends AbstractParentQueue {

  private static final Logger LOG =
      LoggerFactory.getLogger(ParentQueue.class);

  public ParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(queueContext, queueName, parent, old, false);
  }

  public ParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old, boolean isDynamic)
      throws IOException {
    super(queueContext, queueName, parent, old, isDynamic);
    super.setupQueueConfigs(queueContext.getClusterResource());
  }

  public ParentQueue addDynamicParentQueue(String queuePath)
      throws SchedulerDynamicEditException {
    return (ParentQueue) addDynamicChildQueue(queuePath, false);
  }

  public LeafQueue addDynamicLeafQueue(String queuePath)
      throws SchedulerDynamicEditException {
    return (LeafQueue) addDynamicChildQueue(queuePath, true);
  }

  // New method to add child queue
  private CSQueue addDynamicChildQueue(String childQueuePath, boolean isLeaf)
      throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      // Check if queue exists, if queue exists, write a warning message (this
      // should not happen, since it will be handled before calling this method)
      // , but we will move on.
      CSQueue queue =
          queueContext.getQueueManager().getQueueByFullName(
              childQueuePath);
      if (queue != null) {
        LOG.warn(
            "This should not happen, trying to create queue=" + childQueuePath
                + ", however the queue already exists");
        return queue;
      }

      // Check if the max queue limit is exceeded.
      int maxQueues = queueContext.getConfiguration().
          getAutoCreatedQueuesV2MaxChildQueuesLimit(getQueuePath());
      if (childQueues.size() >= maxQueues) {
        throw new SchedulerDynamicEditException(
            "Cannot auto create queue " + childQueuePath + ". Max Child "
                + "Queue limit exceeded which is configured as: " + maxQueues
                + " and number of child queues is: " + childQueues.size());
      }

      // First, check if we allow creation or not
      boolean weightsAreUsed = false;
      try {
        weightsAreUsed = getCapacityConfigurationTypeForQueues(childQueues)
            == QueueCapacityType.WEIGHT;
      } catch (IOException e) {
        LOG.warn("Caught Exception during auto queue creation", e);
      }
      if (!weightsAreUsed && queueContext.getConfiguration().isLegacyQueueMode()) {
        throw new SchedulerDynamicEditException(
            "Trying to create new queue=" + childQueuePath
                + " but not all the queues under parent=" + this.getQueuePath()
                + " are using weight-based capacity. Failed to created queue");
      }

      CSQueue newQueue = createNewQueue(childQueuePath, isLeaf);
      this.childQueues.add(newQueue);
      updateLastSubmittedTimeStamp();

      // Call updateClusterResource.
      // Which will deal with all effectiveMin/MaxResource
      // Calculation
      this.updateClusterResource(queueContext.getClusterResource(),
          new ResourceLimits(queueContext.getClusterResource()));

      return newQueue;
    } finally {
      writeLock.unlock();
    }
  }
}
