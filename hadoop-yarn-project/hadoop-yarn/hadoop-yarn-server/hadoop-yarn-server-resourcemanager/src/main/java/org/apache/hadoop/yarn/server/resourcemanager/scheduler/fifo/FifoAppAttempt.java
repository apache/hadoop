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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;


import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.List;

public class FifoAppAttempt extends FiCaSchedulerApp {
  private static final Log LOG = LogFactory.getLog(FifoAppAttempt.class);

  FifoAppAttempt(ApplicationAttemptId appAttemptId, String user,
      Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    super(appAttemptId, user, queue, activeUsersManager, rmContext);
  }

  public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, Container container) {
    try {
      writeLock.lock();

      if (isStopped) {
        return null;
      }

      // Required sanity check - AM can call 'allocate' to update resource
      // request without locking the scheduler, hence we need to check
      if (getOutstandingAsksCount(schedulerKey) <= 0) {
        return null;
      }

      // Create RMContainer
      RMContainer rmContainer = new RMContainerImpl(container,
          schedulerKey, this.getApplicationAttemptId(), node.getNodeID(),
          appSchedulingInfo.getUser(), this.rmContext, node.getPartition());
      ((RMContainerImpl) rmContainer).setQueueName(this.getQueueName());

      updateAMContainerDiagnostics(AMState.ASSIGNED, null);

      // Add it to allContainers list.
      addToNewlyAllocatedContainers(node, rmContainer);

      ContainerId containerId = container.getId();
      liveContainers.put(containerId, rmContainer);

      // Update consumption and track allocations
      List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
          type, node, schedulerKey, container);

      attemptResourceUsage.incUsed(node.getPartition(),
          container.getResource());

      // Update resource requests related to "request" and store in RMContainer
      ((RMContainerImpl) rmContainer).setResourceRequests(resourceRequestList);

      // Inform the container
      rmContainer.handle(
          new RMContainerEvent(containerId, RMContainerEventType.START));

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: applicationAttemptId=" + containerId
            .getApplicationAttemptId() + " container=" + containerId + " host="
            + container.getNodeId().getHost() + " type=" + type);
      }
      RMAuditLogger.logSuccess(getUser(),
          RMAuditLogger.AuditConstants.ALLOC_CONTAINER, "SchedulerApp",
          getApplicationId(), containerId, container.getResource());

      return rmContainer;
    } finally {
      writeLock.unlock();
    }
  }
}
