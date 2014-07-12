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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerRecoverEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

@SuppressWarnings("unchecked")
public abstract class AbstractYarnScheduler
    <T extends SchedulerApplicationAttempt, N extends SchedulerNode>
    extends AbstractService implements ResourceScheduler {

  private static final Log LOG = LogFactory.getLog(AbstractYarnScheduler.class);

  // Nodes in the cluster, indexed by NodeId
  protected Map<NodeId, N> nodes = new ConcurrentHashMap<NodeId, N>();

  // Whole capacity of the cluster
  protected Resource clusterResource = Resource.newInstance(0, 0);

  protected Resource minimumAllocation;
  protected Resource maximumAllocation;

  protected RMContext rmContext;
  protected Map<ApplicationId, SchedulerApplication<T>> applications;

  protected final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
    EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  /**
   * Construct the service.
   *
   * @param name service name
   */
  public AbstractYarnScheduler(String name) {
    super(name);
  }

  public synchronized List<Container> getTransferredContainers(
      ApplicationAttemptId currentAttempt) {
    ApplicationId appId = currentAttempt.getApplicationId();
    SchedulerApplication<T> app = applications.get(appId);
    List<Container> containerList = new ArrayList<Container>();
    RMApp appImpl = this.rmContext.getRMApps().get(appId);
    if (appImpl.getApplicationSubmissionContext().getUnmanagedAM()) {
      return containerList;
    }
    Collection<RMContainer> liveContainers =
        app.getCurrentAppAttempt().getLiveContainers();
    ContainerId amContainerId =
        rmContext.getRMApps().get(appId).getCurrentAppAttempt()
          .getMasterContainer().getId();
    for (RMContainer rmContainer : liveContainers) {
      if (!rmContainer.getContainerId().equals(amContainerId)) {
        containerList.add(rmContainer.getContainer());
      }
    }
    return containerList;
  }

  public Map<ApplicationId, SchedulerApplication<T>>
      getSchedulerApplications() {
    return applications;
  }

  @Override
  public Resource getClusterResource() {
    return clusterResource;
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return maximumAllocation;
  }

  public T getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
    SchedulerApplication<T> app =
        applications.get(applicationAttemptId.getApplicationId());
    return app == null ? null : app.getCurrentAppAttempt();
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId appAttemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
    if (attempt == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request for appInfo of unknown attempt " + appAttemptId);
      }
      return null;
    }
    return new SchedulerAppReport(attempt);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
    if (attempt == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request for appInfo of unknown attempt " + appAttemptId);
      }
      return null;
    }
    return attempt.getResourceUsageReport();
  }

  public T getCurrentAttemptForContainer(ContainerId containerId) {
    return getApplicationAttempt(containerId.getApplicationAttemptId());
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    SchedulerApplicationAttempt attempt =
        getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    N node = nodes.get(nodeId);
    return node == null ? null : new SchedulerNodeReport(node);
  }

  @Override
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support moving apps between queues");
  }

  private void killOrphanContainerOnNode(RMNode node,
      NMContainerStatus container) {
    if (!container.getContainerState().equals(ContainerState.COMPLETE)) {
      this.rmContext.getDispatcher().getEventHandler().handle(
        new RMNodeCleanContainerEvent(node.getNodeID(),
          container.getContainerId()));
    }
  }

  public synchronized void recoverContainersOnNode(
      List<NMContainerStatus> containerReports, RMNode nm) {
    if (!rmContext.isWorkPreservingRecoveryEnabled()
        || containerReports == null
        || (containerReports != null && containerReports.isEmpty())) {
      return;
    }

    for (NMContainerStatus container : containerReports) {
      ApplicationId appId =
          container.getContainerId().getApplicationAttemptId().getApplicationId();
      RMApp rmApp = rmContext.getRMApps().get(appId);
      if (rmApp == null) {
        LOG.error("Skip recovering container " + container
            + " for unknown application.");
        killOrphanContainerOnNode(nm, container);
        continue;
      }

      // Unmanaged AM recovery is addressed in YARN-1815
      if (rmApp.getApplicationSubmissionContext().getUnmanagedAM()) {
        LOG.info("Skip recovering container " + container + " for unmanaged AM."
            + rmApp.getApplicationId());
        killOrphanContainerOnNode(nm, container);
        continue;
      }

      SchedulerApplication<T> schedulerApp = applications.get(appId);
      if (schedulerApp == null) {
        LOG.info("Skip recovering container  " + container
            + " for unknown SchedulerApplication. Application current state is "
            + rmApp.getState());
        killOrphanContainerOnNode(nm, container);
        continue;
      }

      LOG.info("Recovering container " + container);
      SchedulerApplicationAttempt schedulerAttempt =
          schedulerApp.getCurrentAppAttempt();

      // create container
      RMContainer rmContainer = recoverAndCreateContainer(container, nm);

      // recover RMContainer
      rmContainer.handle(new RMContainerRecoverEvent(container.getContainerId(),
        container));

      // recover scheduler node
      nodes.get(nm.getNodeID()).recoverContainer(rmContainer);

      // recover queue: update headroom etc.
      Queue queue = schedulerAttempt.getQueue();
      queue.recoverContainer(clusterResource, schedulerAttempt, rmContainer);

      // recover scheduler attempt
      schedulerAttempt.recoverContainer(rmContainer);
            
      // set master container for the current running AMContainer for this
      // attempt.
      RMAppAttempt appAttempt = rmApp.getCurrentAppAttempt();
      if (appAttempt != null) {
        Container masterContainer = appAttempt.getMasterContainer();

        // Mark current running AMContainer's RMContainer based on the master
        // container ID stored in AppAttempt.
        if (masterContainer != null
            && masterContainer.getId().equals(rmContainer.getContainerId())) {
          ((RMContainerImpl)rmContainer).setAMContainer(true);
        }
      }
    }
  }

  private RMContainer recoverAndCreateContainer(NMContainerStatus status,
      RMNode node) {
    Container container =
        Container.newInstance(status.getContainerId(), node.getNodeID(),
          node.getHttpAddress(), status.getAllocatedResource(),
          status.getPriority(), null);
    ApplicationAttemptId attemptId =
        container.getId().getApplicationAttemptId();
    RMContainer rmContainer =
        new RMContainerImpl(container, attemptId, node.getNodeID(),
          applications.get(attemptId.getApplicationId()).getUser(), rmContext,
          status.getCreationTime());
    return rmContainer;
  }

  public SchedulerNode getSchedulerNode(NodeId nodeId) {
    return nodes.get(nodeId);
  }
}
