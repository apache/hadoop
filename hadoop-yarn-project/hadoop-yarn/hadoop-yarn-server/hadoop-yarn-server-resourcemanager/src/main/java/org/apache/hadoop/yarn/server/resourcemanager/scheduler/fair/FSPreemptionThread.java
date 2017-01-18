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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SchedulingPlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Thread that handles FairScheduler preemption.
 */
class FSPreemptionThread extends Thread {
  private static final Log LOG = LogFactory.getLog(FSPreemptionThread.class);
  protected final FSContext context;
  private final FairScheduler scheduler;
  private final long warnTimeBeforeKill;
  private final Timer preemptionTimer;

  FSPreemptionThread(FairScheduler scheduler) {
    this.scheduler = scheduler;
    this.context = scheduler.getContext();
    FairSchedulerConfiguration fsConf = scheduler.getConf();
    context.setPreemptionEnabled();
    context.setPreemptionUtilizationThreshold(
        fsConf.getPreemptionUtilizationThreshold());
    warnTimeBeforeKill = fsConf.getWaitTimeBeforeKill();
    preemptionTimer = new Timer("Preemption Timer", true);

    setDaemon(true);
    setName("FSPreemptionThread");
  }

  public void run() {
    while (!Thread.interrupted()) {
      FSAppAttempt starvedApp;
      try{
        starvedApp = context.getStarvedApps().take();
        if (!Resources.isNone(starvedApp.getStarvation())) {
          PreemptableContainers containers =
              identifyContainersToPreempt(starvedApp);
          if (containers != null) {
            preemptContainers(containers.containers);
          }
        }
      } catch (InterruptedException e) {
        LOG.info("Preemption thread interrupted! Exiting.");
        return;
      }
    }
  }

  /**
   * Given an app, identify containers to preempt to satisfy the app's next
   * resource request.
   *
   * @param starvedApp starved application for which we are identifying
   *                   preemption targets
   * @return list of containers to preempt to satisfy starvedApp, null if the
   * app cannot be satisfied by preempting any running containers
   */
  private PreemptableContainers identifyContainersToPreempt(
      FSAppAttempt starvedApp) {
    PreemptableContainers bestContainers = null;

    // Find the nodes that match the next resource request
    SchedulingPlacementSet nextPs =
        starvedApp.getAppSchedulingInfo().getFirstSchedulingPlacementSet();
    PendingAsk firstPendingAsk = nextPs.getPendingAsk(ResourceRequest.ANY);
    // TODO (KK): Should we check other resource requests if we can't match
    // the first one?

    Resource requestCapability = firstPendingAsk.getPerAllocationResource();

    List<FSSchedulerNode> potentialNodes =
        scheduler.getNodeTracker().getNodesByResourceName(
            nextPs.getAcceptedResouceNames().next().toString());

    // From the potential nodes, pick a node that has enough containers
    // from apps over their fairshare
    for (FSSchedulerNode node : potentialNodes) {
      // TODO (YARN-5829): Attempt to reserve the node for starved app. The
      // subsequent if-check needs to be reworked accordingly.
      FSAppAttempt nodeReservedApp = node.getReservedAppSchedulable();
      if (nodeReservedApp != null && !nodeReservedApp.equals(starvedApp)) {
        // This node is already reserved by another app. Let us not consider
        // this for preemption.
        continue;
      }

      int maxAMContainers = bestContainers == null ?
          Integer.MAX_VALUE : bestContainers.numAMContainers;
      PreemptableContainers preemptableContainers =
          identifyContainersToPreemptOnNode(requestCapability, node,
              maxAMContainers);
      if (preemptableContainers != null) {
        if (preemptableContainers.numAMContainers == 0) {
          return preemptableContainers;
        } else {
          bestContainers = preemptableContainers;
        }
      }
    }

    return bestContainers;
  }

  /**
   * Identify containers to preempt on a given node. Try to find a list with
   * least AM containers to avoid preempt AM containers. This method returns a
   * non-null set of containers only if the number of AM containers is less
   * than maxAMContainers.
   *
   * @param request resource requested
   * @param node the node to check
   * @param maxAMContainers max allowed AM containers in the set
   * @return list of preemptable containers with fewer AM containers than
   *         maxAMContainers if such a list exists; null otherwise.
   */
  private PreemptableContainers identifyContainersToPreemptOnNode(
      Resource request, FSSchedulerNode node, int maxAMContainers) {
    PreemptableContainers preemptableContainers =
        new PreemptableContainers(maxAMContainers);

    // Figure out list of containers to consider
    List<RMContainer> containersToCheck =
        node.getRunningContainersWithAMsAtTheEnd();
    containersToCheck.removeAll(node.getContainersForPreemption());

    // Initialize potential with unallocated resources
    Resource potential = Resources.clone(node.getUnallocatedResource());

    for (RMContainer container : containersToCheck) {
      FSAppAttempt app =
          scheduler.getSchedulerApp(container.getApplicationAttemptId());

      if (app.canContainerBePreempted(container)) {
        // Flag container for preemption
        if (!preemptableContainers.addContainer(container)) {
          return null;
        }

        Resources.addTo(potential, container.getAllocatedResource());
      }

      // Check if we have already identified enough containers
      if (Resources.fitsIn(request, potential)) {
        return preemptableContainers;
      } else {
        // TODO (YARN-5829): Unreserve the node for the starved app.
      }
    }
    return null;
  }

  private void preemptContainers(List<RMContainer> containers) {
    // Mark the containers as being considered for preemption on the node.
    // Make sure the containers are subsequently removed by calling
    // FSSchedulerNode#removeContainerForPreemption.
    if (containers.size() > 0) {
      FSSchedulerNode node = (FSSchedulerNode) scheduler.getNodeTracker()
          .getNode(containers.get(0).getNodeId());
      node.addContainersForPreemption(containers);
    }

    // Warn application about containers to be killed
    for (RMContainer container : containers) {
      ApplicationAttemptId appAttemptId = container.getApplicationAttemptId();
      FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);
      FSLeafQueue queue = app.getQueue();
      LOG.info("Preempting container " + container +
          " from queue " + queue.getName());
      app.trackContainerForPreemption(container);
    }

    // Schedule timer task to kill containers
    preemptionTimer.schedule(
        new PreemptContainersTask(containers), warnTimeBeforeKill);
  }

  private class PreemptContainersTask extends TimerTask {
    private List<RMContainer> containers;

    PreemptContainersTask(List<RMContainer> containers) {
      this.containers = containers;
    }

    @Override
    public void run() {
      for (RMContainer container : containers) {
        ContainerStatus status = SchedulerUtils.createPreemptedContainerStatus(
            container.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER);

        LOG.info("Killing container " + container);
        scheduler.completedContainer(
            container, status, RMContainerEventType.KILL);

        FSSchedulerNode containerNode = (FSSchedulerNode)
            scheduler.getNodeTracker().getNode(container.getAllocatedNode());
        containerNode.removeContainerForPreemption(container);
      }
    }
  }

  /**
   * A class to track preemptable containers.
   */
  private static class PreemptableContainers {
    List<RMContainer> containers;
    int numAMContainers;
    int maxAMContainers;

    PreemptableContainers(int maxAMContainers) {
      containers = new ArrayList<>();
      numAMContainers = 0;
      this.maxAMContainers = maxAMContainers;
    }

    /**
     * Add a container if the number of AM containers is less than
     * maxAMContainers.
     *
     * @param container the container to add
     * @return true if success; false otherwise
     */
    private boolean addContainer(RMContainer container) {
      if (container.isAMContainer()) {
        numAMContainers++;
        if (numAMContainers >= maxAMContainers) {
          return false;
        }
      }

      containers.add(container);
      return true;
    }
  }
}
