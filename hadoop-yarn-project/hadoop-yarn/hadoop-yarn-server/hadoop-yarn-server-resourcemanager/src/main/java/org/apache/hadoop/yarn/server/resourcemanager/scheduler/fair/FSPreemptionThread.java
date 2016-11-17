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
          List<RMContainer> containers =
              identifyContainersToPreempt(starvedApp);
          if (containers != null) {
            preemptContainers(containers);
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
  private List<RMContainer> identifyContainersToPreempt(
      FSAppAttempt starvedApp) {
    List<RMContainer> containers = new ArrayList<>(); // return value

    // Find the nodes that match the next resource request
    ResourceRequest request = starvedApp.getNextResourceRequest();
    // TODO (KK): Should we check other resource requests if we can't match
    // the first one?

    Resource requestCapability = request.getCapability();
    List<FSSchedulerNode> potentialNodes =
        scheduler.getNodeTracker().getNodesByResourceName(
            request.getResourceName());

    // From the potential nodes, pick a node that has enough containers
    // from apps over their fairshare
    for (FSSchedulerNode node : potentialNodes) {
      // Reset containers for the new node being considered.
      containers.clear();

      // TODO (YARN-5829): Attempt to reserve the node for starved app. The
      // subsequent if-check needs to be reworked accordingly.
      FSAppAttempt nodeReservedApp = node.getReservedAppSchedulable();
      if (nodeReservedApp != null && !nodeReservedApp.equals(starvedApp)) {
        // This node is already reserved by another app. Let us not consider
        // this for preemption.
        continue;
      }

      // Figure out list of containers to consider
      List<RMContainer> containersToCheck =
          node.getCopiedListOfRunningContainers();
      containersToCheck.removeAll(node.getContainersForPreemption());

      // Initialize potential with unallocated resources
      Resource potential = Resources.clone(node.getUnallocatedResource());
      for (RMContainer container : containersToCheck) {
        FSAppAttempt app =
            scheduler.getSchedulerApp(container.getApplicationAttemptId());

        if (app.canContainerBePreempted(container)) {
          // Flag container for preemption
          containers.add(container);
          Resources.addTo(potential, container.getAllocatedResource());
        }

        // Check if we have already identified enough containers
        if (Resources.fitsIn(requestCapability, potential)) {
          // Mark the containers as being considered for preemption on the node.
          // Make sure the containers are subsequently removed by calling
          // FSSchedulerNode#removeContainerForPreemption.
          node.addContainersForPreemption(containers);
          return containers;
        } else {
          // TODO (YARN-5829): Unreserve the node for the starved app.
        }
      }
    }
    return null;
  }

  private void preemptContainers(List<RMContainer> containers) {
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
}
