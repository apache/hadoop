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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;

/**
 * Thread that handles FairScheduler preemption.
 */
class FSPreemptionThread extends Thread {
  private static final Logger LOG = LoggerFactory.
      getLogger(FSPreemptionThread.class);
  protected final FSContext context;
  private final FairScheduler scheduler;
  private final long warnTimeBeforeKill;
  private final long delayBeforeNextStarvationCheck;
  private final Timer preemptionTimer;
  private final Lock schedulerReadLock;

  @SuppressWarnings("deprecation")
  FSPreemptionThread(FairScheduler scheduler) {
    setDaemon(true);
    setName("FSPreemptionThread");
    this.scheduler = scheduler;
    this.context = scheduler.getContext();
    FairSchedulerConfiguration fsConf = scheduler.getConf();
    context.setPreemptionEnabled();
    context.setPreemptionUtilizationThreshold(
        fsConf.getPreemptionUtilizationThreshold());
    preemptionTimer = new Timer("Preemption Timer", true);

    warnTimeBeforeKill = fsConf.getWaitTimeBeforeKill();
    long allocDelay = (fsConf.isContinuousSchedulingEnabled()
        ? 10 * fsConf.getContinuousSchedulingSleepMs() // 10 runs
        : 4 * scheduler.getNMHeartbeatInterval()); // 4 heartbeats
    delayBeforeNextStarvationCheck = warnTimeBeforeKill + allocDelay +
        fsConf.getWaitTimeBeforeNextStarvationCheck();
    schedulerReadLock = scheduler.getSchedulerReadLock();
  }

  @Override
  public void run() {
    while (!Thread.interrupted()) {
      try {
        FSAppAttempt starvedApp = context.getStarvedApps().take();
        // Hold the scheduler readlock so this is not concurrent with the
        // update thread.
        schedulerReadLock.lock();
        try {
          preemptContainers(identifyContainersToPreempt(starvedApp));
        } finally {
          schedulerReadLock.unlock();
        }
        starvedApp.preemptionTriggered(delayBeforeNextStarvationCheck);
      } catch (InterruptedException e) {
        LOG.info("Preemption thread interrupted! Exiting.");
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Given an app, identify containers to preempt to satisfy the app's
   * starvation.
   *
   * Mechanics:
   * 1. Fetch all {@link ResourceRequest}s corresponding to the amount of
   * starvation.
   * 2. For each {@link ResourceRequest}, get the best preemptable containers.
   *
   * @param starvedApp starved application for which we are identifying
   *                   preemption targets
   * @return list of containers to preempt to satisfy starvedApp
   */
  private List<RMContainer> identifyContainersToPreempt(
      FSAppAttempt starvedApp) {
    List<RMContainer> containersToPreempt = new ArrayList<>();

    // Iterate through enough RRs to address app's starvation
    for (ResourceRequest rr : starvedApp.getStarvedResourceRequests()) {
      List<FSSchedulerNode> potentialNodes = scheduler.getNodeTracker()
              .getNodesByResourceName(rr.getResourceName());
      for (int i = 0; i < rr.getNumContainers(); i++) {
        PreemptableContainers bestContainers =
            getBestPreemptableContainers(rr, potentialNodes);
        if (bestContainers != null) {
          List<RMContainer> containers = bestContainers.getAllContainers();
          if (containers.size() > 0) {
            containersToPreempt.addAll(containers);
            // Reserve the containers for the starved app
            trackPreemptionsAgainstNode(containers, starvedApp);
            // Warn application about containers to be killed
            for (RMContainer container : containers) {
              FSAppAttempt app = scheduler.getSchedulerApp(
                      container.getApplicationAttemptId());
              LOG.info("Preempting container " + container + " from queue: "
                  + (app != null ? app.getQueueName() : "unknown"));
              // If the app has unregistered while building the container list
              // the app might be null, skip notifying the app
              if (app != null) {
                app.trackContainerForPreemption(container);
              }
            }
          }
        }
      }
    } // End of iteration over RRs
    return containersToPreempt;
  }

  private PreemptableContainers identifyContainersToPreemptForOneContainer(
          List<FSSchedulerNode> potentialNodes, ResourceRequest rr) {
    PreemptableContainers bestContainers = null;
    int maxAMContainers = Integer.MAX_VALUE;

    for (FSSchedulerNode node : potentialNodes) {
      PreemptableContainers preemptableContainers =
              identifyContainersToPreemptOnNode(
                      rr.getCapability(), node, maxAMContainers);

      if (preemptableContainers != null) {
        // This set is better than any previously identified set.
        bestContainers = preemptableContainers;
        maxAMContainers = bestContainers.numAMContainers;

        if (maxAMContainers == 0) {
          break;
        }
      }
    }
    return bestContainers;
  }

  /**
   * Identify containers to preempt on a given node. Try to find a list with
   * least AM containers to avoid preempting AM containers. This method returns
   * a non-null set of containers only if the number of AM containers is less
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

    // Initialize potential with unallocated but not reserved resources
    Resource potential = Resources.subtractFromNonNegative(
        Resources.clone(node.getUnallocatedResource()),
        node.getTotalReserved());

    for (RMContainer container : containersToCheck) {
      FSAppAttempt app =
          scheduler.getSchedulerApp(container.getApplicationAttemptId());
      // If the app has unregistered while building the container list the app
      // might be null, just skip this container: it should be cleaned up soon
      if (app == null) {
        LOG.info("Found container " + container + " on node "
            + node.getNodeName() + "without app, skipping preemption");
        continue;
      }
      ApplicationId appId = app.getApplicationId();

      if (app.canContainerBePreempted(container,
              preemptableContainers.getResourcesToPreemptForApp(appId))) {
        // Flag container for preemption
        if (!preemptableContainers.addContainer(container, appId)) {
          return null;
        }

        Resources.addTo(potential, container.getAllocatedResource());
      }

      // Check if we have already identified enough containers
      if (Resources.fitsIn(request, potential)) {
        return preemptableContainers;
      }
    }

    // Return null if the sum of all preemptable containers' resources
    // isn't enough to satisfy the starved request.
    return null;
  }

  private void trackPreemptionsAgainstNode(List<RMContainer> containers,
                                           FSAppAttempt app) {
    FSSchedulerNode node = scheduler.getNodeTracker()
        .getNode(containers.get(0).getNodeId());
    node.addContainersForPreemption(containers, app);
  }

  private void preemptContainers(List<RMContainer> containers) {
    // Schedule timer task to kill containers
    preemptionTimer.schedule(
        new PreemptContainersTask(containers), warnTimeBeforeKill);
  }

  /**
   * Iterate through matching nodes and identify containers to preempt all on
   * one node, also optimizing for least number of AM container preemptions.
   * Only nodes that match the locality level specified in the
   * {@link ResourceRequest} are considered. However, if this would lead to
   * AM preemption, and locality relaxation is allowed, then the search space
   * is expanded to the remaining nodes.
   *
   * @param rr resource request
   * @param potentialNodes list of {@link FSSchedulerNode}
   * @return the list of best preemptable containers for the resource request
   */
  private PreemptableContainers getBestPreemptableContainers(ResourceRequest rr,
      List<FSSchedulerNode> potentialNodes) {
    PreemptableContainers bestContainers =
        identifyContainersToPreemptForOneContainer(potentialNodes, rr);

    if (rr.getRelaxLocality()
        && !ResourceRequest.isAnyLocation(rr.getResourceName())
        && bestContainers != null
        && bestContainers.numAMContainers > 0) {
      List<FSSchedulerNode> remainingNodes =
          scheduler.getNodeTracker().getAllNodes();
      remainingNodes.removeAll(potentialNodes);
      PreemptableContainers spareContainers =
          identifyContainersToPreemptForOneContainer(remainingNodes, rr);
      if (spareContainers != null && spareContainers.numAMContainers
          < bestContainers.numAMContainers) {
        bestContainers = spareContainers;
      }
    }

    return bestContainers;
  }

  private class PreemptContainersTask extends TimerTask {
    private final List<RMContainer> containers;

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
      }
    }
  }

  /**
   * A class to track preemptable containers.
   */
  private static class PreemptableContainers {
    Map<ApplicationId, List<RMContainer>> containersByApp;
    int numAMContainers;
    int maxAMContainers;

    PreemptableContainers(int maxAMContainers) {
      numAMContainers = 0;
      this.maxAMContainers = maxAMContainers;
      this.containersByApp = new HashMap<>();
    }

    /**
     * Add a container if the number of AM containers is less than
     * maxAMContainers.
     *
     * @param container the container to add
     * @return true if success; false otherwise
     */
    private boolean addContainer(RMContainer container, ApplicationId appId) {
      if (container.isAMContainer()) {
        numAMContainers++;
        if (numAMContainers >= maxAMContainers) {
          return false;
        }
      }

      if (!containersByApp.containsKey(appId)) {
        containersByApp.put(appId, new ArrayList<>());
      }

      containersByApp.get(appId).add(container);
      return true;
    }

    private List<RMContainer> getAllContainers() {
      List<RMContainer> allContainers = new ArrayList<>();
      for (List<RMContainer> containersForApp : containersByApp.values()) {
        allContainers.addAll(containersForApp);
      }
      return allContainers;
    }

    private Resource getResourcesToPreemptForApp(ApplicationId appId) {
      Resource resourcesToPreempt = Resources.createResource(0, 0);
      if (containersByApp.containsKey(appId)) {
        for (RMContainer container : containersByApp.get(appId)) {
          Resources.addTo(resourcesToPreempt, container.getAllocatedResource());
        }
      }
      return resourcesToPreempt;
    }
  }
}
