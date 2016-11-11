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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.IntraQueueCandidatesSelector.TAPriorityComparator;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * FifoIntraQueuePreemptionPlugin will handle intra-queue preemption for
 * priority and user-limit.
 */
public class FifoIntraQueuePreemptionPlugin
    implements
      IntraQueuePreemptionComputePlugin {

  protected final CapacitySchedulerPreemptionContext context;
  protected final ResourceCalculator rc;

  private static final Log LOG =
      LogFactory.getLog(FifoIntraQueuePreemptionPlugin.class);

  public FifoIntraQueuePreemptionPlugin(ResourceCalculator rc,
      CapacitySchedulerPreemptionContext preemptionContext) {
    this.context = preemptionContext;
    this.rc = rc;
  }

  @Override
  public Map<String, Resource> getResourceDemandFromAppsPerQueue(
      String queueName, String partition) {

    Map<String, Resource> resToObtainByPartition = new HashMap<>();
    TempQueuePerPartition tq = context
        .getQueueByPartition(queueName, partition);

    Collection<TempAppPerPartition> appsOrderedByPriority = tq.getApps();
    Resource actualPreemptNeeded = resToObtainByPartition.get(partition);

    // Updating pending resource per-partition level.
    if (actualPreemptNeeded == null) {
      actualPreemptNeeded = Resources.createResource(0, 0);
      resToObtainByPartition.put(partition, actualPreemptNeeded);
    }

    for (TempAppPerPartition a1 : appsOrderedByPriority) {
      Resources.addTo(actualPreemptNeeded, a1.getActuallyToBePreempted());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Selected to preempt " + actualPreemptNeeded
          + " resource from partition:" + partition);
    }
    return resToObtainByPartition;
  }

  @Override
  public void computeAppsIdealAllocation(Resource clusterResource,
      Resource partitionBasedResource, TempQueuePerPartition tq,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource totalPreemptedResourceAllowed,
      Resource queueReassignableResource, float maxAllowablePreemptLimit) {

    // 1. AM used resource can be considered as a frozen resource for now.
    // Hence such containers in a queue can be omitted from the preemption
    // calculation.
    Map<String, Resource> perUserAMUsed = new HashMap<String, Resource>();
    Resource amUsed = calculateUsedAMResourcesPerQueue(tq.partition,
        tq.leafQueue, perUserAMUsed);
    Resources.subtractFrom(queueReassignableResource, amUsed);

    // 2. tq.leafQueue will not be null as we validated it in caller side
    Collection<FiCaSchedulerApp> apps = tq.leafQueue.getAllApplications();

    // We do not need preemption for a single app
    if (apps.size() == 1) {
      return;
    }

    // 3. Create all tempApps for internal calculation and return a list from
    // high priority to low priority order.
    TAPriorityComparator taComparator = new TAPriorityComparator();
    PriorityQueue<TempAppPerPartition> orderedByPriority =
        createTempAppForResCalculation(tq.partition, apps, taComparator);

    // 4. Calculate idealAssigned per app by checking based on queue's
    // unallocated resource.Also return apps arranged from lower priority to
    // higher priority.
    TreeSet<TempAppPerPartition> orderedApps =
        calculateIdealAssignedResourcePerApp(clusterResource,
            partitionBasedResource, tq, selectedCandidates,
            queueReassignableResource, orderedByPriority, perUserAMUsed);

    // 5. A configurable limit that could define an ideal allowable preemption
    // limit. Based on current queue's capacity,defined how much % could become
    // preemptable.
    Resource maxIntraQueuePreemptable = Resources.multiply(tq.getGuaranteed(),
        maxAllowablePreemptLimit);
    if (Resources.greaterThan(rc, clusterResource, maxIntraQueuePreemptable,
        tq.getActuallyToBePreempted())) {
      Resources.subtractFrom(maxIntraQueuePreemptable,
          tq.getActuallyToBePreempted());
    } else {
      maxIntraQueuePreemptable = Resource.newInstance(0, 0);
    }

    // 6. We have two configurations here, one is intra queue limit and second
    // one is per-round limit for any time preemption. Take a minimum of these
    Resource preemptionLimit = Resources.min(rc, clusterResource,
        maxIntraQueuePreemptable, totalPreemptedResourceAllowed);

    // 7. From lowest priority app onwards, calculate toBePreempted resource
    // based on demand.
    calculateToBePreemptedResourcePerApp(clusterResource, orderedApps,
        preemptionLimit);

    // Save all apps (low to high) to temp queue for further reference
    tq.addAllApps(orderedApps);

    // 8. There are chances that we may preempt for the demand from same
    // priority level, such cases are to be validated out.
    validateOutSameAppPriorityFromDemand(clusterResource,
        (TreeSet<TempAppPerPartition>) tq.getApps());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Queue Name:" + tq.queueName + ", partition:" + tq.partition);
      for (TempAppPerPartition tmpApp : tq.getApps()) {
        LOG.debug(tmpApp);
      }
    }
  }

  private void calculateToBePreemptedResourcePerApp(Resource clusterResource,
      TreeSet<TempAppPerPartition> orderedApps, Resource preemptionLimit) {

    for (TempAppPerPartition tmpApp : orderedApps) {
      if (Resources.lessThanOrEqual(rc, clusterResource, preemptionLimit,
          Resources.none())
          || Resources.lessThanOrEqual(rc, clusterResource, tmpApp.getUsed(),
              Resources.none())) {
        continue;
      }

      Resource preemtableFromApp = Resources.subtract(tmpApp.getUsed(),
          tmpApp.idealAssigned);
      Resources.subtractFrom(preemtableFromApp, tmpApp.selected);
      Resources.subtractFrom(preemtableFromApp, tmpApp.getAMUsed());

      // Calculate toBePreempted from apps as follows:
      // app.preemptable = min(max(app.used - app.selected - app.ideal, 0),
      // intra_q_preemptable)
      tmpApp.toBePreempted = Resources.min(rc, clusterResource, Resources
          .max(rc, clusterResource, preemtableFromApp, Resources.none()),
          preemptionLimit);

      preemptionLimit = Resources.subtract(preemptionLimit,
          tmpApp.toBePreempted);
    }
  }

  /**
   * Algorithm for calculating idealAssigned is as follows:
   * For each partition:
   *  Q.reassignable = Q.used - Q.selected;
   *  
   * # By default set ideal assigned 0 for app.
   * app.idealAssigned as 0
   * # get user limit from scheduler.
   * userLimitRes = Q.getUserLimit(userName)
   * 
   * # initial all value to 0
   * Map<String, Resource> userToAllocated
   * 
   * # Loop from highest priority to lowest priority app to calculate ideal
   * for app in sorted-by(priority) {
   *  if Q.reassignable < 0:
   *    break;
   *    
   *  if (user-to-allocated.get(app.user) < userLimitRes) {
   *   idealAssigned = min((userLimitRes - userToAllocated.get(app.user)), 
   *                      (app.used + app.pending - app.selected))
   *   app.idealAssigned = min(Q.reassignable, idealAssigned)
   *   userToAllocated.get(app.user) += app.idealAssigned;
   *  } else { 
   *   // skip this app because user-limit reached
   *  }
   *  Q.reassignable -= app.idealAssigned
   * }
   *  
   * @param clusterResource Cluster Resource
   * @param partitionBasedResource resource per partition
   * @param tq TempQueue
   * @param selectedCandidates Already Selected preemption candidates
   * @param queueReassignableResource Resource used in a queue
   * @param orderedByPriority List of running apps
   * @param perUserAMUsed AM used resource
   * @return List of temp apps ordered from low to high priority
   */
  private TreeSet<TempAppPerPartition> calculateIdealAssignedResourcePerApp(
      Resource clusterResource, Resource partitionBasedResource,
      TempQueuePerPartition tq,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource queueReassignableResource,
      PriorityQueue<TempAppPerPartition> orderedByPriority,
      Map<String, Resource> perUserAMUsed) {

    Comparator<TempAppPerPartition> reverseComp = Collections
        .reverseOrder(new TAPriorityComparator());
    TreeSet<TempAppPerPartition> orderedApps = new TreeSet<>(reverseComp);

    Map<String, Resource> userIdealAssignedMapping = new HashMap<>();
    String partition = tq.partition;

    Map<String, Resource> preCalculatedUserLimit =
        new HashMap<String, Resource>();

    while (!orderedByPriority.isEmpty()) {
      // Remove app from the next highest remaining priority and process it to
      // calculate idealAssigned per app.
      TempAppPerPartition tmpApp = orderedByPriority.remove();
      orderedApps.add(tmpApp);

      // Once unallocated resource is 0, we can stop assigning ideal per app.
      if (Resources.lessThanOrEqual(rc, clusterResource,
          queueReassignableResource, Resources.none())) {
        continue;
      }

      String userName = tmpApp.app.getUser();
      Resource userLimitResource = preCalculatedUserLimit.get(userName);

      // Verify whether we already calculated headroom for this user.
      if (userLimitResource == null) {
        userLimitResource = Resources.clone(tq.leafQueue
            .getUserLimitPerUser(userName, partitionBasedResource, partition));

        Resource amUsed = perUserAMUsed.get(userName);
        if (null == amUsed) {
          amUsed = Resources.createResource(0, 0);
        }

        // Real AM used need not have to be considered for user-limit as well.
        userLimitResource = Resources.subtract(userLimitResource, amUsed);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Userlimit for user '" + userName + "' is :"
              + userLimitResource + ", and amUsed is:" + amUsed);
        }

        preCalculatedUserLimit.put(userName, userLimitResource);
      }

      Resource idealAssignedForUser = userIdealAssignedMapping.get(userName);

      if (idealAssignedForUser == null) {
        idealAssignedForUser = Resources.createResource(0, 0);
        userIdealAssignedMapping.put(userName, idealAssignedForUser);
      }

      // Calculate total selected container resources from current app.
      getAlreadySelectedPreemptionCandidatesResource(selectedCandidates,
          tmpApp, partition);

      // For any app, used+pending will give its idealAssigned. However it will
      // be tightly linked to queue's unallocated quota. So lower priority apps
      // idealAssigned may fall to 0 if higher priority apps demand is more.
      Resource appIdealAssigned = Resources.add(tmpApp.getUsedDeductAM(),
          tmpApp.getPending());
      Resources.subtractFrom(appIdealAssigned, tmpApp.selected);

      if (Resources.lessThan(rc, clusterResource, idealAssignedForUser,
          userLimitResource)) {
        appIdealAssigned = Resources.min(rc, clusterResource, appIdealAssigned,
            Resources.subtract(userLimitResource, idealAssignedForUser));
        tmpApp.idealAssigned = Resources.clone(Resources.min(rc,
            clusterResource, queueReassignableResource, appIdealAssigned));
        Resources.addTo(idealAssignedForUser, tmpApp.idealAssigned);
      } else {
        continue;
      }

      // Also set how much resource is needed by this app from others.
      Resource appUsedExcludedSelected = Resources
          .subtract(tmpApp.getUsedDeductAM(), tmpApp.selected);
      if (Resources.greaterThan(rc, clusterResource, tmpApp.idealAssigned,
          appUsedExcludedSelected)) {
        tmpApp.setToBePreemptFromOther(
            Resources.subtract(tmpApp.idealAssigned, appUsedExcludedSelected));
      }

      Resources.subtractFrom(queueReassignableResource, tmpApp.idealAssigned);
    }

    return orderedApps;
  }

  /*
   * Previous policies would have already selected few containers from an
   * application. Calculate total resource from these selected containers.
   */
  private void getAlreadySelectedPreemptionCandidatesResource(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      TempAppPerPartition tmpApp, String partition) {
    tmpApp.selected = Resources.createResource(0, 0);
    Set<RMContainer> containers = selectedCandidates
        .get(tmpApp.app.getApplicationAttemptId());

    if (containers == null) {
      return;
    }

    for (RMContainer cont : containers) {
      if (partition.equals(cont.getNodeLabelExpression())) {
        Resources.addTo(tmpApp.selected, cont.getAllocatedResource());
      }
    }
  }

  private PriorityQueue<TempAppPerPartition> createTempAppForResCalculation(
      String partition, Collection<FiCaSchedulerApp> apps,
      TAPriorityComparator taComparator) {
    PriorityQueue<TempAppPerPartition> orderedByPriority = new PriorityQueue<>(
        100, taComparator);

    // have an internal temp app structure to store intermediate data(priority)
    for (FiCaSchedulerApp app : apps) {

      Resource used = app.getAppAttemptResourceUsage().getUsed(partition);
      Resource amUsed = null;
      if (!app.isWaitingForAMContainer()) {
        amUsed = app.getAMResource(partition);
      }
      Resource pending = app.getTotalPendingRequestsPerPartition()
          .get(partition);
      Resource reserved = app.getAppAttemptResourceUsage()
          .getReserved(partition);

      used = (used == null) ? Resources.createResource(0, 0) : used;
      amUsed = (amUsed == null) ? Resources.createResource(0, 0) : amUsed;
      pending = (pending == null) ? Resources.createResource(0, 0) : pending;
      reserved = (reserved == null) ? Resources.createResource(0, 0) : reserved;

      HashSet<String> partitions = new HashSet<String>(
          app.getAppAttemptResourceUsage().getNodePartitionsSet());
      partitions.addAll(app.getTotalPendingRequestsPerPartition().keySet());

      // Create TempAppPerQueue for further calculation.
      TempAppPerPartition tmpApp = new TempAppPerPartition(app,
          Resources.clone(used), Resources.clone(amUsed),
          Resources.clone(reserved), Resources.clone(pending));

      // Set ideal allocation of app as 0.
      tmpApp.idealAssigned = Resources.createResource(0, 0);

      orderedByPriority.add(tmpApp);
    }
    return orderedByPriority;
  }

  /*
   * Fifo+Priority based preemption policy need not have to preempt resources at
   * same priority level. Such cases will be validated out.
   */
  public void validateOutSameAppPriorityFromDemand(Resource cluster,
      TreeSet<TempAppPerPartition> appsOrderedfromLowerPriority) {

    TempAppPerPartition[] apps = appsOrderedfromLowerPriority
        .toArray(new TempAppPerPartition[appsOrderedfromLowerPriority.size()]);
    if (apps.length <= 0) {
      return;
    }

    int lPriority = 0;
    int hPriority = apps.length - 1;

    while (lPriority < hPriority
        && !apps[lPriority].equals(apps[hPriority])
        && apps[lPriority].getPriority() < apps[hPriority].getPriority()) {
      Resource toPreemptFromOther = apps[hPriority]
          .getToBePreemptFromOther();
      Resource actuallyToPreempt = apps[lPriority].getActuallyToBePreempted();
      Resource delta = Resources.subtract(apps[lPriority].toBePreempted,
          actuallyToPreempt);

      if (Resources.greaterThan(rc, cluster, delta, Resources.none())) {
        Resource toPreempt = Resources.min(rc, cluster,
            toPreemptFromOther, delta);

        apps[hPriority].setToBePreemptFromOther(
            Resources.subtract(toPreemptFromOther, toPreempt));
        apps[lPriority].setActuallyToBePreempted(
            Resources.add(actuallyToPreempt, toPreempt));
      }

      if (Resources.lessThanOrEqual(rc, cluster,
          apps[lPriority].toBePreempted,
          apps[lPriority].getActuallyToBePreempted())) {
        lPriority++;
        continue;
      }

      if (Resources.equals(apps[hPriority].getToBePreemptFromOther(),
          Resources.none())) {
        hPriority--;
        continue;
      }
    }
  }

  private Resource calculateUsedAMResourcesPerQueue(String partition,
      LeafQueue leafQueue, Map<String, Resource> perUserAMUsed) {
    Collection<FiCaSchedulerApp> runningApps = leafQueue.getApplications();
    Resource amUsed = Resources.createResource(0, 0);

    for (FiCaSchedulerApp app : runningApps) {
      Resource userAMResource = perUserAMUsed.get(app.getUser());
      if (null == userAMResource) {
        userAMResource = Resources.createResource(0, 0);
        perUserAMUsed.put(app.getUser(), userAMResource);
      }

      Resources.addTo(userAMResource, app.getAMResource(partition));
      Resources.addTo(amUsed, app.getAMResource(partition));
    }
    return amUsed;
  }
}
