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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.IntraQueueCandidatesSelector.TAPriorityComparator;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.IntraQueuePreemptionOrderPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
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
  public Collection<FiCaSchedulerApp> getPreemptableApps(String queueName,
      String partition) {
    TempQueuePerPartition tq = context.getQueueByPartition(queueName,
        partition);

    List<FiCaSchedulerApp> apps = new ArrayList<FiCaSchedulerApp>();
    for (TempAppPerPartition tmpApp : tq.getApps()) {
      // If a lower priority app was not selected to get preempted, mark such
      // apps out from preemption candidate selection.
      if (Resources.equals(tmpApp.getActuallyToBePreempted(),
          Resources.none())) {
        continue;
      }

      apps.add(tmpApp.app);
    }
    return apps;
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
      TempQueuePerPartition tq,
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
    PriorityQueue<TempAppPerPartition> orderedByPriority = createTempAppForResCalculation(
        tq, apps, clusterResource, perUserAMUsed);

    // 4. Calculate idealAssigned per app by checking based on queue's
    // unallocated resource.Also return apps arranged from lower priority to
    // higher priority.
    TreeSet<TempAppPerPartition> orderedApps = calculateIdealAssignedResourcePerApp(
        clusterResource, tq, selectedCandidates, queueReassignableResource,
        orderedByPriority);

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
        Resources.clone(preemptionLimit));

    // Save all apps (low to high) to temp queue for further reference
    tq.addAllApps(orderedApps);

    // 8. There are chances that we may preempt for the demand from same
    // priority level, such cases are to be validated out.
    validateOutSameAppPriorityFromDemand(clusterResource,
        (TreeSet<TempAppPerPartition>) orderedApps, tq.getUsersPerPartition(),
        context.getIntraQueuePreemptionOrderPolicy());

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
      Resources.subtractFromNonNegative(preemtableFromApp, tmpApp.selected);
      Resources.subtractFromNonNegative(preemtableFromApp, tmpApp.getAMUsed());

      if (context.getIntraQueuePreemptionOrderPolicy()
            .equals(IntraQueuePreemptionOrderPolicy.USERLIMIT_FIRST)) {
        Resources.subtractFromNonNegative(preemtableFromApp,
          tmpApp.getFiCaSchedulerApp().getCSLeafQueue().getMinimumAllocation());
      }

      // Calculate toBePreempted from apps as follows:
      // app.preemptable = min(max(app.used - app.selected - app.ideal, 0),
      // intra_q_preemptable)
      tmpApp.toBePreempted = Resources.min(rc, clusterResource, Resources
          .max(rc, clusterResource, preemtableFromApp, Resources.none()),
          Resources.clone(preemptionLimit));

      preemptionLimit = Resources.subtractFromNonNegative(preemptionLimit,
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
   * @param tq TempQueue
   * @param selectedCandidates Already Selected preemption candidates
   * @param queueReassignableResource Resource used in a queue
   * @param orderedByPriority List of running apps
   * @return List of temp apps ordered from low to high priority
   */
  private TreeSet<TempAppPerPartition> calculateIdealAssignedResourcePerApp(
      Resource clusterResource, TempQueuePerPartition tq,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource queueReassignableResource,
      PriorityQueue<TempAppPerPartition> orderedByPriority) {

    Comparator<TempAppPerPartition> reverseComp = Collections
        .reverseOrder(new TAPriorityComparator());
    TreeSet<TempAppPerPartition> orderedApps = new TreeSet<>(reverseComp);

    String partition = tq.partition;
    Map<String, TempUserPerPartition> usersPerPartition = tq.getUsersPerPartition();

    while (!orderedByPriority.isEmpty()) {
      // Remove app from the next highest remaining priority and process it to
      // calculate idealAssigned per app.
      TempAppPerPartition tmpApp = orderedByPriority.remove();
      orderedApps.add(tmpApp);

      // Once unallocated resource is 0, we can stop assigning ideal per app.
      if (Resources.lessThanOrEqual(rc, clusterResource,
          queueReassignableResource, Resources.none())
          || Resources.isAnyMajorResourceZero(rc, queueReassignableResource)) {
        continue;
      }

      String userName = tmpApp.app.getUser();
      TempUserPerPartition tmpUser = usersPerPartition.get(userName);
      Resource userLimitResource = tmpUser.getUserLimit();
      Resource idealAssignedForUser = tmpUser.idealAssigned;

      // Calculate total selected container resources from current app.
      getAlreadySelectedPreemptionCandidatesResource(selectedCandidates, tmpApp,
          tmpUser, partition);

      // For any app, used+pending will give its idealAssigned. However it will
      // be tightly linked to queue's unallocated quota. So lower priority apps
      // idealAssigned may fall to 0 if higher priority apps demand is more.
      Resource appIdealAssigned = Resources.add(tmpApp.getUsedDeductAM(),
          tmpApp.getPending());
      Resources.subtractFrom(appIdealAssigned, tmpApp.selected);

      if (Resources.lessThan(rc, clusterResource, idealAssignedForUser,
          userLimitResource)) {
        Resource idealAssigned = Resources.min(rc, clusterResource,
            appIdealAssigned,
            Resources.subtract(userLimitResource, idealAssignedForUser));
        tmpApp.idealAssigned = Resources.clone(Resources.min(rc,
            clusterResource, queueReassignableResource, idealAssigned));
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

      Resources.subtractFromNonNegative(queueReassignableResource,
          tmpApp.idealAssigned);
    }

    return orderedApps;
  }

  /*
   * Previous policies would have already selected few containers from an
   * application. Calculate total resource from these selected containers.
   */
  private void getAlreadySelectedPreemptionCandidatesResource(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      TempAppPerPartition tmpApp, TempUserPerPartition tmpUser,
      String partition) {
    tmpApp.selected = Resources.createResource(0, 0);
    Set<RMContainer> containers = selectedCandidates
        .get(tmpApp.app.getApplicationAttemptId());

    if (containers == null) {
      return;
    }

    for (RMContainer cont : containers) {
      if (partition.equals(cont.getNodeLabelExpression())) {
        Resources.addTo(tmpApp.selected, cont.getAllocatedResource());
        Resources.addTo(tmpUser.selected, cont.getAllocatedResource());
      }
    }
  }

  private PriorityQueue<TempAppPerPartition> createTempAppForResCalculation(
      TempQueuePerPartition tq, Collection<FiCaSchedulerApp> apps,
      Resource clusterResource,
      Map<String, Resource> perUserAMUsed) {
    TAPriorityComparator taComparator = new TAPriorityComparator();
    PriorityQueue<TempAppPerPartition> orderedByPriority = new PriorityQueue<>(
        100, taComparator);

    String partition = tq.partition;
    Map<String, TempUserPerPartition> usersPerPartition = tq
        .getUsersPerPartition();

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

      // Create a TempUserPerPartition structure to hold more information
      // regarding each user's entities such as UserLimit etc. This could
      // be kept in a user to TempUserPerPartition map for further reference.
      String userName = app.getUser();
      if (!usersPerPartition.containsKey(userName)) {
        ResourceUsage userResourceUsage = tq.leafQueue.getUser(userName)
            .getResourceUsage();

        // perUserAMUsed was populated with running apps, now we are looping
        // through both running and pending apps.
        Resource userSpecificAmUsed = perUserAMUsed.get(userName);
        amUsed = (userSpecificAmUsed == null)
            ? Resources.none() : userSpecificAmUsed;

        TempUserPerPartition tmpUser = new TempUserPerPartition(
            tq.leafQueue.getUser(userName), tq.queueName,
            Resources.clone(userResourceUsage.getUsed(partition)),
            Resources.clone(amUsed),
            Resources.clone(userResourceUsage.getReserved(partition)),
            Resources.none());

        Resource userLimitResource = Resources.clone(
            tq.leafQueue.getResourceLimitForAllUsers(userName, clusterResource,
                partition, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));

        // Real AM used need not have to be considered for user-limit as well.
        userLimitResource = Resources.subtract(userLimitResource,
            tmpUser.amUsed);
        tmpUser.setUserLimit(userLimitResource);

        if (LOG.isDebugEnabled()) {
          LOG.debug("TempUser:" + tmpUser);
        }

        tmpUser.idealAssigned = Resources.createResource(0, 0);
        tq.addUserPerPartition(userName, tmpUser);
      }
    }
    return orderedByPriority;
  }

  /*
   * Fifo+Priority based preemption policy need not have to preempt resources at
   * same priority level. Such cases will be validated out. But if the demand is
   * from an app of different user, force to preempt resources even if apps are
   * at same priority.
   */
  public void validateOutSameAppPriorityFromDemand(Resource cluster,
      TreeSet<TempAppPerPartition> orderedApps,
      Map<String, TempUserPerPartition> usersPerPartition,
      IntraQueuePreemptionOrderPolicy intraQueuePreemptionOrder) {

    TempAppPerPartition[] apps = orderedApps
        .toArray(new TempAppPerPartition[orderedApps.size()]);
    if (apps.length <= 0) {
      return;
    }

    for (int hPriority = apps.length - 1; hPriority >= 0; hPriority--) {

      // Check whether high priority app with demand needs resource from other
      // user.
      if (Resources.greaterThan(rc, cluster,
          apps[hPriority].getToBePreemptFromOther(), Resources.none())) {

        // Given we have a demand from a high priority app, we can do a reverse
        // scan from lower priority apps to select resources.
        // Since idealAssigned of each app has considered user-limit, this logic
        // will provide eventual consistency w.r.t user-limit as well.
        for (int lPriority = 0; lPriority < apps.length; lPriority++) {

          // Check whether app with demand needs resource from other user.
          if (Resources.greaterThan(rc, cluster, apps[lPriority].toBePreempted,
              Resources.none())) {

            // If apps are of same user, and priority is same, then skip.
            if ((apps[hPriority].getUser().equals(apps[lPriority].getUser()))
                && (apps[lPriority].getPriority() >= apps[hPriority]
                    .getPriority())) {
              continue;
            }

            if (Resources.lessThanOrEqual(rc, cluster,
                apps[lPriority].toBePreempted,
                apps[lPriority].getActuallyToBePreempted())
                || Resources.equals(apps[hPriority].getToBePreemptFromOther(),
                    Resources.none())) {
              continue;
            }

            // Ideally if any application has a higher priority, then it can
            // force to preempt any lower priority app from any user. However
            // if admin enforces user-limit over priority, preemption module
            // will not choose lower priority apps from usre's who are not yet
            // met its user-limit.
            TempUserPerPartition tmpUser = usersPerPartition
                .get(apps[lPriority].getUser());
            if ((!apps[hPriority].getUser().equals(apps[lPriority].getUser()))
                && (!tmpUser.isUserLimitReached(rc, cluster))
                && (intraQueuePreemptionOrder
                    .equals(IntraQueuePreemptionOrderPolicy.USERLIMIT_FIRST))) {
              continue;
            }

            Resource toPreemptFromOther = apps[hPriority]
                .getToBePreemptFromOther();
            Resource actuallyToPreempt = apps[lPriority]
                .getActuallyToBePreempted();

            // A lower priority app could offer more resource to preempt, if
            // multiple higher priority/under served users needs resources.
            // After one iteration, we need to ensure that actuallyToPreempt is
            // subtracted from the resource to preempt.
            Resource preemptableFromLowerPriorityApp = Resources
                .subtract(apps[lPriority].toBePreempted, actuallyToPreempt);

            // In case of user-limit preemption, when app's are from different
            // user and of same priority, we will do user-limit preemption if
            // there is a demand from under UL quota app.
            // However this under UL quota app's demand may be more.
            // Still we should ensure that we are not doing over preemption such
            // that only a maximum of (user's used - UL quota) could be
            // preempted.
            if ((!apps[hPriority].getUser().equals(apps[lPriority].getUser()))
                && (apps[lPriority].getPriority() == apps[hPriority]
                    .getPriority())
                && tmpUser.isUserLimitReached(rc, cluster)) {

              Resource deltaULQuota = Resources
                  .subtract(tmpUser.getUsedDeductAM(), tmpUser.selected);
              Resources.subtractFrom(deltaULQuota, tmpUser.getUserLimit());

              if (tmpUser.isPreemptionQuotaForULDeltaDone()) {
                deltaULQuota = Resources.createResource(0, 0);
              }

              if (Resources.lessThan(rc, cluster, deltaULQuota,
                  preemptableFromLowerPriorityApp)) {
                tmpUser.updatePreemptionQuotaForULDeltaAsDone(true);
                preemptableFromLowerPriorityApp = deltaULQuota;
              }
            }

            if (Resources.greaterThan(rc, cluster,
                preemptableFromLowerPriorityApp, Resources.none())) {
              Resource toPreempt = Resources.min(rc, cluster,
                  toPreemptFromOther, preemptableFromLowerPriorityApp);

              apps[hPriority].setToBePreemptFromOther(
                  Resources.subtract(toPreemptFromOther, toPreempt));
              apps[lPriority].setActuallyToBePreempted(
                  Resources.add(actuallyToPreempt, toPreempt));
            }
          }
        }
      }
    }
  }

  private Resource calculateUsedAMResourcesPerQueue(String partition,
      LeafQueue leafQueue, Map<String, Resource> perUserAMUsed) {
    Collection<FiCaSchedulerApp> runningApps = leafQueue.getApplications();
    Resource amUsed = Resources.createResource(0, 0);

    synchronized (leafQueue) {
      for (FiCaSchedulerApp app : runningApps) {
        Resource userAMResource = perUserAMUsed.get(app.getUser());
        if (null == userAMResource) {
          userAMResource = Resources.createResource(0, 0);
          perUserAMUsed.put(app.getUser(), userAMResource);
        }

        Resources.addTo(userAMResource, app.getAMResource(partition));
        Resources.addTo(amUsed, app.getAMResource(partition));
      }
    }

    return amUsed;
  }

  @Override
  public boolean skipContainerBasedOnIntraQueuePolicy(FiCaSchedulerApp app,
      Resource clusterResource, Resource usedResource, RMContainer c) {
    // Ensure below checks
    // 1. This check must be done only when preemption order is USERLIMIT_FIRST
    // 2. By selecting container "c", check whether this user's resource usage
    // is going below its user-limit.
    // 3. Used resource of user must be always greater than user-limit to
    // skip some containers as per this check. If used resource is under user
    // limit, then these containers of this user has to be preempted as demand
    // might be due to high priority apps running in same user.
    String partition = context.getScheduler()
        .getSchedulerNode(c.getAllocatedNode()).getPartition();
    TempQueuePerPartition tq = context.getQueueByPartition(app.getQueueName(),
        partition);
    TempUserPerPartition tmpUser = tq.getUsersPerPartition().get(app.getUser());

    // Given user is not present, skip the check.
    if (tmpUser == null) {
      return false;
    }

    // For ideal resource computations, user-limit got saved by subtracting am
    // used resource in TempUser. Hence it has to be added back here for
    // complete check.
    Resource userLimit = Resources.add(tmpUser.getUserLimit(), tmpUser.amUsed);

    return Resources.lessThanOrEqual(rc, clusterResource,
        Resources.subtract(usedResource, c.getAllocatedResource()), userLimit)
        && context.getIntraQueuePreemptionOrderPolicy()
            .equals(IntraQueuePreemptionOrderPolicy.USERLIMIT_FIRST);
  }
}
