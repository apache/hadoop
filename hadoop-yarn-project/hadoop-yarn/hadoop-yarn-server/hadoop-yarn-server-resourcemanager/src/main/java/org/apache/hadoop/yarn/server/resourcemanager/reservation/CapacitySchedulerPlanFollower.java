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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlanQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ReservationQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a {@link PlanFollower}. This is invoked on a timer, and
 * it is in charge to publish the state of the {@link Plan}s to the underlying
 * {@link CapacityScheduler}. This implementation does so, by
 * adding/removing/resizing leaf queues in the scheduler, thus affecting the
 * dynamic behavior of the scheduler in a way that is consistent with the
 * content of the plan. It also updates the plan's view on how much resources
 * are available in the cluster.
 * 
 * This implementation of PlanFollower is relatively stateless, and it can
 * synchronize schedulers and Plans that have arbitrary changes (performing set
 * differences among existing queues). This makes it resilient to frequency of
 * synchronization, and RM restart issues (no "catch up" is necessary).
 */
public class CapacitySchedulerPlanFollower implements PlanFollower {

  private static final Logger LOG = LoggerFactory
      .getLogger(CapacitySchedulerPlanFollower.class);

  private Collection<Plan> plans = new ArrayList<Plan>();

  private Clock clock;
  private CapacityScheduler scheduler;

  @Override
  public void init(Clock clock, ResourceScheduler sched, Collection<Plan> plans) {
    LOG.info("Initializing Plan Follower Policy:"
        + this.getClass().getCanonicalName());
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException(
          "CapacitySchedulerPlanFollower can only work with CapacityScheduler");
    }
    this.clock = clock;
    this.scheduler = (CapacityScheduler) sched;
    this.plans.addAll(plans);
  }

  @Override
  public synchronized void run() {
    for (Plan plan : plans) {
      synchronizePlan(plan);
    }
  }

  @Override
  public synchronized void synchronizePlan(Plan plan) {
    String planQueueName = plan.getQueueName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running plan follower edit policy for plan: " + planQueueName);
    }
    // align with plan step
    long step = plan.getStep();
    long now = clock.getTime();
    if (now % step != 0) {
      now += step - (now % step);
    }
    CSQueue queue = scheduler.getQueue(planQueueName);
    if (!(queue instanceof PlanQueue)) {
      LOG.error("The Plan is not an PlanQueue!");
      return;
    }
    PlanQueue planQueue = (PlanQueue) queue;
    // first we publish to the plan the current availability of resources
    Resource clusterResources = scheduler.getClusterResource();
    float planAbsCap = planQueue.getAbsoluteCapacity();
    Resource planResources = Resources.multiply(clusterResources, planAbsCap);
    plan.setTotalCapacity(planResources);

    Set<ReservationAllocation> currentReservations =
        plan.getReservationsAtTime(now);
    Set<String> curReservationNames = new HashSet<String>();
    Resource reservedResources = Resource.newInstance(0, 0);
    int numRes = 0;
    if (currentReservations != null) {
      numRes = currentReservations.size();
      for (ReservationAllocation reservation : currentReservations) {
        curReservationNames.add(reservation.getReservationId().toString());
        Resources.addTo(reservedResources, reservation.getResourcesAtTime(now));
      }
    }
    // create the default reservation queue if it doesnt exist
    String defReservationQueue = planQueueName + PlanQueue.DEFAULT_QUEUE_SUFFIX;
    if (scheduler.getQueue(defReservationQueue) == null) {
      try {
        ReservationQueue defQueue =
            new ReservationQueue(scheduler, defReservationQueue, planQueue);
        scheduler.addQueue(defQueue);
      } catch (SchedulerDynamicEditException e) {
        LOG.warn(
            "Exception while trying to create default reservation queue for plan: {}",
            planQueueName, e);
      } catch (IOException e) {
        LOG.warn(
            "Exception while trying to create default reservation queue for plan: {}",
            planQueueName, e);
      }
    }
    curReservationNames.add(defReservationQueue);
    // if the resources dedicated to this plan has shrunk invoke replanner
    if (Resources.greaterThan(scheduler.getResourceCalculator(),
        clusterResources, reservedResources, planResources)) {
      try {
        plan.getReplanner().plan(plan, null);
      } catch (PlanningException e) {
        LOG.warn("Exception while trying to replan: {}", planQueueName, e);
      }
    }
    // identify the reservations that have expired and new reservations that
    // have to be activated
    List<CSQueue> resQueues = planQueue.getChildQueues();
    Set<String> expired = new HashSet<String>();
    for (CSQueue resQueue : resQueues) {
      String resQueueName = resQueue.getQueueName();
      if (curReservationNames.contains(resQueueName)) {
        // it is already existing reservation, so needed not create new
        // reservation queue
        curReservationNames.remove(resQueueName);
      } else {
        // the reservation has termination, mark for cleanup
        expired.add(resQueueName);
      }
    }
    // garbage collect expired reservations
    cleanupExpiredQueues(plan.getMoveOnExpiry(), expired, defReservationQueue);

    // Add new reservations and update existing ones
    float totalAssignedCapacity = 0f;
    if (currentReservations != null) {
      // first release all excess capacity in default queue
      try {
        scheduler.setEntitlement(defReservationQueue, new QueueEntitlement(0f,
            1.0f));
      } catch (YarnException e) {
        LOG.warn(
            "Exception while trying to release default queue capacity for plan: {}",
            planQueueName, e);
      }
      // sort allocations from the one giving up the most resources, to the
      // one asking for the most
      // avoid order-of-operation errors that temporarily violate 100%
      // capacity bound
      List<ReservationAllocation> sortedAllocations =
          sortByDelta(
              new ArrayList<ReservationAllocation>(currentReservations), now);
      for (ReservationAllocation res : sortedAllocations) {
        String currResId = res.getReservationId().toString();
        if (curReservationNames.contains(currResId)) {
          try {
            ReservationQueue resQueue =
                new ReservationQueue(scheduler, currResId, planQueue);
            scheduler.addQueue(resQueue);
          } catch (SchedulerDynamicEditException e) {
            LOG.warn(
                "Exception while trying to activate reservation: {} for plan: {}",
                currResId, planQueueName, e);
          } catch (IOException e) {
            LOG.warn(
                "Exception while trying to activate reservation: {} for plan: {}",
                currResId, planQueueName, e);
          }
        }
        Resource capToAssign = res.getResourcesAtTime(now);
        float targetCapacity = 0f;
        if (planResources.getMemory() > 0
            && planResources.getVirtualCores() > 0) {
          targetCapacity =
              Resources.divide(scheduler.getResourceCalculator(),
                  clusterResources, capToAssign, planResources);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Assigning capacity of {} to queue {} with target capacity {}",
              capToAssign, currResId, targetCapacity);
        }
        // set maxCapacity to 100% unless the job requires gang, in which
        // case we stick to capacity (as running early/before is likely a
        // waste of resources)
        float maxCapacity = 1.0f;
        if (res.containsGangs()) {
          maxCapacity = targetCapacity;
        }
        try {
          scheduler.setEntitlement(currResId, new QueueEntitlement(
              targetCapacity, maxCapacity));
        } catch (YarnException e) {
          LOG.warn("Exception while trying to size reservation for plan: {}",
              currResId, planQueueName, e);
        }
        totalAssignedCapacity += targetCapacity;
      }
    }
    // compute the default queue capacity
    float defQCap = 1.0f - totalAssignedCapacity;
    if (LOG.isDebugEnabled()) {
      LOG.debug("PlanFollowerEditPolicyTask: total Plan Capacity: {} "
          + "currReservation: {} default-queue capacity: {}", planResources,
          numRes, defQCap);
    }
    // set the default queue to eat-up all remaining capacity
    try {
      scheduler.setEntitlement(defReservationQueue, new QueueEntitlement(
          defQCap, 1.0f));
    } catch (YarnException e) {
      LOG.warn(
          "Exception while trying to reclaim default queue capacity for plan: {}",
          planQueueName, e);
    }
    // garbage collect finished reservations from plan
    try {
      plan.archiveCompletedReservations(now);
    } catch (PlanningException e) {
      LOG.error("Exception in archiving completed reservations: ", e);
    }
    LOG.info("Finished iteration of plan follower edit policy for plan: "
        + planQueueName);

    // Extension: update plan with app states,
    // useful to support smart replanning
  }

  /**
   * Move all apps in the set of queues to the parent plan queue's default
   * reservation queue in a synchronous fashion
   */
  private void moveAppsInQueueSync(String expiredReservation,
      String defReservationQueue) {
    List<ApplicationAttemptId> activeApps =
        scheduler.getAppsInQueue(expiredReservation);
    if (activeApps.isEmpty()) {
      return;
    }
    for (ApplicationAttemptId app : activeApps) {
      // fallback to parent's default queue
      try {
        scheduler.moveApplication(app.getApplicationId(), defReservationQueue);
      } catch (YarnException e) {
        LOG.warn(
            "Encountered unexpected error during migration of application: {} from reservation: {}",
            app, expiredReservation, e);
      }
    }
  }

  /**
   * First sets entitlement of queues to zero to prevent new app submission.
   * Then move all apps in the set of queues to the parent plan queue's default
   * reservation queue if move is enabled. Finally cleanups the queue by killing
   * any apps (if move is disabled or move failed) and removing the queue
   */
  private void cleanupExpiredQueues(boolean shouldMove, Set<String> toRemove,
      String defReservationQueue) {
    for (String expiredReservation : toRemove) {
      try {
        // reduce entitlement to 0
        scheduler.setEntitlement(expiredReservation, new QueueEntitlement(0.0f,
            0.0f));
        if (shouldMove) {
          moveAppsInQueueSync(expiredReservation, defReservationQueue);
        }
        if (scheduler.getAppsInQueue(expiredReservation).size() > 0) {
          scheduler.killAllAppsInQueue(expiredReservation);
          LOG.info("Killing applications in queue: {}", expiredReservation);
        } else {
          scheduler.removeQueue(expiredReservation);
          LOG.info("Queue: " + expiredReservation + " removed");
        }
      } catch (YarnException e) {
        LOG.warn("Exception while trying to expire reservation: {}",
            expiredReservation, e);
      }
    }
  }

  @Override
  public synchronized void setPlans(Collection<Plan> plans) {
    this.plans.clear();
    this.plans.addAll(plans);
  }

  /**
   * Sort in the order from the least new amount of resources asked (likely
   * negative) to the highest. This prevents "order-of-operation" errors related
   * to exceeding 100% capacity temporarily.
   */
  private List<ReservationAllocation> sortByDelta(
      List<ReservationAllocation> currentReservations, long now) {
    Collections.sort(currentReservations, new ReservationAllocationComparator(
        scheduler, now));
    return currentReservations;
  }

  private static class ReservationAllocationComparator implements
      Comparator<ReservationAllocation> {
    CapacityScheduler scheduler;
    long now;

    ReservationAllocationComparator(CapacityScheduler scheduler, long now) {
      this.scheduler = scheduler;
      this.now = now;
    }

    private Resource getUnallocatedReservedResources(
        ReservationAllocation reservation) {
      Resource resResource;
      CSQueue resQueue =
          scheduler.getQueue(reservation.getReservationId().toString());
      if (resQueue != null) {
        resResource =
            Resources.subtract(
                reservation.getResourcesAtTime(now),
                Resources.multiply(scheduler.getClusterResource(),
                    resQueue.getAbsoluteCapacity()));
      } else {
        resResource = reservation.getResourcesAtTime(now);
      }
      return resResource;
    }

    @Override
    public int compare(ReservationAllocation lhs, ReservationAllocation rhs) {
      // compute delta between current and previous reservation, and compare
      // based on that
      Resource lhsRes = getUnallocatedReservedResources(lhs);
      Resource rhsRes = getUnallocatedReservedResources(rhs);
      return lhsRes.compareTo(rhsRes);
    }

  }

}
