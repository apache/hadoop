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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSchedulerPlanFollower implements PlanFollower {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSchedulerPlanFollower.class);

  protected Collection<Plan> plans = new ArrayList<Plan>();
  protected YarnScheduler scheduler;
  protected Clock clock;

  @Override
  public void init(Clock clock, ResourceScheduler sched,
      Collection<Plan> plans) {
    this.clock = clock;
    this.scheduler = sched;
    this.plans.addAll(plans);
  }

  @Override
  public synchronized void run() {
    for (Plan plan : plans) {
      synchronizePlan(plan, true);
    }
  }

  @Override
  public synchronized void setPlans(Collection<Plan> plans) {
    this.plans.clear();
    this.plans.addAll(plans);
  }

  @Override
  public synchronized void synchronizePlan(Plan plan, boolean shouldReplan) {
    String planQueueName = plan.getQueueName();
    LOG.debug("Running plan follower edit policy for plan: {}", planQueueName);
    // align with plan step
    long step = plan.getStep();
    long now = clock.getTime();
    if (now % step != 0) {
      now += step - (now % step);
    }
    Queue planQueue = getPlanQueue(planQueueName);
    if (planQueue == null) {
      return;
    }

    // first we publish to the plan the current availability of resources
    Resource clusterResources = scheduler.getClusterResource();
    Resource planResources =
        getPlanResources(plan, planQueue, clusterResources);
    Set<ReservationAllocation> currentReservations =
        plan.getReservationsAtTime(now);
    Set<String> curReservationNames = new HashSet<String>();
    Resource reservedResources = Resource.newInstance(0, 0);
    int numRes = getReservedResources(now, currentReservations,
        curReservationNames, reservedResources);
    // create the default reservation queue if it doesnt exist
    String defReservationId = getReservationIdFromQueueName(planQueueName)
        + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    String defReservationQueue =
        getReservationQueueName(planQueueName, defReservationId);
    createDefaultReservationQueue(planQueueName, planQueue, defReservationId);
    curReservationNames.add(defReservationId);
    // if the resources dedicated to this plan has shrunk invoke replanner
    boolean shouldResize = false;
    if (arePlanResourcesLessThanReservations(plan.getResourceCalculator(),
        clusterResources, planResources, reservedResources)) {
      if (shouldReplan) {
        try {
          plan.getReplanner().plan(plan, null);
        } catch (PlanningException e) {
          LOG.warn("Exception while trying to replan: {}", planQueueName, e);
        }
      } else {
        shouldResize = true;
      }
    }
    // identify the reservations that have expired and new reservations that
    // have to be activated
    List<? extends Queue> resQueues = getChildReservationQueues(planQueue);
    Set<String> expired = new HashSet<String>();
    for (Queue resQueue : resQueues) {
      String resQueueName = resQueue.getQueueName();
      String reservationId = getReservationIdFromQueueName(resQueueName);
      if (curReservationNames.contains(reservationId)) {
        // it is already existing reservation, so needed not create new
        // reservation queue
        curReservationNames.remove(reservationId);
      } else {
        // the reservation has termination, mark for cleanup
        expired.add(reservationId);
      }
    }
    // garbage collect expired reservations
    cleanupExpiredQueues(planQueueName, plan.getMoveOnExpiry(), expired,
        defReservationQueue);
    // Add new reservations and update existing ones
    float totalAssignedCapacity = 0f;
    if (currentReservations != null) {
      // first release all excess capacity in default queue
      try {
        setQueueEntitlement(planQueueName, defReservationQueue, 0f, 1.0f);
      } catch (YarnException e) {
        LOG.warn(
            "Exception while trying to release default queue capacity for plan: {}",
            planQueueName, e);
      }
      // sort allocations from the one giving up the most resources, to the
      // one asking for the most avoid order-of-operation errors that
      // temporarily violate 100% capacity bound
      List<ReservationAllocation> sortedAllocations = sortByDelta(
          new ArrayList<ReservationAllocation>(currentReservations), now, plan);
      for (ReservationAllocation res : sortedAllocations) {
        String currResId = res.getReservationId().toString();
        if (curReservationNames.contains(currResId)) {
          addReservationQueue(planQueueName, planQueue, currResId);
        }
        Resource capToAssign = res.getResourcesAtTime(now);
        float targetCapacity = 0f;
        if (planResources.getMemorySize() > 0
            && planResources.getVirtualCores() > 0) {
          if (shouldResize) {
            capToAssign = calculateReservationToPlanProportion(
                plan.getResourceCalculator(), planResources, reservedResources,
                capToAssign);
          }
          targetCapacity =
              calculateReservationToPlanRatio(plan.getResourceCalculator(),
                  clusterResources, planResources, capToAssign);
        }
        LOG.debug(
              "Assigning capacity of {} to queue {} with target capacity {}",
              capToAssign, currResId, targetCapacity);
        // set maxCapacity to 100% unless the job requires gang, in which
        // case we stick to capacity (as running early/before is likely a
        // waste of resources)
        float maxCapacity = 1.0f;
        if (res.containsGangs()) {
          maxCapacity = targetCapacity;
        }
        try {
          setQueueEntitlement(planQueueName, currResId, targetCapacity,
              maxCapacity);
        } catch (YarnException e) {
          LOG.warn("Exception while trying to size reservation for plan: {}",
              currResId, planQueueName, e);
        }
        totalAssignedCapacity += targetCapacity;
      }
    }
    // compute the default queue capacity
    float defQCap = 1.0f - totalAssignedCapacity;
    LOG.debug(
          "PlanFollowerEditPolicyTask: total Plan Capacity: {} "
              + "currReservation: {} default-queue capacity: {}",
          planResources, numRes, defQCap);
    // set the default queue to eat-up all remaining capacity
    try {
      setQueueEntitlement(planQueueName, defReservationQueue, defQCap, 1.0f);
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

  protected String getReservationIdFromQueueName(String resQueueName) {
    return resQueueName;
  }

  protected void setQueueEntitlement(String planQueueName, String currResId,
      float targetCapacity, float maxCapacity) throws YarnException {
    String reservationQueueName =
        getReservationQueueName(planQueueName, currResId);
    scheduler.setEntitlement(reservationQueueName,
        new QueueEntitlement(targetCapacity, maxCapacity));
  }

  // Schedulers have different ways of naming queues. See YARN-2773
  protected String getReservationQueueName(String planQueueName,
      String reservationId) {
    return reservationId;
  }

  /**
   * First sets entitlement of queues to zero to prevent new app submission.
   * Then move all apps in the set of queues to the parent plan queue's default
   * reservation queue if move is enabled. Finally cleanups the queue by killing
   * any apps (if move is disabled or move failed) and removing the queue
   *
   * @param planQueueName the name of {@code PlanQueue}
   * @param shouldMove flag to indicate if any running apps should be moved or
   *          killed
   * @param toRemove the remnant apps to clean up
   * @param defReservationQueue the default {@code ReservationQueue} of the
   *          {@link Plan}
   */
  protected void cleanupExpiredQueues(String planQueueName, boolean shouldMove,
      Set<String> toRemove, String defReservationQueue) {
    for (String expiredReservationId : toRemove) {
      try {
        // reduce entitlement to 0
        String expiredReservation =
            getReservationQueueName(planQueueName, expiredReservationId);
        setQueueEntitlement(planQueueName, expiredReservation, 0.0f, 0.0f);
        if (shouldMove) {
          moveAppsInQueueSync(expiredReservation, defReservationQueue);
        }
        List<ApplicationAttemptId> appsInQueue = scheduler.
              getAppsInQueue(expiredReservation);
        int size = (appsInQueue == null ? 0 : appsInQueue.size());
        if (size > 0) {
          scheduler.killAllAppsInQueue(expiredReservation);
          LOG.info("Killing applications in queue: {}", expiredReservation);
        } else {
          scheduler.removeQueue(expiredReservation);
          LOG.info("Queue: " + expiredReservation + " removed");
        }
      } catch (YarnException e) {
        LOG.warn("Exception while trying to expire reservation: {}",
            expiredReservationId, e);
      }
    }
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
            "Encountered unexpected error during migration of application: {}"
                + " from reservation: {}",
            app, expiredReservation, e);
      }
    }
  }

  protected int getReservedResources(long now,
      Set<ReservationAllocation> currentReservations,
      Set<String> curReservationNames, Resource reservedResources) {
    int numRes = 0;
    if (currentReservations != null) {
      numRes = currentReservations.size();
      for (ReservationAllocation reservation : currentReservations) {
        curReservationNames.add(reservation.getReservationId().toString());
        Resources.addTo(reservedResources, reservation.getResourcesAtTime(now));
      }
    }
    return numRes;
  }

  /**
   * Sort in the order from the least new amount of resources asked (likely
   * negative) to the highest. This prevents "order-of-operation" errors related
   * to exceeding 100% capacity temporarily.
   *
   * @param currentReservations the currently active reservations
   * @param now the current time
   * @param plan the {@link Plan} that is being considered
   *
   * @return the sorted list of {@link ReservationAllocation}s
   */
  protected List<ReservationAllocation> sortByDelta(
      List<ReservationAllocation> currentReservations, long now, Plan plan) {
    Collections.sort(currentReservations,
        new ReservationAllocationComparator(now, this, plan));
    return currentReservations;
  }

  /**
   * Get queue associated with reservable queue named.
   *
   * @param planQueueName name of the reservable queue
   * @return queue associated with the reservable queue
   */
  protected abstract Queue getPlanQueue(String planQueueName);

  /**
   * Resizes reservations based on currently available resources.
   */
  private Resource calculateReservationToPlanProportion(
      ResourceCalculator rescCalculator, Resource availablePlanResources,
      Resource totalReservationResources, Resource reservationResources) {
    return Resources.multiply(availablePlanResources, Resources.ratio(
        rescCalculator, reservationResources, totalReservationResources));
  }

  /**
   * Calculates ratio of reservationResources to planResources.
   */
  private float calculateReservationToPlanRatio(
      ResourceCalculator rescCalculator, Resource clusterResources,
      Resource planResources, Resource reservationResources) {
    return Resources.divide(rescCalculator, clusterResources,
        reservationResources, planResources);
  }

  /**
   * Check if plan resources are less than expected reservation resources.
   */
  private boolean arePlanResourcesLessThanReservations(
      ResourceCalculator rescCalculator, Resource clusterResources,
      Resource planResources, Resource reservedResources) {
    return Resources.greaterThan(rescCalculator, clusterResources,
        reservedResources, planResources);
  }

  /**
   * Get a list of reservation queues for this planQueue.
   *
   * @param planQueue the queue for the current {@link Plan}
   *
   * @return the queues corresponding to the reservations
   */
  protected abstract List<? extends Queue> getChildReservationQueues(
      Queue planQueue);

  /**
   * Add a new reservation queue for reservation currResId for this planQueue.
   *
   * @param planQueueName name of the reservable queue.
   * @param queue the queue for the current {@link Plan}.
   * @param currResId curr reservationId.
   */
  protected abstract void addReservationQueue(String planQueueName, Queue queue,
      String currResId);

  /**
   * Creates the default reservation queue for use when no reservation is used
   * for applications submitted to this planQueue.
   *
   * @param planQueueName name of the reservable queue
   * @param queue the queue for the current {@link Plan}
   * @param defReservationQueue name of the default {@code ReservationQueue}
   */
  protected abstract void createDefaultReservationQueue(String planQueueName,
      Queue queue, String defReservationQueue);

  /**
   * Get plan resources for this planQueue.
   *
   * @param plan the current {@link Plan} being considered
   * @param queue the queue for the current {@link Plan}
   * @param clusterResources the resources available in the cluster
   *
   * @return the resources allocated to the specified {@link Plan}
   */
  protected abstract Resource getPlanResources(Plan plan, Queue queue,
      Resource clusterResources);

  /**
   * Get reservation queue resources if it exists otherwise return null.
   *
   * @param plan the current {@link Plan} being considered
   * @param reservationId the identifier of the reservation
   *
   * @return the resources allocated to the specified reservation
   */
  protected abstract Resource getReservationQueueResourceIfExists(Plan plan,
      ReservationId reservationId);

  private static class ReservationAllocationComparator
      implements Comparator<ReservationAllocation> {
    AbstractSchedulerPlanFollower planFollower;
    long now;
    Plan plan;

    ReservationAllocationComparator(long now,
        AbstractSchedulerPlanFollower planFollower, Plan plan) {
      this.now = now;
      this.planFollower = planFollower;
      this.plan = plan;
    }

    private Resource getUnallocatedReservedResources(
        ReservationAllocation reservation) {
      Resource resResource;
      Resource reservationResource =
          planFollower.getReservationQueueResourceIfExists(plan,
              reservation.getReservationId());
      if (reservationResource != null) {
        resResource = Resources.subtract(reservation.getResourcesAtTime(now),
            reservationResource);
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
