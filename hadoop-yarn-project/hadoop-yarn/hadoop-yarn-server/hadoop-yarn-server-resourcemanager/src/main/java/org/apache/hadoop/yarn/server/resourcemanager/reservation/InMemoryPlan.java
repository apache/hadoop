/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents an in memory representation of the state of our
 * reservation system, and provides accelerated access to both individual
 * reservations and aggregate utilization of resources over time.
 */
public class InMemoryPlan implements Plan {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryPlan.class);

  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);
  private final RMStateStore rmStateStore;

  private TreeMap<ReservationInterval, Set<InMemoryReservationAllocation>> currentReservations =
      new TreeMap<ReservationInterval, Set<InMemoryReservationAllocation>>();

  private RLESparseResourceAllocation rleSparseVector;

  private PeriodicRLESparseResourceAllocation periodicRle;

  private Map<String, RLESparseResourceAllocation> userResourceAlloc =
      new HashMap<String, RLESparseResourceAllocation>();

  private Map<String, RLESparseResourceAllocation> userPeriodicResourceAlloc =
      new HashMap<String, RLESparseResourceAllocation>();

  private Map<String, RLESparseResourceAllocation> userActiveReservationCount =
      new HashMap<String, RLESparseResourceAllocation>();

  private Map<ReservationId, InMemoryReservationAllocation> reservationTable =
      new HashMap<ReservationId, InMemoryReservationAllocation>();

  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();
  private final SharingPolicy policy;
  private final ReservationAgent agent;
  private final long step;
  private final ResourceCalculator resCalc;
  private final Resource minAlloc, maxAlloc;
  private final String queueName;
  private final QueueMetrics queueMetrics;
  private final Planner replanner;
  private final boolean getMoveOnExpiry;
  private final Clock clock;
  private final long maxPeriodicity;

  private Resource totalCapacity;

  public InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry,
      RMContext rmContext) {
    this(queueMetrics, policy, agent, totalCapacity, step, resCalc, minAlloc,
        maxAlloc, queueName, replanner, getMoveOnExpiry,
        YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY,
        rmContext);
  }

  public InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry,
      long maxPeriodicity, RMContext rmContext) {
    this(queueMetrics, policy, agent, totalCapacity, step, resCalc, minAlloc,
        maxAlloc, queueName, replanner, getMoveOnExpiry, maxPeriodicity,
        rmContext, new UTCClock());
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry,
      long maxPeriodicty, RMContext rmContext, Clock clock) {
    this.queueMetrics = queueMetrics;
    this.policy = policy;
    this.agent = agent;
    this.step = step;
    this.totalCapacity = totalCapacity;
    this.resCalc = resCalc;
    this.minAlloc = minAlloc;
    this.maxAlloc = maxAlloc;
    this.rleSparseVector = new RLESparseResourceAllocation(resCalc);
    this.maxPeriodicity = maxPeriodicty;
    this.periodicRle =
        new PeriodicRLESparseResourceAllocation(resCalc, this.maxPeriodicity);
    this.queueName = queueName;
    this.replanner = replanner;
    this.getMoveOnExpiry = getMoveOnExpiry;
    this.clock = clock;
    this.rmStateStore = rmContext.getStateStore();
  }

  @Override
  public QueueMetrics getQueueMetrics() {
    return queueMetrics;
  }

  private RLESparseResourceAllocation getUserRLEResourceAllocation(String user,
      long period) {
    RLESparseResourceAllocation resAlloc = null;
    if (period > 0) {
      if (userPeriodicResourceAlloc.containsKey(user)) {
        resAlloc = userPeriodicResourceAlloc.get(user);
      } else {
        resAlloc = new PeriodicRLESparseResourceAllocation(resCalc,
            periodicRle.getTimePeriod());
        userPeriodicResourceAlloc.put(user, resAlloc);
      }
    } else {
      if (userResourceAlloc.containsKey(user)) {
        resAlloc = userResourceAlloc.get(user);
      } else {
        resAlloc = new RLESparseResourceAllocation(resCalc);
        userResourceAlloc.put(user, resAlloc);
      }
    }
    return resAlloc;
  }

  private void gcUserRLEResourceAllocation(String user, long period) {
    if (period > 0) {
      if (userPeriodicResourceAlloc.get(user).isEmpty()) {
        userPeriodicResourceAlloc.remove(user);
      }
    } else {
      if (userResourceAlloc.get(user).isEmpty()) {
        userResourceAlloc.remove(user);
      }
    }
  }

  private void incrementAllocation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    Map<ReservationInterval, Resource> allocationRequests =
        reservation.getAllocationRequests();
    // check if we have encountered the user earlier and if not add an entry
    String user = reservation.getUser();
    long period = reservation.getPeriodicity();
    RLESparseResourceAllocation resAlloc =
        getUserRLEResourceAllocation(user, period);

    RLESparseResourceAllocation resCount = userActiveReservationCount.get(user);
    if (resCount == null) {
      resCount = new RLESparseResourceAllocation(resCalc);
      userActiveReservationCount.put(user, resCount);
    }

    long earliestActive = Long.MAX_VALUE;
    long latestActive = Long.MIN_VALUE;

    for (Map.Entry<ReservationInterval, Resource> r : allocationRequests
        .entrySet()) {

      if (period > 0L) {
        for (int i = 0; i < periodicRle.getTimePeriod() / period; i++) {

          long rStart = r.getKey().getStartTime() + i * period;
          long rEnd = r.getKey().getEndTime() + i * period;

          // handle wrap-around
          if (rEnd > periodicRle.getTimePeriod()) {
            long diff = rEnd - periodicRle.getTimePeriod();
            rEnd = periodicRle.getTimePeriod();
            ReservationInterval newInterval = new ReservationInterval(0, diff);
            periodicRle.addInterval(newInterval, r.getValue());
            resAlloc.addInterval(newInterval, r.getValue());
          }

          ReservationInterval newInterval =
              new ReservationInterval(rStart, rEnd);
          periodicRle.addInterval(newInterval, r.getValue());
          resAlloc.addInterval(newInterval, r.getValue());
        }

      } else {
        rleSparseVector.addInterval(r.getKey(), r.getValue());
        resAlloc.addInterval(r.getKey(), r.getValue());
        if (Resources.greaterThan(resCalc, totalCapacity, r.getValue(),
            ZERO_RESOURCE)) {
          earliestActive = Math.min(earliestActive, r.getKey().getStartTime());
          latestActive = Math.max(latestActive, r.getKey().getEndTime());
        }
      }
    }
    // periodic reservations are active from start time and good till cancelled
    if (period > 0L) {
      earliestActive = reservation.getStartTime();
      latestActive = Long.MAX_VALUE;
    }
    resCount.addInterval(new ReservationInterval(earliestActive, latestActive),
        Resource.newInstance(1, 1));
  }

  private void decrementAllocation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    Map<ReservationInterval, Resource> allocationRequests =
        reservation.getAllocationRequests();
    String user = reservation.getUser();
    long period = reservation.getPeriodicity();
    RLESparseResourceAllocation resAlloc =
        getUserRLEResourceAllocation(user, period);

    long earliestActive = Long.MAX_VALUE;
    long latestActive = Long.MIN_VALUE;
    for (Map.Entry<ReservationInterval, Resource> r : allocationRequests
        .entrySet()) {
      if (period > 0L) {
        for (int i = 0; i < periodicRle.getTimePeriod() / period; i++) {

          long rStart = r.getKey().getStartTime() + i * period;
          long rEnd = r.getKey().getEndTime() + i * period;

          // handle wrap-around
          if (rEnd > periodicRle.getTimePeriod()) {
            long diff = rEnd - periodicRle.getTimePeriod();
            rEnd = periodicRle.getTimePeriod();
            ReservationInterval newInterval = new ReservationInterval(0, diff);
            periodicRle.removeInterval(newInterval, r.getValue());
            resAlloc.removeInterval(newInterval, r.getValue());
          }

          ReservationInterval newInterval =
              new ReservationInterval(rStart, rEnd);
          periodicRle.removeInterval(newInterval, r.getValue());
          resAlloc.removeInterval(newInterval, r.getValue());
        }
      } else {
        rleSparseVector.removeInterval(r.getKey(), r.getValue());
        resAlloc.removeInterval(r.getKey(), r.getValue());
        if (Resources.greaterThan(resCalc, totalCapacity, r.getValue(),
            ZERO_RESOURCE)) {
          earliestActive = Math.min(earliestActive, r.getKey().getStartTime());
          latestActive = Math.max(latestActive, r.getKey().getEndTime());
        }
      }
    }
    gcUserRLEResourceAllocation(user, period);

    RLESparseResourceAllocation resCount = userActiveReservationCount.get(user);
    // periodic reservations are active from start time and good till cancelled
    if (period > 0L) {
      earliestActive = reservation.getStartTime();
      latestActive = Long.MAX_VALUE;
    }
    resCount.removeInterval(
        new ReservationInterval(earliestActive, latestActive),
        Resource.newInstance(1, 1));
    if (resCount.isEmpty()) {
      userActiveReservationCount.remove(user);
    }
  }

  public Set<ReservationAllocation> getAllReservations() {
    readLock.lock();
    try {
      if (currentReservations != null) {
        Set<ReservationAllocation> flattenedReservations =
            new TreeSet<ReservationAllocation>();
        for (Set<InMemoryReservationAllocation> res : currentReservations
            .values()) {
          flattenedReservations.addAll(res);
        }
        return flattenedReservations;
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean addReservation(ReservationAllocation reservation,
      boolean isRecovering) throws PlanningException {
    // Verify the allocation is memory based otherwise it is not supported
    InMemoryReservationAllocation inMemReservation =
        (InMemoryReservationAllocation) reservation;
    if (inMemReservation.getUser() == null) {
      String errMsg = "The specified Reservation with ID "
          + inMemReservation.getReservationId() + " is not mapped to any user";
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    writeLock.lock();
    try {
      if (reservationTable.containsKey(inMemReservation.getReservationId())) {
        String errMsg = "The specified Reservation with ID "
            + inMemReservation.getReservationId() + " already exists";
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      // Validate if we can accept this reservation, throws exception if
      // validation fails
      if (!isRecovering) {
        policy.validate(this, inMemReservation);
        // we record here the time in which the allocation has been accepted
        reservation.setAcceptanceTimestamp(clock.getTime());
        if (rmStateStore != null) {
          rmStateStore.storeNewReservation(
              ReservationSystemUtil.buildStateProto(inMemReservation),
              getQueueName(), inMemReservation.getReservationId().toString());
        }
      }
      ReservationInterval searchInterval = new ReservationInterval(
          inMemReservation.getStartTime(), inMemReservation.getEndTime());
      Set<InMemoryReservationAllocation> reservations =
          currentReservations.get(searchInterval);
      if (reservations == null) {
        reservations = new HashSet<InMemoryReservationAllocation>();
      }
      if (!reservations.add(inMemReservation)) {
        LOG.error("Unable to add reservation: {} to plan.",
            inMemReservation.getReservationId());
        return false;
      }
      currentReservations.put(searchInterval, reservations);
      reservationTable.put(inMemReservation.getReservationId(),
          inMemReservation);
      incrementAllocation(inMemReservation);
      LOG.info("Successfully added reservation: {} to plan.",
          inMemReservation.getReservationId());
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean updateReservation(ReservationAllocation reservation)
      throws PlanningException {
    writeLock.lock();
    boolean result = false;
    try {
      ReservationId resId = reservation.getReservationId();
      ReservationAllocation currReservation = getReservationById(resId);
      if (currReservation == null) {
        String errMsg = "The specified Reservation with ID " + resId
            + " does not exist in the plan";
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      // validate if we can accept this reservation, throws exception if
      // validation fails
      policy.validate(this, reservation);
      if (!removeReservation(currReservation)) {
        LOG.error("Unable to replace reservation: {} from plan.",
            reservation.getReservationId());
        return result;
      }
      try {
        result = addReservation(reservation, false);
      } catch (PlanningException e) {
        LOG.error("Unable to update reservation: {} from plan due to {}.",
            reservation.getReservationId(), e.getMessage());
      }
      if (result) {
        LOG.info("Successfully updated reservation: {} in plan.",
            reservation.getReservationId());
        return result;
      } else {
        // rollback delete
        addReservation(currReservation, false);
        LOG.info("Rollbacked update reservation: {} from plan.",
            reservation.getReservationId());
        return result;
      }
    } finally {
      writeLock.unlock();
    }
  }

  private boolean removeReservation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    ReservationInterval searchInterval = new ReservationInterval(
        reservation.getStartTime(), reservation.getEndTime());
    Set<InMemoryReservationAllocation> reservations =
        currentReservations.get(searchInterval);
    if (reservations != null) {
      if (rmStateStore != null) {
        rmStateStore.removeReservation(getQueueName(),
            reservation.getReservationId().toString());
      }
      if (!reservations.remove(reservation)) {
        LOG.error("Unable to remove reservation: {} from plan.",
            reservation.getReservationId());
        return false;
      }
      if (reservations.isEmpty()) {
        currentReservations.remove(searchInterval);
      }
    } else {
      String errMsg = "The specified Reservation with ID "
          + reservation.getReservationId() + " does not exist in the plan";
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    reservationTable.remove(reservation.getReservationId());
    decrementAllocation(reservation);
    LOG.info("Sucessfully deleted reservation: {} in plan.",
        reservation.getReservationId());
    return true;
  }

  @Override
  public boolean deleteReservation(ReservationId reservationID) {
    writeLock.lock();
    try {
      ReservationAllocation reservation = getReservationById(reservationID);
      if (reservation == null) {
        String errMsg = "The specified Reservation with ID " + reservationID
            + " does not exist in the plan";
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      return removeReservation(reservation);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void archiveCompletedReservations(long tick) {
    // Since we are looking for old reservations, read lock is optimal
    LOG.debug("Running archival at time: {}", tick);
    List<InMemoryReservationAllocation> expiredReservations =
        new ArrayList<InMemoryReservationAllocation>();
    readLock.lock();
    // archive reservations and delete the ones which are beyond
    // the reservation policy "window"
    try {
      long archivalTime = tick - policy.getValidWindow();
      ReservationInterval searchInterval =
          new ReservationInterval(archivalTime, archivalTime);
      SortedMap<ReservationInterval, Set<InMemoryReservationAllocation>> reservations =
          currentReservations.headMap(searchInterval, true);
      if (!reservations.isEmpty()) {
        for (Set<InMemoryReservationAllocation> reservationEntries : reservations
            .values()) {
          for (InMemoryReservationAllocation reservation : reservationEntries) {
            if (reservation.getEndTime() <= archivalTime) {
              expiredReservations.add(reservation);
            }
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    if (expiredReservations.isEmpty()) {
      return;
    }
    // Need write lock only if there are any reservations to be deleted
    writeLock.lock();
    try {
      for (InMemoryReservationAllocation expiredReservation : expiredReservations) {
        removeReservation(expiredReservation);
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Set<ReservationAllocation> getReservationsAtTime(long tick) {
    return getReservations(null, new ReservationInterval(tick, tick), "");
  }

  @Override
  public long getStep() {
    return step;
  }

  @Override
  public SharingPolicy getSharingPolicy() {
    return policy;
  }

  @Override
  public ReservationAgent getReservationAgent() {
    return agent;
  }

  @Override
  public RLESparseResourceAllocation getReservationCountForUserOverTime(
      String user, long start, long end) {
    readLock.lock();
    try {
      RLESparseResourceAllocation userResAlloc =
          userActiveReservationCount.get(user);

      if (userResAlloc != null) {
        return userResAlloc.getRangeOverlapping(start, end);
      } else {
        return new RLESparseResourceAllocation(resCalc);
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public RLESparseResourceAllocation getConsumptionForUserOverTime(String user,
      long start, long end) {
    readLock.lock();
    try {
      // merge periodic and non-periodic allocations
      RLESparseResourceAllocation userResAlloc = userResourceAlloc.get(user);
      RLESparseResourceAllocation userPeriodicResAlloc =
          userPeriodicResourceAlloc.get(user);

      if (userResAlloc != null && userPeriodicResAlloc != null) {
        return RLESparseResourceAllocation.merge(resCalc, totalCapacity,
            userResAlloc, userPeriodicResAlloc, RLEOperator.add, start, end);
      }
      if (userResAlloc != null) {
        return userResAlloc.getRangeOverlapping(start, end);
      }
      if (userPeriodicResAlloc != null) {
        return userPeriodicResAlloc.getRangeOverlapping(start, end);
      }
    } catch (PlanningException e) {
      LOG.warn("Exception while trying to merge periodic"
          + " and non-periodic user allocations: {}", e.getMessage(), e);
    } finally {
      readLock.unlock();
    }
    return new RLESparseResourceAllocation(resCalc);
  }

  @Override
  public Resource getTotalCommittedResources(long t) {
    readLock.lock();
    try {
      return Resources.add(rleSparseVector.getCapacityAtTime(t),
          periodicRle.getCapacityAtTime(t));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Set<ReservationAllocation> getReservations(ReservationId reservationID,
      ReservationInterval interval) {
    return getReservations(reservationID, interval, null);
  }

  @Override
  public Set<ReservationAllocation> getReservations(ReservationId reservationID,
      ReservationInterval interval, String user) {
    if (reservationID != null) {
      ReservationAllocation allocation = getReservationById(reservationID);
      if (allocation == null) {
        return Collections.emptySet();
      }
      return Collections.singleton(allocation);
    }

    long startTime = interval == null ? 0 : interval.getStartTime();
    long endTime = interval == null ? Long.MAX_VALUE : interval.getEndTime();

    ReservationInterval searchInterval =
        new ReservationInterval(endTime, Long.MAX_VALUE);
    readLock.lock();
    try {
      SortedMap<ReservationInterval, Set<InMemoryReservationAllocation>> res =
          currentReservations.headMap(searchInterval, true);
      if (!res.isEmpty()) {
        Set<ReservationAllocation> flattenedReservations = new HashSet<>();
        for (Set<InMemoryReservationAllocation> resEntries : res.values()) {
          for (InMemoryReservationAllocation reservation : resEntries) {
            // validate user
            if (user != null && !user.isEmpty()
                && !reservation.getUser().equals(user)) {
              continue;
            }
            // handle periodic reservations
            long period = reservation.getPeriodicity();
            if (period > 0) {
              // The shift is used to remove the wrap around for the
              // reservation interval. The wrap around will still
              // exist for the search interval.
              long shift = reservation.getStartTime() % period;
              // This is the duration of the reservation since
              // duration < period.
              long periodicReservationEnd =
                  (reservation.getEndTime() -shift) % period;
              long periodicSearchStart = (startTime - shift) % period;
              long periodicSearchEnd = (endTime - shift) % period;
              long searchDuration = endTime - startTime;

              // 1. If the searchDuration is greater than the period, then
              // the reservation is within the interval. This will allow
              // us to ignore cases where search end > search start >
              // reservation end.
              // 2/3. If the search end is less than the reservation end, or if
              // the search start is less than the reservation end, then the
              // reservation will be in the reservation since
              // periodic reservation start is always zero. Note that neither
              // of those values will ever be negative.
              // 4. If the search end is less than the search start, then
              // there is a wrap around, and both values are implicitly
              // greater than the reservation end because of condition 2/3,
              // so the reservation is within the search interval.
              if (searchDuration > period
                  || periodicSearchEnd < periodicReservationEnd
                  || periodicSearchStart < periodicReservationEnd
                  || periodicSearchStart > periodicSearchEnd) {
                flattenedReservations.add(reservation);
              }
            } else {
              // check for non-periodic reservations
              if (reservation.getEndTime() > startTime) {
                flattenedReservations.add(reservation);
              }
            }
          }
        }
        return Collections.unmodifiableSet(flattenedReservations);
      } else {
        return Collections.emptySet();
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ReservationAllocation getReservationById(ReservationId reservationID) {
    if (reservationID == null) {
      return null;
    }
    readLock.lock();
    try {
      return reservationTable.get(reservationID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Resource getTotalCapacity() {
    readLock.lock();
    try {
      return Resources.clone(totalCapacity);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public RLESparseResourceAllocation getAvailableResourceOverTime(String user,
      ReservationId oldId, long start, long end, long period)
      throws PlanningException {
    readLock.lock();
    try {

      // for non-periodic return simple available resources
      if (period == 0) {

        // create RLE of totCapacity
        TreeMap<Long, Resource> totAvailable = new TreeMap<Long, Resource>();
        totAvailable.put(start, Resources.clone(totalCapacity));
        RLESparseResourceAllocation totRLEAvail =
            new RLESparseResourceAllocation(totAvailable, resCalc);

        // subtract used from available
        RLESparseResourceAllocation netAvailable;

        netAvailable = RLESparseResourceAllocation.merge(resCalc,
            Resources.clone(totalCapacity), totRLEAvail, rleSparseVector,
            RLEOperator.subtractTestNonNegative, start, end);

        // remove periodic component
        netAvailable = RLESparseResourceAllocation.merge(resCalc,
            Resources.clone(totalCapacity), netAvailable, periodicRle,
            RLEOperator.subtractTestNonNegative, start, end);

        // add back in old reservation used resources if any
        ReservationAllocation old = reservationTable.get(oldId);
        if (old != null) {

          RLESparseResourceAllocation addBackPrevious =
              old.getResourcesOverTime(start, end);
          netAvailable = RLESparseResourceAllocation.merge(resCalc,
              Resources.clone(totalCapacity), netAvailable, addBackPrevious,
              RLEOperator.add, start, end);
        }
        // lower it if this is needed by the sharing policy
        netAvailable = getSharingPolicy().availableResources(netAvailable, this,
            user, oldId, start, end);
        return netAvailable;
      } else {

        if (periodicRle.getTimePeriod() % period != 0) {
          throw new PlanningException("The reservation periodicity (" + period
              + ") must be" + " an exact divider of the system maxPeriod ("
              + periodicRle.getTimePeriod() + ")");
        }

        if (period < (end - start)) {
          throw new PlanningException(
              "Invalid input: (end - start) = (" + end + " - " + start + ") = "
                  + (end - start) + " > period = " + period);
        }

        // find the minimum resources available among all the instances that fit
        // in the LCM
        long numInstInLCM = periodicRle.getTimePeriod() / period;

        RLESparseResourceAllocation minOverLCM =
            getAvailableResourceOverTime(user, oldId, start, end, 0);
        for (int i = 1; i < numInstInLCM; i++) {

          long rStart = start + i * period;
          long rEnd = end + i * period;

          // recursive invocation of non-periodic range (to pick raw-info)
          RLESparseResourceAllocation snapShot =
              getAvailableResourceOverTime(user, oldId, rStart, rEnd, 0);

          // time-align on start
          snapShot.shift(-(i * period));

          // pick the minimum amount of resources in each time interval
          minOverLCM =
              RLESparseResourceAllocation.merge(resCalc, getTotalCapacity(),
                  minOverLCM, snapShot, RLEOperator.min, start, end);

        }

        return minOverLCM;

      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Resource getMinimumAllocation() {
    return Resources.clone(minAlloc);
  }

  @Override
  public void setTotalCapacity(Resource cap) {
    writeLock.lock();
    try {
      totalCapacity = Resources.clone(cap);
    } finally {
      writeLock.unlock();
    }
  }

  public long getEarliestStartTime() {
    readLock.lock();
    try {
      return rleSparseVector.getEarliestStartTime();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getLastEndTime() {
    readLock.lock();
    try {
      return rleSparseVector.getLatestNonNullTime();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return resCalc;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public Resource getMaximumAllocation() {
    return Resources.clone(maxAlloc);
  }

  @Override
  public long getMaximumPeriodicity() {
    return this.maxPeriodicity;
  }

  public String toCumulativeString() {
    readLock.lock();
    try {
      return rleSparseVector.toString() + "\n" + periodicRle.toString();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Planner getReplanner() {
    return replanner;
  }

  @Override
  public boolean getMoveOnExpiry() {
    return getMoveOnExpiry;
  }

  @Override
  public String toString() {
    readLock.lock();
    try {
      StringBuilder planStr = new StringBuilder("In-memory Plan: ");
      planStr.append("Parent Queue: ").append(queueName)
          .append(" Total Capacity: ").append(totalCapacity).append(" Step: ")
          .append(step);
      for (ReservationAllocation reservation : getAllReservations()) {
        planStr.append(reservation);
      }
      return planStr.toString();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Set<ReservationAllocation> getReservationByUserAtTime(String user,
      long t) {
    readLock.lock();
    try {
      Set<ReservationAllocation> resSet = new HashSet<ReservationAllocation>();
      for (ReservationAllocation ra : getReservationsAtTime(t)) {
        String resUser = ra.getUser();
        if (resUser != null && resUser.equals(user)) {
          resSet.add(ra);
        }
      }
      return resSet;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public RLESparseResourceAllocation getCumulativeLoadOverTime(long start,
      long end) throws PlanningException {
    readLock.lock();
    try {

      RLESparseResourceAllocation ret =
          rleSparseVector.getRangeOverlapping(start, end);
      ret = RLESparseResourceAllocation.merge(resCalc, totalCapacity, ret,
          periodicRle.getRangeOverlapping(start, end), RLEOperator.add, start,
          end);

      return ret;
    } finally {
      readLock.unlock();
    }
  }
}
