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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * This (re)planner scan a period of time from now to a maximum time window (or
 * the end of the last session, whichever comes first) checking the overall
 * capacity is not violated.
 * 
 * It greedily removes sessions in reversed order of acceptance (latest accepted
 * is the first removed).
 */
public class SimpleCapacityReplanner implements Planner {

  private static final Log LOG = LogFactory
      .getLog(SimpleCapacityReplanner.class);

  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  private final Clock clock;

  // this allows to control to time-span of this replanning
  // far into the future time instants might be worth replanning for
  // later on
  private long lengthOfCheckZone;

  public SimpleCapacityReplanner() {
    this(new UTCClock());
  }

  @VisibleForTesting
  SimpleCapacityReplanner(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void init(String planQueueName,
      ReservationSchedulerConfiguration conf) {
    this.lengthOfCheckZone = conf.getEnforcementWindow(planQueueName);
  }

  @Override
  public void plan(Plan plan, List<ReservationDefinition> contracts)
      throws PlanningException {

    if (contracts != null) {
      throw new RuntimeException(
          "SimpleCapacityReplanner cannot handle new reservation contracts");
    }

    ResourceCalculator resCalc = plan.getResourceCalculator();
    Resource totCap = plan.getTotalCapacity();
    long now = clock.getTime();

    // loop on all moment in time from now to the end of the check Zone
    // or the end of the planned sessions whichever comes first
    for (long t = now; (t < plan.getLastEndTime() && t < (now + lengthOfCheckZone)); t +=
        plan.getStep()) {
      Resource excessCap =
          Resources.subtract(plan.getTotalCommittedResources(t), totCap);
      // if we are violating
      if (Resources.greaterThan(resCalc, totCap, excessCap, ZERO_RESOURCE)) {
        // sorted on reverse order of acceptance, so newest reservations first
        Set<ReservationAllocation> curReservations =
            new TreeSet<ReservationAllocation>(plan.getReservationsAtTime(t));
        for (Iterator<ReservationAllocation> resIter =
            curReservations.iterator(); resIter.hasNext()
            && Resources.greaterThan(resCalc, totCap, excessCap, ZERO_RESOURCE);) {
          ReservationAllocation reservation = resIter.next();
          plan.deleteReservation(reservation.getReservationId());
          excessCap =
              Resources.subtract(excessCap, reservation.getResourcesAtTime(t));
          LOG.info("Removing reservation " + reservation.getReservationId()
              + " to repair physical-resource constraints in the plan: "
              + plan.getQueueName());
        }
      }
    }
  }
}
