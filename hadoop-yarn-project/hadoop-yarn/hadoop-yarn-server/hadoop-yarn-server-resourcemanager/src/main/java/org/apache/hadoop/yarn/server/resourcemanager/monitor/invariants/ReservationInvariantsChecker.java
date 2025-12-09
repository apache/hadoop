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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants;

import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.util.UTCClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Invariant checker that checks certain reservation invariants are respected.
 */
public class ReservationInvariantsChecker extends InvariantsChecker {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReservationInvariantsChecker.class);

  private UTCClock clock = new UTCClock();

  @Override
  public void editSchedule() {
    Collection<Plan> plans =
        getContext().getReservationSystem().getAllPlans().values();

    try {
      for (Plan plan : plans) {
        long currReservations =
            plan.getReservationsAtTime(clock.getTime()).size();
        long numberReservationQueues = getContext().getScheduler()
            .getQueueInfo(plan.getQueueName(), true, false).getChildQueues()
            .size();
        if (currReservations != numberReservationQueues - 1) {
          logOrThrow("Number of reservations (" + currReservations
              + ") does NOT match the number of reservationQueues ("
              + (numberReservationQueues - 1) + "), while it should.");
        }
      }
    } catch (IOException io) {
      throw new InvariantViolationException("Issue during invariant check: ",
          io);
    }

  }

}
