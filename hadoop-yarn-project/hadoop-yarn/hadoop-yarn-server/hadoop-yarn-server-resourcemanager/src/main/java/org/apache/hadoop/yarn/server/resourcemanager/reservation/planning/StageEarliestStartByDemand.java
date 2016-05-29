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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.ListIterator;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;

/**
 * Sets the earliest start time of a stage proportional to the job weight. The
 * interval [jobArrival, stageDeadline) is divided as follows. First, each stage
 * is guaranteed at least its requested duration. Then, the stage receives a
 * fraction of the remaining time. The fraction is calculated as the ratio
 * between the weight (total requested resources) of the stage and the total
 * weight of all proceeding stages.
 */

public class StageEarliestStartByDemand implements StageEarliestStart {

  private long step;

  @Override
  public long setEarliestStartTime(Plan plan,
      ReservationDefinition reservation, int index, ReservationRequest current,
      long stageDeadline) {

    step = plan.getStep();

    // If this is the first stage, don't bother with the computation.
    if (index < 1) {
      return reservation.getArrival();
    }

    // Get iterator
    ListIterator<ReservationRequest> li =
        reservation.getReservationRequests().getReservationResources()
            .listIterator(index);
    ReservationRequest rr;

    // Calculate the total weight & total duration
    double totalWeight = calcWeight(current);
    long totalDuration = getRoundedDuration(current, plan);

    while (li.hasPrevious()) {
      rr = li.previous();
      totalWeight += calcWeight(rr);
      totalDuration += getRoundedDuration(rr, plan);
    }

    // Compute the weight of the current stage as compared to remaining ones
    double ratio = calcWeight(current) / totalWeight;

    // Estimate an early start time, such that:
    // 1. Every stage is guaranteed to receive at least its duration
    // 2. The remainder of the window is divided between stages
    // proportionally to its workload (total memory consumption)
    long window = stageDeadline - reservation.getArrival();
    long windowRemainder = window - totalDuration;
    long earlyStart =
        (long) (stageDeadline - getRoundedDuration(current, plan)
            - (windowRemainder * ratio));

    // Realign if necessary (since we did some arithmetic)
    earlyStart = stepRoundUp(earlyStart, step);

    // Return
    return earlyStart;

  }

  // Weight = total memory consumption of stage
  protected double calcWeight(ReservationRequest stage) {
    return (stage.getDuration() * stage.getCapability().getMemorySize())
        * (stage.getNumContainers());
  }

  protected long getRoundedDuration(ReservationRequest stage, Plan plan) {
    return stepRoundUp(stage.getDuration(), step);
  }

  protected static long stepRoundDown(long t, long step) {
    return (t / step) * step;
  }

  protected static long stepRoundUp(long t, long step) {
    return ((t + step - 1) / step) * step;
  }
}
