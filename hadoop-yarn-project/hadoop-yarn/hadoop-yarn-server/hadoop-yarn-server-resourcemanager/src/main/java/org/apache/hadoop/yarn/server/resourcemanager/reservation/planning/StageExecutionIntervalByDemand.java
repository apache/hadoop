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

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.IterativePlanner.StageProvider;

/**
 * An implementation of {@link StageExecutionInterval}, which sets the execution
 * interval of the stage. For ANY and ALL jobs, the interval is
 * [jobArrival,jobDeadline]. For ORDER jobs, the the maximal possible time
 * interval is divided as follows: First, each stage is guaranteed at least its
 * requested duration. Then, the stage receives a fraction of the remaining
 * time. The fraction is calculated as the ratio between the weight (total
 * requested resources) of the stage and the total weight of all remaining
 * stages.
 */

public class StageExecutionIntervalByDemand implements StageExecutionInterval {

  private long step;

  @Override
  public ReservationInterval computeExecutionInterval(Plan plan,
      ReservationDefinition reservation,
      ReservationRequest currentReservationStage, boolean allocateLeft,
      RLESparseResourceAllocation allocations) {

    // Use StageExecutionIntervalUnconstrained to get the maximal interval
    ReservationInterval maxInterval =
        (new StageExecutionIntervalUnconstrained()).computeExecutionInterval(
            plan, reservation, currentReservationStage, allocateLeft,
            allocations);

    ReservationRequestInterpreter jobType =
        reservation.getReservationRequests().getInterpreter();

    // For unconstrained jobs, such as ALL & ANY, we can use the unconstrained
    // version
    if ((jobType != ReservationRequestInterpreter.R_ORDER)
        && (jobType != ReservationRequestInterpreter.R_ORDER_NO_GAP)) {
      return maxInterval;
    }

    // For ORDER and ORDER_NO_GAP, take a sub-interval of maxInterval
    step = plan.getStep();

    double totalWeight = 0.0;
    long totalDuration = 0;

    // Iterate over the stages that haven't been allocated.
    // For allocateLeft == True, we iterate in reverse order, starting from the
    // last
    // stage, until we reach the current stage.
    // For allocateLeft == False, we do the opposite.
    StageProvider stageProvider = new StageProvider(!allocateLeft, reservation);

    while (stageProvider.hasNext()) {
      ReservationRequest rr = stageProvider.next();
      totalWeight += calcWeight(rr);
      totalDuration += getRoundedDuration(rr, step);

      // Stop once we reach current
      if (rr == currentReservationStage) {
        break;
      }
    }

    // Compute the weight of the current stage as compared to remaining ones
    double ratio = calcWeight(currentReservationStage) / totalWeight;

    // Estimate an early start time, such that:
    // 1. Every stage is guaranteed to receive at least its duration
    // 2. The remainder of the window is divided between stages
    // proportionally to its workload (total memory consumption)
    long maxIntervalArrival = maxInterval.getStartTime();
    long maxIntervalDeadline = maxInterval.getEndTime();
    long window = maxIntervalDeadline - maxIntervalArrival;
    long windowRemainder = window - totalDuration;

    if (allocateLeft) {
      long latestEnd =
          (long) (maxIntervalArrival
              + getRoundedDuration(currentReservationStage, step)
              + (windowRemainder * ratio));

      // Realign if necessary (since we did some arithmetic)
      latestEnd = stepRoundDown(latestEnd, step);

      // Return new interval
      return new ReservationInterval(maxIntervalArrival, latestEnd);
    } else {
      long earlyStart =
          (long) (maxIntervalDeadline
              - getRoundedDuration(currentReservationStage, step)
              - (windowRemainder * ratio));

      // Realign if necessary (since we did some arithmetic)
      earlyStart = stepRoundUp(earlyStart, step);

      // Return new interval
      return new ReservationInterval(earlyStart, maxIntervalDeadline);
    }
  }

  // Weight = total memory consumption of stage
  protected double calcWeight(ReservationRequest stage) {
    return (stage.getDuration() * stage.getCapability().getMemorySize())
        * (stage.getNumContainers());
  }

  protected long getRoundedDuration(ReservationRequest stage, Long s) {
    return stepRoundUp(stage.getDuration(), s);
  }

  protected static long stepRoundDown(long t, long s) {
    return (t / s) * s;
  }

  protected static long stepRoundUp(long t, long s) {
    return ((t + s - 1) / s) * s;
  }
}
