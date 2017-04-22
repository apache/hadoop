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
package org.apache.hadoop.yarn.sls;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple support class, used to create reservation requests.
 */
public final class ReservationClientUtil {

  private ReservationClientUtil(){
    //avoid instantiation
  }

  /**
   * Creates a request that envelopes a MR jobs, picking max number of maps and
   * reducers, max durations, and max resources per container.
   *
   * @param reservationId the id of the reservation
   * @param name the name of a reservation
   * @param maxMapRes maximum resources used by any mapper
   * @param numberMaps number of mappers
   * @param maxMapDur maximum duration of any mapper
   * @param maxRedRes maximum resources used by any reducer
   * @param numberReduces number of reducers
   * @param maxRedDur maximum duration of any reducer
   * @param arrival start time of valid range for reservation
   * @param deadline deadline for this reservation
   * @param queueName queue to submit to
   * @return a submission request
   */
  @SuppressWarnings("checkstyle:parameternumber")
  public static ReservationSubmissionRequest createMRReservation(
      ReservationId reservationId, String name, Resource maxMapRes,
      int numberMaps, long maxMapDur, Resource maxRedRes, int numberReduces,
      long maxRedDur, long arrival, long deadline, String queueName) {

    ReservationRequest mapRR = ReservationRequest.newInstance(maxMapRes,
        numberMaps, numberMaps, maxMapDur);
    ReservationRequest redRR = ReservationRequest.newInstance(maxRedRes,
        numberReduces, numberReduces, maxRedDur);

    List<ReservationRequest> listResReq = new ArrayList<ReservationRequest>();
    listResReq.add(mapRR);
    listResReq.add(redRR);

    ReservationRequests reservationRequests = ReservationRequests
        .newInstance(listResReq, ReservationRequestInterpreter.R_ORDER_NO_GAP);
    ReservationDefinition resDef = ReservationDefinition.newInstance(arrival,
        deadline, reservationRequests, name);

    // outermost request
    ReservationSubmissionRequest request = ReservationSubmissionRequest
        .newInstance(resDef, queueName, reservationId);

    return request;
  }
}
