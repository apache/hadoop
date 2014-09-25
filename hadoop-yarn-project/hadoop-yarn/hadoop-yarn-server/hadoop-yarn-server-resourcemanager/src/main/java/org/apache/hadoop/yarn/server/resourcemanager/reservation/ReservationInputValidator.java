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

import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.Resources;

public class ReservationInputValidator {

  private final Clock clock;

  /**
   * Utility class to validate reservation requests.
   */
  public ReservationInputValidator(Clock clock) {
    this.clock = clock;
  }

  private Plan validateReservation(ReservationSystem reservationSystem,
      ReservationId reservationId, String auditConstant) throws YarnException {
    String message = "";
    // check if the reservation id is valid
    if (reservationId == null) {
      message =
          "Missing reservation id."
              + " Please try again by specifying a reservation id.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    String queueName = reservationSystem.getQueueForReservation(reservationId);
    if (queueName == null) {
      message =
          "The specified reservation with ID: " + reservationId
              + " is unknown. Please try again with a valid reservation.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // check if the associated plan is valid
    Plan plan = reservationSystem.getPlan(queueName);
    if (plan == null) {
      message =
          "The specified reservation: " + reservationId
              + " is not associated with any valid plan."
              + " Please try again with a valid reservation.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    return plan;
  }

  private void validateReservationDefinition(ReservationId reservationId,
      ReservationDefinition contract, Plan plan, String auditConstant)
      throws YarnException {
    String message = "";
    // check if deadline is in the past
    if (contract == null) {
      message =
          "Missing reservation definition."
              + " Please try again by specifying a reservation definition.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    if (contract.getDeadline() <= clock.getTime()) {
      message =
          "The specified deadline: " + contract.getDeadline()
              + " is the past. Please try again with deadline in the future.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // Check if at least one RR has been specified
    ReservationRequests resReqs = contract.getReservationRequests();
    if (resReqs == null) {
      message =
          "No resources have been specified to reserve."
              + "Please try again by specifying the resources to reserve.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    List<ReservationRequest> resReq = resReqs.getReservationResources();
    if (resReq == null || resReq.isEmpty()) {
      message =
          "No resources have been specified to reserve."
              + " Please try again by specifying the resources to reserve.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // compute minimum duration and max gang size
    long minDuration = 0;
    Resource maxGangSize = Resource.newInstance(0, 0);
    ReservationRequestInterpreter type =
        contract.getReservationRequests().getInterpreter();
    for (ReservationRequest rr : resReq) {
      if (type == ReservationRequestInterpreter.R_ALL
          || type == ReservationRequestInterpreter.R_ANY) {
        minDuration = Math.max(minDuration, rr.getDuration());
      } else {
        minDuration += rr.getDuration();
      }
      maxGangSize =
          Resources.max(plan.getResourceCalculator(), plan.getTotalCapacity(),
              maxGangSize,
              Resources.multiply(rr.getCapability(), rr.getConcurrency()));
    }
    // verify the allocation is possible (skip for ANY)
    if (contract.getDeadline() - contract.getArrival() < minDuration
        && type != ReservationRequestInterpreter.R_ANY) {
      message =
          "The time difference ("
              + (contract.getDeadline() - contract.getArrival())
              + ") between arrival (" + contract.getArrival() + ") "
              + "and deadline (" + contract.getDeadline() + ") must "
              + " be greater or equal to the minimum resource duration ("
              + minDuration + ")";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // check that the largest gang does not exceed the inventory available
    // capacity (skip for ANY)
    if (Resources.greaterThan(plan.getResourceCalculator(),
        plan.getTotalCapacity(), maxGangSize, plan.getTotalCapacity())
        && type != ReservationRequestInterpreter.R_ANY) {
      message =
          "The size of the largest gang in the reservation refinition ("
              + maxGangSize + ") exceed the capacity available ("
              + plan.getTotalCapacity() + " )";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast) the input and returns the appropriate {@link Plan} associated with
   * the specified {@link Queue} or throws an exception message illustrating the
   * details of any validation check failures
   * 
   * @param reservationSystem the {@link ReservationSystem} to validate against
   * @param request the {@link ReservationSubmissionRequest} defining the
   *          resources required over time for the request
   * @param reservationId the {@link ReservationId} associated with the current
   *          request
   * @return the {@link Plan} to submit the request to
   * @throws YarnException
   */
  public Plan validateReservationSubmissionRequest(
      ReservationSystem reservationSystem,
      ReservationSubmissionRequest request, ReservationId reservationId)
      throws YarnException {
    // Check if it is a managed queue
    String queueName = request.getQueue();
    if (queueName == null || queueName.isEmpty()) {
      String errMsg =
          "The queue to submit is not specified."
              + " Please try again with a valid reservable queue.";
      RMAuditLogger.logFailure("UNKNOWN",
          AuditConstants.SUBMIT_RESERVATION_REQUEST,
          "validate reservation input", "ClientRMService", errMsg);
      throw RPCUtil.getRemoteException(errMsg);
    }
    Plan plan = reservationSystem.getPlan(queueName);
    if (plan == null) {
      String errMsg =
          "The specified queue: " + queueName
              + " is not managed by reservation system."
              + " Please try again with a valid reservable queue.";
      RMAuditLogger.logFailure("UNKNOWN",
          AuditConstants.SUBMIT_RESERVATION_REQUEST,
          "validate reservation input", "ClientRMService", errMsg);
      throw RPCUtil.getRemoteException(errMsg);
    }
    validateReservationDefinition(reservationId,
        request.getReservationDefinition(), plan,
        AuditConstants.SUBMIT_RESERVATION_REQUEST);
    return plan;
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast) the input and returns the appropriate {@link Plan} associated with
   * the specified {@link Queue} or throws an exception message illustrating the
   * details of any validation check failures
   * 
   * @param reservationSystem the {@link ReservationSystem} to validate against
   * @param request the {@link ReservationUpdateRequest} defining the resources
   *          required over time for the request
   * @return the {@link Plan} to submit the request to
   * @throws YarnException
   */
  public Plan validateReservationUpdateRequest(
      ReservationSystem reservationSystem, ReservationUpdateRequest request)
      throws YarnException {
    ReservationId reservationId = request.getReservationId();
    Plan plan =
        validateReservation(reservationSystem, reservationId,
            AuditConstants.UPDATE_RESERVATION_REQUEST);
    validateReservationDefinition(reservationId,
        request.getReservationDefinition(), plan,
        AuditConstants.UPDATE_RESERVATION_REQUEST);
    return plan;
  }

  /**
   * Quick validation on the input to check some obvious fail conditions (fail
   * fast) the input and returns the appropriate {@link Plan} associated with
   * the specified {@link Queue} or throws an exception message illustrating the
   * details of any validation check failures
   * 
   * @param reservationSystem the {@link ReservationSystem} to validate against
   * @param request the {@link ReservationDeleteRequest} defining the resources
   *          required over time for the request
   * @return the {@link Plan} to submit the request to
   * @throws YarnException
   */
  public Plan validateReservationDeleteRequest(
      ReservationSystem reservationSystem, ReservationDeleteRequest request)
      throws YarnException {
    return validateReservation(reservationSystem, request.getReservationId(),
        AuditConstants.DELETE_RESERVATION_REQUEST);
  }

}
