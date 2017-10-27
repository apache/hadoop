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
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
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
   *
   * @param clock the {@link Clock} to use
   */
  public ReservationInputValidator(Clock clock) {
    this.clock = clock;
  }

  private Plan validateReservation(ReservationSystem reservationSystem,
      ReservationId reservationId, String auditConstant) throws YarnException {
    // check if the reservation id is valid
    if (reservationId == null) {
      String message = "Missing reservation id."
          + " Please try again by specifying a reservation id.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    String queue = reservationSystem.getQueueForReservation(reservationId);
    String nullQueueErrorMessage =
        "The specified reservation with ID: " + reservationId
            + " is unknown. Please try again with a valid reservation.";
    String nullPlanErrorMessage = "The specified reservation: " + reservationId
        + " is not associated with any valid plan."
        + " Please try again with a valid reservation.";
    return getPlanFromQueue(reservationSystem, queue, auditConstant,
        nullQueueErrorMessage, nullPlanErrorMessage);
  }

  private void validateReservationDefinition(ReservationId reservationId,
      ReservationDefinition contract, Plan plan, String auditConstant)
      throws YarnException {
    String message = "";
    // check if deadline is in the past
    if (contract == null) {
      message = "Missing reservation definition."
          + " Please try again by specifying a reservation definition.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    if (contract.getDeadline() <= clock.getTime()) {
      message = "The specified deadline: " + contract.getDeadline()
          + " is the past. Please try again with deadline in the future.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // Check if at least one RR has been specified
    ReservationRequests resReqs = contract.getReservationRequests();
    if (resReqs == null) {
      message = "No resources have been specified to reserve."
          + "Please try again by specifying the resources to reserve.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    List<ReservationRequest> resReq = resReqs.getReservationResources();
    if (resReq == null || resReq.isEmpty()) {
      message = "No resources have been specified to reserve."
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
      maxGangSize = Resources.max(plan.getResourceCalculator(),
          plan.getTotalCapacity(), maxGangSize,
          Resources.multiply(rr.getCapability(), rr.getConcurrency()));
    }
    // verify the allocation is possible (skip for ANY)
    long duration = contract.getDeadline() - contract.getArrival();
    if (duration < minDuration && type != ReservationRequestInterpreter.R_ANY) {
      message = "The time difference (" + (duration) + ") between arrival ("
          + contract.getArrival() + ") " + "and deadline ("
          + contract.getDeadline() + ") must "
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
      message = "The size of the largest gang in the reservation definition ("
          + maxGangSize + ") exceed the capacity available ("
          + plan.getTotalCapacity() + " )";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // check that the recurrence is a positive long value.
    String recurrenceExpression = contract.getRecurrenceExpression();
    try {
      long recurrence = Long.parseLong(recurrenceExpression);
      if (recurrence < 0) {
        message = "Negative Period : " + recurrenceExpression + ". Please try"
            + " again with a non-negative long value as period.";
        throw RPCUtil.getRemoteException(message);
      }
      // verify duration is less than recurrence for periodic reservations
      if (recurrence > 0 && duration > recurrence) {
        message = "Duration of the requested reservation: " + duration
            + " is greater than the recurrence: " + recurrence
            + ". Please try again with a smaller duration.";
        throw RPCUtil.getRemoteException(message);
      }
      // verify maximum period is divisible by recurrence expression.
      if (recurrence > 0 && plan.getMaximumPeriodicity() % recurrence != 0) {
        message = "The maximum periodicity: " + plan.getMaximumPeriodicity() +
            " must be divisible by the recurrence expression provided: " +
            recurrence + ". Please try again with a recurrence expression" +
            " that satisfies this requirement.";
        throw RPCUtil.getRemoteException(message);
      }
    } catch (NumberFormatException e) {
      message = "Invalid period " + recurrenceExpression + ". Please try"
          + " again with a non-negative long value as period.";
      throw RPCUtil.getRemoteException(message);
    }
  }

  private Plan getPlanFromQueue(ReservationSystem reservationSystem,
      String queue, String auditConstant) throws YarnException {
    String nullQueueErrorMessage = "The queue is not specified."
        + " Please try again with a valid reservable queue.";
    String nullPlanErrorMessage = "The specified queue: " + queue
        + " is not managed by reservation system."
        + " Please try again with a valid reservable queue.";
    return getPlanFromQueue(reservationSystem, queue, auditConstant,
        nullQueueErrorMessage, nullPlanErrorMessage);
  }

  private Plan getPlanFromQueue(ReservationSystem reservationSystem,
      String queue, String auditConstant, String nullQueueErrorMessage,
      String nullPlanErrorMessage) throws YarnException {
    if (queue == null || queue.isEmpty()) {
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService",
          nullQueueErrorMessage);
      throw RPCUtil.getRemoteException(nullQueueErrorMessage);
    }
    // check if the associated plan is valid
    Plan plan = reservationSystem.getPlan(queue);
    if (plan == null) {
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService",
          nullPlanErrorMessage);
      throw RPCUtil.getRemoteException(nullPlanErrorMessage);
    }
    return plan;
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
   * @throws YarnException if validation fails
   */
  public Plan validateReservationSubmissionRequest(
      ReservationSystem reservationSystem, ReservationSubmissionRequest request,
      ReservationId reservationId) throws YarnException {
    String message;
    if (reservationId == null) {
      message = "Reservation id cannot be null. Please try again specifying "
          + " a valid reservation id by creating a new reservation id.";
      throw RPCUtil.getRemoteException(message);
    }
    // Check if it is a managed queue
    String queue = request.getQueue();
    Plan plan = getPlanFromQueue(reservationSystem, queue,
        AuditConstants.SUBMIT_RESERVATION_REQUEST);

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
   * @throws YarnException if validation fails
   */
  public Plan validateReservationUpdateRequest(
      ReservationSystem reservationSystem, ReservationUpdateRequest request)
      throws YarnException {
    ReservationId reservationId = request.getReservationId();
    Plan plan = validateReservation(reservationSystem, reservationId,
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
   * details of any validation check failures.
   *
   * @param reservationSystem the {@link ReservationSystem} to validate against
   * @param request the {@link ReservationListRequest} defining search
   *          parameters for reservations in the {@link ReservationSystem} that
   *          is being validated against.
   * @return the {@link Plan} to list reservations of.
   * @throws YarnException if validation fails
   */
  public Plan validateReservationListRequest(
      ReservationSystem reservationSystem, ReservationListRequest request)
      throws YarnException {
    String queue = request.getQueue();
    if (request.getEndTime() < request.getStartTime()) {
      String errorMessage = "The specified end time must be greater than "
          + "the specified start time.";
      RMAuditLogger.logFailure("UNKNOWN",
          AuditConstants.LIST_RESERVATION_REQUEST,
          "validate list reservation input", "ClientRMService", errorMessage);
      throw RPCUtil.getRemoteException(errorMessage);
    }
    // Check if it is a managed queue
    return getPlanFromQueue(reservationSystem, queue,
        AuditConstants.LIST_RESERVATION_REQUEST);
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
   * @throws YarnException if validation fails
   */
  public Plan validateReservationDeleteRequest(
      ReservationSystem reservationSystem, ReservationDeleteRequest request)
      throws YarnException {
    return validateReservation(reservationSystem, request.getReservationId(),
        AuditConstants.DELETE_RESERVATION_REQUEST);
  }
}
