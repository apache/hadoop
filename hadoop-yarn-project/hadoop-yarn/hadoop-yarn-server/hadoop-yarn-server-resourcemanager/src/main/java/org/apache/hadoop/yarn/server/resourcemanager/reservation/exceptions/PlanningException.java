package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;

/**
 * Exception thrown by the admission control subsystem when there is a problem
 * in trying to find an allocation for a user {@link ReservationSubmissionRequest}.
 */
public class PlanningException extends Exception {

  private static final long serialVersionUID = -684069387367879218L;

  public PlanningException(String message) {
    super(message);
  }

  public PlanningException(Throwable cause) {
    super(cause);
  }

  public PlanningException(String message, Throwable cause) {
    super(message, cause);
  }

}
