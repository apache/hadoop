package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Exception thrown when an update to an existing reservation is performed
 * by a user that is not the reservation owner. 
 */
@Public
@Unstable
public class MismatchedUserException extends PlanningException {

  private static final long serialVersionUID = 8313222590561668413L;

  public MismatchedUserException(String message) {
    super(message);
  }

  public MismatchedUserException(Throwable cause) {
    super(cause);
  }

  public MismatchedUserException(String message, Throwable cause) {
    super(message, cause);
  }

}