package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * This exception is thrown if the user quota is exceed while accepting or
 * updating a reservation.
 */
@Public
@Unstable
public class PlanningQuotaException extends PlanningException {

  private static final long serialVersionUID = 8206629288380246166L;

  public PlanningQuotaException(String message) {
    super(message);
  }

  public PlanningQuotaException(Throwable cause) {
    super(cause);
  }

  public PlanningQuotaException(String message, Throwable cause) {
    super(message, cause);
  }

}
