package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * This exception is thrown if the request made is not syntactically valid.
 */
@Public
@Unstable
public class ContractValidationException extends PlanningException {

  private static final long serialVersionUID = 1L;

  public ContractValidationException(String message) {
    super(message);
  }

}
