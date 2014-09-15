package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

public class ContractValidationException extends PlanningException {

  private static final long serialVersionUID = 1L;

  public ContractValidationException(String message) {
    super(message);
  }

  
}
