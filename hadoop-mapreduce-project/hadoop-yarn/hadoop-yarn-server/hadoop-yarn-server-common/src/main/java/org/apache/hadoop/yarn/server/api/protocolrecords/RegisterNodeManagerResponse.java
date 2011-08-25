package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;

public interface RegisterNodeManagerResponse {
  public abstract RegistrationResponse getRegistrationResponse();
  
  public abstract void setRegistrationResponse(RegistrationResponse registrationResponse);

}
