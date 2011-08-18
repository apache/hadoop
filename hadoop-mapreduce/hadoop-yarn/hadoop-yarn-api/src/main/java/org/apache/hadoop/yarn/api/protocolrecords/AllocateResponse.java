package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.AMResponse;

public interface AllocateResponse {
  public abstract AMResponse getAMResponse();
  
  public abstract void setAMResponse(AMResponse amResponse);
}
