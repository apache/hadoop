package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface GetApplicationMasterRequest {
  public abstract ApplicationId getApplicationId();
  public abstract void setApplicationId(ApplicationId applicationId);
}
