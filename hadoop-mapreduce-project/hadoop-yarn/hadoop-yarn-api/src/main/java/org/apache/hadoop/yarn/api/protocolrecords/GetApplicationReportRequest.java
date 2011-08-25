package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface GetApplicationReportRequest {
  public abstract ApplicationId getApplicationId();
  public abstract void setApplicationId(ApplicationId applicationId);
}
