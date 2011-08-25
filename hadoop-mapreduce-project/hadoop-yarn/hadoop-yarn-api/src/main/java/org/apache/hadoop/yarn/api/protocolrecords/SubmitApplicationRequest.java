package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

public interface SubmitApplicationRequest {
  public abstract ApplicationSubmissionContext getApplicationSubmissionContext();
  public abstract void setApplicationSubmissionContext(ApplicationSubmissionContext context);
}
