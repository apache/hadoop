package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

public class RMAppManagerSubmitEvent extends RMAppManagerEvent {

  private final ApplicationSubmissionContext submissionContext;

  public RMAppManagerSubmitEvent(ApplicationSubmissionContext submissionContext) {
    super(submissionContext.getApplicationId(), RMAppManagerEventType.APP_SUBMIT);
    this.submissionContext = submissionContext;
  }

  public ApplicationSubmissionContext getSubmissionContext() {
    return this.submissionContext;
  }
}
