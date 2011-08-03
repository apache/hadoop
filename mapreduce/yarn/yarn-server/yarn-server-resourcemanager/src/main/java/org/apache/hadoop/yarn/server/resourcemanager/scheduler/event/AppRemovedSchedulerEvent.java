package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public class AppRemovedSchedulerEvent extends SchedulerEvent {

  private final ApplicationAttemptId applicationAttemptId;

  public AppRemovedSchedulerEvent(ApplicationAttemptId applicationAttemptId) {
    super(SchedulerEventType.APP_REMOVED);
    this.applicationAttemptId = applicationAttemptId;
  }

  public ApplicationAttemptId getApplicationAttemptID() {
    return this.applicationAttemptId;
  }

}
