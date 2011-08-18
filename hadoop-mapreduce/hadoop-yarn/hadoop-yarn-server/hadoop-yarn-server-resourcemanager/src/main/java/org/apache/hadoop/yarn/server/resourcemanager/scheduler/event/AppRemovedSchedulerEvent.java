package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;

public class AppRemovedSchedulerEvent extends SchedulerEvent {

  private final ApplicationAttemptId applicationAttemptId;
  private final RMAppAttemptState finalAttemptState;

  public AppRemovedSchedulerEvent(ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState finalAttemptState) {
    super(SchedulerEventType.APP_REMOVED);
    this.applicationAttemptId = applicationAttemptId;
    this.finalAttemptState = finalAttemptState;
  }

  public ApplicationAttemptId getApplicationAttemptID() {
    return this.applicationAttemptId;
  }

  public RMAppAttemptState getFinalAttemptState() {
    return this.finalAttemptState;
  }
}
