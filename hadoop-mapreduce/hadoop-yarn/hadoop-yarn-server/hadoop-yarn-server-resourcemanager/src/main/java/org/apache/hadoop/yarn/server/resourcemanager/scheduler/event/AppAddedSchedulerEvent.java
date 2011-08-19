package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

public class AppAddedSchedulerEvent extends SchedulerEvent {

  private final ApplicationAttemptId applicationAttemptId;
  private final String queue;
  private final String user;

  public AppAddedSchedulerEvent(ApplicationAttemptId applicationAttemptId,
      String queue, String user) {
    super(SchedulerEventType.APP_ADDED);
    this.applicationAttemptId = applicationAttemptId;
    this.queue = queue;
    this.user = user;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public String getQueue() {
    return queue;
  }

  public String getUser() {
    return user;
  }

}
