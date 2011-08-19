package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptLaunchFailedEvent extends RMAppAttemptEvent {

  private final String message;

  public RMAppAttemptLaunchFailedEvent(ApplicationAttemptId appAttemptId,
      String message) {
    super(appAttemptId, RMAppAttemptEventType.LAUNCH_FAILED);
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }
}
