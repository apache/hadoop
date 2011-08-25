package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptRejectedEvent extends RMAppAttemptEvent {

  private final String message;

  public RMAppAttemptRejectedEvent(ApplicationAttemptId appAttemptId, String message) {
    super(appAttemptId, RMAppAttemptEventType.APP_REJECTED);
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }
}
