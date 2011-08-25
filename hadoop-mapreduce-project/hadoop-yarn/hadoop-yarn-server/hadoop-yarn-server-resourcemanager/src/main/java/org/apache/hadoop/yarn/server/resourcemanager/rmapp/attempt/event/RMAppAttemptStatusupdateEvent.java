package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptStatusupdateEvent extends RMAppAttemptEvent {

  private final float progress;

  public RMAppAttemptStatusupdateEvent(ApplicationAttemptId appAttemptId,
      float progress) {
    super(appAttemptId, RMAppAttemptEventType.STATUS_UPDATE);
    this.progress = progress;
  }

  public float getProgress() {
    return this.progress;
  }

}
