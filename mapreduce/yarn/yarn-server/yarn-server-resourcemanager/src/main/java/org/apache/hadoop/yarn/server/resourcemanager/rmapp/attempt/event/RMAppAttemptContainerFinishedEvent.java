package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptContainerFinishedEvent extends RMAppAttemptEvent {

  private final ContainerId container;

  public RMAppAttemptContainerFinishedEvent(ApplicationAttemptId appAttemptId, 
      ContainerId containerId) {
    super(appAttemptId, RMAppAttemptEventType.CONTAINER_FINISHED);
    this.container = containerId;
  }

  public ContainerId getContainerId() {
    return this.container;
  }

}
