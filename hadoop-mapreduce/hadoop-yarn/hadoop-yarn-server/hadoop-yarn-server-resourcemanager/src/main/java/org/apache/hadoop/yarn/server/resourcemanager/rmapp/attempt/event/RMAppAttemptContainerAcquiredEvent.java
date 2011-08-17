package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptContainerAcquiredEvent extends RMAppAttemptEvent {

  private final Container container;

  public RMAppAttemptContainerAcquiredEvent(ApplicationAttemptId appAttemptId, 
      Container container) {
    super(appAttemptId, RMAppAttemptEventType.CONTAINER_ACQUIRED);
    this.container = container;
  }

  public Container getContainer() {
    return this.container;
  }

}
