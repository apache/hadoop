package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class RMAppEvent extends AbstractEvent<RMAppEventType>{

  private final ApplicationId appId;

  public RMAppEvent(ApplicationId appId, RMAppEventType type) {
    super(type);
    this.appId = appId;
  }

  public ApplicationId getApplicationId() {
    return this.appId;
  }
}
