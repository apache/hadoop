package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class RMAppRejectedEvent extends RMAppEvent {

  private final String message;

  public RMAppRejectedEvent(ApplicationId appId, String message) {
    super(appId, RMAppEventType.APP_REJECTED);
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }
}
