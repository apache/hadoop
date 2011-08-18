package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptRegistrationEvent extends RMAppAttemptEvent {

  private final ApplicationAttemptId appAttemptId;
  private final String host;
  private int rpcport;
  private String trackingurl;

  public RMAppAttemptRegistrationEvent(ApplicationAttemptId appAttemptId,
      String host, int rpcPort, String trackingUrl) {
    super(appAttemptId, RMAppAttemptEventType.REGISTERED);
    this.appAttemptId = appAttemptId;
    this.host = host;
    this.rpcport = rpcPort;
    this.trackingurl = trackingUrl;
  }

  public String getHost() {
    return this.host;
  }

  public int getRpcport() {
    return this.rpcport;
  }

  public String getTrackingurl() {
    return this.trackingurl;
  }
}
