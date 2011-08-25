package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptUnregistrationEvent extends RMAppAttemptEvent {

  private final String trackingUrl;
  private final String finalState;
  private final String diagnostics;

  public RMAppAttemptUnregistrationEvent(ApplicationAttemptId appAttemptId,
      String trackingUrl, String finalState, String diagnostics) {
    super(appAttemptId, RMAppAttemptEventType.UNREGISTERED);
    this.trackingUrl = trackingUrl;
    this.finalState = finalState;
    this.diagnostics = diagnostics;
  }

  public String getTrackingUrl() {
    return this.trackingUrl;
  }

  public String getFinalState() {
    return this.finalState;
  }

  public String getDiagnostics() {
    return this.diagnostics;
  }

}