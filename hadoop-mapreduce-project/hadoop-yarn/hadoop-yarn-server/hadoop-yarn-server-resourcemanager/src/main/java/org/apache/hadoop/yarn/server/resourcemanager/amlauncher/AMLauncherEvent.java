package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

public class AMLauncherEvent extends AbstractEvent<AMLauncherEventType> {

  private final RMAppAttempt appAttempt;

  public AMLauncherEvent(AMLauncherEventType type, RMAppAttempt appAttempt) {
    super(type);
    this.appAttempt = appAttempt;
  }

  public RMAppAttempt getAppAttempt() {
    return this.appAttempt;
  }

}
