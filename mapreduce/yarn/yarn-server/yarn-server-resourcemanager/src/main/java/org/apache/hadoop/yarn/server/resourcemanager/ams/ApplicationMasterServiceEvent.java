package org.apache.hadoop.yarn.server.resourcemanager.ams;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class ApplicationMasterServiceEvent extends
    AbstractEvent<ApplicationMasterServiceEventType> {

  private final ApplicationAttemptId attemptId;

  public ApplicationMasterServiceEvent(ApplicationAttemptId attemptId,
      ApplicationMasterServiceEventType type) {
    super(type);
    this.attemptId = attemptId;
  }

  public ApplicationAttemptId getAppAttemptId() {
    return this.attemptId;
  }

}
