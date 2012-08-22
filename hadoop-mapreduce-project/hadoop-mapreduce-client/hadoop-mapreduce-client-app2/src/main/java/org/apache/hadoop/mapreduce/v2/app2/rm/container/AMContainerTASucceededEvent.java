package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMContainerTASucceededEvent extends AMContainerEvent {

  private final TaskAttemptId attemptId;

  public AMContainerTASucceededEvent(ContainerId containerId,
      TaskAttemptId attemptId) {
    super(containerId, AMContainerEventType.C_TA_SUCCEEDED);
    this.attemptId = attemptId;
  }
  
  public TaskAttemptId getTaskAttemptId() {
    return this.attemptId;
  }
}
