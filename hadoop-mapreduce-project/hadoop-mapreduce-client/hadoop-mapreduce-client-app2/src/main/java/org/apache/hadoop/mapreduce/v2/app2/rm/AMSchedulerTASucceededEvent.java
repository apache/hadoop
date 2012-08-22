package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class AMSchedulerTASucceededEvent extends AMSchedulerEvent {

  private final TaskAttemptId attemptId;

  public AMSchedulerTASucceededEvent(TaskAttemptId attemptId) {
    super(AMSchedulerEventType.S_TA_SUCCEEDED);
    this.attemptId = attemptId;
  }

  public TaskAttemptId getAttemptID() {
    return this.attemptId;
  }

}
