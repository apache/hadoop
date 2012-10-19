package org.apache.hadoop.mapreduce.v2.app2.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class TaskAttemptEventKillRequest extends TaskAttemptEvent {

  private final String message;

  public TaskAttemptEventKillRequest(TaskAttemptId id, String message) {
    super(id, TaskAttemptEventType.TA_KILL_REQUEST);
    this.message = message;
  }

  // TODO: This is not used at the moment.
  public String getMessage() {
    return this.message;
  }

}
