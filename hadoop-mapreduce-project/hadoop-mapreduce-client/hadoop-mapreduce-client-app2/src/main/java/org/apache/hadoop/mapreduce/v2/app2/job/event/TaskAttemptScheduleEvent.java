package org.apache.hadoop.mapreduce.v2.app2.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class TaskAttemptScheduleEvent extends TaskAttemptEvent {

  private final boolean rescheduled;
  
  public TaskAttemptScheduleEvent(TaskAttemptId id, TaskAttemptEventType type, boolean rescheduled) {
    super(id, type);
    this.rescheduled = rescheduled;
  }

  public boolean isRescheduled() {
    return this.rescheduled;
  }
  
}
