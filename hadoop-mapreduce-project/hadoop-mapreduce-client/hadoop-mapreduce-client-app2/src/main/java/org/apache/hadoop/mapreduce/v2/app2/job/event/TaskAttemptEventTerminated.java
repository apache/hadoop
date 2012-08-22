package org.apache.hadoop.mapreduce.v2.app2.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class TaskAttemptEventTerminated extends TaskAttemptEvent {

  public TaskAttemptEventTerminated(TaskAttemptId id) {
    super(id, TaskAttemptEventType.TA_TERMINATED);
    // TODO Auto-generated constructor stub
  }

}
