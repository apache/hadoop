package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public interface GetDiagnosticsRequest {
  public abstract TaskAttemptId getTaskAttemptId();
  
  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
}
