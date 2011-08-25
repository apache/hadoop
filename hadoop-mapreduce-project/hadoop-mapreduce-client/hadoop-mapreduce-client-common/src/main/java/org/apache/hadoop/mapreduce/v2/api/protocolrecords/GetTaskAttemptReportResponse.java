package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;

public interface GetTaskAttemptReportResponse {
  public abstract TaskAttemptReport getTaskAttemptReport();
  
  public abstract void setTaskAttemptReport(TaskAttemptReport taskAttemptReport);
}
