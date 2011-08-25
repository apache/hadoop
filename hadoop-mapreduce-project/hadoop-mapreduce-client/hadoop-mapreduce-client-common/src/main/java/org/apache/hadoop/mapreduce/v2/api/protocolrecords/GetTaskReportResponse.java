package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;

public interface GetTaskReportResponse {
  public abstract TaskReport getTaskReport();
  
  public abstract void setTaskReport(TaskReport taskReport);
}
