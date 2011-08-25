package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

public interface GetTaskReportRequest {
  public abstract TaskId getTaskId();
  
  public abstract void setTaskId(TaskId taskId); 
}
