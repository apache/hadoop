package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

public interface GetTaskReportsRequest {
  
  public abstract JobId getJobId();
  public abstract TaskType getTaskType();
  
  public abstract void setJobId(JobId jobId);
  public abstract void setTaskType(TaskType taskType);
}
