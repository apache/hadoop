package org.apache.hadoop.mapreduce.v2.api.records;

public interface TaskId {
  public abstract JobId getJobId();
  public abstract  TaskType getTaskType();
  public abstract int getId();
  
  public abstract void setJobId(JobId jobId);
  public abstract void setTaskType(TaskType taskType);
  public abstract void setId(int id);
}
