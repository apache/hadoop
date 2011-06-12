package org.apache.hadoop.mapreduce.v2.api.records;

public interface TaskAttemptId {
  public abstract TaskId getTaskId();
  public abstract int getId();
  
  public abstract void setTaskId(TaskId taskId);
  public abstract void setId(int id);
}
