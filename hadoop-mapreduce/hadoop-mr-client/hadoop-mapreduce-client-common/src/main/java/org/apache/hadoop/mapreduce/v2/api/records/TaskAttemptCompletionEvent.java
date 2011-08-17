package org.apache.hadoop.mapreduce.v2.api.records;

public interface TaskAttemptCompletionEvent {
  public abstract TaskAttemptId getAttemptId();
  public abstract TaskAttemptCompletionEventStatus getStatus();
  public abstract String getMapOutputServerAddress();
  public abstract int getAttemptRunTime();
  public abstract int getEventId();
  
  public abstract void setAttemptId(TaskAttemptId taskAttemptId);
  public abstract void setStatus(TaskAttemptCompletionEventStatus status);
  public abstract void setMapOutputServerAddress(String address);
  public abstract void setAttemptRunTime(int runTime);
  public abstract void setEventId(int eventId);
}
