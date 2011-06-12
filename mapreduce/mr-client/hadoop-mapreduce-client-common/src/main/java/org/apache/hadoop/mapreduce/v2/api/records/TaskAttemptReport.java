package org.apache.hadoop.mapreduce.v2.api.records;

public interface TaskAttemptReport {
  public abstract TaskAttemptId getTaskAttemptId();
  public abstract TaskAttemptState getTaskAttemptState();
  public abstract float getProgress();
  public abstract long getStartTime();
  public abstract long getFinishTime();
  public abstract Counters getCounters();
  public abstract String getDiagnosticInfo();
  public abstract String getStateString();
  public abstract Phase getPhase();

  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
  public abstract void setTaskAttemptState(TaskAttemptState taskAttemptState);
  public abstract void setProgress(float progress);
  public abstract void setStartTime(long startTime);
  public abstract void setFinishTime(long finishTime);
  public abstract void setCounters(Counters counters);
  public abstract void setDiagnosticInfo(String diagnosticInfo);
  public abstract void setStateString(String stateString);
  public abstract void setPhase(Phase phase);
  
}
