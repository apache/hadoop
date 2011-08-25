package org.apache.hadoop.mapreduce.v2.api.records;

import java.util.List;

public interface TaskReport {
  public abstract TaskId getTaskId();
  public abstract TaskState getTaskState();
  public abstract float getProgress();
  public abstract long getStartTime();
  public abstract long getFinishTime();
  public abstract Counters getCounters();
  
  public abstract List<TaskAttemptId> getRunningAttemptsList();
  public abstract TaskAttemptId getRunningAttempt(int index);
  public abstract int getRunningAttemptsCount();
  
  public abstract TaskAttemptId getSuccessfulAttempt();
  
  public abstract List<String> getDiagnosticsList();
  public abstract String getDiagnostics(int index);
  public abstract int getDiagnosticsCount();
  
  
  public abstract void setTaskId(TaskId taskId);
  public abstract void setTaskState(TaskState taskState);
  public abstract void setProgress(float progress);
  public abstract void setStartTime(long startTime);
  public abstract void setFinishTime(long finishTime);
  public abstract void setCounters(Counters counters);
  
  public abstract void addAllRunningAttempts(List<TaskAttemptId> taskAttempts);
  public abstract void addRunningAttempt(TaskAttemptId taskAttempt);
  public abstract void removeRunningAttempt(int index);
  public abstract void clearRunningAttempts();
  
  public abstract void setSuccessfulAttempt(TaskAttemptId taskAttempt)
;
  public abstract void addAllDiagnostics(List<String> diagnostics);
  public abstract void addDiagnostics(String diagnostics);
  public abstract void removeDiagnostics(int index);
  public abstract void clearDiagnostics();
}
