package org.apache.hadoop.mapreduce.v2.api.records;

public interface JobReport {
  public abstract JobId getJobId();
  public abstract JobState getJobState();
  public abstract float getMapProgress();
  public abstract float getReduceProgress();
  public abstract float getCleanupProgress();
  public abstract float getSetupProgress();
  public abstract long getStartTime();
  public abstract long getFinishTime();

  public abstract void setJobId(JobId jobId);
  public abstract void setJobState(JobState jobState);
  public abstract void setMapProgress(float progress);
  public abstract void setReduceProgress(float progress);
  public abstract void setCleanupProgress(float progress);
  public abstract void setSetupProgress(float progress);
  public abstract void setStartTime(long startTime);
  public abstract void setFinishTime(long finishTime);
}
