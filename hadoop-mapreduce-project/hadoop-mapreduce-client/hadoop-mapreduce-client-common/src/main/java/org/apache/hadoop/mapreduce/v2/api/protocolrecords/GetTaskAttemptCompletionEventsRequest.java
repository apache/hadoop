package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;

public interface GetTaskAttemptCompletionEventsRequest {
  public abstract JobId getJobId();
  public abstract int getFromEventId();
  public abstract int getMaxEvents();
  
  public abstract void setJobId(JobId jobId);
  public abstract void setFromEventId(int id);
  public abstract void setMaxEvents(int maxEvents);
}
