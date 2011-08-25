package org.apache.hadoop.mapreduce.v2.app.job.event;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;

public class JobCounterUpdateEvent extends JobEvent {

  List<CounterIncrementalUpdate> counterUpdates = null;
  
  public JobCounterUpdateEvent(JobId jobId) {
    super(jobId, JobEventType.JOB_COUNTER_UPDATE);
    counterUpdates = new ArrayList<JobCounterUpdateEvent.CounterIncrementalUpdate>();
  }

  public void addCounterUpdate(Enum<?> key, long incrValue) {
    counterUpdates.add(new CounterIncrementalUpdate(key, incrValue));
  }
  
  public List<CounterIncrementalUpdate> getCounterUpdates() {
    return counterUpdates;
  }
  
  public static class CounterIncrementalUpdate {
    Enum<?> key;
    long incrValue;
    
    public CounterIncrementalUpdate(Enum<?> key, long incrValue) {
      this.key = key;
      this.incrValue = incrValue;
    }
    
    public Enum<?> getCounterKey() {
      return key;
    }

    public long getIncrementValue() {
      return incrValue;
    }
  }
}
