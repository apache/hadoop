package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.Counters;

public interface GetCountersResponse {
  public abstract Counters getCounters();
  
  public abstract void setCounters(Counters counters);
}
