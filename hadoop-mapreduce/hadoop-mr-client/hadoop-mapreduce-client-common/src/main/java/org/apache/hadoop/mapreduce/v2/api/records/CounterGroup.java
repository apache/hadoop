package org.apache.hadoop.mapreduce.v2.api.records;

import java.util.Map;

public interface CounterGroup {
  public abstract String getName();
  public abstract String getDisplayName();
  
  public abstract Map<String, Counter> getAllCounters();
  public abstract Counter getCounter(String key);
  
  public abstract void setName(String name);
  public abstract void setDisplayName(String displayName);
  
  public abstract void addAllCounters(Map<String, Counter> counters);
  public abstract void setCounter(String key, Counter value);
  public abstract void removeCounter(String key);
  public abstract void clearCounters();
}
