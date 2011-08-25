package org.apache.hadoop.mapreduce.v2.api.records;

import java.util.Map;

public interface Counters {
  public abstract Map<String, CounterGroup> getAllCounterGroups();
  public abstract CounterGroup getCounterGroup(String key);
  public abstract Counter getCounter(Enum<?> key);
  
  public abstract void addAllCounterGroups(Map<String, CounterGroup> counterGroups);
  public abstract void setCounterGroup(String key, CounterGroup value);
  public abstract void removeCounterGroup(String key);
  public abstract void clearCounterGroups();
  
  public abstract void incrCounter(Enum<?> key, long amount);
}
