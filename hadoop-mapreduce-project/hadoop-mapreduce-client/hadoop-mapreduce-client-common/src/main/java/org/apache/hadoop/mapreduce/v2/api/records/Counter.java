package org.apache.hadoop.mapreduce.v2.api.records;

public interface Counter {
  public abstract String getName();
  public abstract String getDisplayName();
  public abstract long getValue();
  
  public abstract void setName(String name);
  public abstract void setDisplayName(String displayName);
  public abstract void setValue(long value);
}
