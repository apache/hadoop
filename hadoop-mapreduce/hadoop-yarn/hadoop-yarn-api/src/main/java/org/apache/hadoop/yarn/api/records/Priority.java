package org.apache.hadoop.yarn.api.records;

public interface Priority extends Comparable<Priority> {
  
  public abstract int getPriority();
  
  public abstract void setPriority(int priority);
  
}
