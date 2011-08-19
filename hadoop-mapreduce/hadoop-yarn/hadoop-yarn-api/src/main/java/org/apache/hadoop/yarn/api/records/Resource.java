package org.apache.hadoop.yarn.api.records;

public interface Resource extends Comparable<Resource> {
  public abstract int getMemory();
  
  public abstract void setMemory(int memory);

  
}
