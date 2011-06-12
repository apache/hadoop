package org.apache.hadoop.yarn.api.records;

public interface NodeId {
  public abstract int getId();
  
  public abstract void setId(int id);
}
