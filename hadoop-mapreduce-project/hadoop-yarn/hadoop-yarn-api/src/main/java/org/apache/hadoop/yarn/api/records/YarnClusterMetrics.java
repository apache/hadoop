package org.apache.hadoop.yarn.api.records;

public interface YarnClusterMetrics {
  
  public abstract int getNumNodeManagers();
  
  public abstract void setNumNodeManagers(int numNodeManagers);

}
