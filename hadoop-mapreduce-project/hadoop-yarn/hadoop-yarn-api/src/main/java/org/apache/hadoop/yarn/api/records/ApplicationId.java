package org.apache.hadoop.yarn.api.records;



public interface ApplicationId extends Comparable<ApplicationId> {
  public abstract int getId();
  public abstract long getClusterTimestamp();
  
  public abstract void setId(int id);
  public abstract void setClusterTimestamp(long clusterTimestamp);
  
}
