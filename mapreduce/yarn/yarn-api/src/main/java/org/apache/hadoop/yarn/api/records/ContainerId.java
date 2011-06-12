package org.apache.hadoop.yarn.api.records;

public interface ContainerId extends Comparable<ContainerId>{
  public abstract ApplicationId getAppId();
  public abstract int getId();
  
  public abstract void setAppId(ApplicationId appID);
  public abstract void setId(int id);
  
}
