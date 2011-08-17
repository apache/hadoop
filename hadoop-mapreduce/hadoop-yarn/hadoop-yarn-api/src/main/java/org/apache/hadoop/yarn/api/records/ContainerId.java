package org.apache.hadoop.yarn.api.records;

public interface ContainerId extends Comparable<ContainerId>{
  public abstract ApplicationAttemptId getAppAttemptId();
  public abstract ApplicationId getAppId();
  public abstract int getId();
  
  public abstract void setAppAttemptId(ApplicationAttemptId atId);
  public abstract void setAppId(ApplicationId appID);
  public abstract void setId(int id);
  
}
