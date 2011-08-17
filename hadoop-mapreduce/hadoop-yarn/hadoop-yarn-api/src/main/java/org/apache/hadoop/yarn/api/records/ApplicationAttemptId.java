package org.apache.hadoop.yarn.api.records;

public interface ApplicationAttemptId extends Comparable<ApplicationAttemptId>{
  public abstract ApplicationId getApplicationId();
  public abstract int getAttemptId();
  
  public abstract void setApplicationId(ApplicationId appID);
  public abstract void setAttemptId(int attemptId);
  
}
