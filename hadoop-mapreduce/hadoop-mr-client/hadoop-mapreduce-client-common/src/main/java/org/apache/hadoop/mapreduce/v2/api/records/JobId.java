package org.apache.hadoop.mapreduce.v2.api.records;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface JobId {
  public abstract ApplicationId getAppId();
  public abstract int getId();
  
  public abstract void setAppId(ApplicationId appId);
  public abstract void setId(int id);

}
