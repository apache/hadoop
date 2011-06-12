package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ApplicationMaster;

public interface FinishApplicationMasterRequest {
  public abstract ApplicationMaster getApplicationMaster();
  public abstract void setApplicationMaster(ApplicationMaster applicationMaster);
}
