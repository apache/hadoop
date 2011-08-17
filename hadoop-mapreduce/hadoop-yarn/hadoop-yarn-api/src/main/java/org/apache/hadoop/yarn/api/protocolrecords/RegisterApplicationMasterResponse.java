package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.Resource;

public interface RegisterApplicationMasterResponse {
  public Resource getMinimumResourceCapability();
  public void setMinimumResourceCapability(Resource capability);
  public Resource getMaximumResourceCapability();
  public void setMaximumResourceCapability(Resource capability);
}
