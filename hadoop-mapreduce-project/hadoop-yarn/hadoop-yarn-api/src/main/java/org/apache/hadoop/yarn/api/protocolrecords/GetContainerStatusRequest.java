package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ContainerId;

public interface GetContainerStatusRequest {
  public abstract ContainerId getContainerId();
  public abstract void setContainerId(ContainerId containerId);
}
