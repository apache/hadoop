package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ContainerStatus;

public interface GetContainerStatusResponse {
  public abstract ContainerStatus getStatus();
  public abstract void setStatus(ContainerStatus containerStatus);
}
