package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

public interface StartContainerRequest {
  public abstract ContainerLaunchContext getContainerLaunchContext();
  
  public abstract void setContainerLaunchContext(ContainerLaunchContext context);
}
