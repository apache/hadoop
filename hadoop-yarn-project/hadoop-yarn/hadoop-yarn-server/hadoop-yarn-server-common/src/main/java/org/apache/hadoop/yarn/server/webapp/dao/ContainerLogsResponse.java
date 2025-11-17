package org.apache.hadoop.yarn.server.webapp.dao;

import java.util.List;

public class ContainerLogsResponse {
  
  private List<ContainerLogsInfo> containerLogsInfo;


  public ContainerLogsResponse(List<ContainerLogsInfo> containerLogsInfo) {
    this.containerLogsInfo = containerLogsInfo;
  }

  public ContainerLogsResponse() {
  }

  public List<ContainerLogsInfo> getContainerLogsInfo() {
    return containerLogsInfo;
  }

  public void setContainerLogsInfo(List<ContainerLogsInfo> containerLogsInfo) {
    this.containerLogsInfo = containerLogsInfo;
  }
}
