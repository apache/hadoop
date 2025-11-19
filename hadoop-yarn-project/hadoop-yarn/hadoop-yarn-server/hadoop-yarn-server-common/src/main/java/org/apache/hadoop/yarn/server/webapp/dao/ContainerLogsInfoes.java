package org.apache.hadoop.yarn.server.webapp.dao;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;


@XmlRootElement
public class ContainerLogsInfoes {
  private List<ContainerLogsInfo> containerLogsInfo;


  public ContainerLogsInfoes(List<ContainerLogsInfo> containerLogsInfo) {
    this.containerLogsInfo = containerLogsInfo;
  }

  public ContainerLogsInfoes() {
  }

  public List<ContainerLogsInfo> getContainerLogsInfo() {
    return containerLogsInfo;
  }

  public void setContainerLogsInfo(List<ContainerLogsInfo> containerLogsInfo) {
    this.containerLogsInfo = containerLogsInfo;
  }
}
