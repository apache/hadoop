package org.apache.hadoop.yarn.api.records;

import java.util.List;

public interface AMResponse {
  public boolean getReboot();
  public int getResponseId();
  
  public List<Container> getContainerList();
  public Container getContainer(int index);
  public int getContainerCount();

  public void setReboot(boolean reboot);
  public void setResponseId(int responseId);
  
  public void addAllContainers(List<Container> containers);
  public void addContainer(Container container);
  public void removeContainer(int index);
  public void clearContainers();
  
  public void setAvailableResources(Resource limit);
  public Resource getAvailableResources();
}