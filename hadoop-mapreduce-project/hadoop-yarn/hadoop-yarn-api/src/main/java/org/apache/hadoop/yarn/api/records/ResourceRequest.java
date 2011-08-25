package org.apache.hadoop.yarn.api.records;

public interface ResourceRequest extends Comparable<ResourceRequest>{
  public abstract Priority getPriority();
  public abstract String getHostName();
  public abstract Resource getCapability();
  public abstract int getNumContainers();
  
  public abstract void setPriority(Priority priority);
  public abstract void setHostName(String hostName);
  public abstract void setCapability(Resource capability);
  public abstract void setNumContainers(int numContainers);
  

}
