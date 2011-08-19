package org.apache.hadoop.yarn.api.records;

public interface NodeReport {
  NodeId getNodeId();
  void setNodeId(NodeId nodeId);
  String getHttpAddress();
  void setHttpAddress(String httpAddress);
  String getRackName();
  void setRackName(String rackName);
  Resource getUsed();        
  void setUsed(Resource used);
  Resource getCapability();
  void setCapability(Resource capability);
  int getNumContainers();
  void setNumContainers(int numContainers);
  NodeHealthStatus getNodeHealthStatus();
  void setNodeHealthStatus(NodeHealthStatus nodeHealthStatus);
}
