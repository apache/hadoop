package org.apache.hadoop.yarn.api.records;

public interface NodeManagerInfo {
  String getNodeAddress();
  void setNodeAddress(String nodeAddress);
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
  NodeId getNodeId();
  void setNodeId(NodeId nodeId);
}
