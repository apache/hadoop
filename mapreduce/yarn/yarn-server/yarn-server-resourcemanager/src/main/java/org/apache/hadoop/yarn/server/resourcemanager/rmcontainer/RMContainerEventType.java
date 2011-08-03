package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

public enum RMContainerEventType {

  // Source: scheduler
  START,

  // Source: App
  ACQUIRED,
  KILL, // Also from Node on NodeRemoval

  LAUNCHED,
  FINISHED,

  // Source: ApplicationMasterService
  RELEASED,

  // Source: ContainerAllocationExpirer  
  EXPIRE
}
