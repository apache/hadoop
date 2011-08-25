package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

public enum RMContainerEventType {

  // Source: scheduler
  START,

  // Source: SchedulerApp
  ACQUIRED,
  KILL, // Also from Node on NodeRemoval
  RESERVED,

  LAUNCHED,
  FINISHED,

  // Source: ApplicationMasterService->Scheduler
  RELEASED,

  // Source: ContainerAllocationExpirer  
  EXPIRE
}
