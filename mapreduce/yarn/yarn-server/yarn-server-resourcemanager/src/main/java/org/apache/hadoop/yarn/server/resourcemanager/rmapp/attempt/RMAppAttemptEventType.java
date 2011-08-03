package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

public enum RMAppAttemptEventType {
  // Source: RMApp
  START,
  KILL,

  // Source: AMLauncher
  LAUNCHED,
  LAUNCH_FAILED,

  // Source: AMLivelinessMonitor
  EXPIRE,
  
  // Source: ApplicationMasterService
  REGISTERED,
  STATUS_UPDATE,
  UNREGISTERED,

  // Source: Containers
  CONTAINER_ACQUIRED,
  CONTAINER_ALLOCATED,
  CONTAINER_FINISHED,

  // Source: Scheduler
  APP_REJECTED,
  APP_ACCEPTED,

}
