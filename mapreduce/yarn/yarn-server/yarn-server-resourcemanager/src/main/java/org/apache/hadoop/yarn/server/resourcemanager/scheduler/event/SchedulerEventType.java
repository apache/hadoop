package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

public enum SchedulerEventType {

  // Source: Node
  NODE_ADDED,
  NODE_REMOVED,
  NODE_UPDATE,

  // Source: Container
  CONTAINER_FINISHED,
  
  // Source: App
  APP_ADDED,
  APP_REMOVED,

}
