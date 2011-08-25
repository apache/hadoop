package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

public enum RMNodeEventType {
  // Source: AdminService
  DECOMMISSION,

  // ResourceTrackerService
  STATUS_UPDATE,
  REBOOTING,

  // Source: Application
  CLEANUP_APP,

  // Source: Container
  CONTAINER_ALLOCATED,
  CLEANUP_CONTAINER,

  // Source: NMLivelinessMonitor
  EXPIRE
}
