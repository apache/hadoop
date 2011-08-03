package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

public enum RMContainerState {
  NEW, ALLOCATED, ACQUIRED, RUNNING, COMPLETED, EXPIRED, RELEASED, KILLED
}
