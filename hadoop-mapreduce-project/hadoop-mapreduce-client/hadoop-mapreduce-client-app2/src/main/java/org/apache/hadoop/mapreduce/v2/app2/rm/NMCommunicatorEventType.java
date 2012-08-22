package org.apache.hadoop.mapreduce.v2.app2.rm;

// TODO - Re-use the events in ContainerLauncher..
public enum NMCommunicatorEventType {
  CONTAINER_LAUNCH_REQUEST,
  CONTAINER_STOP_REQUEST
}
