package org.apache.hadoop.mapreduce.v2.app2.rm.container;

public enum AMContainerState {
  ALLOCATED,
  LAUNCHING,
  IDLE,
  RUNNING,
  STOPPING,
  COMPLETED,
}
