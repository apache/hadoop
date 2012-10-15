package org.apache.hadoop.mapreduce.v2.app2.rm;

public enum AMSchedulerEventType {
  //Producer: TaskAttempt
  S_TA_LAUNCH_REQUEST,
  S_TA_ENDED, // Annotated with FAILED/KILLED/SUCCEEDED.

  //Producer: RMCommunicator
  S_CONTAINERS_ALLOCATED,

  //Producer: Container. (Maybe RMCommunicator)
  S_CONTAINER_COMPLETED,

  //Producer: Node
  S_NODE_BLACKLISTED,
  S_NODE_UNHEALTHY,
  S_NODE_HEALTHY
  // The scheduler should have a way of knowing about unusable nodes. Acting on
  // this information to change requests etc is scheduler specific.
}
