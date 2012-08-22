package org.apache.hadoop.mapreduce.v2.app2.rm.node;

public enum AMNodeEventType {
  //Producer: Scheduler
  N_CONTAINER_ALLOCATED,
  
  //Producer: TaskAttempt
  N_TA_SUCCEEDED,
  N_TA_ENDED, // TODO XXX: Maybe rename.
  
  //Producer: RMCommunicator
  N_TURNED_UNHEALTHY,
  N_TURNED_HEALTHY,
  
  //Producer: AMNodeManager
  N_BLACKLISTING_ENABLED,
  N_BLACKLISTING_DISABLED,
  
  //Producer: Node - Will not reach NodeImpl. Used to compute whether blacklisting should be ignored.
  N_NODE_WAS_BLACKLISTED
}
