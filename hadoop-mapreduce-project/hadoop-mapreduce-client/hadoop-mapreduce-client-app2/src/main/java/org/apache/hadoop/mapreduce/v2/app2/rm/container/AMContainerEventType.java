package org.apache.hadoop.mapreduce.v2.app2.rm.container;

public enum AMContainerEventType {

  // TODO Merge START/LAUNCH, STOP/HALT
  
  //Producer: Scheduler
  C_START_REQUEST,
  C_ASSIGN_TA,
  
  //Producer: NMCommunicator
  C_LAUNCHED,
  C_LAUNCH_FAILED, // TODO XXX: Send a diagnostic update message to the TaskAttempts assigned to this container ?
  
  //Producer: TAL: PULL_TA is a sync call.
  C_PULL_TA,
  
  //Producer: Scheduler via TA
  C_TA_SUCCEEDED,
  
  //Producer:RMCommunicator
  C_COMPLETED,
  C_NODE_FAILED,
  
  //Producer: TA-> Scheduler -> Container (in case of failure etc)
  //          Scheduler -> Container (in case of pre-emption etc)
  //          Node -> Container (in case of Node unhealthy etc)
  C_STOP_REQUEST,
  
  //Producer: NMCommunicator
  C_STOP_FAILED,
  
  //Producer: ContainerHeartbeatHandler
  C_TIMED_OUT,
}
