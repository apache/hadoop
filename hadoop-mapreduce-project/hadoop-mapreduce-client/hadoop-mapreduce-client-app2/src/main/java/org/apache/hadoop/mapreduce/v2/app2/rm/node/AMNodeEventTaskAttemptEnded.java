package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

public class AMNodeEventTaskAttemptEnded extends AMNodeEvent {

  private final boolean failed;
  // TODO XXX: contianerId, taskAttemptId not really required in AMNodeSucceeded/Ended events.
  private final ContainerId containerId;
  private final TaskAttemptId taskAttemptId;
  
  public AMNodeEventTaskAttemptEnded(NodeId nodeId, ContainerId containerId, TaskAttemptId taskAttemptId, boolean failed) {
    super(nodeId, AMNodeEventType.N_TA_ENDED);
    this.failed = failed;
    this.containerId = containerId;
    this.taskAttemptId = taskAttemptId;
  }

  public boolean failed() {
    return failed;
  }
  
  public boolean killed() {
    return !failed;
  }
  
  public ContainerId getContainerId() {
    return this.containerId;
  }
  
  public TaskAttemptId getTaskAttemptId() {
    return this.taskAttemptId;
  }
}