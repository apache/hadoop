package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

public class AMNodeEventTaskAttemptSucceeded extends AMNodeEvent {

  // TODO These two parameters really aren't required in this event.
  private final ContainerId containerId;
  private final TaskAttemptId taskAttemptId;

  public AMNodeEventTaskAttemptSucceeded(NodeId nodeId,
      ContainerId containerId, TaskAttemptId taskAttemptId) {
    super(nodeId, AMNodeEventType.N_TA_SUCCEEDED);
    this.containerId = containerId;
    this.taskAttemptId = taskAttemptId;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }

  public TaskAttemptId getTaskAttemptId() {
    return this.taskAttemptId;
  }

}
