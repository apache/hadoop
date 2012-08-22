package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMContainerAssignTAEvent extends AMContainerEvent {

  private final TaskAttemptId attemptId;
  // TODO Maybe have tht TAL pull the remoteTask from the TaskAttempt itself ?
  private final org.apache.hadoop.mapred.Task remoteTask;
  
  public AMContainerAssignTAEvent(ContainerId containerId,
      TaskAttemptId attemptId, org.apache.hadoop.mapred.Task remoteTask) {
    super(containerId, AMContainerEventType.C_ASSIGN_TA);
    this.attemptId = attemptId;
    this.remoteTask = remoteTask;
  }
  
  public org.apache.hadoop.mapred.Task getRemoteTask() {
    return this.remoteTask;
  }
  
  public TaskAttemptId getTaskAttemptId() {
    return this.attemptId;
  }

}
