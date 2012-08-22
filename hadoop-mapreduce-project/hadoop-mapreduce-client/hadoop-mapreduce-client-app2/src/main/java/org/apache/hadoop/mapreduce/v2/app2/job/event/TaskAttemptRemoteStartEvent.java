package org.apache.hadoop.mapreduce.v2.app2.job.event;

import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class TaskAttemptRemoteStartEvent extends TaskAttemptEvent {

  private final ContainerId containerId;
  // TODO Can appAcls be handled elsewhere ?
  private final Map<ApplicationAccessType, String> applicationACLs;
  private final int shufflePort;

  public TaskAttemptRemoteStartEvent(TaskAttemptId id, ContainerId containerId,
      Map<ApplicationAccessType, String> appAcls, int shufflePort) {
    super(id, TaskAttemptEventType.TA_STARTED_REMOTELY);
    this.containerId = containerId;
    this.applicationACLs = appAcls;
    this.shufflePort = shufflePort;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public Map<ApplicationAccessType, String> getApplicationACLs() {
    return applicationACLs;
  }

  public int getShufflePort() {
    return shufflePort;
  }
}
