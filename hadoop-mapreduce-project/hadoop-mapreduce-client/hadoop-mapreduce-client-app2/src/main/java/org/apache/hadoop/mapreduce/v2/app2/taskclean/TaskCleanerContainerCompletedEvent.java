package org.apache.hadoop.mapreduce.v2.app2.taskclean;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class TaskCleanerContainerCompletedEvent extends AbstractEvent<TaskCleaner.EventType> {

  private ContainerId containerId;
  
  public TaskCleanerContainerCompletedEvent(ContainerId containerId) {
    super(TaskCleaner.EventType.CONTAINER_COMPLETED);
    this.containerId = containerId;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }
}
