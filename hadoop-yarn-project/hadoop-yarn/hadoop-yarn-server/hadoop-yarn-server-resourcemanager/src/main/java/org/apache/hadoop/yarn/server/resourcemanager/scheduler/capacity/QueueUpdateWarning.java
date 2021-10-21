package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

public enum QueueUpdateWarning {
  BRANCH_UNDERUTILIZED("Remaining resource found in branch under parent queue '%s'. %s"),
  QUEUE_OVERUTILIZED("Queue '%' is configured to use more resources than what is available under its parent. %s"),
  QUEUE_ZERO_RESOURCE("Queue '%s' is assigned zero resource. %s"),
  BRANCH_DOWNSCALED("Child queues with absolute configured capacity under parent queue '%s' are downscaled due to insufficient cluster resource. %s");

  private final String message;
  private String queue;
  private String additionalInfo = "";

  QueueUpdateWarning(String message) {
    this.message = message;
  }

  public QueueUpdateWarning ofQueue(String queuePath) {
    this.queue = queuePath;

    return this;
  }

  public QueueUpdateWarning withInfo(String info) {
    additionalInfo = info;

    return this;
  }

  @Override
  public String toString() {
    return String.format(message, queue, additionalInfo);
  }
}
