package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

public enum QueueUpdateWarning {
  BRANCH_UNDERUTILIZED("Remaining resource found in branch under parent queue '%s'."),
  QUEUE_OVERUTILIZED("Queue '%' is configured to use more resources than what is available under its parent."),
  QUEUE_ZERO_RESOURCE("Queue '%s' is assigned zero resource."),
  BRANCH_DOWNSCALED("Child queues with absolute configured capacity under parent queue '%s' are downscaled due to insufficient cluster resource.");

  private final String message;
  private String queue;

  QueueUpdateWarning(String message) {
    this.message = message;
  }

  public QueueUpdateWarning ofQueue(String queuePath) {
    this.queue = queuePath;

    return this;
  }

  @Override
  public String toString() {
    return String.format(message, queue);
  }
}
