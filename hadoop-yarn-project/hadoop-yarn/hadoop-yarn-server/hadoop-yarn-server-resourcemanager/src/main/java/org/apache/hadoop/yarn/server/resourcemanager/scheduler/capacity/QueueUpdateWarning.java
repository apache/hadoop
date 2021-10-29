package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

public class QueueUpdateWarning {
  private final String queue;
  private final QueueUpdateWarningType warningType;
  private String info = "";

  public QueueUpdateWarning(QueueUpdateWarningType queueUpdateWarningType, String queue) {
    this.warningType = queueUpdateWarningType;
    this.queue = queue;
  }

  public enum QueueUpdateWarningType {
    BRANCH_UNDERUTILIZED("Remaining resource found in branch under parent queue '%s'. %s"),
    QUEUE_OVERUTILIZED("Queue '%' is configured to use more resources than what is available under its parent. %s"),
    QUEUE_ZERO_RESOURCE("Queue '%s' is assigned zero resource. %s"),
    BRANCH_DOWNSCALED("Child queues with absolute configured capacity under parent queue '%s' are downscaled due to insufficient cluster resource. %s"),
    QUEUE_EXCEEDS_MAX_RESOURCE("Queue '%s' exceeds its maximum available resources. %s"),
    QUEUE_MAX_RESOURCE_EXCEEDS_PARENT("Maximum resources of queue '%s' are greater than its parent's. %s");

    private final String template;

    QueueUpdateWarningType(String template) {
      this.template = template;
    }

    public QueueUpdateWarning ofQueue(String queue) {
      return new QueueUpdateWarning(this, queue);
    }

    public String getTemplate() {
      return template;
    }
  }

  public QueueUpdateWarning withInfo(String info) {
    this.info = info;

    return this;
  }

  public String getQueue() {
    return queue;
  }

  public QueueUpdateWarningType getWarningType() {
    return warningType;
  }

  @Override
  public String toString() {
    return String.format(warningType.getTemplate(), queue, info);
  }
}
