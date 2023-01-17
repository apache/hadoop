package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

public final class QueuePrefixes {

  private QueuePrefixes() {
  }

  public static String getQueuePrefix(QueuePath queuePath) {
    String queueName = PREFIX + queuePath.getFullPath() + DOT;
    return queueName;
  }

  public static String getNodeLabelPrefix(QueuePath queuePath, String label) {
    if (label.equals(CommonNodeLabelsManager.NO_LABEL)) {
      return getQueuePrefix(queuePath);
    }
    return getQueuePrefix(queuePath) + ACCESSIBLE_NODE_LABELS + DOT + label + DOT;
  }

  /**
   * Get the auto created leaf queue's template configuration prefix
   * Leaf queue's template capacities are configured at the parent queue
   *
   * @param queuePath parent queue's path
   * @return Config prefix for leaf queue template configurations
   */
  public static String getAutoCreatedQueueTemplateConfPrefix(QueuePath queuePath) {
    return queuePath.getFullPath() + DOT + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
  }

  public static QueuePath getAutoCreatedQueueObjectTemplateConfPrefix(QueuePath queuePath) {
    return new QueuePath(queuePath.getFullPath(), AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX);
  }
}
