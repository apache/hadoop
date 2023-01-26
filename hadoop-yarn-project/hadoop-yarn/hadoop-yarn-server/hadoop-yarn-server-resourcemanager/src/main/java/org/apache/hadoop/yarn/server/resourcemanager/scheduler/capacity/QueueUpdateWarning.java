/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

/**
 * Represents a warning event that occurred during a queue capacity update phase.
 */
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
    QUEUE_OVERUTILIZED("Queue '%s' is configured to use more resources than what is available " +
        "under its parent. %s"),
    QUEUE_ZERO_RESOURCE("Queue '%s' is assigned zero resource. %s"),
    BRANCH_DOWNSCALED("Child queues with absolute configured capacity under parent queue '%s' are" +
        " downscaled due to insufficient cluster resource. %s"),
    QUEUE_EXCEEDS_MAX_RESOURCE("Queue '%s' exceeds its maximum available resources. %s"),
    QUEUE_MAX_RESOURCE_EXCEEDS_PARENT("Maximum resources of queue '%s' are greater than its " +
        "parent's. %s");

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
