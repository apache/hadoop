/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.QueueState;

/**
 * Encapsulates Queue entitlement and state updates needed
 * for adjusting capacity dynamically
 *
 */
@Private
@Unstable
public abstract class QueueManagementChange {

  private final CSQueue queue;

  /**
   * Updating the queue may involve entitlement updates
   * and/or QueueState changes
   *
   * QueueAction can potentially be enhanced
   * for adding, removing queues for queue management
   */
  public enum QueueAction {
    UPDATE_QUEUE
  }

  private AutoCreatedLeafQueueConfig
      queueTemplateUpdate;

  private final QueueAction queueAction;
  /**
   * Updated Queue state with the new entitlement
   */
  private QueueState transitionToQueueState;

  public QueueManagementChange(final CSQueue queue,
      final QueueAction queueAction) {
    this.queue = queue;
    this.queueAction = queueAction;
  }

  public QueueManagementChange(final CSQueue queue,
      final QueueAction queueAction, QueueState targetQueueState,
      final AutoCreatedLeafQueueConfig
          queueTemplateUpdates) {
    this(queue, queueAction, queueTemplateUpdates);
    this.transitionToQueueState = targetQueueState;
  }

  public QueueManagementChange(final CSQueue queue,
      final QueueAction queueAction,
      final AutoCreatedLeafQueueConfig
      queueTemplateUpdates) {
    this(queue, queueAction);
    this.queueTemplateUpdate = queueTemplateUpdates;
  }

  public QueueState getTransitionToQueueState() {
    return transitionToQueueState;
  }

  public CSQueue getQueue() {
    return queue;
  }

  public AutoCreatedLeafQueueConfig getUpdatedQueueTemplate() {
    return queueTemplateUpdate;
  }

  public QueueAction getQueueAction() {
    return queueAction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof QueueManagementChange))
      return false;

    QueueManagementChange that = (QueueManagementChange) o;

    if (queue != null ? !queue.equals(that.queue) : that.queue != null)
      return false;
    if (queueTemplateUpdate != null ? !queueTemplateUpdate.equals(
        that.queueTemplateUpdate) : that.queueTemplateUpdate != null)
      return false;
    if (queueAction != that.queueAction)
      return false;
    return transitionToQueueState == that.transitionToQueueState;
  }

  @Override
  public int hashCode() {
    int result = queue != null ? queue.hashCode() : 0;
    result = 31 * result + (queueTemplateUpdate != null ?
        queueTemplateUpdate.hashCode() :
        0);
    result = 31 * result + (queueAction != null ? queueAction.hashCode() : 0);
    result = 31 * result + (transitionToQueueState != null ?
        transitionToQueueState.hashCode() :
        0);
    return result;
  }

  @Override
  public String toString() {
    return "QueueManagementChange{" + "queue=" + queue
        + ", updatedEntitlementsByPartition=" + queueTemplateUpdate
        + ", queueAction=" + queueAction + ", transitionToQueueState="
        + transitionToQueueState + '}';
  }

  public static class UpdateQueue extends QueueManagementChange {

    public UpdateQueue(final CSQueue queue, QueueState targetQueueState,
        final AutoCreatedLeafQueueConfig
            queueTemplateUpdate) {
      super(queue, QueueAction.UPDATE_QUEUE, targetQueueState,
          queueTemplateUpdate);
    }

    public UpdateQueue(final CSQueue queue,
        final AutoCreatedLeafQueueConfig
            queueTemplateUpdate) {
      super(queue, QueueAction.UPDATE_QUEUE, queueTemplateUpdate);
    }
  }
}
