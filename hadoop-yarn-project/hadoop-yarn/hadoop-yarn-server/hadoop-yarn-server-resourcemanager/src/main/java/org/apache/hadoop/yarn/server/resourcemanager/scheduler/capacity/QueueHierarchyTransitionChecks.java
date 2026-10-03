/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks of the transition from the existing queue hierarchy to a newly configured one:
 * queue removal, moves and conversions between leaf, parent and auto create enabled parent
 * queues. Every check compares an existing queue with its counterpart in the new hierarchy and
 * returns null when it passes, or the error message when it fails; the caller decides which
 * exception to throw.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueHierarchyTransitionChecks {
  private static final Logger LOG = LoggerFactory.getLogger(
      CapacitySchedulerConfigValidator.class);

  private QueueHierarchyTransitionChecks() {
  }

  /**
   * The kind of a queue, as far as the hierarchy transition checks are concerned.
   */
  public enum QueueKind {
    /** Any leaf queue. */
    LEAF,
    /** A parent queue that is not auto create enabled (includes reservable queues). */
    PARENT,
    /** An auto create enabled parent queue (auto queue creation v1). */
    MANAGED_PARENT,
    /** Neither a leaf nor a parent queue. */
    OTHER
  }

  /**
   * Parses the state left in the new configuration of a queue, the way the removal check reads
   * it: case sensitive, and an invalid value counts as not configured.
   * @param state the raw value of the state property, may be null
   * @param queuePath the full path of the queue, used for logging
   * @return the state, or null if not configured or invalid
   */
  public static QueueState parseConfiguredState(String state, String queuePath) {
    if (state != null) {
      try {
        return QueueState.valueOf(state);
      } catch (Exception ex) {
        LOG.warn("Not a valid queue state for queue: {}, state: {}", queuePath, state);
      }
    }
    return null;
  }

  /**
   * Checks that an existing queue missing from the new configuration can be removed: either
   * its current state or the state left in the new configuration is STOPPED, or it is a
   * dynamic queue. Auto created leaf queues of auto queue creation v1 and reservation queues
   * are not checked at all by the caller.
   * @param oldQueue the existing queue
   * @param newConfiguredState the state configured for the queue in the new configuration,
   *                           see {@link #parseConfiguredState(String, String)}
   * @return null if the check passes, otherwise the error message
   */
  public static String checkQueueRemoval(QueueSnapshot oldQueue,
      QueueState newConfiguredState) {
    if (isEitherQueueStopped(oldQueue.getState(), newConfiguredState)
        || oldQueue.isDynamic()) {
      return null;
    }
    return oldQueue.getQueuePath() + " cannot be"
        + " deleted from the capacity scheduler configuration, as the"
        + " queue is not yet in stopped state. Current State : "
        + oldQueue.getState();
  }

  /**
   * Checks that a queue is not moved from one hierarchy to another.
   * @param oldQueue the existing queue
   * @param newQueue the queue in the new hierarchy
   * @return null if the check passes, otherwise the error message
   */
  public static String checkSameQueuePath(QueueSnapshot oldQueue, QueueSnapshot newQueue) {
    if (!oldQueue.getQueuePath().equals(newQueue.getQueuePath())) {
      return oldQueue.getQueuePath() + " is moved from:" + oldQueue.getQueuePath() + " to:"
          + newQueue.getQueuePath()
          + " after refresh, which is not allowed.";
    }
    return null;
  }

  /**
   * Checks that a parent queue is neither converted to nor from an auto create enabled parent
   * queue.
   * @param oldQueue the existing queue
   * @param newQueue the queue in the new hierarchy
   * @return null if the check passes, otherwise the error message
   */
  public static String checkParentQueueConversion(QueueSnapshot oldQueue,
      QueueSnapshot newQueue) {
    if (!isParent(oldQueue.getKind())) {
      return null;
    }
    if (oldQueue.getKind() != QueueKind.MANAGED_PARENT
        && newQueue.getKind() == QueueKind.MANAGED_PARENT) {
      return "Can not convert parent queue: " + oldQueue.getQueuePath()
          + " to auto create enabled parent queue since "
          + "it could have other pre-configured queues which is not "
          + "supported";
    }
    if (oldQueue.getKind() == QueueKind.MANAGED_PARENT
        && newQueue.getKind() != QueueKind.MANAGED_PARENT) {
      return "Cannot convert auto create enabled parent queue: "
          + oldQueue.getQueuePath() + " to leaf queue. Please check "
          + " parent queue's configuration "
          + CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED
          + " is set to true";
    }
    return null;
  }

  /**
   * Checks that a leaf queue is only converted to a parent queue when either the existing queue
   * or the new queue is STOPPED.
   * @param oldQueue the existing queue
   * @param newQueue the queue in the new hierarchy, with its initial state
   * @return null if the check passes, otherwise the error message
   */
  public static String checkLeafQueueConversion(QueueSnapshot oldQueue, QueueSnapshot newQueue) {
    if (isLeafToParentConversion(oldQueue, newQueue)
        && !isEitherQueueStopped(oldQueue.getState(), newQueue.getState())) {
      return "Can not convert the leaf queue: " + oldQueue.getQueuePath()
          + " to parent queue since "
          + "it is not yet in stopped state. Current State : "
          + oldQueue.getState();
    }
    return null;
  }

  static boolean isLeafToParentConversion(QueueSnapshot oldQueue, QueueSnapshot newQueue) {
    return oldQueue.getKind() == QueueKind.LEAF && isParent(newQueue.getKind());
  }

  static boolean isParent(QueueKind kind) {
    return kind == QueueKind.PARENT || kind == QueueKind.MANAGED_PARENT;
  }

  static boolean isEitherQueueStopped(QueueState a, QueueState b) {
    return a == QueueState.STOPPED || b == QueueState.STOPPED;
  }

  /**
   * The properties of a queue that the hierarchy transition checks look at.
   */
  public static final class QueueSnapshot {
    private final String queuePath;
    private final QueueKind kind;
    private final QueueState state;
    private final boolean dynamic;

    /**
     * @param queuePath the full path of the queue
     * @param kind the kind of the queue
     * @param state the state of the queue: the current state of an existing queue, or the
     *              initial state of a queue in the new hierarchy
     * @param dynamic whether the queue is a dynamic queue, see
     *                {@link AbstractCSQueue#isDynamicQueue()}
     */
    public QueueSnapshot(String queuePath, QueueKind kind, QueueState state, boolean dynamic) {
      this.queuePath = queuePath;
      this.kind = kind;
      this.state = state;
      this.dynamic = dynamic;
    }

    public String getQueuePath() {
      return queuePath;
    }

    public QueueKind getKind() {
      return kind;
    }

    public QueueState getState() {
      return state;
    }

    public boolean isDynamic() {
      return dynamic;
    }
  }
}
