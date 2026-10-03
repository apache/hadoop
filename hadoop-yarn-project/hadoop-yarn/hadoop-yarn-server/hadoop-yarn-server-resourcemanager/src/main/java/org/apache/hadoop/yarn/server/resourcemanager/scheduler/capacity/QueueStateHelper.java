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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.util.Set;

/**
 * Collects all logic that are handling queue state transitions.
 */
public final class QueueStateHelper {
  private static final Set<QueueState> VALID_STATE_CONFIGURATIONS = ImmutableSet.of(
      QueueState.RUNNING, QueueState.STOPPED);
  private static final QueueState DEFAULT_STATE = QueueState.RUNNING;

  private QueueStateHelper() {}

  /**
   * Sets the current state of the queue based on its previous state, its parent's state and its
   * configured state.
   * @param queue the queue whose state is set
   */
  public static void setQueueState(AbstractCSQueue queue) {
    QueueState previousState = queue.getState();
    QueueState configuredState = queue.getQueueContext().getConfiguration().getConfiguredState(
        queue.getQueuePathObject());
    QueueState parentState = (queue.getParent() == null) ? null : queue.getParent().getState();

    // verify that we can not any value for State other than RUNNING/STOPPED
    String configuredStateError = checkConfiguredState(configuredState);
    if (configuredStateError != null) {
      throw new IllegalArgumentException(configuredStateError);
    }

    if (previousState == null) {
      initializeState(queue, configuredState, parentState);
    } else {
      reinitializeState(queue, previousState, configuredState);
    }
  }

  private static void reinitializeState(
      AbstractCSQueue queue, QueueState previousState, QueueState configuredState) {
    // when we get a refreshQueue request from AdminService,
    if (previousState == QueueState.RUNNING) {
      if (configuredState == QueueState.STOPPED) {
        queue.stopQueue();
      }
    } else {
      if (configuredState == QueueState.RUNNING) {
        try {
          queue.activateQueue();
        } catch (YarnException ex) {
          throw new IllegalArgumentException(ex.getMessage());
        }
      }
    }
  }

  private static void initializeState(
      AbstractCSQueue queue, QueueState configuredState, QueueState parentState) {
    if (parentState != null) {
      String initialStateError = checkInitialState(new InitialStateInput(
          queue.getQueuePath(), configuredState, queue.getParent().getQueuePath(), parentState));
      if (initialStateError != null) {
        throw new IllegalArgumentException(initialStateError);
      }
    }

    queue.updateQueueState(getInitialState(configuredState, parentState));
  }

  /**
   * Checks that the configured state is one that can be set in the configuration.
   * The parsing of the value itself happens in
   * {@link CapacitySchedulerConfiguration#getConfiguredState(QueuePath)}.
   * @param configuredState the configured state of the queue, null if not configured
   * @return null if the state is valid, otherwise the error message
   */
  public static String checkConfiguredState(QueueState configuredState) {
    if (configuredState != null && !VALID_STATE_CONFIGURATIONS.contains(configuredState)) {
      return "Invalid queue state configuration. We can only use RUNNING or STOPPED.";
    }
    return null;
  }

  /**
   * Checks that a newly created queue configured as RUNNING is not placed under a parent that
   * is not RUNNING.
   * @param input the queue and its parent state
   * @return null if the check passes, otherwise the error message
   */
  public static String checkInitialState(InitialStateInput input) {
    if (input.getParentState() != null && input.getConfiguredState() == QueueState.RUNNING
        && input.getParentState() != QueueState.RUNNING) {
      return "The parent queue:" + input.getParentQueuePath()
          + " cannot be STOPPED as the child queue:" + input.getQueuePath()
          + " is in RUNNING state.";
    }
    return null;
  }

  /**
   * Checks that a queue is activated only under a RUNNING parent. The
   * exception type of a failure is {@link YarnException}.
   * @param parentQueuePath the full path of the parent, null for root
   * @param parentState the state of the parent, null for root
   * @return null if the queue can be activated, otherwise the error message
   */
  public static String checkParentRunning(String parentQueuePath,
      QueueState parentState) {
    if (parentQueuePath == null || parentState == QueueState.RUNNING) {
      return null;
    }
    return "The parent Queue:" + parentQueuePath
        + " is not running. Please activate the parent queue first";
  }

  /**
   * Computes the state of a newly created queue: the configured state, otherwise the state of
   * the parent (DRAINING is inherited as STOPPED), otherwise RUNNING.
   * @param configuredState the configured state of the queue, null if not configured
   * @param parentState the state of the parent queue, null for root
   * @return the initial state of the queue
   */
  public static QueueState getInitialState(QueueState configuredState, QueueState parentState) {
    if (configuredState != null) {
      return configuredState;
    }
    if (parentState != null) {
      return parentState == QueueState.DRAINING ? QueueState.STOPPED : parentState;
    }
    return DEFAULT_STATE;
  }

  /**
   * Input of {@link #checkInitialState(InitialStateInput)}.
   */
  public static final class InitialStateInput {
    private final String queuePath;
    private final QueueState configuredState;
    private final String parentQueuePath;
    private final QueueState parentState;

    public InitialStateInput(String queuePath, QueueState configuredState,
        String parentQueuePath, QueueState parentState) {
      this.queuePath = queuePath;
      this.configuredState = configuredState;
      this.parentQueuePath = parentQueuePath;
      this.parentState = parentState;
    }

    public String getQueuePath() {
      return queuePath;
    }

    public QueueState getConfiguredState() {
      return configuredState;
    }

    public String getParentQueuePath() {
      return parentQueuePath;
    }

    public QueueState getParentState() {
      return parentState;
    }
  }
}
