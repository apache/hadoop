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
    if (configuredState != null && !VALID_STATE_CONFIGURATIONS.contains(configuredState)) {
      throw new IllegalArgumentException("Invalid queue state configuration."
          + " We can only use RUNNING or STOPPED.");
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
    QueueState currentState = configuredState == null ? DEFAULT_STATE : configuredState;

    if (parentState != null) {
      if (configuredState == QueueState.RUNNING && parentState != QueueState.RUNNING) {
        throw new IllegalArgumentException(
            "The parent queue:" + queue.getParent().getQueuePath()
                + " cannot be STOPPED as the child queue:" + queue.getQueuePath()
                + " is in RUNNING state.");
      }

      if (configuredState == null) {
        currentState = parentState == QueueState.DRAINING ? QueueState.STOPPED : parentState;
      }
    }

    queue.updateQueueState(currentState);
  }
}
