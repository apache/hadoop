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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Placement rule and mapping rule checks, shared by the scheduler's placement
 * rule setup and configuration validation. Each check returns null when it
 * passes, or the error message the setup fails with.
 *
 * Mapping rule targets are validated against a {@link QueueIndex}, a narrow
 * view of the queue hierarchy, so the same validation can run against the
 * live queue manager or against queues described by configuration.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class PlacementRuleChecks {

  private PlacementRuleChecks() {
  }

  /**
   * Checks that no placement rule is listed twice in
   * {@code yarn.scheduler.queue-placement-rules}. The exception type of a
   * failure is {@link java.io.IOException}.
   * @param input the configured placement rule names in order
   * @return null if the names are distinct, the error message otherwise
   */
  public static String checkDuplicatePlacementRules(PlacementRuleNames input) {
    Set<String> distinguishRuleSet = new HashSet<>();
    for (String pls : input.getNames()) {
      if (!distinguishRuleSet.add(pls)) {
        return "Invalid PlacementRule inputs which "
            + "contains duplicate rule strings";
      }
    }
    return null;
  }

  /**
   * Returns a queue index backed by the given queue manager. Every lookup goes
   * to the queue manager, so it sees the current static and dynamic queues.
   * @param queueManager the queue manager
   * @return the queue index
   */
  public static QueueIndex queueIndexOf(
      final CapacitySchedulerQueueManager queueManager) {
    return new QueueIndex() {
      @Override
      public QueueRef getQueue(String queueName) {
        return QueueRef.of(queueManager.getQueue(queueName));
      }

      @Override
      public boolean isAmbiguous(String shortName) {
        return queueManager.isAmbiguous(shortName);
      }
    };
  }

  /**
   * The queue lookups mapping rule validation needs. Implementations follow
   * the lookup rules of the scheduler's queue store: a name is a full path or
   * an unambiguous short name, and existing dynamic queues are included.
   */
  public interface QueueIndex {
    /**
     * @param queueName full path or short name of a queue
     * @return the queue, or null if no queue is found by that name
     */
    QueueRef getQueue(String queueName);

    /**
     * @param shortName short name of a queue
     * @return true if more than one queue has this short name
     */
    boolean isAmbiguous(String shortName);
  }

  /**
   * The kinds of queues mapping rule validation distinguishes.
   */
  public enum QueueKind {
    /** Any leaf queue. */
    LEAF,
    /** A {@link ParentQueue}, which can be an AQC v2 parent. */
    PARENT,
    /** A {@link ManagedParentQueue} (AQC v1 parent). */
    MANAGED_PARENT,
    /** Any other parent queue, for example a reservation plan queue. */
    OTHER_PARENT,
    /** A queue that is neither a leaf nor a parent queue. */
    OTHER
  }

  /**
   * A queue as seen by mapping rule validation.
   */
  public static final class QueueRef {
    private final String queuePath;
    private final QueueKind kind;
    private final boolean eligibleForAutoQueueCreation;

    /**
     * @param queuePath full path of the queue
     * @param kind kind of the queue
     * @param eligibleForAutoQueueCreation true if the queue is a parent that
     *                                     allows AQC v2 queue creation, which
     *                                     includes every dynamic parent
     */
    public QueueRef(String queuePath, QueueKind kind,
        boolean eligibleForAutoQueueCreation) {
      this.queuePath = queuePath;
      this.kind = kind;
      this.eligibleForAutoQueueCreation = eligibleForAutoQueueCreation;
    }

    /**
     * @param queue a live queue, may be null
     * @return the reference of the queue, or null if the queue is null
     */
    public static QueueRef of(CSQueue queue) {
      if (queue == null) {
        return null;
      }
      QueueKind kind;
      if (queue instanceof AbstractLeafQueue) {
        kind = QueueKind.LEAF;
      } else if (queue instanceof ManagedParentQueue) {
        kind = QueueKind.MANAGED_PARENT;
      } else if (queue instanceof ParentQueue) {
        kind = QueueKind.PARENT;
      } else if (queue instanceof AbstractParentQueue) {
        kind = QueueKind.OTHER_PARENT;
      } else {
        kind = QueueKind.OTHER;
      }
      boolean eligible = queue instanceof AbstractParentQueue
          && ((AbstractParentQueue) queue).isEligibleForAutoQueueCreation();
      return new QueueRef(queue.getQueuePath(), kind, eligible);
    }

    public String getQueuePath() {
      return queuePath;
    }

    public QueueKind getKind() {
      return kind;
    }

    public boolean isEligibleForAutoQueueCreation() {
      return eligibleForAutoQueueCreation;
    }

    public boolean isLeaf() {
      return kind == QueueKind.LEAF;
    }

    public boolean isParent() {
      return kind == QueueKind.PARENT || kind == QueueKind.MANAGED_PARENT
          || kind == QueueKind.OTHER_PARENT;
    }
  }

  /**
   * Input of {@link #checkDuplicatePlacementRules(PlacementRuleNames)}.
   */
  public static final class PlacementRuleNames {
    private final List<String> names;

    public PlacementRuleNames(Collection<String> names) {
      this.names = Collections.unmodifiableList(new ArrayList<>(names));
    }

    public List<String> getNames() {
      return names;
    }
  }
}
