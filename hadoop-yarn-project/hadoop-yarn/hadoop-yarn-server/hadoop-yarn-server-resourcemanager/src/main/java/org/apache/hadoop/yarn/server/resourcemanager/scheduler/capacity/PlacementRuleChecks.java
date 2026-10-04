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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleValidationContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementRuleChecks.class);

  /**
   * The mapping rule variables with a special meaning, which are immutable
   * in every variable context.
   */
  public static final Set<String> IMMUTABLE_VARIABLES = ImmutableSet.of(
      "%user",
      "%primary_group",
      "%secondary_group",
      "%application",
      "%specified"
      );

  private PlacementRuleChecks() {
  }

  /**
   * Creates the context mapping rules are validated in: the immutable
   * variables and {@code %default} are known, and the rules may add more.
   * @param queueIndex the queues rule targets are validated against
   * @return the validation context
   * @throws IOException if a variable cannot be registered
   */
  public static MappingRuleValidationContext newMappingRuleValidationContext(
      QueueIndex queueIndex) throws IOException {
    MappingRuleValidationContext validationContext =
        new MappingRuleValidationContextImpl(queueIndex);

    //Adding all immutable variables to the known variable list
    for (String var : IMMUTABLE_VARIABLES) {
      try {
        validationContext.addImmutableVariable(var);
      } catch (YarnException e) {
        LOG.error("Error initializing placement variables, unable to register" +
            " '{}': {}", var, e.getMessage());
        throw new IOException(e);
      }
    }
    //Immutables + %default are the only officially supported variables,
    //We initialize the context with these, and let the rules to extend the list
    try {
      validationContext.addVariable("%default");
    } catch (YarnException e) {
      LOG.error("Error initializing placement variables, unable to register" +
          " '%default': " + e.getMessage());
      throw new IOException(e);
    }

    return validationContext;
  }

  /**
   * Checks that no placement rule is listed twice in
   * {@code yarn.scheduler.queue-placement-rules}. The exception type of a
   * failure is {@link java.io.IOException}.
   * @param names the configured placement rule names in order
   * @return null if the names are distinct, the error message otherwise
   */
  public static String checkDuplicatePlacementRules(Collection<String> names) {
    Set<String> distinguishRuleSet = new HashSet<>();
    for (String pls : names) {
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
   * Returns a queue index over the given queues, with the lookup rules of the
   * scheduler's queue store: a full path always finds its queue, a short name
   * finds the queue only when no other queue has the same short name, and
   * {@code root} is not a short name.
   * @param queues the queues, each full path at most once
   * @return the queue index
   */
  public static QueueIndex queueIndexOf(Collection<QueueRef> queues) {
    final Map<String, QueueRef> byName = new HashMap<>();
    final Map<String, Integer> shortNameCounts = new HashMap<>();
    Map<String, QueueRef> byShortName = new HashMap<>();
    for (QueueRef queue : queues) {
      byName.put(queue.getQueuePath(), queue);
      String path = queue.getQueuePath();
      String shortName = path.substring(path.lastIndexOf('.') + 1);
      if (!shortName.equals(CapacitySchedulerConfiguration.ROOT)) {
        Integer count = shortNameCounts.get(shortName);
        shortNameCounts.put(shortName, count == null ? 1 : count + 1);
        byShortName.put(shortName, queue);
      }
    }
    for (Map.Entry<String, QueueRef> entry : byShortName.entrySet()) {
      if (shortNameCounts.get(entry.getKey()) == 1) {
        byName.put(entry.getKey(), entry.getValue());
      }
    }
    return new QueueIndex() {
      @Override
      public QueueRef getQueue(String queueName) {
        return queueName == null ? null : byName.get(queueName);
      }

      @Override
      public boolean isAmbiguous(String shortName) {
        Integer count = shortName == null ? null
            : shortNameCounts.get(shortName);
        return count != null && count > 1;
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
      boolean eligible = queue instanceof AbstractParentQueue
          && ((AbstractParentQueue) queue).isEligibleForAutoQueueCreation();
      return new QueueRef(queue.getQueuePath(), QueueKind.of(queue), eligible);
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
      return kind.isLeaf();
    }

    public boolean isParent() {
      return kind.isParent();
    }
  }
}
