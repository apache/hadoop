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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueApplicationLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs.DynamicQueue;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.*;

/**
 * Resolves the configuration of every queue from a {@link ConfigSnapshot},
 * top-down, the way queue construction reads it today, without building
 * queues.
 * <p>
 * Values are read through the {@link QueueProperty} readers that the
 * {@code CapacitySchedulerConfiguration} getters use. For dynamic queues the
 * snapshot is overlaid with the entries trunk writes into the live
 * configuration before the queue reads it:
 * <ul>
 * <li>v2 dynamic leaves first get {@code user-limit-factor=-1} and
 * {@code maximum-am-resource-percent=1.0}, unconditionally;</li>
 * <li>v2 templates of the parent then write the type specific template
 * ({@code leaf-template} or {@code parent-template}) and the generic
 * {@code template}, skipping keys that are explicitly set for the queue in
 * the snapshot, so a type specific entry from a wildcard path beats a generic
 * entry from the exact path;</li>
 * <li>a v1 auto-created leaf gets every {@code leaf-queue-template} entry of
 * its managed parent, overwriting explicit keys.</li>
 * </ul>
 * Static queues never receive templates. Inheritance from the parent
 * (PARENT steps) uses the parent's resolved values, and derived values are
 * computed from resolved values only; effective resources, which depend on
 * the cluster resource, are not resolved.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueConfigResolver {
  static final QueuePath ROOT =
      new QueuePath(CapacitySchedulerConfiguration.ROOT);

  private static final String LABELS_PREFIX =
      CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS;
  private static final String DYNAMIC_LEAF_DEFAULT = "dynamic leaf default";
  private static final String DYNAMIC_QUEUE_DEFAULT = "dynamic queue default";
  private static final String LEAF_TEMPLATE_PSEUDO_QUEUE =
      DOT + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
  private static final String V2_TEMPLATE_PSEUDO_QUEUE = DOT
      + AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX.substring(0,
          AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX.length() - 1);

  /** Properties resolved by dedicated code instead of a plain read. */
  private static final Set<QueueProperty<?>> SPECIAL = new HashSet<>(
      java.util.Arrays.<QueueProperty<?>>asList(LABELED_CAPACITY,
          LABELED_MAXIMUM_CAPACITY, USER_WEIGHTS, STATE,
          ACCESSIBLE_NODE_LABELS, DEFAULT_NODE_LABEL_EXPRESSION,
          ACL_APPLICATION_MAX_PRIORITY, QUEUE_ORDERING_POLICY,
          ORDERING_POLICY_PARAMETERS, MAXIMUM_ALLOCATION, PREEMPTION_DISABLED,
          INTRA_QUEUE_PREEMPTION_DISABLED, MAXIMUM_APPLICATION_LIFETIME,
          DEFAULT_APPLICATION_LIFETIME, LEAF_QUEUE_TEMPLATE, AQC_V2_TEMPLATE,
          AQC_V2_LEAF_TEMPLATE, AQC_V2_PARENT_TEMPLATE));

  private QueueConfigResolver() {
  }

  /**
   * Resolves every configured queue and every existing dynamic queue.
   * @param snapshot the configuration
   * @param inputs what the scheduler holds outside the configuration
   * @return the resolved tree
   */
  public static ResolvedQueueTree resolve(ConfigSnapshot snapshot,
      ResolutionInputs inputs) {
    Map<String, Set<String>> labels;
    RuntimeException labelFailure = null;
    try {
      labels = configuredNodeLabelsByQueue(snapshot);
    } catch (RuntimeException e) {
      labels = Collections.emptyMap();
      labelFailure = e;
    }
    ResolvedQueueTree tree = new ResolvedQueueTree(snapshot, inputs, labels);

    Set<QueuePath> dynamicParents = new HashSet<>();
    for (DynamicQueue queue : inputs.getDynamicQueues()) {
      if (!queue.isLeaf()) {
        dynamicParents.add(queue.getPath());
      }
    }
    resolveConfigured(tree, null, ROOT, dynamicParents, labelFailure);

    // Existing dynamic queues that are not configured keep their dynamic
    // nature; configured ones were turned static above
    List<DynamicQueue> dynamicQueues =
        new ArrayList<>(inputs.getDynamicQueues());
    Collections.sort(dynamicQueues, new Comparator<DynamicQueue>() {
      @Override
      public int compare(DynamicQueue a, DynamicQueue b) {
        return Integer.compare(a.getPath().getPathComponents().length,
            b.getPath().getPathComponents().length);
      }
    });
    for (DynamicQueue queue : dynamicQueues) {
      QueuePath path = queue.getPath();
      ResolvedQueueConfig parent = path.isRoot() ? null
          : tree.get(path.getParentObject());
      if (tree.get(path) == null && parent != null) {
        tree.add(resolveDynamic(tree, parent, path,
            queue.isLeaf() ? Kind.LEAF : Kind.PARENT,
            queue.isLegacyAutoCreated(), labelFailure));
      }
    }

    // The default ReservationQueue of every PlanQueue
    for (ResolvedQueueConfig queue : new ArrayList<>(tree.getQueues())) {
      if (queue.getKind() == Kind.PLAN) {
        QueuePath path = QueuePath.createFromQueues(
            queue.getQueuePath().getFullPath(), queue.getQueuePath()
                .getLeafName() + ReservationConstants.DEFAULT_QUEUE_SUFFIX);
        tree.add(resolveDynamic(tree, queue, path, Kind.RESERVATION, false,
            labelFailure));
      }
    }

    boolean legacyMode = isLegacyQueueMode(snapshot);
    Map<QueuePath, Resolved<Map<String, Float>>> weightSums = new HashMap<>();
    for (ResolvedQueueConfig queue : tree.getQueues()) {
      QueuePath path = queue.getQueuePath();
      Resolved<Map<String, Float>> sums = null;
      if (!path.isRoot()) {
        sums = weightSums.get(path.getParentObject());
        if (sums == null) {
          sums = weightSums(tree.getChildren(path.getParentObject()));
          weightSums.put(path.getParentObject(), sums);
        }
      }
      derive(tree, queue, legacyMode, sums);
    }
    return tree;
  }

  /**
   * Resolves a dynamic queue that does not exist yet, as it would be set up
   * if it were created now under its parent in the tree. A leaf under a
   * managed parent is resolved as a v1 auto-created leaf.
   * @param tree the resolved tree
   * @param path the path of the new queue
   * @param leaf whether the new queue is a leaf
   * @return the resolved configuration of the queue, not added to the tree
   * @throws IllegalArgumentException if the parent is not in the tree
   */
  public static ResolvedQueueConfig resolveDynamicQueue(ResolvedQueueTree tree,
      QueuePath path, boolean leaf) {
    ResolvedQueueConfig parent = tree.get(path.getParentObject());
    if (parent == null) {
      throw new IllegalArgumentException("Parent of " + path
          + " is not in the resolved tree");
    }
    boolean legacy = leaf && isManagedParent(parent);
    Resolved<Set<String>> rootLabels =
        tree.getRoot().get(CONFIGURED_NODE_LABELS);
    ResolvedQueueConfig queue = resolveDynamic(tree, parent, path,
        leaf ? Kind.LEAF : Kind.PARENT, legacy,
        rootLabels.isFailed() ? failureOf(rootLabels) : null);
    List<ResolvedQueueConfig> siblings =
        new ArrayList<>(tree.getChildren(parent.getQueuePath()));
    siblings.add(queue);
    derive(tree, queue, isLegacyQueueMode(tree.getSnapshot()),
        weightSums(siblings));
    return queue;
  }

  /**
   * Indexes the labels of every queue path that has
   * {@code <path>.accessible-node-labels.<label>.<property>} keys, including
   * template pseudo-queues; every indexed path also gets the empty label.
   * @param snapshot the configuration
   * @return queue path to its configured labels
   * @throws StringIndexOutOfBoundsException for a key that ends right after
   *         the label
   */
  public static Map<String, Set<String>> configuredNodeLabelsByQueue(
      ConfigSnapshot snapshot) {
    Map<String, Set<String>> labelsByQueue = new HashMap<>();
    Map<String, String> schedulerEntries =
        snapshot.getRawPropertiesWithPrefix(PREFIX, false);
    for (String key : schedulerEntries.keySet()) {
      // Consider all keys that have the accessible-node-labels prefix,
      // excluding <queue-path>.accessible-node-labels itself
      if (key.contains(LABELS_PREFIX + DOT)) {
        int labelStartIdx = key.indexOf(LABELS_PREFIX)
            + LABELS_PREFIX.length() + 1;
        int labelEndIndx = key.indexOf('.', labelStartIdx);
        String labelName = key.substring(labelStartIdx, labelEndIndx);
        String queuePath =
            key.substring(0, key.indexOf(LABELS_PREFIX) - 1);
        Set<String> labels = labelsByQueue.get(queuePath);
        if (labels == null) {
          labels = new HashSet<>();
          labels.add(RMNodeLabelsManager.NO_LABEL);
          labelsByQueue.put(queuePath, labels);
        }
        labels.add(labelName);
      }
    }
    return labelsByQueue;
  }

  private static void resolveConfigured(ResolvedQueueTree tree,
      ResolvedQueueConfig parent, QueuePath path, Set<QueuePath> dynamicParents,
      RuntimeException labelFailure) {
    Function<String, String> conf = snapshotLookup(tree.getSnapshot());
    // The queue kind is decided as CapacitySchedulerQueueManager.parseQueue
    // does; a value that fails to read counts as unset, its failure is kept
    // in the resolved property
    List<String> children = Collections.emptyList();
    boolean parentLike = dynamicParents.contains(path);
    boolean reservable = false;
    try {
      children = QUEUES.read(conf, path);
      parentLike = parentLike
          || AUTO_QUEUE_CREATION_V2_ENABLED.read(conf, path)
          || AUTO_CREATE_CHILD_QUEUE_ENABLED.read(conf, path);
      reservable = RESERVABLE.read(conf, path);
    } catch (RuntimeException e) {
      // Recorded when the queue's properties are read
    }
    Kind kind;
    if (path.isRoot()) {
      kind = Kind.ROOT;
    } else if (children.isEmpty() && !parentLike) {
      kind = reservable ? Kind.PLAN : Kind.LEAF;
    } else {
      kind = Kind.PARENT;
    }

    Resolved<Set<String>> labels = labelFailure != null
        ? Resolved.<Set<String>>failed(labelFailure, ValueSource.DERIVED, null)
        : labels(path.isRoot() ? allLabels(tree.getConfiguredNodeLabels())
            : labelsOf(tree.getConfiguredNodeLabels(), path.getFullPath()));
    ResolvedQueueConfig queue =
        resolveQueue(tree, parent, path, kind, false, labels, null);
    tree.add(queue);
    for (String child : children) {
      resolveConfigured(tree, queue,
          QueuePath.createFromQueues(path.getFullPath(), child),
          dynamicParents, labelFailure);
    }
  }

  /**
   * Resolves a queue that receives template entries: a v1 auto-created leaf
   * ({@code legacy}), a v2 dynamic leaf or parent, or a ReservationQueue,
   * which takes the v2 path of its PlanQueue parent.
   */
  private static ResolvedQueueConfig resolveDynamic(ResolvedQueueTree tree,
      ResolvedQueueConfig parent, QueuePath path, Kind kind, boolean legacy,
      RuntimeException labelFailure) {
    ConfigSnapshot snapshot = tree.getSnapshot();
    boolean leaf = kind != Kind.PARENT;
    String childPrefix = QueuePrefixes.getQueuePrefix(path);
    Map<String, OverlayEntry> overlay = new LinkedHashMap<>();
    if (legacy) {
      Resolved<Map<String, String>> template = parent.get(LEAF_QUEUE_TEMPLATE);
      if (template != null && !template.isFailed()) {
        for (Map.Entry<String, String> entry
            : template.getValue().entrySet()) {
          String name = entry.getKey().replaceFirst(
              AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX, path.getLeafName());
          overlay.put(name, new OverlayEntry(entry.getKey(),
              entry.getValue(), ValueSource.TEMPLATE_V1));
        }
      }
    } else {
      if (leaf) {
        overlay.put(childPrefix + CapacitySchedulerConfiguration
            .USER_LIMIT_FACTOR, new OverlayEntry("-1.0"));
        overlay.put(childPrefix + CapacitySchedulerConfiguration
            .MAXIMUM_AM_RESOURCE_SUFFIX, new OverlayEntry("1.0"));
      }
      Map<String, String> typeSpecific = templateEntries(parent,
          leaf ? AQC_V2_LEAF_TEMPLATE : AQC_V2_PARENT_TEMPLATE);
      Map<String, String> common = templateEntries(parent, AQC_V2_TEMPLATE);
      for (Map.Entry<String, String> entry : typeSpecific.entrySet()) {
        if (snapshot.getRaw(childPrefix + entry.getKey()) == null) {
          overlay.put(childPrefix + entry.getKey(), v2Entry(snapshot, parent,
              leaf ? AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX
                  : AutoCreatedQueueTemplate.AUTO_QUEUE_PARENT_TEMPLATE_PREFIX,
              entry));
        }
      }
      for (Map.Entry<String, String> entry : common.entrySet()) {
        if (snapshot.getRaw(childPrefix + entry.getKey()) == null
            && !typeSpecific.containsKey(entry.getKey())) {
          overlay.put(childPrefix + entry.getKey(), v2Entry(snapshot, parent,
              AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX, entry));
        }
      }
    }
    expandOverlay(snapshot, overlay);

    // Labels are registered only from the exact template pseudo-queue
    Resolved<Set<String>> labels;
    if (labelFailure != null) {
      labels = Resolved.failed(labelFailure, ValueSource.DERIVED, null);
    } else {
      Set<String> templateLabels = tree.getConfiguredNodeLabels().get(
          parent.getQueuePath().getFullPath() + (legacy
              ? LEAF_TEMPLATE_PSEUDO_QUEUE : V2_TEMPLATE_PSEUDO_QUEUE));
      labels = labels(templateLabels != null && templateLabels.size() > 1
          ? Collections.unmodifiableSet(new HashSet<>(templateLabels))
          : labelsOf(tree.getConfiguredNodeLabels(), path.getFullPath()));
    }
    ResolvedQueueConfig queue =
        resolveQueue(tree, parent, path, kind, true, labels, overlay);
    // A PlanQueue is a managed parent, so its ReservationQueue takes the
    // ACLs of its leaf-queue-template
    resolveDynamicAcls(queue, parent, snapshot,
        legacy || kind == Kind.RESERVATION);
    if (kind == Kind.RESERVATION) {
      // The plan's user limits replace the reservation queue's own
      queue.put(USER_LIMIT, CommonNodeLabelsManager.NO_LABEL,
          inherit(parent.get(USER_LIMIT), parent));
      queue.put(USER_LIMIT_FACTOR, CommonNodeLabelsManager.NO_LABEL,
          inherit(parent.get(USER_LIMIT_FACTOR), parent));
    } else if (!legacy) {
      // v1 auto-created leaves are not dynamic queue objects, so they keep
      // the vectors read from their path; their entitlement replaces them
      resolveDynamicCapacityVectors(queue, queue.getConfiguredNodeLabels());
    }
    return queue;
  }

  /**
   * @param labels the configured node labels of the queue; a failure to
   *               index them leaves only the empty label
   */
  private static ResolvedQueueConfig resolveQueue(ResolvedQueueTree tree,
      ResolvedQueueConfig parent, QueuePath path, Kind kind, boolean dynamic,
      Resolved<Set<String>> labeled, Map<String, OverlayEntry> overlay) {
    ConfigSnapshot snapshot = tree.getSnapshot();
    Set<String> labels = labeled.isFailed() ? noLabel() : labeled.getValue();
    ResolvedQueueConfig queue =
        new ResolvedQueueConfig(path, kind, dynamic, labels);
    queue.put(CONFIGURED_NODE_LABELS, CommonNodeLabelsManager.NO_LABEL,
        labeled);
    Lookup lookup = new Lookup(snapshot, overlay, path);

    for (QueueProperty<?> property : ALL_PROPERTIES) {
      if (SPECIAL.contains(property)
          || !property.getAppliesTo().contains(kind)) {
        continue;
      }
      if (property.isLabeled()) {
        for (String label : labels) {
          readInto(queue, lookup, property, label, null);
        }
      } else {
        readInto(queue, lookup, property, CommonNodeLabelsManager.NO_LABEL,
            null);
      }
    }
    for (String label : labels) {
      if (!label.equals(CommonNodeLabelsManager.NO_LABEL)) {
        readInto(queue, lookup, LABELED_CAPACITY, label, null);
        readInto(queue, lookup, LABELED_MAXIMUM_CAPACITY, label, null);
      }
    }
    if (ACL_APPLICATION_MAX_PRIORITY.getAppliesTo().contains(kind)) {
      readInto(queue, lookup, ACL_APPLICATION_MAX_PRIORITY,
          CommonNodeLabelsManager.NO_LABEL,
          tree.getInputs().getClusterMaximumApplicationPriority());
      // Ordering policy parameters are iterated from the live configuration,
      // which holds raw values and the template entries. The prefix query
      // also returns the ordering-policy key itself, which is not a parameter.
      String parameterPrefix = ORDERING_POLICY_PARAMETERS.getKey(path, null);
      Map<String, String> parameters = new HashMap<>();
      for (Map.Entry<String, String> entry : snapshot
          .getRawPropertiesWithPrefix(parameterPrefix, true).entrySet()) {
        if (entry.getKey().startsWith(parameterPrefix)) {
          parameters.put(entry.getKey().substring(parameterPrefix.length()),
              entry.getValue());
        }
      }
      if (overlay != null) {
        for (Map.Entry<String, OverlayEntry> entry : overlay.entrySet()) {
          if (entry.getKey().startsWith(parameterPrefix)) {
            parameters.put(entry.getKey().substring(parameterPrefix.length()),
                entry.getValue().raw);
          }
        }
      }
      readMap(queue, lookup, ORDERING_POLICY_PARAMETERS, parameters,
          Function.<String>identity());
    }
    if (AQC_V2_TEMPLATE.getAppliesTo().contains(kind)) {
      resolveTemplates(queue, snapshot, path);
    }
    if (kind == Kind.PLAN) {
      resolvePlanCapacityVectors(queue, labels);
    }
    resolveInherited(tree, queue, parent, lookup);
    return queue;
  }

  /** PARENT steps, in the order queue setup evaluates them. */
  private static void resolveInherited(ResolvedQueueTree tree,
      ResolvedQueueConfig queue, ResolvedQueueConfig parent, Lookup lookup) {
    ConfigSnapshot snapshot = tree.getSnapshot();
    QueuePath path = queue.getQueuePath();
    String none = CommonNodeLabelsManager.NO_LABEL;

    // Node labels (QueueNodeLabelsSettings)
    Resolved<Set<String>> accessible =
        read(lookup, ACCESSIBLE_NODE_LABELS, path, none, null);
    if (!accessible.isFailed() && accessible.getValue() == null
        && parent != null) {
      accessible = inherit(parent.get(ACCESSIBLE_NODE_LABELS), parent);
    }
    queue.put(ACCESSIBLE_NODE_LABELS, none, accessible);
    Resolved<String> expression =
        read(lookup, DEFAULT_NODE_LABEL_EXPRESSION, path, none, null);
    Resolved<Set<String>> parentAccessible =
        parent == null ? null : parent.get(ACCESSIBLE_NODE_LABELS);
    if (!expression.isFailed() && expression.getValue() == null
        && parent != null && !accessible.isFailed()
        && accessible.getValue() != null && !parentAccessible.isFailed()
        && accessible.getValue().containsAll(parentAccessible.getValue())) {
      expression = inherit(parent.get(DEFAULT_NODE_LABEL_EXPRESSION), parent);
    }
    queue.put(DEFAULT_NODE_LABEL_EXPRESSION, none, expression);

    // Maximum allocation (QueueAllocationSettings)
    queue.put(MAXIMUM_ALLOCATION, none,
        resolveMaximumAllocation(tree, queue, parent, lookup));

    // Initial state (QueueStateHelper), DRAINING is inherited as STOPPED
    Resolved<QueueState> state = read(lookup, STATE, path, none, null);
    if (!state.isFailed() && state.getValue() == null) {
      if (parent == null) {
        state = Resolved.of(QueueState.RUNNING, ValueSource.DEFAULT, null);
      } else {
        Resolved<QueueState> parentState = parent.get(STATE);
        state = parentState.isFailed() ? parentState : Resolved.of(
            parentState.getValue() == QueueState.DRAINING
                ? QueueState.STOPPED : parentState.getValue(),
            ValueSource.PARENT, parent.getQueuePath().getFullPath());
      }
    }
    queue.put(STATE, none, state);

    // User weights, unioned with the parent's; read from the snapshot only,
    // since template entries are invisible to the user weight lookup. The
    // entries map to their full keys, so that only the weight keys the
    // reader picks are substituted, as UserWeights does
    String weightPrefix = USER_WEIGHTS.getKey(path, null);
    Map<String, String> weightKeys = new HashMap<>();
    for (String key : snapshot.getRawPropertiesWithPrefix(weightPrefix, false)
        .keySet()) {
      weightKeys.put(key, weightPrefix + key);
    }
    Resolved<Map<String, Float>> weights = readMap(null, lookup, USER_WEIGHTS,
        weightKeys, snapshotLookup(snapshot));
    if (!weights.isFailed() && parent != null) {
      Resolved<Map<String, Float>> parentWeights = parent.get(USER_WEIGHTS);
      if (parentWeights.isFailed()) {
        weights = parentWeights;
      } else if (!parentWeights.getValue().isEmpty()) {
        Map<String, Float> union = new HashMap<>(parentWeights.getValue());
        union.putAll(weights.getValue());
        weights = Resolved.of(Collections.unmodifiableMap(union),
            weights.getValue().isEmpty() ? ValueSource.PARENT
                : ValueSource.QUEUE,
            weights.getValue().isEmpty()
                ? parent.getQueuePath().getFullPath() : weightPrefix);
      }
    }
    queue.put(USER_WEIGHTS, none, weights);

    // Preemption (CSQueuePreemptionSettings)
    queue.put(PREEMPTION_DISABLED, none, resolveDisabledFlag(snapshot, lookup,
        queue, parent, PREEMPTION_DISABLED,
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS));
    queue.put(INTRA_QUEUE_PREEMPTION_DISABLED, none, resolveDisabledFlag(
        snapshot, lookup, queue, parent, INTRA_QUEUE_PREEMPTION_DISABLED,
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
        CapacitySchedulerConfiguration.DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED));

    // Lifetimes (QueueAppLifetimeAndLimitSettings)
    Resolved<Long> maxLifetime =
        read(lookup, MAXIMUM_APPLICATION_LIFETIME, path, none, null);
    if (!maxLifetime.isFailed() && parent != null
        && maxLifetime.getValue() < 0) {
      maxLifetime = inherit(parent.get(MAXIMUM_APPLICATION_LIFETIME), parent);
    }
    queue.put(MAXIMUM_APPLICATION_LIFETIME, none, maxLifetime);
    queue.put(DEFAULT_APPLICATION_LIFETIME, none,
        resolveDefaultLifetime(lookup, queue, parent, maxLifetime));

    // Queue ordering policy, the parent's policy is the default
    if (QUEUE_ORDERING_POLICY.getAppliesTo().contains(queue.getKind())) {
      Resolved<String> parentPolicy =
          parent == null ? null : parent.get(QUEUE_ORDERING_POLICY);
      Resolved<String> policy;
      if (parentPolicy != null && parentPolicy.isFailed()) {
        policy = parentPolicy;
      } else {
        policy = read(lookup, QUEUE_ORDERING_POLICY, path, none,
            parentPolicy == null ? null : parentPolicy.getValue());
        if (parentPolicy != null && policy.getSource() == ValueSource.DEFAULT) {
          policy = Resolved.of(policy.getValue(), ValueSource.PARENT,
              parent.getQueuePath().getFullPath());
        }
      }
      queue.put(QUEUE_ORDERING_POLICY, none, policy);
    }
  }

  private static Resolved<Resource> resolveMaximumAllocation(
      ResolvedQueueTree tree, ResolvedQueueConfig queue,
      ResolvedQueueConfig parent, Lookup lookup) {
    QueuePath path = queue.getQueuePath();
    String none = CommonNodeLabelsManager.NO_LABEL;
    Resolved<Resource> base = parent == null
        ? Resolved.of(tree.getInputs().getClusterMaximumAllocation(),
            ValueSource.GLOBAL, "cluster maximum allocation")
        : inherit(parent.get(MAXIMUM_ALLOCATION), parent);
    if (base.isFailed()) {
      return base;
    }
    Resolved<Resource> queueMax =
        read(lookup, MAXIMUM_ALLOCATION, path, none, null);
    if (queueMax.isFailed()) {
      return queueMax;
    }
    Resource maximumAllocation = Resources.clone(base.getValue());
    if (queueMax.getValue() != Resources.none()) {
      for (ResourceInformation ri : queueMax.getValue().getResources()) {
        maximumAllocation.setResourceInformation(ri.getName(), ri);
      }
      return Resolved.of(maximumAllocation, queueMax.getSource(),
          queueMax.getSourceDetail());
    }
    // Backward compatible per-resource keys, only when maximum-allocation
    // is not set
    Resolved<Long> memory = read(lookup, MAXIMUM_ALLOCATION_MB, path, none,
        null);
    if (memory.isFailed()) {
      return Resolved.failed(failureOf(memory), memory.getSource(),
          memory.getSourceDetail());
    }
    Resolved<Integer> vcores = read(lookup, MAXIMUM_ALLOCATION_VCORES, path,
        none, null);
    if (vcores.isFailed()) {
      return Resolved.failed(failureOf(vcores), vcores.getSource(),
          vcores.getSourceDetail());
    }
    Resolved<?> source = base;
    if (memory.getValue() != CapacitySchedulerConfiguration.UNDEFINED) {
      maximumAllocation.setMemorySize(memory.getValue());
      source = memory;
    }
    if (vcores.getValue() != CapacitySchedulerConfiguration.UNDEFINED) {
      maximumAllocation.setVirtualCores(vcores.getValue());
      source = vcores;
    }
    return Resolved.of(maximumAllocation, source.getSource(),
        source.getSourceDetail());
  }

  private static Resolved<Boolean> resolveDisabledFlag(ConfigSnapshot snapshot,
      Lookup lookup, ResolvedQueueConfig queue, ResolvedQueueConfig parent,
      QueueProperty<Boolean> property, String gateKey, boolean gateDefault) {
    try {
      if (!readBoolean(snapshotLookup(snapshot), gateKey, gateDefault)) {
        return Resolved.of(true, ValueSource.GLOBAL, gateKey);
      }
    } catch (RuntimeException e) {
      return Resolved.failed(e, ValueSource.GLOBAL, gateKey);
    }
    String none = CommonNodeLabelsManager.NO_LABEL;
    if (parent == null) {
      return read(lookup, property, queue.getQueuePath(), none, false);
    }
    Resolved<Boolean> inherited = parent.get(property);
    if (inherited.isFailed()) {
      return inherited;
    }
    Resolved<Boolean> own = read(lookup, property, queue.getQueuePath(), none,
        inherited.getValue());
    return own.getSource() == ValueSource.DEFAULT
        ? inherit(inherited, parent) : own;
  }

  private static Resolved<Long> resolveDefaultLifetime(Lookup lookup,
      ResolvedQueueConfig queue, ResolvedQueueConfig parent,
      Resolved<Long> maxLifetime) {
    QueuePath path = queue.getQueuePath();
    Resolved<Long> own = read(lookup, DEFAULT_APPLICATION_LIFETIME, path,
        CommonNodeLabelsManager.NO_LABEL, null);
    if (own.isFailed()) {
      return own;
    }
    long defaultLifetime = own.getValue();
    boolean specified = defaultLifetime >= 0
        || (parent != null && parent.isDefaultLifetimeSpecified());
    queue.setDefaultLifetimeSpecified(specified);
    if (parent == null) {
      return own;
    }
    if (maxLifetime.isFailed()) {
      // Queue setup reads the maximum first and fails on it
      return Resolved.failed(failureOf(maxLifetime), ValueSource.DEFAULT,
          maxLifetime.getSourceDetail());
    }
    Resolved<Long> parentDefault = parent.get(DEFAULT_APPLICATION_LIFETIME);
    if (parentDefault.isFailed()) {
      return parentDefault;
    }
    long myMax = maxLifetime.getValue();
    boolean inherited = false;
    if (defaultLifetime < 0) {
      if (specified) {
        defaultLifetime = Math.min(parentDefault.getValue(), myMax);
        inherited = true;
      } else {
        defaultLifetime = myMax;
      }
    }
    // The check of the default against the maximum is not a resolution step
    if (defaultLifetime <= 0) {
      defaultLifetime = myMax;
      inherited = false;
    }
    if (defaultLifetime == own.getValue()) {
      return own;
    }
    return inherited
        ? Resolved.of(defaultLifetime, ValueSource.PARENT,
            parent.getQueuePath().getFullPath())
        : Resolved.of(defaultLifetime, ValueSource.DEFAULT,
            MAXIMUM_APPLICATION_LIFETIME.getKey(path, null));
  }

  private static void resolveTemplates(ResolvedQueueConfig queue,
      ConfigSnapshot snapshot, QueuePath path) {
    String none = CommonNodeLabelsManager.NO_LABEL;
    String queuePrefix = QueuePrefixes.getQueuePrefix(path);
    Map<String, String> leafTemplate = snapshot.getRawPropertiesWithPrefix(
        queuePrefix + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX, true);
    queue.put(LEAF_QUEUE_TEMPLATE, none, Resolved.of(leafTemplate,
        leafTemplate.isEmpty() ? ValueSource.DEFAULT : ValueSource.QUEUE,
        LEAF_QUEUE_TEMPLATE.getKey(path, null)));

    Resolved<Integer> depth = queue.get(AUTO_QUEUE_CREATION_V2_MAX_DEPTH);
    if (depth.isFailed()) {
      RuntimeException failure = failureOf(depth);
      queue.put(AQC_V2_TEMPLATE, none, Resolved.<Map<String, String>>failed(
          failure, depth.getSource(), depth.getSourceDetail()));
      queue.put(AQC_V2_LEAF_TEMPLATE, none, Resolved.<Map<String, String>>
          failed(failure, depth.getSource(), depth.getSourceDetail()));
      queue.put(AQC_V2_PARENT_TEMPLATE, none, Resolved.<Map<String, String>>
          failed(failure, depth.getSource(), depth.getSourceDetail()));
      return;
    }
    List<QueuePath> owners = path.isInvalid()
        ? Collections.<QueuePath>emptyList()
        : path.getWildcardedQueuePaths(depth.getValue());
    putTemplate(queue, snapshot, owners, AQC_V2_TEMPLATE);
    putTemplate(queue, snapshot, owners, AQC_V2_LEAF_TEMPLATE);
    putTemplate(queue, snapshot, owners, AQC_V2_PARENT_TEMPLATE);
  }

  private static void putTemplate(ResolvedQueueConfig queue,
      ConfigSnapshot snapshot, List<QueuePath> owners,
      QueueProperty<Map<String, String>> property) {
    // The most specific owner path wins
    Map<String, String> entries = new HashMap<>();
    for (QueuePath owner : owners) {
      Map<String, String> ownerEntries = snapshot.getRawPropertiesWithPrefix(
          QueuePrefixes.getQueuePrefix(owner) + property.getName(), false);
      for (Map.Entry<String, String> entry : ownerEntries.entrySet()) {
        if (!entry.getKey().isEmpty()) {
          entries.putIfAbsent(entry.getKey(), entry.getValue());
        }
      }
    }
    queue.put(property, CommonNodeLabelsManager.NO_LABEL, Resolved.of(
        Collections.unmodifiableMap(entries),
        entries.isEmpty() ? ValueSource.DEFAULT : ValueSource.QUEUE,
        property.getKey(queue.getQueuePath(), null)));
  }

  /**
   * Dynamic queues take their queue ACLs from the parent's templates, with
   * {@code " "} as the fallback, replacing the ACLs read from their own keys.
   */
  private static void resolveDynamicAcls(ResolvedQueueConfig queue,
      ResolvedQueueConfig parent, ConfigSnapshot snapshot, boolean legacy) {
    for (QueueProperty<String> acl : java.util.Arrays.asList(
        ACL_SUBMIT_APPLICATIONS, ACL_ADMINISTER_QUEUE)) {
      Resolved<String> value = null;
      if (legacy) {
        // v1 reads the substituted template value
        String key = QueuePrefixes.getQueuePrefix(QueuePrefixes
            .getAutoCreatedQueueObjectTemplateConfPrefix(
                parent.getQueuePath())) + acl.getName();
        try {
          String text = snapshot.get(key);
          if (text != null) {
            value = Resolved.of(text, ValueSource.TEMPLATE_V1, key);
          }
        } catch (RuntimeException e) {
          value = Resolved.failed(e, ValueSource.TEMPLATE_V1, key);
        }
      } else {
        // v2 uses the raw template text, type specific entries first
        QueueProperty<Map<String, String>> typeSpecific =
            queue.getKind() == Kind.LEAF ? AQC_V2_LEAF_TEMPLATE
                : AQC_V2_PARENT_TEMPLATE;
        for (QueueProperty<Map<String, String>> template
            : java.util.Arrays.asList(typeSpecific, AQC_V2_TEMPLATE)) {
          String raw = templateEntries(parent, template).get(acl.getName());
          if (value == null && raw != null) {
            value = Resolved.of(raw, ValueSource.TEMPLATE_V2,
                template.getKey(parent.getQueuePath(), null) + acl.getName());
          }
        }
      }
      queue.put(acl, CommonNodeLabelsManager.NO_LABEL, value != null ? value
          : Resolved.of(CapacitySchedulerConfiguration.NONE_ACL,
              ValueSource.DEFAULT, DYNAMIC_QUEUE_DEFAULT));
    }
  }

  /** v2 dynamic queues without a capacity vector get 1w and 100%. */
  private static void resolveDynamicCapacityVectors(ResolvedQueueConfig queue,
      Set<String> labels) {
    for (String label : labels) {
      Resolved<QueueCapacityVector> min = queue.get(CAPACITY_VECTOR, label);
      if (!min.isFailed() && min.getValue().isEmpty()) {
        queue.put(CAPACITY_VECTOR, label, Resolved.of(
            QueueCapacityVector.of(1, ResourceUnitCapacityType.WEIGHT),
            ValueSource.DEFAULT, DYNAMIC_QUEUE_DEFAULT));
        queue.put(MAXIMUM_CAPACITY_VECTOR, label, Resolved.of(
            QueueCapacityVector.of(100, ResourceUnitCapacityType.PERCENTAGE),
            ValueSource.DEFAULT, DYNAMIC_QUEUE_DEFAULT));
      }
    }
  }

  /** PlanQueue vectors are replaced by the percentage capacities. */
  private static void resolvePlanCapacityVectors(ResolvedQueueConfig queue,
      Set<String> labels) {
    for (String label : labels) {
      boolean noLabel = label.equals(CommonNodeLabelsManager.NO_LABEL);
      Resolved<Float> capacity = queue.get(
          noLabel ? CAPACITY : LABELED_CAPACITY, label);
      Resolved<Float> maximum = queue.get(
          noLabel ? MAXIMUM_CAPACITY : LABELED_MAXIMUM_CAPACITY, label);
      if (capacity.isFailed() || maximum.isFailed()) {
        continue;
      }
      queue.put(CAPACITY_VECTOR, label, Resolved.of(QueueCapacityVector.of(
          capacity.getValue() / 100 * 100, ResourceUnitCapacityType.PERCENTAGE),
          ValueSource.DERIVED, CAPACITY.getKey(queue.getQueuePath(), label)));
      queue.put(MAXIMUM_CAPACITY_VECTOR, label, Resolved.of(
          QueueCapacityVector.of(maximum.getValue() / 100 * 100,
              ResourceUnitCapacityType.PERCENTAGE), ValueSource.DERIVED,
          MAXIMUM_CAPACITY.getKey(queue.getQueuePath(), label)));
    }
  }

  /**
   * Derived values: configured absolute capacities, the effective maximum
   * applications and maximum applications per user, for a cluster that has
   * resources. Effective resources and the values computed from them at
   * runtime are not derived.
   * @param extraSibling a queue that is not in the tree but is a sibling of
   *                     the others, or {@code null}
   */
  private static void derive(ResolvedQueueTree tree, ResolvedQueueConfig queue,
      boolean legacyMode, Resolved<Map<String, Float>> siblingWeights) {
    QueuePath path = queue.getQueuePath();
    ResolvedQueueConfig parent =
        path.isRoot() ? null : tree.get(path.getParentObject());
    for (String label : queue.getConfiguredNodeLabels()) {
      if (queue.getKind() == Kind.RESERVATION) {
        // The default reservation queue's entitlement is the whole plan
        Resolved<Float> plan = parent.get(ABSOLUTE_CAPACITY, label);
        queue.put(ABSOLUTE_CAPACITY, label, plan == null || plan.isFailed()
            ? (plan == null ? Resolved.of(0f, ValueSource.DERIVED, null) : plan)
            : Resolved.of(plan.getValue(), ValueSource.DERIVED,
                "plan absolute capacity x entitlement 1.0"));
        continue;
      }
      queue.put(ABSOLUTE_CAPACITY, label, deriveAbsoluteCapacity(queue,
          parent, siblingWeights, label, legacyMode, false));
      queue.put(ABSOLUTE_MAXIMUM_CAPACITY, label, deriveAbsoluteCapacity(queue,
          parent, siblingWeights, label, legacyMode, true));
    }
    if (EFFECTIVE_MAXIMUM_APPLICATIONS.getAppliesTo().contains(
        queue.getKind())) {
      Resolved<Integer> maxApps = deriveMaximumApplications(
          snapshotLookup(tree.getSnapshot()), queue, legacyMode);
      queue.put(EFFECTIVE_MAXIMUM_APPLICATIONS,
          CommonNodeLabelsManager.NO_LABEL, maxApps);
      queue.put(MAXIMUM_APPLICATIONS_PER_USER,
          CommonNodeLabelsManager.NO_LABEL,
          deriveMaximumApplicationsPerUser(queue, maxApps));
    }
  }

  /** Queues read the legacy queue mode flag from their configuration. */
  private static boolean isLegacyQueueMode(ConfigSnapshot snapshot) {
    try {
      return readBoolean(snapshotLookup(snapshot),
          PREFIX + "legacy-queue-mode.enabled",
          CapacitySchedulerConfiguration.DEFAULT_LEGACY_QUEUE_MODE);
    } catch (RuntimeException e) {
      return CapacitySchedulerConfiguration.DEFAULT_LEGACY_QUEUE_MODE;
    }
  }

  private static Resolved<Float> deriveAbsoluteCapacity(
      ResolvedQueueConfig queue, ResolvedQueueConfig parent,
      Resolved<Map<String, Float>> siblingWeights, String label,
      boolean legacyMode, boolean maximum) {
    boolean noLabel = label.equals(CommonNodeLabelsManager.NO_LABEL);
    Resolved<Float> configured = maximum
        ? queue.get(noLabel ? MAXIMUM_CAPACITY : LABELED_MAXIMUM_CAPACITY, label)
        : queue.get(noLabel ? CAPACITY : LABELED_CAPACITY, label);
    if (configured.isFailed()) {
      return configured;
    }
    float parentValue = 1f;
    if (parent != null) {
      Resolved<Float> parentAbsolute = parent.get(
          maximum ? ABSOLUTE_MAXIMUM_CAPACITY : ABSOLUTE_CAPACITY, label);
      if (parentAbsolute == null) {
        parentValue = 0f;
      } else if (parentAbsolute.isFailed()) {
        return parentAbsolute;
      } else {
        parentValue = parentAbsolute.getValue();
      }
    }
    float capacity = configured.getValue() / 100;
    String formula = (maximum ? "maximum-capacity" : "capacity")
        + " x parent absolute";
    if (maximum) {
      // A non-positive maximum keeps the cleared value
      return Resolved.of(capacity > 0f ? capacity * parentValue : 0f,
          ValueSource.DERIVED, formula);
    }
    if (!legacyMode) {
      return Resolved.of(capacity * parentValue, ValueSource.DERIVED, formula);
    }
    // Legacy weight normalization (AbstractParentQueue)
    Resolved<Float> weight = queue.get(CAPACITY_WEIGHT, label);
    if (weight.isFailed()) {
      return weight;
    }
    float normalizedWeight;
    if (parent == null) {
      normalizedWeight = weight.getValue() > 0 ? 1f : 0f;
    } else if (siblingWeights.isFailed()) {
      return Resolved.failed(failureOf(siblingWeights),
          siblingWeights.getSource(), siblingWeights.getSourceDetail());
    } else {
      Float sum = siblingWeights.getValue() == null ? null
          : siblingWeights.getValue().get(label);
      normalizedWeight = sum == null || Math.abs(sum) <= 1e-6 ? 0f
          : weightOf(queue, weight.getValue()) / sum;
    }
    float effective = Math.max(capacity, normalizedWeight);
    return Resolved.of(effective > 0f ? effective * parentValue : 0f,
        ValueSource.DERIVED, "max(capacity, normalized weight)"
            + " x parent absolute");
  }

  /**
   * Sums the weights of siblings per label.
   * @return label to the sum, a {@code null} map when no sibling uses
   *         weights, or the first weight failure
   */
  private static Resolved<Map<String, Float>> weightSums(
      List<ResolvedQueueConfig> siblings) {
    boolean weightMode = false;
    Map<String, Float> sums = new HashMap<>();
    for (ResolvedQueueConfig sibling : siblings) {
      for (String label : sibling.getConfiguredNodeLabels()) {
        Resolved<Float> weight = sibling.get(CAPACITY_WEIGHT, label);
        if (weight == null) {
          continue;
        }
        if (weight.isFailed()) {
          return Resolved.failed(failureOf(weight), weight.getSource(),
              weight.getSourceDetail());
        }
        float value = weightOf(sibling, weight.getValue());
        weightMode |= value >= 0;
        Float sum = sums.get(label);
        sums.put(label, (sum == null ? 0f : sum) + Math.max(0, value));
      }
    }
    return Resolved.of(weightMode ? sums : null, ValueSource.DERIVED, null);
  }

  /** Dynamic queues without a weight count as weight 1. */
  private static float weightOf(ResolvedQueueConfig queue, float weight) {
    return queue.isDynamic() && weight == -1f ? 1f : weight;
  }

  private static Resolved<Integer> deriveMaximumApplications(
      Function<String, String> conf, ResolvedQueueConfig queue,
      boolean legacyMode) {
    Resolved<Integer> explicit = queue.get(MAXIMUM_APPLICATIONS);
    if (explicit.isFailed() || explicit.getValue() >= 0) {
      return explicit;
    }
    try {
      if (queue.getKind() == Kind.PLAN) {
        Resolved<Float> absolute = queue.get(ABSOLUTE_CAPACITY);
        if (absolute.isFailed()) {
          return failedInteger(absolute);
        }
        return Resolved.of(QueueApplicationLimits.scaleByAbsoluteCapacity(
            CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS,
            absolute.getValue()), ValueSource.DERIVED,
            "10000 x absolute capacity");
      }
      int globalPerQueue = readInt(conf,
          CapacitySchedulerConfiguration.QUEUE_GLOBAL_MAX_APPLICATION,
          (int) CapacitySchedulerConfiguration.UNDEFINED);
      int system = readInt(conf,
          CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS,
          CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS);
      int base =
          QueueApplicationLimits.baseMaximumApplications(globalPerQueue, system);
      if (QueueApplicationLimits.usesBaseMaximumApplications(globalPerQueue,
          isAbsoluteResourceType(queue, legacyMode))) {
        return Resolved.of(base, ValueSource.GLOBAL,
            CapacitySchedulerConfiguration.QUEUE_GLOBAL_MAX_APPLICATION);
      }
      int maxApps = -1;
      for (String label : queue.getConfiguredNodeLabels()) {
        Resolved<Float> absolute = queue.get(ABSOLUTE_CAPACITY, label);
        if (absolute.isFailed()) {
          return failedInteger(absolute);
        }
        maxApps = Math.max(maxApps, QueueApplicationLimits
            .scaleByAbsoluteCapacity(base, absolute.getValue()));
      }
      return Resolved.of(maxApps, ValueSource.DERIVED,
          "maximum applications x absolute capacity");
    } catch (RuntimeException e) {
      return Resolved.failed(e, ValueSource.GLOBAL, null);
    }
  }

  private static boolean isAbsoluteResourceType(ResolvedQueueConfig queue,
      boolean legacyMode) {
    String none = CommonNodeLabelsManager.NO_LABEL;
    if (legacyMode) {
      Resolved<Boolean> absolute = queue.get(CAPACITY_IS_ABSOLUTE_RESOURCE);
      return absolute != null && !absolute.isFailed() && absolute.getValue();
    }
    Resolved<QueueCapacityVector> vector = queue.get(CAPACITY_VECTOR, none);
    if (vector == null || vector.isFailed()) {
      return false;
    }
    return QueueCapacityChecks.capacityConfigTypeOf(vector.getValue())
        == CapacityConfigType.ABSOLUTE_RESOURCE;
  }

  private static Resolved<Integer> deriveMaximumApplicationsPerUser(
      ResolvedQueueConfig queue, Resolved<Integer> maxApps) {
    Resolved<Float> userLimit = queue.get(USER_LIMIT);
    Resolved<Float> userLimitFactor = queue.get(USER_LIMIT_FACTOR);
    for (Resolved<?> input : java.util.Arrays.<Resolved<?>>asList(maxApps,
        userLimit, userLimitFactor)) {
      if (input.isFailed()) {
        return failedInteger(input);
      }
    }
    // A PlanQueue does not cap the per-user value at the queue value
    int perUser = QueueApplicationLimits.maximumApplicationsPerUser(
        maxApps.getValue(), userLimit.getValue(), userLimitFactor.getValue(),
        queue.getKind() != Kind.PLAN);
    return Resolved.of(perUser, ValueSource.DERIVED,
        userLimitFactor.getValue() == -1 ? "maximum applications"
            : "maximum applications x user limit x user limit factor");
  }

  private static <T> void readInto(ResolvedQueueConfig queue, Lookup lookup,
      QueueProperty<T> property, String label, Object argument) {
    queue.put(property, label,
        read(lookup, property, queue.getQueuePath(), label, argument));
  }

  private static <T> Resolved<T> read(Lookup lookup, QueueProperty<T> property,
      QueuePath path, String label, Object argument) {
    lookup.reset();
    try {
      T value = property.read(lookup, lookup.queuePrefix, path, label,
          argument);
      return Resolved.of(value, lookup.source(), lookup.sourceDetail());
    } catch (RuntimeException e) {
      return Resolved.failed(e, lookup.source(), lookup.sourceDetail());
    }
  }

  private static <T> Resolved<T> readMap(ResolvedQueueConfig queue,
      Lookup lookup, QueueProperty<T> property, Map<String, String> entries,
      Function<String, String> mapper) {
    String prefix = property.getKey(lookup.queuePath(), null);
    Resolved<T> value;
    try {
      value = Resolved.of(property.read(mapper, lookup.queuePath(),
          CommonNodeLabelsManager.NO_LABEL, entries),
          entries.isEmpty() ? ValueSource.DEFAULT : ValueSource.QUEUE,
          entries.isEmpty() ? null : prefix);
    } catch (RuntimeException e) {
      value = Resolved.failed(e, ValueSource.QUEUE, prefix);
    }
    if (queue != null) {
      queue.put(property, CommonNodeLabelsManager.NO_LABEL, value);
    }
    return value;
  }

  private static <T> Resolved<T> inherit(Resolved<T> parentValue,
      ResolvedQueueConfig parent) {
    if (parentValue.isFailed()) {
      return parentValue;
    }
    return Resolved.of(parentValue.getValue(), ValueSource.PARENT,
        parent.getQueuePath().getFullPath());
  }

  private static RuntimeException failureOf(Resolved<?> failed) {
    try {
      failed.getValue();
    } catch (RuntimeException e) {
      return e;
    }
    throw new IllegalStateException("Not a failed value");
  }

  private static Resolved<Integer> failedInteger(Resolved<?> failed) {
    return Resolved.failed(failureOf(failed), failed.getSource(),
        failed.getSourceDetail());
  }

  private static boolean isManagedParent(ResolvedQueueConfig queue) {
    Resolved<Boolean> enabled = queue.get(AUTO_CREATE_CHILD_QUEUE_ENABLED);
    return queue.getKind() == Kind.PARENT && enabled != null
        && !enabled.isFailed() && enabled.getValue();
  }

  private static Map<String, String> templateEntries(ResolvedQueueConfig parent,
      QueueProperty<Map<String, String>> property) {
    Resolved<Map<String, String>> entries = parent.get(property);
    return entries == null || entries.isFailed()
        ? Collections.<String, String>emptyMap() : entries.getValue();
  }

  /** Finds the wildcard owner path a v2 template entry was taken from. */
  private static OverlayEntry v2Entry(ConfigSnapshot snapshot,
      ResolvedQueueConfig parent, String templatePrefix,
      Map.Entry<String, String> entry) {
    Resolved<Integer> depth = parent.get(AUTO_QUEUE_CREATION_V2_MAX_DEPTH);
    for (QueuePath owner : parent.getQueuePath().getWildcardedQueuePaths(
        depth.getValue())) {
      String sourceKey = QueuePrefixes.getQueuePrefix(owner) + templatePrefix
          + entry.getKey();
      if (snapshot.getRaw(sourceKey) != null) {
        return new OverlayEntry(sourceKey, entry.getValue(),
            ValueSource.TEMPLATE_V2);
      }
    }
    throw new IllegalStateException("No template owner for " + entry.getKey());
  }

  private static Resolved<Set<String>> labels(Set<String> labels) {
    return Resolved.of(labels, ValueSource.DERIVED,
        "accessible-node-labels.<label>.* keys");
  }

  private static Set<String> noLabel() {
    return Collections.singleton(CommonNodeLabelsManager.NO_LABEL);
  }

  private static Set<String> labelsOf(Map<String, Set<String>> labels,
      String path) {
    Set<String> set = labels.get(path);
    return set == null ? noLabel()
        : Collections.unmodifiableSet(new HashSet<>(set));
  }

  private static Set<String> allLabels(Map<String, Set<String>> labels) {
    Set<String> all = new HashSet<>();
    for (Set<String> set : labels.values()) {
      all.addAll(set);
    }
    return all.isEmpty() ? noLabel() : Collections.unmodifiableSet(all);
  }

  private static Function<String, String> snapshotLookup(
      final ConfigSnapshot snapshot) {
    return new Function<String, String>() {
      @Override
      public String apply(String key) {
        return snapshot.get(key);
      }
    };
  }

  /**
   * Expands the variables of the overlay entries the way trunk reads them
   * back: trunk copies the raw text of every entry into the live
   * configuration first, so a reference resolves to an entry of the overlay
   * before a key of the snapshot.
   */
  private static void expandOverlay(final ConfigSnapshot snapshot,
      final Map<String, OverlayEntry> overlay) {
    OverlayConfiguration conf = null;
    for (Map.Entry<String, OverlayEntry> entry : overlay.entrySet()) {
      OverlayEntry value = entry.getValue();
      if (value.raw.indexOf('$') < 0) {
        value.value = value.raw;
        continue;
      }
      if (conf == null) {
        conf = new OverlayConfiguration(snapshot, overlay);
      }
      try {
        value.value = conf.get(entry.getKey());
      } catch (RuntimeException e) {
        value.failure = e;
      }
    }
  }

  /**
   * A configuration of the overlay over the raw snapshot, for the variable
   * substitution of {@link Configuration#get(String)}, which reads the
   * referenced keys through {@link Configuration#getRaw(String)}.
   */
  private static final class OverlayConfiguration extends Configuration {
    private final ConfigSnapshot snapshot;
    private final Map<String, OverlayEntry> overlay;

    private OverlayConfiguration(ConfigSnapshot snapshot,
        Map<String, OverlayEntry> overlay) {
      super(false);
      this.snapshot = snapshot;
      this.overlay = overlay;
      for (Map.Entry<String, OverlayEntry> entry : overlay.entrySet()) {
        set(entry.getKey(), entry.getValue().raw);
      }
    }

    @Override
    public String getRaw(String name) {
      OverlayEntry entry = overlay.get(name);
      return entry != null ? entry.raw : snapshot.getRaw(name);
    }
  }

  /**
   * An entry trunk writes into the live configuration for a dynamic queue.
   * The value is set by {@link #expandOverlay} once every entry is known.
   */
  private static final class OverlayEntry {
    private final String raw;
    private final ValueSource source;
    private final String sourceKey;
    private String value;
    private RuntimeException failure;

    /** A dynamic leaf default, written before the templates. */
    private OverlayEntry(String value) {
      this(DYNAMIC_LEAF_DEFAULT, value, ValueSource.DEFAULT);
    }

    /** A template entry, with the raw text of its template key. */
    private OverlayEntry(String sourceKey, String raw, ValueSource source) {
      this.raw = raw;
      this.source = source;
      this.sourceKey = sourceKey;
    }
  }

  /**
   * Returns values from the overlay and the snapshot, and records the key
   * the last read value came from: a key of the queue wins over a global key,
   * because the getters read their fallbacks first.
   */
  private static final class Lookup implements Function<String, String> {
    private final ConfigSnapshot snapshot;
    private final Map<String, OverlayEntry> overlay;
    private final String queuePrefix;
    private final QueuePath queuePath;
    private String queueKey;
    private String globalKey;

    private Lookup(ConfigSnapshot snapshot, Map<String, OverlayEntry> overlay,
        QueuePath queuePath) {
      this.snapshot = snapshot;
      this.overlay = overlay;
      this.queuePrefix = QueuePrefixes.getQueuePrefix(queuePath);
      this.queuePath = queuePath;
    }

    private QueuePath queuePath() {
      return queuePath;
    }

    private void reset() {
      queueKey = null;
      globalKey = null;
    }

    @Override
    public String apply(String key) {
      String value;
      OverlayEntry entry = overlay == null ? null : overlay.get(key);
      if (entry != null) {
        if (entry.failure != null) {
          queueKey = key;
          throw entry.failure;
        }
        value = entry.value;
      } else {
        value = snapshot.get(key);
      }
      if (value != null) {
        if (key.startsWith(queuePrefix)) {
          queueKey = key;
        } else {
          globalKey = key;
        }
      }
      return value;
    }

    private ValueSource source() {
      if (queueKey != null) {
        OverlayEntry entry = overlay == null ? null : overlay.get(queueKey);
        return entry == null ? ValueSource.QUEUE : entry.source;
      }
      return globalKey != null ? ValueSource.GLOBAL : ValueSource.DEFAULT;
    }

    private String sourceDetail() {
      if (queueKey != null) {
        OverlayEntry entry = overlay == null ? null : overlay.get(queueKey);
        return entry == null ? queueKey : entry.sourceKey;
      }
      return globalKey;
    }
  }
}
