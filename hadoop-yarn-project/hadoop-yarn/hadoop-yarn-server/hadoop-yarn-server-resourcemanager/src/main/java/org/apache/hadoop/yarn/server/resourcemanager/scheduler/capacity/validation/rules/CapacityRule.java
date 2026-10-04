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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityChecks.LabelCapacity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityChecks.QueueCapacityInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueConfigResolver;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue.Severity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.ALLOW_ZERO_CAPACITY_SUM;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.CAPACITY_IS_ABSOLUTE_RESOURCE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.CAPACITY_VECTOR;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.CAPACITY_WEIGHT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.LABELED_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.LABELED_MAXIMUM_AM_RESOURCE_PERCENT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.LABELED_MAXIMUM_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_CAPACITY_VECTOR;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MAXIMUM_RESOURCE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MINIMUM_RESOURCE;

/**
 * Capacity checks ({@link QueueCapacityChecks}): capacity values parse, the
 * configured minimum and maximum resources fit, and in legacy queue mode a
 * queue and its children do not mix capacity types and the children's
 * capacities add up. The capacity calculation warnings of non-legacy mode
 * (C17 to C22) need effective resources and are not evaluated.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class CapacityRule implements ValidationRule {
  /** C01 to C04, a capacity, maximum capacity or weight that fails. */
  public static final String INVALID_CAPACITY = "invalid-capacity";
  /** C04, a malformed absolute resource. */
  public static final String INVALID_CAPACITY_RESOURCE =
      "invalid-capacity-resource";
  /** C04, a malformed capacity vector. */
  public static final String INVALID_CAPACITY_VECTOR =
      "invalid-capacity-vector";
  /** C06. */
  public static final String MAXIMUM_RESOURCE_EXCEEDS_PARENT =
      "maximum-resource-exceeds-parent";
  /** C07. */
  public static final String MINIMUM_RESOURCE_EXCEEDS_MAXIMUM =
      "minimum-resource-exceeds-maximum";
  /** C08. */
  public static final String MIXED_CAPACITY_CONFIG_TYPE =
      "mixed-capacity-config-type";
  /** C09. */
  public static final String MIXED_CHILDREN_CAPACITY_TYPES =
      "mixed-children-capacity-types";
  /** C10. */
  public static final String ABSOLUTE_CAPACITY_MISMATCH =
      "absolute-capacity-mismatch";
  /** C11. */
  public static final String CHILDREN_MINIMUM_RESOURCE_EXCEEDS_PARENT =
      "children-minimum-resource-exceeds-parent";
  /** C12 to C14. */
  public static final String CHILDREN_CAPACITY_SUM = "children-capacity-sum";
  /** C15, C09 over the live children including dynamic queues. */
  public static final String MIXED_DYNAMIC_CHILDREN_CAPACITY_TYPES =
      "mixed-dynamic-children-capacity-types";
  /** Percentage capacity above the maximum capacity, warning. */
  public static final String CAPACITY_EXCEEDS_MAXIMUM_CAPACITY =
      "capacity-exceeds-maximum-capacity";
  /** Unlabeled maximum capacity outside [0, 100], warning. */
  public static final String MAXIMUM_CAPACITY_OUT_OF_RANGE =
      "maximum-capacity-out-of-range";

  private static final String NO_LABEL = RMNodeLabelsManager.NO_LABEL;

  @Override
  public String getId() {
    return "capacity";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    Map<QueuePath, Map<String, Resource>> maximumResources = new HashMap<>();
    Set<QueuePath> failed = new HashSet<>();
    for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
      Set<String> reported = new HashSet<>();
      if (!checkValues(queue, reported, issues)) {
        failed.add(queue.getQueuePath());
      }
      checkResourceLimits(context, queue, maximumResources, reported, issues);
      if (context.isLegacyQueueMode()) {
        checkConfigTypeConsistent(queue, issues);
      }
      if (!queue.isDynamic()) {
        warnMaximumCapacity(queue, issues);
      }
      if (context.isManagedParent(queue)) {
        checkTemplateValues(context, queue, issues);
      }
    }

    if (!context.isLegacyQueueMode()) {
      return;
    }
    for (ResolvedQueueConfig parent : context.getTree().getQueues()) {
      if (parent.getKind() != Kind.ROOT && parent.getKind() != Kind.PARENT) {
        continue;
      }
      List<ResolvedQueueConfig> configured =
          context.getConfiguredChildren(parent);
      boolean childrenPassed = configured.isEmpty()
          || checkChildren(context, parent, configured, failed, issues);
      List<ResolvedQueueConfig> all =
          context.getTree().getChildren(parent.getQueuePath());
      // The capacities of AQC v1 leaves follow their entitlements at
      // runtime, which validation cannot see
      if (childrenPassed && all.size() > configured.size()
          && !context.isManagedParent(parent)) {
        checkDynamicChildren(parent, all, failed, issues);
      }
    }
  }

  /**
   * C01 to C04: the values queue setup reads, in its order.
   * @return true if every value resolved
   */
  private static boolean checkValues(ResolvedQueueConfig queue,
      Set<String> reported, List<ValidationIssue> issues) {
    boolean ok = true;
    for (String label : queue.getConfiguredNodeLabels()) {
      boolean noLabel = label.equals(NO_LABEL);
      ok &= !report(queue, noLabel ? CAPACITY : LABELED_CAPACITY, label,
          INVALID_CAPACITY, reported, issues);
      ok &= !report(queue,
          noLabel ? MAXIMUM_CAPACITY : LABELED_MAXIMUM_CAPACITY, label,
          INVALID_CAPACITY, reported, issues);
      report(queue, LABELED_MAXIMUM_AM_RESOURCE_PERCENT, label,
          ValueRule.INVALID_VALUE, reported, issues);
      ok &= !report(queue, CAPACITY_WEIGHT, label, INVALID_CAPACITY,
          reported, issues);
    }
    for (String label : queue.getConfiguredNodeLabels()) {
      ok &= !report(queue, MINIMUM_RESOURCE, label, INVALID_CAPACITY_RESOURCE,
          reported, issues);
      ok &= !report(queue, MAXIMUM_RESOURCE, label, INVALID_CAPACITY_RESOURCE,
          reported, issues);
    }
    for (String label : queue.getConfiguredNodeLabels()) {
      ok &= !report(queue, CAPACITY_VECTOR, label, INVALID_CAPACITY_VECTOR,
          reported, issues);
      ok &= !report(queue, MAXIMUM_CAPACITY_VECTOR, label,
          INVALID_CAPACITY_VECTOR, reported, issues);
    }
    return ok;
  }

  /** Reports a failed value once per key. */
  private static boolean report(ResolvedQueueConfig queue,
      QueueProperty<?> property, String label, String ruleId,
      Set<String> reported, List<ValidationIssue> issues) {
    Resolved<?> value = queue.get(property, label);
    if (value == null || !value.isFailed()) {
      return false;
    }
    if (reported.add(RuleSupport.keyOf(queue, property, label, value))) {
      RuleSupport.reportFailure(issues, queue, property, label, ruleId,
          Severity.ERROR);
    }
    return true;
  }

  /** C06 and C07, as AbstractCSQueue.updateConfigurableResourceLimits. */
  private static void checkResourceLimits(ValidationContext context,
      ResolvedQueueConfig queue,
      Map<QueuePath, Map<String, Resource>> maximumResources,
      Set<String> reported, List<ValidationIssue> issues) {
    ResolvedQueueConfig parent = context.getParent(queue);
    Map<String, Resource> parentMaximums = parent == null ? null
        : maximumResources.get(parent.getQueuePath());
    if (parent != null && parentMaximums == null) {
      return;
    }
    Resource cluster = context.getFacts().getClusterResource();
    ResourceCalculator calculator =
        context.getFacts().getResourceCalculator();
    String path = queue.getQueuePath().getFullPath();
    Map<String, Resource> maximums = new HashMap<>();
    for (String label : queue.getConfiguredNodeLabels()) {
      Resolved<Resource> min = queue.get(MINIMUM_RESOURCE, label);
      Resolved<Resource> max = queue.get(MAXIMUM_RESOURCE, label);
      if (!RuleSupport.ok(min) || !RuleSupport.ok(max)) {
        return;
      }
      Resource maximum = max.getValue();
      if (parent != null) {
        Resource parentMax = parentMaximums.get(label);
        if (parentMax == null) {
          parentMax = Resources.none();
        }
        String error = QueueCapacityChecks.checkMaxResourceWithinParent(path,
            maximum, parentMax, cluster, calculator);
        if (error != null) {
          issues.add(ValidationIssue.error(path,
              RuleSupport.keyOf(queue, MAXIMUM_RESOURCE, label, max),
              MAXIMUM_RESOURCE_EXCEEDS_PARENT, error));
        }
        maximum = QueueCapacityChecks.inheritMaxResourceFromParent(
            min.getValue(), maximum, parentMax);
      }
      String error = QueueCapacityChecks.checkMinResourceWithinMax(path,
          min.getValue(), maximum, cluster, calculator);
      if (error != null) {
        issues.add(ValidationIssue.error(path,
            RuleSupport.keyOf(queue, MINIMUM_RESOURCE, label, min),
            MINIMUM_RESOURCE_EXCEEDS_MAXIMUM, error));
      }
      maximums.put(label, maximum);
    }
    maximumResources.put(queue.getQueuePath(), maximums);
  }

  /** C08, legacy queue mode. */
  private static void checkConfigTypeConsistent(ResolvedQueueConfig queue,
      List<ValidationIssue> issues) {
    QueuePath path = queue.getQueuePath();
    CapacityConfigType established = null;
    for (String label : queue.getConfiguredNodeLabels()) {
      Resolved<Boolean> absolute =
          queue.get(CAPACITY_IS_ABSOLUTE_RESOURCE, label);
      if (!RuleSupport.ok(absolute)) {
        return;
      }
      CapacityConfigType type =
          QueueCapacityChecks.capacityConfigTypeOf(absolute.getValue());
      if (established == null) {
        established = type;
        continue;
      }
      String error = QueueCapacityChecks.checkCapacityConfigTypeConsistent(
          path.getFullPath(), path.isRoot(), true, established, type);
      if (error != null) {
        issues.add(ValidationIssue.error(path.getFullPath(),
            RuleSupport.key(queue, CAPACITY, label),
            MIXED_CAPACITY_CONFIG_TYPE, error));
        return;
      }
    }
  }

  /**
   * The capacity configuration type queue setup derives: in legacy queue
   * mode absolute or percentage, otherwise from the capacity vector types.
   * @param context the validation context
   * @param queue the queue
   * @return the type of the first configured label, or null if a value
   *         failed to resolve
   */
  static CapacityConfigType capacityConfigType(ValidationContext context,
      ResolvedQueueConfig queue) {
    for (String label : queue.getConfiguredNodeLabels()) {
      if (context.isLegacyQueueMode()) {
        Resolved<Boolean> absolute =
            queue.get(CAPACITY_IS_ABSOLUTE_RESOURCE, label);
        if (!RuleSupport.ok(absolute)) {
          return null;
        }
        return QueueCapacityChecks.capacityConfigTypeOf(absolute.getValue());
      }
      Resolved<QueueCapacityVector> vector = queue.get(CAPACITY_VECTOR, label);
      if (!RuleSupport.ok(vector)) {
        return null;
      }
      return QueueCapacityChecks.capacityConfigTypeOf(vector.getValue());
    }
    return CapacityConfigType.NONE;
  }

  /**
   * C09 to C14, as AbstractParentQueue.setChildQueues.
   * @return true if the checks passed
   */
  private static boolean checkChildren(ValidationContext context,
      ResolvedQueueConfig parent, List<ResolvedQueueConfig> children,
      Set<QueuePath> failed, List<ValidationIssue> issues) {
    if (failed.contains(parent.getQueuePath())) {
      return false;
    }
    for (ResolvedQueueConfig child : children) {
      if (failed.contains(child.getQueuePath())) {
        return false;
      }
    }
    String path = parent.getQueuePath().getFullPath();
    String childrenKey = RuleSupport.key(parent, "queues");
    Set<String> labels = parent.getConfiguredNodeLabels();
    List<QueueCapacityInput> childInputs = inputs(children, labels);
    List<QueueCapacityInput> parentInput =
        inputs(Collections.singletonList(parent), labels);
    if (childInputs == null || parentInput == null) {
      return false;
    }

    String error = QueueCapacityChecks.checkCapacityTypesNotMixed(path,
        labels, childInputs);
    if (error == null) {
      error = QueueCapacityChecks.checkCapacityTypesNotMixed(path, labels,
          parentInput);
    }
    if (error != null) {
      issues.add(ValidationIssue.error(path, childrenKey,
          MIXED_CHILDREN_CAPACITY_TYPES, error));
      return false;
    }
    QueueCapacityType childrenType =
        QueueCapacityChecks.getCapacityConfigurationType(labels, childInputs);
    QueueCapacityType parentType =
        QueueCapacityChecks.getCapacityConfigurationType(labels, parentInput);

    if (childrenType == QueueCapacityType.ABSOLUTE_RESOURCE
        || parentType == QueueCapacityType.ABSOLUTE_RESOURCE) {
      error = QueueCapacityChecks.checkAbsoluteResourceUsedByParentAndChildren(
          path, parentType, childrenType);
      if (error != null) {
        issues.add(ValidationIssue.error(path, childrenKey,
            ABSOLUTE_CAPACITY_MISMATCH, error));
        return false;
      }
      for (String label : labels) {
        List<Resource> childrenMinimums = new ArrayList<>();
        for (ResolvedQueueConfig child : children) {
          Resolved<Resource> min = child.get(MINIMUM_RESOURCE, label);
          childrenMinimums.add(min == null ? Resources.none()
              : min.getValue());
        }
        error = QueueCapacityChecks.checkChildrenMinResourceWithinParent(
            parent.getQueuePath().getLeafName(),
            parent.get(MINIMUM_RESOURCE, label).getValue(), childrenMinimums,
            context.getFacts().getResourceByLabel(label),
            context.getFacts().getResourceCalculator());
        if (error != null) {
          issues.add(ValidationIssue.error(path,
              RuleSupport.key(parent, MINIMUM_RESOURCE, label),
              CHILDREN_MINIMUM_RESOURCE_EXCEEDS_PARENT, error));
          return false;
        }
      }
    }

    if (childrenType == QueueCapacityType.PERCENT) {
      boolean allowZeroCapacitySum =
          parent.get(ALLOW_ZERO_CAPACITY_SUM).getValue();
      for (String label : labels) {
        error = QueueCapacityChecks.checkChildrenCapacitySum(
            parent.getQueuePath().getLeafName(), label, parentType,
            parentInput.get(0).get(label).getCapacity(), allowZeroCapacitySum,
            childInputs);
        if (error != null) {
          issues.add(ValidationIssue.error(path, childrenKey,
              CHILDREN_CAPACITY_SUM, error));
          return false;
        }
      }
    }
    return true;
  }

  /**
   * C15: in legacy queue mode the capacity update repeats C09 over the live
   * children of a parent, including its dynamic queues.
   */
  private static void checkDynamicChildren(ResolvedQueueConfig parent,
      List<ResolvedQueueConfig> children, Set<QueuePath> failed,
      List<ValidationIssue> issues) {
    for (ResolvedQueueConfig child : children) {
      if (failed.contains(child.getQueuePath())) {
        return;
      }
    }
    String path = parent.getQueuePath().getFullPath();
    List<QueueCapacityInput> inputs =
        inputs(children, parent.getConfiguredNodeLabels());
    if (inputs == null) {
      return;
    }
    String error = QueueCapacityChecks.checkCapacityTypesNotMixed(path,
        parent.getConfiguredNodeLabels(), inputs);
    if (error != null) {
      issues.add(ValidationIssue.error(path, RuleSupport.key(parent, "queues"),
          MIXED_DYNAMIC_CHILDREN_CAPACITY_TYPES, error));
    }
  }

  /**
   * The capacity values queue setup stores for each label of the parent;
   * labels a queue does not configure report the unset values.
   * @return the inputs, or null if a value failed to resolve
   */
  private static List<QueueCapacityInput> inputs(
      List<ResolvedQueueConfig> queues, Set<String> labels) {
    List<QueueCapacityInput> inputs = new ArrayList<>(queues.size());
    for (ResolvedQueueConfig queue : queues) {
      Map<String, LabelCapacity> byLabel = new HashMap<>();
      for (String label : labels) {
        if (!queue.getConfiguredNodeLabels().contains(label)) {
          continue;
        }
        Resolved<Float> capacity = queue.get(
            label.equals(NO_LABEL) ? CAPACITY : LABELED_CAPACITY, label);
        Resolved<Float> weight = queue.get(CAPACITY_WEIGHT, label);
        Resolved<QueueCapacityVector> vector = queue.get(CAPACITY_VECTOR,
            label);
        Resolved<Boolean> absolute =
            queue.get(CAPACITY_IS_ABSOLUTE_RESOURCE, label);
        if (!RuleSupport.ok(capacity) || !RuleSupport.ok(weight)
            || !RuleSupport.ok(vector) || !RuleSupport.ok(absolute)) {
          return null;
        }
        // Queue setup stores the weight of a vector that only has one
        // weight for every resource in place of the configured weight
        Float uniformWeight =
            QueueCapacityChecks.uniformWeightOf(vector.getValue());
        byLabel.put(label, new LabelCapacity(capacity.getValue() / 100,
            uniformWeight != null ? uniformWeight : weight.getValue(),
            absolute.getValue()));
      }
      inputs.add(new QueueCapacityInput(queue.getQueuePath().getFullPath(),
          byLabel));
    }
    return inputs;
  }

  /** Percentage capacities that trunk does not range check, warnings. */
  private static void warnMaximumCapacity(ResolvedQueueConfig queue,
      List<ValidationIssue> issues) {
    if (queue.getQueuePath().isRoot()) {
      return;
    }
    String path = queue.getQueuePath().getFullPath();
    for (String label : queue.getConfiguredNodeLabels()) {
      boolean noLabel = label.equals(NO_LABEL);
      Resolved<Float> capacity =
          queue.get(noLabel ? CAPACITY : LABELED_CAPACITY, label);
      Resolved<Float> maximum = queue.get(
          noLabel ? MAXIMUM_CAPACITY : LABELED_MAXIMUM_CAPACITY, label);
      Resolved<Float> weight = queue.get(CAPACITY_WEIGHT, label);
      Resolved<Boolean> absolute =
          queue.get(CAPACITY_IS_ABSOLUTE_RESOURCE, label);
      Resolved<QueueCapacityVector> maxVector =
          queue.get(MAXIMUM_CAPACITY_VECTOR, label);
      if (!RuleSupport.ok(capacity) || !RuleSupport.ok(maximum)
          || !RuleSupport.ok(weight) || !RuleSupport.ok(absolute)
          || !RuleSupport.ok(maxVector) || absolute.getValue()
          || weight.getValue() >= 0 || !isPercentage(maxVector.getValue())) {
        continue;
      }
      String maximumKey = RuleSupport.keyOf(queue,
          noLabel ? MAXIMUM_CAPACITY : LABELED_MAXIMUM_CAPACITY, label,
          maximum);
      if (noLabel && (maximum.getValue() < 0 || maximum.getValue() > 100)) {
        issues.add(ValidationIssue.warning(path, maximumKey,
            MAXIMUM_CAPACITY_OUT_OF_RANGE, "Maximum capacity "
                + maximum.getValue() + " of queue " + path
                + " is outside [0, 100]."));
      } else if (capacity.getValue() > maximum.getValue()) {
        issues.add(ValidationIssue.warning(path, maximumKey,
            CAPACITY_EXCEEDS_MAXIMUM_CAPACITY, "Capacity "
                + capacity.getValue() + " of queue " + path
                + (noLabel ? "" : " for node label " + label)
                + " is greater than its maximum capacity "
                + maximum.getValue() + "."));
      }
    }
  }

  private static boolean isPercentage(QueueCapacityVector vector) {
    Set<ResourceUnitCapacityType> types = vector.getDefinedCapacityTypes();
    return types.isEmpty() || (types.size() == 1
        && types.iterator().next() == ResourceUnitCapacityType.PERCENTAGE);
  }

  /**
   * C01 to C04 for the AQC v1 leaf queue template, whose capacities the
   * managed parent loads when it is built.
   */
  private static void checkTemplateValues(ValidationContext context,
      ResolvedQueueConfig parent, List<ValidationIssue> issues) {
    QueuePath templatePath = QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(parent.getQueuePath());
    Set<String> labels;
    try {
      labels = QueueConfigResolver.configuredNodeLabelsByQueue(
          context.getProposed()).get(templatePath.getFullPath());
    } catch (RuntimeException e) {
      // Reported by the node label rule
      return;
    }
    if (labels == null) {
      labels = Collections.singleton(NO_LABEL);
    }
    for (String label : labels) {
      boolean noLabel = label.equals(NO_LABEL);
      List<QueueProperty<?>> properties = new ArrayList<>();
      properties.add(noLabel ? CAPACITY : LABELED_CAPACITY);
      properties.add(noLabel ? MAXIMUM_CAPACITY : LABELED_MAXIMUM_CAPACITY);
      properties.add(CAPACITY_WEIGHT);
      for (QueueProperty<?> property : properties) {
        try {
          property.read(context.getProposed()::get, templatePath, label, null);
        } catch (RuntimeException e) {
          issues.add(ValidationIssue.error(parent.getQueuePath().getFullPath(),
              property.getKey(templatePath, label), INVALID_CAPACITY,
              e));
          return;
        }
      }
    }
  }
}
