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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueStructureChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueConfigResolver;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.AUTO_CREATE_CHILD_QUEUE_ENABLED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.AUTO_QUEUE_CREATION_V2_ENABLED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.MINIMUM_RESOURCE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.QUEUES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.RESERVABLE;

/**
 * Structural checks of the parsed hierarchy ({@link QueueStructureChecks}):
 * root has children, only leaves are reservable, and an AQC v1 leaf queue
 * template does not configure an absolute minimum resource under a
 * percentage parent.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class StructureRule implements ValidationRule {
  /** H01. */
  public static final String MISSING_CHILD_QUEUES = "missing-child-queues";
  /** H02. */
  public static final String RESERVABLE_PARENT_QUEUE =
      "reservable-parent-queue";
  /** H04. */
  public static final String MANAGED_PARENT_TEMPLATE_TYPE =
      "managed-parent-template-type";
  /** A child queue listed twice, warning. */
  public static final String DUPLICATE_CHILD_QUEUE = "duplicate-child-queue";
  /** AQC v1 and v2 both enabled on a queue, warning. */
  public static final String CONFLICTING_AUTO_QUEUE_CREATION =
      "conflicting-auto-queue-creation";

  @Override
  public String getId() {
    return "queue-structure";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    Map<String, Set<String>> labelsByQueue = null;
    for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
      if (queue.isDynamic() || queue.getKind() == Kind.RESERVATION) {
        continue;
      }
      QueuePath path = queue.getQueuePath();
      Resolved<List<String>> children = queue.get(QUEUES);
      Resolved<Boolean> reservable = queue.get(RESERVABLE);
      Resolved<Boolean> v1 = queue.get(AUTO_CREATE_CHILD_QUEUE_ENABLED);
      Resolved<Boolean> v2 = queue.get(AUTO_QUEUE_CREATION_V2_ENABLED);
      if (!RuleSupport.ok(children) || !RuleSupport.ok(reservable)
          || !RuleSupport.ok(v1) || !RuleSupport.ok(v2)) {
        continue;
      }
      ClusterFacts.QueueFacts live =
          context.getFacts().getQueue(path.getFullPath());
      boolean dynamicParent = live != null && live.isDynamic()
          && live.getKind().isParent();
      int childCount = children.getValue().size();
      boolean autoQueueCreationEnabledParent =
          dynamicParent || v1.getValue() || v2.getValue();
      String error = QueueStructureChecks.isParent(childCount,
          autoQueueCreationEnabledParent)
          ? QueueStructureChecks.checkReservable(path.getFullPath(), childCount,
              reservable.getValue(), autoQueueCreationEnabledParent)
          : QueueStructureChecks.checkRootHasChildQueues(path.getLeafName(),
              path.isRoot(), childCount, autoQueueCreationEnabledParent);
      if (error != null) {
        issues.add(ValidationIssue.error(path.getFullPath(),
            RuleSupport.key(queue, path.isRoot() ? QUEUES : RESERVABLE, ""),
            path.isRoot() ? MISSING_CHILD_QUEUES : RESERVABLE_PARENT_QUEUE,
            error));
      }

      Set<String> names = new HashSet<>();
      for (String child : children.getValue()) {
        if (!names.add(child)) {
          issues.add(ValidationIssue.warning(path.getFullPath(),
              RuleSupport.key(queue, QUEUES, ""), DUPLICATE_CHILD_QUEUE,
              "Queue " + child + " is listed more than once under "
                  + path.getFullPath() + "; it is set up only once."));
        }
      }
      if (v1.getValue() && v2.getValue()) {
        issues.add(ValidationIssue.warning(path.getFullPath(),
            RuleSupport.key(queue, AUTO_QUEUE_CREATION_V2_ENABLED, ""),
            CONFLICTING_AUTO_QUEUE_CREATION, "Queue " + path.getFullPath()
                + " enables both auto queue creation v1 and v2; v1 is"
                + " used."));
      }

      if (context.isManagedParent(queue)) {
        if (labelsByQueue == null) {
          try {
            labelsByQueue = QueueConfigResolver.configuredNodeLabelsByQueue(
                context.getProposed());
          } catch (RuntimeException e) {
            // Reported by the node label rule
            return;
          }
        }
        checkTemplateType(context, queue, labelsByQueue, issues);
      }
    }
  }

  /** H04, as ManagedParentQueue checks it. */
  private static void checkTemplateType(ValidationContext context,
      ResolvedQueueConfig parent, Map<String, Set<String>> labelsByQueue,
      List<ValidationIssue> issues) {
    CapacityConfigType parentType = CapacityRule.capacityConfigType(context,
        parent);
    if (parentType == null) {
      return;
    }
    QueuePath templatePath = QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(parent.getQueuePath());
    Set<String> templateLabels = labelsByQueue.get(templatePath.getFullPath());
    if (templateLabels == null) {
      templateLabels = Collections.singleton(
          RMNodeLabelsManager.NO_LABEL);
    }
    for (String label : templateLabels) {
      Resource templateMin;
      try {
        templateMin = MINIMUM_RESOURCE.read(context.getProposed()::get,
            templatePath, label, null);
      } catch (RuntimeException e) {
        issues.add(ValidationIssue.error(parent.getQueuePath().getFullPath(),
            MINIMUM_RESOURCE.getKey(templatePath, label),
            CapacityRule.INVALID_CAPACITY_RESOURCE, e));
        return;
      }
      String error = QueueStructureChecks.checkLeafQueueTemplateConfigType(
          parent.getQueuePath().getFullPath(), parentType, templateMin);
      if (error != null) {
        issues.add(ValidationIssue.error(parent.getQueuePath().getFullPath(),
            MINIMUM_RESOURCE.getKey(templatePath, label),
            MANAGED_PARENT_TEMPLATE_TYPE, error));
        return;
      }
    }
  }
}
