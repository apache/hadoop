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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueLabelChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueStructureChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueConfigResolver;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ValueSource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.ACCESSIBLE_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.CONFIGURED_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.DEFAULT_NODE_LABEL_EXPRESSION;

/**
 * Node label checks ({@link QueueLabelChecks}): the labeled keys can be
 * indexed, a child's accessible labels are a subset of its parent's, a
 * leaf can access the labels of its default label expression, and an AQC v1
 * leaf queue template only uses labels of its managed parent.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class NodeLabelRule implements ValidationRule {
  /** A labeled key that ends right after the label. */
  public static final String INVALID_NODE_LABEL_KEY = "invalid-node-label-key";
  /** L01 and L02. */
  public static final String CHILD_LABELS_NOT_SUBSET =
      "child-labels-not-subset";
  /** L03. */
  public static final String INVALID_DEFAULT_LABEL_EXPRESSION =
      "invalid-default-label-expression";
  /** L04, warning. */
  public static final String ROOT_ACCESSIBLE_LABELS_IGNORED =
      "root-accessible-labels-ignored";
  /** L05. */
  public static final String INVALID_TEMPLATE_LABEL = "invalid-template-label";
  /** A label the cluster does not know, warning. */
  public static final String UNKNOWN_NODE_LABEL = "unknown-node-label";

  @Override
  public String getId() {
    return "node-labels";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    ResolvedQueueConfig root = context.getTree().getRoot();
    Resolved<Set<String>> rootLabels = root.get(CONFIGURED_NODE_LABELS);
    Map<String, Set<String>> labelsByQueue = null;
    if (rootLabels.isFailed()) {
      try {
        rootLabels.getValue();
      } catch (RuntimeException e) {
        issues.add(ValidationIssue.error(null, null, INVALID_NODE_LABEL_KEY,
            e));
      }
    } else {
      labelsByQueue = QueueConfigResolver.configuredNodeLabelsByQueue(
          context.getProposed());
    }
    if (context.getProposed().get(
        RuleSupport.key(root, ACCESSIBLE_NODE_LABELS, "")) != null) {
      issues.add(ValidationIssue.warning(root.getQueuePath().getFullPath(),
          RuleSupport.key(root, ACCESSIBLE_NODE_LABELS, ""),
          ROOT_ACCESSIBLE_LABELS_IGNORED, "Accessible node labels for root"
              + " queue will be ignored, it will be automatically set to"
              + " \"*\"."));
    }

    Set<String> clusterLabels = context.getFacts().getNodeLabels();
    for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
      ResolvedQueueConfig parent = context.getParent(queue);
      Resolved<Set<String>> accessible = queue.get(ACCESSIBLE_NODE_LABELS);
      if (parent == null || !RuleSupport.ok(accessible)) {
        continue;
      }
      String path = queue.getQueuePath().getFullPath();
      String accessibleKey = RuleSupport.key(queue, ACCESSIBLE_NODE_LABELS, "");
      Resolved<Set<String>> parentAccessible =
          parent.get(ACCESSIBLE_NODE_LABELS);
      if (RuleSupport.ok(parentAccessible)) {
        String error = QueueLabelChecks.checkAccessibleLabelsSubset(
            false, accessible.getValue(), parentAccessible.getValue());
        if (error != null) {
          issues.add(ValidationIssue.error(path,
              RuleSupport.keyOf(queue, ACCESSIBLE_NODE_LABELS, "", accessible),
              CHILD_LABELS_NOT_SUBSET, error));
        }
      }

      if (queue.getKind() == Kind.LEAF || queue.getKind() == Kind.RESERVATION) {
        Resolved<String> expression = queue.get(DEFAULT_NODE_LABEL_EXPRESSION);
        if (RuleSupport.ok(expression)) {
          String error = QueueLabelChecks.checkDefaultLabelExpression(
              path, accessible.getValue(), expression.getValue());
          if (error != null) {
            issues.add(ValidationIssue.error(path, RuleSupport.keyOf(queue,
                DEFAULT_NODE_LABEL_EXPRESSION, "", expression),
                INVALID_DEFAULT_LABEL_EXPRESSION, error));
          }
        }
      }

      if (accessible.getSource() == ValueSource.PARENT
          || accessible.getValue() == null) {
        continue;
      }
      Set<String> unknown = new TreeSet<>();
      for (String label : accessible.getValue()) {
        if (!label.equals(RMNodeLabelsManager.ANY)
            && !label.equals(RMNodeLabelsManager.NO_LABEL)
            && !clusterLabels.contains(label)) {
          unknown.add(label);
        }
      }
      if (!unknown.isEmpty()) {
        issues.add(ValidationIssue.warning(path, accessibleKey,
            UNKNOWN_NODE_LABEL, "Queue " + path + " can access node labels "
                + unknown + " that do not exist in the cluster."));
      }
    }

    if (labelsByQueue != null) {
      for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
        if (context.isManagedParent(queue)) {
          checkTemplateLabels(context, queue, labelsByQueue, issues);
        }
      }
    }
  }

  /** L05, as GuaranteedOrZeroCapacityOverTimePolicy checks it. */
  private static void checkTemplateLabels(ValidationContext context,
      ResolvedQueueConfig parent, Map<String, Set<String>> labelsByQueue,
      List<ValidationIssue> issues) {
    try {
      if (!(QueueStructureChecks.loadQueueManagementPolicy(
          context.getConfiguration(), parent.getQueuePath())
          instanceof GuaranteedOrZeroCapacityOverTimePolicy)) {
        return;
      }
    } catch (YarnRuntimeException e) {
      // Reported by the policy rule
      return;
    }
    Resolved<Set<String>> accessible = parent.get(ACCESSIBLE_NODE_LABELS);
    if (!RuleSupport.ok(accessible)) {
      return;
    }
    String templatePath = QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(parent.getQueuePath())
        .getFullPath();
    Set<String> templateLabels = labelsByQueue.get(templatePath);
    if (templateLabels == null) {
      templateLabels = Collections.singleton(RMNodeLabelsManager.NO_LABEL);
    }
    boolean anyLabel = accessible.getValue().contains(RMNodeLabelsManager.ANY);
    Set<String> parentLabels = new HashSet<>(anyLabel
        ? parent.getConfiguredNodeLabels() : accessible.getValue());
    parentLabels.add(RMNodeLabelsManager.NO_LABEL);
    String error = QueueLabelChecks.checkLeafQueueTemplateLabels(
        parent.getQueuePath().getFullPath(),
        new LinkedHashSet<>(templateLabels), parentLabels);
    if (error != null) {
      // With "*" the parent's labels also include the labels it uses at
      // runtime, which validation cannot see
      issues.add(new ValidationIssue(parent.getQueuePath().getFullPath(),
          null, INVALID_TEMPLATE_LABEL, RuleSupport.severity(!anyLabel),
          error));
    }
  }
}
