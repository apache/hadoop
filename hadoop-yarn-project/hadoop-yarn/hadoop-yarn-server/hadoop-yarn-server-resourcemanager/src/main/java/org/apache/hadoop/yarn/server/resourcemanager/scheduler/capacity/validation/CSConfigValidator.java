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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueTree;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.AllocationRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.CapacityRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.HierarchyRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.LimitRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.NodeLabelRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlacementRulesRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.StateRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.StructureRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.ValueRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates a proposed Capacity Scheduler configuration without constructing
 * queues or touching live scheduler state: the configuration is resolved
 * once, every rule checks the resolved tree against the cluster facts, and
 * the issues are ordered the way a refresh evaluates the checks, so the first
 * ERROR is the failure a refresh would report.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class CSConfigValidator {
  private static final Logger LOG =
      LoggerFactory.getLogger(CSConfigValidator.class);

  private static final List<ValidationRule> RULES = Collections.unmodifiableList(
      Arrays.<ValidationRule>asList(new AllocationRule(), new StructureRule(),
          new NodeLabelRule(), new CapacityRule(), new StateRule(),
          new LimitRule(), new ValueRule(), new HierarchyRule(),
          new PlacementRulesRule()));

  /**
   * @param proposed proposed configuration
   * @param facts live cluster facts
   * @return the issues found, without explain
   */
  public ValidationResult validate(ConfigSnapshot proposed,
      ClusterFacts facts) {
    return validate(proposed, facts, null);
  }

  /**
   * Validates and, when {@code explainQueuePaths} is not null, adds the
   * resolved values of those queues to the result.
   *
   * @param proposed proposed configuration
   * @param facts live cluster facts
   * @param explainQueuePaths full queue paths to explain, or null for none
   * @return the issues found and the requested explain entries
   */
  public ValidationResult validate(ConfigSnapshot proposed,
      ClusterFacts facts, Collection<String> explainQueuePaths) {
    ValidationContext context = new ValidationContext(proposed, facts);
    List<ValidationIssue> found = new ArrayList<>();
    for (ValidationRule rule : RULES) {
      try {
        rule.check(context, found);
      } catch (RuntimeException e) {
        LOG.warn("Validation rule {} failed", rule.getId(), e);
        throw e;
      }
    }
    // A failure some values share, for example a maximum lifetime the
    // default lifetime falls back to, is reported once
    List<ValidationIssue> issues =
        new ArrayList<>(new LinkedHashSet<>(found));
    // Stable, so issues of one check keep the order the rule found them in
    Collections.sort(issues, new IssueOrder(context));
    ValidationResult result = new ValidationResult(issues);
    if (explainQueuePaths == null) {
      return result;
    }
    return result.withExplain(explain(context.getTree(), explainQueuePaths));
  }

  private static Map<String, List<ValidationResult.ExplainEntry>> explain(
      ResolvedQueueTree tree, Collection<String> queuePaths) {
    Map<String, List<ValidationResult.ExplainEntry>> explain =
        new LinkedHashMap<>();
    for (String path : queuePaths) {
      ResolvedQueueConfig queue = tree.get(new QueuePath(path));
      if (queue == null || explain.containsKey(path)) {
        continue;
      }
      List<ValidationResult.ExplainEntry> entries = new ArrayList<>();
      for (Map.Entry<String, Resolved<?>> entry
          : queue.explain().entrySet()) {
        Resolved<?> value = entry.getValue();
        entries.add(new ValidationResult.ExplainEntry(entry.getKey(),
            value.isFailed() ? null : format(value.getValue()),
            value.getSource() == null ? null : value.getSource().name(),
            value.getSourceDetail()));
      }
      explain.put(path, entries);
    }
    return explain;
  }

  /** Formats a resolved value; collections are sorted for stable output. */
  private static String format(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Collection
        && !(value instanceof List)) {
      List<String> items = new ArrayList<>();
      for (Object item : (Collection<?>) value) {
        items.add(String.valueOf(item));
      }
      return String.join(",", new TreeSet<>(items));
    }
    if (value instanceof Map) {
      return String.valueOf(new TreeMap<Object, Object>((Map<?, ?>) value));
    }
    return String.valueOf(value);
  }
}
