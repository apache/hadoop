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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.FSPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementFactory;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks.QueueRef;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.WorkflowPriorityMappingsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.AUTO_QUEUE_CREATION_V2_ENABLED;

/**
 * Placement rule checks ({@link PlacementRuleChecks}), as the scheduler sets
 * up its placement rules after a refresh: the rule list has no duplicates,
 * the rule classes load, and the mapping rules parse and target queues that
 * exist or can be created, and the workflow priority mappings, which the
 * scheduler parses after its placement rules, are well formed. Targets are
 * validated against a queue index of the proposed hierarchy and the live
 * dynamic queues.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class PlacementRulesRule implements ValidationRule {
  /** P01. */
  public static final String DUPLICATE_PLACEMENT_RULES =
      "duplicate-placement-rules";
  /** P02 and P03. */
  public static final String INVALID_PLACEMENT_RULE = "invalid-placement-rule";
  /** P04 to P06. */
  public static final String INVALID_MAPPING_RULES = "invalid-mapping-rules";
  /** P07, warning. */
  public static final String MAPPING_RULE_JSON_MISSING =
      "mapping-rule-json-missing";
  /** P08 to P13. */
  public static final String INVALID_MAPPING_RULE_TARGET =
      "invalid-mapping-rule-target";
  /** P14. */
  public static final String INVALID_WORKFLOW_PRIORITY_MAPPING =
      "invalid-workflow-priority-mapping";

  @Override
  public String getId() {
    return "placement";
  }

  @Override
  public void check(ValidationContext context, List<ValidationIssue> issues) {
    checkPlacementRules(context, issues);
    try {
      WorkflowPriorityMappingsManager.parseWorkflowPriorityMappings(
          context.getConfiguration().getWorkflowPriorityMappings());
    } catch (IllegalArgumentException e) {
      issues.add(ValidationIssue.error(null,
          CapacitySchedulerConfiguration.WORKFLOW_PRIORITY_MAPPINGS,
          INVALID_WORKFLOW_PRIORITY_MAPPING, e));
    }
  }

  /** P01 to P13. */
  private static void checkPlacementRules(ValidationContext context,
      List<ValidationIssue> issues) {
    CapacitySchedulerConfiguration conf = context.getConfiguration();
    Collection<String> names =
        conf.getStringCollection(YarnConfiguration.QUEUE_PLACEMENT_RULES);
    Set<String> distinct;
    try {
      distinct = CapacitySchedulerConfigValidator.validatePlacementRules(names);
    } catch (IOException e) {
      issues.add(ValidationIssue.error(null,
          YarnConfiguration.QUEUE_PLACEMENT_RULES, DUPLICATE_PLACEMENT_RULES,
          RuleSupport.message(e)));
      return;
    }
    if (distinct.isEmpty()) {
      distinct.add(YarnConfiguration.USER_GROUP_PLACEMENT_RULE);
    }

    boolean mappingRulesChecked = false;
    for (String name : distinct) {
      if (name.equals(YarnConfiguration.USER_GROUP_PLACEMENT_RULE)
          || name.equals(YarnConfiguration.APP_NAME_PLACEMENT_RULE)) {
        if (!mappingRulesChecked) {
          mappingRulesChecked = true;
          if (!checkMappingRules(context, issues)) {
            return;
          }
        }
      } else if (!checkRuleClass(conf, name, issues)) {
        return;
      }
    }
  }

  /**
   * P02 and P03: the class loads as a placement rule, and a Fair Scheduler
   * rule, which only initializes for the Fair Scheduler, is rejected.
   */
  private static boolean checkRuleClass(CapacitySchedulerConfiguration conf,
      String name, List<ValidationIssue> issues) {
    String message = null;
    try {
      Class<? extends PlacementRule> ruleClass =
          Class.forName(name).asSubclass(PlacementRule.class);
      if (FSPlacementRule.class.isAssignableFrom(ruleClass)) {
        try {
          PlacementFactory.getPlacementRule(name, conf).initialize(null);
        } catch (IOException e) {
          message = e.toString();
        }
      }
    } catch (ClassNotFoundException e) {
      message = e.toString();
    } catch (ClassCastException e) {
      message = RuleSupport.message(e);
    }
    if (message != null) {
      issues.add(ValidationIssue.error(null,
          YarnConfiguration.QUEUE_PLACEMENT_RULES, INVALID_PLACEMENT_RULE,
          message));
      return false;
    }
    return true;
  }

  /** P04 to P13, as CSMappingPlacementRule initializes. */
  private static boolean checkMappingRules(ValidationContext context,
      List<ValidationIssue> issues) {
    CapacitySchedulerConfiguration conf = context.getConfiguration();
    if (CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON.equals(
        conf.get(CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT))
        && conf.get(CapacitySchedulerConfiguration.MAPPING_RULE_JSON, "")
            .isEmpty()
        && conf.get(CapacitySchedulerConfiguration.MAPPING_RULE_JSON_FILE, "")
            .isEmpty()) {
      issues.add(ValidationIssue.warning(null,
          CapacitySchedulerConfiguration.MAPPING_RULE_JSON,
          MAPPING_RULE_JSON_MISSING, "Mapping rule is set to JSON, but no"
              + " inline JSON nor a JSON file was provided!"));
    }

    List<MappingRule> rules;
    try {
      rules = conf.getMappingRules();
    } catch (IOException | RuntimeException e) {
      issues.add(ValidationIssue.error(null,
          CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
          INVALID_MAPPING_RULES, RuleSupport.message(e)));
      return false;
    }
    if (rules.isEmpty()) {
      return true;
    }

    MappingRuleValidationContext validationContext;
    try {
      validationContext = PlacementRuleChecks.newMappingRuleValidationContext(
          PlacementRuleChecks.queueIndexOf(queueRefs(context)));
    } catch (IOException e) {
      issues.add(ValidationIssue.error(null, null,
          INVALID_MAPPING_RULE_TARGET, RuleSupport.message(e)));
      return false;
    }
    boolean valid = true;
    for (MappingRule rule : rules) {
      try {
        rule.validate(validationContext);
      } catch (YarnException e) {
        issues.add(ValidationIssue.error(null, null,
            INVALID_MAPPING_RULE_TARGET, e.getMessage()));
        valid = false;
      }
    }
    return valid;
  }

  /**
   * The queues the scheduler holds after the refresh: the proposed
   * hierarchy, which includes the live dynamic queues under parents that
   * remain, and the live reservation queues of remaining plans.
   */
  private static List<QueueRef> queueRefs(ValidationContext context) {
    Map<String, QueueRef> refs = new LinkedHashMap<>();
    for (ResolvedQueueConfig queue : context.getTree().getQueues()) {
      QueueKind kind = context.getQueueKind(queue);
      boolean eligible = false;
      if (kind.isParent()) {
        Resolved<Boolean> v2 = queue.get(AUTO_QUEUE_CREATION_V2_ENABLED);
        eligible = queue.isDynamic()
            || (RuleSupport.ok(v2) && v2.getValue());
      }
      refs.put(queue.getQueuePath().getFullPath(),
          new QueueRef(queue.getQueuePath().getFullPath(), kind, eligible));
    }
    for (ClusterFacts.QueueFacts live
        : context.getFacts().getQueues().values()) {
      QueuePath path = new QueuePath(live.getQueuePath());
      if (!path.isRoot() && !refs.containsKey(live.getQueuePath())
          && live.isAutoCreatedLeaf()
          && refs.containsKey(path.getParentObject().getFullPath())) {
        refs.put(live.getQueuePath(),
            new QueueRef(live.getQueuePath(), live.getKind(), false));
      }
    }
    return new ArrayList<>(refs.values());
  }
}
