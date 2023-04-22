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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This class is responsible for making application submissions to queue
 * assignments, based on the configured ruleset. This class supports all
 * features supported by UserGroupMappingPlacementRule and
 * AppNameMappingPlacementRule classes, also adding some features which are
 * present in fair scheduler queue placement. This helps to reduce the gap
 * between the two schedulers.
 */
public class CSMappingPlacementRule extends PlacementRule {
  private static final Logger LOG = LoggerFactory
      .getLogger(CSMappingPlacementRule.class);
  private static final String DOT = ".";
  private static final String DOT_REPLACEMENT = "_dot_";

  private CapacitySchedulerQueueManager queueManager;
  private List<MappingRule> mappingRules;

  /**
   * These are the variables we associate a special meaning, these should be
   * immutable for each variable context.
   */
  private ImmutableSet<String> immutableVariables = ImmutableSet.of(
      "%user",
      "%primary_group",
      "%secondary_group",
      "%application",
      "%specified"
      );

  private Groups groups;
  private boolean overrideWithQueueMappings;
  private boolean failOnConfigError = true;

  @VisibleForTesting
  public void setGroups(Groups groups) {
    this.groups = groups;
  }

  @VisibleForTesting
  public void setFailOnConfigError(boolean failOnConfigError) {
    this.failOnConfigError = failOnConfigError;
  }

  private MappingRuleValidationContext buildValidationContext()
      throws IOException {
    Preconditions.checkNotNull(queueManager, "Queue manager must be " +
        "initialized before building validation a context!");

    MappingRuleValidationContext validationContext =
        new MappingRuleValidationContextImpl(queueManager);

    //Adding all immutable variables to the known variable list
    for (String var : immutableVariables) {
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

  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    if (!(scheduler instanceof CapacityScheduler)) {
      throw new IOException(
        "CSMappingPlacementRule can be only used with CapacityScheduler");
    }
    LOG.info("Initializing {} queue mapping manager.",
        getClass().getSimpleName());

    CapacitySchedulerContext csContext = (CapacitySchedulerContext) scheduler;
    queueManager = csContext.getCapacitySchedulerQueueManager();

    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    overrideWithQueueMappings = conf.getOverrideWithQueueMappings();

    if (groups == null) {
      groups = Groups.getUserToGroupsMappingService(csContext.getConf());
    }

    MappingRuleValidationContext validationContext = buildValidationContext();

    //Getting and validating mapping rules
    mappingRules = conf.getMappingRules();
    for (MappingRule rule : mappingRules) {
      try {
        rule.validate(validationContext);
      } catch (YarnException e) {
        LOG.error("Error initializing queue mappings, rule '{}' " +
            "has encountered a validation error: {}", rule, e.getMessage());
        if (failOnConfigError) {
          throw new IOException(e);
        }
      }
    }

    LOG.info("Initialized queue mappings, can override user specified " +
        "queues: {}  number of rules: {} mapping rules: {}",
        overrideWithQueueMappings, mappingRules.size(), mappingRules);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Initialized with the following mapping rules:");
      mappingRules.forEach(rule -> LOG.debug(rule.toString()));
    }

    return mappingRules.size() > 0;
  }

  /**
   * Sets group related data for the provided variable context.
   * Primary group is the first group returned by getGroups.
   * To determine secondary group we traverse all groups
   * (as there could be more than one and position is not guaranteed) and
   * ensure there is queue with the same name.
   * This method also sets the groups set for the variable context for group
   * matching.
   * @param vctx Variable context to be updated
   * @param user Name of the user
   * @throws IOException
   */
  private void setupGroupsForVariableContext(VariableContext vctx, String user)
      throws IOException {
    if (groups == null) {
      LOG.warn(
          "Group provider hasn't been set, cannot query groups for user {}",
          user);
      //enforcing empty primary group instead of null, which would be considered
      //as unknown variable and would evaluate to '%primary_group'
      vctx.put("%primary_group", "");
      vctx.put("%secondary_group", "");
      return;
    }
    Set<String> groupsSet = groups.getGroupsSet(user);
    if (groupsSet.isEmpty()) {
      LOG.warn("There are no groups for user {}", user);
      vctx.putExtraDataset("groups", groupsSet);
      return;
    }
    Iterator<String> it = groupsSet.iterator();
    String primaryGroup = cleanName(it.next());

    ArrayList<String> secondaryGroupList = new ArrayList<>();

    while (it.hasNext()) {
      String groupName = cleanName(it.next());
      secondaryGroupList.add(groupName);
    }

    if (secondaryGroupList.size() == 0) {
      //if we have no chance to have a secondary group to speed up evaluation
      //we simply register it as a regular variable with "" as a value
      vctx.put("%secondary_group", "");
      if (LOG.isDebugEnabled()) {
        LOG.debug("User {} does not have any potential Secondary group", user);
      }
    } else {
      vctx.putConditional(
          MappingRuleConditionalVariables.SecondaryGroupVariable.VARIABLE_NAME,
          new MappingRuleConditionalVariables.SecondaryGroupVariable(
              this.queueManager,
              secondaryGroupList
              ));
    }

    vctx.put("%primary_group", primaryGroup);
    vctx.putExtraDataset("groups", groupsSet);
  }

  private VariableContext createVariableContext(
      ApplicationSubmissionContext asc, String user) {
    VariableContext vctx = new VariableContext();

    String cleanedName = cleanName(user);
    if (!user.equals(cleanedName)) {
      vctx.putOriginal("%user", user);
    }
    vctx.put("%user", cleanedName);
    //If the specified matches the default it means NO queue have been specified
    //as per ClientRMService#submitApplication which sets the queue to default
    //when no queue is provided.
    //To place queues specifically to default, users must use root.default
    if (!asc.getQueue().equals(YarnConfiguration.DEFAULT_QUEUE_NAME)) {
      vctx.put("%specified", asc.getQueue());
    } else {
      //Adding specified as empty will prevent it to be undefined and it won't
      //try to place the application to a queue named '%specified', queue path
      //validation will reject the empty path or the path with empty parts,
      //so we still hit the fallback action of this rule if no queue
      //is specified
      vctx.put("%specified", "");
    }

    vctx.put("%application", asc.getApplicationName());
    vctx.put("%default", "root.default");
    try {
      setupGroupsForVariableContext(vctx, user);
    } catch (IOException e) {
      LOG.warn("Unable to setup groups: {}", e.getMessage());
    }

    vctx.setImmutables(immutableVariables);
    return vctx;
  }

  private String validateAndNormalizeQueue(
      String queueName, boolean allowCreate) throws YarnException {
    QueuePath path = new QueuePath(queueName);

    if (path.hasEmptyPart()) {
      throw new YarnException("Invalid path returned by rule: '" +
          queueName + "'");
    }

    String leaf = path.getLeafName();
    String parent = path.getParent();

    String normalizedName;
    if (parent != null) {
      normalizedName = validateAndNormalizeQueueWithParent(
          parent, leaf, allowCreate);
    } else {
      normalizedName = validateAndNormalizeQueueWithNoParent(leaf);
    }

    CSQueue queue = queueManager.getQueueByFullName(normalizedName);
    if (queue != null && !(queue instanceof AbstractLeafQueue)) {
      throw new YarnException("Mapping rule returned a non-leaf queue '" +
          normalizedName + "', cannot place application in it.");
    }

    return normalizedName;
  }

  private String validateAndNormalizeQueueWithParent(
      String parent, String leaf, boolean allowCreate) throws YarnException {
    String normalizedPath =
        MappingRuleValidationHelper.normalizeQueuePathRoot(
            queueManager, parent + DOT + leaf);
    MappingRuleValidationHelper.ValidationResult validity =
        MappingRuleValidationHelper.validateQueuePathAutoCreation(
            queueManager, normalizedPath);

    switch (validity) {
    case AMBIGUOUS_PARENT:
      throw new YarnException("Mapping rule specified a parent queue '" +
          parent + "', but it is ambiguous.");
    case AMBIGUOUS_QUEUE:
      throw new YarnException("Mapping rule specified a target queue '" +
          normalizedPath + "', but it is ambiguous.");
    case EMPTY_PATH:
      throw new YarnException("Mapping rule did not specify a target queue.");
    case NO_PARENT_PROVIDED:
      throw new YarnException("Mapping rule did not specify an existing queue" +
          " nor a dynamic parent queue.");
    case NO_DYNAMIC_PARENT:
      throw new YarnException("Mapping rule specified a parent queue '" +
          parent + "', but it is not a dynamic parent queue, " +
          "and no queue exists with name '" + leaf + "' under it.");
    case QUEUE_EXISTS:
      break;
    case CREATABLE:
      if (!allowCreate) {
        throw new YarnException("Mapping rule doesn't allow auto-creation of " +
            "the queue '" + normalizedPath + "'.");
      }
      break;
    default:
      //Probably the QueueCreationValidation have
      //new items, which are not handled here
      throw new YarnException("Unknown queue path validation result. '" +
          validity + "'.");
    }

    //at this point we either have a dynamic parent or the queue actually
    //exists, returning it
    return normalizedPath;
  }

  private String validateAndNormalizeQueueWithNoParent(String leaf)
      throws YarnException {
    //in this case we don't have a parent specified so we expect the queue to
    //exist, otherwise the mapping will not be valid for this case
    CSQueue queue = queueManager.getQueue(leaf);
    if (queue == null) {
      if (queueManager.isAmbiguous(leaf)) {
        throw new YarnException("Queue '" + leaf + "' specified in mapping" +
            " rule is ambiguous");
      } else {
        throw new YarnException("Queue '" + leaf + "' specified in mapping" +
            " rule does not exist.");
      }
    }

    //normalizing queue path
    return queue.getQueuePath();
  }

  /**
   * Evaluates the mapping rule using the provided variable context. For
   * placement results we check if the placement is valid, and in case of
   * invalid placements we use the rule's fallback settings to get the result.
   * @param rule The mapping rule to be evaluated
   * @param variables The variables and their respective values
   * @return Evaluation result
   */
  private MappingRuleResult evaluateRule(
      MappingRule rule, VariableContext variables) {
    MappingRuleResult result = rule.evaluate(variables);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Evaluated rule '{}' with result: '{}'", rule, result);
    }

    if (result.getResult() == MappingRuleResultType.PLACE) {
      try {
        result.updateNormalizedQueue(validateAndNormalizeQueue(
            result.getQueue(), result.isCreateAllowed()));
      } catch (Exception e) {
        result = rule.getFallback();
        LOG.info("Cannot place to queue '{}' returned by mapping rule. " +
            "Reason: '{}' Fallback operation: '{}'",
            result.getQueue(), e.getMessage(), result);
      }
    }

    return result;
  }

  private ApplicationPlacementContext createPlacementContext(String queueName) {
    int parentQueueNameEndIndex = queueName.lastIndexOf(DOT);
    if (parentQueueNameEndIndex > -1) {
      String parent = queueName.substring(0, parentQueueNameEndIndex).trim();
      String leaf = queueName.substring(parentQueueNameEndIndex + 1).trim();
      return new ApplicationPlacementContext(leaf, parent);
    }

    //this statement is here only for future proofing and consistency.
    //Currently there is no valid queue name which does not have a parent
    //and valid for app placement. Since we normalize all paths, the only queue
    //which can have no parent at this point is 'root', which is neither a
    //leaf queue nor a managerParent queue. But it might become one, and
    //it's better to leave the code consistent.
    return new ApplicationPlacementContext(queueName);
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    return getPlacementForApp(asc, user, false);
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user, boolean recovery)
        throws YarnException {
    //We only use the mapping rules if overrideWithQueueMappings enabled
    //or the application is submitted to the default queue, which effectively
    //means the application doesn't have any specific queue.
    String appQueue = asc.getQueue();
    LOG.debug("Looking placement for app '{}' originally submitted to queue " +
        "'{}', with override enabled '{}'",
        asc.getApplicationName(), appQueue, overrideWithQueueMappings);
    if (appQueue != null &&
        !appQueue.equals(YarnConfiguration.DEFAULT_QUEUE_NAME) &&
        !overrideWithQueueMappings &&
        !recovery) {
      LOG.info("Have no jurisdiction over application submission '{}', " +
          "moving to next PlacementRule engine", asc.getApplicationName());
      return null;
    }

    VariableContext variables;
    variables = createVariableContext(asc, user);

    ApplicationPlacementContext ret = null;
    for (MappingRule rule : mappingRules) {
      MappingRuleResult result = evaluateRule(rule, variables);
      switch (result.getResult()) {
      case PLACE_TO_DEFAULT:
        ret = placeToDefault(asc, variables, rule);
        break;
      case PLACE:
        ret = placeToQueue(asc, rule, result);
        break;
      case REJECT:
        LOG.info("Rejecting application '{}', reason: Mapping rule '{}' " +
            " fallback action is set to REJECT.",
            asc.getApplicationName(), rule);
        //We intentionally omit the details, we don't want any server side
        //config information to leak to the client side
        throw new YarnException("Application submission have been rejected by" +
            " a mapping rule. Please see the logs for details");
      case SKIP:
      //SKIP means skip to the next rule, which is the default behaviour of
      //the for loop, so we don't need to take any extra actions
        break;
      default:
        LOG.error("Invalid result '{}'", result);
      }

      //If we already have a return value, we can return it!
      if (ret != null) {
        break;
      }
    }

    if (ret == null) {
      //If no rule was applied we return null, to let the engine move onto the
      //next placementRule class
      LOG.info("No matching rule found for application '{}', moving to next " +
          "PlacementRule engine", asc.getApplicationName());
    }

    if (recovery) {
      //we need this part for backwards compatibility with recovery
      //the legacy code checked if the placement matches the queue of the
      //application to be recovered, and if it did, it created an
      //ApplicationPlacementContext.
      //However at a later point this is going to be changed, there are two
      //major issues with this approach:
      //  1) The recovery only uses LEAF queue names, which must be updated
      //  2) The ORIGINAL queue which the application was submitted is NOT
      //     stored this might result in different placement evaluation since
      //     now we can have rules which give different result based on what
      //     the user submitted.
      if (ret == null || !ret.getQueue().equals(asc.getQueue())) {
        return null;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Placement final result '{}' for application '{}'",
          (ret == null ? "null" : ret.getFullQueuePath()),
          asc.getApplicationId());
    }

    return ret;
  }

  private ApplicationPlacementContext placeToQueue(
      ApplicationSubmissionContext asc,
      MappingRule rule,
      MappingRuleResult result) {
    LOG.debug("Application '{}' have been placed to queue '{}' by " +
        "rule {}", asc.getApplicationName(), result.getNormalizedQueue(), rule);
    //evaluateRule will only return a PLACE rule, if it is verified
    //and normalized, so it is safe here to simply create the placement
    //context
    return createPlacementContext(result.getNormalizedQueue());
  }

  private ApplicationPlacementContext placeToDefault(
      ApplicationSubmissionContext asc,
      VariableContext variables,
      MappingRule rule) throws YarnException {
    try {
      String queueName = validateAndNormalizeQueue(
          variables.replacePathVariables("%default"), false);
      LOG.debug("Application '{}' have been placed to queue '{}' by " +
          "the fallback option of rule {}",
          asc.getApplicationName(), queueName, rule);
      return createPlacementContext(queueName);
    } catch (YarnException e) {
      LOG.error("Rejecting application due to a failed fallback" +
          " action '{}'" + ", reason: {}", asc.getApplicationName(),
          e);
      //We intentionally omit the details, we don't want any server side
      //config information to leak to the client side
      throw new YarnException("Application submission have been rejected by a" +
          " mapping rule. Please see the logs for details");
    }
  }

  private String cleanName(String name) {
    if (name.contains(DOT)) {
      String converted = name.replaceAll("\\.", DOT_REPLACEMENT);
      LOG.warn("Name {} is converted to {} when it is used as a queue name.",
          name, converted);
      return converted;
    } else {
      return name;
    }
  }

  @VisibleForTesting
  public Groups getGroups() {
    return groups;
  }
}
