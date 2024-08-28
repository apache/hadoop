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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import static org.apache.hadoop.util.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleAction;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleActions;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleMatcher;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleMatchers;

// These are generated classes - use GeneratePojos class to create them
// if they are missing
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.VisibleForTesting;

public class MappingRuleCreator {
  private static final String ALL_USER = "*";
  private static Logger LOG = LoggerFactory.getLogger(MappingRuleCreator.class);

  public MappingRulesDescription getMappingRulesFromJsonFile(String filePath)
      throws IOException {
    byte[] fileContents = Files.readAllBytes(Paths.get(filePath));
    return getMappingRulesFromJson(fileContents);
  }

  MappingRulesDescription getMappingRulesFromJson(byte[] contents)
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(contents, MappingRulesDescription.class);
  }

  MappingRulesDescription getMappingRulesFromJson(String contents)
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(contents, MappingRulesDescription.class);
  }

  public List<MappingRule> getMappingRulesFromFile(String jsonPath)
      throws IOException {
    MappingRulesDescription desc = getMappingRulesFromJsonFile(jsonPath);
    return getMappingRules(desc);
  }

  public List<MappingRule> getMappingRulesFromString(String json)
      throws IOException {
    MappingRulesDescription desc = getMappingRulesFromJson(json);
    return getMappingRules(desc);
  }

  @VisibleForTesting
  List<MappingRule> getMappingRules(MappingRulesDescription rules) {
    List<MappingRule> mappingRules = new ArrayList<>();

    for (Rule rule : rules.getRules()) {
      checkMandatoryParameters(rule);

      MappingRuleMatcher matcher = createMatcher(rule);
      MappingRuleAction action = createAction(rule);
      setFallbackToAction(rule, action);

      MappingRule mappingRule = new MappingRule(matcher, action);
      mappingRules.add(mappingRule);
    }

    return mappingRules;
  }

  private MappingRuleMatcher createMatcher(Rule rule) {
    String matches = rule.getMatches();
    Type type = rule.getType();

    MappingRuleMatcher matcher = null;
    switch (type) {
    case USER:
      if (ALL_USER.equals(matches)) {
        matcher = MappingRuleMatchers.createAllMatcher();
      } else {
        matcher = MappingRuleMatchers.createUserMatcher(matches);
      }
      break;
    case GROUP:
      checkArgument(!ALL_USER.equals(matches), "Cannot match '*' for groups");
      matcher = MappingRuleMatchers.createUserGroupMatcher(matches);
      break;
    case APPLICATION:
      matcher = MappingRuleMatchers.createApplicationNameMatcher(matches);
      break;
    default:
      throw new IllegalArgumentException("Unknown type: " + type);
    }

    return matcher;
  }

  private MappingRuleAction createAction(Rule rule) {
    Policy policy = rule.getPolicy();
    String queue = rule.getParentQueue();

    boolean create;
    if (rule.getCreate() == null) {
      LOG.debug("Create flag is not set for rule {},"
          + "using \"true\" as default", rule);
      create = true;
    } else {
      create = rule.getCreate();
    }

    MappingRuleAction action = null;
    switch (policy) {
    case DEFAULT_QUEUE:
      action = MappingRuleActions.createPlaceToDefaultAction();
      break;
    case REJECT:
      action = MappingRuleActions.createRejectAction();
      break;
    case PRIMARY_GROUP:
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(queue, "%primary_group"), create);
      break;
    case SECONDARY_GROUP:
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(queue, "%secondary_group"), create);
      break;
    case PRIMARY_GROUP_USER:
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(rule.getParentQueue(),
              "%primary_group.%user"), create);
      break;
    case SECONDARY_GROUP_USER:
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(rule.getParentQueue(),
              "%secondary_group.%user"), create);
      break;
    case SPECIFIED:
      action = MappingRuleActions.createPlaceToQueueAction("%specified",
          create);
      break;
    case CUSTOM:
      String customTarget = rule.getCustomPlacement();
      checkArgument(customTarget != null, "custom queue is undefined");
      action = MappingRuleActions.createPlaceToQueueAction(customTarget,
          create);
      break;
    case USER:
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(rule.getParentQueue(),
              "%user"), create);
      break;
    case APPLICATION_NAME:
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(rule.getParentQueue(),
              "%application"), create);
      break;
    case SET_DEFAULT_QUEUE:
      String defaultQueue = rule.getValue();
      checkArgument(defaultQueue != null, "default queue is undefined");
      action = MappingRuleActions.createUpdateDefaultAction(defaultQueue);
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported policy: " + policy);
    }

    return action;
  }

  private void setFallbackToAction(Rule rule, MappingRuleAction action) {
    FallbackResult fallbackResult = rule.getFallbackResult();

    if (fallbackResult == null) {
      action.setFallbackSkip();
      LOG.debug("Fallback is not defined for rule {}, using SKIP as default", rule);
      return;
    }

    switch (fallbackResult) {
    case PLACE_DEFAULT:
      action.setFallbackDefaultPlacement();
      break;
    case REJECT:
      action.setFallbackReject();
      break;
    case SKIP:
      action.setFallbackSkip();
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported fallback rule " + fallbackResult);
    }
  }

  private String getTargetQueue(String parent, String placeholder) {
    return (parent == null) ? placeholder : parent + "." + placeholder;
  }

  private void checkMandatoryParameters(Rule rule) {
    checkArgument(rule.getPolicy() != null, "Rule policy is undefined");
    checkArgument(rule.getType() != null, "Rule type is undefined");
    checkArgument(rule.getMatches() != null, "Match string is undefined");
    checkArgument(!StringUtils.isEmpty(rule.getMatches()),
        "Match string is empty");
  }
}