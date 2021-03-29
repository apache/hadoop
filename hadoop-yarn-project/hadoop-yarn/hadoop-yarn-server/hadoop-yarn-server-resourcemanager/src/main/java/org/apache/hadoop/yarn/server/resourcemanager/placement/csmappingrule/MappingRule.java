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

package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;

/**
 * Mapping rule represents a single mapping setting defined by the user. All
 * rules have matchers and actions. Matcher determine if a mapping rule applies
 * to a given applicationSubmission, while action represent the course of action
 * we need to take when a rule applies.
 *
 * MappingRules also support fallback actions, which will be evaluated when the
 * main action fails due to any reason (Eg. trying to place to a queue which
 * does not exist)
 */
public class MappingRule {
  public static final String USER_MAPPING = "u";
  public static final String GROUP_MAPPING = "g";
  public static final String APPLICATION_MAPPING = "a";
  private final MappingRuleMatcher matcher;
  private final MappingRuleAction action;

  public MappingRule(MappingRuleMatcher matcher, MappingRuleAction action) {
    this.matcher = matcher;
    this.action = action;
  }

  /**
   * This method evaluates the rule, and returns the MappingRuleResult, if
   * the rule matches, skip action otherwise.
   * @param variables The variable context, which contains all the variables
   * @return The rule's result or skip action if the rule doesn't apply
   */
  public MappingRuleResult evaluate(VariableContext variables) {
    if (matcher.match(variables)) {
      return action.execute(variables);
    }

    return MappingRuleResult.createSkipResult();
  }

  /**
   * Returns the associated action's fallback.
   * @return The fallback of the action
   */
  public MappingRuleResult getFallback() {
    return action.getFallback();
  }

  /**
   * Creates a MappingRule object from the legacy style configuration. The
   * configuration is a [TYPE]:SOURCE:PATH (eg. u:bob:root.users.%user).
   * Using the source and path parts of the legacy rule, this method will
   * create an application MappingRule which behaves as the legacy rule defined.
   * This version of the method does not require type, since legacy application
   * mappings omitted the 'a', so this method is to be used for those rules,
   * which in all case are application mappings.
   * @param source This part of the rule determines which applications the rule
   *               will be applied
   * @param path The path where the application is to be placed
   * @return MappingRule based on the provided settings
   */
  public static MappingRule createLegacyRule(String source, String path) {
    return createLegacyRule(APPLICATION_MAPPING, source, path);
  }

  /**
   * Creates a MappingRule object from the legacy style configuration. The
   * configuration is a [TYPE]:SOURCE:PATH (eg. u:bob:root.users.%user).
   * Using the type, source and path parts of the legacy rule, this method will
   * create a MappingRule which behaves as the legacy rule defined.
   * @param type The type of the rule, can be
   *             'u' for user mapping, 'g' for group mapping or
   *             'a' for application mapping
   * @param source This part of the rule determines which submissions this rule
   *               should apply to (eg. if type is 'u', source will match
   *               against the user name)
   * @param path The path where the application is to be placed
   * @return MappingRule based on the provided settings
   */
  public static MappingRule createLegacyRule(
      String type, String source, String path) {
    MappingRuleMatcher matcher;
    MappingRuleAction action = MappingRuleActions.createPlaceToQueueAction(
        path, true);
    //While legacy rule fallback handling is a bit inconsistent, the most cases
    //it fall back to default queue placement, so this is the best approximation
    action.setFallbackDefaultPlacement();

    switch (type) {
    case USER_MAPPING:
      if (source.equals("%user")) {
        matcher = MappingRuleMatchers.createAllMatcher();
      } else {
        matcher = MappingRuleMatchers.createUserMatcher(source);
      }
      break;
    case GROUP_MAPPING:
      matcher = MappingRuleMatchers.createUserGroupMatcher(source);
      break;
    case APPLICATION_MAPPING:
      matcher = MappingRuleMatchers.createApplicationNameMatcher(source);
      break;
    default:
      throw new IllegalArgumentException("Invalid mapping rule type '" +
            type + "'");
    }

    return new MappingRule(matcher, action);
  }

  public void validate(MappingRuleValidationContext ctx)
      throws YarnException {
    this.action.validate(ctx);
  }

  @Override
  public String toString() {
    return "MappingRule{" +
      "matcher=" + matcher +
      ", action=" + action +
      '}';
  }
}
