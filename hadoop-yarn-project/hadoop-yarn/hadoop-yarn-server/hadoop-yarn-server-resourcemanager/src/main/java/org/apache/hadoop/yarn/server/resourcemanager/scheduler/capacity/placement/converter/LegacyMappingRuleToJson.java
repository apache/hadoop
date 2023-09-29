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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;

import java.util.ArrayList;
import java.util.Collection;

public class LegacyMappingRuleToJson {
  //Legacy rule parse helper constants
  public static final String RULE_PART_DELIMITER = ":";
  public static final String PREFIX_USER_MAPPING = "u";
  public static final String PREFIX_GROUP_MAPPING = "g";

  //Legacy rule matcher variables
  public static final String MATCHER_APPLICATION = "%application";
  public static final String MATCHER_USER = "%user";

  //Legacy rule mapping variables, which can be used in target queues
  public static final String MAPPING_PRIMARY_GROUP = "%primary_group";
  public static final String MAPPING_SECONDARY_GROUP = "%secondary_group";
  public static final String MAPPING_USER = MATCHER_USER;

  //JSON Format match all token (actually only used for users)
  public static final String JSON_MATCH_ALL = "*";

  //Frequently used JSON node names for rule definitions
  public static final String JSON_NODE_POLICY = "policy";
  public static final String JSON_NODE_PARENT_QUEUE = "parentQueue";
  public static final String JSON_NODE_CUSTOM_PLACEMENT = "customPlacement";
  public static final String JSON_NODE_MATCHES = "matches";

  /**
   * Our internal object mapper, used to create JSON nodes.
   */
  private ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Collection to store the legacy group mapping rule strings.
   */
  private Collection<String> userGroupMappingRules = new ArrayList<>();
  /**
   * Collection to store the legacy application name mapping rule strings.
   */
  private Collection<String> applicationNameMappingRules = new ArrayList<>();

  /**
   * This setter method is used to set the raw string format of the legacy
   * user group mapping rules. This method expect a string formatted just like
   * in the configuration file of the Capacity Scheduler.
   * eg. u:bob:root.groups.%primary_group,u:%user:root.default
   *
   * @param rules The string containing ALL the UserGroup mapping rules in
   *              legacy format
   * @return This object for daisy chain support
   */
  public LegacyMappingRuleToJson setUserGroupMappingRules(String rules) {
    setUserGroupMappingRules(StringUtils.getTrimmedStringCollection(rules));
    return this;
  }

  /**
   * This setter method is used to set the the user group mapping rules as a
   * string collection, where each entry is one rule.
   *
   * @param rules One rule per entry
   * @return This object for daisy chain support
   */
  public LegacyMappingRuleToJson setUserGroupMappingRules(
      Collection<String> rules) {
    if (rules != null) {
      userGroupMappingRules = rules;
    } else {
      userGroupMappingRules = new ArrayList<>();
    }
    return this;
  }

  /**
   * This setter method is used to set the raw string format of the legacy
   * application name mapping rules. This method expect a string formatted
   * just like in the configuration file of the Capacity Scheduler.
   * eg. mapreduce:root.apps.%application,%application:root.default
   *
   * @param rules The string containing ALL the application name mapping rules
   *              in legacy format
   * @return This object for daisy chain support
   */
  public LegacyMappingRuleToJson setAppNameMappingRules(String rules) {
    setAppNameMappingRules(StringUtils.getTrimmedStringCollection(rules));
    return this;
  }

  /**
   * This setter method is used to set the the application name mapping rules as
   * a string collection, where each entry is one rule.
   *
   * @param rules One rule per entry
   * @return This object for daisy chain support
   */
  public LegacyMappingRuleToJson setAppNameMappingRules(
      Collection<String> rules) {
    if (rules != null) {
      applicationNameMappingRules = rules;
    } else {
      applicationNameMappingRules = new ArrayList<>();
    }

    return this;
  }

  /**
   * This method will do the conversion based on the already set mapping rules.
   * First the rules to be converted must be set via setAppNameMappingRules and
   * setUserGroupMappingRules methods.
   * @return JSON Format of the provided mapping rules, null if no rules are set
   */
  public String convert() {
    //creating the basic JSON config structure
    ObjectNode rootNode = objectMapper.createObjectNode();
    ArrayNode rulesNode = objectMapper.createArrayNode();
    rootNode.set("rules", rulesNode);

    //Processing and adding all the user group mapping rules
    for (String rule : userGroupMappingRules) {
      rulesNode.add(convertUserGroupMappingRule(rule));
    }

    //Processing and adding all the application name mapping rules
    for (String rule : applicationNameMappingRules) {
      rulesNode.add(convertAppNameMappingRule(rule));
    }

    //If there are no converted rules we return null
    if (rulesNode.size() == 0) {
      return null;
    }

    try {
      return objectMapper
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(rootNode);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }

    return null;
  }

  /**
   * This intermediate helper method is used to process User Group mapping rules
   * and invoke the proper mapping rule creation method.
   * @param rule The legacy format of the single rule to be converted.
   * @return The ObjectNode which can be added to the rules part of the config.
   */
  ObjectNode convertUserGroupMappingRule(String rule) {
    String[] mapping = splitRule(rule, 3);
    String ruleType = mapping[0];
    String ruleMatch = mapping[1];
    String ruleTarget = mapping[2];

    if (ruleType.equals(PREFIX_USER_MAPPING)) {
      return createUserMappingRule(ruleMatch, ruleTarget);
    }

    if (ruleType.equals(PREFIX_GROUP_MAPPING)) {
      return createGroupMappingRule(ruleMatch, ruleTarget);
    }

    throw new IllegalArgumentException(
        "User group mapping rule must start with prefix '" +
            PREFIX_USER_MAPPING + "' or '" + PREFIX_GROUP_MAPPING + "'");
  }

  /**
   * This intermediate helper method is used to process Application name mapping
   * rules and invoke the proper mapping rule creation method.
   * @param rule The legacy format of the single rule to be converted.
   * @return The ObjectNode which can be added to the rules part of the config.
   */
  ObjectNode convertAppNameMappingRule(String rule) {
    String[] mapping = splitRule(rule, 2);
    String ruleMatch = mapping[0];
    String ruleTarget = mapping[1];

    return createApplicationNameMappingRule(ruleMatch, ruleTarget);
  }
  /**
   * Helper method which splits the rules into parts, and checks if it has
   * exactly the required amount of parts, and none of them is empty!
   * @param rule The mapping rule to be split
   * @param expectedParts The number of expected parts
   * @return The split String[] of the parts
   * @throws IllegalArgumentException if the number of parts don't match or any
   *  of them is empty.
   */
  private String[] splitRule(String rule, int expectedParts) {
    //Splitting
    String[] mapping = StringUtils
        .getTrimmedStringCollection(rule, RULE_PART_DELIMITER)
        .toArray(new String[] {});

    //Checking for part count
    if (mapping.length != expectedParts) {
      throw new IllegalArgumentException("Invalid rule '" + rule +
          "' expected parts: " + expectedParts +
          " actual parts: " + mapping.length);
    }

    //Checking for empty parts
    for (int i = 0; i < mapping.length; i++) {
      if (mapping[i].length() == 0) {
        throw new IllegalArgumentException("Invalid rule '" + rule +
            "' with empty part, mapping rules must not contain empty parts!");
      }
    }

    return mapping;
  }

  /**
   * This helper method is to create a default rule node for the converter,
   * setting fields which are common in all rules.
   * @param type The type of the rule can be user/group/application
   * @return The object node with the preset fields
   */
  private ObjectNode createDefaultRuleNode(String type) {
    return objectMapper
        .createObjectNode()
        .put("type", type)
        //All legacy rule fallback to place to default
        .put("fallbackResult", "placeDefault")
        //All legacy rules allow creation
        .put("create", true);
  }

  /**
   * This method will create the JSON node for a single User Mapping Rule.
   * @param match The match part of the rule it can be either an actual user
   *              name or '%user' to match all users
   * @param target The queue to place to user into, some queue path variables
   *               are supported (%user, %primary_group, %secondary_group).
   * @return The ObjectNode which represents the rule
   */
  private ObjectNode createUserMappingRule(String match, String target) {
    ObjectNode ruleNode = createDefaultRuleNode("user");
    QueuePath targetPath = new QueuePath(target);

    //We have a special token in the JSON format to match all user, replacing
    //matcher
    if (match.equals(MATCHER_USER)) {
      match = JSON_MATCH_ALL;
    }
    ruleNode.put(JSON_NODE_MATCHES, match);

    switch (targetPath.getLeafName()) {
    case MAPPING_USER:
      ruleNode.put(JSON_NODE_POLICY, "user");
      if (targetPath.hasParent()) {
        //Parsing parent path, to be able to determine the short name of parent
        QueuePath targetParentPath =
            new QueuePath(targetPath.getParent());
        String parentShortName = targetParentPath.getLeafName();

        if (parentShortName.equals(MAPPING_PRIMARY_GROUP)) {
          //%primary_group.%user mapping
          ruleNode.put(JSON_NODE_POLICY, "primaryGroupUser");

          //Yep, this is confusing. The policy primaryGroupUser actually
          // appends the %primary_group.%user to the parent path, so we need to
          // remove it from the parent path to avoid duplication.
          targetPath = new QueuePath(targetParentPath.getParent(),
              targetPath.getLeafName());
        } else if (parentShortName.equals(MAPPING_SECONDARY_GROUP)) {
          //%secondary_group.%user mapping
          ruleNode.put(JSON_NODE_POLICY, "secondaryGroupUser");

          //Yep, this is confusing. The policy secondaryGroupUser actually
          // appends the %secondary_group.%user to the parent path, so we need
          // to remove it from the parent path to avoid duplication.
          targetPath = new QueuePath(targetParentPath.getParent(),
              targetPath.getLeafName());
        }

        //[parent].%user mapping
      }
      break;
    case MAPPING_PRIMARY_GROUP:
      //[parent].%primary_group mapping
      ruleNode.put(JSON_NODE_POLICY, "primaryGroup");
      break;
    case MAPPING_SECONDARY_GROUP:
      //[parent].%secondary_group mapping
      ruleNode.put(JSON_NODE_POLICY, "secondaryGroup");
      break;
    default:
      //static path mapping
      ruleNode.put(JSON_NODE_POLICY, "custom");
      ruleNode.put(JSON_NODE_CUSTOM_PLACEMENT, targetPath.getFullPath());
      break;
    }

    //if the target queue has a parent part, and the rule can have a parent
    //we add it to the node
    if (targetPath.hasParent()) {
      ruleNode.put(JSON_NODE_PARENT_QUEUE, targetPath.getParent());
    }

    return ruleNode;
  }

  /**
   * This method will create the JSON node for a single Group Mapping Rule.
   * @param match The name of the group to match for
   * @param target The queue to place to user into, some queue path variables
   *               are supported (%user).
   * @return The ObjectNode which represents the rule
   */
  private ObjectNode createGroupMappingRule(String match, String target) {
    ObjectNode ruleNode = createDefaultRuleNode("group");
    QueuePath targetPath = new QueuePath(target);

    //we simply used the source match part all valid legacy matchers are valid
    //matchers for the JSON format as well
    ruleNode.put(JSON_NODE_MATCHES, match);

    if (targetPath.getLeafName().matches(MATCHER_USER)) {
      //g:group:[parent].%user mapping
      ruleNode.put(JSON_NODE_POLICY, "user");

      //if the target queue has a parent part we add it to the node
      if (targetPath.hasParent()) {
        ruleNode.put(JSON_NODE_PARENT_QUEUE, targetPath.getParent());
      }
    } else {
      //static path mapping
      ruleNode.put(JSON_NODE_POLICY, "custom");
      ruleNode.put(JSON_NODE_CUSTOM_PLACEMENT, targetPath.getFullPath());
    }

    return ruleNode;
  }


  /**
   * This method will create the JSON node for a single Application Name
   * Mapping Rule.
   * @param match The name of the application to match for or %application to
   *              match all applications
   * @param target The queue to place to user into, some queue path variables
   *               are supported (%application).
   * @return The ObjectNode which represents the rule
   */
  private ObjectNode createApplicationNameMappingRule(
      String match, String target) {
    ObjectNode ruleNode = createDefaultRuleNode("application");
    QueuePath targetPath = new QueuePath(target);

    //we simply used the source match part all valid legacy matchers are valid
    //matchers for the JSON format as well
    ruleNode.put(JSON_NODE_MATCHES, match);

    if (targetPath.getLeafName().matches(MATCHER_APPLICATION)) {
      //[parent].%application mapping
      ruleNode.put(JSON_NODE_POLICY, "applicationName");

      //if the target queue has a parent part we add it to the node
      if (targetPath.hasParent()) {
        ruleNode.put(JSON_NODE_PARENT_QUEUE, targetPath.getParent());
      }
    } else {
      //static path mapping
      ruleNode.put(JSON_NODE_POLICY, "custom");
      ruleNode.put(JSON_NODE_CUSTOM_PLACEMENT, targetPath.getFullPath());
    }

    return ruleNode;
  }
}