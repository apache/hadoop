/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.server.resourcemanager.placement.DefaultPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.FSPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PrimaryGroupPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SecondaryGroupExistingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SpecifiedPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserPlacementRule;

class QueuePlacementConverter {

  private static final String USER = "%user";
  private static final String PRIMARY_GROUP = "%primary_group";
  private static final String SECONDARY_GROUP = "%secondary_group";

  Map<String, String> convertPlacementPolicy(PlacementManager placementManager,
      FSConfigToCSConfigRuleHandler ruleHandler, boolean userAsDefaultQueue) {
    StringBuilder mapping = new StringBuilder();
    Map<String, String> properties = new HashMap<>();

    if (userAsDefaultQueue) {
      mapping.append("u:" + USER + ":" + USER);
    }

    int ruleCount = 0;
    for (PlacementRule rule : placementManager.getPlacementRules()) {
      if (((FSPlacementRule)rule).getCreateFlag()) {
        ruleHandler.handleQueueAutoCreate(rule.getName());
      }

      ruleCount++;
      if (rule instanceof UserPlacementRule) {
        UserPlacementRule userRule = (UserPlacementRule) rule;
        if (mapping.length() > 0) {
          mapping.append(";");
        }

        // nested rule
        if (userRule.getParentRule() != null) {
          handleNestedRule(mapping, userRule);
        } else {
          if (!userAsDefaultQueue) {
            mapping.append("u:" + USER + ":" + USER);
          }
        }
      } else if (rule instanceof SpecifiedPlacementRule) {
        if (ruleCount > 1) {
          ruleHandler.handleSpecifiedNotFirstRule();
        }
        properties.put(
            "yarn.scheduler.capacity.queue-mappings-override.enable", "false");
      } else if (rule instanceof PrimaryGroupPlacementRule) {
        if (mapping.length() > 0) {
          mapping.append(";");
        }
        mapping.append("u:" + USER + ":" + PRIMARY_GROUP);
      } else if (rule instanceof DefaultPlacementRule) {
        DefaultPlacementRule defaultRule = (DefaultPlacementRule) rule;
        if (mapping.length() > 0) {
          mapping.append(";");
        }
        mapping.append("u:" + USER + ":").append(defaultRule.defaultQueueName);
      } else if (rule instanceof SecondaryGroupExistingPlacementRule) {
        // TODO: wait for YARN-9840
        mapping.append("u:" + USER + ":" + SECONDARY_GROUP);
      } else {
        throw new IllegalArgumentException("Unknown placement rule: " + rule);
      }
    }

    if (mapping.length() > 0) {
      properties.put("yarn.scheduler.capacity.queue-mappings",
          mapping.toString());
    }

    return properties;
  }

  private void handleNestedRule(StringBuilder mapping,
      UserPlacementRule userRule) {
    PlacementRule pr = userRule.getParentRule();
    if (pr instanceof PrimaryGroupPlacementRule) {
      // TODO: wait for YARN-9841
      mapping.append("u:" + USER + ":" + PRIMARY_GROUP + "." + USER);
    } else if (pr instanceof SecondaryGroupExistingPlacementRule) {
      // TODO: wait for YARN-9865
      mapping.append("u:" + USER + ":" + SECONDARY_GROUP + "." + USER);
    } else if (pr instanceof DefaultPlacementRule) {
      DefaultPlacementRule defaultRule = (DefaultPlacementRule) pr;
      mapping.append("u:" + USER + ":")
        .append(defaultRule.defaultQueueName)
        .append("." + USER);
    } else {
      throw new UnsupportedOperationException("Unsupported nested rule: "
          + pr.getClass().getCanonicalName());
    }
  }
}
