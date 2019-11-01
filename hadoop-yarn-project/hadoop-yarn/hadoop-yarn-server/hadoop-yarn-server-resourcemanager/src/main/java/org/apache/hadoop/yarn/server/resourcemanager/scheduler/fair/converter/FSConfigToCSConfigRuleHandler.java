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

import static java.lang.String.format;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that determines what should happen if the FS-&gt;CS converter
 * encounters a property that is currently not supported.
 *
 * Acceptable values are either "abort" or "warning".
 */
public class FSConfigToCSConfigRuleHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSConfigToCSConfigRuleHandler.class);


  public static final String MAX_CHILD_QUEUE_LIMIT =
      "maxChildQueue.limit";

  public static final String MAX_CAPACITY_PERCENTAGE =
      "maxCapacityPercentage.action";

  public static final String MAX_CHILD_CAPACITY =
      "maxChildCapacity.action";

  public static final String USER_MAX_RUNNING_APPS =
      "userMaxRunningApps.action";

  public static final String USER_MAX_APPS_DEFAULT =
      "userMaxAppsDefault.action";

  public static final String DYNAMIC_MAX_ASSIGN =
      "dynamicMaxAssign.action";

  public static final String SPECIFIED_NOT_FIRST =
      "specifiedNotFirstRule.action";

  public static final String RESERVATION_SYSTEM =
      "reservationSystem.action";

  public static final String QUEUE_AUTO_CREATE =
      "queueAutoCreate.action";

  @VisibleForTesting
  enum RuleAction {
    WARNING,
    ABORT
  }

  private Map<String, RuleAction> actions;
  private Properties properties;

  void loadRulesFromFile(String ruleFile) throws IOException {
    if (ruleFile == null) {
      throw new IllegalArgumentException("Rule file cannot be null!");
    }

    properties = new Properties();
    try (InputStream is = new FileInputStream(new File(ruleFile))) {
      properties.load(is);
    }
    actions = new HashMap<>();
    initPropertyActions();
  }

  public FSConfigToCSConfigRuleHandler() {
    properties = new Properties();
    actions = new HashMap<>();
  }

  @VisibleForTesting
  FSConfigToCSConfigRuleHandler(Properties props) {
    properties = props;
    actions = new HashMap<>();
    initPropertyActions();
  }

  private void initPropertyActions() {
    setActionForProperty(MAX_CAPACITY_PERCENTAGE);
    setActionForProperty(MAX_CHILD_CAPACITY);
    setActionForProperty(USER_MAX_RUNNING_APPS);
    setActionForProperty(USER_MAX_APPS_DEFAULT);
    setActionForProperty(DYNAMIC_MAX_ASSIGN);
    setActionForProperty(SPECIFIED_NOT_FIRST);
    setActionForProperty(RESERVATION_SYSTEM);
    setActionForProperty(QUEUE_AUTO_CREATE);
  }

  public void handleMaxCapacityPercentage(String queueName) {
    handle(MAX_CAPACITY_PERCENTAGE, null,
        format("<maxResources> defined in percentages for queue %s",
            queueName));
  }

  public void handleMaxChildCapacity() {
    handle(MAX_CHILD_CAPACITY, "<maxChildResources>", null);
  }

  public void handleChildQueueCount(String queue, int count) {
    String value = properties.getProperty(MAX_CHILD_QUEUE_LIMIT);
    if (value != null) {
      if (StringUtils.isNumeric(value)) {
        int maxChildQueue = Integer.parseInt(value);
        if (count > maxChildQueue) {
          throw new ConversionException(
              format("Queue %s has too many children: %d", queue, count));
        }
      } else {
        throw new ConversionException(
            "Rule setting: maxChildQueue.limit is not an integer");
      }
    }
  }

  public void handleUserMaxApps() {
    handle(USER_MAX_RUNNING_APPS, "<maxRunningApps>", null);
  }

  public void handleUserMaxAppsDefault() {
    handle(USER_MAX_APPS_DEFAULT, "<userMaxAppsDefault>", null);
  }

  public void handleDynamicMaxAssign() {
    handle(DYNAMIC_MAX_ASSIGN,
        FairSchedulerConfiguration.DYNAMIC_MAX_ASSIGN, null);
  }

  public void handleSpecifiedNotFirstRule() {
    handle(SPECIFIED_NOT_FIRST,
        null,
        "The <specified> tag is not the first placement rule, this cannot be"
        + " converted properly");
  }

  public void handleReservationSystem() {
    handle(RESERVATION_SYSTEM,
        null,
        "Conversion of reservation system is not supported");
  }

  public void handleQueueAutoCreate(String placementRule) {
    handle(QUEUE_AUTO_CREATE,
        null,
        format(
            "Placement rules: queue auto-create is not supported (type: %s)",
            placementRule));
  }

  private void handle(String actionName, String fsSetting, String message) {
    RuleAction action = actions.get(actionName);

    if (action != null) {
      switch (action) {
      case ABORT:
        String exceptionMessage;
        if (message != null) {
          exceptionMessage = message;
        } else {
          exceptionMessage = format("Setting %s is not supported", fsSetting);
        }
        throw new UnsupportedPropertyException(exceptionMessage);
      case WARNING:
        if (message != null) {
          LOG.warn(message);
        } else {
          LOG.warn("Setting {} is not supported, ignoring conversion",
              fsSetting);
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown action " + action);
      }
    }
  }

  private void setActionForProperty(String property) {
    String action = properties.getProperty(property);

    if (action == null) {
      LOG.info("No rule set for {}, defaulting to WARNING", property);
      actions.put(property, RuleAction.WARNING);
    } else if (action.equalsIgnoreCase("warning")) {
      actions.put(property, RuleAction.WARNING);
    } else if (action.equalsIgnoreCase("abort")) {
      actions.put(property, RuleAction.ABORT);
    } else {
      LOG.warn("Unknown action {} set for rule {}, defaulting to WARNING",
          action, property);
      actions.put(property, RuleAction.WARNING);
    }
  }

  @VisibleForTesting
  public Map<String, RuleAction> getActions() {
    return actions;
  }
}
