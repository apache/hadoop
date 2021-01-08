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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

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

  private ConversionOptions conversionOptions;

  public static final String MAX_CHILD_QUEUE_LIMIT =
      "maxChildQueue.limit";

  public static final String MAX_CAPACITY_PERCENTAGE =
      "maxCapacityPercentage.action";

  public static final String MAX_CHILD_CAPACITY =
      "maxChildCapacity.action";

  public static final String MAX_RESOURCES =
      "maxResources.action";

  public static final String MIN_RESOURCES =
      "minResources.action";

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

  public static final String FAIR_AS_DRF =
      "fairAsDrf.action";

  public static final String MAPPED_DYNAMIC_QUEUE =
      "mappedDynamicQueue.action";

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
  }

  public FSConfigToCSConfigRuleHandler(ConversionOptions conversionOptions) {
    this.properties = new Properties();
    this.actions = new HashMap<>();
    this.conversionOptions = conversionOptions;
  }

  @VisibleForTesting
  FSConfigToCSConfigRuleHandler(Properties props,
      ConversionOptions conversionOptions) {
    this.properties = props;
    this.actions = new HashMap<>();
    this.conversionOptions = conversionOptions;
    initPropertyActions();
  }

  public void initPropertyActions() {
    setActionForProperty(MAX_CAPACITY_PERCENTAGE);
    setActionForProperty(MAX_CHILD_CAPACITY);
    setActionForProperty(MAX_RESOURCES);
    setActionForProperty(MIN_RESOURCES);
    setActionForProperty(USER_MAX_RUNNING_APPS);
    setActionForProperty(USER_MAX_APPS_DEFAULT);
    setActionForProperty(DYNAMIC_MAX_ASSIGN);
    setActionForProperty(SPECIFIED_NOT_FIRST);
    setActionForProperty(RESERVATION_SYSTEM);
    setActionForProperty(QUEUE_AUTO_CREATE);
    setActionForProperty(FAIR_AS_DRF);
    setActionForProperty(MAPPED_DYNAMIC_QUEUE);
  }

  public void handleMaxCapacityPercentage(String queueName) {
    handle(MAX_CAPACITY_PERCENTAGE, null,
        format("<maxResources> defined in percentages for queue %s",
            queueName));
  }

  public void handleMaxChildCapacity() {
    handle(MAX_CHILD_CAPACITY, "<maxChildResources>", null);
  }

  public void handleMaxResources() {
    handle(MAX_RESOURCES, "<maxResources>", null);
  }

  public void handleMinResources() {
    handle(MIN_RESOURCES, "<minResources>", null);
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
            "Placement rules: queue auto-create is not supported (type: %s),"
            + " please configure auto-create-child-queue property manually",
            placementRule));
  }

  public void handleFairAsDrf(String queueName) {
    handle(FAIR_AS_DRF,
        null,
        format(
            "Queue %s will use DRF policy instead of Fair",
            queueName));
  }

  public void handleDynamicMappedQueue(String mapping, boolean create) {
    String msg = "Mapping rule %s is dynamic - this might cause inconsistent"
        + " behaviour compared to FS.";

    if (create) {
      msg += " Also, setting auto-create-child-queue=true is"
          + " necessary, because the create flag was set to true on the"
          + " original placement rule.";
    }

    handle(MAPPED_DYNAMIC_QUEUE,
        null,
        format(msg, mapping));
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
        conversionOptions.handleError(exceptionMessage);
        break;
      case WARNING:
        String loggedMsg = (message != null) ? message :
            format("Setting %s is not supported, ignoring conversion",
                fsSetting);
        conversionOptions.handleWarning(loggedMsg, LOG);
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
