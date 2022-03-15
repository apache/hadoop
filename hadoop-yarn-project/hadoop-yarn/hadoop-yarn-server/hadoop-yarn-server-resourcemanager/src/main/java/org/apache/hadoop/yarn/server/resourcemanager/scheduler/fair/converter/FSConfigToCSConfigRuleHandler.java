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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
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

  public static final String DYNAMIC_MAX_ASSIGN =
      "dynamicMaxAssign.action";

  public static final String RESERVATION_SYSTEM =
      "reservationSystem.action";

  public static final String QUEUE_AUTO_CREATE =
      "queueAutoCreate.action";

  public static final String FAIR_AS_DRF =
      "fairAsDrf.action";

  public static final String QUEUE_DYNAMIC_CREATE =
      "queueDynamicCreate.action";

  public static final String PARENT_DYNAMIC_CREATE =
      "parentDynamicCreate.action";

  public static final String CHILD_STATIC_DYNAMIC_CONFLICT =
      "childStaticDynamicConflict.action";

  public static final String PARENT_CHILD_CREATE_DIFFERS =
      "parentChildCreateDiff.action";

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
    setActionForProperty(DYNAMIC_MAX_ASSIGN);
    setActionForProperty(RESERVATION_SYSTEM);
    setActionForProperty(QUEUE_AUTO_CREATE);
    setActionForProperty(FAIR_AS_DRF);
    setActionForProperty(QUEUE_DYNAMIC_CREATE);
    setActionForProperty(PARENT_DYNAMIC_CREATE);
    setActionForProperty(CHILD_STATIC_DYNAMIC_CONFLICT);
    setActionForProperty(PARENT_CHILD_CREATE_DIFFERS);
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

  public void handleReservationSystem() {
    handle(RESERVATION_SYSTEM,
        null,
        "Conversion of reservation system is not supported");
  }

  public void handleFairAsDrf(String queueName) {
    handle(FAIR_AS_DRF,
        null,
        format(
            "Queue %s will use DRF policy instead of Fair",
            queueName));
  }

  public void handleRuleAutoCreateFlag(String queue) {
    String msg = format("Placement rules: create=true is enabled for"
        + " path %s - you have to make sure that these queues are"
        + " managed queues and set auto-create-child-queues=true."
        + " Other queues cannot statically exist under this path!", queue);

    handle(QUEUE_DYNAMIC_CREATE, null, msg);
  }

  public void handleFSParentCreateFlag(String parentPath) {
    String msg = format("Placement rules: create=true is enabled for parent"
        + " path %s - this is not supported in Capacity Scheduler."
        + " The parent must exist as a static queue and cannot be"
        + " created automatically", parentPath);

    handle(PARENT_DYNAMIC_CREATE, null, msg);
  }

  public void handleChildStaticDynamicConflict(String parentPath) {
    String msg = String.format("Placement rules: rule maps to"
        + " path %s, but this queue already contains static queue definitions!"
        + " This configuration is invalid and *must* be corrected", parentPath);

    handle(CHILD_STATIC_DYNAMIC_CONFLICT, null, msg);
  }

  public void handleFSParentAndChildCreateFlagDiff(Policy policy) {
    String msg = String.format("Placement rules: the policy %s originally uses"
        + " true/false or false/true \"create\" settings on the Fair Scheduler"
        + " side. This is not supported and create flag will be set"
        + " to *true* in the generated JSON rule chain", policy.name());

    handle(PARENT_CHILD_CREATE_DIFFERS, null, msg);
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
