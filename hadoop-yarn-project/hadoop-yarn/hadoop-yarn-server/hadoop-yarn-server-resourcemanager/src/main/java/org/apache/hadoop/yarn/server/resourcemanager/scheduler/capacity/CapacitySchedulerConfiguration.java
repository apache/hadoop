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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.QueueCapacityConfigParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.MappingRuleCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.QueueMappingBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AppPriorityACLConfigurationParser.AppPriorityACLKeyType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.WorkflowPriorityMappingsManager.WorkflowPriorityMapping;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.PriorityUtilizationQueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeLookupPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodePolicySpec;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyForPendingApps;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyWithExclusivePartitions;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getAutoCreatedQueueObjectTemplateConfPrefix;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getNodeLabelPrefix;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

public class CapacitySchedulerConfiguration extends ReservationSchedulerConfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(CapacitySchedulerConfiguration.class);

  private static final String CS_CONFIGURATION_FILE = "capacity-scheduler.xml";

  @Private
  public static final String PREFIX = "yarn.scheduler.capacity.";

  @Private
  public static final String DOT = ".";

  @Private
  public static final String MAXIMUM_APPLICATIONS_SUFFIX =
    "maximum-applications";

  @Private
  public static final String MAXIMUM_SYSTEM_APPLICATIONS =
    PREFIX + MAXIMUM_APPLICATIONS_SUFFIX;

  @Private
  public static final String MAXIMUM_AM_RESOURCE_SUFFIX =
    "maximum-am-resource-percent";

  @Private
  public static final String MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT =
    PREFIX + MAXIMUM_AM_RESOURCE_SUFFIX;

  @Private
  public static final String QUEUES = "queues";

  @Private
  public static final String CAPACITY = "capacity";

  @Private
  public static final String MAXIMUM_CAPACITY = "maximum-capacity";

  @Private
  public static final String USER_LIMIT = "minimum-user-limit-percent";

  @Private
  public static final String USER_LIMIT_FACTOR = "user-limit-factor";

  @Private
  public static final String USER_WEIGHT = "weight";

  @Private
  public static final String USER_SETTINGS = "user-settings";

  @Private
  public static final String USER_WEIGHT_REGEX = "\\S+\\." + USER_WEIGHT;

  @Private
  public static final Pattern USER_WEIGHT_PATTERN = Pattern.compile(
      USER_WEIGHT_REGEX);

  @Private
  public static final float DEFAULT_USER_WEIGHT = 1.0f;

  @Private
  public static final String STATE = "state";

  @Private
  public static final String ACCESSIBLE_NODE_LABELS = "accessible-node-labels";

  @Private
  public static final String DEFAULT_NODE_LABEL_EXPRESSION =
      "default-node-label-expression";

  public static final String RESERVE_CONT_LOOK_ALL_NODES = PREFIX
      + "reservations-continue-look-all-nodes";

  @Private
  public static final boolean DEFAULT_RESERVE_CONT_LOOK_ALL_NODES = true;

  public static final String SKIP_ALLOCATE_ON_NODES_WITH_RESERVED_CONTAINERS = PREFIX
      + "skip-allocate-on-nodes-with-reserved-containers";

  @Private
  public static final boolean DEFAULT_SKIP_ALLOCATE_ON_NODES_WITH_RESERVED_CONTAINERS = false;

  @Private
  public static final String MAXIMUM_ALLOCATION = "maximum-allocation";

  @Private
  public static final String MAXIMUM_ALLOCATION_MB = "maximum-allocation-mb";

  @Private
  public static final String MAXIMUM_ALLOCATION_VCORES =
          "maximum-allocation-vcores";
  /**
   * Ordering policy of queues
   */
  public static final String ORDERING_POLICY = "ordering-policy";

  /*
   * Ordering policy inside a leaf queue to sort apps
   */
  public static final String FIFO_APP_ORDERING_POLICY = "fifo";

  public static final String FAIR_APP_ORDERING_POLICY = "fair";

  public static final String FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY
      = "fifo-with-partitions";

  public static final String FIFO_FOR_PENDING_APPS
      = "fifo-for-pending-apps";

  public static final String DEFAULT_APP_ORDERING_POLICY =
      FIFO_APP_ORDERING_POLICY;

  @Private
  public static final int DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS = 10000;

  @Private
  public static final float
  DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT = 0.1f;

  @Private
  public static final float UNDEFINED = -1;

  @Private
  public static final float MINIMUM_CAPACITY_VALUE = 0;

  @Private
  public static final float MAXIMUM_CAPACITY_VALUE = 100;

  @Private
  public static final float DEFAULT_MAXIMUM_CAPACITY_VALUE = -1.0f;

  @Private
  public static final int DEFAULT_USER_LIMIT = 100;

  @Private
  public static final float DEFAULT_USER_LIMIT_FACTOR = 1.0f;

  @Private
  public static final String ALL_ACL = "*";

  @Private
  public static final String NONE_ACL = " ";

  @Private public static final String ENABLE_USER_METRICS =
      PREFIX +"user-metrics.enable";
  @Private public static final boolean DEFAULT_ENABLE_USER_METRICS = false;

  /** ResourceComparator for scheduling. */
  @Private public static final String RESOURCE_CALCULATOR_CLASS =
      PREFIX + "resource-calculator";

  @Private public static final Class<? extends ResourceCalculator>
  DEFAULT_RESOURCE_CALCULATOR_CLASS = DefaultResourceCalculator.class;

  @Private
  public static final String ROOT = "root";

  @Private
  public static final String NODE_LOCALITY_DELAY =
     PREFIX + "node-locality-delay";

  @Private
  public static final int DEFAULT_NODE_LOCALITY_DELAY = 40;

  @Private
  public static final String RACK_LOCALITY_ADDITIONAL_DELAY =
          PREFIX + "rack-locality-additional-delay";

  @Private
  public static final int DEFAULT_RACK_LOCALITY_ADDITIONAL_DELAY = -1;

  @Private
  public static final String RACK_LOCALITY_FULL_RESET =
      PREFIX + "rack-locality-full-reset";

  @Private
  public static final int DEFAULT_OFFSWITCH_PER_HEARTBEAT_LIMIT = 1;

  @Private
  public static final String OFFSWITCH_PER_HEARTBEAT_LIMIT =
      PREFIX + "per-node-heartbeat.maximum-offswitch-assignments";

  @Private
  public static final boolean DEFAULT_RACK_LOCALITY_FULL_RESET = true;

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_PREFIX =
      PREFIX + "schedule-asynchronously";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_ENABLE =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".enable";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".maximum-threads";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_PENDING_BACKLOGS =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".maximum-pending-backlogs";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_INTERVAL =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".scheduling-interval-ms";
  @Private
  public static final long DEFAULT_SCHEDULE_ASYNCHRONOUSLY_INTERVAL = 5;

  @Private
  public static final String APP_FAIL_FAST = PREFIX + "application.fail-fast";

  @Private
  public static final boolean DEFAULT_APP_FAIL_FAST = false;

  @Private
  public static final Integer
      DEFAULT_SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_PENDING_BACKLOGS = 100;

  @Private
  public static final boolean DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE = false;

  @Private
  public static final String QUEUE_MAPPING = PREFIX + "queue-mappings";

  @Private
  public static final String QUEUE_MAPPING_NAME =
      YarnConfiguration.QUEUE_PLACEMENT_RULES + ".app-name";

  @Private
  public static final String ENABLE_QUEUE_MAPPING_OVERRIDE = QUEUE_MAPPING + "-override.enable";

  @Private
  public static final boolean DEFAULT_ENABLE_QUEUE_MAPPING_OVERRIDE = false;

  @Private
  public static final String WORKFLOW_PRIORITY_MAPPINGS =
      PREFIX + "workflow-priority-mappings";

  @Private
  public static final String ENABLE_WORKFLOW_PRIORITY_MAPPINGS_OVERRIDE =
      WORKFLOW_PRIORITY_MAPPINGS + "-override.enable";

  @Private
  public static final boolean DEFAULT_ENABLE_WORKFLOW_PRIORITY_MAPPINGS_OVERRIDE = false;

  @Private
  public static final String QUEUE_PREEMPTION_DISABLED = "disable_preemption";

  @Private
  public static final String AM_PREEMPTION_ENABLED = PREFIX + "enabled_am_preemption";

  @Private
  public static final boolean DEFAULT_AM_PREEMPTION = true;

  @Private
  public static final String DEFAULT_APPLICATION_PRIORITY = "default-application-priority";

  @Private
  public static final Integer DEFAULT_CONFIGURATION_APPLICATION_PRIORITY = 0;

  @Private
  public static final String AVERAGE_CAPACITY = "average-capacity";

  @Private
  public static final String IS_RESERVABLE = "reservable";

  @Private
  public static final String RESERVATION_WINDOW = "reservation-window";

  @Private
  public static final String INSTANTANEOUS_MAX_CAPACITY =
      "instantaneous-max-capacity";

  @Private
  public static final String RESERVATION_ADMISSION_POLICY =
      "reservation-policy";

  @Private
  public static final String RESERVATION_AGENT_NAME = "reservation-agent";

  @Private
  public static final String RESERVATION_SHOW_RESERVATION_AS_QUEUE =
      "show-reservations-as-queues";

  @Private
  public static final String RESERVATION_PLANNER_NAME = "reservation-planner";

  @Private
  public static final String RESERVATION_MOVE_ON_EXPIRY =
      "reservation-move-on-expiry";

  @Private
  public static final String RESERVATION_ENFORCEMENT_WINDOW =
      "reservation-enforcement-window";

  @Private
  public static final String LAZY_PREEMPTION_ENABLED =
      PREFIX + "lazy-preemption-enabled";

  @Private
  public static final boolean DEFAULT_LAZY_PREEMPTION_ENABLED = false;

  @Private
  public static final String ASSIGN_MULTIPLE_ENABLED = PREFIX
      + "per-node-heartbeat.multiple-assignments-enabled";

  @Private
  public static final boolean DEFAULT_ASSIGN_MULTIPLE_ENABLED = true;

  /** Maximum number of containers to assign on each check-in. */
  @Private
  public static final String MAX_ASSIGN_PER_HEARTBEAT = PREFIX
      + "per-node-heartbeat.maximum-container-assignments";

  /**
   * Avoid potential risk that greedy assign multiple may involve
   * */
  @Private
  public static final int DEFAULT_MAX_ASSIGN_PER_HEARTBEAT = 100;

  /** Configuring absolute min/max resources in a queue. **/
  @Private
  public static final String MINIMUM_RESOURCE = "min-resource";

  @Private
  public static final String MAXIMUM_RESOURCE = "max-resource";

  public static final String DEFAULT_RESOURCE_TYPES = "memory,vcores";

  public static final String PATTERN_FOR_ABSOLUTE_RESOURCE = "^\\[[\\w\\.,\\-_=\\ /]+\\]$";

  public static final Pattern RESOURCE_PATTERN = Pattern.compile(PATTERN_FOR_ABSOLUTE_RESOURCE);

  private static final String WEIGHT_SUFFIX = "w";

  public static final String MAX_PARALLEL_APPLICATIONS = "max-parallel-apps";

  public static final int DEFAULT_MAX_PARALLEL_APPLICATIONS = Integer.MAX_VALUE;

  public static final String ALLOW_ZERO_CAPACITY_SUM =
      "allow-zero-capacity-sum";

  public static final boolean DEFAULT_ALLOW_ZERO_CAPACITY_SUM = false;
  public static final String MAPPING_RULE_FORMAT =
      PREFIX + "mapping-rule-format";
  public static final String MAPPING_RULE_JSON =
      PREFIX + "mapping-rule-json";
  public static final String MAPPING_RULE_JSON_FILE =
      PREFIX + "mapping-rule-json-file";

  public static final String MAPPING_RULE_FORMAT_LEGACY = "legacy";
  public static final String MAPPING_RULE_FORMAT_JSON = "json";

  public static final String MAPPING_RULE_FORMAT_DEFAULT =
      MAPPING_RULE_FORMAT_LEGACY;

  private static final QueueCapacityConfigParser queueCapacityConfigParser
      = new QueueCapacityConfigParser();
  private static final String LEGACY_QUEUE_MODE_ENABLED = PREFIX + "legacy-queue-mode.enabled";
  public static final boolean DEFAULT_LEGACY_QUEUE_MODE = true;

  private ConfigurationProperties configurationProperties;

  public static QueueCapacityConfigParser getQueueCapacityConfigParser() {
    return queueCapacityConfigParser;
  }

  public int getMaximumAutoCreatedQueueDepth(QueuePath queuePath) {
    return getInt(getQueuePrefix(queuePath) + MAXIMUM_QUEUE_DEPTH,
        getInt(PREFIX + MAXIMUM_QUEUE_DEPTH, DEFAULT_MAXIMUM_QUEUE_DEPTH));
  }

  public void setMaximumAutoCreatedQueueDepth(QueuePath queue, int value) {
    setInt(getQueuePrefix(queue) + MAXIMUM_QUEUE_DEPTH, value);
  }

  public void setMaximumAutoCreatedQueueDepth(int value) {
    setInt(PREFIX + MAXIMUM_QUEUE_DEPTH, value);
  }

  /**
   * Different resource types supported.
   */
  public enum AbsoluteResourceType {
    MEMORY, VCORES;
  }

  AppPriorityACLConfigurationParser priorityACLConfig = new AppPriorityACLConfigurationParser();

  public CapacitySchedulerConfiguration() {
    this(new Configuration());
  }

  public CapacitySchedulerConfiguration(Configuration configuration) {
    this(configuration, true);
  }

  public CapacitySchedulerConfiguration(Configuration configuration,
      boolean useLocalConfigurationProvider) {
    super(configuration);
    if (useLocalConfigurationProvider) {
      addResource(CS_CONFIGURATION_FILE);
    }
  }

  static String getUserPrefix(String user) {
    return PREFIX + "user." + user + DOT;
  }

  public void setMaximumSystemApplications(int numMaxApps) {
    setInt(MAXIMUM_SYSTEM_APPLICATIONS, numMaxApps);
  }

  public int getMaximumSystemApplications() {
    int maxApplications =
      getInt(MAXIMUM_SYSTEM_APPLICATIONS, DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS);
    return maxApplications;
  }

  public void setMaximumApplicationMasterResourcePercent(float percent) {
    setFloat(PREFIX + MAXIMUM_AM_RESOURCE_SUFFIX, percent);
  }

  public float getMaximumApplicationMasterResourcePercent() {
    return getFloat(MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
        DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT);
  }


  /**
   * Get the maximum applications per queue setting.
   * @param queue path of the queue
   * @return setting specified or -1 if not set
   */
  public int getMaximumApplicationsPerQueue(QueuePath queue) {
    int maxApplicationsPerQueue =
        getInt(getQueuePrefix(queue) + MAXIMUM_APPLICATIONS_SUFFIX,
            (int)UNDEFINED);
    return maxApplicationsPerQueue;
  }

  @VisibleForTesting
  public void setMaximumApplicationsPerQueue(QueuePath queue,
      int numMaxApps) {
    setInt(getQueuePrefix(queue) + MAXIMUM_APPLICATIONS_SUFFIX,
            numMaxApps);
  }

  /**
   * Get the maximum am resource percent per queue setting.
   * @param queue path of the queue
   * @return per queue setting or defaults to the global am-resource-percent
   *         setting if per queue setting not present
   */
  public float getMaximumApplicationMasterResourcePerQueuePercent(QueuePath queue) {
    return getFloat(getQueuePrefix(queue) + MAXIMUM_AM_RESOURCE_SUFFIX,
    		getMaximumApplicationMasterResourcePercent());
  }

  public void setMaximumApplicationMasterResourcePerQueuePercent(QueuePath queue,
      float percent) {
    setFloat(getQueuePrefix(queue) + MAXIMUM_AM_RESOURCE_SUFFIX, percent);
  }

  private void throwExceptionForUnexpectedWeight(float weight, QueuePath queue,
      String label) {
    if ((weight < -1e-6 && Math.abs(weight + 1) > 1e-6) || weight > 10000) {
      throw new IllegalArgumentException(
          "Illegal " + "weight=" + weight + " for queue=" + queue.getFullPath() + "label="
              + label
              + ". Acceptable values: [0, 10000], -1 is same as not set");
    }
  }

  public float getNonLabeledQueueWeight(QueuePath queue) {
    String configuredValue = get(getQueuePrefix(queue) + CAPACITY);
    float weight = extractFloatValueFromWeightConfig(configuredValue);
    throwExceptionForUnexpectedWeight(weight, queue, "");
    return weight;
  }

  public void setNonLabeledQueueWeight(QueuePath queue, float weight) {
    set(getQueuePrefix(queue) + CAPACITY, weight + WEIGHT_SUFFIX);
  }

  public void setLabeledQueueWeight(QueuePath queue, String label, float weight) {
    set(getNodeLabelPrefix(queue, label) + CAPACITY, weight + WEIGHT_SUFFIX);
  }

  public float getLabeledQueueWeight(QueuePath queue, String label) {
    String configuredValue = get(getNodeLabelPrefix(queue, label) + CAPACITY);
    float weight = extractFloatValueFromWeightConfig(configuredValue);
    throwExceptionForUnexpectedWeight(weight, queue, label);
    return weight;
  }

  public float getNonLabeledQueueCapacity(QueuePath queue) {
    String configuredCapacity = get(getQueuePrefix(queue) + CAPACITY);
    boolean absoluteResourceConfigured = (configuredCapacity != null)
        && RESOURCE_PATTERN.matcher(configuredCapacity).find();
    boolean isCapacityVectorFormat = queueCapacityConfigParser
        .isCapacityVectorFormat(configuredCapacity);
    if (absoluteResourceConfigured || configuredWeightAsCapacity(
        configuredCapacity) || isCapacityVectorFormat) {
      // Return capacity in percentage as 0 for non-root queues and 100 for
      // root.From AbstractCSQueue, absolute resource will be parsed and
      // updated. Once nodes are added/removed in cluster, capacity in
      // percentage will also be re-calculated.
      return queue.isRoot() ? 100.0f : 0f;
    }

    float capacity = queue.isRoot()
        ? 100.0f
        : (configuredCapacity == null)
            ? 0f
            : Float.parseFloat(configuredCapacity);
    if (capacity < MINIMUM_CAPACITY_VALUE
        || capacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException(
          "Illegal " + "capacity of " + capacity + " for queue " + queue.getFullPath());
    }
    LOG.debug("CSConf - getCapacity: queuePrefix={}, capacity={}",
        getQueuePrefix(queue), capacity);

    return capacity;
  }

  public void setCapacity(QueuePath queue, float capacity) {
    if (queue.isRoot()) {
      throw new IllegalArgumentException(
          "Cannot set capacity, root queue has a fixed capacity of 100.0f");
    }
    setFloat(getQueuePrefix(queue) + CAPACITY, capacity);
    LOG.debug("CSConf - setCapacity: queuePrefix={}, capacity={}",
        getQueuePrefix(queue), capacity);

  }

  @VisibleForTesting
  public void setCapacity(QueuePath queue, String absoluteResourceCapacity) {
    if (queue.isRoot()) {
      throw new IllegalArgumentException(
          "Cannot set capacity, root queue has a fixed capacity");
    }
    set(getQueuePrefix(queue) + CAPACITY, absoluteResourceCapacity);
    LOG.debug("CSConf - setCapacity: queuePrefix={}, capacity={}",
        getQueuePrefix(queue), absoluteResourceCapacity);

  }

  public float getNonLabeledQueueMaximumCapacity(QueuePath queue) {
    String configuredCapacity = get(getQueuePrefix(queue) + MAXIMUM_CAPACITY);
    boolean matcher = (configuredCapacity != null)
        && RESOURCE_PATTERN.matcher(configuredCapacity).find()
        || queueCapacityConfigParser.isCapacityVectorFormat(configuredCapacity);
    if (matcher) {
      // Return capacity in percentage as 0 for non-root queues and 100 for
      // root.From AbstractCSQueue, absolute resource will be parsed and
      // updated. Once nodes are added/removed in cluster, capacity in
      // percentage will also be re-calculated.
      return 100.0f;
    }

    float maxCapacity = (configuredCapacity == null)
        ? MAXIMUM_CAPACITY_VALUE
        : Float.parseFloat(configuredCapacity);
    maxCapacity = (maxCapacity == DEFAULT_MAXIMUM_CAPACITY_VALUE)
        ? MAXIMUM_CAPACITY_VALUE
        : maxCapacity;
    return maxCapacity;
  }

  public void setMaximumCapacity(QueuePath queue, float maxCapacity) {
    if (maxCapacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException("Illegal " +
          "maximum-capacity of " + maxCapacity + " for queue " + queue.getFullPath());
    }
    setFloat(getQueuePrefix(queue) + MAXIMUM_CAPACITY, maxCapacity);
    LOG.debug("CSConf - setMaxCapacity: queuePrefix={}, maxCapacity={}",
        getQueuePrefix(queue), maxCapacity);
  }

  public void setCapacityByLabel(QueuePath queue, String label, float capacity) {
    setFloat(getNodeLabelPrefix(queue, label) + CAPACITY, capacity);
  }

  @VisibleForTesting
  public void setCapacityByLabel(QueuePath queue, String label,
                                 String absoluteResourceCapacity) {
    set(getNodeLabelPrefix(queue, label) + CAPACITY, absoluteResourceCapacity);
  }

  public void setMaximumCapacityByLabel(QueuePath queue, String label,
      float capacity) {
    setFloat(getNodeLabelPrefix(queue, label) + MAXIMUM_CAPACITY, capacity);
  }

  public void setMaximumCapacityByLabel(QueuePath queue, String label,
      String absoluteResourceCapacity) {
    set(getNodeLabelPrefix(queue, label) + MAXIMUM_CAPACITY,
        absoluteResourceCapacity);
  }

  public float getUserLimit(QueuePath queue) {
    float defaultUserLimit = getFloat(PREFIX + USER_LIMIT, DEFAULT_USER_LIMIT);
    float userLimit = getFloat(getQueuePrefix(queue) + USER_LIMIT,
        defaultUserLimit);
    return userLimit;
  }

  // TODO (wangda): We need to better distinguish app ordering policy and queue
  // ordering policy's classname / configuration options, etc. And dedup code
  // if possible.
  @SuppressWarnings("unchecked")
  public <S extends SchedulableEntity> OrderingPolicy<S> getAppOrderingPolicy(
      QueuePath queue) {

    String policyType = get(getQueuePrefix(queue) + ORDERING_POLICY,
        DEFAULT_APP_ORDERING_POLICY);

    OrderingPolicy<S> orderingPolicy;

    if (policyType.trim().equals(FIFO_APP_ORDERING_POLICY)) {
       policyType = FifoOrderingPolicy.class.getName();
    }
    if (policyType.trim().equals(FAIR_APP_ORDERING_POLICY)) {
       policyType = FairOrderingPolicy.class.getName();
    }
    if (policyType.trim().equals(FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY)) {
      policyType = FifoOrderingPolicyWithExclusivePartitions.class.getName();
    }
    if (policyType.trim().equals(FIFO_FOR_PENDING_APPS)) {
      policyType = FifoOrderingPolicyForPendingApps.class.getName();
    }

    try {
      orderingPolicy = (OrderingPolicy<S>)
        Class.forName(policyType).newInstance();
    } catch (Exception e) {
      String message = "Unable to construct ordering policy for: " + policyType + ", " + e.getMessage();
      throw new RuntimeException(message, e);
    }

    Map<String, String> config = new HashMap<String, String>();
    String confPrefix = getQueuePrefix(queue) + ORDERING_POLICY + ".";
    for (Map.Entry<String, String> kv : this) {
      if (kv.getKey().startsWith(confPrefix)) {
         config.put(kv.getKey().substring(confPrefix.length()), kv.getValue());
      }
    }
    orderingPolicy.configure(config);
    return orderingPolicy;
  }

  public void setUserLimit(QueuePath queue, float userLimit) {
    setFloat(getQueuePrefix(queue) + USER_LIMIT, userLimit);
    LOG.debug("here setUserLimit: queuePrefix={}, userLimit={}",
        getQueuePrefix(queue), getUserLimit(queue));
  }

  @VisibleForTesting
  public void setDefaultUserLimit(float defaultUserLimit) {
    setFloat(PREFIX + USER_LIMIT, defaultUserLimit);
  }

  public float getUserLimitFactor(QueuePath queue) {
    float defaultUserLimitFactor = getFloat(PREFIX + USER_LIMIT_FACTOR, DEFAULT_USER_LIMIT_FACTOR);
    float userLimitFactor =
        getFloat(getQueuePrefix(queue) + USER_LIMIT_FACTOR,
            defaultUserLimitFactor);
    return userLimitFactor;
  }

  public void setUserLimitFactor(QueuePath queuePath, float userLimitFactor) {
    setFloat(getQueuePrefix(queuePath) + USER_LIMIT_FACTOR, userLimitFactor);
  }

  @VisibleForTesting
  public void setDefaultUserLimitFactor(float defaultUserLimitFactor) {
    setFloat(PREFIX + USER_LIMIT_FACTOR, defaultUserLimitFactor);
  }

  public QueueState getConfiguredState(QueuePath queue) {
    String state = get(getQueuePrefix(queue) + STATE);
    if (state == null) {
      return null;
    } else {
      return QueueState.valueOf(StringUtils.toUpperCase(state));
    }
  }

  public QueueState getState(QueuePath queue) {
    QueueState state = getConfiguredState(queue);
    return (state == null) ? QueueState.RUNNING : state;
  }

  @Private
  @VisibleForTesting
  public void setState(QueuePath queue, QueueState state) {
    set(getQueuePrefix(queue) + STATE, state.name());
  }

  public void setAccessibleNodeLabels(QueuePath queue, Set<String> labels) {
    if (labels == null) {
      return;
    }
    String str = StringUtils.join(",", labels);
    set(getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS, str);
  }

  public Set<String> getAccessibleNodeLabels(QueuePath queue) {
    String accessibleLabelStr =
        get(getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS);

    // When accessible-label is null,
    if (accessibleLabelStr == null) {
      // Only return null when queue is not ROOT
      if (!queue.isRoot()) {
        return null;
      }
    } else {
      // print a warning when accessibleNodeLabel specified in config and queue
      // is ROOT
      if (queue.isRoot()) {
        LOG.warn("Accessible node labels for root queue will be ignored,"
            + " it will be automatically set to \"*\".");
      }
    }

    // always return ANY for queue root
    if (queue.isRoot()) {
      return ImmutableSet.of(RMNodeLabelsManager.ANY);
    }

    // In other cases, split the accessibleLabelStr by ","
    Set<String> set = new HashSet<String>();
    for (String str : accessibleLabelStr.split(",")) {
      if (!str.trim().isEmpty()) {
        set.add(str.trim());
      }
    }

    // if labels contains "*", only keep ANY behind
    if (set.contains(RMNodeLabelsManager.ANY)) {
      set.clear();
      set.add(RMNodeLabelsManager.ANY);
    }
    return Collections.unmodifiableSet(set);
  }

  public void setCapacityVector(QueuePath queuePath, String label, String capacityVector) {
    String capacityPropertyName = getNodeLabelPrefix(queuePath, label) + CAPACITY;
    set(capacityPropertyName, capacityVector);
  }

  public void setMaximumCapacityVector(QueuePath queuePath, String label, String capacityVector) {
    String capacityPropertyName = getNodeLabelPrefix(queuePath, label) + MAXIMUM_CAPACITY;
    set(capacityPropertyName, capacityVector);
  }

  private boolean configuredWeightAsCapacity(String configureValue) {
    if (configureValue == null) {
      return false;
    }
    return configureValue.endsWith(WEIGHT_SUFFIX);
  }

  private float extractFloatValueFromWeightConfig(String configureValue) {
    if (!configuredWeightAsCapacity(configureValue)) {
      return -1f;
    } else {
      return Float.parseFloat(
          configureValue.substring(0, configureValue.indexOf(WEIGHT_SUFFIX)));
    }
  }

  private float internalGetLabeledQueueCapacity(QueuePath queue, String label,
      String suffix, float defaultValue) {
    String capacityPropertyName = getNodeLabelPrefix(queue, label) + suffix;
    String configuredCapacity = get(capacityPropertyName);
    boolean absoluteResourceConfigured =
        (configuredCapacity != null) && RESOURCE_PATTERN.matcher(
            configuredCapacity).find();
    if (absoluteResourceConfigured || configuredWeightAsCapacity(
        configuredCapacity) || queueCapacityConfigParser.isCapacityVectorFormat(configuredCapacity)) {
      // Return capacity in percentage as 0 for non-root queues and 100 for
      // root.From AbstractCSQueue, absolute resource, and weight will be parsed
      // and updated separately. Once nodes are added/removed in cluster,
      // capacity is percentage will also be re-calculated.
      return queue.isRoot() ? 100.0f : defaultValue;
    }

    float capacity = queue.isRoot() ? 100.0f
        : getFloat(capacityPropertyName, defaultValue);
    if (capacity < MINIMUM_CAPACITY_VALUE
        || capacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException(
          "Illegal capacity of " + capacity + " for node-label=" + label
              + " in queue=" + queue
              + ", valid capacity should in range of [0, 100].");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "CSConf - getCapacityOfLabel: prefix=" + getNodeLabelPrefix(queue,
              label) + ", capacity=" + capacity);
    }
    return capacity;
  }

  public float getLabeledQueueCapacity(QueuePath queue, String label) {
    return internalGetLabeledQueueCapacity(queue, label, CAPACITY, 0f);
  }

  public float getLabeledQueueMaximumCapacity(QueuePath queue, String label) {
    return internalGetLabeledQueueCapacity(queue, label, MAXIMUM_CAPACITY, 100f);
  }

  public String getDefaultNodeLabelExpression(QueuePath queue) {
    String defaultLabelExpression = get(getQueuePrefix(queue)
        + DEFAULT_NODE_LABEL_EXPRESSION);
    if (defaultLabelExpression == null) {
      return null;
    }
    return defaultLabelExpression.trim();
  }

  public void setDefaultNodeLabelExpression(QueuePath queue, String exp) {
    set(getQueuePrefix(queue) + DEFAULT_NODE_LABEL_EXPRESSION, exp);
  }

  public float getMaximumAMResourcePercentPerPartition(QueuePath queue,
      String label) {
    // If per-partition max-am-resource-percent is not configured,
    // use default value as max-am-resource-percent for this queue.
    return getFloat(getNodeLabelPrefix(queue, label)
        + MAXIMUM_AM_RESOURCE_SUFFIX,
        getMaximumApplicationMasterResourcePerQueuePercent(queue));
  }

  public void setMaximumAMResourcePercentPerPartition(QueuePath queue,
      String label, float percent) {
    setFloat(getNodeLabelPrefix(queue, label)
        + MAXIMUM_AM_RESOURCE_SUFFIX, percent);
  }

  /*
   * Returns whether we should continue to look at all heart beating nodes even
   * after the reservation limit was hit. The node heart beating in could
   * satisfy the request thus could be a better pick then waiting for the
   * reservation to be fullfilled.  This config is refreshable.
   */
  public boolean getReservationContinueLook() {
    return getBoolean(RESERVE_CONT_LOOK_ALL_NODES,
        DEFAULT_RESERVE_CONT_LOOK_ALL_NODES);
  }

  public boolean getSkipAllocateOnNodesWithReservedContainer() {
    return getBoolean(SKIP_ALLOCATE_ON_NODES_WITH_RESERVED_CONTAINERS,
        DEFAULT_SKIP_ALLOCATE_ON_NODES_WITH_RESERVED_CONTAINERS);
  }

  private static String getAclKey(QueueACL acl) {
    return "acl_" + StringUtils.toLowerCase(acl.toString());
  }

  public AccessControlList getAcl(QueuePath queue, QueueACL acl) {
    String queuePrefix = getQueuePrefix(queue);
    // The root queue defaults to all access if not defined
    // Sub queues inherit access if not defined
    String defaultAcl = queue.isRoot() ? ALL_ACL : NONE_ACL;
    String aclString = get(queuePrefix + getAclKey(acl), defaultAcl);
    return new AccessControlList(aclString);
  }

  public void setAcl(QueuePath queue, QueueACL acl, String aclString) {
    String queuePrefix = getQueuePrefix(queue);
    set(queuePrefix + getAclKey(acl), aclString);
  }

  private static String getAclKey(ReservationACL acl) {
    return "acl_" + StringUtils.toLowerCase(acl.toString());
  }

  private static String getAclKey(AccessType acl) {
    return "acl_" + StringUtils.toLowerCase(acl.toString());
  }

  /**
   * Creates a mapping of queue ACLs for a Legacy Auto Created Leaf Queue.
   *
   * @param parentQueuePath the parent's queue path
   * @return A mapping of the queue ACLs.
   */
  public Map<AccessType, AccessControlList> getACLsForLegacyAutoCreatedLeafQueue(
      QueuePath parentQueuePath) {
    final String prefix =
        getQueuePrefix(getAutoCreatedQueueObjectTemplateConfPrefix(
            parentQueuePath));

    Map<String, String> properties = new HashMap<>();
    for (QueueACL acl : QueueACL.values()) {
      final String key = getAclKey(acl);
      final String value = get(prefix + key);
      if (value != null) {
        properties.put(key, get(prefix + key));
      }
    }
    return getACLsFromProperties(properties);
  }

  /**
   * Creates a mapping of queue ACLs for a Flexible Auto Created Parent Queue.
   * The .parent-template is preferred to .template ACLs.
   *
   * @param aqc The AQC templates to use.
   * @return A mapping of the queue ACLs.
   */
  public static Map<AccessType, AccessControlList> getACLsForFlexibleAutoCreatedParentQueue(
      AutoCreatedQueueTemplate aqc) {
    return getACLsFromProperties(aqc.getParentOnlyProperties(),
        aqc.getTemplateProperties());
  }

  /**
   * Creates a mapping of queue ACLs for a Flexible Auto Created Leaf Queue.
   * The .leaf-template is preferred to .template ACLs.
   *
   * @param aqc The AQC templates to use.
   * @return A mapping of the queue ACLs.
   */
  public static Map<AccessType, AccessControlList> getACLsForFlexibleAutoCreatedLeafQueue(
      AutoCreatedQueueTemplate aqc) {
    return getACLsFromProperties(aqc.getLeafOnlyProperties(),
        aqc.getTemplateProperties());
  }

  /**
   * Transforms the string ACL properties to AccessType and AccessControlList mapping.
   *
   * @param properties The ACL properties.
   * @return A mapping of the queue ACLs.
   */
  private static Map<AccessType, AccessControlList> getACLsFromProperties(
      Map<String, String> properties) {
    return getACLsFromProperties(properties, new HashMap<>());
  }

  /**
   * Transforms the string ACL properties to AccessType and AccessControlList mapping.
   *
   * @param properties The ACL properties.
   * @param fallbackProperties The fallback properties to use.
   * @return A mapping of the queue ACLs.
   */
  private static Map<AccessType, AccessControlList> getACLsFromProperties(
      Map<String, String> properties, Map<String, String> fallbackProperties) {
    Map<AccessType, AccessControlList> acls = new HashMap<>();
    for (QueueACL acl : QueueACL.values()) {
      String aclStr = properties.get(getAclKey(acl));
      if (aclStr == null) {
        aclStr = fallbackProperties.get(getAclKey(acl));
        if (aclStr == null) {
          aclStr = NONE_ACL;
        }
      }
      acls.put(SchedulerUtils.toAccessType(acl),
          new AccessControlList(aclStr));
    }
    return acls;
  }

  @Override
  public Map<ReservationACL, AccessControlList> getReservationAcls(QueuePath
        queue) {
    Map<ReservationACL, AccessControlList> resAcls = new HashMap<>();
    for (ReservationACL acl : ReservationACL.values()) {
      resAcls.put(acl, getReservationAcl(queue, acl));
    }
    return resAcls;
  }

  private AccessControlList getReservationAcl(QueuePath queue, ReservationACL
        acl) {
    String queuePrefix = getQueuePrefix(queue);
    // The root queue defaults to all access if not defined
    // Sub queues inherit access if not defined
    String defaultAcl = ALL_ACL;
    String aclString = get(queuePrefix + getAclKey(acl), defaultAcl);
    return new AccessControlList(aclString);
  }

  private void setAcl(QueuePath queue, ReservationACL acl, String aclString) {
    String queuePrefix = getQueuePrefix(queue);
    set(queuePrefix + getAclKey(acl), aclString);
  }

  private void setAcl(QueuePath queue, AccessType acl, String aclString) {
    String queuePrefix = getQueuePrefix(queue);
    set(queuePrefix + getAclKey(acl), aclString);
  }

  public Map<AccessType, AccessControlList> getAcls(QueuePath queue) {
    Map<AccessType, AccessControlList> acls =
      new HashMap<AccessType, AccessControlList>();
    for (QueueACL acl : QueueACL.values()) {
      acls.put(SchedulerUtils.toAccessType(acl), getAcl(queue, acl));
    }
    return acls;
  }

  public void setAcls(QueuePath queue, Map<QueueACL, AccessControlList> acls) {
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      setAcl(queue, e.getKey(), e.getValue().getAclString());
    }
  }

  @VisibleForTesting
  public void setReservationAcls(QueuePath queue,
        Map<ReservationACL, AccessControlList> acls) {
    for (Map.Entry<ReservationACL, AccessControlList> e : acls.entrySet()) {
      setAcl(queue, e.getKey(), e.getValue().getAclString());
    }
  }

  @VisibleForTesting
  public void setPriorityAcls(QueuePath queue, Priority priority,
      Priority defaultPriority, String[] acls) {
    StringBuilder aclString = new StringBuilder();

    StringBuilder userAndGroup = new StringBuilder();
    for (int i = 0; i < acls.length; i++) {
      userAndGroup.append(AppPriorityACLKeyType.values()[i] + "=" + acls[i].trim())
          .append(" ");
    }

    aclString.append("[" + userAndGroup.toString().trim() + " "
        + "max_priority=" + priority.getPriority() + " " + "default_priority="
        + defaultPriority.getPriority() + "]");

    setAcl(queue, AccessType.APPLICATION_MAX_PRIORITY, aclString.toString());
  }

  public List<AppPriorityACLGroup> getPriorityAcls(QueuePath queue,
      Priority clusterMaxPriority) {
    String queuePrefix = getQueuePrefix(queue);
    String defaultAcl = ALL_ACL;
    String aclString = get(
        queuePrefix + getAclKey(AccessType.APPLICATION_MAX_PRIORITY),
        defaultAcl);

    return priorityACLConfig.getPriorityAcl(clusterMaxPriority, aclString);
  }

  public List<String> getQueues(QueuePath queue) {
    LOG.debug("CSConf - getQueues called for: queuePrefix={}",
        getQueuePrefix(queue));
    String[] queues = getStrings(getQueuePrefix(queue) + QUEUES);
    List<String> trimmedQueueNames = new ArrayList<String>();
    if (null != queues) {
      for (String s : queues) {
        trimmedQueueNames.add(s.trim());
      }
    }

    LOG.debug("CSConf - getQueues: queuePrefix={}, queues={}",
        getQueuePrefix(queue),
        ((queues == null) ? "" : StringUtils.arrayToString(queues)));

    return trimmedQueueNames;
  }

  public void setQueues(QueuePath queue, String[] subQueues) {
    set(getQueuePrefix(queue) + QUEUES, StringUtils.arrayToString(subQueues));
    LOG.debug("CSConf - setQueues: qPrefix={}, queues={}",
        getQueuePrefix(queue), StringUtils.arrayToString(subQueues));
  }

  public Resource getMinimumAllocation() {
    int minimumMemory = getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int minimumCores = getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    return Resources.createResource(minimumMemory, minimumCores);
  }

  @Private
  public Priority getQueuePriority(QueuePath queue) {
    String queuePolicyPrefix = getQueuePrefix(queue);
    Priority pri = Priority.newInstance(
        getInt(queuePolicyPrefix + "priority", 0));
    return pri;
  }

  @Private
  public void setQueuePriority(QueuePath queue, int priority) {
    String queuePolicyPrefix = getQueuePrefix(queue);
    setInt(queuePolicyPrefix + "priority", priority);
  }

  /**
   * Get maximum_allocation setting for the specified queue from the
   * configuration.
   *
   * @param queue
   *          name of the queue
   * @return Resource object or Resource.none if not set
   */
  public Resource getQueueMaximumAllocation(QueuePath queue) {
    String queuePrefix = getQueuePrefix(queue);
    String rawQueueMaxAllocation = get(queuePrefix + MAXIMUM_ALLOCATION, null);
    if (Strings.isNullOrEmpty(rawQueueMaxAllocation)) {
      return Resources.none();
    } else {
      return ResourceUtils.createResourceFromString(rawQueueMaxAllocation,
              ResourceUtils.getResourcesTypeInfo());
    }
  }

  public void setQueueMaximumAllocation(QueuePath queue, String maximumAllocation) {
    String queuePrefix = getQueuePrefix(queue);
    set(queuePrefix + MAXIMUM_ALLOCATION, maximumAllocation);
  }

  /**
   * Get all configuration properties parsed in a
   * {@code ConfigurationProperties} object.
   * @return configuration properties
   */
  public ConfigurationProperties getConfigurationProperties() {
    if (configurationProperties == null) {
      reinitializeConfigurationProperties();
    }

    return configurationProperties;
  }

  /**
   * Reinitializes the cached {@code ConfigurationProperties} object.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void reinitializeConfigurationProperties() {
    // Props are always Strings, therefore this cast is safe
    Map<String, String> props = (Map) getProps();
    configurationProperties = new ConfigurationProperties(props);
  }

  public void setQueueMaximumAllocationMb(QueuePath queue, int value) {
    String queuePrefix = getQueuePrefix(queue);
    setInt(queuePrefix + MAXIMUM_ALLOCATION_MB, value);
  }

  public void setQueueMaximumAllocationVcores(QueuePath queue, int value) {
    String queuePrefix = getQueuePrefix(queue);
    setInt(queuePrefix + MAXIMUM_ALLOCATION_VCORES, value);
  }

  public long getQueueMaximumAllocationMb(QueuePath queue) {
    String queuePrefix = getQueuePrefix(queue);
    return getInt(queuePrefix + MAXIMUM_ALLOCATION_MB, (int)UNDEFINED);
  }

  public int getQueueMaximumAllocationVcores(QueuePath queue) {
    String queuePrefix = getQueuePrefix(queue);
    return getInt(queuePrefix + MAXIMUM_ALLOCATION_VCORES, (int)UNDEFINED);
  }

  public boolean getEnableUserMetrics() {
    return getBoolean(ENABLE_USER_METRICS, DEFAULT_ENABLE_USER_METRICS);
  }

  public int getOffSwitchPerHeartbeatLimit() {
    int limit = getInt(OFFSWITCH_PER_HEARTBEAT_LIMIT,
        DEFAULT_OFFSWITCH_PER_HEARTBEAT_LIMIT);
    if (limit < 1) {
      LOG.warn(OFFSWITCH_PER_HEARTBEAT_LIMIT + "(" + limit + ") < 1. Using 1.");
      limit = 1;
    }
    return limit;
  }

  public void setOffSwitchPerHeartbeatLimit(int limit) {
    setInt(OFFSWITCH_PER_HEARTBEAT_LIMIT, limit);
  }

  public int getNodeLocalityDelay() {
    return getInt(NODE_LOCALITY_DELAY, DEFAULT_NODE_LOCALITY_DELAY);
  }

  @VisibleForTesting
  public void setNodeLocalityDelay(int nodeLocalityDelay) {
    setInt(NODE_LOCALITY_DELAY, nodeLocalityDelay);
  }

  public int getRackLocalityAdditionalDelay() {
    return getInt(RACK_LOCALITY_ADDITIONAL_DELAY,
        DEFAULT_RACK_LOCALITY_ADDITIONAL_DELAY);
  }

  public boolean getRackLocalityFullReset() {
    return getBoolean(RACK_LOCALITY_FULL_RESET,
        DEFAULT_RACK_LOCALITY_FULL_RESET);
  }

  public ResourceCalculator getResourceCalculator() {
    return ReflectionUtils.newInstance(
        getClass(
            RESOURCE_CALCULATOR_CLASS,
            DEFAULT_RESOURCE_CALCULATOR_CLASS,
            ResourceCalculator.class),
        this);
  }

  public boolean getUsePortForNodeName() {
    return getBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
  }

  public void setResourceComparator(
      Class<? extends ResourceCalculator> resourceCalculatorClass) {
    setClass(
        RESOURCE_CALCULATOR_CLASS,
        resourceCalculatorClass,
        ResourceCalculator.class);
  }

  public boolean getScheduleAynschronously() {
    return getBoolean(SCHEDULE_ASYNCHRONOUSLY_ENABLE,
      DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE);
  }

  public void setScheduleAynschronously(boolean async) {
    setBoolean(SCHEDULE_ASYNCHRONOUSLY_ENABLE, async);
  }

  public boolean getOverrideWithQueueMappings() {
    return getBoolean(ENABLE_QUEUE_MAPPING_OVERRIDE,
        DEFAULT_ENABLE_QUEUE_MAPPING_OVERRIDE);
  }

  @Private
  @VisibleForTesting
  public void setOverrideWithQueueMappings(boolean overrideWithQueueMappings) {
    setBoolean(ENABLE_QUEUE_MAPPING_OVERRIDE, overrideWithQueueMappings);
  }

  public List<QueueMapping> getQueueMappingEntity(
      String queueMappingSuffix) {
    String queueMappingName = buildQueueMappingRuleProperty(queueMappingSuffix);

    List<QueueMapping> mappings =
        new ArrayList<QueueMapping>();
    Collection<String> mappingsString =
        getTrimmedStringCollection(queueMappingName);
    for (String mappingValue : mappingsString) {
      String[] mapping =
          StringUtils.getTrimmedStringCollection(mappingValue, ":")
              .toArray(new String[] {});
      if (mapping.length != 2 || mapping[1].length() == 0) {
        throw new IllegalArgumentException(
            "Illegal queue mapping " + mappingValue);
      }

      //Mappings should be consistent, and have the parent path parsed
      // from the beginning
      QueueMapping m = QueueMapping.QueueMappingBuilder.create()
          .type(QueueMapping.MappingType.APPLICATION)
          .source(mapping[0])
          .parsePathString(mapping[1])
          .build();
      mappings.add(m);
    }

    return mappings;
  }

  private String buildQueueMappingRuleProperty (String queueMappingSuffix) {
    StringBuilder queueMapping = new StringBuilder();
    queueMapping.append(YarnConfiguration.QUEUE_PLACEMENT_RULES)
        .append(".").append(queueMappingSuffix);
    return queueMapping.toString();
  }

  @VisibleForTesting
  public void setQueueMappingEntities(List<QueueMapping> queueMappings,
      String queueMappingSuffix) {
    if (queueMappings == null) {
      return;
    }

    List<String> queueMappingStrs = new ArrayList<>();
    for (QueueMapping mapping : queueMappings) {
      queueMappingStrs.add(mapping.toTypelessString());
    }

    String mappingRuleProp = buildQueueMappingRuleProperty(queueMappingSuffix);
    setStrings(mappingRuleProp, StringUtils.join(",", queueMappingStrs));
  }

  public boolean getOverrideWithWorkflowPriorityMappings() {
    return getBoolean(ENABLE_WORKFLOW_PRIORITY_MAPPINGS_OVERRIDE,
        DEFAULT_ENABLE_WORKFLOW_PRIORITY_MAPPINGS_OVERRIDE);
  }

  public Collection<String> getWorkflowPriorityMappings() {
    return getTrimmedStringCollection(WORKFLOW_PRIORITY_MAPPINGS);
  }

  /**
   * Get user/group mappings to queues.
   *
   * @return user/groups mappings or null on illegal configs
   */
  public List<QueueMapping> getQueueMappings() {
    List<QueueMapping> mappings =
        new ArrayList<QueueMapping>();
    Collection<String> mappingsString =
        getTrimmedStringCollection(QUEUE_MAPPING);
    for (String mappingValue : mappingsString) {
      String[] mapping =
          StringUtils.getTrimmedStringCollection(mappingValue, ":")
              .toArray(new String[] {});
      if (mapping.length != 3 || mapping[1].length() == 0
          || mapping[2].length() == 0) {
        throw new IllegalArgumentException(
            "Illegal queue mapping " + mappingValue);
      }

      QueueMapping m;
      try {
        QueueMapping.MappingType mappingType;
        if (mapping[0].equals("u")) {
          mappingType = QueueMapping.MappingType.USER;
        } else if (mapping[0].equals("g")) {
          mappingType = QueueMapping.MappingType.GROUP;
        } else {
          throw new IllegalArgumentException(
              "unknown mapping prefix " + mapping[0]);
        }
        //forcing the queue path to be split to parent and leafQueue, to make
        //queue mapping parentPath and queueName consistent
        m = QueueMappingBuilder.create()
                .type(mappingType)
                .source(mapping[1])
                .parsePathString(mapping[2])
                .build();
      } catch (Throwable t) {
        throw new IllegalArgumentException(
            "Illegal queue mapping " + mappingValue);
      }

      if (m != null) {
        mappings.add(m);
      }
    }

    return mappings;
  }

  public List<MappingRule> parseLegacyMappingRules() {
    List<MappingRule> mappings = new ArrayList<MappingRule>();
    Collection<String> mappingsString =
        getTrimmedStringCollection(QUEUE_MAPPING);

    for (String mappingValue : mappingsString) {
      String[] mapping =
          StringUtils.getTrimmedStringCollection(mappingValue, ":")
              .toArray(new String[] {});
      if (mapping.length != 3 || mapping[1].length() == 0
          || mapping[2].length() == 0) {
        throw new IllegalArgumentException(
            "Illegal queue mapping " + mappingValue);
      }

      if (mapping[0].equals("u") || mapping[0].equals("g")) {
        mappings.add(MappingRule.createLegacyRule(
            mapping[0], mapping[1], mapping[2]));
      } else {
        throw new IllegalArgumentException(
            "unknown mapping prefix " + mapping[0]);
      }
    }

    mappingsString = getTrimmedStringCollection(QUEUE_MAPPING_NAME);
    for (String mappingValue : mappingsString) {
      String[] mapping =
          StringUtils.getTrimmedStringCollection(mappingValue, ":")
              .toArray(new String[] {});
      if (mapping.length != 2 || mapping[1].length() == 0) {
        throw new IllegalArgumentException(
            "Illegal queue mapping " + mappingValue);
      }

      mappings.add(MappingRule.createLegacyRule(mapping[0], mapping[1]));
    }

    return mappings;
  }

  public List<MappingRule> parseJSONMappingRules() throws IOException {
    String mappingJson = get(MAPPING_RULE_JSON, "");
    String mappingJsonFile = get(MAPPING_RULE_JSON_FILE, "");
    MappingRuleCreator creator = new MappingRuleCreator();

    if (!mappingJson.equals("")) {
      LOG.info("Reading mapping rules from provided inline JSON '{}'.",
          mappingJson);
      try {
        return creator.getMappingRulesFromString(mappingJson);
      } catch (IOException e) {
        LOG.error("Error parsing mapping rule inline JSON.");
        throw e;
      }
    } else if (!mappingJsonFile.equals("")) {
      LOG.info("Reading mapping rules from JSON file '{}'.",
          mappingJsonFile);
      try {
        return creator.getMappingRulesFromFile(mappingJsonFile.trim());
      } catch (IOException e) {
        LOG.error("Error reading or parsing mapping rule JSON file '{}'.",
            mappingJsonFile);
        throw e;
      }
    } else {
      LOG.warn("Mapping rule is set to JSON, but no inline JSON nor a JSON " +
          "file was provided! Starting with no mapping rules!");
    }

    return new ArrayList<>();
  }

  public void setMappingRuleFormat(String format) {
    set(MAPPING_RULE_FORMAT, format);
  }

  public void setMappingRuleJson(String json) {
    set(MAPPING_RULE_JSON, json);
  }

  public List<MappingRule> getMappingRules() throws IOException {
    String mappingFormat =
        get(MAPPING_RULE_FORMAT, MAPPING_RULE_FORMAT_DEFAULT);
    if (mappingFormat.equals(MAPPING_RULE_FORMAT_LEGACY)) {
      return parseLegacyMappingRules();
    } else if (mappingFormat.equals(MAPPING_RULE_FORMAT_JSON)) {
      return parseJSONMappingRules();
    } else {
      throw new IllegalArgumentException(
          "Illegal queue mapping format '" + mappingFormat + "' please use '" +
          MAPPING_RULE_FORMAT_LEGACY + "' or '" + MAPPING_RULE_FORMAT_JSON +
          "'");
    }
  }

  @Private
  @VisibleForTesting
  public void setQueuePlacementRules(Collection<String> queuePlacementRules) {
    if (queuePlacementRules == null) {
      return;
    }
    String str = StringUtils.join(",", queuePlacementRules);
    setStrings(YarnConfiguration.QUEUE_PLACEMENT_RULES, str);
  }

  @Private
  @VisibleForTesting
  public void setQueueMappings(List<QueueMapping> queueMappings) {
    if (queueMappings == null) {
      return;
    }

    List<String> queueMappingStrs = new ArrayList<>();
    for (QueueMapping mapping : queueMappings) {
      queueMappingStrs.add(mapping.toString());
    }

    setStrings(QUEUE_MAPPING, StringUtils.join(",", queueMappingStrs));
  }


  @Private
  @VisibleForTesting
  public void setAppNameMappings(List<QueueMapping> queueMappings) {
    if (queueMappings == null) {
      return;
    }

    List<String> queueMappingStrs = new ArrayList<>();
    for (QueueMapping mapping : queueMappings) {
      String rule = mapping.toString();
      String[] parts = rule.split(":");
      queueMappingStrs.add(parts[1] + ":" + parts[2]);
    }

    setStrings(QUEUE_MAPPING_NAME, StringUtils.join(",", queueMappingStrs));
  }

  @Private
  @VisibleForTesting
  void setWorkflowPriorityMappings(
      List<WorkflowPriorityMapping> workflowPriorityMappings) {
    setStrings(WORKFLOW_PRIORITY_MAPPINGS, WorkflowPriorityMappingsManager
        .getWorkflowPriorityMappingStr(workflowPriorityMappings));
  }

  public boolean isReservable(QueuePath queue) {
    boolean isReservable =
        getBoolean(getQueuePrefix(queue) + IS_RESERVABLE, false);
    return isReservable;
  }

  public void setReservable(QueuePath queue, boolean isReservable) {
    setBoolean(getQueuePrefix(queue) + IS_RESERVABLE, isReservable);
    LOG.debug("here setReservableQueue: queuePrefix={}, isReservableQueue={}",
        getQueuePrefix(queue), isReservable(queue));
  }

  @Override
  public long getReservationWindow(QueuePath queue) {
    long reservationWindow =
        getLong(getQueuePrefix(queue) + RESERVATION_WINDOW,
            DEFAULT_RESERVATION_WINDOW);
    return reservationWindow;
  }

  @Override
  public float getAverageCapacity(QueuePath queue) {
    float avgCapacity =
        getFloat(getQueuePrefix(queue) + AVERAGE_CAPACITY,
            MAXIMUM_CAPACITY_VALUE);
    return avgCapacity;
  }

  @Override
  public float getInstantaneousMaxCapacity(QueuePath queue) {
    float instMaxCapacity =
        getFloat(getQueuePrefix(queue) + INSTANTANEOUS_MAX_CAPACITY,
            MAXIMUM_CAPACITY_VALUE);
    return instMaxCapacity;
  }

  public void setInstantaneousMaxCapacity(QueuePath queue, float instMaxCapacity) {
    setFloat(getQueuePrefix(queue) + INSTANTANEOUS_MAX_CAPACITY,
        instMaxCapacity);
  }

  public void setReservationWindow(QueuePath queue, long reservationWindow) {
    setLong(getQueuePrefix(queue) + RESERVATION_WINDOW, reservationWindow);
  }

  public void setAverageCapacity(QueuePath queue, float avgCapacity) {
    setFloat(getQueuePrefix(queue) + AVERAGE_CAPACITY, avgCapacity);
  }

  @Override
  public String getReservationAdmissionPolicy(QueuePath queue) {
    String reservationPolicy =
        get(getQueuePrefix(queue) + RESERVATION_ADMISSION_POLICY,
            DEFAULT_RESERVATION_ADMISSION_POLICY);
    return reservationPolicy;
  }

  public void setReservationAdmissionPolicy(QueuePath queue,
      String reservationPolicy) {
    set(getQueuePrefix(queue) + RESERVATION_ADMISSION_POLICY, reservationPolicy);
  }

  @Override
  public String getReservationAgent(QueuePath queue) {
    String reservationAgent =
        get(getQueuePrefix(queue) + RESERVATION_AGENT_NAME,
            DEFAULT_RESERVATION_AGENT_NAME);
    return reservationAgent;
  }

  public void setReservationAgent(QueuePath queue, String reservationPolicy) {
    set(getQueuePrefix(queue) + RESERVATION_AGENT_NAME, reservationPolicy);
  }

  @Override
  public boolean getShowReservationAsQueues(QueuePath queuePath) {
    boolean showReservationAsQueues =
        getBoolean(getQueuePrefix(queuePath)
            + RESERVATION_SHOW_RESERVATION_AS_QUEUE,
            DEFAULT_SHOW_RESERVATIONS_AS_QUEUES);
    return showReservationAsQueues;
  }

  @Override
  public String getReplanner(QueuePath queue) {
    String replanner =
        get(getQueuePrefix(queue) + RESERVATION_PLANNER_NAME,
            DEFAULT_RESERVATION_PLANNER_NAME);
    return replanner;
  }

  @Override
  public boolean getMoveOnExpiry(QueuePath queue) {
    boolean killOnExpiry =
        getBoolean(getQueuePrefix(queue) + RESERVATION_MOVE_ON_EXPIRY,
            DEFAULT_RESERVATION_MOVE_ON_EXPIRY);
    return killOnExpiry;
  }

  @Override
  public long getEnforcementWindow(QueuePath queue) {
    long enforcementWindow =
        getLong(getQueuePrefix(queue) + RESERVATION_ENFORCEMENT_WINDOW,
            DEFAULT_RESERVATION_ENFORCEMENT_WINDOW);
    return enforcementWindow;
  }

  /**
   * Sets the <em>disable_preemption</em> property in order to indicate
   * whether or not container preemption will be disabled for the specified
   * queue.
   *
   * @param queue queue path
   * @param preemptionDisabled true if preemption is disabled on queue
   */
  public void setPreemptionDisabled(QueuePath queue, boolean preemptionDisabled) {
    setBoolean(getQueuePrefix(queue) + QUEUE_PREEMPTION_DISABLED,
               preemptionDisabled);
  }

  /**
   * Indicates whether preemption is disabled on the specified queue.
   *
   * @param queue queue path to query
   * @param defaultVal used as default if the <em>disable_preemption</em>
   * is not set in the configuration
   * @return true if preemption is disabled on <em>queue</em>, false otherwise
   */
  public boolean getPreemptionDisabled(QueuePath queue, boolean defaultVal) {
    boolean preemptionDisabled =
        getBoolean(getQueuePrefix(queue) + QUEUE_PREEMPTION_DISABLED,
                   defaultVal);
    return preemptionDisabled;
  }

  public boolean getAMPreemptionEnabled(){
    return getBoolean(AM_PREEMPTION_ENABLED, DEFAULT_AM_PREEMPTION);
  }

  /**
   * Indicates whether intra-queue preemption is disabled on the specified queue
   *
   * @param queue queue path to query
   * @param defaultVal used as default if the property is not set in the
   * configuration
   * @return true if preemption is disabled on queue, false otherwise
   */
  public boolean getIntraQueuePreemptionDisabled(QueuePath queue,
      boolean defaultVal) {
    return
        getBoolean(getQueuePrefix(queue) + INTRA_QUEUE_PREEMPTION_CONFIG_PREFIX
            + QUEUE_PREEMPTION_DISABLED, defaultVal);
  }

  public void setPreemptionObserveOnly(boolean value) {
    setBoolean(PREEMPTION_OBSERVE_ONLY, value);
  }

  public boolean getPreemptionObserveOnly() {
    return getBoolean(PREEMPTION_OBSERVE_ONLY, DEFAULT_PREEMPTION_OBSERVE_ONLY);
  }

  /**
   * Get configured node labels in a given queuePath.
   *
   * @param queuePath queue path.
   * @return configured node labels.
   */
  public Set<String> getConfiguredNodeLabels(QueuePath queuePath) {
    Set<String> configuredNodeLabels = new HashSet<String>();
    Entry<String, String> e = null;

    Iterator<Entry<String, String>> iter = iterator();
    while (iter.hasNext()) {
      e = iter.next();
      String key = e.getKey();

      if (key.startsWith(getQueuePrefix(queuePath) + ACCESSIBLE_NODE_LABELS
          + DOT)) {
        // Find <label-name> in
        // <queue-path>.accessible-node-labels.<label-name>.property
        int labelStartIdx =
            key.indexOf(ACCESSIBLE_NODE_LABELS)
                + ACCESSIBLE_NODE_LABELS.length() + 1;
        int labelEndIndx = key.indexOf('.', labelStartIdx);
        String labelName = key.substring(labelStartIdx, labelEndIndx);
        configuredNodeLabels.add(labelName);
      }
    }

    // always add NO_LABEL
    configuredNodeLabels.add(RMNodeLabelsManager.NO_LABEL);

    return configuredNodeLabels;
  }

  /**
   * Get configured node labels for all queues that have accessible-node-labels
   * prefixed properties set.
   * @return configured node labels
   */
  public Map<String, Set<String>> getConfiguredNodeLabelsByQueue() {
    Map<String, Set<String>> labelsByQueue = new HashMap<>();
    Map<String, String> schedulerEntries =
        getConfigurationProperties().getPropertiesWithPrefix(
            CapacitySchedulerConfiguration.PREFIX);

    for (Map.Entry<String, String> propertyEntry
        : schedulerEntries.entrySet()) {
      String key = propertyEntry.getKey();
      // Consider all keys that has accessible-node-labels prefix, excluding
      // <queue-path>.accessible-node-labels itself
      if (key.contains(ACCESSIBLE_NODE_LABELS + DOT)) {
        // Find <label-name> in
        // <queue-path>.accessible-node-labels.<label-name>.property
        int labelStartIdx =
            key.indexOf(ACCESSIBLE_NODE_LABELS)
                + ACCESSIBLE_NODE_LABELS.length() + 1;
        int labelEndIndx = key.indexOf('.', labelStartIdx);
        String labelName = key.substring(labelStartIdx, labelEndIndx);
        // Find queuePath and exclude "." at the end
        String queuePath = key.substring(0, key.indexOf(
            ACCESSIBLE_NODE_LABELS) - 1);
        if (!labelsByQueue.containsKey(queuePath)) {
          labelsByQueue.put(queuePath, new HashSet<>());
          labelsByQueue.get(queuePath).add(RMNodeLabelsManager.NO_LABEL);
        }
        labelsByQueue.get(queuePath).add(labelName);
      }
    }
    return labelsByQueue;
  }

  public Priority getClusterLevelApplicationMaxPriority() {
    return Priority.newInstance(getInt(
        YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        YarnConfiguration.DEFAULT_CLUSTER_LEVEL_APPLICATION_PRIORITY));
  }

  public Integer getDefaultApplicationPriorityConfPerQueue(QueuePath queue) {
    Integer defaultPriority = getInt(getQueuePrefix(queue)
        + DEFAULT_APPLICATION_PRIORITY,
        DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    return defaultPriority;
  }

  @VisibleForTesting
  public void setOrderingPolicy(QueuePath queue, String policy) {
    set(getQueuePrefix(queue) + ORDERING_POLICY, policy);
  }

  @VisibleForTesting
  public void setOrderingPolicyParameter(QueuePath queue,
      String parameterKey, String parameterValue) {
    set(getQueuePrefix(queue) + ORDERING_POLICY + "." + parameterKey,
        parameterValue);
  }

  public boolean getLazyPreemptionEnabled() {
    return getBoolean(LAZY_PREEMPTION_ENABLED, DEFAULT_LAZY_PREEMPTION_ENABLED);
  }

  public static boolean shouldAppFailFast(Configuration conf) {
    return conf.getBoolean(APP_FAIL_FAST, DEFAULT_APP_FAIL_FAST);
  }

  public void setDefaultMaxParallelApps(int value) {
    setInt(PREFIX + MAX_PARALLEL_APPLICATIONS, value);
  }

  public Integer getDefaultMaxParallelApps() {
    return getInt(PREFIX + MAX_PARALLEL_APPLICATIONS,
        DEFAULT_MAX_PARALLEL_APPLICATIONS);
  }

  public void setDefaultMaxParallelAppsPerUser(int value) {
    setInt(PREFIX + "user." + MAX_PARALLEL_APPLICATIONS, value);
  }

  public Integer getDefaultMaxParallelAppsPerUser() {
    return getInt(PREFIX + "user." + MAX_PARALLEL_APPLICATIONS,
        DEFAULT_MAX_PARALLEL_APPLICATIONS);
  }

  public void setMaxParallelAppsForUser(String user, int value) {
    setInt(getUserPrefix(user) + MAX_PARALLEL_APPLICATIONS, value);
  }

  public Integer getMaxParallelAppsForUser(String user) {
    String maxParallelAppsForUser = get(getUserPrefix(user)
        + MAX_PARALLEL_APPLICATIONS);

    return (maxParallelAppsForUser != null) ?
        Integer.valueOf(maxParallelAppsForUser)
        : getDefaultMaxParallelAppsPerUser();
  }

  public void setMaxParallelAppsForQueue(QueuePath queue, String value) {
    set(getQueuePrefix(queue) + MAX_PARALLEL_APPLICATIONS, value);
  }

  public Integer getMaxParallelAppsForQueue(QueuePath queue) {
    String maxParallelAppsForQueue = get(getQueuePrefix(queue)
        + MAX_PARALLEL_APPLICATIONS);

    return (maxParallelAppsForQueue != null) ?
        Integer.valueOf(maxParallelAppsForQueue)
        : getDefaultMaxParallelApps();
  }

  public boolean getAllowZeroCapacitySum(QueuePath queue) {
    return getBoolean(getQueuePrefix(queue)
        + ALLOW_ZERO_CAPACITY_SUM, DEFAULT_ALLOW_ZERO_CAPACITY_SUM);
  }

  public void setAllowZeroCapacitySum(QueuePath queue, boolean value) {
    setBoolean(getQueuePrefix(queue)
        + ALLOW_ZERO_CAPACITY_SUM, value);
  }
  private static final String PREEMPTION_CONFIG_PREFIX =
      "yarn.resourcemanager.monitor.capacity.preemption.";

  private static final String INTRA_QUEUE_PREEMPTION_CONFIG_PREFIX =
      "intra-queue-preemption.";

  /** If true, run the policy but do not affect the cluster with preemption and
   * kill events. */
  public static final String PREEMPTION_OBSERVE_ONLY =
      PREEMPTION_CONFIG_PREFIX + "observe_only";
  public static final boolean DEFAULT_PREEMPTION_OBSERVE_ONLY = false;

  /** Time in milliseconds between invocations of this policy */
  public static final String PREEMPTION_MONITORING_INTERVAL =
      PREEMPTION_CONFIG_PREFIX + "monitoring_interval";
  public static final long DEFAULT_PREEMPTION_MONITORING_INTERVAL = 3000L;

  /** Time in milliseconds between requesting a preemption from an application
   * and killing the container. */
  public static final String PREEMPTION_WAIT_TIME_BEFORE_KILL =
      PREEMPTION_CONFIG_PREFIX + "max_wait_before_kill";
  public static final long DEFAULT_PREEMPTION_WAIT_TIME_BEFORE_KILL = 15000L;

  /** Maximum percentage of resources preemptionCandidates in a single round. By
   * controlling this value one can throttle the pace at which containers are
   * reclaimed from the cluster. After computing the total desired preemption,
   * the policy scales it back within this limit. */
  public static final String TOTAL_PREEMPTION_PER_ROUND =
      PREEMPTION_CONFIG_PREFIX + "total_preemption_per_round";
  public static final float DEFAULT_TOTAL_PREEMPTION_PER_ROUND = 0.1f;

  /** Maximum amount of resources above the target capacity ignored for
   * preemption. This defines a deadzone around the target capacity that helps
   * prevent thrashing and oscillations around the computed target balance.
   * High values would slow the time to capacity and (absent natural
   * completions) it might prevent convergence to guaranteed capacity. */
  public static final String PREEMPTION_MAX_IGNORED_OVER_CAPACITY =
      PREEMPTION_CONFIG_PREFIX + "max_ignored_over_capacity";
  public static final double DEFAULT_PREEMPTION_MAX_IGNORED_OVER_CAPACITY = 0.1;
  /**
   * Given a computed preemption target, account for containers naturally
   * expiring and preempt only this percentage of the delta. This determines
   * the rate of geometric convergence into the deadzone ({@link
   * #PREEMPTION_MAX_IGNORED_OVER_CAPACITY}). For example, a termination factor of 0.5
   * will reclaim almost 95% of resources within 5 * {@link
   * #PREEMPTION_WAIT_TIME_BEFORE_KILL}, even absent natural termination. */
  public static final String PREEMPTION_NATURAL_TERMINATION_FACTOR =
      PREEMPTION_CONFIG_PREFIX + "natural_termination_factor";
  public static final double DEFAULT_PREEMPTION_NATURAL_TERMINATION_FACTOR =
      0.2;

  /**
   * By default, reserved resource will be excluded while balancing capacities
   * of queues.
   *
   * Why doing this? In YARN-4390, we added preemption-based-on-reserved-container
   * Support. To reduce unnecessary preemption for large containers. We will
   * not include reserved resources while calculating ideal-allocation in
   * FifoCandidatesSelector.
   *
   * Changes in YARN-4390 will significantly reduce number of containers preempted
   * When cluster has heterogeneous container requests. (Please check test
   * report: https://issues.apache.org/jira/secure/attachment/12796197/YARN-4390-test-results.pdf
   *
   * However, on the other hand, in some corner cases, especially for
   * fragmented cluster. It could lead to preemption cannot kick in in some
   * cases. Please see YARN-5731.
   *
   * So to solve the problem, make this change to be configurable, and please
   * note that it is an experimental option.
   */
  public static final String
      ADDITIONAL_RESOURCE_BALANCE_BASED_ON_RESERVED_CONTAINERS =
      PREEMPTION_CONFIG_PREFIX
          + "additional_res_balance_based_on_reserved_containers";
  public static final boolean
      DEFAULT_ADDITIONAL_RESOURCE_BALANCE_BASED_ON_RESERVED_CONTAINERS = false;

  /**
   * When calculating which containers to be preempted, we will try to preempt
   * containers for reserved containers first. By default is false.
   */
  public static final String PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS =
      PREEMPTION_CONFIG_PREFIX + "select_based_on_reserved_containers";
  public static final boolean DEFAULT_PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS =
      false;

  /**
   * For intra-queue preemption, priority/user-limit/fairness based selectors
   * can help to preempt containers.
   */
  public static final String INTRAQUEUE_PREEMPTION_ENABLED =
      PREEMPTION_CONFIG_PREFIX +
      INTRA_QUEUE_PREEMPTION_CONFIG_PREFIX + "enabled";
  public static final boolean DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED = false;

  /**
   * For intra-queue preemption, consider those queues which are above used cap
   * limit.
   */
  public static final String INTRAQUEUE_PREEMPTION_MINIMUM_THRESHOLD =
      PREEMPTION_CONFIG_PREFIX +
      INTRA_QUEUE_PREEMPTION_CONFIG_PREFIX + "minimum-threshold";
  public static final float DEFAULT_INTRAQUEUE_PREEMPTION_MINIMUM_THRESHOLD =
      0.5f;

  /**
   * For intra-queue preemption, allowable maximum-preemptable limit per queue.
   */
  public static final String INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT =
      PREEMPTION_CONFIG_PREFIX +
      INTRA_QUEUE_PREEMPTION_CONFIG_PREFIX + "max-allowable-limit";
  public static final float DEFAULT_INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT =
      0.2f;

  /**
   * For intra-queue preemption, enforce a preemption order such as
   * "userlimit_first" or "priority_first".
   */
  public static final String INTRAQUEUE_PREEMPTION_ORDER_POLICY = PREEMPTION_CONFIG_PREFIX
      + INTRA_QUEUE_PREEMPTION_CONFIG_PREFIX + "preemption-order-policy";
  public static final String DEFAULT_INTRAQUEUE_PREEMPTION_ORDER_POLICY = "userlimit_first";

  /**
   * Flag to determine whether or not to preempt containers from apps where some
   * used resources are less than the user's user limit.
   */
  public static final String CROSS_QUEUE_PREEMPTION_CONSERVATIVE_DRF =
      PREEMPTION_CONFIG_PREFIX + "conservative-drf";
  public static final Boolean DEFAULT_CROSS_QUEUE_PREEMPTION_CONSERVATIVE_DRF =
      false;

  public static final String IN_QUEUE_PREEMPTION_CONSERVATIVE_DRF =
      PREEMPTION_CONFIG_PREFIX + INTRA_QUEUE_PREEMPTION_CONFIG_PREFIX +
      "conservative-drf";
  public static final Boolean DEFAULT_IN_QUEUE_PREEMPTION_CONSERVATIVE_DRF =
      true;

  /**
   * Should we allow queues continue grow after all queue reaches their
   * guaranteed capacity.
   */
  public static final String PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED =
      PREEMPTION_CONFIG_PREFIX + "preemption-to-balance-queue-after-satisfied.enabled";
  public static final boolean DEFAULT_PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED = false;

  /**
   * How long we will wait to balance queues, by default it is 5 mins.
   */
  public static final String MAX_WAIT_BEFORE_KILL_FOR_QUEUE_BALANCE_PREEMPTION =
      PREEMPTION_CONFIG_PREFIX + "preemption-to-balance-queue-after-satisfied.max-wait-before-kill";
  public static final long
      DEFAULT_MAX_WAIT_BEFORE_KILL_FOR_QUEUE_BALANCE_PREEMPTION =
      300 * 1000;

  /**
   * Maximum application for a queue to be used when application per queue is
   * not defined.To be consistent with previous version the default value is set
   * as UNDEFINED.
   */
  @Private
  public static final String QUEUE_GLOBAL_MAX_APPLICATION =
      PREFIX + "global-queue-max-application";

  public int getGlobalMaximumApplicationsPerQueue() {
    int maxApplicationsPerQueue =
        getInt(QUEUE_GLOBAL_MAX_APPLICATION, (int) UNDEFINED);
    return maxApplicationsPerQueue;
  }

  public void setGlobalMaximumApplicationsPerQueue(int val) {
    setInt(QUEUE_GLOBAL_MAX_APPLICATION, val);
  }

  /**
   * Ordering policy inside a parent queue to sort queues
   */

  /**
   * Less relative usage queue can get next resource, this is default
   */
  public static final String QUEUE_UTILIZATION_ORDERING_POLICY = "utilization";

  /**
   * Combination of relative usage and priority
   */
  public static final String QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY =
      "priority-utilization";

  public static final String DEFAULT_QUEUE_ORDERING_POLICY =
      QUEUE_UTILIZATION_ORDERING_POLICY;


  @Private
  public void setQueueOrderingPolicy(QueuePath queue, String policy) {
    set(getQueuePrefix(queue) + ORDERING_POLICY, policy);
  }

  @Private
  public QueueOrderingPolicy getQueueOrderingPolicy(QueuePath queue,
      String parentPolicy) {
    String defaultPolicy = parentPolicy;
    if (null == defaultPolicy) {
      defaultPolicy = DEFAULT_QUEUE_ORDERING_POLICY;
    }

    String policyType = get(getQueuePrefix(queue) + ORDERING_POLICY,
        defaultPolicy).trim();

    QueueOrderingPolicy qop;
    if (policyType.equals(QUEUE_UTILIZATION_ORDERING_POLICY)) {
      // Doesn't respect priority
      qop = new PriorityUtilizationQueueOrderingPolicy(false);
    } else if (policyType.equals(
        QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY)) {
      qop = new PriorityUtilizationQueueOrderingPolicy(true);
    } else {
      try {
        qop = (QueueOrderingPolicy) Class.forName(policyType).newInstance();
      } catch (Exception e) {
        String message = "Unable to construct queue ordering policy="
            + policyType + " queue=" + queue.getFullPath();
        throw new YarnRuntimeException(message, e);
      }
    }

    return qop;
  }

  /*
   * Get global configuration for ordering policies
   */
  private String getOrderingPolicyGlobalConfigKey(String orderPolicyName,
      String configKey) {
    return PREFIX + ORDERING_POLICY + DOT + orderPolicyName + DOT + configKey;
  }

  /**
   * Global configurations of queue-priority-utilization ordering policy
   */
  private static final String UNDER_UTILIZED_PREEMPTION_ENABLED =
      "underutilized-preemption.enabled";

  /**
   * Do we allow under-utilized queue with higher priority to preempt queue
   * with lower priority *even if queue with lower priority is not satisfied*.
   *
   * For example, two queues, a and b
   * a.priority = 1, (a.used-capacity - a.reserved-capacity) = 40%
   * b.priority = 0, b.used-capacity = 30%
   *
   * Set this configuration to true to allow queue-a to preempt container from
   * queue-b.
   *
   * (The reason why deduct reserved-capacity from used-capacity for queue with
   * higher priority is: the reserved-capacity is just scheduler's internal
   * implementation to allocate large containers, it is not possible for
   * application to use such reserved-capacity. It is possible that a queue with
   * large container requests have a large number of containers but cannot
   * allocate from any of them. But scheduler will make sure a satisfied queue
   * will not preempt resource from any other queues. A queue is considered to
   * be satisfied when queue's used-capacity - reserved-capacity ≥
   * guaranteed-capacity.)
   *
   * @return allowed or not
   */
  public boolean getPUOrderingPolicyUnderUtilizedPreemptionEnabled() {
    return getBoolean(getOrderingPolicyGlobalConfigKey(
        QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY,
        UNDER_UTILIZED_PREEMPTION_ENABLED), false);
  }

  @VisibleForTesting
  public void setPUOrderingPolicyUnderUtilizedPreemptionEnabled(
      boolean enabled) {
    setBoolean(getOrderingPolicyGlobalConfigKey(
        QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY,
        UNDER_UTILIZED_PREEMPTION_ENABLED), enabled);
  }

  private static final String UNDER_UTILIZED_PREEMPTION_DELAY =
      "underutilized-preemption.reserved-container-delay-ms";

  /**
   * When a reserved container of an underutilized queue is created. Preemption
   * will kick in after specified delay (in ms).
   *
   * The total time to preempt resources for a reserved container from higher
   * priority queue will be: reserved-container-delay-ms +
   * {@link CapacitySchedulerConfiguration#PREEMPTION_WAIT_TIME_BEFORE_KILL}.
   *
   * This parameter is added to make preemption from lower priority queue which
   * is underutilized to be more careful. This parameter takes effect when
   * underutilized-preemption.enabled set to true.
   *
   * @return delay
   */
  public long getPUOrderingPolicyUnderUtilizedPreemptionDelay() {
    return getLong(getOrderingPolicyGlobalConfigKey(
        QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY,
        UNDER_UTILIZED_PREEMPTION_DELAY), 60000L);
  }

  @VisibleForTesting
  public void setPUOrderingPolicyUnderUtilizedPreemptionDelay(
      long timeout) {
    setLong(getOrderingPolicyGlobalConfigKey(
        QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY,
        UNDER_UTILIZED_PREEMPTION_DELAY), timeout);
  }

  private static final String UNDER_UTILIZED_PREEMPTION_MOVE_RESERVATION =
      "underutilized-preemption.allow-move-reservation";

  /**
   * When doing preemption from under-satisfied queues for priority queue.
   * Do we allow move reserved container from one host to another?
   *
   * @return allow or not
   */
  public boolean getPUOrderingPolicyUnderUtilizedPreemptionMoveReservation() {
    return getBoolean(getOrderingPolicyGlobalConfigKey(
        QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY,
        UNDER_UTILIZED_PREEMPTION_MOVE_RESERVATION), false);
  }

  @VisibleForTesting
  public void setPUOrderingPolicyUnderUtilizedPreemptionMoveReservation(
      boolean allowMoveReservation) {
    setBoolean(getOrderingPolicyGlobalConfigKey(
        QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY,
        UNDER_UTILIZED_PREEMPTION_MOVE_RESERVATION), allowMoveReservation);
  }

  /**
   * Get the weights of all users at this queue level from the configuration.
   * Used in computing user-specific user limit, relative to other users.
   * @param queuePath full queue path
   * @return map of user weights, if they exist. Otherwise, return empty map.
   */
  public UserWeights getAllUserWeightsForQueue(QueuePath queuePath) {
    return UserWeights.createByConfig(this, getConfigurationProperties(), queuePath);
  }

  public boolean getAssignMultipleEnabled() {
    return getBoolean(ASSIGN_MULTIPLE_ENABLED, DEFAULT_ASSIGN_MULTIPLE_ENABLED);
  }

  public int getMaxAssignPerHeartbeat() {
    return getInt(MAX_ASSIGN_PER_HEARTBEAT, DEFAULT_MAX_ASSIGN_PER_HEARTBEAT);
  }

  public static final String MAXIMUM_LIFETIME_SUFFIX =
      "maximum-application-lifetime";

  public static final String DEFAULT_LIFETIME_SUFFIX =
      "default-application-lifetime";

  public long getMaximumLifetimePerQueue(QueuePath queue) {
    long maximumLifetimePerQueue = getLong(
        getQueuePrefix(queue) + MAXIMUM_LIFETIME_SUFFIX, (long) UNDEFINED);
    return maximumLifetimePerQueue;
  }

  public void setMaximumLifetimePerQueue(QueuePath queue, long maximumLifetime) {
    setLong(getQueuePrefix(queue) + MAXIMUM_LIFETIME_SUFFIX, maximumLifetime);
  }

  public long getDefaultLifetimePerQueue(QueuePath queue) {
    long maximumLifetimePerQueue = getLong(
        getQueuePrefix(queue) + DEFAULT_LIFETIME_SUFFIX, (long) UNDEFINED);
    return maximumLifetimePerQueue;
  }

  public void setDefaultLifetimePerQueue(QueuePath queue, long defaultLifetime) {
    setLong(getQueuePrefix(queue) + DEFAULT_LIFETIME_SUFFIX, defaultLifetime);
  }

  @Private
  public static final boolean DEFAULT_AUTO_CREATE_CHILD_QUEUE_ENABLED = false;

  @Private
  private static final String AUTO_CREATE_CHILD_QUEUE_PREFIX =
      "auto-create-child-queue.";

  @Private
  public static final String AUTO_CREATE_CHILD_QUEUE_ENABLED =
      AUTO_CREATE_CHILD_QUEUE_PREFIX + "enabled";

  @Private
  protected static final String AUTO_QUEUE_CREATION_V2_PREFIX =
      "auto-queue-creation-v2.";

  @Private
  public static final String AUTO_QUEUE_CREATION_V2_ENABLED =
      AUTO_QUEUE_CREATION_V2_PREFIX + "enabled";

  @Private
  public static final String AUTO_QUEUE_CREATION_V2_MAX_QUEUES =
      AUTO_QUEUE_CREATION_V2_PREFIX + "max-queues";

  @Private
  public static final int
      DEFAULT_AUTO_QUEUE_CREATION_V2_MAX_QUEUES = 1000;

  @Private
  public static final String MAXIMUM_QUEUE_DEPTH =
      AUTO_QUEUE_CREATION_V2_PREFIX + "maximum-queue-depth";

  @Private
  public static final int DEFAULT_MAXIMUM_QUEUE_DEPTH = 2;

  @Private
  public static final boolean DEFAULT_AUTO_QUEUE_CREATION_ENABLED = false;

  @Private
  public static final String AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX =
      "leaf-queue-template";

  @Private
  public static final String AUTO_CREATE_QUEUE_MAX_QUEUES =
      "auto-create-child-queue.max-queues";

  @Private
  public static final int DEFAULT_AUTO_CREATE_QUEUE_MAX_QUEUES = 1000;

  /**
   * If true, this queue will be created as a Parent Queue which Auto Created
   * leaf child queues
   *
   * @param queuePath The queues path
   * @return true if auto create is enabled for child queues else false. Default
   * is false
   */
  @Private
  public boolean isAutoCreateChildQueueEnabled(QueuePath queuePath) {
    boolean isAutoCreateEnabled = getBoolean(
        getQueuePrefix(queuePath) + AUTO_CREATE_CHILD_QUEUE_ENABLED,
        DEFAULT_AUTO_CREATE_CHILD_QUEUE_ENABLED);
    return isAutoCreateEnabled;
  }

  @Private
  @VisibleForTesting
  public void setAutoCreateChildQueueEnabled(QueuePath queuePath,
      boolean autoCreationEnabled) {
    setBoolean(getQueuePrefix(queuePath) +
            AUTO_CREATE_CHILD_QUEUE_ENABLED,
        autoCreationEnabled);
  }

  public void setAutoQueueCreationV2Enabled(QueuePath queuePath,
      boolean autoQueueCreation) {
    setBoolean(
        getQueuePrefix(queuePath) + AUTO_QUEUE_CREATION_V2_ENABLED,
        autoQueueCreation);
  }

  public boolean isAutoQueueCreationV2Enabled(QueuePath queuePath) {
    boolean isAutoQueueCreation = getBoolean(
        getQueuePrefix(queuePath) + AUTO_QUEUE_CREATION_V2_ENABLED,
        DEFAULT_AUTO_QUEUE_CREATION_ENABLED);
    return isAutoQueueCreation;
  }

  @Private
  public static final String FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY =
      "auto-create-child-queue.fail-on-exceeding-parent-capacity";

  @Private
  public static final boolean DEFAULT_FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY =
      false;

  /**
   * Fail further auto leaf queue creation when parent's guaranteed capacity is
   * exceeded.
   *
   * @param queuePath the parent queue's path
   * @return true if configured to fail else false
   */
  @Private
  public boolean getShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(
      QueuePath queuePath) {
    boolean shouldFailAutoQueueCreationOnExceedingGuaranteedCapacity =
        getBoolean(getQueuePrefix(queuePath)
                + FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY,
            DEFAULT_FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY);
    return shouldFailAutoQueueCreationOnExceedingGuaranteedCapacity;
  }

  @VisibleForTesting
  @Private
  public void setShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(
          QueuePath queuePath, boolean autoCreationEnabled) {
    setBoolean(
        getQueuePrefix(queuePath) +
            FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY,
        autoCreationEnabled);
  }

  /**
   * Get the max number of leaf queues that are allowed to be created under
   * a parent queue
   *
   * @param queuePath the paret queue's path
   * @return the max number of leaf queues allowed to be auto created
   */
  @Private
  public int getAutoCreatedQueuesMaxChildQueuesLimit(QueuePath queuePath) {
    return getInt(getQueuePrefix(queuePath) +
            AUTO_CREATE_QUEUE_MAX_QUEUES,
        DEFAULT_AUTO_CREATE_QUEUE_MAX_QUEUES);
  }

  /**
   * Get the max number of queues that are allowed to be created under
   * a parent queue which allowed auto creation v2.
   *
   * @param queuePath the parent queue's path
   * @return the max number of queues allowed to be auto created,
   * in new auto created.
   */
  @Private
  public int getAutoCreatedQueuesV2MaxChildQueuesLimit(QueuePath queuePath) {
    return getInt(getQueuePrefix(queuePath) +
            AUTO_QUEUE_CREATION_V2_MAX_QUEUES,
        DEFAULT_AUTO_QUEUE_CREATION_V2_MAX_QUEUES);
  }

  @VisibleForTesting
  public void setAutoCreatedQueuesV2MaxChildQueuesLimit(QueuePath queuePath,
      int maxQueues) {
    setInt(getQueuePrefix(queuePath) +
        AUTO_QUEUE_CREATION_V2_MAX_QUEUES, maxQueues);
  }

  @Private
  public static final String AUTO_CREATED_QUEUE_MANAGEMENT_POLICY =
      AUTO_CREATE_CHILD_QUEUE_PREFIX + "management-policy";

  @Private
  public static final String DEFAULT_AUTO_CREATED_QUEUE_MANAGEMENT_POLICY =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity"
          + ".queuemanagement."
          + "GuaranteedOrZeroCapacityOverTimePolicy";

  @Private
  private static final String QUEUE_MANAGEMENT_CONFIG_PREFIX =
      "yarn.resourcemanager.monitor.capacity.queue-management.";

  /**
   * Time in milliseconds between invocations of this policy
   */
  @Private
  public static final String QUEUE_MANAGEMENT_MONITORING_INTERVAL =
      QUEUE_MANAGEMENT_CONFIG_PREFIX + "monitoring-interval";

  @Private
  public static final long DEFAULT_QUEUE_MANAGEMENT_MONITORING_INTERVAL =
      1500L;

  @Private
  public static final boolean
      DEFAULT_AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE = true;

  @Private
  public static final String AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE =
      AUTO_QUEUE_CREATION_V2_PREFIX + "queue-auto-removal.enable";

  // 300s for expired default
  @Private
  public static final long
      DEFAULT_AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME = 300;

  @Private
  public static final String AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME =
      PREFIX + AUTO_QUEUE_CREATION_V2_PREFIX + "queue-expiration-time";

  /**
   * If true, auto created queue with weight mode
   * will be deleted when queue is expired.
   * @param queuePath the queue's path for auto deletion check
   * @return true if auto created queue's deletion when expired is enabled
   * else false. Default
   * is true.
   */
  @Private
  public boolean isAutoExpiredDeletionEnabled(QueuePath queuePath) {
    boolean isAutoExpiredDeletionEnabled = getBoolean(
        getQueuePrefix(queuePath) +
            AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE,
        DEFAULT_AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE);
    return isAutoExpiredDeletionEnabled;
  }

  @Private
  @VisibleForTesting
  public void setAutoExpiredDeletionEnabled(QueuePath queuePath,
      boolean autoRemovalEnable) {
    setBoolean(getQueuePrefix(queuePath) +
            AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE,
        autoRemovalEnable);
  }

  @Private
  @VisibleForTesting
  public void setAutoExpiredDeletionTime(long time) {
    setLong(AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME, time);
  }

  @Private
  @VisibleForTesting
  public long getAutoExpiredDeletionTime() {
    return getLong(AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
        DEFAULT_AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME);
  }

  /**
   * Time in milliseconds between invocations
   * of QueueConfigurationAutoRefreshPolicy.
   */
  @Private
  public static final String QUEUE_AUTO_REFRESH_MONITORING_INTERVAL =
      PREFIX + "queue.auto.refresh.monitoring-interval";

  @Private
  public static final long DEFAULT_QUEUE_AUTO_REFRESH_MONITORING_INTERVAL =
      5000L;

  /**
   * Queue Management computation policy for Auto Created queues
   * @param queue The queue's path
   * @return Configured policy class name
   */
  @Private
  public String getAutoCreatedQueueManagementPolicy(QueuePath queue) {
    String autoCreatedQueueManagementPolicy =
        get(getQueuePrefix(queue) + AUTO_CREATED_QUEUE_MANAGEMENT_POLICY,
            DEFAULT_AUTO_CREATED_QUEUE_MANAGEMENT_POLICY);
    return autoCreatedQueueManagementPolicy;
  }

  /**
   * Get The policy class configured to manage capacities for auto created leaf
   * queues under the specified parent
   *
   * @param queue The parent queue's path
   * @return The policy class configured to manage capacities for auto created
   * leaf queues under the specified parent queue
   */
  @Private
  protected AutoCreatedQueueManagementPolicy
  getAutoCreatedQueueManagementPolicyClass(
      QueuePath queue) {

    String queueManagementPolicyClassName =
        getAutoCreatedQueueManagementPolicy(queue);
    LOG.info("Using Auto Created Queue Management Policy: "
        + queueManagementPolicyClassName + " for queue: " + queue.getFullPath());
    try {
      Class<?> queueManagementPolicyClazz = getClassByName(
          queueManagementPolicyClassName);
      if (AutoCreatedQueueManagementPolicy.class.isAssignableFrom(
          queueManagementPolicyClazz)) {
        return (AutoCreatedQueueManagementPolicy) ReflectionUtils.newInstance(
            queueManagementPolicyClazz, this);
      } else{
        throw new YarnRuntimeException(
            "Class: " + queueManagementPolicyClassName + " not instance of "
                + AutoCreatedQueueManagementPolicy.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate " + "AutoCreatedQueueManagementPolicy: "
              + queueManagementPolicyClassName + " for queue: " + queue.getFullPath(),
          e);
    }
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueConfigCapacity(QueuePath queuePath,
      float val) {
    QueuePath leafQueueConfPrefix = getAutoCreatedQueueObjectTemplateConfPrefix(
        queuePath);
    setCapacity(leafQueueConfPrefix, val);
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueTemplateCapacityByLabel(QueuePath queuePath,
      String label, float val) {
    QueuePath leafQueueConfPrefix =
         getAutoCreatedQueueObjectTemplateConfPrefix(queuePath);
    setCapacityByLabel(leafQueueConfPrefix, label, val);
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueTemplateCapacityByLabel(QueuePath queuePath,
      String label, Resource resource) {

    QueuePath leafQueueConfPrefix =
         getAutoCreatedQueueObjectTemplateConfPrefix(queuePath);

    StringBuilder resourceString = new StringBuilder();

    resourceString
        .append("[" + AbsoluteResourceType.MEMORY.toString().toLowerCase() + "="
            + resource.getMemorySize() + ","
            + AbsoluteResourceType.VCORES.toString().toLowerCase() + "="
            + resource.getVirtualCores()
            + ResourceUtils.
            getCustomResourcesStrings(resource) + "]");

    setCapacityByLabel(leafQueueConfPrefix, label, resourceString.toString());
  }

  @Private
  @VisibleForTesting
  public void setAutoCreatedLeafQueueConfigMaxCapacity(QueuePath queuePath,
      float val) {
    QueuePath leafQueueConfPrefix = getAutoCreatedQueueObjectTemplateConfPrefix(
        queuePath);
    setMaximumCapacity(leafQueueConfPrefix, val);
  }

  @Private
  @VisibleForTesting
  public void setAutoCreatedLeafQueueTemplateMaxCapacity(QueuePath queuePath,
      String label, float val) {
    QueuePath leafQueueConfPrefix = getAutoCreatedQueueObjectTemplateConfPrefix(
        queuePath);
    setMaximumCapacityByLabel(leafQueueConfPrefix, label, val);
  }

  @Private
  @VisibleForTesting
  public void setAutoCreatedLeafQueueTemplateMaxCapacity(QueuePath queuePath,
      String label, Resource resource) {
    QueuePath leafQueueConfPrefix = getAutoCreatedQueueObjectTemplateConfPrefix(
        queuePath);

    StringBuilder resourceString = new StringBuilder();

    resourceString
        .append("[" + AbsoluteResourceType.MEMORY.toString().toLowerCase() + "="
            + resource.getMemorySize() + ","
            + AbsoluteResourceType.VCORES.toString().toLowerCase() + "="
            + resource.getVirtualCores()
            + ResourceUtils.
            getCustomResourcesStrings(resource) + "]");

    setMaximumCapacityByLabel(leafQueueConfPrefix, label, resourceString.toString());
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueConfigUserLimit(QueuePath queuePath,
      int val) {
    QueuePath leafQueueConfPrefix = getAutoCreatedQueueObjectTemplateConfPrefix(
        queuePath);
    setUserLimit(leafQueueConfPrefix, val);
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueConfigUserLimitFactor(QueuePath queuePath,
      float val) {
    QueuePath leafQueueConfPrefix = getAutoCreatedQueueObjectTemplateConfPrefix(
        queuePath);
    setUserLimitFactor(leafQueueConfPrefix, val);
  }

  @Private
  @VisibleForTesting
  public void setAutoCreatedLeafQueueConfigDefaultNodeLabelExpression(QueuePath
      queuePath,
      String expression) {
    QueuePath leafQueueConfPrefix = getAutoCreatedQueueObjectTemplateConfPrefix(
        queuePath);
    setDefaultNodeLabelExpression(leafQueueConfPrefix, expression);
  }

  @Private
  @VisibleForTesting
  public void setAutoCreatedLeafQueueConfigMaximumAllocation(QueuePath
         queuePath, String expression) {
    QueuePath leafQueueConfPrefix = getAutoCreatedQueueObjectTemplateConfPrefix(
        queuePath);
    setQueueMaximumAllocation(leafQueueConfPrefix, expression);
  }

  public static String getUnits(String resourceValue) {
    String units;
    for (int i = 0; i < resourceValue.length(); i++) {
      if (Character.isAlphabetic(resourceValue.charAt(i))) {
        units = resourceValue.substring(i);
        if (StringUtils.isAlpha(units)) {
          return units;
        }
      }
    }
    return "";
  }

  /**
   * Get absolute minimum resource requirement for a queue.
   *
   * @param label
   *          NodeLabel
   * @param queue
   *          queue path
   * @param resourceTypes
   *          Resource types
   * @return ResourceInformation
   */
  public Resource getMinimumResourceRequirement(String label, QueuePath queue,
      Set<String> resourceTypes) {
    return internalGetLabeledResourceRequirementForQueue(queue, label,
        resourceTypes, CAPACITY);
  }

  /**
   * Get absolute maximum resource requirement for a queue.
   *
   * @param label
   *          NodeLabel
   * @param queue
   *          queue path
   * @param resourceTypes
   *          Resource types
   * @return Resource
   */
  public Resource getMaximumResourceRequirement(String label, QueuePath queue,
      Set<String> resourceTypes) {
    return internalGetLabeledResourceRequirementForQueue(queue, label,
        resourceTypes, MAXIMUM_CAPACITY);
  }

  @VisibleForTesting
  public void setMinimumResourceRequirement(String label, QueuePath queue,
      Resource resource) {
    updateMinMaxResourceToConf(label, queue, resource, CAPACITY);
  }

  @VisibleForTesting
  public void setMaximumResourceRequirement(String label, QueuePath queue,
      Resource resource) {
    updateMinMaxResourceToConf(label, queue, resource, MAXIMUM_CAPACITY);
  }

  public Map<String, QueueCapacityVector> parseConfiguredResourceVector(
      QueuePath queuePath, Set<String> labels) {
    Map<String, QueueCapacityVector> queueResourceVectors = new HashMap<>();
    for (String label : labels) {
      String propertyName = QueuePrefixes.getNodeLabelPrefix(
          queuePath, label) + CapacitySchedulerConfiguration.CAPACITY;
      String capacityString = get(propertyName);
      queueResourceVectors.put(label, queueCapacityConfigParser.parse(capacityString, queuePath));
    }

    return queueResourceVectors;
  }

  public Map<String, QueueCapacityVector> parseConfiguredMaximumCapacityVector(
      QueuePath queuePath, Set<String> labels, QueueCapacityVector defaultVector) {
    Map<String, QueueCapacityVector> queueResourceVectors = new HashMap<>();
    for (String label : labels) {
      String propertyName = QueuePrefixes.getNodeLabelPrefix(
          queuePath, label) + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY;
      String capacityString = get(propertyName);
      QueueCapacityVector capacityVector = queueCapacityConfigParser.parse(capacityString,
          queuePath);
      if (capacityVector.isEmpty()) {
        capacityVector = defaultVector;
      }
      queueResourceVectors.put(label, capacityVector);
    }

    return queueResourceVectors;
  }

  private void updateMinMaxResourceToConf(String label, QueuePath queue,
      Resource resource, String type) {
    if (queue.isRoot()) {
      throw new IllegalArgumentException(
          "Cannot set resource, root queue will take 100% of cluster capacity");
    }

    StringBuilder resourceString = new StringBuilder();

    resourceString
        .append("[" + AbsoluteResourceType.MEMORY.toString().toLowerCase() + "="
            + resource.getMemorySize() + ","
            + AbsoluteResourceType.VCORES.toString().toLowerCase() + "="
            + resource.getVirtualCores()
            + ResourceUtils.
            getCustomResourcesStrings(resource) + "]");

    String prefix = getQueuePrefix(queue) + type;
    if (!label.isEmpty()) {
      prefix = getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS + DOT + label
          + DOT + type;
    }
    set(prefix, resourceString.toString());
  }

  public boolean checkConfigTypeIsAbsoluteResource(String label, QueuePath queue,
      Set<String> resourceTypes) {
    String propertyName = getNodeLabelPrefix(queue, label) + CAPACITY;
    String resourceString = get(propertyName);
    if (resourceString == null || resourceString.isEmpty()) {
      return false;
    }

    Matcher matcher = RESOURCE_PATTERN.matcher(resourceString);
    if (matcher.find()) {
      return true;
    }
    return false;
  }

  private Resource internalGetLabeledResourceRequirementForQueue(QueuePath queue,
      String label, Set<String> resourceTypes, String suffix) {
    String propertyName = getNodeLabelPrefix(queue, label) + suffix;
    String resourceString = get(propertyName);
    if (resourceString == null || resourceString.isEmpty()) {
      return Resources.none();
    }

    // Define resource here.
    Resource resource = Resource.newInstance(0L, 0);
    Matcher matcher = RESOURCE_PATTERN.matcher(resourceString);

    /*
     * Absolute resource configuration for a queue will be grouped by "[]".
     * Syntax of absolute resource config could be like below
     * "memory=4Gi vcores=2". Ideally this means "4GB of memory and 2 vcores".
     */
    if (matcher.find()) {
      // Get the sub-group.
      String subGroup = matcher.group(0);
      if (subGroup.trim().isEmpty()) {
        return Resources.none();
      }
      subGroup = subGroup.substring(1, subGroup.length() - 1);
      for (String kvPair : subGroup.trim().split(",")) {
        String[] splits = kvPair.split("=");

        // Ensure that each sub string is key value pair separated by '='.
        if (splits != null && splits.length > 1) {
          updateResourceValuesFromConfig(resourceTypes, resource, splits);
        }
      }
    }

    // Memory has to be configured always.
    if (resource.getMemorySize() == 0L) {
      return Resources.none();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("CSConf - getAbsolueResourcePerQueue: prefix="
          + getNodeLabelPrefix(queue, label) + ", capacity=" + resource);
    }
    return resource;
  }

  private void updateResourceValuesFromConfig(Set<String> resourceTypes,
      Resource resource, String[] splits) {

    String resourceName = splits[0].trim();

    // If key is not a valid type, skip it.
    if (!resourceTypes.contains(resourceName)
        && !ResourceUtils.getResourceTypes().containsKey(resourceName)) {
      LOG.error(resourceName + " not supported.");
      return;
    }

    String units = getUnits(splits[1]);

    if (!UnitsConversionUtil.KNOWN_UNITS.contains(units)) {
      return;
    }

    Long resourceValue = Long
        .valueOf(splits[1].substring(0, splits[1].length() - units.length()));

    // Convert all incoming units to MB if units is configured.
    if (!units.isEmpty()) {
      resourceValue = UnitsConversionUtil.convert(units, "Mi", resourceValue);
    }

    // Custom resource type defined by user.
    // Such as GPU FPGA etc.
    if (!resourceTypes.contains(resourceName)) {
      resource.setResourceInformation(resourceName, ResourceInformation
          .newInstance(resourceName, units, resourceValue));
      return;
    }

    // map it based on key.
    AbsoluteResourceType resType = AbsoluteResourceType
        .valueOf(StringUtils.toUpperCase(resourceName));
    switch (resType) {
    case MEMORY :
      resource.setMemorySize(resourceValue);
      break;
    case VCORES :
      resource.setVirtualCores(resourceValue.intValue());
      break;
    default :
      resource.setResourceInformation(resourceName, ResourceInformation
          .newInstance(resourceName, units, resourceValue));
      break;
    }
  }

  @Private public static final String MULTI_NODE_SORTING_POLICIES =
      PREFIX + "multi-node-sorting.policy.names";

  @Private public static final String MULTI_NODE_SORTING_POLICY_NAME =
      PREFIX + "multi-node-sorting.policy";

  /**
   * resource usage based node sorting algorithm.
   */
  public static final String DEFAULT_NODE_SORTING_POLICY = "default";
  public static final String DEFAULT_NODE_SORTING_POLICY_CLASSNAME
      = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy";
  public static final long DEFAULT_MULTI_NODE_SORTING_INTERVAL = 1000L;

  @Private
  public static final String MULTI_NODE_PLACEMENT_ENABLED = PREFIX
      + "multi-node-placement-enabled";

  @Private
  public static final boolean DEFAULT_MULTI_NODE_PLACEMENT_ENABLED = false;

  public String getMultiNodesSortingAlgorithmPolicy(
      QueuePath queue) {

    String policyName = get(
        getQueuePrefix(queue) + "multi-node-sorting.policy");

    if (policyName == null) {
      policyName = get(MULTI_NODE_SORTING_POLICY_NAME);
    }

    // If node sorting policy is not configured in queue and in cluster level,
    // it is been assumed that this queue is not enabled with multi-node lookup.
    if (policyName == null || policyName.isEmpty()) {
      return null;
    }

    String policyClassName = get(MULTI_NODE_SORTING_POLICY_NAME + DOT
        + policyName.trim() + DOT + "class");

    if (policyClassName == null || policyClassName.isEmpty()) {
      throw new YarnRuntimeException(
          policyName.trim() + " Class is not configured or not an instance of "
              + MultiNodeLookupPolicy.class.getCanonicalName());
    }

    return normalizePolicyName(policyClassName.trim());
  }

  public boolean isLegacyQueueMode() {
    return getBoolean(LEGACY_QUEUE_MODE_ENABLED, DEFAULT_LEGACY_QUEUE_MODE);
  }

  public void setLegacyQueueModeEnabled(boolean value) {
    setBoolean(LEGACY_QUEUE_MODE_ENABLED, value);
  }

  public boolean getMultiNodePlacementEnabled() {
    return getBoolean(MULTI_NODE_PLACEMENT_ENABLED,
        DEFAULT_MULTI_NODE_PLACEMENT_ENABLED);
  }

  public Set<MultiNodePolicySpec> getMultiNodePlacementPolicies() {
    String[] policies = getTrimmedStrings(MULTI_NODE_SORTING_POLICIES);

    // In other cases, split the accessibleLabelStr by ","
    Set<MultiNodePolicySpec> set = new HashSet<MultiNodePolicySpec>();
    for (String str : policies) {
      if (!str.trim().isEmpty()) {
        String policyClassName = get(
            MULTI_NODE_SORTING_POLICY_NAME + DOT + str.trim() + DOT + "class");
        if (str.trim().equals(DEFAULT_NODE_SORTING_POLICY)) {
          policyClassName = get(
              MULTI_NODE_SORTING_POLICY_NAME + DOT + str.trim() + DOT + "class",
              DEFAULT_NODE_SORTING_POLICY_CLASSNAME);
        }

        // This check is needed as default class name is loaded only for
        // DEFAULT_NODE_SORTING_POLICY.
        if (policyClassName == null) {
          throw new YarnRuntimeException(
              str.trim() + " Class is not configured or not an instance of "
                  + MultiNodeLookupPolicy.class.getCanonicalName());
        }
        policyClassName = normalizePolicyName(policyClassName.trim());
        long policySortingInterval = getLong(
            MULTI_NODE_SORTING_POLICY_NAME + DOT + str.trim()
                + DOT + "sorting-interval.ms",
            DEFAULT_MULTI_NODE_SORTING_INTERVAL);
        if (policySortingInterval < 0) {
          throw new YarnRuntimeException(
              str.trim()
                  + " multi-node policy is configured with invalid"
                  + " sorting-interval:" + policySortingInterval);
        }
        set.add(
            new MultiNodePolicySpec(policyClassName, policySortingInterval));
      }
    }

    return Collections.unmodifiableSet(set);
  }

  private String normalizePolicyName(String policyName) {

    // Ensure that custom node sorting algorithm class is valid.
    try {
      Class<?> nodeSortingPolicyClazz = getClassByName(policyName);
      if (MultiNodeLookupPolicy.class
          .isAssignableFrom(nodeSortingPolicyClazz)) {
        return policyName;
      } else {
        throw new YarnRuntimeException(
            "Class: " + policyName + " not instance of "
                + MultiNodeLookupPolicy.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate " + "NodesSortingPolicy: " + policyName, e);
    }
  }
}
