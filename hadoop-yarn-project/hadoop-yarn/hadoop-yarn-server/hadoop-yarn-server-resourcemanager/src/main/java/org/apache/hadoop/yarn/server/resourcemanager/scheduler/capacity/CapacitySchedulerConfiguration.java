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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMappingEntity;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AppPriorityACLConfigurationParser.AppPriorityACLKeyType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.PriorityUtilizationQueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.StringTokenizer;

public class CapacitySchedulerConfiguration extends ReservationSchedulerConfiguration {

  private static final Log LOG = 
    LogFactory.getLog(CapacitySchedulerConfiguration.class);

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
  public static final boolean DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE = false;

  @Private
  public static final String QUEUE_MAPPING = PREFIX + "queue-mappings";

  @Private
  public static final String ENABLE_QUEUE_MAPPING_OVERRIDE = QUEUE_MAPPING + "-override.enable";

  @Private
  public static final boolean DEFAULT_ENABLE_QUEUE_MAPPING_OVERRIDE = false;

  @Private
  public static final String QUEUE_PREEMPTION_DISABLED = "disable_preemption";

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

  @Private
  public static final int DEFAULT_MAX_ASSIGN_PER_HEARTBEAT = -1;

  /** Configuring absolute min/max resources in a queue. **/
  @Private
  public static final String MINIMUM_RESOURCE = "min-resource";

  @Private
  public static final String MAXIMUM_RESOURCE = "max-resource";

  public static final String DEFAULT_RESOURCE_TYPES = "memory,vcores";

  public static final String PATTERN_FOR_ABSOLUTE_RESOURCE = "^\\[[\\w\\.,\\-_=\\ /]+\\]$";

  private static final Pattern RESOURCE_PATTERN = Pattern.compile(PATTERN_FOR_ABSOLUTE_RESOURCE);

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

  static String getQueuePrefix(String queue) {
    String queueName = PREFIX + queue + DOT;
    return queueName;
  }

  static String getQueueOrderingPolicyPrefix(String queue) {
    String queueName = PREFIX + queue + DOT + ORDERING_POLICY + DOT;
    return queueName;
  }
  
  private String getNodeLabelPrefix(String queue, String label) {
    if (label.equals(CommonNodeLabelsManager.NO_LABEL)) {
      return getQueuePrefix(queue);
    }
    return getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS + DOT + label + DOT;
  }
  
  public int getMaximumSystemApplications() {
    int maxApplications = 
      getInt(MAXIMUM_SYSTEM_APPLICATIONS, DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS);
    return maxApplications;
  }
  
  public float getMaximumApplicationMasterResourcePercent() {
    return getFloat(MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 
        DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT);
  }


  /**
   * Get the maximum applications per queue setting.
   * @param queue name of the queue
   * @return setting specified or -1 if not set
   */
  public int getMaximumApplicationsPerQueue(String queue) {
    int maxApplicationsPerQueue = 
      getInt(getQueuePrefix(queue) + MAXIMUM_APPLICATIONS_SUFFIX, 
          (int)UNDEFINED);
    return maxApplicationsPerQueue;
  }

  /**
   * Get the maximum am resource percent per queue setting.
   * @param queue name of the queue
   * @return per queue setting or defaults to the global am-resource-percent 
   *         setting if per queue setting not present
   */
  public float getMaximumApplicationMasterResourcePerQueuePercent(String queue) {
    return getFloat(getQueuePrefix(queue) + MAXIMUM_AM_RESOURCE_SUFFIX, 
    		getMaximumApplicationMasterResourcePercent());
  }
  
  public void setMaximumApplicationMasterResourcePerQueuePercent(String queue,
      float percent) {
    setFloat(getQueuePrefix(queue) + MAXIMUM_AM_RESOURCE_SUFFIX, percent);
  }
  
  public float getNonLabeledQueueCapacity(String queue) {
    String configuredCapacity = get(getQueuePrefix(queue) + CAPACITY);
    boolean matcher = (configuredCapacity != null)
        && RESOURCE_PATTERN.matcher(configuredCapacity).find();
    if (matcher) {
      // Return capacity in percentage as 0 for non-root queues and 100 for
      // root.From AbstractCSQueue, absolute resource will be parsed and
      // updated. Once nodes are added/removed in cluster, capacity in
      // percentage will also be re-calculated.
      return queue.equals("root") ? 100.0f : 0f;
    }

    float capacity = queue.equals("root")
        ? 100.0f
        : (configuredCapacity == null)
            ? 0f
            : Float.parseFloat(configuredCapacity);
    if (capacity < MINIMUM_CAPACITY_VALUE
        || capacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException(
          "Illegal " + "capacity of " + capacity + " for queue " + queue);
    }
    LOG.debug("CSConf - getCapacity: queuePrefix=" + getQueuePrefix(queue)
        + ", capacity=" + capacity);
    return capacity;
  }
  
  public void setCapacity(String queue, float capacity) {
    if (queue.equals("root")) {
      throw new IllegalArgumentException(
          "Cannot set capacity, root queue has a fixed capacity of 100.0f");
    }
    setFloat(getQueuePrefix(queue) + CAPACITY, capacity);
    LOG.debug("CSConf - setCapacity: queuePrefix=" + getQueuePrefix(queue) + 
        ", capacity=" + capacity);
  }

  public float getNonLabeledQueueMaximumCapacity(String queue) {
    String configuredCapacity = get(getQueuePrefix(queue) + MAXIMUM_CAPACITY);
    boolean matcher = (configuredCapacity != null)
        && RESOURCE_PATTERN.matcher(configuredCapacity).find();
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
  
  public void setMaximumCapacity(String queue, float maxCapacity) {
    if (maxCapacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException("Illegal " +
          "maximum-capacity of " + maxCapacity + " for queue " + queue);
    }
    setFloat(getQueuePrefix(queue) + MAXIMUM_CAPACITY, maxCapacity);
    LOG.debug("CSConf - setMaxCapacity: queuePrefix=" + getQueuePrefix(queue) + 
        ", maxCapacity=" + maxCapacity);
  }
  
  public void setCapacityByLabel(String queue, String label, float capacity) {
    setFloat(getNodeLabelPrefix(queue, label) + CAPACITY, capacity);
  }
  
  public void setMaximumCapacityByLabel(String queue, String label,
      float capacity) {
    setFloat(getNodeLabelPrefix(queue, label) + MAXIMUM_CAPACITY, capacity);
  }
  
  public int getUserLimit(String queue) {
    int userLimit = getInt(getQueuePrefix(queue) + USER_LIMIT,
        DEFAULT_USER_LIMIT);
    return userLimit;
  }

  // TODO (wangda): We need to better distinguish app ordering policy and queue
  // ordering policy's classname / configuration options, etc. And dedup code
  // if possible.
  @SuppressWarnings("unchecked")
  public <S extends SchedulableEntity> OrderingPolicy<S> getAppOrderingPolicy(
      String queue) {
  
    String policyType = get(getQueuePrefix(queue) + ORDERING_POLICY,
        DEFAULT_APP_ORDERING_POLICY);
    
    OrderingPolicy<S> orderingPolicy;
    
    if (policyType.trim().equals(FIFO_APP_ORDERING_POLICY)) {
       policyType = FifoOrderingPolicy.class.getName();
    }
    if (policyType.trim().equals(FAIR_APP_ORDERING_POLICY)) {
       policyType = FairOrderingPolicy.class.getName();
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

  public void setUserLimit(String queue, int userLimit) {
    setInt(getQueuePrefix(queue) + USER_LIMIT, userLimit);
    LOG.debug("here setUserLimit: queuePrefix=" + getQueuePrefix(queue) + 
        ", userLimit=" + getUserLimit(queue));
  }
  
  public float getUserLimitFactor(String queue) {
    float userLimitFactor = 
      getFloat(getQueuePrefix(queue) + USER_LIMIT_FACTOR, 
          DEFAULT_USER_LIMIT_FACTOR);
    return userLimitFactor;
  }

  public void setUserLimitFactor(String queue, float userLimitFactor) {
    setFloat(getQueuePrefix(queue) + USER_LIMIT_FACTOR, userLimitFactor); 
  }
  
  public QueueState getConfiguredState(String queue) {
    String state = get(getQueuePrefix(queue) + STATE);
    if (state == null) {
      return null;
    } else {
      return QueueState.valueOf(StringUtils.toUpperCase(state));
    }
  }

  public QueueState getState(String queue) {
    QueueState state = getConfiguredState(queue);
    return (state == null) ? QueueState.RUNNING : state;
  }

  @Private
  @VisibleForTesting
  public void setState(String queue, QueueState state) {
    set(getQueuePrefix(queue) + STATE, state.name());
  }

  public void setAccessibleNodeLabels(String queue, Set<String> labels) {
    if (labels == null) {
      return;
    }
    String str = StringUtils.join(",", labels);
    set(getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS, str);
  }
  
  public Set<String> getAccessibleNodeLabels(String queue) {
    String accessibleLabelStr =
        get(getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS);

    // When accessible-label is null, 
    if (accessibleLabelStr == null) {
      // Only return null when queue is not ROOT
      if (!queue.equals(ROOT)) {
        return null;
      }
    } else {
      // print a warning when accessibleNodeLabel specified in config and queue
      // is ROOT
      if (queue.equals(ROOT)) {
        LOG.warn("Accessible node labels for root queue will be ignored,"
            + " it will be automatically set to \"*\".");
      }
    }

    // always return ANY for queue root
    if (queue.equals(ROOT)) {
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
  
  private float internalGetLabeledQueueCapacity(String queue, String label, String suffix,
      float defaultValue) {
    String capacityPropertyName = getNodeLabelPrefix(queue, label) + suffix;
    boolean matcher = (capacityPropertyName != null)
        && RESOURCE_PATTERN.matcher(capacityPropertyName).find();
    if (matcher) {
      // Return capacity in percentage as 0 for non-root queues and 100 for
      // root.From AbstractCSQueue, absolute resource will be parsed and
      // updated. Once nodes are added/removed in cluster, capacity in
      // percentage will also be re-calculated.
      return defaultValue;
    }

    float capacity = getFloat(capacityPropertyName, defaultValue);
    if (capacity < MINIMUM_CAPACITY_VALUE
        || capacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException("Illegal capacity of " + capacity
          + " for node-label=" + label + " in queue=" + queue
          + ", valid capacity should in range of [0, 100].");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("CSConf - getCapacityOfLabel: prefix="
          + getNodeLabelPrefix(queue, label) + ", capacity=" + capacity);
    }
    return capacity;
  }
  
  public float getLabeledQueueCapacity(String queue, String label) {
    return internalGetLabeledQueueCapacity(queue, label, CAPACITY, 0f);
  }
  
  public float getLabeledQueueMaximumCapacity(String queue, String label) {
    return internalGetLabeledQueueCapacity(queue, label, MAXIMUM_CAPACITY, 100f);
  }
  
  public String getDefaultNodeLabelExpression(String queue) {
    String defaultLabelExpression = get(getQueuePrefix(queue)
        + DEFAULT_NODE_LABEL_EXPRESSION);
    if (defaultLabelExpression == null) {
      return null;
    }
    return defaultLabelExpression.trim();
  }
  
  public void setDefaultNodeLabelExpression(String queue, String exp) {
    set(getQueuePrefix(queue) + DEFAULT_NODE_LABEL_EXPRESSION, exp);
  }

  public float getMaximumAMResourcePercentPerPartition(String queue,
      String label) {
    // If per-partition max-am-resource-percent is not configured,
    // use default value as max-am-resource-percent for this queue.
    return getFloat(getNodeLabelPrefix(queue, label)
        + MAXIMUM_AM_RESOURCE_SUFFIX,
        getMaximumApplicationMasterResourcePerQueuePercent(queue));
  }

  public void setMaximumAMResourcePercentPerPartition(String queue,
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
  
  private static String getAclKey(QueueACL acl) {
    return "acl_" + StringUtils.toLowerCase(acl.toString());
  }

  public AccessControlList getAcl(String queue, QueueACL acl) {
    String queuePrefix = getQueuePrefix(queue);
    // The root queue defaults to all access if not defined
    // Sub queues inherit access if not defined
    String defaultAcl = queue.equals(ROOT) ? ALL_ACL : NONE_ACL;
    String aclString = get(queuePrefix + getAclKey(acl), defaultAcl);
    return new AccessControlList(aclString);
  }

  public void setAcl(String queue, QueueACL acl, String aclString) {
    String queuePrefix = getQueuePrefix(queue);
    set(queuePrefix + getAclKey(acl), aclString);
  }

  private static String getAclKey(ReservationACL acl) {
    return "acl_" + StringUtils.toLowerCase(acl.toString());
  }

  private static String getAclKey(AccessType acl) {
    return "acl_" + StringUtils.toLowerCase(acl.toString());
  }

  @Override
  public Map<ReservationACL, AccessControlList> getReservationAcls(String
        queue) {
    Map<ReservationACL, AccessControlList> resAcls = new HashMap<>();
    for (ReservationACL acl : ReservationACL.values()) {
      resAcls.put(acl, getReservationAcl(queue, acl));
    }
    return resAcls;
  }

  private AccessControlList getReservationAcl(String queue, ReservationACL
        acl) {
    String queuePrefix = getQueuePrefix(queue);
    // The root queue defaults to all access if not defined
    // Sub queues inherit access if not defined
    String defaultAcl = ALL_ACL;
    String aclString = get(queuePrefix + getAclKey(acl), defaultAcl);
    return new AccessControlList(aclString);
  }

  private void setAcl(String queue, ReservationACL acl, String aclString) {
    String queuePrefix = getQueuePrefix(queue);
    set(queuePrefix + getAclKey(acl), aclString);
  }

  private void setAcl(String queue, AccessType acl, String aclString) {
    String queuePrefix = getQueuePrefix(queue);
    set(queuePrefix + getAclKey(acl), aclString);
  }

  public Map<AccessType, AccessControlList> getAcls(String queue) {
    Map<AccessType, AccessControlList> acls =
      new HashMap<AccessType, AccessControlList>();
    for (QueueACL acl : QueueACL.values()) {
      acls.put(SchedulerUtils.toAccessType(acl), getAcl(queue, acl));
    }
    return acls;
  }

  public void setAcls(String queue, Map<QueueACL, AccessControlList> acls) {
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      setAcl(queue, e.getKey(), e.getValue().getAclString());
    }
  }

  @VisibleForTesting
  public void setReservationAcls(String queue,
        Map<ReservationACL, AccessControlList> acls) {
    for (Map.Entry<ReservationACL, AccessControlList> e : acls.entrySet()) {
      setAcl(queue, e.getKey(), e.getValue().getAclString());
    }
  }

  @VisibleForTesting
  public void setPriorityAcls(String queue, Priority priority,
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

  public List<AppPriorityACLGroup> getPriorityAcls(String queue,
      Priority clusterMaxPriority) {
    String queuePrefix = getQueuePrefix(queue);
    String defaultAcl = ALL_ACL;
    String aclString = get(
        queuePrefix + getAclKey(AccessType.APPLICATION_MAX_PRIORITY),
        defaultAcl);

    return priorityACLConfig.getPriorityAcl(clusterMaxPriority, aclString);
  }

  public String[] getQueues(String queue) {
    LOG.debug("CSConf - getQueues called for: queuePrefix=" + getQueuePrefix(queue));
    String[] queues = getStrings(getQueuePrefix(queue) + QUEUES);
    List<String> trimmedQueueNames = new ArrayList<String>();
    if (null != queues) {
      for (String s : queues) {
        trimmedQueueNames.add(s.trim());
      }
      queues = trimmedQueueNames.toArray(new String[0]);
    }
 
    LOG.debug("CSConf - getQueues: queuePrefix=" + getQueuePrefix(queue) + 
        ", queues=" + ((queues == null) ? "" : StringUtils.arrayToString(queues)));
    return queues;
  }
  
  public void setQueues(String queue, String[] subQueues) {
    set(getQueuePrefix(queue) + QUEUES, StringUtils.arrayToString(subQueues));
    LOG.debug("CSConf - setQueues: qPrefix=" + getQueuePrefix(queue) + 
        ", queues=" + StringUtils.arrayToString(subQueues));
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
  public Priority getQueuePriority(String queue) {
    String queuePolicyPrefix = getQueuePrefix(queue);
    Priority pri = Priority.newInstance(
        getInt(queuePolicyPrefix + "priority", 0));
    return pri;
  }

  @Private
  public void setQueuePriority(String queue, int priority) {
    String queuePolicyPrefix = getQueuePrefix(queue);
    setInt(queuePolicyPrefix + "priority", priority);
  }

  /**
   * Get the per queue setting for the maximum limit to allocate to
   * each container request.
   *
   * @param queue
   *          name of the queue
   * @return setting specified per queue else falls back to the cluster setting
   */
  public Resource getMaximumAllocationPerQueue(String queue) {
    // Only support to specify memory and vcores maximum allocation per queue
    // for now.
    String queuePrefix = getQueuePrefix(queue);
    long maxAllocationMbPerQueue = getInt(queuePrefix + MAXIMUM_ALLOCATION_MB,
        (int)UNDEFINED);
    int maxAllocationVcoresPerQueue = getInt(
        queuePrefix + MAXIMUM_ALLOCATION_VCORES, (int)UNDEFINED);
    if (LOG.isDebugEnabled()) {
      LOG.debug("max alloc mb per queue for " + queue + " is "
          + maxAllocationMbPerQueue);
      LOG.debug("max alloc vcores per queue for " + queue + " is "
          + maxAllocationVcoresPerQueue);
    }
    Resource clusterMax = ResourceUtils.fetchMaximumAllocationFromConfig(this);
    if (maxAllocationMbPerQueue == (int)UNDEFINED) {
      LOG.info("max alloc mb per queue for " + queue + " is undefined");
      maxAllocationMbPerQueue = clusterMax.getMemorySize();
    }
    if (maxAllocationVcoresPerQueue == (int)UNDEFINED) {
       LOG.info("max alloc vcore per queue for " + queue + " is undefined");
      maxAllocationVcoresPerQueue = clusterMax.getVirtualCores();
    }
    // Copy from clusterMax and overwrite per-queue's maximum memory/vcore
    // allocation.
    Resource result = Resources.clone(clusterMax);
    result.setMemorySize(maxAllocationMbPerQueue);
    result.setVirtualCores(maxAllocationVcoresPerQueue);
    if (maxAllocationMbPerQueue > clusterMax.getMemorySize()
        || maxAllocationVcoresPerQueue > clusterMax.getVirtualCores()) {
      throw new IllegalArgumentException(
          "Queue maximum allocation cannot be larger than the cluster setting"
          + " for queue " + queue
          + " max allocation per queue: " + result
          + " cluster setting: " + clusterMax);
    }
    return result;
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

  public List<QueueMappingEntity> getQueueMappingEntity(
      String queueMappingSuffix) {
    String queueMappingName = buildQueueMappingRuleProperty(queueMappingSuffix);

    List<QueueMappingEntity> mappings =
        new ArrayList<QueueMappingEntity>();
    Collection<String> mappingsString =
        getTrimmedStringCollection(queueMappingName);
    for (String mappingValue : mappingsString) {
      String[] mapping =
          getTrimmedStringCollection(mappingValue, ":")
              .toArray(new String[] {});
      if (mapping.length != 2 || mapping[1].length() == 0) {
        throw new IllegalArgumentException(
            "Illegal queue mapping " + mappingValue);
      }

      QueueMappingEntity m = new QueueMappingEntity(mapping[0], mapping[1]);

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
  public void setQueueMappingEntities(List<QueueMappingEntity> queueMappings,
      String queueMappingSuffix) {
    if (queueMappings == null) {
      return;
    }

    List<String> queueMappingStrs = new ArrayList<>();
    for (QueueMappingEntity mapping : queueMappings) {
      queueMappingStrs.add(mapping.toString());
    }

    String mappingRuleProp = buildQueueMappingRuleProperty(queueMappingSuffix);
    setStrings(mappingRuleProp, StringUtils.join(",", queueMappingStrs));
  }

  /**
   * Returns a collection of strings, trimming leading and trailing whitespeace
   * on each value
   *
   * @param str
   *          String to parse
   * @param delim
   *          delimiter to separate the values
   * @return Collection of parsed elements.
   */
  private static Collection<String> getTrimmedStringCollection(String str,
      String delim) {
    List<String> values = new ArrayList<String>();
    if (str == null)
      return values;
    StringTokenizer tokenizer = new StringTokenizer(str, delim);
    while (tokenizer.hasMoreTokens()) {
      String next = tokenizer.nextToken();
      if (next == null || next.trim().isEmpty()) {
        continue;
      }
      values.add(next.trim());
    }
    return values;
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
          getTrimmedStringCollection(mappingValue, ":")
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
        m = new QueueMapping(
                mappingType,
                mapping[1],
                mapping[2]);
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

  public boolean isReservable(String queue) {
    boolean isReservable =
        getBoolean(getQueuePrefix(queue) + IS_RESERVABLE, false);
    return isReservable;
  }

  public void setReservable(String queue, boolean isReservable) {
    setBoolean(getQueuePrefix(queue) + IS_RESERVABLE, isReservable);
    LOG.debug("here setReservableQueue: queuePrefix=" + getQueuePrefix(queue)
        + ", isReservableQueue=" + isReservable(queue));
  }

  @Override
  public long getReservationWindow(String queue) {
    long reservationWindow =
        getLong(getQueuePrefix(queue) + RESERVATION_WINDOW,
            DEFAULT_RESERVATION_WINDOW);
    return reservationWindow;
  }

  @Override
  public float getAverageCapacity(String queue) {
    float avgCapacity =
        getFloat(getQueuePrefix(queue) + AVERAGE_CAPACITY,
            MAXIMUM_CAPACITY_VALUE);
    return avgCapacity;
  }

  @Override
  public float getInstantaneousMaxCapacity(String queue) {
    float instMaxCapacity =
        getFloat(getQueuePrefix(queue) + INSTANTANEOUS_MAX_CAPACITY,
            MAXIMUM_CAPACITY_VALUE);
    return instMaxCapacity;
  }

  public void setInstantaneousMaxCapacity(String queue, float instMaxCapacity) {
    setFloat(getQueuePrefix(queue) + INSTANTANEOUS_MAX_CAPACITY,
        instMaxCapacity);
  }

  public void setReservationWindow(String queue, long reservationWindow) {
    setLong(getQueuePrefix(queue) + RESERVATION_WINDOW, reservationWindow);
  }

  public void setAverageCapacity(String queue, float avgCapacity) {
    setFloat(getQueuePrefix(queue) + AVERAGE_CAPACITY, avgCapacity);
  }

  @Override
  public String getReservationAdmissionPolicy(String queue) {
    String reservationPolicy =
        get(getQueuePrefix(queue) + RESERVATION_ADMISSION_POLICY,
            DEFAULT_RESERVATION_ADMISSION_POLICY);
    return reservationPolicy;
  }

  public void setReservationAdmissionPolicy(String queue,
      String reservationPolicy) {
    set(getQueuePrefix(queue) + RESERVATION_ADMISSION_POLICY, reservationPolicy);
  }

  @Override
  public String getReservationAgent(String queue) {
    String reservationAgent =
        get(getQueuePrefix(queue) + RESERVATION_AGENT_NAME,
            DEFAULT_RESERVATION_AGENT_NAME);
    return reservationAgent;
  }

  public void setReservationAgent(String queue, String reservationPolicy) {
    set(getQueuePrefix(queue) + RESERVATION_AGENT_NAME, reservationPolicy);
  }

  @Override
  public boolean getShowReservationAsQueues(String queuePath) {
    boolean showReservationAsQueues =
        getBoolean(getQueuePrefix(queuePath)
            + RESERVATION_SHOW_RESERVATION_AS_QUEUE,
            DEFAULT_SHOW_RESERVATIONS_AS_QUEUES);
    return showReservationAsQueues;
  }

  @Override
  public String getReplanner(String queue) {
    String replanner =
        get(getQueuePrefix(queue) + RESERVATION_PLANNER_NAME,
            DEFAULT_RESERVATION_PLANNER_NAME);
    return replanner;
  }

  @Override
  public boolean getMoveOnExpiry(String queue) {
    boolean killOnExpiry =
        getBoolean(getQueuePrefix(queue) + RESERVATION_MOVE_ON_EXPIRY,
            DEFAULT_RESERVATION_MOVE_ON_EXPIRY);
    return killOnExpiry;
  }

  @Override
  public long getEnforcementWindow(String queue) {
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
  public void setPreemptionDisabled(String queue, boolean preemptionDisabled) {
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
  public boolean getPreemptionDisabled(String queue, boolean defaultVal) {
    boolean preemptionDisabled =
        getBoolean(getQueuePrefix(queue) + QUEUE_PREEMPTION_DISABLED,
                   defaultVal);
    return preemptionDisabled;
  }

  /**
   * Indicates whether intra-queue preemption is disabled on the specified queue
   *
   * @param queue queue path to query
   * @param defaultVal used as default if the property is not set in the
   * configuration
   * @return true if preemption is disabled on queue, false otherwise
   */
  public boolean getIntraQueuePreemptionDisabled(String queue,
      boolean defaultVal) {
    return
        getBoolean(getQueuePrefix(queue) + INTRA_QUEUE_PREEMPTION_CONFIG_PREFIX
            + QUEUE_PREEMPTION_DISABLED, defaultVal);
  }

  /**
   * Get configured node labels in a given queuePath
   */
  public Set<String> getConfiguredNodeLabels(String queuePath) {
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

  public Integer getDefaultApplicationPriorityConfPerQueue(String queue) {
    Integer defaultPriority = getInt(getQueuePrefix(queue)
        + DEFAULT_APPLICATION_PRIORITY,
        DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    return defaultPriority;
  }

  @VisibleForTesting
  public void setOrderingPolicy(String queue, String policy) {
    set(getQueuePrefix(queue) + ORDERING_POLICY, policy);
  }

  @VisibleForTesting
  public void setOrderingPolicyParameter(String queue,
      String parameterKey, String parameterValue) {
    set(getQueuePrefix(queue) + ORDERING_POLICY + "." + parameterKey,
        parameterValue);
  }

  public boolean getLazyPreemptionEnabled() {
    return getBoolean(LAZY_PREEMPTION_ENABLED, DEFAULT_LAZY_PREEMPTION_ENABLED);
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
  public void setQueueOrderingPolicy(String queue, String policy) {
    set(getQueuePrefix(queue) + ORDERING_POLICY, policy);
  }

  @Private
  public QueueOrderingPolicy getQueueOrderingPolicy(String queue,
      String parentPolicy) {
    String defaultPolicy = parentPolicy;
    if (null == defaultPolicy) {
      defaultPolicy = DEFAULT_QUEUE_ORDERING_POLICY;
    }

    String policyType = get(getQueuePrefix(queue) + ORDERING_POLICY,
        defaultPolicy);

    QueueOrderingPolicy qop;
    if (policyType.trim().equals(QUEUE_UTILIZATION_ORDERING_POLICY)) {
      // Doesn't respect priority
      qop = new PriorityUtilizationQueueOrderingPolicy(false);
    } else if (policyType.trim().equals(
        QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY)) {
      qop = new PriorityUtilizationQueueOrderingPolicy(true);
    } else {
      String message =
          "Unable to construct queue ordering policy=" + policyType + " queue="
              + queue;
      throw new YarnRuntimeException(message);
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
   * @return map of user weights, if they exists. Otherwise, return empty map.
   */
  public Map<String, Float> getAllUserWeightsForQueue(String queuePath) {
    Map <String, Float> userWeights = new HashMap <String, Float>();
    String qPathPlusPrefix =
        getQueuePrefix(queuePath).replaceAll("\\.", "\\\\.")
        + USER_SETTINGS + "\\.";
    String weightKeyRegex =
        qPathPlusPrefix + "\\w+\\." + USER_WEIGHT;
    Map<String, String> props = getValByRegex(weightKeyRegex);
    for (Entry<String, String> e : props.entrySet()) {
      String userName =
          e.getKey().replaceFirst(qPathPlusPrefix, "")
          .replaceFirst("\\." + USER_WEIGHT, "");
      if (userName != null && !userName.isEmpty()) {
        userWeights.put(userName, new Float(e.getValue()));
      }
    }
    return userWeights;
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

  public long getMaximumLifetimePerQueue(String queue) {
    long maximumLifetimePerQueue = getLong(
        getQueuePrefix(queue) + MAXIMUM_LIFETIME_SUFFIX, (long) UNDEFINED);
    return maximumLifetimePerQueue;
  }

  public void setMaximumLifetimePerQueue(String queue, long maximumLifetime) {
    setLong(getQueuePrefix(queue) + MAXIMUM_LIFETIME_SUFFIX, maximumLifetime);
  }

  public long getDefaultLifetimePerQueue(String queue) {
    long maximumLifetimePerQueue = getLong(
        getQueuePrefix(queue) + DEFAULT_LIFETIME_SUFFIX, (long) UNDEFINED);
    return maximumLifetimePerQueue;
  }

  public void setDefaultLifetimePerQueue(String queue, long defaultLifetime) {
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
  public boolean isAutoCreateChildQueueEnabled(String queuePath) {
    boolean isAutoCreateEnabled = getBoolean(
        getQueuePrefix(queuePath) + AUTO_CREATE_CHILD_QUEUE_ENABLED,
        DEFAULT_AUTO_CREATE_CHILD_QUEUE_ENABLED);
    return isAutoCreateEnabled;
  }

  @Private
  @VisibleForTesting
  public void setAutoCreateChildQueueEnabled(String queuePath,
      boolean autoCreationEnabled) {
    setBoolean(getQueuePrefix(queuePath) +
            AUTO_CREATE_CHILD_QUEUE_ENABLED,
        autoCreationEnabled);
  }

  /**
   * Get the auto created leaf queue's template configuration prefix
   * Leaf queue's template capacities are configured at the parent queue
   *
   * @param queuePath parent queue's path
   * @return Config prefix for leaf queue template configurations
   */
  @Private
  public String getAutoCreatedQueueTemplateConfPrefix(String queuePath) {
    return queuePath + DOT + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
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
      String queuePath) {
    boolean shouldFailAutoQueueCreationOnExceedingGuaranteedCapacity =
        getBoolean(getQueuePrefix(queuePath)
                + FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY,
            DEFAULT_FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY);
    return shouldFailAutoQueueCreationOnExceedingGuaranteedCapacity;
  }

  @VisibleForTesting
  @Private
  public void setShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(
      String queuePath, boolean autoCreationEnabled) {
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
  public int getAutoCreatedQueuesMaxChildQueuesLimit(String queuePath) {
    return getInt(getQueuePrefix(queuePath) +
            AUTO_CREATE_QUEUE_MAX_QUEUES,
        DEFAULT_AUTO_CREATE_QUEUE_MAX_QUEUES);
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

  /**
   * Queue Management computation policy for Auto Created queues
   * @param queue The queue's path
   * @return Configured policy class name
   */
  @Private
  public String getAutoCreatedQueueManagementPolicy(String queue) {
    String autoCreatedQueueManagementPolicy =
        get(getQueuePrefix(queue) + AUTO_CREATED_QUEUE_MANAGEMENT_POLICY,
            DEFAULT_AUTO_CREATED_QUEUE_MANAGEMENT_POLICY);
    return autoCreatedQueueManagementPolicy;
  }

  /**
   * Get The policy class configured to manage capacities for auto created leaf
   * queues under the specified parent
   *
   * @param queueName The parent queue's name
   * @return The policy class configured to manage capacities for auto created
   * leaf queues under the specified parent queue
   */
  @Private
  protected AutoCreatedQueueManagementPolicy
  getAutoCreatedQueueManagementPolicyClass(
      String queueName) {

    String queueManagementPolicyClassName =
        getAutoCreatedQueueManagementPolicy(queueName);
    LOG.info("Using Auto Created Queue Management Policy: "
        + queueManagementPolicyClassName + " for queue: " + queueName);
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
              + queueManagementPolicyClassName + " for queue: " + queueName,
          e);
    }
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueConfigCapacity(String queuePath,
      float val) {
    String leafQueueConfPrefix = getAutoCreatedQueueTemplateConfPrefix(
        queuePath);
    setCapacity(leafQueueConfPrefix, val);
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueTemplateCapacityByLabel(String queuePath,
      String label, float val) {
    String leafQueueConfPrefix = getAutoCreatedQueueTemplateConfPrefix(
        queuePath);
    setCapacityByLabel(leafQueueConfPrefix, label, val);
  }

  @Private
  @VisibleForTesting
  public void setAutoCreatedLeafQueueConfigMaxCapacity(String queuePath,
      float val) {
    String leafQueueConfPrefix = getAutoCreatedQueueTemplateConfPrefix(
        queuePath);
    setMaximumCapacity(leafQueueConfPrefix, val);
  }

  @Private
  @VisibleForTesting
  public void setAutoCreatedLeafQueueTemplateMaxCapacity(String queuePath,
      String label, float val) {
    String leafQueueConfPrefix = getAutoCreatedQueueTemplateConfPrefix(
        queuePath);
    setMaximumCapacityByLabel(leafQueueConfPrefix, label, val);
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueConfigUserLimit(String queuePath,
      int val) {
    String leafQueueConfPrefix = getAutoCreatedQueueTemplateConfPrefix(
        queuePath);
    setUserLimit(leafQueueConfPrefix, val);
  }

  @VisibleForTesting
  @Private
  public void setAutoCreatedLeafQueueConfigUserLimitFactor(String queuePath,
      float val) {
    String leafQueueConfPrefix = getAutoCreatedQueueTemplateConfPrefix(
        queuePath);
    setUserLimitFactor(leafQueueConfPrefix, val);
  }

  @Private
  @VisibleForTesting
  public void setAutoCreatedLeafQueueConfigDefaultNodeLabelExpression(String
      queuePath,
      String expression) {
    String leafQueueConfPrefix = getAutoCreatedQueueTemplateConfPrefix(
        queuePath);
    setDefaultNodeLabelExpression(leafQueueConfPrefix, expression);
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
  public Resource getMinimumResourceRequirement(String label, String queue,
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
  public Resource getMaximumResourceRequirement(String label, String queue,
      Set<String> resourceTypes) {
    return internalGetLabeledResourceRequirementForQueue(queue, label,
        resourceTypes, MAXIMUM_CAPACITY);
  }

  @VisibleForTesting
  public void setMinimumResourceRequirement(String label, String queue,
      Resource resource) {
    updateMinMaxResourceToConf(label, queue, resource, CAPACITY);
  }

  @VisibleForTesting
  public void setMaximumResourceRequirement(String label, String queue,
      Resource resource) {
    updateMinMaxResourceToConf(label, queue, resource, MAXIMUM_CAPACITY);
  }

  private void updateMinMaxResourceToConf(String label, String queue,
      Resource resource, String type) {
    if (queue.equals("root")) {
      throw new IllegalArgumentException(
          "Cannot set resource, root queue will take 100% of cluster capacity");
    }

    StringBuilder resourceString = new StringBuilder();
    resourceString
        .append("[" + AbsoluteResourceType.MEMORY.toString().toLowerCase() + "="
            + resource.getMemorySize() + ","
            + AbsoluteResourceType.VCORES.toString().toLowerCase() + "="
            + resource.getVirtualCores() + "]");

    String prefix = getQueuePrefix(queue) + type;
    if (!label.isEmpty()) {
      prefix = getQueuePrefix(queue) + ACCESSIBLE_NODE_LABELS + DOT + label
          + DOT + type;
    }
    set(prefix, resourceString.toString());
  }

  private Resource internalGetLabeledResourceRequirementForQueue(String queue,
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

    // If key is not a valid type, skip it.
    if (!resourceTypes.contains(splits[0])) {
      return;
    }

    String units = getUnits(splits[1]);
    Long resourceValue = Long
        .valueOf(splits[1].substring(0, splits[1].length() - units.length()));

    // Convert all incoming units to MB if units is configured.
    if (!units.isEmpty()) {
      resourceValue = UnitsConversionUtil.convert(units, "Mi", resourceValue);
    }

    // map it based on key.
    AbsoluteResourceType resType = AbsoluteResourceType
        .valueOf(StringUtils.toUpperCase(splits[0].trim()));
    switch (resType) {
    case MEMORY :
      resource.setMemorySize(resourceValue);
      break;
    case VCORES :
      resource.setVirtualCores(resourceValue.intValue());
      break;
    default :
      resource.setResourceInformation(splits[0].trim(), ResourceInformation
          .newInstance(splits[0].trim(), units, resourceValue));
      break;
    }
  }
}
