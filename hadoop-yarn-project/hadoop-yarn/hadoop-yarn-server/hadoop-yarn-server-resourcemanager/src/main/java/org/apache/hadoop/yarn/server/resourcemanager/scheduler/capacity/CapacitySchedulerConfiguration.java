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
import java.util.StringTokenizer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;

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
  
  public static final String ORDERING_POLICY = "ordering-policy";
  
  public static final String FIFO_ORDERING_POLICY = "fifo";

  public static final String FAIR_ORDERING_POLICY = "fair";

  public static final String DEFAULT_ORDERING_POLICY = FIFO_ORDERING_POLICY;
  
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
  public static final String RACK_LOCALITY_FULL_RESET =
      PREFIX + "rack-locality-full-reset";

  @Private
  public static final boolean DEFAULT_RACK_LOCALITY_FULL_RESET = true;

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_PREFIX =
      PREFIX + "schedule-asynchronously";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_ENABLE =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".enable";

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
  public static final String LAZY_PREEMPTION_ENALBED = PREFIX + "lazy-preemption-enabled";

  @Private
  public static final boolean DEFAULT_LAZY_PREEMPTION_ENABLED = false;

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
    float capacity = queue.equals("root") ? 100.0f : getFloat(
        getQueuePrefix(queue) + CAPACITY, UNDEFINED);
    if (capacity < MINIMUM_CAPACITY_VALUE || capacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException("Illegal " +
      		"capacity of " + capacity + " for queue " + queue);
    }
    LOG.debug("CSConf - getCapacity: queuePrefix=" + getQueuePrefix(queue) + 
        ", capacity=" + capacity);
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
    float maxCapacity = getFloat(getQueuePrefix(queue) + MAXIMUM_CAPACITY,
        MAXIMUM_CAPACITY_VALUE);
    maxCapacity = (maxCapacity == DEFAULT_MAXIMUM_CAPACITY_VALUE) ? 
        MAXIMUM_CAPACITY_VALUE : maxCapacity;
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
  
  @SuppressWarnings("unchecked")
  public <S extends SchedulableEntity> OrderingPolicy<S> getOrderingPolicy(
      String queue) {
  
    String policyType = get(getQueuePrefix(queue) + ORDERING_POLICY, 
      DEFAULT_ORDERING_POLICY);
    
    OrderingPolicy<S> orderingPolicy;
    
    if (policyType.trim().equals(FIFO_ORDERING_POLICY)) {
       policyType = FifoOrderingPolicy.class.getName();
    }
    if (policyType.trim().equals(FAIR_ORDERING_POLICY)) {
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
  
  public QueueState getState(String queue) {
    String state = get(getQueuePrefix(queue) + STATE);
    return (state != null) ? 
        QueueState.valueOf(StringUtils.toUpperCase(state)) : QueueState.RUNNING;
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

  public Resource getMaximumAllocation() {
    int maximumMemory = getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    int maximumCores = getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    return Resources.createResource(maximumMemory, maximumCores);
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
    Resource clusterMax = getMaximumAllocation();
    if (maxAllocationMbPerQueue == (int)UNDEFINED) {
      LOG.info("max alloc mb per queue for " + queue + " is undefined");
      maxAllocationMbPerQueue = clusterMax.getMemorySize();
    }
    if (maxAllocationVcoresPerQueue == (int)UNDEFINED) {
       LOG.info("max alloc vcore per queue for " + queue + " is undefined");
      maxAllocationVcoresPerQueue = clusterMax.getVirtualCores();
    }
    Resource result = Resources.createResource(maxAllocationMbPerQueue,
        maxAllocationVcoresPerQueue);
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

  public int getNodeLocalityDelay() {
    return getInt(NODE_LOCALITY_DELAY, DEFAULT_NODE_LOCALITY_DELAY);
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
    return getBoolean(LAZY_PREEMPTION_ENALBED, DEFAULT_LAZY_PREEMPTION_ENABLED);
  }

  private static final String PREEMPTION_CONFIG_PREFIX =
      "yarn.resourcemanager.monitor.capacity.preemption.";

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
  public static final float DEFAULT_PREEMPTION_MAX_IGNORED_OVER_CAPACITY = 0.1f;
  /**
   * Given a computed preemption target, account for containers naturally
   * expiring and preempt only this percentage of the delta. This determines
   * the rate of geometric convergence into the deadzone ({@link
   * #PREEMPTION_MAX_IGNORED_OVER_CAPACITY}). For example, a termination factor of 0.5
   * will reclaim almost 95% of resources within 5 * {@link
   * #PREEMPTION_WAIT_TIME_BEFORE_KILL}, even absent natural termination. */
  public static final String PREEMPTION_NATURAL_TERMINATION_FACTOR =
      PREEMPTION_CONFIG_PREFIX + "natural_termination_factor";
  public static final float DEFAULT_PREEMPTION_NATURAL_TERMINATION_FACTOR =
      0.2f;

  /**
   * When calculating which containers to be preempted, we will try to preempt
   * containers for reserved containers first. By default is false.
   */
  public static final String PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS =
      PREEMPTION_CONFIG_PREFIX + "select_based_on_reserved_containers";
  public static final boolean DEFAULT_PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS =
      false;
}
