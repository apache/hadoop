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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

public class CapacitySchedulerConfiguration extends Configuration {

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
  public static final int DEFAULT_NODE_LOCALITY_DELAY = -1;

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_PREFIX =
      PREFIX + "schedule-asynchronously";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_ENABLE =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".enable";

  @Private
  public static final boolean DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE = false;
  
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

  private String getQueuePrefix(String queue) {
    String queueName = PREFIX + queue + DOT;
    return queueName;
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
  
  public float getCapacity(String queue) {
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

  public float getMaximumCapacity(String queue) {
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
  
  public int getUserLimit(String queue) {
    int userLimit = getInt(getQueuePrefix(queue) + USER_LIMIT,
        DEFAULT_USER_LIMIT);
    return userLimit;
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
        QueueState.valueOf(state.toUpperCase()) : QueueState.RUNNING;
  }
  
  private static String getAclKey(QueueACL acl) {
    return "acl_" + acl.toString().toLowerCase();
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

  public Map<QueueACL, AccessControlList> getAcls(String queue) {
    Map<QueueACL, AccessControlList> acls =
      new HashMap<QueueACL, AccessControlList>();
    for (QueueACL acl : QueueACL.values()) {
      acls.put(acl, getAcl(queue, acl));
    }
    return acls;
  }

  public void setAcls(String queue, Map<QueueACL, AccessControlList> acls) {
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      setAcl(queue, e.getKey(), e.getValue().getAclString());
    }
  }

  public String[] getQueues(String queue) {
    LOG.debug("CSConf - getQueues called for: queuePrefix=" + getQueuePrefix(queue));
    String[] queues = getStrings(getQueuePrefix(queue) + QUEUES);
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

  public boolean getEnableUserMetrics() {
    return getBoolean(ENABLE_USER_METRICS, DEFAULT_ENABLE_USER_METRICS);
  }

  public int getNodeLocalityDelay() {
    int delay = getInt(NODE_LOCALITY_DELAY, DEFAULT_NODE_LOCALITY_DELAY);
    return (delay == DEFAULT_NODE_LOCALITY_DELAY) ? 0 : delay;
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

}
