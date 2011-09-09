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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;

public class CapacitySchedulerConfiguration extends Configuration {

  private static final Log LOG = 
    LogFactory.getLog(CapacitySchedulerConfiguration.class);
  
  private static final String CS_CONFIGURATION_FILE = "capacity-scheduler.xml";
  
  @Private
  public static final String PREFIX = "yarn.scheduler.capacity.";
  
  @Private
  public static final String DOT = ".";
  
  @Private
  public static final String MAXIMUM_SYSTEM_APPLICATIONS =
    PREFIX + "maximum-applications";
  
  @Private
  public static final String MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT =
    PREFIX + "maximum-am-resource-percent";
  
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

  private static final int MINIMUM_MEMORY = 1024;

  @Private
  public static final String MINIMUM_ALLOCATION = 
    PREFIX + "minimum-allocation-mb";

  private static final int MAXIMUM_MEMORY = 10240;

  @Private
  public static final String MAXIMUM_ALLOCATION = 
    PREFIX + "maximum-allocation-mb";

  @Private
  public static final int DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS = 10000;
  
  @Private
  public static final float 
  DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT = 0.1f;
  
  @Private
  public static final int UNDEFINED = -1;
  
  @Private
  public static final int MINIMUM_CAPACITY_VALUE = 1;
  
  @Private
  public static final int MAXIMUM_CAPACITY_VALUE = 100;
  
  @Private
  public static final int DEFAULT_USER_LIMIT = 100;
  
  @Private
  public static final float DEFAULT_USER_LIMIT_FACTOR = 1.0f;
  
  @Private
  public static final String DEFAULT_ACL = "*";

  @Private public static final String ENABLE_USER_METRICS =
      PREFIX +"user-metrics.enable";
  @Private public static final boolean DEFAULT_ENABLE_USER_METRICS = false;

  public CapacitySchedulerConfiguration() {
    this(new Configuration());
  }
  
  public CapacitySchedulerConfiguration(Configuration configuration) {
    super(configuration);
    addResource(CS_CONFIGURATION_FILE);
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
  
  public int getCapacity(String queue) {
    int capacity = getInt(getQueuePrefix(queue) + CAPACITY, UNDEFINED);
    if (capacity < MINIMUM_CAPACITY_VALUE || capacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException("Illegal " +
      		"capacity of " + capacity + " for queue " + queue);
    }
    LOG.info("CSConf - setCapacity: queuePrefix=" + getQueuePrefix(queue) + 
        ", capacity=" + capacity);
    return capacity;
  }
  
  public void setCapacity(String queue, int capacity) {
    setInt(getQueuePrefix(queue) + CAPACITY, capacity);
    LOG.info("CSConf - setCapacity: queuePrefix=" + getQueuePrefix(queue) + 
        ", capacity=" + capacity);
  }

  public int getMaximumCapacity(String queue) {
    int maxCapacity = 
      getInt(getQueuePrefix(queue) + MAXIMUM_CAPACITY, UNDEFINED);
    return maxCapacity;
  }
  
  public int getUserLimit(String queue) {
    int userLimit = 
      getInt(getQueuePrefix(queue) + USER_LIMIT, DEFAULT_USER_LIMIT);
    return userLimit;
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
  
  public Map<QueueACL, AccessControlList> getAcls(String queue) {
    Map<QueueACL, AccessControlList> acls = 
      new HashMap<QueueACL, AccessControlList>();
    String queuePrefix = getQueuePrefix(queue);
    for (QueueACL acl : QueueACL.values()) {
      acls.put(acl, 
          new AccessControlList(get(queuePrefix + getAclKey(acl), 
              DEFAULT_ACL)));
    }
    return acls;
  }

  public String[] getQueues(String queue) {
    LOG.info("CSConf - getQueues called for: queuePrefix=" + getQueuePrefix(queue));
    String[] queues = getStrings(getQueuePrefix(queue) + QUEUES);
    LOG.info("CSConf - getQueues: queuePrefix=" + getQueuePrefix(queue) + 
        ", queues=" + ((queues == null) ? "" : StringUtils.arrayToString(queues)));
    return queues;
  }
  
  public void setQueues(String queue, String[] subQueues) {
    set(getQueuePrefix(queue) + QUEUES, StringUtils.arrayToString(subQueues));
    LOG.info("CSConf - setQueues: qPrefix=" + getQueuePrefix(queue) + 
        ", queues=" + StringUtils.arrayToString(subQueues));
  }
  
  public Resource getMinimumAllocation() {
    int minimumMemory = getInt(MINIMUM_ALLOCATION, MINIMUM_MEMORY);
    return Resources.createResource(minimumMemory);
  }

  public Resource getMaximumAllocation() {
    int maximumMemory = getInt(MAXIMUM_ALLOCATION, MAXIMUM_MEMORY);
    return Resources.createResource(maximumMemory);
  }

  public boolean getEnableUserMetrics() {
    return getBoolean(ENABLE_USER_METRICS, DEFAULT_ENABLE_USER_METRICS);
  }
}
