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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerLeafQueueInfo extends CapacitySchedulerQueueInfo {

  protected int numActiveApplications;
  protected int numPendingApplications;
  protected int numContainers;
  protected int maxApplications;
  protected int maxApplicationsPerUser;
  protected int userLimit;
  protected UsersInfo users; // To add another level in the XML
  protected float userLimitFactor;
  protected ResourceInfo AMResourceLimit;
  protected ResourceInfo usedAMResource;
  protected ResourceInfo userAMResourceLimit;
  protected boolean preemptionDisabled;
  protected boolean intraQueuePreemptionDisabled;
  protected String defaultNodeLabelExpression;
  protected int defaultPriority;
  protected boolean isAutoCreatedLeafQueue;

  @XmlTransient
  protected String orderingPolicyInfo;

  CapacitySchedulerLeafQueueInfo() {
  };

  CapacitySchedulerLeafQueueInfo(LeafQueue q) {
    super(q);
    numActiveApplications = q.getNumActiveApplications();
    numPendingApplications = q.getNumPendingApplications();
    numContainers = q.getNumContainers();
    maxApplications = q.getMaxApplications();
    maxApplicationsPerUser = q.getMaxApplicationsPerUser();
    userLimit = q.getUserLimit();
    users = new UsersInfo(q.getUsersManager().getUsersInfo());
    userLimitFactor = q.getUserLimitFactor();
    AMResourceLimit = new ResourceInfo(q.getAMResourceLimit());
    usedAMResource = new ResourceInfo(q.getQueueResourceUsage().getAMUsed());
    preemptionDisabled = q.getPreemptionDisabled();
    intraQueuePreemptionDisabled = q.getIntraQueuePreemptionDisabled();
    orderingPolicyInfo = q.getOrderingPolicy().getInfo();
    defaultNodeLabelExpression = q.getDefaultNodeLabelExpression();
    defaultPriority = q.getDefaultApplicationPriority().getPriority();
    ArrayList<UserInfo> usersList = users.getUsersList();
    if (usersList.isEmpty()) {
      // If no users are present, consider AM Limit for that queue.
      userAMResourceLimit = resources.getPartitionResourceUsageInfo(
          RMNodeLabelsManager.NO_LABEL).getAMLimit();
    } else {
      userAMResourceLimit = usersList.get(0).getResourceUsageInfo()
          .getPartitionResourceUsageInfo(RMNodeLabelsManager.NO_LABEL)
          .getAMLimit();
    }

    if ( q instanceof AutoCreatedLeafQueue) {
      isAutoCreatedLeafQueue = true;
    }
  }

  @Override
  protected void populateQueueResourceUsage(ResourceUsage queueResourceUsage) {
    resources = new ResourcesInfo(queueResourceUsage);
  }

  @Override
  protected void populateQueueCapacities(QueueCapacities qCapacities,
      QueueResourceQuotas qResQuotas) {
    capacities = new QueueCapacitiesInfo(qCapacities, qResQuotas);
  }

  public int getNumActiveApplications() {
    return numActiveApplications;
  }

  public int getNumPendingApplications() {
    return numPendingApplications;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public int getMaxApplications() {
    return maxApplications;
  }

  public int getMaxApplicationsPerUser() {
    return maxApplicationsPerUser;
  }

  public int getUserLimit() {
    return userLimit;
  }

  //Placing here because of JERSEY-1199
  public UsersInfo getUsers() {
    return users;
  }

  public float getUserLimitFactor() {
    return userLimitFactor;
  }
  
  public ResourceInfo getAMResourceLimit() {
    return AMResourceLimit;
  }
  
  public ResourceInfo getUsedAMResource() {
    return usedAMResource;
  }

  public ResourceInfo getUserAMResourceLimit() {
    return userAMResourceLimit; 
  }

  public boolean getPreemptionDisabled() {
    return preemptionDisabled;
  }

  public boolean getIntraQueuePreemptionDisabled() {
    return intraQueuePreemptionDisabled;
  }
  
  public String getOrderingPolicyInfo() {
    return orderingPolicyInfo;
  }

  public String getDefaultNodeLabelExpression() {
    return defaultNodeLabelExpression;
  }

  public int getDefaultApplicationPriority() {
    return defaultPriority;
  }

  public boolean isAutoCreatedLeafQueue() {
    return isAutoCreatedLeafQueue;
  }
}
