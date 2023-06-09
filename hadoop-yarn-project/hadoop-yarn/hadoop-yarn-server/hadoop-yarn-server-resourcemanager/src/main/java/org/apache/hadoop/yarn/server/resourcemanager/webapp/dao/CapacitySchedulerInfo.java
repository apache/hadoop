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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.helper.CapacitySchedulerInfoHelper;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo.getSortedQueueAclInfoList;

@XmlRootElement(name = "capacityScheduler")
@XmlType(name = "capacityScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerInfo extends SchedulerInfo {

  protected float capacity;
  protected float usedCapacity;
  protected float maxCapacity;
  protected float weight;
  protected float normalizedWeight;
  protected String queueName;
  private String queuePath;
  protected int maxParallelApps;
  private boolean isAbsoluteResource;
  protected CapacitySchedulerQueueInfoList queues;
  protected QueueCapacitiesInfo capacities;
  protected CapacitySchedulerHealthInfo health;
  protected ResourceInfo maximumAllocation;
  protected QueueAclsInfo queueAcls;
  protected int queuePriority;
  protected String orderingPolicyInfo;
  protected String mode;
  protected String queueType;
  protected String creationMethod;
  protected String autoCreationEligibility;
  protected String defaultNodeLabelExpression;
  protected AutoQueueTemplatePropertiesInfo autoQueueTemplateProperties;
  protected AutoQueueTemplatePropertiesInfo autoQueueParentTemplateProperties;
  protected AutoQueueTemplatePropertiesInfo autoQueueLeafTemplateProperties;

  @XmlTransient
  static final float EPSILON = 1e-8f;

  public CapacitySchedulerInfo() {
  } // JAXB needs this

  public CapacitySchedulerInfo(CSQueue parent, CapacityScheduler cs) {
    this.queueName = parent.getQueueName();
    this.queuePath = parent.getQueuePath();
    this.usedCapacity = parent.getUsedCapacity() * 100;
    this.capacity = parent.getCapacity() * 100;
    float max = parent.getMaximumCapacity();
    if (max < EPSILON || max > 1f)
      max = 1f;
    this.maxCapacity = max * 100;
    this.weight = parent.getQueueCapacities().getWeight();
    this.normalizedWeight = parent.getQueueCapacities().getNormalizedWeight();
    this.maxParallelApps = parent.getMaxParallelApps();

    capacities = new QueueCapacitiesInfo(parent.getQueueCapacities(),
        parent.getQueueResourceQuotas(), false);
    queues = getQueues(cs, parent);
    health = new CapacitySchedulerHealthInfo(cs);
    maximumAllocation = new ResourceInfo(parent.getMaximumAllocation());

    isAbsoluteResource = parent.getCapacityConfigType() ==
        AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE;

    CapacitySchedulerConfiguration conf = cs.getConfiguration();
    queueAcls = new QueueAclsInfo();
    queueAcls.addAll(getSortedQueueAclInfoList(parent, queueName, conf));

    queuePriority = parent.getPriority().getPriority();
    if (parent instanceof AbstractParentQueue) {
      AbstractParentQueue queue = (AbstractParentQueue) parent;
      orderingPolicyInfo = queue.getQueueOrderingPolicy()
          .getConfigName();
      autoQueueTemplateProperties = CapacitySchedulerInfoHelper
          .getAutoCreatedTemplate(queue.getAutoCreatedQueueTemplate()
              .getTemplateProperties());
      autoQueueParentTemplateProperties = CapacitySchedulerInfoHelper
          .getAutoCreatedTemplate(queue.getAutoCreatedQueueTemplate()
              .getParentOnlyProperties());
      autoQueueLeafTemplateProperties = CapacitySchedulerInfoHelper
          .getAutoCreatedTemplate(queue.getAutoCreatedQueueTemplate()
              .getLeafOnlyProperties());
    }
    mode = CapacitySchedulerInfoHelper.getMode(parent);
    queueType = CapacitySchedulerInfoHelper.getQueueType(parent);
    creationMethod = CapacitySchedulerInfoHelper.getCreationMethod(parent);
    autoCreationEligibility = CapacitySchedulerInfoHelper
        .getAutoCreationEligibility(parent);

    defaultNodeLabelExpression = parent.getDefaultNodeLabelExpression();
    schedulerName = "Capacity Scheduler";
  }

  public float getCapacity() {
    return this.capacity;
  }

  public float getUsedCapacity() {
    return this.usedCapacity;
  }

  public QueueCapacitiesInfo getCapacities() {
    return capacities;
  }

  public float getMaxCapacity() {
    return this.maxCapacity;
  }

  public String getQueueName() {
    return this.queueName;
  }

  public String getQueuePath() {
    return this.queuePath;
  }

  public ResourceInfo getMaximumAllocation() {
    return maximumAllocation;
  }

  public QueueAclsInfo getQueueAcls() {
    return queueAcls;
  }

  public int getPriority() {
    return queuePriority;
  }

  public String getOrderingPolicyInfo() {
    return orderingPolicyInfo;
  }

  public CapacitySchedulerQueueInfoList getQueues() {
    return this.queues;
  }

  protected CapacitySchedulerQueueInfoList getQueues(
      CapacityScheduler cs, CSQueue parent) {
    CapacitySchedulerQueueInfoList queuesInfo =
        new CapacitySchedulerQueueInfoList();
    // JAXB marshalling leads to situation where the "type" field injected
    // for JSON changes from string to array depending on order of printing
    // Issue gets fixed if all the leaf queues are marshalled before the
    // non-leaf queues. See YARN-4785 for more details.
    List<CSQueue> childQueues = new ArrayList<>();
    List<CSQueue> childLeafQueues = new ArrayList<>();
    List<CSQueue> childNonLeafQueues = new ArrayList<>();
    for (CSQueue queue : parent.getChildQueues()) {
      if (queue instanceof AbstractLeafQueue) {
        childLeafQueues.add(queue);
      } else {
        childNonLeafQueues.add(queue);
      }
    }
    childQueues.addAll(childLeafQueues);
    childQueues.addAll(childNonLeafQueues);

    for (CSQueue queue : childQueues) {
      CapacitySchedulerQueueInfo info;
      if (queue instanceof AbstractLeafQueue) {
        info = new CapacitySchedulerLeafQueueInfo(cs, (AbstractLeafQueue) queue);
      } else {
        info = new CapacitySchedulerQueueInfo(cs, queue);
        info.queues = getQueues(cs, queue);
      }
      queuesInfo.addToQueueInfoList(info);
    }
    return queuesInfo;
  }

  public String getMode() {
    return mode;
  }

  public String getQueueType() {
    return queueType;
  }
}
