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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import com.google.common.collect.Sets;

public abstract class AbstractCSQueue implements CSQueue {
  
  CSQueue parent;
  final String queueName;

  volatile int numContainers;
  
  Resource minimumAllocation;
  Resource maximumAllocation;
  QueueState state;
  final QueueMetrics metrics;
  
  final ResourceCalculator resourceCalculator;
  Set<String> accessibleLabels;
  RMNodeLabelsManager labelManager;
  String defaultLabelExpression;
  
  Map<QueueACL, AccessControlList> acls = 
      new HashMap<QueueACL, AccessControlList>();
  boolean reservationsContinueLooking;

  // Track resource usage-by-label like used-resource/pending-resource, etc.
  ResourceUsage queueUsage;
  
  // Track capacities like used-capcity/abs-used-capacity/capacity/abs-capacity,
  // etc.
  QueueCapacities queueCapacities;
  
  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);
  protected CapacitySchedulerContext csContext;
  
  public AbstractCSQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this.labelManager = cs.getRMContext().getNodeLabelManager();
    this.parent = parent;
    this.queueName = queueName;
    this.resourceCalculator = cs.getResourceCalculator();
    
    // must be called after parent and queueName is set
    this.metrics = old != null ? old.getMetrics() :
        QueueMetrics.forQueue(getQueuePath(), parent,
            cs.getConfiguration().getEnableUserMetrics(),
            cs.getConf());
    this.csContext = cs;

    // initialize ResourceUsage
    queueUsage = new ResourceUsage();

    // initialize QueueCapacities
    queueCapacities = new QueueCapacities(parent == null);    
  }
  
  protected void setupConfigurableCapacities() {
    CSQueueUtils.loadUpdateAndCheckCapacities(
        getQueuePath(),
        csContext.getConfiguration(), 
        queueCapacities,
        parent == null ? null : parent.getQueueCapacities());
  }
  
  @Override
  public synchronized float getCapacity() {
    return queueCapacities.getCapacity();
  }

  @Override
  public synchronized float getAbsoluteCapacity() {
    return queueCapacities.getAbsoluteCapacity();
  }

  @Override
  public float getAbsoluteMaximumCapacity() {
    return queueCapacities.getAbsoluteMaximumCapacity();
  }

  @Override
  public synchronized float getAbsoluteUsedCapacity() {
    return queueCapacities.getAbsoluteUsedCapacity();
  }

  @Override
  public float getMaximumCapacity() {
    return queueCapacities.getMaximumCapacity();
  }

  @Override
  public synchronized float getUsedCapacity() {
    return queueCapacities.getUsedCapacity();
  }

  @Override
  public Resource getUsedResources() {
    return queueUsage.getUsed();
  }

  public synchronized int getNumContainers() {
    return numContainers;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }
  
  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }
  
  @Override
  public String getQueueName() {
    return queueName;
  }
  
  @Override
  public synchronized CSQueue getParent() {
    return parent;
  }

  @Override
  public synchronized void setParent(CSQueue newParentQueue) {
    this.parent = (ParentQueue)newParentQueue;
  }
  
  public Set<String> getAccessibleNodeLabels() {
    return accessibleLabels;
  }
  
  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    synchronized (this) {
      if (acls.get(acl).isUserAllowed(user)) {
        return true;
      }
    }
    
    if (parent != null) {
      return parent.hasAccess(acl, user);
    }
    
    return false;
  }
  
  @Override
  public synchronized void setUsedCapacity(float usedCapacity) {
    queueCapacities.setUsedCapacity(usedCapacity);
  }
  
  @Override
  public synchronized void setAbsoluteUsedCapacity(float absUsedCapacity) {
    queueCapacities.setAbsoluteUsedCapacity(absUsedCapacity);
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  synchronized void setMaxCapacity(float maximumCapacity) {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(),
        queueCapacities.getCapacity(), maximumCapacity);
    float absMaxCapacity =
        CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);
    CSQueueUtils.checkAbsoluteCapacity(getQueueName(),
        queueCapacities.getAbsoluteCapacity(),
        absMaxCapacity);
    
    queueCapacities.setMaximumCapacity(maximumCapacity);
    queueCapacities.setAbsoluteMaximumCapacity(absMaxCapacity);
  }

  @Override
  public String getDefaultNodeLabelExpression() {
    return defaultLabelExpression;
  }
  
  synchronized void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    // get labels
    this.accessibleLabels =
        csContext.getConfiguration().getAccessibleNodeLabels(getQueuePath());
    this.defaultLabelExpression = csContext.getConfiguration()
        .getDefaultNodeLabelExpression(getQueuePath());

    // inherit from parent if labels not set
    if (this.accessibleLabels == null && parent != null) {
      this.accessibleLabels = parent.getAccessibleNodeLabels();
    }
    
    // inherit from parent if labels not set
    if (this.defaultLabelExpression == null && parent != null
        && this.accessibleLabels.containsAll(parent.getAccessibleNodeLabels())) {
      this.defaultLabelExpression = parent.getDefaultNodeLabelExpression();
    }

    // After we setup labels, we can setup capacities
    setupConfigurableCapacities();
    
    this.minimumAllocation = csContext.getMinimumResourceCapability();
    this.maximumAllocation = csContext.getMaximumResourceCapability();
    
    this.state = csContext.getConfiguration().getState(getQueuePath());
    this.acls = csContext.getConfiguration().getAcls(getQueuePath());

    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        resourceCalculator, this, parent, clusterResource, minimumAllocation);
    
    // Check if labels of this queue is a subset of parent queue, only do this
    // when we not root
    if (parent != null && parent.getParent() != null) {
      if (parent.getAccessibleNodeLabels() != null
          && !parent.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
        // if parent isn't "*", child shouldn't be "*" too
        if (this.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
          throw new IOException("Parent's accessible queue is not ANY(*), "
              + "but child's accessible queue is *");
        } else {
          Set<String> diff =
              Sets.difference(this.getAccessibleNodeLabels(),
                  parent.getAccessibleNodeLabels());
          if (!diff.isEmpty()) {
            throw new IOException("Some labels of child queue is not a subset "
                + "of parent queue, these labels=["
                + StringUtils.join(diff, ",") + "]");
          }
        }
      }
    }

    this.reservationsContinueLooking = csContext.getConfiguration()
        .getReservationContinueLook();

  }
  
  protected QueueInfo getQueueInfo() {
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setQueueName(queueName);
    queueInfo.setAccessibleNodeLabels(accessibleLabels);
    queueInfo.setCapacity(queueCapacities.getCapacity());
    queueInfo.setMaximumCapacity(queueCapacities.getMaximumCapacity());
    queueInfo.setQueueState(state);
    queueInfo.setDefaultNodeLabelExpression(defaultLabelExpression);
    queueInfo.setCurrentCapacity(getUsedCapacity());
    return queueInfo;
  }
  
  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
  
  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }
  
  synchronized void allocateResource(Resource clusterResource, 
      Resource resource, Set<String> nodeLabels) {
    
    // Update usedResources by labels
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      queueUsage.incUsed(resource);
    } else {
      for (String label : Sets.intersection(accessibleLabels, nodeLabels)) {
        queueUsage.incUsed(label, resource);
      }
    }

    ++numContainers;
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
  }
  
  protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, Set<String> nodeLabels) {
    // Update usedResources by labels
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      queueUsage.decUsed(resource);
    } else {
      for (String label : Sets.intersection(accessibleLabels, nodeLabels)) {
        queueUsage.decUsed(label, resource);
      }
    }

    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
    --numContainers;
  }
  
  @Private
  public boolean getReservationContinueLooking() {
    return reservationsContinueLooking;
  }
  
  @Private
  public Map<QueueACL, AccessControlList> getACLs() {
    return acls;
  }

  @Private
  public QueueCapacities getQueueCapacities() {
    return queueCapacities;
  }
  
  @Private
  public ResourceUsage getQueueResourceUsage() {
    return queueUsage;
  }
}
