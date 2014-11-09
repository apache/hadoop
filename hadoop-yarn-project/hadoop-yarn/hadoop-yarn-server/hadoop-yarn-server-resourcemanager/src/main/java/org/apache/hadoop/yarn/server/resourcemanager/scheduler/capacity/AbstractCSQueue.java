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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.Sets;

public abstract class AbstractCSQueue implements CSQueue {
  
  CSQueue parent;
  final String queueName;
  
  float capacity;
  float maximumCapacity;
  float absoluteCapacity;
  float absoluteMaxCapacity;
  float absoluteUsedCapacity = 0.0f;

  float usedCapacity = 0.0f;
  volatile int numContainers;
  
  final Resource minimumAllocation;
  final Resource maximumAllocation;
  QueueState state;
  final QueueMetrics metrics;
  
  final ResourceCalculator resourceCalculator;
  Set<String> accessibleLabels;
  RMNodeLabelsManager labelManager;
  String defaultLabelExpression;
  Resource usedResources = Resources.createResource(0, 0);
  QueueInfo queueInfo;
  Map<String, Float> absoluteCapacityByNodeLabels;
  Map<String, Float> capacitiyByNodeLabels;
  Map<String, Resource> usedResourcesByNodeLabels = new HashMap<String, Resource>();
  Map<String, Float> absoluteMaxCapacityByNodeLabels;
  Map<String, Float> maxCapacityByNodeLabels;
  
  Map<QueueACL, AccessControlList> acls = 
      new HashMap<QueueACL, AccessControlList>();
  boolean reservationsContinueLooking;
  
  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);
  
  public AbstractCSQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this.minimumAllocation = cs.getMinimumResourceCapability();
    this.maximumAllocation = cs.getMaximumResourceCapability();
    this.labelManager = cs.getRMContext().getNodeLabelManager();
    this.parent = parent;
    this.queueName = queueName;
    this.resourceCalculator = cs.getResourceCalculator();
    this.queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    
    // must be called after parent and queueName is set
    this.metrics = old != null ? old.getMetrics() :
        QueueMetrics.forQueue(getQueuePath(), parent,
            cs.getConfiguration().getEnableUserMetrics(),
            cs.getConf());
    
    // get labels
    this.accessibleLabels = cs.getConfiguration().getAccessibleNodeLabels(getQueuePath());
    this.defaultLabelExpression = cs.getConfiguration()
        .getDefaultNodeLabelExpression(getQueuePath());
    
    this.queueInfo.setQueueName(queueName);
    
    // inherit from parent if labels not set
    if (this.accessibleLabels == null && parent != null) {
      this.accessibleLabels = parent.getAccessibleNodeLabels();
    }
    SchedulerUtils.checkIfLabelInClusterNodeLabels(labelManager,
        this.accessibleLabels);
    
    // inherit from parent if labels not set
    if (this.defaultLabelExpression == null && parent != null
        && this.accessibleLabels.containsAll(parent.getAccessibleNodeLabels())) {
      this.defaultLabelExpression = parent.getDefaultNodeLabelExpression();
    }
    
    // set capacity by labels
    capacitiyByNodeLabels =
        cs.getConfiguration().getNodeLabelCapacities(getQueuePath(), accessibleLabels,
            labelManager);

    // set maximum capacity by labels
    maxCapacityByNodeLabels =
        cs.getConfiguration().getMaximumNodeLabelCapacities(getQueuePath(),
            accessibleLabels, labelManager);
  }
  
  @Override
  public synchronized float getCapacity() {
    return capacity;
  }

  @Override
  public synchronized float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  @Override
  public float getAbsoluteMaximumCapacity() {
    return absoluteMaxCapacity;
  }

  @Override
  public synchronized float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity;
  }

  @Override
  public float getMaximumCapacity() {
    return maximumCapacity;
  }

  @Override
  public synchronized float getUsedCapacity() {
    return usedCapacity;
  }

  @Override
  public synchronized Resource getUsedResources() {
    return usedResources;
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
    this.usedCapacity = usedCapacity;
  }
  
  @Override
  public synchronized void setAbsoluteUsedCapacity(float absUsedCapacity) {
    this.absoluteUsedCapacity = absUsedCapacity;
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  synchronized void setMaxCapacity(float maximumCapacity) {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absMaxCapacity =
        CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);
    CSQueueUtils.checkAbsoluteCapacity(getQueueName(), absoluteCapacity,
        absMaxCapacity);
    
    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absMaxCapacity;
  }

  @Override
  public float getAbsActualCapacity() {
    // for now, simply return actual capacity = guaranteed capacity for parent
    // queue
    return absoluteCapacity;
  }

  @Override
  public String getDefaultNodeLabelExpression() {
    return defaultLabelExpression;
  }
  
  synchronized void setupQueueConfigs(Resource clusterResource, float capacity,
      float absoluteCapacity, float maximumCapacity, float absoluteMaxCapacity,
      QueueState state, Map<QueueACL, AccessControlList> acls,
      Set<String> labels, String defaultLabelExpression,
      Map<String, Float> nodeLabelCapacities,
      Map<String, Float> maximumNodeLabelCapacities,
      boolean reservationContinueLooking)
      throws IOException {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    CSQueueUtils.checkAbsoluteCapacity(getQueueName(), absoluteCapacity,
        absoluteMaxCapacity);

    this.capacity = capacity;
    this.absoluteCapacity = absoluteCapacity;

    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absoluteMaxCapacity;

    this.state = state;

    this.acls = acls;
    
    // set labels
    this.accessibleLabels = labels;
    
    // set label expression
    this.defaultLabelExpression = defaultLabelExpression;
    
    // copy node label capacity
    this.capacitiyByNodeLabels = new HashMap<String, Float>(nodeLabelCapacities);
    this.maxCapacityByNodeLabels =
        new HashMap<String, Float>(maximumNodeLabelCapacities);
    
    this.queueInfo.setAccessibleNodeLabels(this.accessibleLabels);
    this.queueInfo.setCapacity(this.capacity);
    this.queueInfo.setMaximumCapacity(this.maximumCapacity);
    this.queueInfo.setQueueState(this.state);
    this.queueInfo.setDefaultNodeLabelExpression(this.defaultLabelExpression);

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
    
    // calculate absolute capacity by each node label
    this.absoluteCapacityByNodeLabels =
        CSQueueUtils.computeAbsoluteCapacityByNodeLabels(
            this.capacitiyByNodeLabels, parent);
    
    // calculate maximum capacity by each node label
    this.absoluteMaxCapacityByNodeLabels =
        CSQueueUtils.computeAbsoluteMaxCapacityByNodeLabels(
            maximumNodeLabelCapacities, parent);
    
    // check absoluteMaximumNodeLabelCapacities is valid
    CSQueueUtils.checkAbsoluteCapacitiesByLabel(getQueueName(),
        absoluteCapacityByNodeLabels, absoluteCapacityByNodeLabels);
    
    this.reservationsContinueLooking = reservationContinueLooking;
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
    Resources.addTo(usedResources, resource);
    
    // Update usedResources by labels
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      if (!usedResourcesByNodeLabels.containsKey(RMNodeLabelsManager.NO_LABEL)) {
        usedResourcesByNodeLabels.put(RMNodeLabelsManager.NO_LABEL,
            Resources.createResource(0));
      }
      Resources.addTo(usedResourcesByNodeLabels.get(RMNodeLabelsManager.NO_LABEL),
          resource);
    } else {
      for (String label : Sets.intersection(accessibleLabels, nodeLabels)) {
        if (!usedResourcesByNodeLabels.containsKey(label)) {
          usedResourcesByNodeLabels.put(label, Resources.createResource(0));
        }
        Resources.addTo(usedResourcesByNodeLabels.get(label), resource);
      }
    }

    ++numContainers;
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
  }
  
  protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, Set<String> nodeLabels) {
    // Update queue metrics
    Resources.subtractFrom(usedResources, resource);

    // Update usedResources by labels
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      if (!usedResourcesByNodeLabels.containsKey(RMNodeLabelsManager.NO_LABEL)) {
        usedResourcesByNodeLabels.put(RMNodeLabelsManager.NO_LABEL,
            Resources.createResource(0));
      }
      Resources.subtractFrom(
          usedResourcesByNodeLabels.get(RMNodeLabelsManager.NO_LABEL), resource);
    } else {
      for (String label : Sets.intersection(accessibleLabels, nodeLabels)) {
        if (!usedResourcesByNodeLabels.containsKey(label)) {
          usedResourcesByNodeLabels.put(label, Resources.createResource(0));
        }
        Resources.subtractFrom(usedResourcesByNodeLabels.get(label), resource);
      }
    }

    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
    --numContainers;
  }
  
  @Private
  public float getCapacityByNodeLabel(String label) {
    if (StringUtils.equals(label, RMNodeLabelsManager.NO_LABEL)) {
      if (null == parent) {
        return 1f;
      }
      return getCapacity();
    }
    
    if (!capacitiyByNodeLabels.containsKey(label)) {
      return 0f;
    } else {
      return capacitiyByNodeLabels.get(label);
    }
  }
  
  @Private
  public float getAbsoluteCapacityByNodeLabel(String label) {
    if (StringUtils.equals(label, RMNodeLabelsManager.NO_LABEL)) {
      if (null == parent) {
        return 1f; 
      }
      return getAbsoluteCapacity();
    }
    
    if (!absoluteCapacityByNodeLabels.containsKey(label)) {
      return 0f;
    } else {
      return absoluteCapacityByNodeLabels.get(label);
    }
  }
  
  @Private
  public float getAbsoluteMaximumCapacityByNodeLabel(String label) {
    if (StringUtils.equals(label, RMNodeLabelsManager.NO_LABEL)) {
      return getAbsoluteMaximumCapacity();
    }
    
    if (!absoluteMaxCapacityByNodeLabels.containsKey(label)) {
      return 0f;
    } else {
      return absoluteMaxCapacityByNodeLabels.get(label);
    }
  }
  
  @Private
  public boolean getReservationContinueLooking() {
    return reservationsContinueLooking;
  }
  
  @Private
  public Map<QueueACL, AccessControlList> getACLs() {
    return acls;
  }
}
