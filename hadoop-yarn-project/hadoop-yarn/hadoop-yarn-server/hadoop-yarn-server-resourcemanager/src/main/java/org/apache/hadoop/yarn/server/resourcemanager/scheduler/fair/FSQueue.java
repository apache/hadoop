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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public abstract class FSQueue implements Queue, Schedulable {
  private Resource fairShare = Resources.createResource(0, 0);
  private Resource steadyFairShare = Resources.createResource(0, 0);
  private final String name;
  protected final FairScheduler scheduler;
  private final FSQueueMetrics metrics;
  
  protected final FSParentQueue parent;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  protected SchedulingPolicy policy = SchedulingPolicy.DEFAULT_POLICY;

  private long fairSharePreemptionTimeout = Long.MAX_VALUE;
  private long minSharePreemptionTimeout = Long.MAX_VALUE;
  private float fairSharePreemptionThreshold = 0.5f;

  public FSQueue(String name, FairScheduler scheduler, FSParentQueue parent) {
    this.name = name;
    this.scheduler = scheduler;
    this.metrics = FSQueueMetrics.forQueue(getName(), parent, true, scheduler.getConf());
    metrics.setMinShare(getMinShare());
    metrics.setMaxShare(getMaxShare());
    this.parent = parent;
  }
  
  public String getName() {
    return name;
  }
  
  @Override
  public String getQueueName() {
    return name;
  }
  
  public SchedulingPolicy getPolicy() {
    return policy;
  }
  
  public FSParentQueue getParent() {
    return parent;
  }

  protected void throwPolicyDoesnotApplyException(SchedulingPolicy policy)
      throws AllocationConfigurationException {
    throw new AllocationConfigurationException("SchedulingPolicy " + policy
        + " does not apply to queue " + getName());
  }

  public abstract void setPolicy(SchedulingPolicy policy)
      throws AllocationConfigurationException;

  @Override
  public ResourceWeights getWeights() {
    return scheduler.getAllocationConfiguration().getQueueWeight(getName());
  }
  
  @Override
  public Resource getMinShare() {
    return scheduler.getAllocationConfiguration().getMinResources(getName());
  }
  
  @Override
  public Resource getMaxShare() {
    return scheduler.getAllocationConfiguration().getMaxResources(getName());
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public Priority getPriority() {
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }
  
  @Override
  public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setQueueName(getQueueName());

    if (scheduler.getClusterResource().getMemory() == 0) {
      queueInfo.setCapacity(0.0f);
    } else {
      queueInfo.setCapacity((float) getFairShare().getMemory() /
          scheduler.getClusterResource().getMemory());
    }

    if (getFairShare().getMemory() == 0) {
      queueInfo.setCurrentCapacity(0.0f);
    } else {
      queueInfo.setCurrentCapacity((float) getResourceUsage().getMemory() /
          getFairShare().getMemory());
    }

    ArrayList<QueueInfo> childQueueInfos = new ArrayList<QueueInfo>();
    if (includeChildQueues) {
      Collection<FSQueue> childQueues = getChildQueues();
      for (FSQueue child : childQueues) {
        childQueueInfos.add(child.getQueueInfo(recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueueInfos);
    queueInfo.setQueueState(QueueState.RUNNING);
    return queueInfo;
  }
  
  @Override
  public FSQueueMetrics getMetrics() {
    return metrics;
  }

  /** Get the fair share assigned to this Schedulable. */
  public Resource getFairShare() {
    return fairShare;
  }

  @Override
  public void setFairShare(Resource fairShare) {
    this.fairShare = fairShare;
    metrics.setFairShare(fairShare);
  }

  /** Get the steady fair share assigned to this Schedulable. */
  public Resource getSteadyFairShare() {
    return steadyFairShare;
  }

  public void setSteadyFairShare(Resource steadyFairShare) {
    this.steadyFairShare = steadyFairShare;
    metrics.setSteadyFairShare(steadyFairShare);
  }

  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return scheduler.getAllocationConfiguration().hasAccess(name, acl, user);
  }

  public long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  public void setFairSharePreemptionTimeout(long fairSharePreemptionTimeout) {
    this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
  }

  public long getMinSharePreemptionTimeout() {
    return minSharePreemptionTimeout;
  }

  public void setMinSharePreemptionTimeout(long minSharePreemptionTimeout) {
    this.minSharePreemptionTimeout = minSharePreemptionTimeout;
  }

  public float getFairSharePreemptionThreshold() {
    return fairSharePreemptionThreshold;
  }

  public void setFairSharePreemptionThreshold(float fairSharePreemptionThreshold) {
    this.fairSharePreemptionThreshold = fairSharePreemptionThreshold;
  }

  /**
   * Recomputes the shares for all child queues and applications based on this
   * queue's current share
   */
  public abstract void recomputeShares();

  /**
   * Update the min/fair share preemption timeouts and threshold for this queue.
   */
  public void updatePreemptionVariables() {
    // For min share timeout
    minSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getMinSharePreemptionTimeout(getName());
    if (minSharePreemptionTimeout == -1 && parent != null) {
      minSharePreemptionTimeout = parent.getMinSharePreemptionTimeout();
    }
    // For fair share timeout
    fairSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionTimeout(getName());
    if (fairSharePreemptionTimeout == -1 && parent != null) {
      fairSharePreemptionTimeout = parent.getFairSharePreemptionTimeout();
    }
    // For fair share preemption threshold
    fairSharePreemptionThreshold = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionThreshold(getName());
    if (fairSharePreemptionThreshold < 0 && parent != null) {
      fairSharePreemptionThreshold = parent.getFairSharePreemptionThreshold();
    }
  }

  /**
   * Gets the children of this queue, if any.
   */
  public abstract List<FSQueue> getChildQueues();
  
  /**
   * Adds all applications in the queue and its subqueues to the given collection.
   * @param apps the collection to add the applications to
   */
  public abstract void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps);
  
  /**
   * Return the number of apps for which containers can be allocated.
   * Includes apps in subqueues.
   */
  public abstract int getNumRunnableApps();
  
  /**
   * Helper method to check if the queue should attempt assigning resources
   * 
   * @return true if check passes (can assign) or false otherwise
   */
  protected boolean assignContainerPreCheck(FSSchedulerNode node) {
    if (!Resources.fitsIn(getResourceUsage(),
        scheduler.getAllocationConfiguration().getMaxResources(getName()))
        || node.getReservedContainer() != null) {
      return false;
    }
    return true;
  }

  /**
   * Returns true if queue has at least one app running.
   */
  public boolean isActive() {
    return getNumRunnableApps() > 0;
  }

  /** Convenient toString implementation for debugging. */
  @Override
  public String toString() {
    return String.format("[%s, demand=%s, running=%s, share=%s, w=%s]",
        getName(), getDemand(), getResourceUsage(), fairShare, getWeights());
  }
  
  @Override
  public Set<String> getAccessibleNodeLabels() {
    // TODO, add implementation for FS
    return null;
  }
  
  @Override
  public String getDefaultNodeLabelExpression() {
    // TODO, add implementation for FS
    return null;
  }
}
