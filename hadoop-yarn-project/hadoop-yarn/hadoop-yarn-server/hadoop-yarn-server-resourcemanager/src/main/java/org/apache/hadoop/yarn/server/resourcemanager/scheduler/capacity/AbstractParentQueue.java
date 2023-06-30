/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getACLsForFlexibleAutoCreatedParentQueue;

public abstract class AbstractParentQueue extends AbstractCSQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractParentQueue.class);

  protected final List<CSQueue> childQueues;
  private final boolean rootQueue;
  private AtomicInteger numApplications = new AtomicInteger(0);

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private QueueOrderingPolicy queueOrderingPolicy;

  private long lastSkipQueueDebugLoggingTimestamp = -1;

  private int runnableApps;

  private final boolean allowZeroCapacitySum;

  private AutoCreatedQueueTemplate autoCreatedQueueTemplate;

  // A ratio of the queue's effective minimum resource and the summary of the configured
  // minimum resource of its children grouped by labels and calculated for each resource names
  // distinctively.
  private final Map<String, Map<String, Float>> effectiveMinResourceRatio =
      new ConcurrentHashMap<>();

  public AbstractParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old)
      throws IOException {
    this(queueContext, queueName, parent, old, false);
  }

  public AbstractParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old, boolean isDynamic) throws
      IOException {

    super(queueContext, queueName, parent, old);
    setDynamicQueue(isDynamic);
    this.rootQueue = (parent == null);

    float rawCapacity = queueContext.getConfiguration()
          .getNonLabeledQueueCapacity(this.queuePath);

    if (rootQueue &&
          (rawCapacity != CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE)) {
      throw new IllegalArgumentException("Illegal " +
            "capacity of " + rawCapacity + " for queue " + queueName +
            ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE);
    }

    this.childQueues = new ArrayList<>();
    this.allowZeroCapacitySum =
          queueContext.getConfiguration()
              .getAllowZeroCapacitySum(getQueuePath());

  }

  // returns what is configured queue ordering policy
  private String getQueueOrderingPolicyConfigName() {
    return queueOrderingPolicy == null ?
        null :
        queueOrderingPolicy.getConfigName();
  }

  protected void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    writeLock.lock();
    try {
      CapacitySchedulerConfiguration configuration = queueContext.getConfiguration();
      autoCreatedQueueTemplate = new AutoCreatedQueueTemplate(
          configuration, this.queuePath);
      super.setupQueueConfigs(clusterResource);
      StringBuilder aclsString = new StringBuilder();
      for (Map.Entry<AccessType, AccessControlList> e : getACLs().entrySet()) {
        aclsString.append(e.getKey()).append(":")
            .append(e.getValue().getAclString());
      }

      StringBuilder labelStrBuilder = new StringBuilder();
      if (getAccessibleNodeLabels() != null) {
        for (String nodeLabel : getAccessibleNodeLabels()) {
          labelStrBuilder.append(nodeLabel).append(",");
        }
      }

      // Initialize queue ordering policy
      queueOrderingPolicy = configuration.getQueueOrderingPolicy(
          getQueuePath(), parent == null ?
              null :
              ((AbstractParentQueue) parent).getQueueOrderingPolicyConfigName());
      queueOrderingPolicy.setQueues(childQueues);

      LOG.info(getQueueName() + ", " + getCapacityOrWeightString()
          + ", absoluteCapacity=" + getAbsoluteCapacity()
          + ", maxCapacity=" + getMaximumCapacity()
          + ", absoluteMaxCapacity=" + getAbsoluteMaximumCapacity()
          + ", state=" + getState() + ", acls="
          + aclsString + ", labels=" + labelStrBuilder + "\n"
          + ", reservationsContinueLooking=" + isReservationsContinueLooking()
          + ", orderingPolicy=" + getQueueOrderingPolicyConfigName()
          + ", priority=" + getPriority()
          + ", allowZeroCapacitySum=" + allowZeroCapacitySum);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  protected void setDynamicQueueACLProperties() {
    super.setDynamicQueueACLProperties();

    if (parent instanceof AbstractParentQueue) {
      acls.putAll(getACLsForFlexibleAutoCreatedParentQueue(
          ((AbstractParentQueue) parent).getAutoCreatedQueueTemplate()));
    }
  }

  private static float PRECISION = 0.0005f; // 0.05% precision

  // Check weight configuration, throw exception when configuration is invalid
  // return true when all children use weight mode.
  public QueueCapacityType getCapacityConfigurationTypeForQueues(
      Collection<CSQueue> queues) throws IOException {
    // Do we have ANY queue set capacity in any labels?
    boolean percentageIsSet = false;

    // Do we have ANY queue set weight in any labels?
    boolean weightIsSet = false;

    // Do we have ANY queue set absolute in any labels?
    boolean absoluteMinResSet = false;

    StringBuilder diagMsg = new StringBuilder();

    for (CSQueue queue : queues) {
      for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
        float capacityByLabel = queue.getQueueCapacities().getCapacity(nodeLabel);
        if (capacityByLabel > 0) {
          percentageIsSet = true;
        }
        float weightByLabel = queue.getQueueCapacities().getWeight(nodeLabel);
        // By default weight is set to -1, so >= 0 is enough.
        if (weightByLabel >= 0) {
          weightIsSet = true;
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses weight mode}. ");
        }
        if (!queue.getQueueResourceQuotas().getConfiguredMinResource(nodeLabel)
            .equals(Resources.none())) {
          absoluteMinResSet = true;
          // There's a special handling: when absolute resource is configured,
          // capacity will be calculated (and set) for UI/metrics purposes, so
          // when asboluteMinResource is set, unset percentage
          percentageIsSet = false;
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses absolute mode}. ");
        }
        if (percentageIsSet) {
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses percentage mode}. ");
        }
      }
    }
    // If we have mixed capacity, weight or absolute resource (any of the two)
    // We will throw exception
    // Root queue is an exception here, because by default root queue returns
    // 100 as capacity no matter what. We should look into this case in the
    // future. To avoid impact too many code paths, we don;t check root queue's
    // config.
    if (queues.iterator().hasNext() &&
        !queues.iterator().next().getQueuePath().equals(
        CapacitySchedulerConfiguration.ROOT) &&
        (percentageIsSet ? 1 : 0) + (weightIsSet ? 1 : 0) + (absoluteMinResSet ?
            1 :
            0) > 1) {
      throw new IOException("Parent queue '" + getQueuePath()
          + "' have children queue used mixed of "
          + " weight mode, percentage and absolute mode, it is not allowed, please "
          + "double check, details:" + diagMsg.toString());
    }

    if (weightIsSet || queues.isEmpty()) {
      return QueueCapacityType.WEIGHT;
    } else if (absoluteMinResSet) {
      return QueueCapacityType.ABSOLUTE_RESOURCE;
    } else {
      return QueueCapacityType.PERCENT;
    }
  }

  public enum QueueCapacityType {
    WEIGHT, ABSOLUTE_RESOURCE, PERCENT;
  }

  /**
   * Set child queue and verify capacities
   * +--------------+---------------------------+-------------------------------------+------------------------+
   * |              | parent-weight             | parent-pct                          | parent-abs             |
   * +--------------+---------------------------+-------------------------------------+------------------------+
   * | child-weight | No specific check         | No specific check                   | X                      |
   * +--------------+---------------------------+-------------------------------------+------------------------+
   * | child-pct    | Sum(children.capacity) =  | When:                               | X                      |
   * |              |   0 OR 100                |   parent.capacity>0                 |                        |
   * |              |                           |     sum(children.capacity)=100 OR 0 |                        |
   * |              |                           |   parent.capacity=0                 |                        |
   * |              |                           |     sum(children.capacity)=0        |                        |
   * +--------------+---------------------------+-------------------------------------+------------------------+
   * | child-abs    | X                         | X                                   | Sum(children.minRes)<= |
   * |              |                           |                                     | parent.minRes          |
   * +--------------+---------------------------+-------------------------------------+------------------------+
   * @param childQueues
   */
  void setChildQueues(Collection<CSQueue> childQueues) throws IOException {
    writeLock.lock();
    try {
      boolean isLegacyQueueMode = queueContext.getConfiguration().isLegacyQueueMode();
      if (isLegacyQueueMode) {
        QueueCapacityType childrenCapacityType =
            getCapacityConfigurationTypeForQueues(childQueues);
        QueueCapacityType parentCapacityType =
            getCapacityConfigurationTypeForQueues(ImmutableList.of(this));

        if (childrenCapacityType == QueueCapacityType.ABSOLUTE_RESOURCE
            || parentCapacityType == QueueCapacityType.ABSOLUTE_RESOURCE) {
          // We don't allow any mixed absolute + {weight, percentage} between
          // children and parent
          if (childrenCapacityType != parentCapacityType && !this.getQueuePath()
              .equals(CapacitySchedulerConfiguration.ROOT)) {
            throw new IOException("Parent=" + this.getQueuePath()
                + ": When absolute minResource is used, we must make sure both "
                + "parent and child all use absolute minResource");
          }

          // Ensure that for each parent queue: parent.min-resource >=
          // Σ(child.min-resource).
          for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
            Resource minRes = Resources.createResource(0, 0);
            for (CSQueue queue : childQueues) {
              // Accumulate all min/max resource configured for all child queues.
              Resources.addTo(minRes, queue.getQueueResourceQuotas()
                  .getConfiguredMinResource(nodeLabel));
            }
            Resource resourceByLabel = labelManager.getResourceByLabel(nodeLabel,
                queueContext.getClusterResource());
            Resource parentMinResource =
                usageTracker.getQueueResourceQuotas().getConfiguredMinResource(nodeLabel);
            if (!parentMinResource.equals(Resources.none()) && Resources.lessThan(
                resourceCalculator, resourceByLabel, parentMinResource, minRes)) {
              throw new IOException(
                  "Parent Queues" + " capacity: " + parentMinResource
                      + " is less than" + " to its children:" + minRes
                      + " for queue:" + getQueueName());
            }
          }
        }

        // When child uses percent
        if (childrenCapacityType == QueueCapacityType.PERCENT) {
          float childrenPctSum = 0;
          // check label capacities
          for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
            // check children's labels
            childrenPctSum = 0;
            for (CSQueue queue : childQueues) {
              childrenPctSum += queue.getQueueCapacities().getCapacity(nodeLabel);
            }

            if (Math.abs(1 - childrenPctSum) > PRECISION) {
              // When children's percent sum != 100%
              if (Math.abs(childrenPctSum) > PRECISION) {
                // It is wrong when percent sum != {0, 1}
                throw new IOException(
                    "Illegal" + " capacity sum of " + childrenPctSum
                        + " for children of queue " + getQueueName() + " for label="
                        + nodeLabel + ". It should be either 0 or 1.0");
              } else {
                // We also allow children's percent sum = 0 under the following
                // conditions
                // - Parent uses weight mode
                // - Parent uses percent mode, and parent has
                //   (capacity=0 OR allowZero)
                if (parentCapacityType == QueueCapacityType.PERCENT) {
                  if ((Math.abs(queueCapacities.getCapacity(nodeLabel))
                      > PRECISION) && (!allowZeroCapacitySum)) {
                    throw new IOException(
                        "Illegal" + " capacity sum of " + childrenPctSum
                            + " for children of queue " + getQueueName()
                            + " for label=" + nodeLabel
                            + ". It is set to 0, but parent percent != 0, and "
                            + "doesn't allow children capacity to set to 0");
                  }
                }
              }
            } else {
              // Even if child pct sum == 1.0, we will make sure parent has
              // positive percent.
              if (parentCapacityType == QueueCapacityType.PERCENT && Math.abs(
                  queueCapacities.getCapacity(nodeLabel)) <= 0f
                  && !allowZeroCapacitySum) {
                throw new IOException(
                    "Illegal" + " capacity sum of " + childrenPctSum
                        + " for children of queue " + getQueueName() + " for label="
                        + nodeLabel + ". queue=" + getQueueName()
                        + " has zero capacity, but child"
                        + "queues have positive capacities");
              }
            }
          }
        }
      }

      this.childQueues.clear();
      this.childQueues.addAll(childQueues);
      if (LOG.isDebugEnabled()) {
        LOG.debug("setChildQueues: " + getChildQueuesToPrint());
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public QueueInfo getQueueInfo(
      boolean includeChildQueues, boolean recursive) {
    readLock.lock();
    try {
      QueueInfo queueInfo = getQueueInfo();

      List<QueueInfo> childQueuesInfo = new ArrayList<>();
      if (includeChildQueues) {
        for (CSQueue child : childQueues) {
          // Get queue information recursively?
          childQueuesInfo.add(child.getQueueInfo(recursive, recursive));
        }
      }
      queueInfo.setChildQueues(childQueuesInfo);

      return queueInfo;
    } finally {
      readLock.unlock();
    }

  }

  private QueueUserACLInfo getUserAclInfo(
      UserGroupInformation user) {
    readLock.lock();
    try {
      QueueUserACLInfo userAclInfo = recordFactory.newRecordInstance(
          QueueUserACLInfo.class);
      List<QueueACL> operations = new ArrayList<QueueACL>();
      for (QueueACL operation : QueueACL.values()) {
        if (hasAccess(operation, user)) {
          operations.add(operation);
        }
      }

      userAclInfo.setQueueName(getQueuePath());
      userAclInfo.setUserAcls(operations);
      return userAclInfo;
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    readLock.lock();
    try {
      List<QueueUserACLInfo> userAcls = new ArrayList<>();

      // Add parent queue acls
      userAcls.add(getUserAclInfo(user));

      // Add children queue acls
      for (CSQueue child : childQueues) {
        userAcls.addAll(child.getQueueUserAclInfo(user));
      }

      return userAcls;
    } finally {
      readLock.unlock();
    }

  }

  public String toString() {
    return getQueueName() + ": " +
        "numChildQueue= " + childQueues.size() + ", " +
        getCapacityOrWeightString() + ", " +
        "absoluteCapacity=" + queueCapacities.getAbsoluteCapacity() + ", " +
        "usedResources=" + usageTracker.getQueueUsage().getUsed() + ", " +
        "usedCapacity=" + getUsedCapacity() + ", " +
        "numApps=" + getNumApplications() + ", " +
        "numContainers=" + getNumContainers();
  }

  public CSQueue createNewQueue(String childQueuePath, boolean isLeaf)
      throws SchedulerDynamicEditException {
    try {
      AbstractCSQueue childQueue;
      String queueShortName = childQueuePath.substring(
          childQueuePath.lastIndexOf(".") + 1);

      if (isLeaf) {
        childQueue = new LeafQueue(queueContext,
            queueShortName, this, null, true);
      } else {
        childQueue = new ParentQueue(queueContext, queueShortName, this, null, true);
      }
      childQueue.setDynamicQueue(true);
      // It should be sufficient now, we don't need to set more, because weights
      // related setup will be handled in updateClusterResources

      return childQueue;
    } catch (IOException e) {
      throw new SchedulerDynamicEditException(e.toString());
    }
  }

  // New method to remove child queue
  public void removeChildQueue(CSQueue queue)
      throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      if (!(queue instanceof AbstractCSQueue) ||
          !((AbstractCSQueue) queue).isDynamicQueue()) {
        throw new SchedulerDynamicEditException("Queue " + getQueuePath()
            + " can not remove " + queue.getQueuePath() +
            " because it is not a dynamic queue");
      }

      // We need to check if the parent of the child queue is exactly this
      // ParentQueue object
      if (queue.getParent() != this) {
        throw new SchedulerDynamicEditException("Queue " + getQueuePath()
            + " can not remove " + queue.getQueuePath() +
            " because it has a different parent queue");
      }

      // Now we can do remove and update
      this.childQueues.remove(queue);
      queueContext.getQueueManager()
          .removeQueue(queue.getQueuePath());

      // Call updateClusterResource,
      // which will deal with all effectiveMin/MaxResource
      // Calculation
      this.updateClusterResource(queueContext.getClusterResource(),
          new ResourceLimits(queueContext.getClusterResource()));

    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Check whether this queue supports adding additional child queues
   * dynamically.
   * @return true, if queue is eligible to create additional queues dynamically,
   * false otherwise
   */
  public boolean isEligibleForAutoQueueCreation() {
    return isDynamicQueue() || queueContext.getConfiguration().
        isAutoQueueCreationV2Enabled(getQueuePath());
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue,
        Resource clusterResource) throws IOException {
    writeLock.lock();
    try {
      // We skip reinitialize for dynamic queues, when this is called, and
      // new queue is different from this queue, we will make this queue to be
      // static queue.
      if (newlyParsedQueue != this) {
        this.setDynamicQueue(false);
      }

      // Sanity check
      if (!(newlyParsedQueue instanceof AbstractParentQueue) || !newlyParsedQueue
          .getQueuePath().equals(getQueuePath())) {
        throw new IOException(
            "Trying to reinitialize " + getQueuePath() + " from "
                + newlyParsedQueue.getQueuePath());
      }

      AbstractParentQueue newlyParsedParentQueue = (AbstractParentQueue) newlyParsedQueue;

      // Set new configs
      setupQueueConfigs(clusterResource);

      // Re-configure existing child queues and add new ones
      // The CS has already checked to ensure all existing child queues are present!
      Map<String, CSQueue> currentChildQueues = getQueuesMap(childQueues);
      Map<String, CSQueue> newChildQueues = getQueuesMap(
          newlyParsedParentQueue.childQueues);

      // Reinitialize dynamic queues as well, because they are not parsed
      for (String queue : Sets.difference(currentChildQueues.keySet(),
          newChildQueues.keySet())) {
        CSQueue candidate = currentChildQueues.get(queue);
        if (candidate instanceof AbstractCSQueue) {
          if (((AbstractCSQueue) candidate).isDynamicQueue()) {
            candidate.reinitialize(candidate, clusterResource);
          }
        }
      }

      for (Map.Entry<String, CSQueue> e : newChildQueues.entrySet()) {
        String newChildQueueName = e.getKey();
        CSQueue newChildQueue = e.getValue();

        CSQueue childQueue = currentChildQueues.get(newChildQueueName);

        // Check if the child-queue already exists
        if (childQueue != null) {
          // Check if the child-queue has been converted into parent queue or
          // parent Queue has been converted to child queue. The CS has already
          // checked to ensure that this child-queue is in STOPPED state if
          // Child queue has been converted to ParentQueue.
          if ((childQueue instanceof AbstractLeafQueue
              && newChildQueue instanceof AbstractParentQueue)
              || (childQueue instanceof AbstractParentQueue
                  && newChildQueue instanceof AbstractLeafQueue)) {
            // We would convert this LeafQueue to ParentQueue, or vice versa.
            // consider this as the combination of DELETE then ADD.
            newChildQueue.setParent(this);
            currentChildQueues.put(newChildQueueName, newChildQueue);
            // inform CapacitySchedulerQueueManager
            CapacitySchedulerQueueManager queueManager =
                queueContext.getQueueManager();
            queueManager.addQueue(newChildQueueName, newChildQueue);
            continue;
          }
          // Re-init existing queues
          childQueue.reinitialize(newChildQueue, clusterResource);
          LOG.info(getQueuePath() + ": re-configured queue: " + childQueue);
        } else{
          // New child queue, do not re-init

          // Set parent to 'this'
          newChildQueue.setParent(this);

          // Save in list of current child queues
          currentChildQueues.put(newChildQueueName, newChildQueue);

          LOG.info(
              getQueuePath() + ": added new child queue: " + newChildQueue);
        }
      }

      // remove the deleted queue in the refreshed xml.
      for (Iterator<Map.Entry<String, CSQueue>> itr = currentChildQueues
          .entrySet().iterator(); itr.hasNext();) {
        Map.Entry<String, CSQueue> e = itr.next();
        String queueName = e.getKey();
        if (!newChildQueues.containsKey(queueName)) {
          if (((AbstractCSQueue)e.getValue()).isDynamicQueue()) {
            // Don't remove dynamic queue if we cannot find it in the config.
            continue;
          }
          itr.remove();
        }
      }

      // Re-sort all queues
      setChildQueues(currentChildQueues.values());

      // Make sure we notifies QueueOrderingPolicy
      queueOrderingPolicy.setQueues(childQueues);
    } finally {
      writeLock.unlock();
    }
  }

  private Map<String, CSQueue> getQueuesMap(List<CSQueue> queues) {
    Map<String, CSQueue> queuesMap = new HashMap<String, CSQueue>();
    for (CSQueue queue : queues) {
      queuesMap.put(queue.getQueuePath(), queue);
    }
    return queuesMap;
  }

  @Override
  public void submitApplication(ApplicationId applicationId, String user,
      String queue) throws AccessControlException {
    writeLock.lock();
    try {
      // Sanity check
      validateSubmitApplication(applicationId, user, queue);

      addApplication(applicationId, user);
    } finally {
      writeLock.unlock();
    }

    // Inform the parent queue
    if (parent != null) {
      try {
        parent.submitApplication(applicationId, user, queue);
      } catch (AccessControlException ace) {
        LOG.info("Failed to submit application to parent-queue: " +
            parent.getQueuePath(), ace);
        removeApplication(applicationId, user);
        throw ace;
      }
    }
  }

  public void validateSubmitApplication(ApplicationId applicationId,
      String userName, String queue) throws AccessControlException {
    writeLock.lock();
    try {
      if (queue.equals(getQueueName())) {
        throw new AccessControlException(
            "Cannot submit application " + "to non-leaf queue: " + getQueueName());
      }

      if (getState() != QueueState.RUNNING) {
        throw new AccessControlException("Queue " + getQueuePath()
            + " is STOPPED. Cannot accept submission of application: "
            + applicationId);
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName) {
    // submit attempt logic.
  }

  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName, boolean isMoveApp) {
    throw new UnsupportedOperationException("Submission of application attempt"
        + " to parent queue is not supported");
  }

  @Override
  public void finishApplicationAttempt(FiCaSchedulerApp application,
      String queue) {
    // finish attempt logic.
  }

  private void addApplication(ApplicationId applicationId,
      String user) {
    numApplications.incrementAndGet();

    LOG.info(
        "Application added -" + " appId: " + applicationId + " user: " + user
            + " leaf-queue of parent: " + getQueuePath() + " #applications: "
            + getNumApplications());
  }

  @Override
  public void finishApplication(ApplicationId application, String user) {

    removeApplication(application, user);

    appFinished();

    // Inform the parent queue
    if (parent != null) {
      parent.finishApplication(application, user);
    }
  }

  private void removeApplication(ApplicationId applicationId,
      String user) {
    numApplications.decrementAndGet();

    LOG.info("Application removed -" + " appId: " + applicationId + " user: "
        + user + " leaf-queue of parent: " + getQueuePath()
        + " #applications: " + getNumApplications());
  }

  private String getParentName() {
    return parent != null ? parent.getQueuePath() : "";
  }

  @Override
  public CSAssignment assignContainers(Resource clusterResource,
       CandidateNodeSet<FiCaSchedulerNode> candidates,
       ResourceLimits resourceLimits, SchedulingMode schedulingMode) {
    FiCaSchedulerNode node = CandidateNodeSetUtils.getSingleNode(candidates);

    // if our queue cannot access this node, just return
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !queueNodeLabelsSettings.isAccessibleToPartition(candidates.getPartition())) {
      if (LOG.isDebugEnabled()) {
        long now = System.currentTimeMillis();
        // Do logging every 1 sec to avoid excessive logging.
        if (now - this.lastSkipQueueDebugLoggingTimestamp > 1000) {
          LOG.debug("Skip this queue=" + getQueuePath()
              + ", because it is not able to access partition=" + candidates
              .getPartition());
          this.lastSkipQueueDebugLoggingTimestamp = now;
        }
      }

      ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
          getParentName(), getQueuePath(), ActivityState.REJECTED,
          ActivityDiagnosticConstant.QUEUE_NOT_ABLE_TO_ACCESS_PARTITION);
      if (rootQueue) {
        ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
            node);
      }

      return CSAssignment.NULL_ASSIGNMENT;
    }

    // Check if this queue need more resource, simply skip allocation if this
    // queue doesn't need more resources.
    if (!super.hasPendingResourceRequest(candidates.getPartition(),
        clusterResource, schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        long now = System.currentTimeMillis();
        // Do logging every 1 sec to avoid excessive logging.
        if (now - this.lastSkipQueueDebugLoggingTimestamp > 1000) {
          LOG.debug("Skip this queue=" + getQueuePath()
              + ", because it doesn't need more resource, schedulingMode="
              + schedulingMode.name() + " node-partition=" + candidates
              .getPartition());
          this.lastSkipQueueDebugLoggingTimestamp = now;
        }
      }

      ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
          getParentName(), getQueuePath(), ActivityState.SKIPPED,
          ActivityDiagnosticConstant.QUEUE_DO_NOT_NEED_MORE_RESOURCE);
      if (rootQueue) {
        ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
            node);
      }

      return CSAssignment.NULL_ASSIGNMENT;
    }

    CSAssignment assignment = new CSAssignment(Resources.createResource(0, 0),
        NodeType.NODE_LOCAL);

    while (canAssign(clusterResource, node)) {
      LOG.debug("Trying to assign containers to child-queue of {}",
          getQueuePath());

      // Are we over maximum-capacity for this queue?
      // This will also consider parent's limits and also continuous reservation
      // looking
      if (!super.canAssignToThisQueue(clusterResource,
          candidates.getPartition(),
          resourceLimits, Resources
              .createResource(getMetrics().getReservedMB(),
                  getMetrics().getReservedVirtualCores()), schedulingMode)) {

        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParentName(), getQueuePath(), ActivityState.REJECTED,
            ActivityDiagnosticConstant.QUEUE_HIT_MAX_CAPACITY_LIMIT);
        if (rootQueue) {
          ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
              node);
        }

        break;
      }

      // Schedule
      CSAssignment assignedToChild = assignContainersToChildQueues(
          clusterResource, candidates, resourceLimits, schedulingMode);
      assignment.setType(assignedToChild.getType());
      assignment.setRequestLocalityType(
          assignedToChild.getRequestLocalityType());
      assignment.setExcessReservation(assignedToChild.getExcessReservation());
      assignment.setContainersToKill(assignedToChild.getContainersToKill());
      assignment.setFulfilledReservation(
          assignedToChild.isFulfilledReservation());
      assignment.setFulfilledReservedContainer(
          assignedToChild.getFulfilledReservedContainer());

      // Done if no child-queue assigned anything
      if (Resources.greaterThan(resourceCalculator, clusterResource,
          assignedToChild.getResource(), Resources.none())) {

        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParentName(), getQueuePath(), ActivityState.ACCEPTED,
            ActivityDiagnosticConstant.EMPTY);

        boolean isReserved =
            assignedToChild.getAssignmentInformation().getReservationDetails()
                != null && !assignedToChild.getAssignmentInformation()
                .getReservationDetails().isEmpty();
        if (rootQueue) {
          ActivitiesLogger.NODE.finishAllocatedNodeAllocation(
              activitiesManager, node,
              assignedToChild.getAssignmentInformation()
                  .getFirstAllocatedOrReservedContainerId(),
              isReserved ?
                  AllocationState.RESERVED : AllocationState.ALLOCATED);
        }

        // Track resource utilization in this pass of the scheduler
        Resources.addTo(assignment.getResource(),
            assignedToChild.getResource());
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
            assignedToChild.getAssignmentInformation().getAllocated());
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
            assignedToChild.getAssignmentInformation().getReserved());
        assignment.getAssignmentInformation().incrAllocations(
            assignedToChild.getAssignmentInformation().getNumAllocations());
        assignment.getAssignmentInformation().incrReservations(
            assignedToChild.getAssignmentInformation().getNumReservations());
        assignment.getAssignmentInformation().getAllocationDetails().addAll(
            assignedToChild.getAssignmentInformation()
                .getAllocationDetails());
        assignment.getAssignmentInformation().getReservationDetails().addAll(
            assignedToChild.getAssignmentInformation()
                .getReservationDetails());
        assignment.setIncreasedAllocation(
            assignedToChild.isIncreasedAllocation());

        if (LOG.isDebugEnabled()) {
          LOG.debug("assignedContainer reserved=" + isReserved + " queue="
              + getQueuePath() + " usedCapacity=" + getUsedCapacity()
              + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + " used="
              + usageTracker.getQueueUsage().getUsed() + " cluster=" + clusterResource);

          LOG.debug(
              "ParentQ=" + getQueuePath() + " assignedSoFarInThisIteration="
                  + assignment.getResource() + " usedCapacity="
                  + getUsedCapacity() + " absoluteUsedCapacity="
                  + getAbsoluteUsedCapacity());
        }
      } else{
        assignment.setSkippedType(assignedToChild.getSkippedType());

        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParentName(), getQueuePath(), ActivityState.SKIPPED,
            ActivityDiagnosticConstant.EMPTY);
        if (rootQueue) {
          ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
              node);
        }

        break;
      }

      /*
       * Previously here, we can allocate more than one container for each
       * allocation under rootQ. Now this logic is not proper any more
       * in global scheduling world.
       *
       * So here do not try to allocate more than one container for each
       * allocation, let top scheduler make the decision.
       */
      break;
    }

    return assignment;
  }

  private boolean canAssign(Resource clusterResource, FiCaSchedulerNode node) {
    // When node == null means global scheduling is enabled, always return true
    if (null == node) {
      return true;
    }

    // Two conditions need to meet when trying to allocate:
    // 1) Node doesn't have reserved container
    // 2) Node's available-resource + killable-resource should > 0
    boolean accept = node.getReservedContainer() == null &&
        Resources.fitsIn(resourceCalculator, queueAllocationSettings.getMinimumAllocation(),
            Resources.add(node.getUnallocatedResource(), node.getTotalKillableResources()));
    if (!accept) {
      ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
          getParentName(), getQueuePath(), ActivityState.REJECTED,
          () -> node.getReservedContainer() != null ?
              ActivityDiagnosticConstant.
                  QUEUE_SKIPPED_BECAUSE_SINGLE_NODE_RESERVED :
              ActivityDiagnosticConstant.
                  QUEUE_SKIPPED_BECAUSE_SINGLE_NODE_RESOURCE_INSUFFICIENT);
      if (rootQueue) {
        ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
            node);
      }
    }
    return accept;
  }

  public ResourceLimits getResourceLimitsOfChild(CSQueue child,
      Resource clusterResource, ResourceLimits parentLimits,
      String nodePartition, boolean netLimit) {
    // Set resource-limit of a given child, child.limit =
    // min(my.limit - my.used + child.used, child.max)

    // First, cap parent limit by parent's max
    parentLimits.setLimit(Resources.min(resourceCalculator, clusterResource,
        parentLimits.getLimit(),
        usageTracker.getQueueResourceQuotas().getEffectiveMaxResource(nodePartition)));

    // Parent available resource = parent-limit - parent-used-resource
    Resource limit = parentLimits.getLimit();
    if (netLimit) {
      limit = parentLimits.getNetLimit();
    }
    Resource parentMaxAvailableResource = Resources.subtract(
        limit, usageTracker.getQueueUsage().getUsed(nodePartition));

    // Deduct killable from used
    Resources.addTo(parentMaxAvailableResource,
        getTotalKillableResource(nodePartition));

    // Child's limit = parent-available-resource + child-used
    Resource childLimit = Resources.add(parentMaxAvailableResource,
        child.getQueueResourceUsage().getUsed(nodePartition));

    // Normalize before return
    childLimit =
        Resources.roundDown(resourceCalculator, childLimit,
            queueAllocationSettings.getMinimumAllocation());

    return new ResourceLimits(childLimit);
  }

  private Iterator<CSQueue> sortAndGetChildrenAllocationIterator(
      String partition) {
    return queueOrderingPolicy.getAssignmentIterator(partition);
  }

  private CSAssignment assignContainersToChildQueues(Resource cluster,
      CandidateNodeSet<FiCaSchedulerNode> candidates, ResourceLimits limits,
      SchedulingMode schedulingMode) {
    CSAssignment assignment = CSAssignment.NULL_ASSIGNMENT;

    printChildQueues();

    // Try to assign to most 'under-served' sub-queue
    for (Iterator<CSQueue> iter = sortAndGetChildrenAllocationIterator(
        candidates.getPartition()); iter.hasNext(); ) {
      CSQueue childQueue = iter.next();
      LOG.debug("Trying to assign to queue: {} stats: {}",
          childQueue.getQueuePath(), childQueue);

      // Get ResourceLimits of child queue before assign containers
      ResourceLimits childLimits =
          getResourceLimitsOfChild(childQueue, cluster, limits,
              candidates.getPartition(), true);

      CSAssignment childAssignment = childQueue.assignContainers(cluster,
          candidates, childLimits, schedulingMode);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Assigned to queue: " + childQueue.getQueuePath() +
            " stats: " + childQueue + " --> " +
            childAssignment.getResource() + ", " + childAssignment.getType());
      }

      if (Resources.greaterThan(
              resourceCalculator, cluster,
              childAssignment.getResource(), Resources.none())) {
        assignment = childAssignment;
        break;
      } else if (childAssignment.getSkippedType() ==
          CSAssignment.SkippedType.QUEUE_LIMIT) {
        if (assignment.getSkippedType() !=
            CSAssignment.SkippedType.QUEUE_LIMIT) {
          assignment = childAssignment;
        }
        Resource blockedHeadroom = null;
        if (childQueue instanceof AbstractLeafQueue) {
          blockedHeadroom = childLimits.getHeadroom();
        } else {
          blockedHeadroom = childLimits.getBlockedHeadroom();
        }
        Resource resourceToSubtract = Resources.max(resourceCalculator,
            cluster, blockedHeadroom, Resources.none());
        limits.addBlockedHeadroom(resourceToSubtract);
        if(LOG.isDebugEnabled()) {
          LOG.debug("Decrease parentLimits " + limits.getLimit() +
              " for " + this.getQueuePath() + " by " +
              resourceToSubtract + " as childQueue=" +
              childQueue.getQueuePath() + " is blocked");
        }
      }
    }

    return assignment;
  }

  String getChildQueuesToPrint() {
    StringBuilder sb = new StringBuilder();
    for (CSQueue q : childQueues) {
      sb.append(q.getQueuePath() +
          " usedCapacity=(" + q.getUsedCapacity() + "), " +
          " label=("
          + StringUtils.join(q.getAccessibleNodeLabels().iterator(), ",")
          + ")");
    }
    return sb.toString();
  }

  private void printChildQueues() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("printChildQueues - queue: " + getQueuePath()
          + " child-queues: " + getChildQueuesToPrint());
    }
  }

  private void internalReleaseResource(Resource clusterResource,
      FiCaSchedulerNode node, Resource releasedResource) {
    writeLock.lock();
    try {
      super.releaseResource(clusterResource, releasedResource,
          node.getPartition());

      LOG.debug("completedContainer {}, cluster={}", this, clusterResource);

    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void completedContainer(Resource clusterResource,
      FiCaSchedulerApp application, FiCaSchedulerNode node,
      RMContainer rmContainer, ContainerStatus containerStatus,
      RMContainerEventType event, CSQueue completedChildQueue,
      boolean sortQueues) {
    if (application != null) {
      internalReleaseResource(clusterResource, node,
          rmContainer.getContainer().getResource());

      // Inform the parent
      if (parent != null) {
        // complete my parent
        parent.completedContainer(clusterResource, application,
            node, rmContainer, null, event, this, sortQueues);
      }
    }
  }

  @Override
  public void refreshAfterResourceCalculation(Resource clusterResource,
      ResourceLimits resourceLimits) {
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        this, labelManager, null);
    // Update configured capacity/max-capacity for default partition only
    CSQueueUtils.updateConfiguredCapacityMetrics(resourceCalculator,
        labelManager.getResourceByLabel(null, clusterResource),
        RMNodeLabelsManager.NO_LABEL, this);

    LOG.info("Refresh after resource calculation (PARENT) {}\n"
            + "effectiveMinResource = {}\n"
            + "effectiveMaxResource = {}\n"
            + "capacity = {}\n"
            + "maxCapacity = {}\n"
            + "absoluteCapacity = {}\n"
            + "absoluteMaxCapacity = {}",
        queuePath,
        getEffectiveCapacity(NO_LABEL),
        getEffectiveMaxCapacity(NO_LABEL),
        getCapacity(),
        getMaximumCapacity(),
        getAbsoluteCapacity(),
        getAbsoluteMaximumCapacity());
  }

  @Override
  public void updateClusterResource(Resource clusterResource,
      ResourceLimits resourceLimits) {
    if (queueContext.getConfiguration().isLegacyQueueMode()) {
      updateClusterResourceLegacyMode(clusterResource, resourceLimits);
      return;
    }

    CapacitySchedulerQueueCapacityHandler handler =
        queueContext.getQueueManager().getQueueCapacityHandler();
    if (rootQueue) {
      handler.updateRoot(this, clusterResource);
      handler.updateChildren(clusterResource, this);
    } else {
      handler.updateChildren(clusterResource, getParent());
    }
  }

  public void updateClusterResourceLegacyMode(Resource clusterResource,
                                              ResourceLimits resourceLimits) {
    writeLock.lock();
    try {
      // Special handle root queue
      if (rootQueue) {
        for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
          if (queueCapacities.getWeight(nodeLabel) > 0) {
            queueCapacities.setNormalizedWeight(nodeLabel, 1f);
          }
        }
      }

      // Update absolute capacities of this queue, this need to happen before
      // below calculation for effective capacities
      updateAbsoluteCapacities();

      // Normalize all dynamic queue queue's weight to 1 for all accessible node
      // labels, this is important because existing node labels could keep
      // changing when new node added, or node label mapping changed. We need
      // this to ensure auto created queue can access all labels.
      for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
        for (CSQueue queue : childQueues) {
          // For dynamic queue, we will set weight to 1 every time, because it
          // is possible new labels added to the parent.
          if (((AbstractCSQueue) queue).isDynamicQueue()) {
            if (queue.getQueueCapacities().getWeight(nodeLabel) == -1f) {
              queue.getQueueCapacities().setWeight(nodeLabel, 1f);
            }
          }
        }
      }

      // Normalize weight of children
      if (getCapacityConfigurationTypeForQueues(childQueues)
          == QueueCapacityType.WEIGHT) {
        for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
          float sumOfWeight = 0;

          for (CSQueue queue : childQueues) {
            if (queue.getQueueCapacities().getExistingNodeLabels()
                .contains(nodeLabel)) {
              float weight = Math.max(0,
                  queue.getQueueCapacities().getWeight(nodeLabel));
              sumOfWeight += weight;
            }
          }
          // When sum of weight == 0, skip setting normalized_weight (so
          // normalized weight will be 0).
          if (Math.abs(sumOfWeight) > 1e-6) {
            for (CSQueue queue : childQueues) {
              if (queue.getQueueCapacities().getExistingNodeLabels()
                  .contains(nodeLabel)) {
                queue.getQueueCapacities().setNormalizedWeight(nodeLabel,
                    queue.getQueueCapacities().getWeight(nodeLabel) /
                        sumOfWeight);
              }
            }
          }
        }
      }

      // Update effective capacity in all parent queue.
      for (String label : queueNodeLabelsSettings.getConfiguredNodeLabels()) {
        calculateEffectiveResourcesAndCapacity(label, clusterResource);
      }

      // Update all children
      for (CSQueue childQueue : childQueues) {
        // Get ResourceLimits of child queue before assign containers
        ResourceLimits childLimits = getResourceLimitsOfChild(childQueue,
            clusterResource, resourceLimits,
            RMNodeLabelsManager.NO_LABEL, false);
        childQueue.updateClusterResource(clusterResource, childLimits);
      }

      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, null);
      // Update configured capacity/max-capacity for default partition only
      CSQueueUtils.updateConfiguredCapacityMetrics(resourceCalculator,
          labelManager.getResourceByLabel(null, clusterResource),
          RMNodeLabelsManager.NO_LABEL, this);
    } catch (IOException e) {
      LOG.error("Error during updating cluster resource: ", e);
      throw new YarnRuntimeException("Fatal issue during scheduling", e);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean hasChildQueues() {
    return true;
  }

  private void calculateEffectiveResourcesAndCapacity(String label,
      Resource clusterResource) {
    // Update effective resources for my self;
    if (rootQueue) {
      Resource resourceByLabel = labelManager.getResourceByLabel(label, clusterResource);
      usageTracker.getQueueResourceQuotas().setEffectiveMinResource(label, resourceByLabel);
      usageTracker.getQueueResourceQuotas().setEffectiveMaxResource(label, resourceByLabel);
    } else {
      super.updateEffectiveResources(clusterResource);
    }

    recalculateEffectiveMinRatio(label, clusterResource);
  }

  private void recalculateEffectiveMinRatio(String label, Resource clusterResource) {
    // For root queue, ensure that max/min resource is updated to latest
    // cluster resource.
    Resource resourceByLabel = labelManager.getResourceByLabel(label, clusterResource);

    // Total configured min resources of direct children of this given parent queue
    Resource configuredMinResources = Resource.newInstance(0L, 0);
    for (CSQueue childQueue : getChildQueues()) {
      Resources.addTo(configuredMinResources,
          childQueue.getQueueResourceQuotas().getConfiguredMinResource(label));
    }

    // Factor to scale down effective resource: When cluster has sufficient
    // resources, effective_min_resources will be same as configured min_resources.
    Resource numeratorForMinRatio = null;
    if (getQueuePath().equals("root")) {
      if (!resourceByLabel.equals(Resources.none()) && Resources.lessThan(resourceCalculator,
          clusterResource, resourceByLabel, configuredMinResources)) {
        numeratorForMinRatio = resourceByLabel;
      }
    } else {
      if (Resources.lessThan(resourceCalculator, clusterResource,
          usageTracker.getQueueResourceQuotas().getEffectiveMinResource(label),
          configuredMinResources)) {
        numeratorForMinRatio = usageTracker.getQueueResourceQuotas().getEffectiveMinResource(label);
      }
    }

    effectiveMinResourceRatio.put(label, getEffectiveMinRatio(
        configuredMinResources, numeratorForMinRatio));
  }

  private Map<String, Float> getEffectiveMinRatio(
      Resource configuredMinResources, Resource numeratorForMinRatio) {
    Map<String, Float> effectiveMinRatioPerResource = new HashMap<>();
    if (numeratorForMinRatio != null) {
      int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
      for (int i = 0; i < maxLength; i++) {
        ResourceInformation nResourceInformation = numeratorForMinRatio
            .getResourceInformation(i);
        ResourceInformation dResourceInformation = configuredMinResources
            .getResourceInformation(i);

        long nValue = nResourceInformation.getValue();
        long dValue = UnitsConversionUtil.convert(
            dResourceInformation.getUnits(), nResourceInformation.getUnits(),
            dResourceInformation.getValue());
        if (dValue != 0) {
          effectiveMinRatioPerResource.put(nResourceInformation.getName(),
              (float) nValue / dValue);
        }
      }
    }
    return ImmutableMap.copyOf(effectiveMinRatioPerResource);
  }

  @Override
  public List<CSQueue> getChildQueues() {
    readLock.lock();
    try {
      return new ArrayList<CSQueue>(childQueues);
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt attempt, RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    if (rmContainer.getExecutionType() != ExecutionType.GUARANTEED) {
      return;
    }

    // Careful! Locking order is important!
    writeLock.lock();
    try {
      FiCaSchedulerNode node = queueContext.getNode(
          rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource,
          rmContainer.getContainer().getResource(), node.getPartition());
    } finally {
      writeLock.unlock();
    }

    if (parent != null) {
      parent.recoverContainer(clusterResource, attempt, rmContainer);
    }
  }

  @Override
  public ActiveUsersManager getAbstractUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
      for (CSQueue queue : childQueues) {
        queue.collectSchedulerApplications(apps);
      }
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public void attachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          queueContext.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, rmContainer.getContainer()
          .getResource(), node.getPartition());
      LOG.info("movedContainer" + " queueMoveIn=" + getQueuePath()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + usageTracker.getQueueUsage().getUsed() +
          " cluster=" + clusterResource);
      // Inform the parent
      if (parent != null) {
        parent.attachContainer(clusterResource, application, rmContainer);
      }
    }
  }

  @Override
  public void detachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          queueContext.getNode(rmContainer.getContainer().getNodeId());
      super.releaseResource(clusterResource,
          rmContainer.getContainer().getResource(),
          node.getPartition());
      LOG.info("movedContainer" + " queueMoveOut=" + getQueuePath()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + usageTracker.getQueueUsage().getUsed() +
          " cluster=" + clusterResource);
      // Inform the parent
      if (parent != null) {
        parent.detachContainer(clusterResource, application, rmContainer);
      }
    }
  }

  public int getNumApplications() {
    return numApplications.get();
  }

  void allocateResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    writeLock.lock();
    try {
      super.allocateResource(clusterResource, resource, nodePartition);

      /**
       * check if we need to kill (killable) containers if maximum resource violated.
       * Doing this because we will deduct killable resource when going from root.
       * For example:
       * <pre>
       *      Root
       *      /   \
       *     a     b
       *   /  \
       *  a1  a2
       * </pre>
       *
       * a: max=10G, used=10G, killable=2G
       * a1: used=8G, killable=2G
       * a2: used=2G, pending=2G, killable=0G
       *
       * When we get queue-a to allocate resource, even if queue-a
       * reaches its max resource, we deduct its used by killable, so we can allocate
       * at most 2G resources. ResourceLimits passed down to a2 has headroom set to 2G.
       *
       * If scheduler finds a 2G available resource in existing cluster, and assigns it
       * to a2, now a2's used= 2G + 2G = 4G, and a's used = 8G + 4G = 12G > 10G
       *
       * When this happens, we have to preempt killable container (on same or different
       * nodes) of parent queue to avoid violating parent's max resource.
       */
      if (!usageTracker.getQueueResourceQuotas().getEffectiveMaxResource(nodePartition)
          .equals(Resources.none())) {
        if (Resources.lessThan(resourceCalculator, clusterResource,
            usageTracker.getQueueResourceQuotas().getEffectiveMaxResource(nodePartition),
            usageTracker.getQueueUsage().getUsed(nodePartition))) {
          killContainersToEnforceMaxQueueCapacity(nodePartition,
              clusterResource);
        }
      } else {
        if (getQueueCapacities()
            .getAbsoluteMaximumCapacity(nodePartition) < getQueueCapacities()
                .getAbsoluteUsedCapacity(nodePartition)) {
          killContainersToEnforceMaxQueueCapacity(nodePartition,
              clusterResource);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void killContainersToEnforceMaxQueueCapacity(String partition,
      Resource clusterResource) {
    Iterator<RMContainer> killableContainerIter = getKillableContainers(
        partition);
    if (!killableContainerIter.hasNext()) {
      return;
    }

    Resource partitionResource = labelManager.getResourceByLabel(partition,
        null);
    Resource maxResource = getEffectiveMaxCapacity(partition);

    while (Resources.greaterThan(resourceCalculator, partitionResource,
        usageTracker.getQueueUsage().getUsed(partition), maxResource)) {
      RMContainer toKillContainer = killableContainerIter.next();
      FiCaSchedulerApp attempt = queueContext.getApplicationAttempt(
          toKillContainer.getContainerId().getApplicationAttemptId());
      FiCaSchedulerNode node = queueContext.getNode(
          toKillContainer.getAllocatedNode());
      if (null != attempt && null != node) {
        AbstractLeafQueue lq = attempt.getCSLeafQueue();
        lq.completedContainer(clusterResource, attempt, node, toKillContainer,
            SchedulerUtils.createPreemptedContainerStatus(
                toKillContainer.getContainerId(),
                SchedulerUtils.PREEMPTED_CONTAINER), RMContainerEventType.KILL,
            null, false);
        LOG.info("Killed container=" + toKillContainer.getContainerId()
            + " from queue=" + lq.getQueuePath() + " to make queue=" + this
            .getQueuePath() + "'s max-capacity enforced");
      }

      if (!killableContainerIter.hasNext()) {
        break;
      }
    }
  }

  public void apply(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    if (request.anythingAllocatedOrReserved()) {
      ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode>
          allocation = request.getFirstAllocatedOrReservedContainer();
      SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
          schedulerContainer = allocation.getAllocatedOrReservedContainer();

      // Do not modify queue when allocation from reserved container
      if (allocation.getAllocateFromReservedContainer() == null) {
        writeLock.lock();
        try {
          // Book-keeping
          // Note: Update headroom to account for current allocation too...
          allocateResource(cluster, allocation.getAllocatedOrReservedResource(),
              schedulerContainer.getNodePartition());

          LOG.info("assignedContainer" + " queue=" + getQueuePath()
              + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
              + getAbsoluteUsedCapacity() + " used=" + usageTracker.getQueueUsage().getUsed()
              + " cluster=" + cluster);
        } finally {
          writeLock.unlock();
        }
      }
    }

    if (parent != null) {
      parent.apply(cluster, request);
    }
  }

  @Override
  public void stopQueue() {
    this.writeLock.lock();
    try {
      if (getNumApplications() > 0) {
        updateQueueState(QueueState.DRAINING);
      } else {
        updateQueueState(QueueState.STOPPED);
      }
      if (getChildQueues() != null) {
        for(CSQueue child : getChildQueues()) {
          child.stopQueue();
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  public QueueOrderingPolicy getQueueOrderingPolicy() {
    return queueOrderingPolicy;
  }

  @Override
  int getNumRunnableApps() {
    readLock.lock();
    try {
      return runnableApps;
    } finally {
      readLock.unlock();
    }
  }

  void incrementRunnableApps() {
    writeLock.lock();
    try {
      runnableApps++;
    } finally {
      writeLock.unlock();
    }
  }

  void decrementRunnableApps() {
    writeLock.lock();
    try {
      runnableApps--;
    } finally {
      writeLock.unlock();
    }
  }

  Map<String, Float> getEffectiveMinRatio(String label) {
    return effectiveMinResourceRatio.get(label);
  }

  @Override
  public boolean isEligibleForAutoDeletion() {
    return isDynamicQueue() && getChildQueues().size() == 0 &&
        queueContext.getConfiguration().
            isAutoExpiredDeletionEnabled(this.getQueuePath());
  }

  public AutoCreatedQueueTemplate getAutoCreatedQueueTemplate() {
    return autoCreatedQueueTemplate;
  }

}
