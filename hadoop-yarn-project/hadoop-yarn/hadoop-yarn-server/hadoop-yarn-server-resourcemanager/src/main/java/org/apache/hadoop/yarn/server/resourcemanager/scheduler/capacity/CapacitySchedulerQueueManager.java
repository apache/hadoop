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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueStateManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.security.AppPriorityACLsManager;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 *
 * Context of the Queues in Capacity Scheduler.
 *
 */
@Private
@Unstable
public class CapacitySchedulerQueueManager implements SchedulerQueueManager<
    CSQueue, CapacitySchedulerConfiguration>{

  private static final Logger LOG = LoggerFactory.getLogger(
      CapacitySchedulerQueueManager.class);

  static class QueueHook {
    public CSQueue hook(CSQueue queue) {
      return queue;
    }
  }

  private static final QueueHook NOOP = new QueueHook();
  private CapacitySchedulerContext csContext;
  private final YarnAuthorizationProvider authorizer;
  private final CSQueueStore queues = new CSQueueStore();
  private CSQueue root;
  private final RMNodeLabelsManager labelManager;
  private AppPriorityACLsManager appPriorityACLManager;

  private QueueStateManager<CSQueue, CapacitySchedulerConfiguration>
      queueStateManager;

  /**
   * Construct the service.
   * @param conf the configuration
   * @param labelManager the labelManager
   * @param appPriorityACLManager App priority ACL manager
   */
  public CapacitySchedulerQueueManager(Configuration conf,
      RMNodeLabelsManager labelManager,
      AppPriorityACLsManager appPriorityACLManager) {
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
    this.labelManager = labelManager;
    this.queueStateManager = new QueueStateManager<>();
    this.appPriorityACLManager = appPriorityACLManager;
  }

  @Override
  public CSQueue getRootQueue() {
    return this.root;
  }

  @Override
  public Map<String, CSQueue> getQueues() {
    return queues.getFullNameQueues();
  }

  @VisibleForTesting
  public Map<String, CSQueue> getShortNameQueues() {
    return queues.getShortNameQueues();
  }

  @Override
  public void removeQueue(String queueName) {
    this.queues.remove(queueName);
  }

  @Override
  public void addQueue(String queueName, CSQueue queue) {
    this.queues.add(queue);
  }

  @Override
  public CSQueue getQueue(String queueName) {
    return queues.get(queueName);
  }

  public CSQueue getQueueByFullName(String name) {
    return queues.getByFullName(name);
  }

  String normalizeQueueName(String name) {
    CSQueue queue = this.queues.get(name);
    if (queue != null) {
      return queue.getQueuePath();
    }
    //We return the original name here instead of null, to make sure we don't
    // introduce a NPE, and let the process fail where it would fail for unknown
    // queues, resulting more informative error messages.
    return name;
  }

  public boolean isAmbiguous(String shortName) {
    return queues.isAmbiguous(shortName);
  }

  /**
   * Set the CapacitySchedulerContext.
   * @param capacitySchedulerContext the CapacitySchedulerContext
   */
  public void setCapacitySchedulerContext(
      CapacitySchedulerContext capacitySchedulerContext) {
    this.csContext = capacitySchedulerContext;
  }

  /**
   * Initialized the queues.
   * @param conf the CapacitySchedulerConfiguration
   * @throws IOException if fails to initialize queues
   */
  public void initializeQueues(CapacitySchedulerConfiguration conf)
    throws IOException {
    root = parseQueue(this.csContext, conf, null,
        CapacitySchedulerConfiguration.ROOT, queues, queues, NOOP);
    setQueueAcls(authorizer, appPriorityACLManager, queues);
    labelManager.reinitializeQueueLabels(getQueueToLabels());
    this.queueStateManager.initialize(this);
    LOG.info("Initialized root queue " + root);
  }

  @Override
  public void reinitializeQueues(CapacitySchedulerConfiguration newConf)
      throws IOException {
    // Parse new queues
    CSQueueStore newQueues = new CSQueueStore();
    CSQueue newRoot =  parseQueue(this.csContext, newConf, null,
        CapacitySchedulerConfiguration.ROOT, newQueues, queues, NOOP);

    // When failing over, if using configuration store, don't validate queue
    // hierarchy since queues can be removed without being STOPPED.
    if (!csContext.isConfigurationMutable() ||
        csContext.getRMContext().getHAServiceState()
            != HAServiceProtocol.HAServiceState.STANDBY) {
      // Ensure queue hierarchy in the new XML file is proper.
      CapacitySchedulerConfigValidator
              .validateQueueHierarchy(queues, newQueues, newConf);
    }

    // Add new queues and delete OldQeueus only after validation.
    updateQueues(queues, newQueues);

    // Re-configure queues
    root.reinitialize(newRoot, this.csContext.getClusterResource());

    setQueueAcls(authorizer, appPriorityACLManager, queues);

    // Re-calculate headroom for active applications
    Resource clusterResource = this.csContext.getClusterResource();
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    labelManager.reinitializeQueueLabels(getQueueToLabels());
    this.queueStateManager.initialize(this);
  }

  /**
   * Parse the queue from the configuration.
   * @param csContext the CapacitySchedulerContext
   * @param conf the CapacitySchedulerConfiguration
   * @param parent the parent queue
   * @param queueName the queue name
   * @param queues all the queues
   * @param oldQueues the old queues
   * @param hook the queue hook
   * @return the CSQueue
   * @throws IOException
   */
  static CSQueue parseQueue(
      CapacitySchedulerContext csContext,
      CapacitySchedulerConfiguration conf,
      CSQueue parent, String queueName,
      CSQueueStore queues,
      CSQueueStore oldQueues,
      QueueHook hook) throws IOException {
    CSQueue queue;
    String fullQueueName = (parent == null) ?
        queueName :
        (parent.getQueuePath() + "." + queueName);
    String[] childQueueNames = conf.getQueues(fullQueueName);
    boolean isReservableQueue = conf.isReservable(fullQueueName);
    boolean isAutoCreateEnabled = conf.isAutoCreateChildQueueEnabled(
        fullQueueName);
    if (childQueueNames == null || childQueueNames.length == 0) {
      if (null == parent) {
        throw new IllegalStateException(
            "Queue configuration missing child queue names for " + queueName);
      }
      // Check if the queue will be dynamically managed by the Reservation
      // system
      if (isReservableQueue) {
        queue = new PlanQueue(csContext, queueName, parent,
            oldQueues.get(fullQueueName));

        //initializing the "internal" default queue, for SLS compatibility
        String defReservationId =
            queueName + ReservationConstants.DEFAULT_QUEUE_SUFFIX;

        List<CSQueue> childQueues = new ArrayList<>();
        ReservationQueue resQueue = new ReservationQueue(csContext,
            defReservationId, (PlanQueue) queue);
        try {
          resQueue.setEntitlement(new QueueEntitlement(1.0f, 1.0f));
        } catch (SchedulerDynamicEditException e) {
          throw new IllegalStateException(e);
        }
        childQueues.add(resQueue);
        ((PlanQueue) queue).setChildQueues(childQueues);
        queues.add(resQueue);

      } else if (isAutoCreateEnabled) {
        queue = new ManagedParentQueue(csContext, queueName, parent,
            oldQueues.get(fullQueueName));

      } else{
        queue = new LeafQueue(csContext, queueName, parent,
            oldQueues.get(fullQueueName));
        // Used only for unit tests
        queue = hook.hook(queue);
      }
    } else{
      if (isReservableQueue) {
        throw new IllegalStateException(
            "Only Leaf Queues can be reservable for " + fullQueueName);
      }

      ParentQueue parentQueue;
      if (isAutoCreateEnabled) {
        parentQueue = new ManagedParentQueue(csContext, queueName, parent,
            oldQueues.get(fullQueueName));
      } else{
        parentQueue = new ParentQueue(csContext, queueName, parent,
            oldQueues.get(fullQueueName));
      }

      // Used only for unit tests
      queue = hook.hook(parentQueue);

      List<CSQueue> childQueues = new ArrayList<>();
      for (String childQueueName : childQueueNames) {
        CSQueue childQueue = parseQueue(csContext, conf, queue, childQueueName,
            queues, oldQueues, hook);
        childQueues.add(childQueue);
      }
      parentQueue.setChildQueues(childQueues);

    }

    queues.add(queue);

    LOG.info("Initialized queue: " + fullQueueName);
    return queue;
  }

  /**
   * Updates to our list of queues: Adds the new queues and deletes the removed
   * ones... be careful, do not overwrite existing queues.
   *
   * @param existingQueues, the existing queues
   * @param newQueues the new queues based on new XML
   */
  private void updateQueues(CSQueueStore existingQueues,
                            CSQueueStore newQueues) {
    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    for (CSQueue queue : newQueues.getQueues()) {
      if (existingQueues.get(queue.getQueuePath()) == null) {
        existingQueues.add(queue);
      }
    }

    for (CSQueue queue: existingQueues.getQueues()) {
      if (newQueues.get(queue.getQueuePath()) == null && !(
          queue instanceof AutoCreatedLeafQueue && conf
              .isAutoCreateChildQueueEnabled(
                queue.getParent().getQueuePath()))) {
        existingQueues.remove(queue);
      }
    }

  }

  @VisibleForTesting
  /**
   * Set the acls for the queues.
   * @param authorizer the yarnAuthorizationProvider
   * @param queues the queues
   * @throws IOException if fails to set queue acls
   */
  public static void setQueueAcls(YarnAuthorizationProvider authorizer,
      AppPriorityACLsManager appPriorityACLManager, CSQueueStore queues)
      throws IOException {
    List<Permission> permissions = new ArrayList<>();
    for (CSQueue queue : queues.getQueues()) {
      AbstractCSQueue csQueue = (AbstractCSQueue) queue;
      permissions.add(
          new Permission(csQueue.getPrivilegedEntity(), csQueue.getACLs()));

      if (queue instanceof LeafQueue) {
        LeafQueue lQueue = (LeafQueue) queue;

        // Clear Priority ACLs first since reinitialize also call same.
        appPriorityACLManager.clearPriorityACLs(lQueue.getQueuePath());
        appPriorityACLManager.addPrioirityACLs(lQueue.getPriorityACLs(),
            lQueue.getQueuePath());
      }
    }
    authorizer.setPermission(permissions,
        UserGroupInformation.getCurrentUser());
  }

  /**
   * Check that the String provided in input is the name of an existing,
   * LeafQueue, if successful returns the queue.
   *
   * @param queue the queue name
   * @return the LeafQueue
   * @throws YarnException if the queue does not exist or the queue
   *           is not the type of LeafQueue.
   */
  public LeafQueue getAndCheckLeafQueue(String queue) throws YarnException {
    CSQueue ret = this.getQueue(queue);
    if (ret == null) {
      throw new YarnException("The specified Queue: " + queue
          + " doesn't exist");
    }
    if (!(ret instanceof LeafQueue)) {
      throw new YarnException("The specified Queue: " + queue
          + " is not a Leaf Queue.");
    }
    return (LeafQueue) ret;
  }

  /**
   * Get the default priority of the queue.
   * @param queueName the queue name
   * @return the default priority of the queue
   */
  public Priority getDefaultPriorityForQueue(String queueName) {
    Queue queue = getQueue(queueName);
    if (null == queue || null == queue.getDefaultApplicationPriority()) {
      // Return with default application priority
      return Priority.newInstance(CapacitySchedulerConfiguration
          .DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    }
    return Priority.newInstance(queue.getDefaultApplicationPriority()
        .getPriority());
  }

  /**
   * Get a map of queueToLabels.
   * @return the map of queueToLabels
   */
  private Map<String, Set<String>> getQueueToLabels() {
    Map<String, Set<String>> queueToLabels = new HashMap<>();
    for (CSQueue queue :  getQueues().values()) {
      queueToLabels.put(queue.getQueuePath(), queue.getAccessibleNodeLabels());
    }
    return queueToLabels;
  }

  @Private
  public QueueStateManager<CSQueue, CapacitySchedulerConfiguration>
      getQueueStateManager() {
    return this.queueStateManager;
  }
}
