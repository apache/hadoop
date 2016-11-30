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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerQueueManager;

/**
 *
 * Context of the Queues in Capacity Scheduler.
 *
 */
@Private
@Unstable
public class CapacitySchedulerQueueManager implements SchedulerQueueManager<
    CSQueue, CapacitySchedulerConfiguration>{

  private static final Log LOG = LogFactory.getLog(
      CapacitySchedulerQueueManager.class);

  static final Comparator<CSQueue> NON_PARTITIONED_QUEUE_COMPARATOR =
      new Comparator<CSQueue>() {
    @Override
    public int compare(CSQueue q1, CSQueue q2) {
      if (q1.getUsedCapacity() < q2.getUsedCapacity()) {
        return -1;
      } else if (q1.getUsedCapacity() > q2.getUsedCapacity()) {
        return 1;
      }

      return q1.getQueuePath().compareTo(q2.getQueuePath());
    }
  };

  static final PartitionedQueueComparator PARTITIONED_QUEUE_COMPARATOR =
      new PartitionedQueueComparator();

  static class QueueHook {
    public CSQueue hook(CSQueue queue) {
      return queue;
    }
  }

  private static final QueueHook NOOP = new QueueHook();
  private CapacitySchedulerContext csContext;
  private final YarnAuthorizationProvider authorizer;
  private final Map<String, CSQueue> queues = new ConcurrentHashMap<>();
  private CSQueue root;
  private final RMNodeLabelsManager labelManager;

  /**
   * Construct the service.
   * @param conf the configuration
   * @param labelManager the labelManager
   */
  public CapacitySchedulerQueueManager(Configuration conf,
      RMNodeLabelsManager labelManager) {
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
    this.labelManager = labelManager;
  }

  @Override
  public CSQueue getRootQueue() {
    return this.root;
  }

  @Override
  public Map<String, CSQueue> getQueues() {
    return queues;
  }

  @Override
  public void removeQueue(String queueName) {
    this.queues.remove(queueName);
  }

  @Override
  public void addQueue(String queueName, CSQueue queue) {
    this.queues.put(queueName, queue);
  }

  @Override
  public CSQueue getQueue(String queueName) {
    return queues.get(queueName);
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
    setQueueAcls(authorizer, queues);
    labelManager.reinitializeQueueLabels(getQueueToLabels());
    LOG.info("Initialized root queue " + root);
  }

  @Override
  public void reinitializeQueues(CapacitySchedulerConfiguration newConf)
      throws IOException {
    // Parse new queues
    Map<String, CSQueue> newQueues = new HashMap<>();
    CSQueue newRoot =  parseQueue(this.csContext, newConf, null,
        CapacitySchedulerConfiguration.ROOT, newQueues, queues, NOOP);

    // Ensure all existing queues are still present
    validateExistingQueues(queues, newQueues);

    // Add new queues
    addNewQueues(queues, newQueues);

    // Re-configure queues
    root.reinitialize(newRoot, this.csContext.getClusterResource());

    setQueueAcls(authorizer, queues);

    // Re-calculate headroom for active applications
    Resource clusterResource = this.csContext.getClusterResource();
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    labelManager.reinitializeQueueLabels(getQueueToLabels());
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
      CSQueue parent, String queueName, Map<String, CSQueue> queues,
      Map<String, CSQueue> oldQueues,
      QueueHook hook) throws IOException {
    CSQueue queue;
    String fullQueueName =
        (parent == null) ? queueName
            : (parent.getQueuePath() + "." + queueName);
    String[] childQueueNames = conf.getQueues(fullQueueName);
    boolean isReservableQueue = conf.isReservable(fullQueueName);
    if (childQueueNames == null || childQueueNames.length == 0) {
      if (null == parent) {
        throw new IllegalStateException(
            "Queue configuration missing child queue names for " + queueName);
      }
      // Check if the queue will be dynamically managed by the Reservation
      // system
      if (isReservableQueue) {
        queue =
            new PlanQueue(csContext, queueName, parent,
                oldQueues.get(queueName));
      } else {
        queue =
            new LeafQueue(csContext, queueName, parent,
                oldQueues.get(queueName));

        // Used only for unit tests
        queue = hook.hook(queue);
      }
    } else {
      if (isReservableQueue) {
        throw new IllegalStateException(
            "Only Leaf Queues can be reservable for " + queueName);
      }
      ParentQueue parentQueue =
          new ParentQueue(csContext, queueName, parent,
              oldQueues.get(queueName));

      // Used only for unit tests
      queue = hook.hook(parentQueue);

      List<CSQueue> childQueues = new ArrayList<>();
      for (String childQueueName : childQueueNames) {
        CSQueue childQueue =
            parseQueue(csContext, conf, queue, childQueueName,
              queues, oldQueues, hook);
        childQueues.add(childQueue);
      }
      parentQueue.setChildQueues(childQueues);
    }

    if (queue instanceof LeafQueue && queues.containsKey(queueName)
        && queues.get(queueName) instanceof LeafQueue) {
      throw new IOException("Two leaf queues were named " + queueName
          + ". Leaf queue names must be distinct");
    }
    queues.put(queueName, queue);

    LOG.info("Initialized queue: " + queue);
    return queue;
  }

  /**
   * Ensure all existing queues are present. Queues cannot be deleted
   * @param queues existing queues
   * @param newQueues new queues
   */
  private void validateExistingQueues(
      Map<String, CSQueue> queues, Map<String, CSQueue> newQueues)
      throws IOException {
    // check that all static queues are included in the newQueues list
    for (Map.Entry<String, CSQueue> e : queues.entrySet()) {
      if (!(e.getValue() instanceof ReservationQueue)) {
        String queueName = e.getKey();
        CSQueue oldQueue = e.getValue();
        CSQueue newQueue = newQueues.get(queueName);
        if (null == newQueue) {
          throw new IOException(queueName + " cannot be found during refresh!");
        } else if (!oldQueue.getQueuePath().equals(newQueue.getQueuePath())) {
          throw new IOException(queueName + " is moved from:"
              + oldQueue.getQueuePath() + " to:" + newQueue.getQueuePath()
              + " after refresh, which is not allowed.");
        }
      }
    }
  }

  /**
   * Add the new queues (only) to our list of queues...
   * ... be careful, do not overwrite existing queues.
   * @param queues the existing queues
   * @param newQueues the new queues
   */
  private void addNewQueues(
      Map<String, CSQueue> queues, Map<String, CSQueue> newQueues) {
    for (Map.Entry<String, CSQueue> e : newQueues.entrySet()) {
      String queueName = e.getKey();
      CSQueue queue = e.getValue();
      if (!queues.containsKey(queueName)) {
        queues.put(queueName, queue);
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
      Map<String, CSQueue> queues) throws IOException {
    List<Permission> permissions = new ArrayList<>();
    for (CSQueue queue : queues.values()) {
      AbstractCSQueue csQueue = (AbstractCSQueue) queue;
      permissions.add(
          new Permission(csQueue.getPrivilegedEntity(), csQueue.getACLs()));
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
      queueToLabels.put(queue.getQueueName(), queue.getAccessibleNodeLabels());
    }
    return queueToLabels;
  }
}
