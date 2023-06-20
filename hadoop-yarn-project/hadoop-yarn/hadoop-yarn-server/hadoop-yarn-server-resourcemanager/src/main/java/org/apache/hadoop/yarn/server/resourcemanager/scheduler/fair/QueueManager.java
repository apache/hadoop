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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Maintains a list of queues as well as scheduling parameters for each queue,
 * such as guaranteed share allocations, from the fair scheduler config file.
 */
@Private
@Unstable
public class QueueManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueueManager.class.getName());

  private final class IncompatibleQueueRemovalTask {

    private final String queueToCreate;
    private final FSQueueType queueType;

    private IncompatibleQueueRemovalTask(String queueToCreate,
        FSQueueType queueType) {
      this.queueToCreate = queueToCreate;
      this.queueType = queueType;
    }

    private void execute() {
      Boolean removed =
          removeEmptyIncompatibleQueues(queueToCreate, queueType).orElse(null);
      if (Boolean.TRUE.equals(removed)) {
        FSQueue queue = getQueue(queueToCreate, true, queueType, false, null);
        if (queue != null &&
            // if queueToCreate is present in the allocation config, set it
            // to static
            scheduler.allocConf.configuredQueues.values().stream()
            .anyMatch(s -> s.contains(queueToCreate))) {
          queue.setDynamic(false);
        }
      }
      if (!Boolean.FALSE.equals(removed)) {
        incompatibleQueuesPendingRemoval.remove(this);
      }
    }
  }

  public static final String ROOT_QUEUE = "root";
  
  private final FairScheduler scheduler;

  private final Collection<FSLeafQueue> leafQueues = 
      new CopyOnWriteArrayList<>();
  private final Map<String, FSQueue> queues = new HashMap<>();
  private Set<IncompatibleQueueRemovalTask> incompatibleQueuesPendingRemoval =
      new HashSet<>();
  private FSParentQueue rootQueue;

  public QueueManager(FairScheduler scheduler) {
    this.scheduler = scheduler;
  }
  
  public FSParentQueue getRootQueue() {
    return rootQueue;
  }

  public void initialize() {
    // Policies of root and default queue are set to
    // SchedulingPolicy.DEFAULT_POLICY since the allocation file hasn't been
    // loaded yet.
    rootQueue = new FSParentQueue("root", scheduler, null);
    rootQueue.setDynamic(false);
    queues.put(rootQueue.getName(), rootQueue);

    // Recursively reinitialize to propagate queue properties
    rootQueue.reinit(true);
  }

  /**
   * Get a leaf queue by name, creating it if the create param is
   * <code>true</code> and the queue does not exist.
   * If the queue is not or can not be a leaf queue, i.e. it already exists as
   * a parent queue, or one of the parents in its name is already a leaf queue,
   * <code>null</code> is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   * @param name name of the queue
   * @param create <code>true</code> if the queue must be created if it does
   *               not exist, <code>false</code> otherwise
   * @return the leaf queue or <code>null</code> if the queue cannot be found
   */
  public FSLeafQueue getLeafQueue(String name, boolean create) {
    return getLeafQueue(name, create, null, true);
  }

  /**
   * Get a leaf queue by name, creating it if the create param is
   * <code>true</code> and the queue does not exist.
   * If the queue is not or can not be a leaf queue, i.e. it already exists as
   * a parent queue, or one of the parents in its name is already a leaf queue,
   * <code>null</code> is returned.
   *
   * If the application will be assigned to the queue if the applicationId is
   * not <code>null</code>
   * @param name name of the queue
   * @param create <code>true</code> if the queue must be created if it does
   *               not exist, <code>false</code> otherwise
   * @param applicationId the application ID to assign to the queue
   * @return the leaf queue or <code>null</code> if teh queue cannot be found
   */
  public FSLeafQueue getLeafQueue(String name, boolean create,
                                  ApplicationId applicationId) {
    return getLeafQueue(name, create, applicationId, true);
  }

  private FSLeafQueue getLeafQueue(String name, boolean create,
                                   ApplicationId applicationId,
                                   boolean recomputeSteadyShares) {
    FSQueue queue = getQueue(name, create, FSQueueType.LEAF,
        recomputeSteadyShares, applicationId);
    if (queue instanceof FSParentQueue) {
      return null;
    }
    return (FSLeafQueue) queue;
  }

  /**
   * Remove a leaf queue if empty.
   * @param name name of the queue
   * @return true if queue was removed or false otherwise
   */
  public boolean removeLeafQueue(String name) {
    name = ensureRootPrefix(name);
    return !Boolean.FALSE.equals(
        removeEmptyIncompatibleQueues(name, FSQueueType.PARENT).orElse(null));
  }


  /**
   * Get a parent queue by name, creating it if the create param is
   * <code>true</code> and the queue does not exist.
   * If the queue is not or can not be a parent queue, i.e. it already exists
   * as a leaf queue, or one of the parents in its name is already a leaf
   * queue, <code>null</code> is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   * @param name name of the queue
   * @param create <code>true</code> if the queue must be created if it does
   *               not exist, <code>false</code> otherwise
   * @return the parent queue or <code>null</code> if the queue cannot be found
   */
  public FSParentQueue getParentQueue(String name, boolean create) {
    return getParentQueue(name, create, true);
  }

  /**
   * Get a parent queue by name, creating it if the create param is
   * <code>true</code> and the queue does not exist.
   * If the queue is not or can not be a parent queue, i.e. it already exists
   * as a leaf queue, or one of the parents in its name is already a leaf
   * queue, <code>null</code> is returned.
   *
   * The root part of the name is optional, so a queue underneath the root
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root
   * could be referred to as just "parent1.queue2".
   * @param name name of the queue
   * @param create <code>true</code> if the queue must be created if it does
   *               not exist, <code>false</code> otherwise
   * @param recomputeSteadyShares <code>true</code> if the steady fair share
   *                              should be recalculated when a queue is added,
   *                              <code>false</code> otherwise
   * @return the parent queue or <code>null</code> if the queue cannot be found
   */
  public FSParentQueue getParentQueue(String name, boolean create,
      boolean recomputeSteadyShares) {
    FSQueue queue = getQueue(name, create, FSQueueType.PARENT,
        recomputeSteadyShares, null);
    if (queue instanceof FSLeafQueue) {
      return null;
    }
    return (FSParentQueue) queue;
  }

  private FSQueue getQueue(String name, boolean create, FSQueueType queueType,
      boolean recomputeSteadyShares, ApplicationId applicationId) {
    boolean recompute = recomputeSteadyShares;
    name = ensureRootPrefix(name);
    FSQueue queue;
    synchronized (queues) {
      queue = queues.get(name);
      if (queue == null && create) {
        // if the queue doesn't exist,create it and return
        queue = createQueue(name, queueType);
      } else {
        recompute = false;
      }
      // At this point the queue exists and we need to assign the app if to the
      // but only to a leaf queue
      if (applicationId != null && queue instanceof FSLeafQueue) {
        ((FSLeafQueue)queue).addAssignedApp(applicationId);
      }
    }
    // Don't recompute if it is an existing queue or no change was made
    if (recompute && queue != null) {
      rootQueue.recomputeSteadyShares();
    }
    return queue;
  }

  /**
   * Create a leaf or parent queue based on what is specified in
   * {@code queueType} and place it in the tree. Create any parents that don't
   * already exist.
   * 
   * @return the created queue, if successful or null if not allowed (one of the
   * parent queues in the queue name is already a leaf queue)
   */
  @VisibleForTesting
  FSQueue createQueue(String name, FSQueueType queueType) {
    List<String> newQueueNames = new ArrayList<>();
    FSParentQueue parent = buildNewQueueList(name, newQueueNames);
    FSQueue queue = null;

    if (parent != null) {
      // Now that we know everything worked out, make all the queues
      // and add them to the map.
      queue = createNewQueues(queueType, parent, newQueueNames);
    }

    return queue;
  }

  /**
   * Compile a list of all parent queues of the given queue name that do not
   * already exist. The queue names will be added to the {@code newQueueNames}
   * list. The list will be in order of increasing queue depth. The first
   * element of the list will be the parent closest to the root. The last
   * element added will be the queue to be created. This method returns the
   * deepest parent that does exist.
   *
   * @param name the fully qualified name of the queue to create
   * @param newQueueNames the list to which to add non-existent queues
   * @return the deepest existing parent queue
   */
  private FSParentQueue buildNewQueueList(String name,
      List<String> newQueueNames) {
    newQueueNames.add(name);
    int sepIndex = name.length();
    FSParentQueue parent = null;

    // Move up the queue tree until we reach one that exists.
    while (sepIndex != -1) {
      int prevSepIndex = sepIndex;
      sepIndex = name.lastIndexOf('.', sepIndex-1);
      String node = name.substring(sepIndex+1, prevSepIndex);
      if (!isQueueNameValid(node)) {
        throw new InvalidQueueNameException("Illegal node name at offset " +
            (sepIndex+1) + " for queue name " + name);
      }

      String curName = name.substring(0, sepIndex);
      FSQueue queue = queues.get(curName);

      if (queue == null) {
        newQueueNames.add(0, curName);
      } else {
        if (queue instanceof FSParentQueue) {
          parent = (FSParentQueue)queue;
        }

        // If the queue isn't a parent queue, parent will still be null when
        // we break

        break;
      }
    }

    return parent;
  }

  /**
   * Create all queues in the {@code newQueueNames} list. The list must be in
   * order of increasing depth. All but the last element in the list will be
   * created as parent queues. The last element will be created as the type
   * specified by the {@code queueType} parameter. The first queue will be
   * created as a child of the {@code topParent} queue. All subsequent queues
   * will be created as a child of the previously created queue.
   *
   * @param queueType the type of the last queue to create
   * @param topParent the parent of the first queue to create
   * @param newQueueNames the list of queues to create
   * @return the last queue created
   */
  private FSQueue createNewQueues(FSQueueType queueType,
      FSParentQueue topParent, List<String> newQueueNames) {
    AllocationConfiguration queueConf = scheduler.getAllocationConfiguration();
    Iterator<String> i = newQueueNames.iterator();
    FSParentQueue parent = topParent;
    FSQueue queue = null;

    while (i.hasNext()) {
      FSParentQueue newParent = null;
      String queueName = i.next();

      // Check if child policy is allowed
      SchedulingPolicy childPolicy = scheduler.getAllocationConfiguration().
          getSchedulingPolicy(queueName);
      if (!parent.getPolicy().isChildPolicyAllowed(childPolicy)) {
        LOG.error("Can't create queue '" + queueName + "'," +
                "the child scheduling policy is not allowed by parent queue!");
        return null;
      }

      // Only create a leaf queue at the very end
      if (!i.hasNext() && (queueType != FSQueueType.PARENT)) {
        FSLeafQueue leafQueue = new FSLeafQueue(queueName, scheduler, parent);
        leafQueues.add(leafQueue);
        queue = leafQueue;
      } else {
        if (childPolicy instanceof FifoPolicy) {
          LOG.error("Can't create queue '" + queueName + "', since "
              + FifoPolicy.NAME + " is only for leaf queues.");
          return null;
        }
        newParent = new FSParentQueue(queueName, scheduler, parent);
        queue = newParent;
      }

      parent.addChildQueue(queue);
      setChildResourceLimits(parent, queue, queueConf);
      queues.put(queue.getName(), queue);

      // If we just created a leaf node, the newParent is null, but that's OK
      // because we only create a leaf node in the very last iteration.
      parent = newParent;
    }

    return queue;
  }

  /**
   * For the given child queue, set the max resources based on the
   * parent queue's default child resource settings. This method assumes that
   * the child queue is ad hoc and hence does not do any safety checks around
   * overwriting existing max resource settings.
   *
   * @param parent the parent queue
   * @param child the child queue
   * @param queueConf the {@link AllocationConfiguration}
   */
  private void setChildResourceLimits(FSParentQueue parent, FSQueue child,
          AllocationConfiguration queueConf) {
    Map<FSQueueType, Set<String>> configuredQueues =
        queueConf.getConfiguredQueues();

    // Ad hoc queues do not exist in the configured queues map
    if (!configuredQueues.get(FSQueueType.LEAF).contains(child.getName()) &&
        !configuredQueues.get(FSQueueType.PARENT).contains(child.getName())) {
      // For ad hoc queues, set their max resource allocations based on
      // their parents' default child settings.
      ConfigurableResource maxChild = parent.getMaxChildQueueResource();

      if (maxChild != null) {
        child.setMaxShare(maxChild);
      }
    }
  }

  /**
   * Make way for the given queue if possible, by removing incompatible
   * queues with no apps in them. Incompatibility could be due to
   * (1) queueToCreate being currently a parent but needs to change to leaf
   * (2) queueToCreate being currently a leaf but needs to change to parent
   * (3) an existing leaf queue in the ancestry of queueToCreate.
   * 
   * We will never remove the root queue or the default queue in this way.
   *
   * @return Optional.of(Boolean.TRUE)  if there was an incompatible queue that
   *                                    has been removed,
   *         Optional.of(Boolean.FALSE) if there was an incompatible queue that
   *                                    have not be removed,
   *         Optional.empty()           if there is no incompatible queue.
   */
  private Optional<Boolean> removeEmptyIncompatibleQueues(String queueToCreate,
      FSQueueType queueType) {
    queueToCreate = ensureRootPrefix(queueToCreate);

    // Ensure queueToCreate is not root and doesn't
    // have the default queue in its ancestry.
    if (queueToCreate.equals(ROOT_QUEUE) ||
        queueToCreate.startsWith(
            ROOT_QUEUE + "." + YarnConfiguration.DEFAULT_QUEUE_NAME + ".")) {
      return Optional.empty();
    }

    FSQueue queue = queues.get(queueToCreate);
    // Queue exists already.
    if (queue != null) {
      if (queue instanceof FSLeafQueue) {
        if (queueType == FSQueueType.LEAF) {
          return Optional.empty();
        }
        // remove incompatibility since queue is a leaf currently
        // needs to change to a parent.
        return Optional.of(removeQueueIfEmpty(queue));
      } else {
        if (queueType == FSQueueType.PARENT) {
          return Optional.empty();
        }
        // If it's an existing parent queue and needs to change to leaf, 
        // remove it if it's empty.
        return Optional.of(removeQueueIfEmpty(queue));
      }
    }

    // Queue doesn't exist already. Check if the new queue would be created
    // under an existing leaf queue. If so, try removing that leaf queue.
    int sepIndex = queueToCreate.length();
    sepIndex = queueToCreate.lastIndexOf('.', sepIndex-1);
    while (sepIndex != -1) {
      String prefixString = queueToCreate.substring(0, sepIndex);
      FSQueue prefixQueue = queues.get(prefixString);
      if (prefixQueue != null && prefixQueue instanceof FSLeafQueue) {
        return Optional.of(removeQueueIfEmpty(prefixQueue));
      }
      sepIndex = queueToCreate.lastIndexOf('.', sepIndex-1);
    }
    return Optional.empty();
  }

  /**
   * Removes all empty dynamic queues (including empty dynamic parent queues).
   */
  public void removeEmptyDynamicQueues() {
    synchronized (queues) {
      Set<FSParentQueue> parentQueuesToCheck = new HashSet<>();
      for (FSQueue queue : getQueues()) {
        if (queue.isDynamic() && queue.getChildQueues().isEmpty()) {
          boolean removed = removeQueueIfEmpty(queue);
          if (removed && queue.getParent().isDynamic()) {
            parentQueuesToCheck.add(queue.getParent());
          }
        }
      }
      while (!parentQueuesToCheck.isEmpty()) {
        FSParentQueue queue = parentQueuesToCheck.iterator().next();
        if (queue.isEmpty()) {
          removeQueue(queue);
          if (queue.getParent().isDynamic()) {
            parentQueuesToCheck.add(queue.getParent());
          }
        }
        parentQueuesToCheck.remove(queue);
      }
    }
  }

  /**
   * Re-checking incompatible queues that could not be removed earlier due to
   * not being empty, and removing those that became empty.
   */
  public void removePendingIncompatibleQueues() {
    synchronized (queues) {
      for (IncompatibleQueueRemovalTask removalTask :
          ImmutableSet.copyOf(incompatibleQueuesPendingRemoval)) {
        removalTask.execute();
      }
    }
  }

  /**
   * Remove the queue if it and its descendents are all empty.
   * @param queue
   * @return true if removed, false otherwise
   */
  private boolean removeQueueIfEmpty(FSQueue queue) {
    if (queue.isEmpty()) {
      removeQueue(queue);
      return true;
    }
    return false;
  }
  
  /**
   * Remove a queue and all its descendents.
   */
  private void removeQueue(FSQueue queue) {
    synchronized (queues) {
      if (queue instanceof FSLeafQueue) {
        leafQueues.remove(queue);
      } else {
        for (FSQueue childQueue:queue.getChildQueues()) {
          removeQueue(childQueue);
        }
      }
      queues.remove(queue.getName());
      FSParentQueue parent = queue.getParent();
      parent.removeChildQueue(queue);
    }
  }
  
  /**
   * Gets a queue by name.
   * @param name queue name.
   * @return queue objects, FSQueue.
   */
  public FSQueue getQueue(String name) {
    name = ensureRootPrefix(name);
    synchronized (queues) {
      return queues.get(name);
    }
  }

  /**
   * Return whether a queue exists already.
   *
   * @param name queue name.
   * @return Returns true if the queue exists,
   * otherwise returns false.
   */
  public boolean exists(String name) {
    name = ensureRootPrefix(name);
    synchronized (queues) {
      return queues.containsKey(name);
    }
  }
  
  /**
   * Get a collection of all leaf queues.
   * @return a collection of all leaf queues.
   */
  public Collection<FSLeafQueue> getLeafQueues() {
    synchronized (queues) {
      return leafQueues;
    }
  }
  
  /**
   * Get a collection of all queues.
   * @return a collection of all queues.
   */
  public Collection<FSQueue> getQueues() {
    synchronized (queues) {
      return ImmutableList.copyOf(queues.values());
    }
  }
  
  private static String ensureRootPrefix(String name) {
    if (!name.startsWith(ROOT_QUEUE + ".") && !name.equals(ROOT_QUEUE)) {
      name = ROOT_QUEUE + "." + name;
    }
    return name;
  }
  
  public void updateAllocationConfiguration(AllocationConfiguration queueConf) {
    // Create leaf queues and the parent queues in a leaf's
    // ancestry if they do not exist
    synchronized (queues) {
      // Verify and set scheduling policies for existing queues before creating
      // any queue, since we need parent policies to determine if we can create
      // its children.
      if (!rootQueue.verifyAndSetPolicyFromConf(queueConf)) {
        LOG.error("Setting scheduling policies for existing queues failed!");
      }

      ensureQueueExistsAndIsCompatibleAndIsStatic(queueConf, FSQueueType.LEAF);

      // At this point all leaves and 'parents with
      // at least one child' would have been created.
      // Now create parents with no configured leaf.
      ensureQueueExistsAndIsCompatibleAndIsStatic(queueConf,
          FSQueueType.PARENT);
    }

    // Initialize all queues recursively
    rootQueue.reinit(true);
    // Update steady fair shares for all queues
    rootQueue.recomputeSteadyShares();
  }

  private void ensureQueueExistsAndIsCompatibleAndIsStatic(
      AllocationConfiguration queueConf, FSQueueType queueType) {
    for (String name : queueConf.getConfiguredQueues().get(queueType)) {
      Boolean removed =
          removeEmptyIncompatibleQueues(name, queueType).orElse(null);
      if (Boolean.FALSE.equals(removed)) {
        incompatibleQueuesPendingRemoval.add(
            new IncompatibleQueueRemovalTask(name, queueType));
      } else {
        FSQueue queue = getQueue(name, true, queueType, false, null);
        if (queue != null) {
          queue.setDynamic(false);
        }
      }
    }
  }

  /**
   * Setting a set of queues to dynamic.
   * @param queueNames The names of the queues to be set to dynamic
   */
  protected void setQueuesToDynamic(Set<String> queueNames) {
    synchronized (queues) {
      for (String queueName : queueNames) {
        queues.get(queueName).setDynamic(true);
      }
    }
  }

  /**
   * Check whether queue name is valid,
   * return true if it is valid, otherwise return false.
   */
  @VisibleForTesting
  boolean isQueueNameValid(String node) {
    // use the same white space trim as in QueueMetrics() otherwise things fail
    // This needs to trim additional Unicode whitespace characters beyond what
    // the built-in JDK methods consider whitespace. See YARN-5272.
    return !node.isEmpty() &&
        node.equals(FairSchedulerUtilities.trimQueueName(node));
  }
}
