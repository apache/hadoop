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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.xml.parsers.ParserConfigurationException;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.xml.sax.SAXException;

import com.google.common.annotations.VisibleForTesting;
import java.util.Iterator;
import java.util.Set;

/**
 * Maintains a list of queues as well as scheduling parameters for each queue,
 * such as guaranteed share allocations, from the fair scheduler config file.
 */
@Private
@Unstable
public class QueueManager {
  public static final Log LOG = LogFactory.getLog(
    QueueManager.class.getName());

  public static final String ROOT_QUEUE = "root";
  
  private final FairScheduler scheduler;

  private final Collection<FSLeafQueue> leafQueues = 
      new CopyOnWriteArrayList<FSLeafQueue>();
  private final Map<String, FSQueue> queues = new HashMap<String, FSQueue>();
  private FSParentQueue rootQueue;

  public QueueManager(FairScheduler scheduler) {
    this.scheduler = scheduler;
  }
  
  public FSParentQueue getRootQueue() {
    return rootQueue;
  }

  public void initialize(Configuration conf) throws IOException,
      SAXException, AllocationConfigurationException, ParserConfigurationException {
    // Policies of root and default queue are set to
    // SchedulingPolicy.DEFAULT_POLICY since the allocation file hasn't been
    // loaded yet.
    rootQueue = new FSParentQueue("root", scheduler, null);
    queues.put(rootQueue.getName(), rootQueue);

    // Create the default queue
    getLeafQueue(YarnConfiguration.DEFAULT_QUEUE_NAME, true);
    // Recursively reinitialize to propagate queue properties
    rootQueue.reinit(true);
  }

  /**
   * Get a leaf queue by name, creating it if the create param is true and is necessary.
   * If the queue is not or can not be a leaf queue, i.e. it already exists as a
   * parent queue, or one of the parents in its name is already a leaf queue,
   * null is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   */
  public FSLeafQueue getLeafQueue(String name, boolean create) {
    return getLeafQueue(name, create, true);
  }

  public FSLeafQueue getLeafQueue(
      String name,
      boolean create,
      boolean recomputeSteadyShares) {
    FSQueue queue = getQueue(
        name,
        create,
        FSQueueType.LEAF,
        recomputeSteadyShares
    );
    if (queue instanceof FSParentQueue) {
      return null;
    }
    return (FSLeafQueue) queue;
  }

  /**
   * Remove a leaf queue if empty
   * @param name name of the queue
   * @return true if queue was removed or false otherwise
   */
  public boolean removeLeafQueue(String name) {
    name = ensureRootPrefix(name);
    return removeEmptyIncompatibleQueues(name, FSQueueType.PARENT);
  }


  /**
   * Get a parent queue by name, creating it if the create param is true and is necessary.
   * If the queue is not or can not be a parent queue, i.e. it already exists as a
   * leaf queue, or one of the parents in its name is already a leaf queue,
   * null is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   */
  public FSParentQueue getParentQueue(String name, boolean create) {
    return getParentQueue(name, create, true);
  }

  public FSParentQueue getParentQueue(
      String name,
      boolean create,
      boolean recomputeSteadyShares) {
    FSQueue queue = getQueue(
        name,
        create,
        FSQueueType.PARENT,
        recomputeSteadyShares
    );
    if (queue instanceof FSLeafQueue) {
      return null;
    }
    return (FSParentQueue) queue;
  }

  private FSQueue getQueue(
      String name,
      boolean create,
      FSQueueType queueType,
      boolean recomputeSteadyShares) {
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
    }
    if (recompute) {
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
        LOG.error("Can't create queue '" + queueName + "'.");
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
  void setChildResourceLimits(FSParentQueue parent, FSQueue child,
      AllocationConfiguration queueConf) {
    Map<FSQueueType, Set<String>> configuredQueues =
        queueConf.getConfiguredQueues();

    // Ad hoc queues do not exist in the configured queues map
    if (!configuredQueues.get(FSQueueType.LEAF).contains(child.getName()) &&
        !configuredQueues.get(FSQueueType.PARENT).contains(child.getName())) {
      // For ad hoc queues, set their max reource allocations based on
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
   * @return true if we can create queueToCreate or it already exists.
   */
  private boolean removeEmptyIncompatibleQueues(String queueToCreate,
      FSQueueType queueType) {
    queueToCreate = ensureRootPrefix(queueToCreate);

    // Ensure queueToCreate is not root and doesn't have the default queue in its
    // ancestry.
    if (queueToCreate.equals(ROOT_QUEUE) ||
        queueToCreate.startsWith(
            ROOT_QUEUE + "." + YarnConfiguration.DEFAULT_QUEUE_NAME + ".")) {
      return false;
    }

    FSQueue queue = queues.get(queueToCreate);
    // Queue exists already.
    if (queue != null) {
      if (queue instanceof FSLeafQueue) {
        if (queueType == FSQueueType.LEAF) {
          // if queue is already a leaf then return true
          return true;
        }
        // remove incompatibility since queue is a leaf currently
        // needs to change to a parent.
        return removeQueueIfEmpty(queue);
      } else {
        if (queueType == FSQueueType.PARENT) {
          return true;
        }
        // If it's an existing parent queue and needs to change to leaf, 
        // remove it if it's empty.
        return removeQueueIfEmpty(queue);
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
        return removeQueueIfEmpty(prefixQueue);
      }
      sepIndex = queueToCreate.lastIndexOf('.', sepIndex-1);
    }
    return true;
  }

  /**
   * Remove the queue if it and its descendents are all empty.
   * @param queue
   * @return true if removed, false otherwise
   */
  private boolean removeQueueIfEmpty(FSQueue queue) {
    if (isEmpty(queue)) {
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
   * Returns true if there are no applications, running or not, in the given
   * queue or any of its descendents.
   */
  protected boolean isEmpty(FSQueue queue) {
    if (queue instanceof FSLeafQueue) {
      FSLeafQueue leafQueue = (FSLeafQueue)queue;
      return queue.getNumRunnableApps() == 0 &&
          leafQueue.getNumNonRunnableApps() == 0;
    } else {
      for (FSQueue child : queue.getChildQueues()) {
        if (!isEmpty(child)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Gets a queue by name.
   */
  public FSQueue getQueue(String name) {
    name = ensureRootPrefix(name);
    synchronized (queues) {
      return queues.get(name);
    }
  }

  /**
   * Return whether a queue exists already.
   */
  public boolean exists(String name) {
    name = ensureRootPrefix(name);
    synchronized (queues) {
      return queues.containsKey(name);
    }
  }
  
  /**
   * Get a collection of all leaf queues
   */
  public Collection<FSLeafQueue> getLeafQueues() {
    synchronized (queues) {
      return leafQueues;
    }
  }
  
  /**
   * Get a collection of all queues
   */
  public Collection<FSQueue> getQueues() {
    synchronized (queues) {
      return ImmutableList.copyOf(queues.values());
    }
  }
  
  private String ensureRootPrefix(String name) {
    if (!name.startsWith(ROOT_QUEUE + ".") && !name.equals(ROOT_QUEUE)) {
      name = ROOT_QUEUE + "." + name;
    }
    return name;
  }
  
  public void updateAllocationConfiguration(AllocationConfiguration queueConf) {
    // Create leaf queues and the parent queues in a leaf's ancestry if they do not exist
    synchronized (queues) {
      // Verify and set scheduling policies for existing queues before creating
      // any queue, since we need parent policies to determine if we can create
      // its children.
      if (!rootQueue.verifyAndSetPolicyFromConf(queueConf)) {
        LOG.error("Setting scheduling policies for existing queues failed!");
      }

      for (String name : queueConf.getConfiguredQueues().get(
              FSQueueType.LEAF)) {
        if (removeEmptyIncompatibleQueues(name, FSQueueType.LEAF)) {
          getLeafQueue(name, true, false);
        }
      }
      // At this point all leaves and 'parents with
      // at least one child' would have been created.
      // Now create parents with no configured leaf.
      for (String name : queueConf.getConfiguredQueues().get(
          FSQueueType.PARENT)) {
        if (removeEmptyIncompatibleQueues(name, FSQueueType.PARENT)) {
          getParentQueue(name, true, false);
        }
      }
    }

    // Initialize all queues recursively
    rootQueue.reinit(true);
    // Update steady fair shares for all queues
    rootQueue.recomputeSteadyShares();
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
