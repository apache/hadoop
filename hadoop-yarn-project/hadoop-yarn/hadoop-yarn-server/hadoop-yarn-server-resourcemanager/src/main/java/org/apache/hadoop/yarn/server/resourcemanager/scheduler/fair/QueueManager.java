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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.xml.sax.SAXException;

/**
 * Maintains a list of queues as well as scheduling parameters for each queue,
 * such as guaranteed share allocations, from the fair scheduler config file.
 * 
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
    rootQueue = new FSParentQueue("root", scheduler, null);
    queues.put(rootQueue.getName(), rootQueue);
    
    // Create the default queue
    getLeafQueue(YarnConfiguration.DEFAULT_QUEUE_NAME, true);
  }
  
  /**
   * Get a queue by name, creating it if the create param is true and is necessary.
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
    name = ensureRootPrefix(name);
    synchronized (queues) {
      FSQueue queue = queues.get(name);
      if (queue == null && create) {
        FSLeafQueue leafQueue = createLeafQueue(name);
        if (leafQueue == null) {
          return null;
        }
        queue = leafQueue;
      } else if (queue instanceof FSParentQueue) {
        return null;
      }
      return (FSLeafQueue)queue;
    }
  }
  
  /**
   * Creates a leaf queue and places it in the tree. Creates any
   * parents that don't already exist.
   * 
   * @return
   *    the created queue, if successful. null if not allowed (one of the parent
   *    queues in the queue name is already a leaf queue)
   */
  private FSLeafQueue createLeafQueue(String name) {
    List<String> newQueueNames = new ArrayList<String>();
    newQueueNames.add(name);
    int sepIndex = name.length();
    FSParentQueue parent = null;

    // Move up the queue tree until we reach one that exists.
    while (sepIndex != -1) {
      sepIndex = name.lastIndexOf('.', sepIndex-1);
      FSQueue queue;
      String curName = null;
      curName = name.substring(0, sepIndex);
      queue = queues.get(curName);

      if (queue == null) {
        newQueueNames.add(curName);
      } else {
        if (queue instanceof FSParentQueue) {
          parent = (FSParentQueue)queue;
          break;
        } else {
          return null;
        }
      }
    }
    
    // At this point, parent refers to the deepest existing parent of the
    // queue to create.
    // Now that we know everything worked out, make all the queues
    // and add them to the map.
    AllocationConfiguration queueConf = scheduler.getAllocationConfiguration();
    FSLeafQueue leafQueue = null;
    for (int i = newQueueNames.size()-1; i >= 0; i--) {
      String queueName = newQueueNames.get(i);
      if (i == 0) {
        // First name added was the leaf queue
        leafQueue = new FSLeafQueue(name, scheduler, parent);
        try {
          leafQueue.setPolicy(queueConf.getDefaultSchedulingPolicy());
        } catch (AllocationConfigurationException ex) {
          LOG.warn("Failed to set default scheduling policy "
              + queueConf.getDefaultSchedulingPolicy() + " on new leaf queue.", ex);
        }
        parent.addChildQueue(leafQueue);
        queues.put(leafQueue.getName(), leafQueue);
        leafQueues.add(leafQueue);
      } else {
        FSParentQueue newParent = new FSParentQueue(queueName, scheduler, parent);
        try {
          newParent.setPolicy(queueConf.getDefaultSchedulingPolicy());
        } catch (AllocationConfigurationException ex) {
          LOG.warn("Failed to set default scheduling policy "
              + queueConf.getDefaultSchedulingPolicy() + " on new parent queue.", ex);
        }
        parent.addChildQueue(newParent);
        queues.put(newParent.getName(), newParent);
        parent = newParent;
      }
    }
    
    return leafQueue;
  }

  /**
   * Make way for the given leaf queue if possible, by removing incompatible
   * queues with no apps in them. Incompatibility could be due to
   * (1) leafToCreate being currently being a parent, or (2) an existing leaf queue in
   * the ancestry of leafToCreate.
   * 
   * We will never remove the root queue or the default queue in this way.
   *
   * @return true if we can create leafToCreate or it already exists.
   */
  private boolean removeEmptyIncompatibleQueues(String leafToCreate) {
    leafToCreate = ensureRootPrefix(leafToCreate);

    // Ensure leafToCreate is not root and doesn't have the default queue in its
    // ancestry.
    if (leafToCreate.equals(ROOT_QUEUE) ||
        leafToCreate.startsWith(
            ROOT_QUEUE + "." + YarnConfiguration.DEFAULT_QUEUE_NAME + ".")) {
      return false;
    }

    FSQueue queue = queues.get(leafToCreate);
    // Queue exists already.
    if (queue != null) {
      if (queue instanceof FSLeafQueue) {
        // If it's an already existing leaf, we're ok.
        return true;
      } else {
        // If it's an existing parent queue, remove it if it's empty.
        return removeQueueIfEmpty(queue);
      }
    }

    // Queue doesn't exist already. Check if the new queue would be created
    // under an existing leaf queue. If so, try removing that leaf queue.
    int sepIndex = leafToCreate.length();
    sepIndex = leafToCreate.lastIndexOf('.', sepIndex-1);
    while (sepIndex != -1) {
      String prefixString = leafToCreate.substring(0, sepIndex);
      FSQueue prefixQueue = queues.get(prefixString);
      if (prefixQueue != null && prefixQueue instanceof FSLeafQueue) {
        return removeQueueIfEmpty(prefixQueue);
      }
      sepIndex = leafToCreate.lastIndexOf('.', sepIndex-1);
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
    if (queue instanceof FSLeafQueue) {
      leafQueues.remove(queue);
    } else {
      List<FSQueue> childQueues = queue.getChildQueues();
      while (!childQueues.isEmpty()) {
        removeQueue(childQueues.get(0));
      }
    }
    queues.remove(queue.getName());
    queue.getParent().getChildQueues().remove(queue);
  }
  
  /**
   * Returns true if there are no applications, running or not, in the given
   * queue or any of its descendents.
   */
  protected boolean isEmpty(FSQueue queue) {
    if (queue instanceof FSLeafQueue) {
      FSLeafQueue leafQueue = (FSLeafQueue)queue;
      return queue.getNumRunnableApps() == 0 &&
          leafQueue.getNonRunnableAppSchedulables().isEmpty();
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
    return queues.values();
  }
  
  private String ensureRootPrefix(String name) {
    if (!name.startsWith(ROOT_QUEUE + ".") && !name.equals(ROOT_QUEUE)) {
      name = ROOT_QUEUE + "." + name;
    }
    return name;
  }
  
  public void updateAllocationConfiguration(AllocationConfiguration queueConf) {
    // Make sure all queues exist
    for (String name : queueConf.getQueueNames()) {
      if (removeEmptyIncompatibleQueues(name)) {
        getLeafQueue(name, true);
      }
    }
    
    for (FSQueue queue : queues.values()) {
      // Update queue metrics
      FSQueueMetrics queueMetrics = queue.getMetrics();
      queueMetrics.setMinShare(queue.getMinShare());
      queueMetrics.setMaxShare(queue.getMaxShare());
      // Set scheduling policies
      try {
        SchedulingPolicy policy = queueConf.getSchedulingPolicy(queue.getName());
        policy.initialize(scheduler.getClusterCapacity());
        queue.setPolicy(policy);
      } catch (AllocationConfigurationException ex) {
        LOG.warn("Cannot apply configured scheduling policy to queue "
            + queue.getName(), ex);
      }
    }
  }
}
