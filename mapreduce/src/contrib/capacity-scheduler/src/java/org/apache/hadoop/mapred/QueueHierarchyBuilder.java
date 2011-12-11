/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Properties;

/**
 * Hierarchy builder for the CapacityScheduler.
 * 
 */
class QueueHierarchyBuilder {

  static final Log LOG = LogFactory.getLog(QueueHierarchyBuilder.class);
  
  QueueHierarchyBuilder() {
  }

  /**
   * Create a new {@link AbstractQueue}s-hierarchy and set the new queue
   * properties in the passed {@link CapacitySchedulerConf}.
   * 
   * @param rootChildren
   * @param schedConf
   * @return the root {@link AbstractQueue} of the newly created hierarchy.
   */
  AbstractQueue createHierarchy(List<JobQueueInfo> rootChildren,
      CapacitySchedulerConf schedConf) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Root queues defined : ");
      for (JobQueueInfo q : rootChildren) {
        LOG.debug(q.getQueueName());
      }
    }

    // Create the root.
    AbstractQueue newRootAbstractQueue = createRootAbstractQueue();

    // Create the complete hierarchy rooted at newRootAbstractQueue
    createHierarchy(newRootAbstractQueue, rootChildren, schedConf);

    // Distribute any un-configured capacities
    newRootAbstractQueue.distributeUnConfiguredCapacity();

    return newRootAbstractQueue;
  }

  static final String TOTAL_CAPACITY_OVERFLOWN_MSG =
      "The cumulative capacity for the queues (%s) at the same level "
          + "has overflown over 100%% at %f%%";

  /**
   * Recursively create a complete AbstractQueues-hierarchy. 'Parent' is the
   * root of the hierarchy. 'Children' is the immediate children of the 'parent'
   * and may in-turn be the parent of further child queues. Any JobQueueInfo
   * which doesn't have any more children is used to create a JobQueue in the
   * AbstractQueues-hierarchy and every other AbstractQueue is used to create a
   * ContainerQueue.
   * 
   * <p>
   * 
   * While creating the hierarchy, we make sure at each level that the total
   * capacity of all the children at that level doesn't cross 100%
   * 
   * @param parent the queue that will be the root of the new hierarchy.
   * @param children the immediate children of the 'parent'
   * @param schedConfig Configuration object to which the new queue
   *          properties are set. The new queue properties are set with key
   *          names obtained by expanding the queue-names to reflect the whole
   *          hierarchy.
   */
  private void createHierarchy(AbstractQueue parent,
      List<JobQueueInfo> children, CapacitySchedulerConf schedConfig) {
    //check if children have further childrens.
    if (children != null && !children.isEmpty()) {
      float totalCapacity = 0.0f;
      for (JobQueueInfo qs : children) {

        //Check if this child has any more children.
        List<JobQueueInfo> childQueues = qs.getChildren();

        if (childQueues != null && childQueues.size() > 0) {
          //generate a new ContainerQueue and recursively
          //create hierarchy.
          AbstractQueue cq =
              new ContainerQueue(parent, loadContext(qs.getProperties(),
                  qs.getQueueName(), schedConfig));
          //update totalCapacity
          totalCapacity += cq.qsc.getCapacityPercent();
          LOG.info("Created a ContainerQueue " + qs.getQueueName()
              + " and added it as a child to " + parent.getName());
          //create child hiearchy
          createHierarchy(cq, childQueues, schedConfig);
        } else {
          //if not this is a JobQueue.

          //create a JobQueue.
          AbstractQueue jq =
              new JobQueue(parent, loadContext(qs.getProperties(),
                  qs.getQueueName(), schedConfig));
          totalCapacity += jq.qsc.getCapacityPercent();
          LOG.info("Created a jobQueue " + qs.getQueueName()
              + " and added it as a child to " + parent.getName());
        }
      }

      //check for totalCapacity at each level , the total for children
      //shouldn't cross 100.

      if (totalCapacity > 100.0) {
        StringBuilder childQueueNames = new StringBuilder();
        for (JobQueueInfo child : children) {
          childQueueNames.append(child.getQueueName()).append(",");
        }
        throw new IllegalArgumentException(String.format(
            TOTAL_CAPACITY_OVERFLOWN_MSG,
            childQueueNames.toString().substring(0,
                childQueueNames.toString().length() - 1),
            Float.valueOf(totalCapacity)));
      }
    }
  }

  /**
   * Create a new {@link QueueSchedulingContext} from the given props. Also set
   * these properties in the passed scheduler configuration object.
   * 
   * @param props Properties to be set in the {@link QueueSchedulingContext}
   * @param queueName Queue name
   * @param schedConf Scheduler configuration object to set the properties in.
   * @return the generated {@link QueueSchedulingContext} object
   */
  private QueueSchedulingContext loadContext(Properties props,
      String queueName, CapacitySchedulerConf schedConf) {
    schedConf.setProperties(queueName,props);
    float capacity = schedConf.getCapacity(queueName);
    float stretchCapacity = schedConf.getMaxCapacity(queueName);
    if (capacity == -1.0) {
      LOG.info("No capacity specified for queue " + queueName);
    }
    int ulMin = schedConf.getMinimumUserLimitPercent(queueName);
    // create our QSC and add to our hashmap
    QueueSchedulingContext qsi = new QueueSchedulingContext(
      queueName, capacity, stretchCapacity, ulMin
    );
    qsi.setSupportsPriorities(
      schedConf.isPrioritySupported(
        queueName));
    return qsi;
  }

  /**
   * Create an {@link AbstractQueue} with an empty
   * {@link QueueSchedulingContext}. Can be used to as the root queue to create
   * {@link AbstractQueue} hierarchies.
   * 
   * @return a root {@link AbstractQueue}
   */
  static AbstractQueue createRootAbstractQueue() {
    QueueSchedulingContext rootContext =
        new QueueSchedulingContext("", 100, -1, -1);
    AbstractQueue root = new ContainerQueue(null, rootContext);
    return root;
  }
}
