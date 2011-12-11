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

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

/**
 * Composite class for Queue hierarchy.
 */
class ContainerQueue extends AbstractQueue {

  //List of immediate children for this container queue.
  //Duplicate childrens are not allowed.
  private  List<AbstractQueue> children;
  public ContainerQueue(AbstractQueue parent , QueueSchedulingContext qsc) {
    super(parent,qsc);
  }

  /**
   * Update current contexts and update children's contexts
   * @param mapClusterCapacity
   * @param reduceClusterCapacity
   */
  @Override
  public void update(int mapClusterCapacity, int reduceClusterCapacity) {
    super.update(mapClusterCapacity,reduceClusterCapacity);
    updateChildrenContext();
  }
  
  /**
   * set normalized capacity values for children.
   * and update children.
   */
  private void updateChildrenContext() {
    for (AbstractQueue queue : children) {
      int normalizedMapClusterCapacity = qsc.getMapTSC().getCapacity();
      int normalizedReduceClusterCapacity = qsc.getReduceTSC().getCapacity();

      //update children context,
      // normalize mapClusterCapacity,reduceClusterCapacity to the current.
      queue.update(
        normalizedMapClusterCapacity, normalizedReduceClusterCapacity);

      //update current TaskSchedulingContext information
      //At parent level , these information is cumulative of all
      //children's TSC values.
      //Typically JobQueue's TSC would change first . so as of now
      //parental level values would be stale unless we call update , which
      //happens incase of new heartbeat.
      //This behaviour shuold be fine , as before assignTask we first update
      //then sort the whole hierarchy
      qsc.getMapTSC().update(queue.getQueueSchedulingContext().getMapTSC());
      qsc.getReduceTSC().update(queue.getQueueSchedulingContext().getReduceTSC());
    }
  }

  
  /**
   * @param queueComparator
   */
  @Override
  public void sort(Comparator queueComparator) {
    //sort immediate children
    Collections.sort(children, queueComparator);

    //recursive sort all children.    
    for (AbstractQueue child : children) {
      child.sort(queueComparator);
    }
  }

  /**
   * Returns the sorted order of the leaf level queues.
   * @return
   */
  @Override
  public List<AbstractQueue> getDescendentJobQueues() {
    List<AbstractQueue> l = new ArrayList<AbstractQueue>();

    for (AbstractQueue child : children) {
      l.addAll(child.getDescendentJobQueues());
    }
    return l;
  }

  @Override
  List<AbstractQueue> getDescendantContainerQueues() {
    List<AbstractQueue> l = new ArrayList<AbstractQueue>();
    for (AbstractQueue child : this.getChildren()) {
      if (child.getChildren() != null && child.getChildren().size() > 0) {
        l.add(child);
        l.addAll(child.getDescendantContainerQueues());
      }
    }
    return l;
  }

  /**
   * Used for test only.
   * @return
   */
  @Override
  List<AbstractQueue> getChildren() {
    return children;
  }

  @Override
  public void addChild(AbstractQueue queue) {
    if (children == null) {
      children = new ArrayList<AbstractQueue>();
    }
    if(children.contains(queue)) {
      LOG.warn(" The queue " + queue.getName() + " already " +
        "exists hence ignoring  the current value ");
      return;
    }
    this.children.add(queue);
  }


  /**
   *
   */
  @Override
  void distributeUnConfiguredCapacity() {
    List<AbstractQueue> unConfiguredQueues = new ArrayList<AbstractQueue>();
    float totalCapacity = 0;
    for (AbstractQueue q : children) {
      if (q.qsc.getCapacityPercent() == -1) {
        //Add into unConfigured queue.
        unConfiguredQueues.add(q);
      } else {
        //If capacity is set , then add that to totalCapacity.
        LOG.info(" the capacity percent of the queue " + q.getName() + "  is " +
          "" + q.qsc.getCapacityPercent());
        totalCapacity += q.qsc.getCapacityPercent();

        //As we already know current Capacity percent of this queue
        //make children distribute unconfigured Capacity.
        q.distributeUnConfiguredCapacity();
      }
    }

    if (!unConfiguredQueues.isEmpty()) {
      LOG.info("Total capacity to be distributed among the others are  " +
        "" + (100 - totalCapacity));      

      //We have list of queues at this level which are unconfigured.
      //100 - totalCapacity is the capacity remaining.
      //Divide it equally among all the un configured queues.      
      float capacityShare = (100 - totalCapacity) / unConfiguredQueues.size();

      //We dont have to check for 100 - totalCapacity being -ve , as
      //we already do it while loading.
      for (AbstractQueue q : unConfiguredQueues) {
        if(q.qsc.getMaxCapacityPercent() > 0) {
          if (q.qsc.getMaxCapacityPercent() < capacityShare) {
            throw new IllegalStateException(
              " Capacity share (" + capacityShare + ")for unconfigured queue " +
                q.getName() +
                " is greater than its maximum-capacity percentage " +
                q.qsc.getMaxCapacityPercent());
          }
        }
        q.qsc.setCapacityPercent(capacityShare);
        LOG.info("Capacity share for un configured queue " + q.getName() + "" +
          " is " + capacityShare);
        //we have q's capacity now.
        //make children also distribute it among themselves.
        q.distributeUnConfiguredCapacity();
      }
    }
  }
}
