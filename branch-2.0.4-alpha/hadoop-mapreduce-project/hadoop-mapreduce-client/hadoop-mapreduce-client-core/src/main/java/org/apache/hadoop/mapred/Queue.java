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
package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.authorize.AccessControlList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * A class for storing the properties of a job queue.
 */
class Queue implements Comparable<Queue>{

  private static final Log LOG = LogFactory.getLog(Queue.class);

  //Queue name
  private String name = null;

  //acls list
  private Map<String, AccessControlList> acls;

  //Queue State
  private QueueState state = QueueState.RUNNING;

  // An Object that can be used by schedulers to fill in
  // arbitrary scheduling information. The toString method
  // of these objects will be called by the framework to
  // get a String that can be displayed on UI.
  private Object schedulingInfo;

  private Set<Queue> children;

  private Properties props;

  /**
   * Default constructor is useful in creating the hierarchy.
   * The variables are populated using mutator methods.
   */
  Queue() {
    
  }

  /**
   * Create a job queue
   * @param name name of the queue
   * @param acls ACLs for the queue
   * @param state state of the queue
   */
  Queue(String name, Map<String, AccessControlList> acls, QueueState state) {
	  this.name = name;
	  this.acls = acls;
	  this.state = state;
  }
  
  /**
   * Return the name of the queue
   * 
   * @return name of the queue
   */
  String getName() {
    return name;
  }
  
  /**
   * Set the name of the queue
   * @param name name of the queue
   */
  void setName(String name) {
    this.name = name;
  }

  /**
   * Return the ACLs for the queue
   * 
   * The keys in the map indicate the operations that can be performed,
   * and the values indicate the list of users/groups who can perform
   * the operation.
   * 
   * @return Map containing the operations that can be performed and
   *          who can perform the operations.
   */
  Map<String, AccessControlList> getAcls() {
    return acls;
  }
  
  /**
   * Set the ACLs for the queue
   * @param acls Map containing the operations that can be performed and
   *          who can perform the operations.
   */
  void setAcls(Map<String, AccessControlList> acls) {
    this.acls = acls;
  }
  
  /**
   * Return the state of the queue.
   * @return state of the queue
   */
  QueueState getState() {
    return state;
  }
  
  /**
   * Set the state of the queue.
   * @param state state of the queue.
   */
  void setState(QueueState state) {
    this.state = state;
  }
  
  /**
   * Return the scheduling information for the queue
   * @return scheduling information for the queue.
   */
  Object getSchedulingInfo() {
    return schedulingInfo;
  }
  
  /**
   * Set the scheduling information from the queue.
   * @param schedulingInfo scheduling information for the queue.
   */
  void setSchedulingInfo(Object schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }

  /**
   * Copy the scheduling information from the sourceQueue into this queue
   * recursively.
   * 
   * @param sourceQueue
   */
  void copySchedulingInfo(Queue sourceQueue) {
    // First update the children queues recursively.
    Set<Queue> destChildren = getChildren();
    if (destChildren != null) {
      Iterator<Queue> itr1 = destChildren.iterator();
      Iterator<Queue> itr2 = sourceQueue.getChildren().iterator();
      while (itr1.hasNext()) {
        itr1.next().copySchedulingInfo(itr2.next());
      }
    }

    // Now, copy the information for the root-queue itself
    setSchedulingInfo(sourceQueue.getSchedulingInfo());
  }

  /**
   *
   */
  void addChild(Queue child) {
    if(children == null) {
      children = new TreeSet<Queue>();
    }

    children.add(child);
  }

  /**
   *
   * @return
   */
  Set<Queue> getChildren() {
    return children;
  }

  /**
   * 
   * @param props
   */
  void setProperties(Properties props) {
     this.props = props;
  }

  /**
   *
   * @return
   */
  Properties getProperties() {
    return this.props;
  }

  /**
   * This methods helps in traversing the
   * tree hierarchy.
   *
   * Returns list of all inner queues.i.e nodes which has children.
   * below this level.
   *
   * Incase of children being null , returns an empty map.
   * This helps in case of creating union of inner and leaf queues.
   * @return
   */
  Map<String,Queue> getInnerQueues() {
    Map<String,Queue> l = new HashMap<String,Queue>();

    //If no children , return empty set.
    //This check is required for root node.
    if(children == null) {
      return l;
    }

    //check for children if they are parent.
    for(Queue child:children) {
      //check if children are themselves parent add them
      if(child.getChildren() != null && child.getChildren().size() > 0) {
        l.put(child.getName(),child);
        l.putAll(child.getInnerQueues());
      }
    }
    return l;
  }

  /**
   * This method helps in maintaining the single
   * data structure across QueueManager.
   *
   * Now if we just maintain list of root queues we
   * should be done.
   *
   * Doesn't return null .
   * Adds itself if this is leaf node.
   * @return
   */
  Map<String,Queue> getLeafQueues() {
    Map<String,Queue> l = new HashMap<String,Queue>();
    if(children == null) {
      l.put(name,this);
      return l;
    }

    for(Queue child:children) {
      l.putAll(child.getLeafQueues());
    }
    return l;
  }


  @Override
  public int compareTo(Queue queue) {
    return name.compareTo(queue.getName());
  }
  
  @Override
  public boolean equals(Object o) {
    if(o == this) {
      return true;
    }
    if(! (o instanceof Queue)) {
      return false;
    }
    
    return ((Queue)o).getName().equals(name);
  }

  @Override
  public String toString() {
    return this.getName();
  }

  @Override
  public int hashCode() {
    return this.getName().hashCode();
  }

  /**
   * Return hierarchy of {@link JobQueueInfo} objects
   * under this Queue.
   *
   * @return JobQueueInfo[]
   */
  JobQueueInfo getJobQueueInfo() {
    JobQueueInfo queueInfo = new JobQueueInfo();
    queueInfo.setQueueName(name);
    LOG.debug("created jobQInfo " + queueInfo.getQueueName());
    queueInfo.setQueueState(state.getStateName());
    if (schedulingInfo != null) {
      queueInfo.setSchedulingInfo(schedulingInfo.toString());
    }

    if (props != null) {
      //Create deep copy of properties.
      Properties newProps = new Properties();
      for (Object key : props.keySet()) {
        newProps.setProperty(key.toString(), props.getProperty(key.toString()));
      }
      queueInfo.setProperties(newProps);
    }

    if (children != null && children.size() > 0) {
      List<JobQueueInfo> list = new ArrayList<JobQueueInfo>();
      for (Queue child : children) {
        list.add(child.getJobQueueInfo());
      }
      queueInfo.setChildren(list);
    }
    return queueInfo;
  }

  /**
   * For each node validate if current node hierarchy is same newState.
   * recursively check for child nodes.
   * 
   * @param newState
   * @return
   */
  boolean isHierarchySameAs(Queue newState) {
    if(newState == null) {
      return false;
    }
    //First check if names are equal
    if(!(name.equals(newState.getName())) ) {
      LOG.info(" current name " + name + " not equal to " + newState.getName());
      return false;
    }

    if (children == null || children.size() == 0) {
      if(newState.getChildren() != null && newState.getChildren().size() > 0) {
        LOG.info( newState + " has added children in refresh ");
        return false;
      }
    } else if(children.size() > 0) {
      //check for the individual children and then see if all of them
      //are updated.
      if (newState.getChildren() == null) {
        LOG.fatal("In the current state, queue " + getName() + " has "
            + children.size() + " but the new state has none!");
        return false;
      }
      int childrenSize = children.size();
      int newChildrenSize = newState.getChildren().size();
      if (childrenSize != newChildrenSize) {
        LOG.fatal("Number of children for queue " + newState.getName()
            + " in newState is " + newChildrenSize + " which is not equal to "
            + childrenSize + " in the current state.");
        return false;
      }
      //children are pre sorted as they are stored in treeset.
      //hence order shold be the same.
      Iterator<Queue> itr1 = children.iterator();
      Iterator<Queue> itr2 = newState.getChildren().iterator();

      while(itr1.hasNext()) {
        Queue q = itr1.next();
        Queue newq = itr2.next();
        if(! (q.isHierarchySameAs(newq)) ) {
          LOG.info(" Queue " + q.getName() + " not equal to " + newq.getName());
          return false;
        }
      }
    }
    return true;
  }
}
