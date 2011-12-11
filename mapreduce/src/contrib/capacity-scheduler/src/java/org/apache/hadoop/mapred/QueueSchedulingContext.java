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

import org.apache.hadoop.mapreduce.TaskType;

import java.util.Map;
import java.util.HashMap;

/**
 * ********************************************************************
 * Keeping track of scheduling information for queues
 * <p/>
 * We need to maintain scheduling information relevant to a queue (its
 * name, capacity, etc), along with information specific to
 * each kind of task, Map or Reduce (num of running tasks, pending
 * tasks etc).
 * <p/>
 * This scheduling information is used to decide how to allocate
 * tasks, redistribute capacity, etc.
 * <p/>
 * A QueueSchedulingContext(qsc) object represents scheduling information for
 * a queue. 
 * ********************************************************************
 */
public class QueueSchedulingContext {

    //Name of this queue
    private String queueName;

    //Get the maximum capacity of this queue for running map tasks
    // in the cluster.
    private int mapCapacity;

    //Get the maximum capacity of this queue for running reduce tasks 
    // in the cluster.
    private int reduceCapacity;

    /**
     * capacity(%) is set in the config as
     * mapred.capacity-scheduler.queue.<queue-name>.capacity"
     * Percentage of the number of slots in the cluster that are
     * to be available for jobs in this queue.
     */
    private float capacityPercent = 0;

  /**
   * maxCapacityStretch(%) is set in config as
   * mapred.capacity-scheduler.queue.<queue-name>.maximum-capacity
   * maximum-capacity-stretch defines a limit beyond which a sub-queue
   * cannot use the capacity of its parent queue.
   */
    private float maxCapacityPercent = -1;

    /**
     * to handle user limits, we need to know how many users have jobs in
     * the queue.
     */
    private Map<String, Integer> numJobsByUser = new HashMap<String, Integer>();

    /**
     * min value of user limit (same for all users)
     */
    private int ulMin;
  
    // whether the queue supports priorities
    //default is false
    private boolean supportsPriorities = false;

    //No of waiting jobs.
    private int numOfWaitingJobs = 0;

    //State of mapCapacity
    private int prevMapCapacity = 0;

    //State of reduceCapacity
    private int prevReduceCapacity = 0;


    /**
     * We keep a TaskSchedulingInfo object for each kind of task we support
     */
    private TaskSchedulingContext mapTSC;
    private TaskSchedulingContext reduceTSC;

  QueueSchedulingContext(
    String queueName, float capacityPercent, float maxCapacityPercent,
    int ulMin) {
    this.setQueueName(queueName);
    this.setCapacityPercent(capacityPercent);
    this.setMaxCapacityPercent(maxCapacityPercent);
    this.setUlMin(ulMin);
    this.setMapTSC(new TaskSchedulingContext());
    this.setReduceTSC(new TaskSchedulingContext());
  }

  /**
     * return information about the queue
     *
     * @return a String representing the information about the queue.
     */
    @Override
    public String toString() {
      // We print out the queue information first, followed by info
      // on map and reduce tasks and job info
      StringBuffer sb = new StringBuffer();
      sb.append("Queue configuration\n");
      sb.append("Capacity Percentage: ");
      sb.append(getCapacityPercent());
      sb.append("%\n");
      sb.append(String.format("User Limit: %d%s\n", getUlMin(), "%"));
      sb.append(
        String.format(
          "Priority Supported: %s\n",
          (supportsPriorities()) ?
            "YES" : "NO"));
      sb.append("-------------\n");

      sb.append("Map tasks\n");
      sb.append(getMapTSC().toString());
      sb.append("-------------\n");
      sb.append("Reduce tasks\n");
      sb.append(getReduceTSC().toString());
      sb.append("-------------\n");

      sb.append("Job info\n");
      sb.append(
        String.format(
          "Number of Waiting Jobs: %d\n",
          this.getNumOfWaitingJobs()));
      sb.append(
        String.format(
          "Number of users who have submitted jobs: %d\n",
          getNumJobsByUser().size()));
      return sb.toString();
    }

  String getQueueName() {
    return queueName;
  }

  void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  int getMapCapacity() {
    return mapCapacity;
  }

  void setMapCapacity(int mapCapacity) {
    this.mapCapacity = mapCapacity;
  }

  int getReduceCapacity() {
    return reduceCapacity;
  }

  void setReduceCapacity(int reduceCapacity) {
    this.reduceCapacity = reduceCapacity;
  }

  float getCapacityPercent() {
    return capacityPercent;
  }

  void setCapacityPercent(float capacityPercent) {
    this.capacityPercent = capacityPercent;
  }

  Map<String, Integer> getNumJobsByUser() {
    return numJobsByUser;
  }

  void setNumJobsByUser(Map<String, Integer> numJobsByUser) {
    this.numJobsByUser = numJobsByUser;
  }

  int getUlMin() {
    return ulMin;
  }

  void setUlMin(int ulMin) {
    this.ulMin = ulMin;
  }

  TaskSchedulingContext getMapTSC() {
    return mapTSC;
  }

  void setMapTSC(TaskSchedulingContext mapTSC) {
    this.mapTSC = mapTSC;
  }

  TaskSchedulingContext getReduceTSC() {
    return reduceTSC;
  }

  void setReduceTSC(TaskSchedulingContext reduceTSC) {
    this.reduceTSC = reduceTSC;
  }

  boolean supportsPriorities() {
    return supportsPriorities;
  }

  void setSupportsPriorities(boolean supportsPriorities) {
    this.supportsPriorities = supportsPriorities;
  }

  int getNumOfWaitingJobs() {
    return numOfWaitingJobs;
  }

  void setNumOfWaitingJobs(int numOfWaitingJobs) {
    this.numOfWaitingJobs = numOfWaitingJobs;
  }

  float getMaxCapacityPercent() {
    return maxCapacityPercent;
  }

  void setMaxCapacityPercent(float maxCapacityPercent) {
    this.maxCapacityPercent = maxCapacityPercent;
  }

  void updateContext(int mapClusterCapacity , int reduceClusterCapacity) {
    setMapCapacity(mapClusterCapacity);
    setReduceCapacity(reduceClusterCapacity);
    // if # of slots have changed since last time, update.
    // First, compute whether the total number of TT slots have changed
    // compute new capacities, if TT slots have changed
    if (getMapCapacity() != prevMapCapacity) {
      getMapTSC().setCapacity(
        (int)
          (getCapacityPercent() * getMapCapacity() / 100));

      //Check if max capacity percent is set for this queue.
      //if yes then set the maxcapacity for this queue.
      if (getMaxCapacityPercent() > 0) {
        getMapTSC().setMaxCapacity(
          (int) (getMaxCapacityPercent() * getMapCapacity() /
            100)
        );
      }
    }

    //REDUCES
    if (getReduceCapacity() != prevReduceCapacity) {
      getReduceTSC().setCapacity(
        (int)
          (getCapacityPercent() * getReduceCapacity() / 100));

      //set stretch capacity for reduce
      //check if max capacity percent is set for this QueueSchedulingContext.
      //if yes then set the maxCapacity for this JobQueue.
      if (getMaxCapacityPercent() > 0) {
        getReduceTSC().setMaxCapacity(
          (int) (getMaxCapacityPercent() * getReduceCapacity() /
            100));
      }
    }

    // reset running/pending tasks, tasks per user
    getMapTSC().resetTaskVars();
    getReduceTSC().resetTaskVars();
    // update stats on running jobs
    prevMapCapacity = getMapCapacity();
    prevReduceCapacity = getReduceCapacity();
  }
}
