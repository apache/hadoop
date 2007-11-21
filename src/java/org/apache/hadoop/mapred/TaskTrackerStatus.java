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

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

/**************************************************
 * A TaskTrackerStatus is a MapReduce primitive.  Keeps
 * info on a TaskTracker.  The JobTracker maintains a set
 * of the most recent TaskTrackerStatus objects for each
 * unique TaskTracker it knows about.
 *
 **************************************************/
class TaskTrackerStatus implements Writable {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (TaskTrackerStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new TaskTrackerStatus(); }
       });
  }

  String trackerName;
  String host;
  int httpPort;
  int failures;
  List<TaskStatus> taskReports;
    
  volatile long lastSeen;
  private int maxMapTasks;
  private int maxReduceTasks;
    
  /**
   */
  public TaskTrackerStatus() {
    taskReports = new ArrayList<TaskStatus>();
  }

  /**
   */
  public TaskTrackerStatus(String trackerName, String host, 
                           int httpPort, List<TaskStatus> taskReports, 
                           int failures, int maxMapTasks,
                           int maxReduceTasks) {
    this.trackerName = trackerName;
    this.host = host;
    this.httpPort = httpPort;

    this.taskReports = new ArrayList<TaskStatus>(taskReports);
    this.failures = failures;
    this.maxMapTasks = maxMapTasks;
    this.maxReduceTasks = maxReduceTasks;
  }

  /**
   */
  public String getTrackerName() {
    return trackerName;
  }
  /**
   */
  public String getHost() {
    return host;
  }

  /**
   * Get the port that this task tracker is serving http requests on.
   * @return the http port
   */
  public int getHttpPort() {
    return httpPort;
  }
    
  /**
   * Get the number of tasks that have failed on this tracker.
   * @return The number of failed tasks
   */
  public int getFailures() {
    return failures;
  }
    
  /**
   * Get the current tasks at the TaskTracker.
   * Tasks are tracked by a {@link TaskStatus} object.
   * 
   * @return a list of {@link TaskStatus} representing 
   *         the current tasks at the TaskTracker.
   */
  public List<TaskStatus> getTaskReports() {
    return taskReports;
  }
    
  /**
   * Return the current MapTask count
   */
  public int countMapTasks() {
    int mapCount = 0;
    for (Iterator it = taskReports.iterator(); it.hasNext();) {
      TaskStatus ts = (TaskStatus) it.next();
      TaskStatus.State state = ts.getRunState();
      if (ts.getIsMap() &&
          ((state == TaskStatus.State.RUNNING) ||
           (state == TaskStatus.State.UNASSIGNED))) {
        mapCount++;
      }
    }
    return mapCount;
  }

  /**
   * Return the current ReduceTask count
   */
  public int countReduceTasks() {
    int reduceCount = 0;
    for (Iterator it = taskReports.iterator(); it.hasNext();) {
      TaskStatus ts = (TaskStatus) it.next();
      TaskStatus.State state = ts.getRunState();
      if ((!ts.getIsMap()) &&
          ((state == TaskStatus.State.RUNNING) ||  
           (state == TaskStatus.State.UNASSIGNED))) {
        reduceCount++;
      }
    }
    return reduceCount;
  }

  /**
   */
  public long getLastSeen() {
    return lastSeen;
  }
  /**
   */
  public void setLastSeen(long lastSeen) {
    this.lastSeen = lastSeen;
  }

  /**
   * Get the maximum concurrent tasks for this node.  (This applies
   * per type of task - a node with maxTasks==1 will run up to 1 map
   * and 1 reduce concurrently).
   * @return maximum tasks this node supports
   */
  public int getMaxMapTasks() {
    return maxMapTasks;
  }
  public int getMaxReduceTasks() {
    return maxReduceTasks;
  }  
  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, trackerName);
    UTF8.writeString(out, host);
    out.writeInt(httpPort);
    out.writeInt(failures);
    out.writeInt(maxMapTasks);
    out.writeInt(maxReduceTasks);
    out.writeInt(taskReports.size());
    for (TaskStatus taskStatus : taskReports) {
      TaskStatus.writeTaskStatus(out, taskStatus);
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.trackerName = UTF8.readString(in);
    this.host = UTF8.readString(in);
    this.httpPort = in.readInt();
    this.failures = in.readInt();
    this.maxMapTasks = in.readInt();
    this.maxReduceTasks = in.readInt();
    taskReports.clear();
    int numTasks = in.readInt();
    for (int i = 0; i < numTasks; i++) {
      taskReports.add(TaskStatus.readTaskStatus(in));
    }
  }
}
