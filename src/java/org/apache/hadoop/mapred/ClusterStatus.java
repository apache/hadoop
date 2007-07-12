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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;

/**
 * Summarizes the size and current state of the cluster.
 */
public class ClusterStatus implements Writable {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ClusterStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new ClusterStatus(); }
       });
  }

  private int task_trackers;
  private int map_tasks;
  private int reduce_tasks;
  private int max_tasks;

  ClusterStatus() {}
  
  ClusterStatus(int trackers, int maps, int reduces, int max) {
    task_trackers = trackers;
    map_tasks = maps;
    reduce_tasks = reduces;
    max_tasks = max;
  }
  

  /**
   * The number of task trackers in the cluster.
   */
  public int getTaskTrackers() {
    return task_trackers;
  }
  
  /**
   * The number of currently running map tasks.
   */
  public int getMapTasks() {
    return map_tasks;
  }
  
  /**
   * The number of current running reduce tasks.
   */
  public int getReduceTasks() {
    return reduce_tasks;
  }
  
  /**
   * The maximum capacity for running tasks in the cluster.
   */
  public int getMaxTasks() {
    return max_tasks;
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(task_trackers);
    out.writeInt(map_tasks);
    out.writeInt(reduce_tasks);
    out.writeInt(max_tasks);
  }

  public void readFields(DataInput in) throws IOException {
    task_trackers = in.readInt();
    map_tasks = in.readInt();
    reduce_tasks = in.readInt();
    max_tasks = in.readInt();
  }

}
