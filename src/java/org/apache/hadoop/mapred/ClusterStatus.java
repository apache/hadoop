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
import org.apache.hadoop.io.WritableUtils;

/**
 * Status information on the current state of the Map-Reduce cluster.
 * 
 * <p><code>ClusterStatus</code> provides clients with information such as:
 * <ol>
 *   <li>
 *   Size of the cluster. 
 *   </li>
 *   <li>
 *   Task capacity of the cluster. 
 *   </li>
 *   <li>
 *   The number of currently running map & reduce tasks.
 *   </li>
 *   <li>
 *   State of the <code>JobTracker</code>.
 *   </li>
 * </ol></p>
 * 
 * <p>Clients can query for the latest <code>ClusterStatus</code>, via 
 * {@link JobClient#getClusterStatus()}.</p>
 * 
 * @see JobClient
 */
public class ClusterStatus implements Writable {

  private int task_trackers;
  private int map_tasks;
  private int reduce_tasks;
  private int max_map_tasks;
  private int max_reduce_tasks;
  private JobTracker.State state;

  ClusterStatus() {}
  
  /**
   * Construct a new cluster status.
   * 
   * @param trackers no. of tasktrackers in the cluster
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param max the maximum no. of tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   */
  ClusterStatus(int trackers, int maps, int reduces, int maxMaps,
                int maxReduces, JobTracker.State state) {
    task_trackers = trackers;
    map_tasks = maps;
    reduce_tasks = reduces;
    max_map_tasks = maxMaps;
    max_reduce_tasks = maxReduces;
    this.state = state;
  }
  

  /**
   * Get the number of task trackers in the cluster.
   * 
   * @return the number of task trackers in the cluster.
   */
  public int getTaskTrackers() {
    return task_trackers;
  }
  
  /**
   * Get the number of currently running map tasks in the cluster.
   * 
   * @return the number of currently running map tasks in the cluster.
   */
  public int getMapTasks() {
    return map_tasks;
  }
  
  /**
   * Get the number of currently running reduce tasks in the cluster.
   * 
   * @return the number of currently running reduce tasks in the cluster.
   */
  public int getReduceTasks() {
    return reduce_tasks;
  }
  
  /**
   * Get the maximum capacity for running map tasks in the cluster.
   * 
   * @return the maximum capacity for running map tasks in the cluster.
   */
  public int getMaxMapTasks() {
    return max_map_tasks;
  }

  /**
   * Get the maximum capacity for running reduce tasks in the cluster.
   * 
   * @return the maximum capacity for running reduce tasks in the cluster.
   */
  public int getMaxReduceTasks() {
    return max_reduce_tasks;
  }
  
  /**
   * Get the current state of the <code>JobTracker</code>, 
   * as {@link JobTracker.State}
   * 
   * @return the current state of the <code>JobTracker</code>.
   */
  public JobTracker.State getJobTrackerState() {
    return state;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(task_trackers);
    out.writeInt(map_tasks);
    out.writeInt(reduce_tasks);
    out.writeInt(max_map_tasks);
    out.writeInt(max_reduce_tasks);
    WritableUtils.writeEnum(out, state);
  }

  public void readFields(DataInput in) throws IOException {
    task_trackers = in.readInt();
    map_tasks = in.readInt();
    reduce_tasks = in.readInt();
    max_map_tasks = in.readInt();
    max_reduce_tasks = in.readInt();
    state = WritableUtils.readEnum(in, JobTracker.State.class);
  }

}
