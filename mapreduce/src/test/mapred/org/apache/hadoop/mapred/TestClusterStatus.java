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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.TaskType;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestClusterStatus extends TestCase {

  private static String[] trackers = new String[] { "tracker_tracker1:1000",
      "tracker_tracker2:1000", "tracker_tracker3:1000" };
  private static JobTracker jobTracker;
  private static int mapSlotsPerTracker = 4;
  private static int reduceSlotsPerTracker = 2;
  private static MiniMRCluster mr;
  private static Cluster cluster;
  // heartbeat responseId. increment this after sending a heartbeat
  private static short responseId = 1;
  
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestClusterStatus.class)) {
      protected void setUp() throws Exception {
      
        mr = new MiniMRCluster(0, "file:///", 1);
        jobTracker = mr.getJobTrackerRunner().getJobTracker();
        for (String tracker : trackers) {
          FakeObjectUtilities.establishFirstContact(jobTracker, tracker);
        }
        cluster = new Cluster(mr.createJobConf());
      }

      protected void tearDown() throws Exception {
        cluster.close();
        mr.shutdown();
      }
    };
    return setup;
  }
  
  private TaskTrackerStatus getTTStatus(String trackerName,
      List<TaskStatus> taskStatuses) {
    return new TaskTrackerStatus(trackerName, 
      JobInProgress.convertTrackerNameToHostName(trackerName), 0,
      taskStatuses, 0, mapSlotsPerTracker, reduceSlotsPerTracker);
  }
  
  public void testClusterMetrics() throws IOException, InterruptedException {
    assertEquals("tasktracker count doesn't match", trackers.length,
      cluster.getClusterStatus().getTaskTrackerCount());
    
    List<TaskStatus> list = new ArrayList<TaskStatus>();

    // create a map task status, which uses 2 slots. 
    int mapSlotsPerTask = 2;
    TaskStatus ts = TaskStatus.createTaskStatus(true, 
      new TaskAttemptID("jt", 1, TaskType.MAP, 0, 0), 0.0f, mapSlotsPerTask,
      TaskStatus.State.RUNNING, "", "", trackers[0], 
      TaskStatus.Phase.MAP, null);
    list.add(ts);
    
    // create a reduce task status, which uses 1 slot.
    int reduceSlotsPerTask = 1;
    ts = TaskStatus.createTaskStatus(false, 
      new TaskAttemptID("jt", 1, TaskType.REDUCE, 0, 0), 0.0f,
      reduceSlotsPerTask,
      TaskStatus.State.RUNNING, "", "", trackers[0], 
      TaskStatus.Phase.REDUCE, null);
    list.add(ts);
    
    // create TaskTrackerStatus and send heartbeats
    TaskTrackerStatus[] status = new TaskTrackerStatus[trackers.length];
    status[0] = getTTStatus(trackers[0], list);
    status[1] = getTTStatus(trackers[1], new ArrayList<TaskStatus>());
    status[2] = getTTStatus(trackers[2], new ArrayList<TaskStatus>());
    for (int i = 0; i< trackers.length; i++) {
      FakeObjectUtilities.sendHeartBeat(jobTracker, status[i], false, 
        trackers[i], responseId);
    }
    responseId++;
    // assert ClusterMetrics
    ClusterMetrics metrics = cluster.getClusterStatus();
    assertEquals("occupied map slots do not match", mapSlotsPerTask,
      metrics.getOccupiedMapSlots());
    assertEquals("occupied reduce slots do not match", reduceSlotsPerTask,
      metrics.getOccupiedReduceSlots());
    assertEquals("map slot capacities do not match",
      mapSlotsPerTracker * trackers.length,
      metrics.getMapSlotCapacity());
    assertEquals("reduce slot capacities do not match",
      reduceSlotsPerTracker * trackers.length,
      metrics.getReduceSlotCapacity());
    
    // assert the values in ClusterStatus also
    assertEquals("running map tasks do not match", 1,
      jobTracker.getClusterStatus().getMapTasks());
    assertEquals("running reduce tasks do not match", 1,
      jobTracker.getClusterStatus().getReduceTasks());
    assertEquals("map slot capacities do not match",
      mapSlotsPerTracker * trackers.length,
      jobTracker.getClusterStatus().getMaxMapTasks());
    assertEquals("reduce slot capacities do not match",
      reduceSlotsPerTracker * trackers.length,
      jobTracker.getClusterStatus().getMaxReduceTasks());
    cluster.close();
  }
}
