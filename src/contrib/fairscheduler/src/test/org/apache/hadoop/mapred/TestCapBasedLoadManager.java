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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskStatus.State;

import junit.framework.TestCase;

/**
 * Exercise the canAssignMap and canAssignReduce methods in 
 * CapBasedLoadManager.
 */
public class TestCapBasedLoadManager extends TestCase {
  
  /**
   * Returns a running MapTaskStatus.
   */
  private TaskStatus getRunningMapTaskStatus() {
    TaskStatus ts = new MapTaskStatus();
    ts.setRunState(State.RUNNING);
    return ts;
  }

  /**
   * Returns a running ReduceTaskStatus.
   */
  private TaskStatus getRunningReduceTaskStatus() {
    TaskStatus ts = new ReduceTaskStatus();
    ts.setRunState(State.RUNNING);
    return ts;
  }
  
  /**
   * Returns a TaskTrackerStatus with the specified statistics. 
   * @param mapCap        The capacity of map tasks 
   * @param reduceCap     The capacity of reduce tasks
   * @param runningMap    The number of running map tasks
   * @param runningReduce The number of running reduce tasks
   */
  private TaskTrackerStatus getTaskTrackerStatus(int mapCap, int reduceCap, 
      int runningMap, int runningReduce) {
    List<TaskStatus> ts = new ArrayList<TaskStatus>();
    for (int i = 0; i < runningMap; i++) {
      ts.add(getRunningMapTaskStatus());
    }
    for (int i = 0; i < runningReduce; i++) {
      ts.add(getRunningReduceTaskStatus());
    }
    TaskTrackerStatus tracker = new TaskTrackerStatus("tracker", 
        "tracker_host", 1234, ts, 0, mapCap, reduceCap);
    return tracker;
  }

  /**
   * A single test of canAssignMap.
   */
  private void oneTestCanAssignMap(float maxDiff, int mapCap, int runningMap,
      int totalMapSlots, int totalRunnableMap, boolean expected) {
    
    CapBasedLoadManager manager = new CapBasedLoadManager();
    Configuration conf = new Configuration();
    conf.setFloat("mapred.fairscheduler.load.max.diff", maxDiff);
    manager.setConf(conf);
    
    TaskTrackerStatus ts = getTaskTrackerStatus(mapCap, 1, runningMap, 1);
    
    assertEquals( "When maxDiff=" + maxDiff + ", with totalRunnableMap=" 
        + totalRunnableMap + " and totalMapSlots=" + totalMapSlots
        + ", a tracker with runningMap=" + runningMap + " and mapCap="
        + mapCap + " should " + (expected ? "" : "not ")
        + "be able to take more Maps.",
        expected,
        manager.canAssignMap(ts, totalRunnableMap, totalMapSlots)
        );
  }
  
  
  /** 
   * Test canAssignMap method.
   */
  public void testCanAssignMap() {
    oneTestCanAssignMap(0.0f, 5, 0, 50, 1, true);
    oneTestCanAssignMap(0.0f, 5, 1, 50, 10, false);
    oneTestCanAssignMap(0.2f, 5, 1, 50, 10, true);
    oneTestCanAssignMap(0.0f, 5, 1, 50, 11, true);
    oneTestCanAssignMap(0.0f, 5, 2, 50, 11, false);
    oneTestCanAssignMap(0.3f, 5, 2, 50, 6, true);
    oneTestCanAssignMap(1.0f, 5, 5, 50, 50, false);
  }
  
  
  /**
   * A single test of canAssignReduce.
   */
  private void oneTestCanAssignReduce(float maxDiff, int ReduceCap,
      int runningReduce, int totalReduceSlots, int totalRunnableReduce,
      boolean expected) {
    
    CapBasedLoadManager manager = new CapBasedLoadManager();
    Configuration conf = new Configuration();
    conf.setFloat("mapred.fairscheduler.load.max.diff", maxDiff);
    manager.setConf(conf);
    
    TaskTrackerStatus ts = getTaskTrackerStatus(1, ReduceCap, 1,
        runningReduce);
    
    assertEquals( "When maxDiff=" + maxDiff + ", with totalRunnableReduce=" 
        + totalRunnableReduce + " and totalReduceSlots=" + totalReduceSlots
        + ", a tracker with runningReduce=" + runningReduce
        + " and ReduceCap=" + ReduceCap + " should "
        + (expected ? "" : "not ") + "be able to take more Reduces.",
        expected,
        manager.canAssignReduce(ts, totalRunnableReduce, totalReduceSlots)
        );
  }
    
  /** 
   * Test canAssignReduce method.
   */
  public void testCanAssignReduce() {
    oneTestCanAssignReduce(0.0f, 5, 0, 50, 1, true);
    oneTestCanAssignReduce(0.0f, 5, 1, 50, 10, false);
    oneTestCanAssignReduce(0.2f, 5, 1, 50, 10, true);
    oneTestCanAssignReduce(0.0f, 5, 1, 50, 11, true);
    oneTestCanAssignReduce(0.0f, 5, 2, 50, 11, false);
    oneTestCanAssignReduce(0.3f, 5, 2, 50, 6, true);
    oneTestCanAssignReduce(1.0f, 5, 5, 50, 50, false);
  }
  
}
