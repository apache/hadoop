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

import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

//
// Test case for SimulatorJobTracker.
// We create a table of expected list of new events generated and 
// another table of expected heartbeat() in parameters and the task actions 
// to performe for each timestamp t. We then run the task tracker with our
// own little event queue and check if exactly those things happen that
// are listed in the two tables/
//
public class TestSimulatorTaskTracker {
  MockSimulatorJobTracker jobTracker;
  SimulatorTaskTracker taskTracker;

  static final Log LOG = LogFactory.getLog(TestSimulatorTaskTracker.class);
  
  // Our own little event queue, checks the events against the expected before \
  // enqueueing them
  CheckedEventQueue eventQueue;
  
  // Global test parameters
  final int heartbeatInterval = 10;
  final long simulationStartTime = 100;
  
  // specify number of heartbeats since simulation start for mapStarts
  final long[] mapStarts = {2, 3, 4};
  // specify number of heartbeats since mapStarts[i] for mapKills[i], 
  // use -1 for no kill
  final long[] mapKills = {1, -1, 2};
  final long[] mapRuntimes = {53, 17, 42};
  
  // specify number of heartbeats since start for reduceStarts
  final long[] reduceStarts = {3, 4, 6};
  // specify number of heartbeats since mapStarts[i] for mapKills[i], 
  // use -1 for no kill
  final long[] reduceKills = {1, -1, 6};  
  final long[] mapDoneDelays = {11, 0, 33};
  final long[] reduceRuntimes = {49, 25, 64};
  
  final static String taskAttemptIdPrefix = "attempt_200907150128_0007_";
  final String taskTrackerName = "test_task_tracker";
  
  @Before
  public void setUp() {
    try {
      jobTracker = new MockSimulatorJobTracker(simulationStartTime,
                                               heartbeatInterval, true);
    } catch (Exception e) {
      Assert.fail("Couldn't set up the mock job tracker: " + e);
    }
    Configuration ttConf = new Configuration();
    ttConf.set("mumak.tasktracker.tracker.name", taskTrackerName);
    ttConf.set("mumak.tasktracker.host.name", "test_host");
    ttConf.setInt("mapred.tasktracker.map.tasks.maximum", 3);
    ttConf.setInt("mapred.tasktracker.reduce.tasks.maximum", 3);
    ttConf.setInt("mumak.tasktracker.heartbeat.fuzz", -1);    
    taskTracker = new SimulatorTaskTracker(jobTracker, ttConf);
    eventQueue = new CheckedEventQueue(simulationStartTime); 
  }
  
  @Test
  public void testInitAndHeartbeat() {
    LOG.debug("Testing init and hearbeat mechanism");
    genericTest(5, 0, 0, false);
  }

  // All further tests assume that testInitAndHeartbeat passed
  @Test
  public void testSingleMapTask() {
    LOG.debug("Testing with a single map task");
    genericTest(20, 1, 0, false);
  }

  @Test
  public void testSingleReduceTask() {
    LOG.debug("Testing with a single reduce task");
    genericTest(20, 0, 1, false);
  }
  
  @Test
  public void testMultipleMapTasks() {
    LOG.debug("Testing with multiple map tasks");
    genericTest(20, mapStarts.length, 0, false);
  }
  
  @Test
  public void testMultipleReduceTasks() {
    LOG.debug("Testing with multiple reduce tasks");
    genericTest(20, 0, reduceStarts.length, false);
  }

  @Test
  public void testMultipleMapAndReduceTasks() {
    LOG.debug("Testing with multiple map and reduce tasks");
    genericTest(20, mapStarts.length, reduceStarts.length, false);
  }

  @Test
  public void testKillSingleMapTask() {
    LOG.debug("Testing killing a single map task");
    genericTest(20, 1, 0, true);
  }

  @Test
  public void testKillSingleReduceTask() {
    LOG.debug("Testing killing a single reduce task");
    genericTest(20, 0, 1, true);
  }
  
  @Test
  public void testKillMultipleMapTasks() {
    LOG.debug("Testing killing multiple map tasks");
    genericTest(20, mapStarts.length, 0, true);
  }

  @Test
  public void testKillMultipleReduceTasks() {
    LOG.debug("Testing killing multiple reduce tasks");
    genericTest(20, 0, reduceStarts.length, true);
  }
  
  @Test
  public void testKillMultipleMapAndReduceTasks() {
    LOG.debug("Testing killing multiple map and reduce tasks");
    genericTest(20, mapStarts.length, reduceStarts.length, true);
  }
  
  protected void genericTest(int numAccepts, int numMaps, int numReduces, 
                             boolean testKill) {
    LOG.debug("Generic test with numAccepts=" + numAccepts +
              ", numMaps=" + numMaps + ", numReduces=" + numReduces +
              ", testKill=" + testKill);
    
    setUpHeartbeats(numAccepts);
    for(int i=0; i<numMaps; i++) {
      setUpMapTask(i, testKill);
    }
    for(int i=0; i<numReduces; i++) {
      setUpReduceTask(i, testKill);
    }    
    runTaskTracker();
  }
  
  // numAccepts must be at least 1
  private void setUpHeartbeats(int numAccepts) {
    eventQueue.expectHeartbeats(taskTracker, numAccepts, heartbeatInterval);
    jobTracker.expectEmptyHeartbeats(taskTrackerName, numAccepts);
  }
  
  private void setUpMapTask(TaskAttemptID mapTaskId, long mapStart, 
                            long mapRuntime, long mapKill) {
    jobTracker.runMapTask(taskTrackerName, mapTaskId, mapStart, mapRuntime, 
                          mapKill);
    eventQueue.expectMapTask(taskTracker, mapTaskId, mapStart, mapRuntime);
  }

  private void setUpMapTask(int idx, boolean testKill) {
    TaskAttemptID mapTaskId = createTaskAttemptID(true, idx);
    long mapStart = simulationStartTime + heartbeatInterval*mapStarts[idx];
    long mapKill = -1;
    if (testKill && 0 <= mapKills[idx]) {
      mapKill = mapStart + heartbeatInterval*mapKills[idx];
    }
    setUpMapTask(mapTaskId, mapStart, mapRuntimes[idx], mapKill);
  }

  private void setUpReduceTask(TaskAttemptID reduceTaskId, long reduceStart, 
                               long mapDoneDelay, long reduceRuntime,
                               long reduceKill) {
    jobTracker.runReduceTask(taskTrackerName, reduceTaskId, reduceStart,
                             mapDoneDelay, reduceRuntime, reduceKill);
    long mapDone = jobTracker.nextHeartbeat(reduceStart + mapDoneDelay);
    if (reduceKill < 0 ||  mapDone < reduceKill) {
      // it generates completion events iff it survives mapDone 
      eventQueue.expectReduceTask(taskTracker, reduceTaskId, 
                                  mapDone, reduceRuntime);
    }
  }

  private void setUpReduceTask(int idx, boolean testKill) {
    TaskAttemptID reduceTaskId = createTaskAttemptID(false, idx);
    long reduceStart = simulationStartTime + 
                       heartbeatInterval*reduceStarts[idx];
    long reduceKill = -1;
    if (testKill && 0 <= reduceKills[idx]) {
      reduceKill = reduceStart + heartbeatInterval*reduceKills[idx];
    }
    setUpReduceTask(reduceTaskId, reduceStart, mapDoneDelays[idx],
                    reduceRuntimes[idx], reduceKill);
  }
   
  //
  // runs a single task tracker
  // checks that generated events conform to expectedEvents
  // and the mock jobtracker checks that the heartbeats() sent to it are right
  //
  private void runTaskTracker() {
    long runUntil = eventQueue.getLastCheckTime();
    LOG.debug("Running task tracker until simulation time=" + runUntil);

    List<SimulatorEvent> events = taskTracker.init(simulationStartTime);
    eventQueue.addAll(events);
    while (true) {
      // can't be empty as it must go past runUntil for verifiability
      // besides it is never empty because of HeartbeatEvent            
      SimulatorEvent currentEvent = eventQueue.get();
      // copy time, make sure TT does not modify it
      long now = currentEvent.getTimeStamp();
      LOG.debug("Number of events to deliver=" + (eventQueue.getSize()+1) +
                ", now=" + now);
      if (now > runUntil) {
        break;
      }                             
      LOG.debug("Calling accept(), event=" + currentEvent + ", now=" + now);
      events = taskTracker.accept(currentEvent);
      LOG.debug("Accept() returned " + events.size() + " new event(s)");
      for (SimulatorEvent newEvent: events) {
        LOG.debug("New event " + newEvent);
      }
      eventQueue.addAll(events);
      LOG.debug("Done checking and enqueuing new events");
    }
    
    // make sure we have seen all expected events, even for the last 
    // time checked
    eventQueue.checkMissingExpected();
    // Mock JT should have consumed all entries from its heartbeat table
    jobTracker.checkMissingHeartbeats();
  }
  
  // taskNumber should be < 10
  static private TaskAttemptID createTaskAttemptID(boolean isMap, 
                                                   int taskNumber) {
    String attempt = taskAttemptIdPrefix + (isMap ? "m" : "r") + 
                     "_00000" + taskNumber + "_0";
    TaskAttemptID taskId = null;
    try {
      taskId = TaskAttemptID.forName(attempt);
    } catch (IllegalArgumentException iae) {
      Assert.fail("Invalid task attempt id string " + iae);
    }
    return taskId;
  }
}
