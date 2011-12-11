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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * An EventQueue that checks events against a list of expected events upon
 * enqueueing. Also contains routines for creating expected HeartbeatEvents and
 * all expected events related to running map or reduce tasks on a task tracker.
 */
class CheckedEventQueue extends SimulatorEventQueue {
  /**
   * expected list of events to be returned from all EventListener.accept()
   * called at time t, t is the key if no events are generated an empty list
   * needs to be put there
   * 
   * IMPORTANT: this is NOT the events to be delivered at time t from the event
   * queue, it is the list events to be inserted into the event queue at time t
   */
  private SortedMap<Long, List<SimulatorEvent>> expectedEvents = 
      new TreeMap<Long, List<SimulatorEvent>>();
       
  // current simulation time
  private long now;
  private long simulationStartTime;
  
  /**
   * We need the simulation start time so that we know the time of the first
   * add().
   * 
   * @param simulationStartTime
   *          Simulation start time.
   */
  public CheckedEventQueue(long simulationStartTime) {
    now = simulationStartTime;
    this.simulationStartTime = simulationStartTime;
  }
  
  void check(SimulatorEvent event) {
    for (Iterator<Map.Entry<Long, List<SimulatorEvent>>> it = expectedEvents.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Long, List<SimulatorEvent>> entry = it.next();
      long insertTime = entry.getKey();
      Assert.assertTrue(insertTime <= now);
      if (insertTime < now) {
        List<SimulatorEvent> events = entry.getValue();
        if (!events.isEmpty()) {
          Assert.fail("There are " + events.size() + " events at time "
            + insertTime + " before " + now + ". First event: "+events.get(0));
        }
        it.remove();
      } else { // insertTime == now
        break;
      }
    }
    
    List<SimulatorEvent> expected = expectedEvents.get(now);
    boolean found = false;
    for (SimulatorEvent ee : expected) {
      if (isSameEvent(ee, event)) {
        expected.remove(ee);
        found = true;
        break;
      }
    }

    Assert.assertTrue("Unexpected event to enqueue, now=" + now  + ", event=" + 
               event + ", expecting=" + expected, found);
  }
  
  /**
   * We intercept the main routine of the real EventQueue and check the new
   * event returned by accept() against the expectedEvents table
   */
  @Override
  public boolean add(SimulatorEvent event) {
    check(event);
    return super.add(event);
  }
  
  @Override
  public boolean addAll(Collection<? extends SimulatorEvent> events) {
    for (SimulatorEvent event : events) {
      check(event);
    }
    return super.addAll(events);
  }

  // We need to override get() to track the current simulation time
  @Override
  public SimulatorEvent get() {
    SimulatorEvent ret = super.get();
    if (ret != null) {
      now = ret.getTimeStamp();
    }
    return ret;
  }

  /**
   * Auxiliary function for populating the expectedEvents table If event is null
   * then just marks that an accept happens at time 'when', and the list of new
   * events is empty
   */
  public void addExpected(long when, SimulatorEvent event) {
    Assert.assertNotNull(event);
    List<SimulatorEvent> expected = expectedEvents.get(when);
    if (expected == null) {
      expected = new ArrayList<SimulatorEvent>();
      expectedEvents.put(when, expected);
    }
    expected.add(event);
  }
  
  public long getLastCheckTime() {
    return expectedEvents.lastKey();
  }
  
  // there should be an empty expected event list left for the last
  // time to check
  public void checkMissingExpected() {
    Assert.assertTrue(expectedEvents.size() <= 1);
    for (List<SimulatorEvent> events : expectedEvents.values()) {
      Assert.assertTrue(events.isEmpty());
    }
  }
  
  // fills in the expected events corresponding to the execution of a map task
  public void expectMapTask(SimulatorTaskTracker taskTracker,
                            TaskAttemptID taskId,
                            long mapStart, long mapRuntime) {
    long mapDone = mapStart + mapRuntime;
    org.apache.hadoop.mapred.TaskAttemptID taskIdOldApi =
        org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId);
    MapTaskStatus status = new MapTaskStatus(taskIdOldApi, 1.0f, 1,
        State.SUCCEEDED, null, null, null, Phase.MAP, null);
    status.setStartTime(mapStart);
    status.setFinishTime(mapDone);
    TaskAttemptCompletionEvent completionEvent = 
        new TaskAttemptCompletionEvent(taskTracker, status);
    addExpected(mapStart, completionEvent);
  }

  // fills in the expected events corresponding to the execution of a reduce 
  // task
  public void expectReduceTask(SimulatorTaskTracker taskTracker,
                               TaskAttemptID taskId, long mapDone, 
                               long reduceRuntime) {
    long reduceDone = mapDone + reduceRuntime;
    org.apache.hadoop.mapred.TaskAttemptID taskIdOldApi =
        org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId);
    ReduceTaskStatus status = new ReduceTaskStatus(taskIdOldApi, 1.0f, 1,
        State.SUCCEEDED, null, null, null, Phase.REDUCE, null);
    status.setStartTime(mapDone);
    status.setFinishTime(reduceDone);
    TaskAttemptCompletionEvent completionEvent = 
        new TaskAttemptCompletionEvent(taskTracker, status);
    addExpected(mapDone, completionEvent);

  }
  
  /**
   * Fills in the events corresponding to the self heartbeats numAccepts is the
   * number of times accept() will be called, it must be >= 1
   */
  public void expectHeartbeats(SimulatorTaskTracker taskTracker,
                               int numAccepts, int heartbeatInterval) {
    // initial heartbeat
    addExpected(simulationStartTime,
        new HeartbeatEvent(taskTracker, simulationStartTime));
    long simulationTime = simulationStartTime;
    for(int i=0; i<numAccepts; i++) {
      long heartbeatTime = simulationTime + heartbeatInterval;
      HeartbeatEvent he = new HeartbeatEvent(taskTracker, heartbeatTime);
      addExpected(simulationTime, he);
      simulationTime = heartbeatTime;
    }
  }
  
  /**
   * Returns true iff two events are the same. We did not use equals() because
   * we may want to test for partial equality only, and we don't want to bother
   * writing new hashCode()s either.
   */
  protected boolean isSameEvent(SimulatorEvent event, SimulatorEvent otherEvent) {
    // check for null reference
    Assert.assertNotNull(event);
    Assert.assertNotNull(otherEvent);
    // type check
    if (!event.getClass().equals(otherEvent.getClass())) {
      return false;
    }
    // compare significant fields
    if (event.listener != otherEvent.listener || 
        event.timestamp != otherEvent.timestamp) {
      return false;
    }
    if (event instanceof TaskAttemptCompletionEvent) {
      TaskStatus s = ((TaskAttemptCompletionEvent)event).getStatus();
      TaskStatus os = ((TaskAttemptCompletionEvent)otherEvent).getStatus();
      if (!s.getTaskID().equals(os.getTaskID())) {
        return false;
      }
    } 
    return true;
  }
}
