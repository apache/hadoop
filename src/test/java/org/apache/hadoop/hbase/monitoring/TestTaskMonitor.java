/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.monitoring;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class TestTaskMonitor {

  @Test
  public void testTaskMonitorBasics() {
    TaskMonitor tm = new TaskMonitor();
    assertTrue("Task monitor should start empty",
        tm.getTasks().isEmpty());
    
    // Make a task and fetch it back out
    MonitoredTask task = tm.createStatus("Test task");
    MonitoredTask taskFromTm = tm.getTasks().get(0);
    
    // Make sure the state is reasonable.
    assertEquals(task.getDescription(), taskFromTm.getDescription());
    assertEquals(-1, taskFromTm.getCompletionTimestamp());
    assertEquals(MonitoredTask.State.RUNNING, taskFromTm.getState());
    
    // Mark it as finished
    task.markComplete("Finished!");
    assertEquals(MonitoredTask.State.COMPLETE, task.getState());
    
    // It should still show up in the TaskMonitor list
    assertEquals(1, tm.getTasks().size());
    
    // If we mark its completion time back a few minutes, it should get gced
    task.expireNow();
    assertEquals(0, tm.getTasks().size());
  }
  
  @Test
  public void testTasksGetAbortedOnLeak() throws InterruptedException {
    final TaskMonitor tm = new TaskMonitor();
    assertTrue("Task monitor should start empty",
        tm.getTasks().isEmpty());
    
    final AtomicBoolean threadSuccess = new AtomicBoolean(false);
    // Make a task in some other thread and leak it
    Thread t = new Thread() {
      @Override
      public void run() {
        MonitoredTask task = tm.createStatus("Test task");    
        assertEquals(MonitoredTask.State.RUNNING, task.getState());
        threadSuccess.set(true);
      }
    };
    t.start();
    t.join();
    // Make sure the thread saw the correct state
    assertTrue(threadSuccess.get());
    
    // Make sure the leaked reference gets cleared
    System.gc();
    System.gc();
    System.gc();
    
    // Now it should be aborted 
    MonitoredTask taskFromTm = tm.getTasks().get(0);
    assertEquals(MonitoredTask.State.ABORTED, taskFromTm.getState());
  }
  
  @Test
  public void testTaskLimit() throws Exception {
    TaskMonitor tm = new TaskMonitor();
    for (int i = 0; i < TaskMonitor.MAX_TASKS + 10; i++) {
      tm.createStatus("task " + i);
    }
    // Make sure it was limited correctly
    assertEquals(TaskMonitor.MAX_TASKS, tm.getTasks().size());
    // Make sure we culled the earlier tasks, not later
    // (i.e. tasks 0 through 9 should have been deleted)
    assertEquals("task 10", tm.getTasks().get(0).getDescription());
  }

}
