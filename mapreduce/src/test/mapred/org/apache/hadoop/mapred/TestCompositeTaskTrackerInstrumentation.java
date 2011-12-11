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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestCompositeTaskTrackerInstrumentation {
  private static final Log LOG = LogFactory.getLog(
      TestCompositeTaskTrackerInstrumentation.class);

  @Test
  public void testCompositeInstrumentation() throws IOException {
    // Create two instrumentation instances
    TaskTracker tt = new TaskTracker();
    DummyTaskTrackerInstrumentation inst1 =
      new DummyTaskTrackerInstrumentation(tt);
    DummyTaskTrackerInstrumentation inst2 =
      new DummyTaskTrackerInstrumentation(tt);
    
    // Add them to a composite object
    ArrayList<TaskTrackerInstrumentation> insts =
      new ArrayList<TaskTrackerInstrumentation>();
    insts.add(inst1);
    insts.add(inst2);
    CompositeTaskTrackerInstrumentation comp =
      new CompositeTaskTrackerInstrumentation(tt, insts);

    // Create some dummy objects to pass to instrumentation methods
    TaskAttemptID tid = new TaskAttemptID();
    File file = new File("file");
    Task task = new MapTask();
    TaskStatus status = new MapTaskStatus();

    // Test that completeTask propagates to listeners
    assertFalse(inst1.completeTaskCalled);
    assertFalse(inst2.completeTaskCalled);
    comp.completeTask(tid);
    assertTrue(inst1.completeTaskCalled);
    assertTrue(inst2.completeTaskCalled);

    // Test that timedoutTask propagates to listeners
    assertFalse(inst1.timedoutTaskCalled);
    assertFalse(inst2.timedoutTaskCalled);
    comp.timedoutTask(tid);
    assertTrue(inst1.timedoutTaskCalled);
    assertTrue(inst2.timedoutTaskCalled);

    // Test that taskFailedPing propagates to listeners
    assertFalse(inst1.taskFailedPingCalled);
    assertFalse(inst2.taskFailedPingCalled);
    comp.taskFailedPing(tid);
    assertTrue(inst1.taskFailedPingCalled);
    assertTrue(inst2.taskFailedPingCalled);

    // Test that reportTaskLaunch propagates to listeners
    assertFalse(inst1.reportTaskLaunchCalled);
    assertFalse(inst2.reportTaskLaunchCalled);
    comp.reportTaskLaunch(tid, file, file);
    assertTrue(inst1.reportTaskLaunchCalled);
    assertTrue(inst2.reportTaskLaunchCalled);

    // Test that reportTaskEnd propagates to listeners
    assertFalse(inst1.reportTaskEndCalled);
    assertFalse(inst2.reportTaskEndCalled);
    comp.reportTaskEnd(tid);
    assertTrue(inst1.reportTaskEndCalled);
    assertTrue(inst2.reportTaskEndCalled);

    // Test that statusUpdate propagates to listeners
    assertFalse(inst1.statusUpdateCalled);
    assertFalse(inst2.statusUpdateCalled);
    comp.statusUpdate(task, status);
    assertTrue(inst1.statusUpdateCalled);
    assertTrue(inst2.statusUpdateCalled);
  }
}
