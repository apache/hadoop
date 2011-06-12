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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;

public class TestTaskTrackerInstrumentation {
  private static final Log LOG = LogFactory.getLog(
      TestTaskTrackerInstrumentation.class);

  @Test
  public void testCreateInstrumentationWithSingleClass() {
    // Check that if only a single instrumentation class is given,
    // that class is used directly
    JobConf conf = new JobConf();
    conf.set(TTConfig.TT_INSTRUMENTATION,
        DummyTaskTrackerInstrumentation.class.getName());
    TaskTracker tracker = new TaskTracker();
    TaskTrackerInstrumentation inst =
      TaskTracker.createInstrumentation(tracker, conf);
    assertEquals(DummyTaskTrackerInstrumentation.class.getName(),
        inst.getClass().getName());
  }

  @Test
  public void testCreateInstrumentationWithMultipleClasses() {
    // Set up configuration to create two dummy instrumentation objects
    JobConf conf = new JobConf();
    String dummyClass = DummyTaskTrackerInstrumentation.class.getName();
    String classList = dummyClass + "," + dummyClass;
    conf.set(TTConfig.TT_INSTRUMENTATION, classList);
    TaskTracker tracker = new TaskTracker();

    // Check that a composite instrumentation object is created
    TaskTrackerInstrumentation inst =
      TaskTracker.createInstrumentation(tracker, conf);
    assertEquals(CompositeTaskTrackerInstrumentation.class.getName(),
        inst.getClass().getName());
    
    // Check that each member of the composite is a dummy instrumentation
    CompositeTaskTrackerInstrumentation comp =
      (CompositeTaskTrackerInstrumentation) inst;
    List<TaskTrackerInstrumentation> insts = comp.getInstrumentations();
    assertEquals(2, insts.size());
    assertEquals(DummyTaskTrackerInstrumentation.class.getName(),
        insts.get(0).getClass().getName());
    assertEquals(DummyTaskTrackerInstrumentation.class.getName(),
        insts.get(1).getClass().getName());
  }

  @Test
  public void testCreateInstrumentationWithDefaultClass() {
    // Check that if no instrumentation class is given, the default
    // class (TaskTrackerMetricsInst) is used.
    JobConf conf = new JobConf();
    TaskTracker tracker = new TaskTracker();
    tracker.setConf(conf); // Needed to avoid NullPointerExcepton in
                           // TaskTrackerMetricsInst constructor
    TaskTrackerInstrumentation inst =
      TaskTracker.createInstrumentation(tracker, conf);
    assertEquals(TaskTrackerMetricsInst.class.getName(),
        inst.getClass().getName());
  }

  @Test
  public void testCreateInstrumentationWithEmptyParam() {
    // Check that if an empty string is given, the default instrumentation
    // class (TaskTrackerMetricsInst) is used. An error message should also
    // be written to the log, but we do not capture that.
    JobConf conf = new JobConf();
    conf.set(TTConfig.TT_INSTRUMENTATION, "");
    TaskTracker tracker = new TaskTracker();
    tracker.setConf(conf); // Needed to avoid NullPointerExcepton in
                           // TaskTrackerMetricsInst constructor
    TaskTrackerInstrumentation inst =
      TaskTracker.createInstrumentation(tracker, conf);
    assertEquals(TaskTrackerMetricsInst.class.getName(),
        inst.getClass().getName());
  }

  @Test
  public void testCreateInstrumentationWithInvalidParam() {
    // Check that if an invalid class list is given, the default
    // instrumentation class (TaskTrackerMetricsInst) is used. An error 
    // should also be written to the log, but we do not capture that.
    JobConf conf = new JobConf();
    conf.set(TTConfig.TT_INSTRUMENTATION, "XYZ,ZZY");
    TaskTracker tracker = new TaskTracker();
    tracker.setConf(conf); // Needed to avoid NullPointerExcepton in
                           // TaskTrackerMetricsInst constructor
    TaskTrackerInstrumentation inst =
      TaskTracker.createInstrumentation(tracker, conf);
    assertEquals(TaskTrackerMetricsInst.class.getName(),
        inst.getClass().getName());
  }
}
