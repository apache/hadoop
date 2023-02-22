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

package org.apache.hadoop.mapreduce.v2.app;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;


public class TestTaskHeartbeatHandler {
  
  @SuppressWarnings("unchecked")
  @Test
  public void testTaskTimeout() throws InterruptedException {
    EventHandler mockHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    TaskHeartbeatHandler hb = new TaskHeartbeatHandler(mockHandler, clock, 1);
    
    
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.TASK_TIMEOUT, 10); //10 ms
    // set TASK_PROGRESS_REPORT_INTERVAL to a value smaller than TASK_TIMEOUT
    // so that TASK_TIMEOUT is not overridden
    conf.setLong(MRJobConfig.TASK_PROGRESS_REPORT_INTERVAL, 5);
    conf.setInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 10); //10 ms
    conf.setDouble(MRJobConfig.TASK_LOG_PROGRESS_DELTA_THRESHOLD, 0.01);
    
    hb.init(conf);
    hb.start();
    try {
      ApplicationId appId = ApplicationId.newInstance(0L, 5);
      JobId jobId = MRBuilderUtils.newJobId(appId, 4);
      TaskId tid = MRBuilderUtils.newTaskId(jobId, 3, TaskType.MAP);
      TaskAttemptId taid = MRBuilderUtils.newTaskAttemptId(tid, 2);
      hb.register(taid);
      // Task heartbeat once to avoid stuck
      hb.progressing(taid);
      Thread.sleep(100);
      //Events only happen when the task is canceled
      verify(mockHandler, times(2)).handle(any(Event.class));
    } finally {
      hb.stop();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTaskTimeoutDisable() throws InterruptedException {
    EventHandler mockHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    TaskHeartbeatHandler hb = new TaskHeartbeatHandler(mockHandler, clock, 1);

    Configuration conf = new Configuration();
    conf.setLong(MRJobConfig.TASK_STUCK_TIMEOUT_MS, 0); // no timeout
    conf.setInt(MRJobConfig.TASK_TIMEOUT, 0); // no timeout
    // set TASK_PROGRESS_REPORT_INTERVAL to a value smaller than TASK_TIMEOUT
    // so that TASK_TIMEOUT is not overridden
    conf.setLong(MRJobConfig.TASK_PROGRESS_REPORT_INTERVAL, 0);
    conf.setInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 10); //10 ms

    hb.init(conf);
    hb.start();
    try {
      ApplicationId appId = ApplicationId.newInstance(0L, 5);
      JobId jobId = MRBuilderUtils.newJobId(appId, 4);
      TaskId tid = MRBuilderUtils.newTaskId(jobId, 3, TaskType.MAP);
      TaskAttemptId taid = MRBuilderUtils.newTaskAttemptId(tid, 2);
      hb.register(taid);

      ConcurrentMap<TaskAttemptId, TaskHeartbeatHandler.ReportTime>
          runningAttempts = hb.getRunningAttempts();
      for (Map.Entry<TaskAttemptId, TaskHeartbeatHandler.ReportTime> entry
          : runningAttempts.entrySet()) {
        assertFalse(entry.getValue().isReported());
      }

      Thread.sleep(100);

      // Timeout is disabled, so the task should not be canceled
      verify(mockHandler, never()).handle(any(Event.class));
    } finally {
      hb.stop();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTaskStuck() throws InterruptedException {
    EventHandler mockHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    TaskHeartbeatHandler hb = new TaskHeartbeatHandler(mockHandler, clock, 1);


    Configuration conf = new Configuration();
    conf.setLong(MRJobConfig.TASK_STUCK_TIMEOUT_MS, 10); // 10ms
    conf.setInt(MRJobConfig.TASK_TIMEOUT, 1000); //1000 ms
    // set TASK_PROGRESS_REPORT_INTERVAL to a value smaller than TASK_TIMEOUT
    // so that TASK_TIMEOUT is not overridden
    conf.setLong(MRJobConfig.TASK_PROGRESS_REPORT_INTERVAL, 5);
    conf.setInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 10); //10 ms

    hb.init(conf);
    hb.start();
    try {
      ApplicationId appId = ApplicationId.newInstance(0L, 5);
      JobId jobId = MRBuilderUtils.newJobId(appId, 4);
      TaskId tid = MRBuilderUtils.newTaskId(jobId, 3, TaskType.MAP);
      TaskAttemptId taid = MRBuilderUtils.newTaskAttemptId(tid, 2);
      hb.register(taid);

      ConcurrentMap<TaskAttemptId, TaskHeartbeatHandler.ReportTime>
          runningAttempts = hb.getRunningAttempts();
      for (Map.Entry<TaskAttemptId, TaskHeartbeatHandler.ReportTime> entry
          : runningAttempts.entrySet()) {
        assertFalse(entry.getValue().isReported());
      }

      Thread.sleep(100);

      //Events only happen when the task is canceled
      verify(mockHandler, times(2)).handle(any(Event.class));
    } finally {
      hb.stop();
    }
  }

  /**
   * Test if the final heartbeat timeout is set correctly when task progress
   * report interval is set bigger than the task timeout in the configuration.
   */
  @Test
  public void testTaskTimeoutConfigSmallerThanTaskProgressReportInterval() {
    testTaskTimeoutWrtProgressReportInterval(1000L, 5000L);
  }

  /**
   * Test if the final heartbeat timeout is set correctly when task progress
   * report interval is set smaller than the task timeout in the configuration.
   */
  @Test
  public void testTaskTimeoutConfigBiggerThanTaskProgressReportInterval() {
    testTaskTimeoutWrtProgressReportInterval(5000L, 1000L);
  }

  /**
   * Test if the final heartbeat timeout is set correctly when task progress
   * report interval is not set in the configuration.
   */
  @Test
  public void testTaskTimeoutConfigWithoutTaskProgressReportInterval() {
    final long taskTimeoutConfiged = 2000L;

    final Configuration conf = new Configuration();
    conf.setLong(MRJobConfig.TASK_TIMEOUT, taskTimeoutConfiged);

    final long expectedTimeout = taskTimeoutConfiged;
    verifyTaskTimeoutConfig(conf, expectedTimeout);
  }

  @Test
  public void testTaskUnregistered() throws Exception {
    EventHandler mockHandler = mock(EventHandler.class);
    ControlledClock clock = new ControlledClock();
    clock.setTime(0);
    final TaskHeartbeatHandler hb =
        new TaskHeartbeatHandler(mockHandler, clock, 1);
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 1);
    conf.setDouble(MRJobConfig.TASK_LOG_PROGRESS_DELTA_THRESHOLD, 0.01);
    hb.init(conf);
    hb.start();
    try {
      ApplicationId appId = ApplicationId.newInstance(0L, 5);
      JobId jobId = MRBuilderUtils.newJobId(appId, 4);
      TaskId tid = MRBuilderUtils.newTaskId(jobId, 3, TaskType.MAP);
      final TaskAttemptId taid = MRBuilderUtils.newTaskAttemptId(tid, 2);
      Assert.assertFalse(hb.hasRecentlyUnregistered(taid));
      hb.register(taid);
      Assert.assertFalse(hb.hasRecentlyUnregistered(taid));
      hb.unregister(taid);
      Assert.assertTrue(hb.hasRecentlyUnregistered(taid));
      long unregisterTimeout = conf.getLong(MRJobConfig.TASK_EXIT_TIMEOUT,
          MRJobConfig.TASK_EXIT_TIMEOUT_DEFAULT);
      clock.setTime(unregisterTimeout + 1);
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return !hb.hasRecentlyUnregistered(taid);
        }
      }, 10, 10000);
    } finally {
      hb.stop();
    }
  }

  /**
   * Test if task timeout is set properly in response to the configuration of
   * the task progress report interval.
   */
  private static void testTaskTimeoutWrtProgressReportInterval(
      long timeoutConfig, long taskreportInterval) {
    final Configuration conf = new Configuration();
    conf.setLong(MRJobConfig.TASK_TIMEOUT, timeoutConfig);
    conf.setLong(MRJobConfig.TASK_PROGRESS_REPORT_INTERVAL, taskreportInterval);

    // expected task timeout is at least twice as long as task report interval
    final long expectedTimeout = Math.max(timeoutConfig, taskreportInterval*2);
    verifyTaskTimeoutConfig(conf, expectedTimeout);
  }

  /**
   * Verify task timeout is set as expected in TaskHeartBeatHandler with given
   * configuration.
   * @param conf the configuration
   * @param expectedTimeout expected timeout value
   */
  private static void verifyTaskTimeoutConfig(final Configuration conf,
      final long expectedTimeout) {
    final TaskHeartbeatHandler hb =
        new TaskHeartbeatHandler(null, SystemClock.getInstance(), 1);
    hb.init(conf);

    Assert.assertTrue("The value of the task timeout is incorrect.",
        hb.getTaskTimeOut() == expectedTimeout);
  }
}
