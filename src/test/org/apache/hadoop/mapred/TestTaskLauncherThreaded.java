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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapred.TaskTracker.TaskLauncher;
import org.apache.hadoop.mapred.TaskTracker.RunningJob;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;

/**
 * Tests {@link TaskLauncherThreaded}
 */
public class TestTaskLauncherThreaded {
  private static int jobLocalizedCount = 0;
  private static int jobLaunchCount = 0;
  private static boolean quitWaiting = false;
  private static boolean firstJobStarted = false;
  private static boolean firstJobFinished = false;

  private static class MyTaskTracker extends TaskTracker {

    // stub out functions called from startNewTask
    @Override
    RunningJob localizeJob(TaskInProgress tip)
        throws IOException, InterruptedException {
      if (firstJobStarted == false) {
        firstJobStarted = true;
        while (quitWaiting == false) {
          Thread.sleep(100);
        }
        firstJobFinished = true;
      }
      // mock out a RunningJob
      RunningJob rjob = mock(RunningJob.class);
      when(rjob.getLocalizedJobConf()).thenReturn(new Path("testing"));
      when(rjob.getJobConf()).thenReturn(new JobConf());
      jobLocalizedCount++;

      return rjob;
    }

    @Override
    protected void launchTaskForJob(TaskInProgress tip, JobConf jobConf,
                                RunningJob rjob) throws IOException {
      jobLaunchCount++;
    }
  }

  /**
   * Tests the case "task localizing doesn't block other tasks".
   *
   * Launches one task that simulates a task doing large localization,
   * then starts a second task and verifies that second task is not
   * blocked waiting behind the first task.
   *
   * @throws IOException
   */
  @Test
  public void testLocalizationNotBlockingOtherTasks() throws IOException {
    // setup a TaskTracker
    JobConf ttConf = new JobConf();
    ttConf.setInt("mapred.tasktracker.map.tasks.maximum", 4);
    TaskTracker tt = new MyTaskTracker();

    tt.runningJobs = new TreeMap<JobID, TaskTracker.RunningJob>();
    tt.runningTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    tt.setIndexCache(new IndexCache(ttConf));
    tt.setTaskMemoryManagerEnabledFlag();

    // start map-task launcher with four slots
    TaskLauncher mapLauncher = tt.new TaskLauncher(TaskType.MAP, 4);
    mapLauncher.start();

    // launch a task which simulates large localization
    String jtId = "test";
    TaskAttemptID attemptID = new TaskAttemptID(jtId, 1, true, 0, 0);
    Task task = new MapTask(null, attemptID, 0, null, 2);
    mapLauncher.addToTaskQueue(new LaunchTaskAction(task));
    // verify that task is added to runningTasks
    TaskInProgress runningTip = tt.runningTasks.get(attemptID);
    assertNotNull(runningTip);

    // wait for a while for the first task to start initializing
    // this loop waits at most for 30 seconds
    for (int i = 0; i < 300; i++) {
      if (firstJobStarted == true) {
        break;
      }
      UtilsForTests.waitFor(100);
    }

    // Now start a second task and make sure it doesn't wait while first one initializes
    String secondjtId = "test2";
    TaskAttemptID secondAttemptID = new TaskAttemptID(secondjtId, 1, true, 0, 0);
    Task secondTask = new MapTask(null, secondAttemptID, 0, null, 2);
    mapLauncher.addToTaskQueue(new LaunchTaskAction(secondTask));
    // verify that task is added to runningTasks
    TaskInProgress secondRunningTip = tt.runningTasks.get(secondAttemptID);
    assertNotNull(secondRunningTip);

    // wait for a while for the second task to be launched
    // this loop waits at most for 30 seconds
    for (int i = 0; i < 300; i++) {
      if (jobLaunchCount > 0) {
        break;
      }
      UtilsForTests.waitFor(100);
    }

    assertEquals("Second task didn't run or both ran", 1, jobLocalizedCount);
    assertEquals("second task didn't try to launch", 1, jobLaunchCount);
    assertFalse("Second task didn't finish first task initializing", firstJobFinished);

    // tell first task to stop waiting
    quitWaiting = true;

    // wait for a while for the first task finishes initializing
    // this loop waits at most for 30 seconds
    for (int i = 0; i < 300; i++) {
      if (firstJobFinished == true) {
        break;
      }
      UtilsForTests.waitFor(100);
    }
    assertTrue("First task didn't finish initializing", firstJobFinished);

    // wait for a while for the first task finishes
    // this loop waits at most for 30 seconds
    for (int i = 0; i < 300; i++) {
      if (jobLaunchCount > 1) {
        break;
      }
      UtilsForTests.waitFor(100);
    }
    assertEquals("Both tasks didn't run", 2, jobLocalizedCount);
    assertEquals("First task didn't try to launch", 2, jobLaunchCount);

  }

}
