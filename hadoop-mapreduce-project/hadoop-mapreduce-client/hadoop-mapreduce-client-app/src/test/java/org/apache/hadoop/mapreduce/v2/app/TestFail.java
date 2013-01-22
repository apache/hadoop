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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskAttemptListenerImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherImpl;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.junit.Test;

/**
 * Tests the state machine with respect to Job/Task/TaskAttempt failure 
 * scenarios.
 */
@SuppressWarnings("unchecked")
public class TestFail {

  @Test
  //First attempt is failed and second attempt is passed
  //The job succeeds.
  public void testFailTask() throws Exception {
    MRApp app = new MockFirstFailingAttemptMRApp(1, 0);
    Configuration conf = new Configuration();
    // this test requires two task attempts, but uberization overrides max to 1
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.SUCCEEDED);
    Map<TaskId,Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", 1, tasks.size());
    Task task = tasks.values().iterator().next();
    Assert.assertEquals("Task state not correct", TaskState.SUCCEEDED,
        task.getReport().getTaskState());
    Map<TaskAttemptId, TaskAttempt> attempts =
        tasks.values().iterator().next().getAttempts();
    Assert.assertEquals("Num attempts is not correct", 2, attempts.size());
    //one attempt must be failed 
    //and another must have succeeded
    Iterator<TaskAttempt> it = attempts.values().iterator();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.FAILED,
        it.next().getReport().getTaskAttemptState());
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.SUCCEEDED,
        it.next().getReport().getTaskAttemptState());
  }

  @Test
  public void testMapFailureMaxPercent() throws Exception {
    MRApp app = new MockFirstFailingTaskMRApp(4, 0);
    Configuration conf = new Configuration();
    
    //reduce the no of attempts so test run faster
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 2);
    conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 1);
    
    conf.setInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 20);
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.FAILED);
    
    //setting the failure percentage to 25% (1/4 is 25) will
    //make the Job successful
    app = new MockFirstFailingTaskMRApp(4, 0);
    conf = new Configuration();
    
    //reduce the no of attempts so test run faster
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 2);
    conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 1);
    
    conf.setInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 25);
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
    job = app.submit(conf);
    app.waitForState(job, JobState.SUCCEEDED);
  }

  @Test
  public void testReduceFailureMaxPercent() throws Exception {
    MRApp app = new MockFirstFailingTaskMRApp(2, 4);
    Configuration conf = new Configuration();
    
    //reduce the no of attempts so test run faster
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
    conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 2);
    
    conf.setInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 50);//no failure due to Map
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
    conf.setInt(MRJobConfig.REDUCE_FAILURES_MAXPERCENT, 20);
    conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 1);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.FAILED);
    
    //setting the failure percentage to 25% (1/4 is 25) will
    //make the Job successful
    app = new MockFirstFailingTaskMRApp(2, 4);
    conf = new Configuration();
    
    //reduce the no of attempts so test run faster
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
    conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 2);
    
    conf.setInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 50);//no failure due to Map
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
    conf.setInt(MRJobConfig.REDUCE_FAILURES_MAXPERCENT, 25);
    conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 1);
    job = app.submit(conf);
    app.waitForState(job, JobState.SUCCEEDED);
  }

  @Test
  //All Task attempts are timed out, leading to Job failure
  public void testTimedOutTask() throws Exception {
    MRApp app = new TimeOutTaskMRApp(1, 0);
    Configuration conf = new Configuration();
    int maxAttempts = 2;
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, maxAttempts);
    // disable uberization (requires entire job to be reattempted, so max for
    // subtask attempts is overridden to 1)
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.FAILED);
    Map<TaskId,Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", 1, tasks.size());
    Task task = tasks.values().iterator().next();
    Assert.assertEquals("Task state not correct", TaskState.FAILED,
        task.getReport().getTaskState());
    Map<TaskAttemptId, TaskAttempt> attempts =
        tasks.values().iterator().next().getAttempts();
    Assert.assertEquals("Num attempts is not correct", maxAttempts,
        attempts.size());
    for (TaskAttempt attempt : attempts.values()) {
      Assert.assertEquals("Attempt state not correct", TaskAttemptState.FAILED,
          attempt.getReport().getTaskAttemptState());
    }
  }

  @Test
  public void testTaskFailWithUnusedContainer() throws Exception {
    MRApp app = new MRAppWithFailingTaskAndUnusedContainer();
    Configuration conf = new Configuration();
    int maxAttempts = 1;
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, maxAttempts);
    // disable uberization (requires entire job to be reattempted, so max for
    // subtask attempts is overridden to 1)
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", 1, tasks.size());
    Task task = tasks.values().iterator().next();
    app.waitForState(task, TaskState.SCHEDULED);
    Map<TaskAttemptId, TaskAttempt> attempts = tasks.values().iterator()
        .next().getAttempts();
    Assert.assertEquals("Num attempts is not correct", maxAttempts, attempts
        .size());
    TaskAttempt attempt = attempts.values().iterator().next();
    app.waitForInternalState((TaskAttemptImpl) attempt,
        TaskAttemptStateInternal.ASSIGNED);
    app.getDispatcher().getEventHandler().handle(
        new TaskAttemptEvent(attempt.getID(),
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
    app.waitForState(job, JobState.FAILED);
  }

  static class MRAppWithFailingTaskAndUnusedContainer extends MRApp {

    public MRAppWithFailingTaskAndUnusedContainer() {
      super(1, 0, false, "TaskFailWithUnsedContainer", true);
    }

    @Override
    protected ContainerLauncher createContainerLauncher(AppContext context) {
      return new ContainerLauncherImpl(context) {
        @Override
        public void handle(ContainerLauncherEvent event) {

          switch (event.getType()) {
          case CONTAINER_REMOTE_LAUNCH:
            super.handle(event); // Unused event and container.
            break;
          case CONTAINER_REMOTE_CLEANUP:
            getContext().getEventHandler().handle(
                new TaskAttemptEvent(event.getTaskAttemptID(),
                    TaskAttemptEventType.TA_CONTAINER_CLEANED));
            break;
          }
        }

        @Override
        protected ContainerManager getCMProxy(ContainerId contianerID,
            String containerManagerBindAddr, ContainerToken containerToken)
            throws IOException {
          try {
            synchronized (this) {
              wait(); // Just hang the thread simulating a very slow NM.
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          return null;
        }
      };
    };
  }

  static class TimeOutTaskMRApp extends MRApp {
    TimeOutTaskMRApp(int maps, int reduces) {
      super(maps, reduces, false, "TimeOutTaskMRApp", true);
    }
    @Override
    protected TaskAttemptListener createTaskAttemptListener(AppContext context) {
      //This will create the TaskAttemptListener with TaskHeartbeatHandler
      //RPC servers are not started
      //task time out is reduced
      //when attempt times out, heartbeat handler will send the lost event
      //leading to Attempt failure
      return new TaskAttemptListenerImpl(getContext(), null, null) {
        @Override
        public void startRpcServer(){};
        @Override
        public void stopRpcServer(){};
        @Override
        public InetSocketAddress getAddress() {
          return NetUtils.createSocketAddr("localhost", 1234);
        }
        public void init(Configuration conf) {
          conf.setInt(MRJobConfig.TASK_TIMEOUT, 1*1000);//reduce timeout
          conf.setInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 1*1000);
          super.init(conf);
        }
      };
    }
  }

  //Attempts of first Task are failed
  static class MockFirstFailingTaskMRApp extends MRApp {

    MockFirstFailingTaskMRApp(int maps, int reduces) {
      super(maps, reduces, true, "MockFirstFailingTaskMRApp", true);
    }

    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      if (attemptID.getTaskId().getId() == 0) {//check if it is first task
        // send the Fail event
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, 
                TaskAttemptEventType.TA_FAILMSG));
      } else {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID,
                TaskAttemptEventType.TA_DONE));
      }
    }
  }

  //First attempt is failed
  static class MockFirstFailingAttemptMRApp extends MRApp {
    MockFirstFailingAttemptMRApp(int maps, int reduces) {
      super(maps, reduces, true, "MockFirstFailingAttemptMRApp", true);
    }

    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      if (attemptID.getTaskId().getId() == 0 && attemptID.getId() == 0) {
        //check if it is first task's first attempt
        // send the Fail event
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, 
                TaskAttemptEventType.TA_FAILMSG));
      } else {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID,
                TaskAttemptEventType.TA_DONE));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    TestFail t = new TestFail();
    t.testFailTask();
    t.testTimedOutTask();
    t.testMapFailureMaxPercent();
    t.testReduceFailureMaxPercent();
    t.testTaskFailWithUnusedContainer();
  }
}
