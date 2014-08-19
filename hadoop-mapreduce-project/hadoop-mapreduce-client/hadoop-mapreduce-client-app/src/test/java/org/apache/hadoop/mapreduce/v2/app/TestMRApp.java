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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;

/**
 * Tests the state machine of MR App.
 */
@SuppressWarnings("unchecked")
public class TestMRApp {

  @Test
  public void testMapReduce() throws Exception {
    MRApp app = new MRApp(2, 2, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    Assert.assertEquals(System.getProperty("user.name"),job.getUserName());
  }

  @Test
  public void testZeroMaps() throws Exception {
    MRApp app = new MRApp(0, 1, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
  }
  
  @Test
  public void testZeroMapReduces() throws Exception{
    MRApp app = new MRApp(0, 0, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
  }
  
  @Test
  public void testCommitPending() throws Exception {
    MRApp app = new MRApp(1, 0, false, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 1, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task task = it.next();
    app.waitForState(task, TaskState.RUNNING);
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    app.waitForState(attempt, TaskAttemptState.RUNNING);

    //send the commit pending signal to the task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            attempt.getID(),
            TaskAttemptEventType.TA_COMMIT_PENDING));

    //wait for first attempt to commit pending
    app.waitForState(attempt, TaskAttemptState.COMMIT_PENDING);

    //re-send the commit pending signal to the task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            attempt.getID(),
            TaskAttemptEventType.TA_COMMIT_PENDING));

    //the task attempt should be still at COMMIT_PENDING
    app.waitForState(attempt, TaskAttemptState.COMMIT_PENDING);

    //send the done signal to the task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));

    app.waitForState(job, JobState.SUCCEEDED);
  }

  //@Test
  public void testCompletedMapsForReduceSlowstart() throws Exception {
    MRApp app = new MRApp(2, 1, false, this.getClass().getName(), true);
    Configuration conf = new Configuration();
    //after half of the map completion, reduce will start
    conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.5f);
    //uberization forces full slowstart (1.0), so disable that
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("Num tasks not correct", 3, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task mapTask2 = it.next();
    Task reduceTask = it.next();
    
    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    TaskAttempt task1Attempt = mapTask1.getAttempts().values().iterator().next();
    TaskAttempt task2Attempt = mapTask2.getAttempts().values().iterator().next();
    
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt, TaskAttemptState.RUNNING);
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);
    
    // reduces must be in NEW state
    Assert.assertEquals("Reduce Task state not correct",
        TaskState.NEW, reduceTask.getReport().getTaskState());
    
    //send the done signal to the 1st map task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapTask1.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait for first map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    //Once the first map completes, it will schedule the reduces
    //now reduce must be running
    app.waitForState(reduceTask, TaskState.RUNNING);
    
    //send the done signal to 2nd map and the reduce to complete the job
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapTask2.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduceTask.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    app.waitForState(job, JobState.SUCCEEDED);
  }
  
  /**
   * The test verifies that the AM re-runs maps that have run on bad nodes. It
   * also verifies that the AM records all success/killed events so that reduces
   * are notified about map output status changes. It also verifies that the
   * re-run information is preserved across AM restart
   */
  @Test
  public void testUpdatedNodes() throws Exception {
    int runCount = 0;
    MRApp app = new MRAppWithHistory(2, 2, false, this.getClass().getName(),
        true, ++runCount);
    Configuration conf = new Configuration();
    // after half of the map completion, reduce will start
    conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.5f);
    // uberization forces full slowstart (1.0), so disable that
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 4, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task mapTask2 = it.next();

    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    app.waitForState(mapTask2, TaskState.RUNNING);

    TaskAttempt task1Attempt = mapTask1.getAttempts().values().iterator()
        .next();
    TaskAttempt task2Attempt = mapTask2.getAttempts().values().iterator()
        .next();
    NodeId node1 = task1Attempt.getNodeId();
    NodeId node2 = task2Attempt.getNodeId();
    Assert.assertEquals(node1, node2);

    // send the done signal to the task
    app.getContext()
        .getEventHandler()
        .handle(
            new TaskAttemptEvent(task1Attempt.getID(),
                TaskAttemptEventType.TA_DONE));
    app.getContext()
        .getEventHandler()
        .handle(
            new TaskAttemptEvent(task2Attempt.getID(),
                TaskAttemptEventType.TA_DONE));

    // all maps must be succeeded
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    app.waitForState(mapTask2, TaskState.SUCCEEDED);

    TaskAttemptCompletionEvent[] events = job.getTaskAttemptCompletionEvents(0,
        100);
    Assert.assertEquals("Expecting 2 completion events for success", 2,
        events.length);

    // send updated nodes info
    ArrayList<NodeReport> updatedNodes = new ArrayList<NodeReport>();
    NodeReport nr = RecordFactoryProvider.getRecordFactory(null)
        .newRecordInstance(NodeReport.class);
    nr.setNodeId(node1);
    nr.setNodeState(NodeState.UNHEALTHY);
    updatedNodes.add(nr);
    app.getContext().getEventHandler()
        .handle(new JobUpdatedNodesEvent(job.getID(), updatedNodes));

    app.waitForState(task1Attempt, TaskAttemptState.KILLED);
    app.waitForState(task2Attempt, TaskAttemptState.KILLED);

    events = job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals("Expecting 2 more completion events for killed", 4,
        events.length);

    // all maps must be back to running
    app.waitForState(mapTask1, TaskState.RUNNING);
    app.waitForState(mapTask2, TaskState.RUNNING);

    Iterator<TaskAttempt> itr = mapTask1.getAttempts().values().iterator();
    itr.next();
    task1Attempt = itr.next();

    // send the done signal to the task
    app.getContext()
        .getEventHandler()
        .handle(
            new TaskAttemptEvent(task1Attempt.getID(),
                TaskAttemptEventType.TA_DONE));

    // map1 must be succeeded. map2 must be running
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    app.waitForState(mapTask2, TaskState.RUNNING);

    events = job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals("Expecting 1 more completion events for success", 5,
        events.length);

    // Crash the app again.
    app.stop();

    // rerun
    // in rerun the 1st map will be recovered from previous run
    app = new MRAppWithHistory(2, 2, false, this.getClass().getName(), false,
        ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("No of tasks not correct", 4, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    mapTask2 = it.next();
    Task reduceTask1 = it.next();
    Task reduceTask2 = it.next();

    // map 1 will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    app.waitForState(mapTask2, TaskState.RUNNING);

    events = job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals(
        "Expecting 2 completion events for killed & success of map1", 2,
        events.length);

    task2Attempt = mapTask2.getAttempts().values().iterator().next();
    app.getContext()
        .getEventHandler()
        .handle(
            new TaskAttemptEvent(task2Attempt.getID(),
                TaskAttemptEventType.TA_DONE));
    app.waitForState(mapTask2, TaskState.SUCCEEDED);

    events = job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals("Expecting 1 more completion events for success", 3,
        events.length);

    app.waitForState(reduceTask1, TaskState.RUNNING);
    app.waitForState(reduceTask2, TaskState.RUNNING);

    TaskAttempt task3Attempt = reduceTask1.getAttempts().values().iterator()
        .next();
    app.getContext()
        .getEventHandler()
        .handle(
            new TaskAttemptEvent(task3Attempt.getID(),
                TaskAttemptEventType.TA_DONE));
    app.waitForState(reduceTask1, TaskState.SUCCEEDED);
    app.getContext()
    .getEventHandler()
    .handle(
        new TaskAttemptEvent(task3Attempt.getID(),
            TaskAttemptEventType.TA_KILL));
    app.waitForState(reduceTask1, TaskState.SUCCEEDED);
    
    TaskAttempt task4Attempt = reduceTask2.getAttempts().values().iterator()
        .next();
    app.getContext()
        .getEventHandler()
        .handle(
            new TaskAttemptEvent(task4Attempt.getID(),
                TaskAttemptEventType.TA_DONE));
    app.waitForState(reduceTask2, TaskState.SUCCEEDED);    

    events = job.getTaskAttemptCompletionEvents(0, 100);
    Assert.assertEquals("Expecting 2 more completion events for reduce success",
        5, events.length);

    // job succeeds
    app.waitForState(job, JobState.SUCCEEDED);
  }

  @Test
  public void testJobError() throws Exception {
    MRApp app = new MRApp(1, 0, false, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 1, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task task = it.next();
    app.waitForState(task, TaskState.RUNNING);

    //send an invalid event on task at current state
    app.getContext().getEventHandler().handle(
        new TaskEvent(
            task.getID(), TaskEventType.T_SCHEDULE));

    //this must lead to job error
    app.waitForState(job, JobState.ERROR);
  }

  @SuppressWarnings("resource")
  @Test
  public void testJobSuccess() throws Exception {
    MRApp app = new MRApp(2, 2, true, this.getClass().getName(), true, false);
    JobImpl job = (JobImpl) app.submit(new Configuration());
    app.waitForInternalState(job, JobStateInternal.SUCCEEDED);
    // AM is not unregistered
    Assert.assertEquals(JobState.RUNNING, job.getState());
    // imitate that AM is unregistered
    app.successfullyUnregistered.set(true);
    app.waitForState(job, JobState.SUCCEEDED);
  }

  @Test
  public void testJobRebootNotLastRetryOnUnregistrationFailure()
      throws Exception {
    MRApp app = new MRApp(1, 0, false, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 1, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task task = it.next();
    app.waitForState(task, TaskState.RUNNING);

    //send an reboot event
    app.getContext().getEventHandler().handle(new JobEvent(job.getID(),
      JobEventType.JOB_AM_REBOOT));

    // return exteranl state as RUNNING since otherwise the JobClient will
    // prematurely exit.
    app.waitForState(job, JobState.RUNNING);
  }

  @Test
  public void testJobRebootOnLastRetryOnUnregistrationFailure()
      throws Exception {
    // make startCount as 2 since this is last retry which equals to
    // DEFAULT_MAX_AM_RETRY
    // The last param mocks the unregistration failure
    MRApp app = new MRApp(1, 0, false, this.getClass().getName(), true, 2, false);

    Configuration conf = new Configuration();
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 1, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task task = it.next();
    app.waitForState(task, TaskState.RUNNING);

    //send an reboot event
    app.getContext().getEventHandler().handle(new JobEvent(job.getID(),
      JobEventType.JOB_AM_REBOOT));

    app.waitForInternalState((JobImpl) job, JobStateInternal.REBOOT);
    // return exteranl state as RUNNING if this is the last retry while
    // unregistration fails
    app.waitForState(job, JobState.RUNNING);
  }

  private final class MRAppWithSpiedJob extends MRApp {
    private JobImpl spiedJob;

    private MRAppWithSpiedJob(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    @Override
    protected Job createJob(Configuration conf, JobStateInternal forcedState, 
        String diagnostic) {
      spiedJob = spy((JobImpl) super.createJob(conf, forcedState, diagnostic));
      ((AppContext) getContext()).getAllJobs().put(spiedJob.getID(), spiedJob);
      return spiedJob;
    }
  }

  @Test
  public void testCountersOnJobFinish() throws Exception {
    MRAppWithSpiedJob app =
        new MRAppWithSpiedJob(1, 1, true, this.getClass().getName(), true);
    JobImpl job = (JobImpl)app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    System.out.println(job.getAllCounters());
    // Just call getCounters
    job.getAllCounters();
    job.getAllCounters();
    // Should be called only once
    verify(job, times(1)).constructFinalFullcounters();
  }

  @Test
  public void checkJobStateTypeConversion() {
    //verify that all states can be converted without 
    // throwing an exception
    for (JobState state : JobState.values()) {
      TypeConverter.fromYarn(state);
    }
  }

  @Test
  public void checkTaskStateTypeConversion() {
    //verify that all states can be converted without 
    // throwing an exception
    for (TaskState state : TaskState.values()) {
      TypeConverter.fromYarn(state);
    }
  }

  private Container containerObtainedByContainerLauncher;
  @Test
  public void testContainerPassThrough() throws Exception {
    MRApp app = new MRApp(0, 1, true, this.getClass().getName(), true) {
      @Override
      protected ContainerLauncher createContainerLauncher(AppContext context) {
        return new MockContainerLauncher() {
          @Override
          public void handle(ContainerLauncherEvent event) {
            if (event instanceof ContainerRemoteLaunchEvent) {
              containerObtainedByContainerLauncher =
                  ((ContainerRemoteLaunchEvent) event).getAllocatedContainer();
            }
            super.handle(event);
          }
        };
      };
    };
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    Collection<Task> tasks = job.getTasks().values();
    Collection<TaskAttempt> taskAttempts =
        tasks.iterator().next().getAttempts().values();
    TaskAttemptImpl taskAttempt =
        (TaskAttemptImpl) taskAttempts.iterator().next();
    // Container from RM should pass through to the launcher. Container object
    // should be the same.
   Assert.assertTrue(taskAttempt.container 
     == containerObtainedByContainerLauncher);
  }

  private final class MRAppWithHistory extends MRApp {
    public MRAppWithHistory(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart, int startCount) {
      super(maps, reduces, autoComplete, testName, cleanOnStart, startCount);
    }

    @Override
    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context, 
          getStartCount());
      return eventHandler;
    }
  }

  public static void main(String[] args) throws Exception {
    TestMRApp t = new TestMRApp();
    t.testMapReduce();
    t.testZeroMapReduces();
    t.testCommitPending();
    t.testCompletedMapsForReduceSlowstart();
    t.testJobError();
    t.testCountersOnJobFinish();
  }
}
