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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletion;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.ControlledClock;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.sun.source.tree.AssertTree;

@SuppressWarnings("unchecked")
public class TestTaskAttempt{

  @Test
  public void testMRAppHistoryForMap() throws Exception {
    MRApp app = new FailingAttemptsMRApp(1, 0);
    testMRAppHistory(app);
  }

  @Test
  public void testMRAppHistoryForReduce() throws Exception {
    MRApp app = new FailingAttemptsMRApp(0, 1);
    testMRAppHistory(app);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testSingleRackRequest() throws Exception {
    TaskAttemptImpl.RequestContainerTransition rct =
        new TaskAttemptImpl.RequestContainerTransition(false);

    EventHandler eventHandler = mock(EventHandler.class);
    String[] hosts = new String[3];
    hosts[0] = "host1";
    hosts[1] = "host2";
    hosts[2] = "host3";
    TaskSplitMetaInfo splitInfo =
        new TaskSplitMetaInfo(hosts, 0, 128 * 1024 * 1024l);

    TaskAttemptImpl mockTaskAttempt =
        createMapTaskAttemptImplForTest(eventHandler, splitInfo);
    TaskAttemptEvent mockTAEvent = mock(TaskAttemptEvent.class);

    rct.transition(mockTaskAttempt, mockTAEvent);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(2)).handle(arg.capture());
    if (!(arg.getAllValues().get(1) instanceof ContainerRequestEvent)) {
      Assert.fail("Second Event not of type ContainerRequestEvent");
    }
    ContainerRequestEvent cre =
        (ContainerRequestEvent) arg.getAllValues().get(1);
    String[] requestedRacks = cre.getRacks();
    //Only a single occurance of /DefaultRack
    assertEquals(1, requestedRacks.length);
  }
 
  @SuppressWarnings("rawtypes")
  @Test
  public void testHostResolveAttempt() throws Exception {
    TaskAttemptImpl.RequestContainerTransition rct =
        new TaskAttemptImpl.RequestContainerTransition(false);

    EventHandler eventHandler = mock(EventHandler.class);
    String[] hosts = new String[3];
    hosts[0] = "192.168.1.1";
    hosts[1] = "host2";
    hosts[2] = "host3";
    TaskSplitMetaInfo splitInfo =
        new TaskSplitMetaInfo(hosts, 0, 128 * 1024 * 1024l);

    TaskAttemptImpl mockTaskAttempt =
        createMapTaskAttemptImplForTest(eventHandler, splitInfo);
    TaskAttemptImpl spyTa = spy(mockTaskAttempt);
    when(spyTa.resolveHost(hosts[0])).thenReturn("host1");

    TaskAttemptEvent mockTAEvent = mock(TaskAttemptEvent.class);
    rct.transition(spyTa, mockTAEvent);
    verify(spyTa).resolveHost(hosts[0]);
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(2)).handle(arg.capture());
    if (!(arg.getAllValues().get(1) instanceof ContainerRequestEvent)) {
      Assert.fail("Second Event not of type ContainerRequestEvent");
    }
    Map<String, Boolean> expected = new HashMap<String, Boolean>();
    expected.put("host1", true);
    expected.put("host2", true);
    expected.put("host3", true);
    ContainerRequestEvent cre =
        (ContainerRequestEvent) arg.getAllValues().get(1);
    String[] requestedHosts = cre.getHosts();
    for (String h : requestedHosts) {
      expected.remove(h);
    }
    assertEquals(0, expected.size());
  }
  
  @Test
  public void testSlotMillisCounterUpdate() throws Exception {
    verifySlotMillis(2048, 2048, 1024);
    verifySlotMillis(2048, 1024, 1024);
    verifySlotMillis(10240, 1024, 2048);
  }

  public void verifySlotMillis(int mapMemMb, int reduceMemMb,
      int minContainerSize) throws Exception {
    Clock actualClock = new SystemClock();
    ControlledClock clock = new ControlledClock(actualClock);
    clock.setTime(10);
    MRApp app =
        new MRApp(1, 1, false, "testSlotMillisCounterUpdate", true, clock);
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MAP_MEMORY_MB, mapMemMb);
    conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, reduceMemMb);
    app.setClusterInfo(new ClusterInfo(BuilderUtils
        .newResource(minContainerSize), BuilderUtils.newResource(10240)));

    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", 2, tasks.size());
    Iterator<Task> taskIter = tasks.values().iterator();
    Task mTask = taskIter.next();
    app.waitForState(mTask, TaskState.RUNNING);
    Task rTask = taskIter.next();
    app.waitForState(rTask, TaskState.RUNNING);
    Map<TaskAttemptId, TaskAttempt> mAttempts = mTask.getAttempts();
    Assert.assertEquals("Num attempts is not correct", 1, mAttempts.size());
    Map<TaskAttemptId, TaskAttempt> rAttempts = rTask.getAttempts();
    Assert.assertEquals("Num attempts is not correct", 1, rAttempts.size());
    TaskAttempt mta = mAttempts.values().iterator().next();
    TaskAttempt rta = rAttempts.values().iterator().next();
    app.waitForState(mta, TaskAttemptState.RUNNING);
    app.waitForState(rta, TaskAttemptState.RUNNING);

    clock.setTime(11);
    app.getContext()
        .getEventHandler()
        .handle(new TaskAttemptEvent(mta.getID(), TaskAttemptEventType.TA_DONE));
    app.getContext()
        .getEventHandler()
        .handle(new TaskAttemptEvent(rta.getID(), TaskAttemptEventType.TA_DONE));
    app.waitForState(job, JobState.SUCCEEDED);
    Assert.assertEquals(mta.getFinishTime(), 11);
    Assert.assertEquals(mta.getLaunchTime(), 10);
    Assert.assertEquals(rta.getFinishTime(), 11);
    Assert.assertEquals(rta.getLaunchTime(), 10);
    Assert.assertEquals((int) Math.ceil((float) mapMemMb / minContainerSize),
        job.getAllCounters().findCounter(JobCounter.SLOTS_MILLIS_MAPS)
            .getValue());
    Assert.assertEquals(
        (int) Math.ceil((float) reduceMemMb / minContainerSize), job
            .getAllCounters().findCounter(JobCounter.SLOTS_MILLIS_REDUCES)
            .getValue());
  }
  
  @SuppressWarnings("rawtypes")
  private TaskAttemptImpl createMapTaskAttemptImplForTest(
      EventHandler eventHandler, TaskSplitMetaInfo taskSplitMetaInfo) {
    Clock clock = new SystemClock();
    return createMapTaskAttemptImplForTest(eventHandler, taskSplitMetaInfo, clock);
  }
  
  @SuppressWarnings("rawtypes")
  private TaskAttemptImpl createMapTaskAttemptImplForTest(
      EventHandler eventHandler, TaskSplitMetaInfo taskSplitMetaInfo, Clock clock) {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    Path jobFile = mock(Path.class);
    JobConf jobConf = new JobConf();
    OutputCommitter outputCommitter = mock(OutputCommitter.class);
    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            taskSplitMetaInfo, jobConf, taListener, outputCommitter, null,
            null, clock, null);
    return taImpl;
  }

  private void testMRAppHistory(MRApp app) throws Exception {
    Configuration conf = new Configuration();
    Job job = app.submit(conf);
    app.waitForState(job, JobState.FAILED);
    Map<TaskId, Task> tasks = job.getTasks();

    Assert.assertEquals("Num tasks is not correct", 1, tasks.size());
    Task task = tasks.values().iterator().next();
    Assert.assertEquals("Task state not correct", TaskState.FAILED, task
        .getReport().getTaskState());
    Map<TaskAttemptId, TaskAttempt> attempts = tasks.values().iterator().next()
        .getAttempts();
    Assert.assertEquals("Num attempts is not correct", 4, attempts.size());

    Iterator<TaskAttempt> it = attempts.values().iterator();
    TaskAttemptReport report = it.next().getReport();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.FAILED,
        report.getTaskAttemptState());
    Assert.assertEquals("Diagnostic Information is not Correct",
        "Test Diagnostic Event", report.getDiagnosticInfo());
    report = it.next().getReport();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.FAILED,
        report.getTaskAttemptState());
  }

  static class FailingAttemptsMRApp extends MRApp {
    FailingAttemptsMRApp(int maps, int reduces) {
      super(maps, reduces, true, "FailingAttemptsMRApp", true);
    }

    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      getContext().getEventHandler().handle(
          new TaskAttemptDiagnosticsUpdateEvent(attemptID,
              "Test Diagnostic Event"));
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_FAILMSG));
    }

    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      return new EventHandler<JobHistoryEvent>() {
        @Override
        public void handle(JobHistoryEvent event) {
          if (event.getType() == org.apache.hadoop.mapreduce.jobhistory.EventType.MAP_ATTEMPT_FAILED) {
            TaskAttemptUnsuccessfulCompletion datum = (TaskAttemptUnsuccessfulCompletion) event
                .getHistoryEvent().getDatum();
            Assert.assertEquals("Diagnostic Information is not Correct",
                "Test Diagnostic Event", datum.get(8).toString());
          }
        }
      };
    }
  }
}
