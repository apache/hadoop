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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletion;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Locality;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptKilledEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestTaskAttempt{
	
  static public class StubbedFS extends RawLocalFileSystem {
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(1, false, 1, 1, 1, f);
    }
  }

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

  @Test
  public void testMRAppHistoryForTAFailedInAssigned() throws Exception {
    // test TA_CONTAINER_LAUNCH_FAILED for map
    FailingAttemptsDuringAssignedMRApp app =
        new FailingAttemptsDuringAssignedMRApp(1, 0,
            TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED);
    testTaskAttemptAssignedFailHistory(app);

    // test TA_CONTAINER_LAUNCH_FAILED for reduce
    app =
        new FailingAttemptsDuringAssignedMRApp(0, 1,
            TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED);
    testTaskAttemptAssignedFailHistory(app);

    // test TA_CONTAINER_COMPLETED for map
    app =
        new FailingAttemptsDuringAssignedMRApp(1, 0,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED);
    testTaskAttemptAssignedFailHistory(app);

    // test TA_CONTAINER_COMPLETED for reduce
    app =
        new FailingAttemptsDuringAssignedMRApp(0, 1,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED);
    testTaskAttemptAssignedFailHistory(app);

    // test TA_FAILMSG for map
    app =
        new FailingAttemptsDuringAssignedMRApp(1, 0,
            TaskAttemptEventType.TA_FAILMSG);
    testTaskAttemptAssignedFailHistory(app);

    // test TA_FAILMSG for reduce
    app =
        new FailingAttemptsDuringAssignedMRApp(0, 1,
            TaskAttemptEventType.TA_FAILMSG);
    testTaskAttemptAssignedFailHistory(app);

    // test TA_KILL for map
    app =
        new FailingAttemptsDuringAssignedMRApp(1, 0,
            TaskAttemptEventType.TA_KILL);
    testTaskAttemptAssignedKilledHistory(app);

    // test TA_KILL for reduce
    app =
        new FailingAttemptsDuringAssignedMRApp(0, 1,
            TaskAttemptEventType.TA_KILL);
    testTaskAttemptAssignedKilledHistory(app);
  }

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
    //Only a single occurrence of /DefaultRack
    assertEquals(1, requestedRacks.length);
  }

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
    spyTa.dataLocalHosts = spyTa.resolveHosts(splitInfo.getLocations());

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
  public void testMillisCountersUpdate() throws Exception {
    verifyMillisCounters(2048, 2048, 1024);
    verifyMillisCounters(2048, 1024, 1024);
    verifyMillisCounters(10240, 1024, 2048);
  }

  public void verifyMillisCounters(int mapMemMb, int reduceMemMb,
      int minContainerSize) throws Exception {
    Clock actualClock = new SystemClock();
    ControlledClock clock = new ControlledClock(actualClock);
    clock.setTime(10);
    MRApp app =
        new MRApp(1, 1, false, "testSlotMillisCounterUpdate", true, clock);
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MAP_MEMORY_MB, mapMemMb);
    conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, reduceMemMb);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 
      minContainerSize);
    app.setClusterInfo(new ClusterInfo(Resource.newInstance(10240, 1)));

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
    Counters counters = job.getAllCounters();
    Assert.assertEquals((int) Math.ceil((float) mapMemMb / minContainerSize),
        counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS).getValue());
    Assert.assertEquals((int) Math.ceil((float) reduceMemMb / minContainerSize),
        counters.findCounter(JobCounter.SLOTS_MILLIS_REDUCES).getValue());
    Assert.assertEquals(1,
        counters.findCounter(JobCounter.MILLIS_MAPS).getValue());
    Assert.assertEquals(1,
        counters.findCounter(JobCounter.MILLIS_REDUCES).getValue());
    Assert.assertEquals(mapMemMb,
        counters.findCounter(JobCounter.MB_MILLIS_MAPS).getValue());
    Assert.assertEquals(reduceMemMb,
        counters.findCounter(JobCounter.MB_MILLIS_REDUCES).getValue());
    Assert.assertEquals(1,
        counters.findCounter(JobCounter.VCORES_MILLIS_MAPS).getValue());
    Assert.assertEquals(1,
        counters.findCounter(JobCounter.VCORES_MILLIS_REDUCES).getValue());
  }

  private TaskAttemptImpl createMapTaskAttemptImplForTest(
      EventHandler eventHandler, TaskSplitMetaInfo taskSplitMetaInfo) {
    Clock clock = new SystemClock();
    return createMapTaskAttemptImplForTest(eventHandler, taskSplitMetaInfo, clock);
  }

  private TaskAttemptImpl createMapTaskAttemptImplForTest(
      EventHandler eventHandler, TaskSplitMetaInfo taskSplitMetaInfo, Clock clock) {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    Path jobFile = mock(Path.class);
    JobConf jobConf = new JobConf();
    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            taskSplitMetaInfo, jobConf, taListener, null,
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

  private void testTaskAttemptAssignedFailHistory
      (FailingAttemptsDuringAssignedMRApp app) throws Exception {
    Configuration conf = new Configuration();
    Job job = app.submit(conf);
    app.waitForState(job, JobState.FAILED);
    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertTrue("No Ta Started JH Event", app.getTaStartJHEvent());
    Assert.assertTrue("No Ta Failed JH Event", app.getTaFailedJHEvent());
  }

  private void testTaskAttemptAssignedKilledHistory
      (FailingAttemptsDuringAssignedMRApp app) throws Exception {
    Configuration conf = new Configuration();
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Map<TaskId, Task> tasks = job.getTasks();
    Task task = tasks.values().iterator().next();
    app.waitForState(task, TaskState.SCHEDULED);
    Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
    TaskAttempt attempt = attempts.values().iterator().next();
    app.waitForState(attempt, TaskAttemptState.KILLED);
    Assert.assertTrue("No Ta Started JH Event", app.getTaStartJHEvent());
    Assert.assertTrue("No Ta Killed JH Event", app.getTaKilledJHEvent());
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

  static class FailingAttemptsDuringAssignedMRApp extends MRApp {
    FailingAttemptsDuringAssignedMRApp(int maps, int reduces,
        TaskAttemptEventType event) {
      super(maps, reduces, true, "FailingAttemptsMRApp", true);
      sendFailEvent = event;
    }

   TaskAttemptEventType sendFailEvent;

   @Override
    protected void containerLaunched(TaskAttemptId attemptID,
        int shufflePort) {
      //do nothing, not send TA_CONTAINER_LAUNCHED event
    }

    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(attemptID, sendFailEvent));
    }

    private boolean receiveTaStartJHEvent = false;
    private boolean receiveTaFailedJHEvent = false;
    private boolean receiveTaKilledJHEvent = false;

    public boolean getTaStartJHEvent(){
      return receiveTaStartJHEvent;
    }

    public boolean getTaFailedJHEvent(){
      return receiveTaFailedJHEvent;
    }

    public boolean getTaKilledJHEvent(){
        return receiveTaKilledJHEvent;
    }

    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      return new EventHandler<JobHistoryEvent>() {
        @Override
        public void handle(JobHistoryEvent event) {
          if (event.getType() == org.apache.hadoop.mapreduce.jobhistory.
              EventType.MAP_ATTEMPT_FAILED) {
            receiveTaFailedJHEvent = true;
          } else if (event.getType() == org.apache.hadoop.mapreduce.
              jobhistory.EventType.MAP_ATTEMPT_KILLED) {
            receiveTaKilledJHEvent = true;
          } else if (event.getType() == org.apache.hadoop.mapreduce.
              jobhistory.EventType.MAP_ATTEMPT_STARTED) {
            receiveTaStartJHEvent = true;
          } else if (event.getType() == org.apache.hadoop.mapreduce.
              jobhistory.EventType.REDUCE_ATTEMPT_FAILED) {
            receiveTaFailedJHEvent = true;
          } else if (event.getType() == org.apache.hadoop.mapreduce.
                  jobhistory.EventType.REDUCE_ATTEMPT_KILLED) {
            receiveTaKilledJHEvent = true;
          } else if (event.getType() == org.apache.hadoop.mapreduce.
              jobhistory.EventType.REDUCE_ATTEMPT_STARTED) {
            receiveTaStartJHEvent = true;
          }
        }
      };
    }
  }

  @Test
  public void testLaunchFailedWhileKilling() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId =
      ApplicationAttemptId.newInstance(appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] {"127.0.0.1"});

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          new Token(), new Credentials(),
          new SystemClock(), null);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId,
        container, mock(Map.class)));
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_KILL));
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED));
    assertFalse(eventHandler.internalError);
    assertEquals("Task attempt is not assigned on the local node", 
        Locality.NODE_LOCAL, taImpl.getLocality());
  }

  @Test
  public void testContainerCleanedWhileRunning() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId =
      ApplicationAttemptId.newInstance(appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] {"127.0.0.1"});

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          new Token(), new Credentials(),
          new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.2", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId,
        container, mock(Map.class)));
    taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
    assertEquals("Task attempt is not in running state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    assertFalse("InternalError occurred trying to handle TA_CONTAINER_CLEANED",
        eventHandler.internalError);
    assertEquals("Task attempt is not assigned on the local rack",
        Locality.RACK_LOCAL, taImpl.getLocality());
  }

  @Test
  public void testContainerCleanedWhileCommitting() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId =
      ApplicationAttemptId.newInstance(appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] {});

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          new Token(), new Credentials(),
          new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId,
        container, mock(Map.class)));
    taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_COMMIT_PENDING));

    assertEquals("Task attempt is not in commit pending state", taImpl.getState(),
        TaskAttemptState.COMMIT_PENDING);
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    assertFalse("InternalError occurred trying to handle TA_CONTAINER_CLEANED",
        eventHandler.internalError);
    assertEquals("Task attempt is assigned locally", Locality.OFF_SWITCH,
        taImpl.getLocality());
  }

  @Test
  public void testDoubleTooManyFetchFailure() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId =
      ApplicationAttemptId.newInstance(appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] {"127.0.0.1"});

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          new Token(), new Credentials(),
          new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId,
        container, mock(Map.class)));
    taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_DONE));
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_CONTAINER_CLEANED));

    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE));
    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE));
    assertEquals("Task attempt is not in FAILED state, still", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertFalse("InternalError occurred trying to handle TA_CONTAINER_CLEANED",
        eventHandler.internalError);
  }



  @Test
  public void testAppDiognosticEventOnUnassignedTask() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] { "127.0.0.1" });

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener,
        new Token(), new Credentials(), new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptDiagnosticsUpdateEvent(attemptId,
        "Task got killed"));
    assertFalse(
        "InternalError occurred trying to handle TA_DIAGNOSTICS_UPDATE on assigned task",
        eventHandler.internalError);
    try {
      taImpl.handle(new TaskAttemptEvent(attemptId,
          TaskAttemptEventType.TA_KILL));
      Assert.assertTrue("No exception on UNASSIGNED STATE KILL event", true);
    } catch (Exception e) {
      Assert.assertFalse(
          "Exception not expected for UNASSIGNED STATE KILL event", true);
    }
  }

  @Test
  public void testTooManyFetchFailureAfterKill() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId =
      ApplicationAttemptId.newInstance(appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] {"127.0.0.1"});

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
        splits, jobConf, taListener,
        mock(Token.class), new Credentials(),
        new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
      TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId,
      container, mock(Map.class)));
    taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
    taImpl.handle(new TaskAttemptEvent(attemptId,
      TaskAttemptEventType.TA_DONE));
    taImpl.handle(new TaskAttemptEvent(attemptId,
      TaskAttemptEventType.TA_CONTAINER_CLEANED));

    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
      TaskAttemptState.SUCCEEDED);
    taImpl.handle(new TaskAttemptEvent(attemptId,
      TaskAttemptEventType.TA_KILL));
    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
      TaskAttemptState.KILLED);
    taImpl.handle(new TaskAttemptEvent(attemptId,
      TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE));
    assertEquals("Task attempt is not in KILLED state, still", taImpl.getState(),
      TaskAttemptState.KILLED);
    assertFalse("InternalError occurred trying to handle TA_CONTAINER_CLEANED",
      eventHandler.internalError);
  }

  @Test
  public void testAppDiognosticEventOnNewTask() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] { "127.0.0.1" });

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener,
        new Token(), new Credentials(), new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");
    taImpl.handle(new TaskAttemptDiagnosticsUpdateEvent(attemptId,
        "Task got killed"));
    assertFalse(
        "InternalError occurred trying to handle TA_DIAGNOSTICS_UPDATE on assigned task",
        eventHandler.internalError);
  }
    
  @Test
  public void testFetchFailureAttemptFinishTime() throws Exception{
	ApplicationId appId = ApplicationId.newInstance(1, 2);
	ApplicationAttemptId appAttemptId =
	ApplicationAttemptId.newInstance(appId, 0);
	JobId jobId = MRBuilderUtils.newJobId(appId, 1);
	TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
	TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
	Path jobFile = mock(Path.class);

	MockEventHandler eventHandler = new MockEventHandler();
	TaskAttemptListener taListener = mock(TaskAttemptListener.class);
	when(taListener.getAddress()).thenReturn(
		new InetSocketAddress("localhost", 0));

	JobConf jobConf = new JobConf();
	jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
	jobConf.setBoolean("fs.file.impl.disable.cache", true);
	jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
	jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

	TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
	when(splits.getLocations()).thenReturn(new String[] {"127.0.0.1"});

	AppContext appCtx = mock(AppContext.class);
	ClusterInfo clusterInfo = mock(ClusterInfo.class);
	when(appCtx.getClusterInfo()).thenReturn(clusterInfo);

	TaskAttemptImpl taImpl =
	  new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
	  splits, jobConf, taListener,mock(Token.class), new Credentials(),
	  new SystemClock(), appCtx);

	NodeId nid = NodeId.newInstance("127.0.0.1", 0);
	ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
	Container container = mock(Container.class);
	when(container.getId()).thenReturn(contId);
	when(container.getNodeId()).thenReturn(nid);
	when(container.getNodeHttpAddress()).thenReturn("localhost:0"); 
	    
	taImpl.handle(new TaskAttemptEvent(attemptId,
	 	TaskAttemptEventType.TA_SCHEDULE));
	taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId,
	    container, mock(Map.class)));
	taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
	taImpl.handle(new TaskAttemptEvent(attemptId,
	    TaskAttemptEventType.TA_DONE));
	taImpl.handle(new TaskAttemptEvent(attemptId,
	    TaskAttemptEventType.TA_CONTAINER_CLEANED));
	    
	assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
		      TaskAttemptState.SUCCEEDED);
	
	assertTrue("Task Attempt finish time is not greater than 0", 
			taImpl.getFinishTime() > 0);
	
	Long finishTime = taImpl.getFinishTime();
	Thread.sleep(5);   
	taImpl.handle(new TaskAttemptEvent(attemptId,
	   TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE));
	
	assertEquals("Task attempt is not in Too Many Fetch Failure state", 
			taImpl.getState(), TaskAttemptState.FAILED);
	
	assertEquals("After TA_TOO_MANY_FETCH_FAILURE,"
		+ " Task attempt finish time is not the same ",
		finishTime, Long.valueOf(taImpl.getFinishTime()));  
  }

  private void containerKillBeforeAssignment(boolean scheduleAttempt)
      throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);

    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, mock(Path.class), 1,
            mock(TaskSplitMetaInfo.class), new JobConf(),
            mock(TaskAttemptListener.class), mock(Token.class),
            new Credentials(), new SystemClock(),
            mock(AppContext.class));
    if (scheduleAttempt) {
      taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
          TaskAttemptEventType.TA_SCHEDULE));
    }
    taImpl.handle(new TaskAttemptKillEvent(taImpl.getID(),"", true));
    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.KILLED);
    assertEquals("Task attempt's internal state is not KILLED",
        taImpl.getInternalState(), TaskAttemptStateInternal.KILLED);
    assertFalse("InternalError occurred", eventHandler.internalError);
    TaskEvent event = eventHandler.lastTaskEvent;
    assertEquals(TaskEventType.T_ATTEMPT_KILLED, event.getType());
    // In NEW state, new map attempt should not be rescheduled.
    assertFalse(((TaskTAttemptKilledEvent)event).getRescheduleAttempt());
  }

  @Test
  public void testContainerKillOnNew() throws Exception {
    containerKillBeforeAssignment(false);
  }

  @Test
  public void testContainerKillOnUnassigned() throws Exception {
    containerKillBeforeAssignment(true);
  }

  @Test
  public void testContainerKillAfterAssigned() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId,
        0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] { "127.0.0.1" });

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener, new Token(),
        new Credentials(), new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.2", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId, container,
        mock(Map.class)));
    assertEquals("Task attempt is not in assinged state",
        taImpl.getInternalState(), TaskAttemptStateInternal.ASSIGNED);
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_KILL));
    assertEquals("Task should be in KILL_CONTAINER_CLEANUP state",
        TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
        taImpl.getInternalState());
  }

  @Test
  public void testContainerKillWhileRunning() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId,
        0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] { "127.0.0.1" });

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener, new Token(),
        new Credentials(), new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.2", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId, container,
        mock(Map.class)));
    taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
    assertEquals("Task attempt is not in running state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_KILL));
    assertFalse("InternalError occurred trying to handle TA_KILL",
        eventHandler.internalError);
    assertEquals("Task should be in KILL_CONTAINER_CLEANUP state",
        TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
        taImpl.getInternalState());
  }

  @Test
  public void testContainerKillWhileCommitPending() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId,
        0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] { "127.0.0.1" });

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener, new Token(),
        new Credentials(), new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.2", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId, container,
        mock(Map.class)));
    taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
    assertEquals("Task attempt is not in running state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_COMMIT_PENDING));
    assertEquals("Task should be in COMMIT_PENDING state",
        TaskAttemptStateInternal.COMMIT_PENDING, taImpl.getInternalState());
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_KILL));
    assertFalse("InternalError occurred trying to handle TA_KILL",
        eventHandler.internalError);
    assertEquals("Task should be in KILL_CONTAINER_CLEANUP state",
        TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
        taImpl.getInternalState());
  }

  @Test
  public void testKillMapTaskAfterSuccess() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in SUCCEEDED state",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    // Send a map task attempt kill event indicating next map attempt has to be
    // reschedule
    taImpl.handle(new TaskAttemptKillEvent(taImpl.getID(),"", true));
    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.KILLED);
    assertEquals("Task attempt's internal state is not KILLED",
        taImpl.getInternalState(), TaskAttemptStateInternal.KILLED);
    assertFalse("InternalError occurred", eventHandler.internalError);
    TaskEvent event = eventHandler.lastTaskEvent;
    assertEquals(TaskEventType.T_ATTEMPT_KILLED, event.getType());
    // Send an attempt killed event to TaskImpl forwarding the same reschedule
    // flag we received in task attempt kill event.
    assertTrue(((TaskTAttemptKilledEvent)event).getRescheduleAttempt());
  }

  private TaskAttemptImpl createTaskAttemptImpl(
      MockEventHandler eventHandler) {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] {"127.0.0.1"});

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);

    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            splits, jobConf, taListener,
            mock(Token.class), new Credentials(),
            new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId,
        container, mock(Map.class)));
    taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
    return taImpl;
  }

  public static class MockEventHandler implements EventHandler {
    public boolean internalError;
    public TaskEvent lastTaskEvent;

    @Override
    public void handle(Event event) {
      if (event instanceof TaskEvent) {
        lastTaskEvent = (TaskEvent)event;
      }
      if (event instanceof JobEvent) {
        JobEvent je = ((JobEvent) event);
        if (JobEventType.INTERNAL_ERROR == je.getType()) {
          internalError = true;
        }
      }
    }

  };
}
