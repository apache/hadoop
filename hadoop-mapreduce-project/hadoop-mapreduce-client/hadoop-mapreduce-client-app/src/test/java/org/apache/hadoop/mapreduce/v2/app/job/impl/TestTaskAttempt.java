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

import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptFailEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapred.ReduceTaskAttemptImpl;
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
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptFinishingMonitor;
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
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptTooManyFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptKilledEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestTaskAttempt{

  private static final String CUSTOM_RESOURCE_NAME = "a-custom-resource";

  static public class StubbedFS extends RawLocalFileSystem {
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(1, false, 1, 1, 1, f);
    }
  }

  private static class CustomResourceTypesConfigurationProvider
      extends LocalConfigurationProvider {

    @Override
    public InputStream getConfigurationInputStream(Configuration bootstrapConf,
        String name) throws YarnException, IOException {
      if (YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE.equals(name)) {
        return new ByteArrayInputStream(
            ("<configuration>\n" +
            " <property>\n" +
            "   <name>yarn.resource-types</name>\n" +
            "   <value>a-custom-resource</value>\n" +
            " </property>\n" +
            " <property>\n" +
            "   <name>yarn.resource-types.a-custom-resource.units</name>\n" +
            "   <value>G</value>\n" +
            " </property>\n" +
            "</configuration>\n").getBytes());
      } else {
        return super.getConfigurationInputStream(bootstrapConf, name);
      }
    }
  }

  private static class TestAppender extends AppenderSkeleton {

    private final List<LoggingEvent> logEvents = new CopyOnWriteArrayList<>();

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    public void close() {
    }

    @Override
    protected void append(LoggingEvent arg0) {
      logEvents.add(arg0);
    }

    private List<LoggingEvent> getLogEvents() {
      return logEvents;
    }
  }

  @BeforeClass
  public static void setupBeforeClass() {
    ResourceUtils.resetResourceTypes(new Configuration());
  }

  @After
  public void tearDown() {
    ResourceUtils.resetResourceTypes(new Configuration());
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

    // test TA_FAILMSG_BY_CLIENT for map
    app =
        new FailingAttemptsDuringAssignedMRApp(1, 0,
            TaskAttemptEventType.TA_FAILMSG_BY_CLIENT);
    testTaskAttemptAssignedFailHistory(app);

    // test TA_FAILMSG_BY_CLIENT for reduce
    app =
        new FailingAttemptsDuringAssignedMRApp(0, 1,
            TaskAttemptEventType.TA_FAILMSG_BY_CLIENT);
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
    verifyMillisCounters(Resource.newInstance(1024, 1), 512);
    verifyMillisCounters(Resource.newInstance(2048, 4), 1024);
    verifyMillisCounters(Resource.newInstance(10240, 8), 2048);
  }

  public void verifyMillisCounters(Resource containerResource,
      int minContainerSize) throws Exception {
    Clock actualClock = SystemClock.getInstance();
    ControlledClock clock = new ControlledClock(actualClock);
    clock.setTime(10);
    MRApp app =
        new MRApp(1, 1, false, "testSlotMillisCounterUpdate", true, clock);
    app.setAllocatedContainerResource(containerResource);
    Configuration conf = new Configuration();
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

    int memoryMb = (int) containerResource.getMemorySize();
    int vcores = containerResource.getVirtualCores();
    Assert.assertEquals((int) Math.ceil((float) memoryMb / minContainerSize),
        counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS).getValue());
    Assert.assertEquals((int) Math.ceil((float) memoryMb / minContainerSize),
        counters.findCounter(JobCounter.SLOTS_MILLIS_REDUCES).getValue());
    Assert.assertEquals(1,
        counters.findCounter(JobCounter.MILLIS_MAPS).getValue());
    Assert.assertEquals(1,
        counters.findCounter(JobCounter.MILLIS_REDUCES).getValue());
    Assert.assertEquals(memoryMb,
        counters.findCounter(JobCounter.MB_MILLIS_MAPS).getValue());
    Assert.assertEquals(memoryMb,
        counters.findCounter(JobCounter.MB_MILLIS_REDUCES).getValue());
    Assert.assertEquals(vcores,
        counters.findCounter(JobCounter.VCORES_MILLIS_MAPS).getValue());
    Assert.assertEquals(vcores,
        counters.findCounter(JobCounter.VCORES_MILLIS_REDUCES).getValue());
  }

  private TaskAttemptImpl createMapTaskAttemptImplForTest(
      EventHandler eventHandler, TaskSplitMetaInfo taskSplitMetaInfo) {
    Clock clock = SystemClock.getInstance();
    return createMapTaskAttemptImplForTest(eventHandler, taskSplitMetaInfo,
        clock, new JobConf());
  }

  private TaskAttemptImpl createMapTaskAttemptImplForTest(
      EventHandler eventHandler, TaskSplitMetaInfo taskSplitMetaInfo,
      Clock clock, JobConf jobConf) {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    Path jobFile = mock(Path.class);
    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            taskSplitMetaInfo, jobConf, taListener, null,
            null, clock, null);
    return taImpl;
  }

  private TaskAttemptImpl createReduceTaskAttemptImplForTest(
      EventHandler eventHandler, Clock clock, JobConf jobConf) {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.REDUCE);
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    Path jobFile = mock(Path.class);
    TaskAttemptImpl taImpl =
        new ReduceTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            1, jobConf, taListener, null,
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
    waitFor(app::getTaStartJHEvent, 100, 800);
    waitFor(app::getTaKilledJHEvent, 100, 800);
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
          new TaskAttemptFailEvent(attemptID));
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
          SystemClock.getInstance(), null);

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
    when(resource.getMemorySize()).thenReturn(1024L);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          new Token(), new Credentials(),
          SystemClock.getInstance(), appCtx);

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
    when(resource.getMemorySize()).thenReturn(1024L);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          new Token(), new Credentials(),
          SystemClock.getInstance(), appCtx);

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
    TaskId reduceTaskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.REDUCE);
    TaskAttemptId reduceTAId =
        MRBuilderUtils.newTaskAttemptId(reduceTaskId, 0);
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
    when(resource.getMemorySize()).thenReturn(1024L);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          new Token(), new Credentials(),
          SystemClock.getInstance(), appCtx);

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
        TaskAttemptEventType.TA_CONTAINER_COMPLETED));

    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    taImpl.handle(new TaskAttemptTooManyFetchFailureEvent(attemptId,
        reduceTAId, "Host"));
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
    when(resource.getMemorySize()).thenReturn(1024L);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener,
        new Token(), new Credentials(), SystemClock.getInstance(), appCtx);

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
    when(resource.getMemorySize()).thenReturn(1024L);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
        splits, jobConf, taListener,
        mock(Token.class), new Credentials(),
        SystemClock.getInstance(), appCtx);

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
      TaskAttemptEventType.TA_CONTAINER_COMPLETED));

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
    when(resource.getMemorySize()).thenReturn(1024L);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener,
        new Token(), new Credentials(), SystemClock.getInstance(), appCtx);

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
    TaskId reducetaskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.REDUCE);
    TaskAttemptId reduceTAId =
        MRBuilderUtils.newTaskAttemptId(reducetaskId, 0);
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
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
      splits, jobConf, taListener,mock(Token.class), new Credentials(),
      SystemClock.getInstance(), appCtx);

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
        TaskAttemptEventType.TA_CONTAINER_COMPLETED));

    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);

    assertTrue("Task Attempt finish time is not greater than 0",
        taImpl.getFinishTime() > 0);

    Long finishTime = taImpl.getFinishTime();
    Thread.sleep(5);
    taImpl.handle(new TaskAttemptTooManyFetchFailureEvent(attemptId,
        reduceTAId, "Host"));

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
            new Credentials(), SystemClock.getInstance(),
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
    when(resource.getMemorySize()).thenReturn(1024L);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener, new Token(),
        new Credentials(), SystemClock.getInstance(), appCtx);

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
    when(resource.getMemorySize()).thenReturn(1024L);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener, new Token(),
        new Credentials(), SystemClock.getInstance(), appCtx);

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
    when(resource.getMemorySize()).thenReturn(1024L);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener, new Token(),
        new Credentials(), SystemClock.getInstance(), appCtx);

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
  public void testKillMapTaskWhileSuccessFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

    // If the map task is killed when it is in SUCCESS_FINISHING_CONTAINER
    // state, the state will move to KILL_CONTAINER_CLEANUP
    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_KILL));
    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.KILLED);
    assertEquals("Task attempt's internal state is not KILL_CONTAINER_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    assertEquals("Task attempt's internal state is not KILL_TASK_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.KILL_TASK_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CLEANUP_DONE));

    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.KILLED);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testKillMapTaskAfterSuccess() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

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

  @Test
  public void testKillMapTaskWhileFailFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptFailEvent(taImpl.getID()));

    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals("Task attempt's internal state is not " +
        "FAIL_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER);

    // If the map task is killed when it is in FAIL_FINISHING_CONTAINER state,
    // the state will stay in FAIL_FINISHING_CONTAINER.
    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_KILL));
    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals("Task attempt's internal state is not " +
        "FAIL_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_TIMED_OUT));
    assertEquals("Task attempt's internal state is not FAIL_CONTAINER_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    assertEquals("Task attempt's internal state is not FAIL_TASK_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_TASK_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CLEANUP_DONE));

    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.FAILED);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testFailMapTaskByClient() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_FAILMSG_BY_CLIENT));

    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals("Task attempt's internal state is not " +
        "FAIL_CONTAINER_CLEANUP", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    assertEquals("Task attempt's internal state is not FAIL_TASK_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_TASK_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CLEANUP_DONE));

    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.FAILED);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testTaskAttemptDiagnosticEventOnFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

    // TA_DIAGNOSTICS_UPDATE doesn't change state
    taImpl.handle(new TaskAttemptDiagnosticsUpdateEvent(taImpl.getID(),
        "Task got updated"));
    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testTimeoutWhileSuccessFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

    // If the task stays in SUCCESS_FINISHING_CONTAINER for too long,
    // TaskAttemptListenerImpl will time out the attempt.
    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_TIMED_OUT));
    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_CONTAINER_CLEANUP", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testTimeoutWhileFailFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptFailEvent(taImpl.getID()));

    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals("Task attempt's internal state is not " +
        "FAIL_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER);

    // If the task stays in FAIL_FINISHING_CONTAINER for too long,
    // TaskAttemptListenerImpl will time out the attempt.
    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_TIMED_OUT));
    assertEquals("Task attempt's internal state is not FAIL_CONTAINER_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testMapperCustomResourceTypes() {
    initResourceTypes();
    EventHandler eventHandler = mock(EventHandler.class);
    TaskSplitMetaInfo taskSplitMetaInfo = new TaskSplitMetaInfo();
    Clock clock = SystemClock.getInstance();
    JobConf jobConf = new JobConf();
    jobConf.setLong(MRJobConfig.MAP_RESOURCE_TYPE_PREFIX
        + CUSTOM_RESOURCE_NAME, 7L);
    TaskAttemptImpl taImpl = createMapTaskAttemptImplForTest(eventHandler,
        taskSplitMetaInfo, clock, jobConf);
    ResourceInformation resourceInfo =
        getResourceInfoFromContainerRequest(taImpl, eventHandler).
        getResourceInformation(CUSTOM_RESOURCE_NAME);
    assertEquals("Expecting the default unit (G)",
        "G", resourceInfo.getUnits());
    assertEquals(7L, resourceInfo.getValue());
  }

  @Test
  public void testReducerCustomResourceTypes() {
    initResourceTypes();
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    JobConf jobConf = new JobConf();
    jobConf.set(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX
        + CUSTOM_RESOURCE_NAME, "3m");
    TaskAttemptImpl taImpl =
        createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
    ResourceInformation resourceInfo =
        getResourceInfoFromContainerRequest(taImpl, eventHandler).
        getResourceInformation(CUSTOM_RESOURCE_NAME);
    assertEquals("Expecting the specified unit (m)",
        "m", resourceInfo.getUnits());
    assertEquals(3L, resourceInfo.getValue());
  }

  @Test
  public void testReducerMemoryRequestViaMapreduceReduceMemoryMb() {
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    JobConf jobConf = new JobConf();
    jobConf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 2048);
    TaskAttemptImpl taImpl =
        createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
    long memorySize =
        getResourceInfoFromContainerRequest(taImpl, eventHandler).
        getMemorySize();
    assertEquals(2048, memorySize);
  }

  @Test
  public void testReducerMemoryRequestViaMapreduceReduceResourceMemory() {
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    JobConf jobConf = new JobConf();
    jobConf.set(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX +
        MRJobConfig.RESOURCE_TYPE_NAME_MEMORY, "2 Gi");
    TaskAttemptImpl taImpl =
        createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
    long memorySize =
        getResourceInfoFromContainerRequest(taImpl, eventHandler).
        getMemorySize();
    assertEquals(2048, memorySize);
  }

  @Test
  public void testReducerMemoryRequestDefaultMemory() {
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    TaskAttemptImpl taImpl =
        createReduceTaskAttemptImplForTest(eventHandler, clock, new JobConf());
    long memorySize =
        getResourceInfoFromContainerRequest(taImpl, eventHandler).
        getMemorySize();
    assertEquals(MRJobConfig.DEFAULT_REDUCE_MEMORY_MB, memorySize);
  }

  @Test
  public void testReducerMemoryRequestWithoutUnits() {
    Clock clock = SystemClock.getInstance();
    for (String memoryResourceName : ImmutableList.of(
        MRJobConfig.RESOURCE_TYPE_NAME_MEMORY,
        MRJobConfig.RESOURCE_TYPE_ALTERNATIVE_NAME_MEMORY)) {
      EventHandler eventHandler = mock(EventHandler.class);
      JobConf jobConf = new JobConf();
      jobConf.setInt(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX +
          memoryResourceName, 2048);
      TaskAttemptImpl taImpl =
          createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
      long memorySize =
          getResourceInfoFromContainerRequest(taImpl, eventHandler).
          getMemorySize();
      assertEquals(2048, memorySize);
    }
  }

  @Test
  public void testReducerMemoryRequestOverriding() {
    for (String memoryName : ImmutableList.of(
        MRJobConfig.RESOURCE_TYPE_NAME_MEMORY,
        MRJobConfig.RESOURCE_TYPE_ALTERNATIVE_NAME_MEMORY)) {
      TestAppender testAppender = new TestAppender();
      final Logger logger = Logger.getLogger(TaskAttemptImpl.class);
      try {
        logger.addAppender(testAppender);
        EventHandler eventHandler = mock(EventHandler.class);
        Clock clock = SystemClock.getInstance();
        JobConf jobConf = new JobConf();
        jobConf.set(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX + memoryName,
            "3Gi");
        jobConf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 2048);
        TaskAttemptImpl taImpl =
            createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
        long memorySize =
            getResourceInfoFromContainerRequest(taImpl, eventHandler).
            getMemorySize();
        assertEquals(3072, memorySize);
        assertTrue(testAppender.getLogEvents().stream()
            .anyMatch(e -> e.getLevel() == Level.WARN && ("Configuration " +
                "mapreduce.reduce.resource." + memoryName + "=3Gi is " +
                "overriding the mapreduce.reduce.memory.mb=2048 configuration")
                    .equals(e.getMessage())));
      } finally {
        logger.removeAppender(testAppender);
      }
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testReducerMemoryRequestMultipleName() {
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    JobConf jobConf = new JobConf();
    for (String memoryName : ImmutableList.of(
        MRJobConfig.RESOURCE_TYPE_NAME_MEMORY,
        MRJobConfig.RESOURCE_TYPE_ALTERNATIVE_NAME_MEMORY)) {
      jobConf.set(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX + memoryName,
          "3Gi");
    }
    createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
  }

  @Test
  public void testReducerCpuRequestViaMapreduceReduceCpuVcores() {
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    JobConf jobConf = new JobConf();
    jobConf.setInt(MRJobConfig.REDUCE_CPU_VCORES, 3);
    TaskAttemptImpl taImpl =
        createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
    int vCores =
        getResourceInfoFromContainerRequest(taImpl, eventHandler).
        getVirtualCores();
    assertEquals(3, vCores);
  }

  @Test
  public void testReducerCpuRequestViaMapreduceReduceResourceVcores() {
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    JobConf jobConf = new JobConf();
    jobConf.set(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX +
        MRJobConfig.RESOURCE_TYPE_NAME_VCORE, "5");
    TaskAttemptImpl taImpl =
        createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
    int vCores =
        getResourceInfoFromContainerRequest(taImpl, eventHandler).
        getVirtualCores();
    assertEquals(5, vCores);
  }

  @Test
  public void testReducerCpuRequestDefaultMemory() {
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    TaskAttemptImpl taImpl =
        createReduceTaskAttemptImplForTest(eventHandler, clock, new JobConf());
    int vCores =
        getResourceInfoFromContainerRequest(taImpl, eventHandler).
        getVirtualCores();
    assertEquals(MRJobConfig.DEFAULT_REDUCE_CPU_VCORES, vCores);
  }

  @Test
  public void testReducerCpuRequestOverriding() {
    TestAppender testAppender = new TestAppender();
    final Logger logger = Logger.getLogger(TaskAttemptImpl.class);
    try {
      logger.addAppender(testAppender);
      EventHandler eventHandler = mock(EventHandler.class);
      Clock clock = SystemClock.getInstance();
      JobConf jobConf = new JobConf();
      jobConf.set(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX +
          MRJobConfig.RESOURCE_TYPE_NAME_VCORE, "7");
      jobConf.setInt(MRJobConfig.REDUCE_CPU_VCORES, 9);
      TaskAttemptImpl taImpl =
          createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
      long vCores =
          getResourceInfoFromContainerRequest(taImpl, eventHandler).
          getVirtualCores();
      assertEquals(7, vCores);
      assertTrue(testAppender.getLogEvents().stream().anyMatch(
          e -> e.getLevel() == Level.WARN && ("Configuration " +
              "mapreduce.reduce.resource.vcores=7 is overriding the " +
              "mapreduce.reduce.cpu.vcores=9 configuration").equals(
                  e.getMessage())));
    } finally {
      logger.removeAppender(testAppender);
    }
  }

  private Resource getResourceInfoFromContainerRequest(
      TaskAttemptImpl taImpl, EventHandler eventHandler) {
    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_SCHEDULE));

    assertEquals("Task attempt is not in STARTING state", taImpl.getState(),
        TaskAttemptState.STARTING);

    ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(2)).handle(captor.capture());

    List<ContainerRequestEvent> containerRequestEvents = new ArrayList<>();
    for (Event e : captor.getAllValues()) {
      if (e instanceof ContainerRequestEvent) {
        containerRequestEvents.add((ContainerRequestEvent) e);
      }
    }
    assertEquals("Expected one ContainerRequestEvent after scheduling "
        + "task attempt", 1, containerRequestEvents.size());

    return containerRequestEvents.get(0).getCapability();
  }

  @Test(expected=IllegalArgumentException.class)
  public void testReducerCustomResourceTypeWithInvalidUnit() {
    initResourceTypes();
    EventHandler eventHandler = mock(EventHandler.class);
    Clock clock = SystemClock.getInstance();
    JobConf jobConf = new JobConf();
    jobConf.set(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX
        + CUSTOM_RESOURCE_NAME, "3z");
    createReduceTaskAttemptImplForTest(eventHandler, clock, jobConf);
  }

  private void initResourceTypes() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        CustomResourceTypesConfigurationProvider.class.getName());
    ResourceUtils.resetResourceTypes(conf);
  }

  private void setupTaskAttemptFinishingMonitor(
      EventHandler eventHandler, JobConf jobConf, AppContext appCtx) {
    TaskAttemptFinishingMonitor taskAttemptFinishingMonitor =
        new TaskAttemptFinishingMonitor(eventHandler);
    taskAttemptFinishingMonitor.init(jobConf);
    when(appCtx.getTaskAttemptFinishingMonitor()).
        thenReturn(taskAttemptFinishingMonitor);
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
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            splits, jobConf, taListener,
            mock(Token.class), new Credentials(),
            SystemClock.getInstance(), appCtx);

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
