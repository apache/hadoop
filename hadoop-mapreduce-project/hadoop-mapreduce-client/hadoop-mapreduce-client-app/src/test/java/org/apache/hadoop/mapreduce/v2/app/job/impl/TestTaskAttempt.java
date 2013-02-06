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
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletion;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
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
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestTaskAttempt{
  @Test
  public void testAttemptContainerRequest() throws Exception {
    //WARNING: This test must run first.  This is because there is an 
    // optimization where the credentials passed in are cached statically so 
    // they do not need to be recomputed when creating a new 
    // ContainerLaunchContext. if other tests run first this code will cache
    // their credentials and this test will fail trying to look for the
    // credentials it inserted in.
    final Text SECRET_KEY_ALIAS = new Text("secretkeyalias");
    final byte[] SECRET_KEY = ("secretkey").getBytes();
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(1);
    acls.put(ApplicationAccessType.VIEW_APP, "otheruser");
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    Path jobFile = mock(Path.class);

    EventHandler eventHandler = mock(EventHandler.class);
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");

    // setup UGI for security so tokens and keys are preserved
    jobConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(jobConf);

    Credentials credentials = new Credentials();
    credentials.addSecretKey(SECRET_KEY_ALIAS, SECRET_KEY);
    Token<JobTokenIdentifier> jobToken = new Token<JobTokenIdentifier>(
        ("tokenid").getBytes(), ("tokenpw").getBytes(),
        new Text("tokenkind"), new Text("tokenservice"));
    
    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            mock(TaskSplitMetaInfo.class), jobConf, taListener,
            jobToken, credentials, new SystemClock(), null);

    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, taImpl.getID().toString());
    ContainerId containerId = BuilderUtils.newContainerId(1, 1, 1, 1);
    
    ContainerLaunchContext launchCtx =
        TaskAttemptImpl.createContainerLaunchContext(acls, containerId,
            jobConf, jobToken, taImpl.createRemoteTask(),
            TypeConverter.fromYarn(jobId), mock(Resource.class),
            mock(WrappedJvmID.class), taListener,
            credentials);

    Assert.assertEquals("ACLs mismatch", acls, launchCtx.getApplicationACLs());
    Credentials launchCredentials = new Credentials();

    DataInputByteBuffer dibb = new DataInputByteBuffer();
    dibb.reset(launchCtx.getContainerTokens());
    launchCredentials.readTokenStorageStream(dibb);

    // verify all tokens specified for the task attempt are in the launch context
    for (Token<? extends TokenIdentifier> token : credentials.getAllTokens()) {
      Token<? extends TokenIdentifier> launchToken =
          launchCredentials.getToken(token.getService());
      Assert.assertNotNull("Token " + token.getService() + " is missing",
          launchToken);
      Assert.assertEquals("Token " + token.getService() + " mismatch",
          token, launchToken);
    }

    // verify the secret key is in the launch context
    Assert.assertNotNull("Secret key missing",
        launchCredentials.getSecretKey(SECRET_KEY_ALIAS));
    Assert.assertTrue("Secret key mismatch", Arrays.equals(SECRET_KEY,
        launchCredentials.getSecretKey(SECRET_KEY_ALIAS)));
  }

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
  
  private TaskAttemptImpl createMapTaskAttemptImplForTest(
      EventHandler eventHandler, TaskSplitMetaInfo taskSplitMetaInfo) {
    Clock clock = new SystemClock();
    return createMapTaskAttemptImplForTest(eventHandler, taskSplitMetaInfo, clock);
  }
  
  private TaskAttemptImpl createMapTaskAttemptImplForTest(
      EventHandler eventHandler, TaskSplitMetaInfo taskSplitMetaInfo, Clock clock) {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
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
  
  @Test
  public void testLaunchFailedWhileKilling() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId = 
      BuilderUtils.newApplicationAttemptId(appId, 0);
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
          mock(Token.class), new Credentials(),
          new SystemClock(), null);

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
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
  }
  
  @Test
  public void testContainerCleanedWhileRunning() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId =
      BuilderUtils.newApplicationAttemptId(appId, 0);
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
    when(clusterInfo.getMinContainerCapability()).thenReturn(resource);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          mock(Token.class), new Credentials(),
          new SystemClock(), appCtx);

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
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
  }

  @Test
  public void testContainerCleanedWhileCommitting() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId =
      BuilderUtils.newApplicationAttemptId(appId, 0);
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
    when(clusterInfo.getMinContainerCapability()).thenReturn(resource);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          mock(Token.class), new Credentials(),
          new SystemClock(), appCtx);

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
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
  }
  
  @Test
  public void testDoubleTooManyFetchFailure() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId =
      BuilderUtils.newApplicationAttemptId(appId, 0);
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
    when(clusterInfo.getMinContainerCapability()).thenReturn(resource);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
          splits, jobConf, taListener,
          mock(Token.class), new Credentials(),
          new SystemClock(), appCtx);

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
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
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
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
    when(clusterInfo.getMinContainerCapability()).thenReturn(resource);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            splits, jobConf, taListener, mock(Token.class),
            new Credentials(), new SystemClock(), appCtx);

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
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
  }

  @Test
  public void testAppDiognosticEventOnNewTask() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
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
    when(clusterInfo.getMinContainerCapability()).thenReturn(resource);
    when(resource.getMemory()).thenReturn(1024);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener, 
        mock(Token.class), new Credentials(), new SystemClock(), appCtx);

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
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
    
  
  public static class MockEventHandler implements EventHandler {
    public boolean internalError;
    
    @Override
    public void handle(Event event) {
      if (event instanceof JobEvent) {
        JobEvent je = ((JobEvent) event);
        if (JobEventType.INTERNAL_ERROR == je.getType()) {
          internalError = true;
        }
      }
    }
    
  };
}
