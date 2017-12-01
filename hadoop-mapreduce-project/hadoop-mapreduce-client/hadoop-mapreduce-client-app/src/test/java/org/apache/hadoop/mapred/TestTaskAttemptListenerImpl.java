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

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.checkpoint.EnumCounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.checkpoint.CheckpointID;
import org.apache.hadoop.mapreduce.checkpoint.FSCheckpointID;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.CheckpointAMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests the behavior of TaskAttemptListenerImpl.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestTaskAttemptListenerImpl {
  private static final String ATTEMPT1_ID =
      "attempt_123456789012_0001_m_000001_0";
  private static final String ATTEMPT2_ID =
      "attempt_123456789012_0001_m_000002_0";

  private static final TaskAttemptId TASKATTEMPTID1 =
      TypeConverter.toYarn(TaskAttemptID.forName(ATTEMPT1_ID));
  private static final TaskAttemptId TASKATTEMPTID2 =
      TypeConverter.toYarn(TaskAttemptID.forName(ATTEMPT2_ID));

  @Mock
  private AppContext appCtx;

  @Mock
  private JobTokenSecretManager secret;

  @Mock
  private RMHeartbeatHandler rmHeartbeatHandler;

  @Mock
  private TaskHeartbeatHandler hbHandler;

  @Mock
  private Dispatcher dispatcher;

  @Mock
  private Task task;

  @SuppressWarnings("rawtypes")
  @Mock
  private EventHandler<Event> ea;

  @SuppressWarnings("rawtypes")
  @Captor
  private ArgumentCaptor<Event> eventCaptor;

  private CheckpointAMPreemptionPolicy policy;
  private JVMId id;
  private WrappedJvmID wid;
  private TaskAttemptID attemptID;
  private TaskAttemptId attemptId;
  private ReduceTaskStatus firstReduceStatus;
  private ReduceTaskStatus secondReduceStatus;
  private ReduceTaskStatus thirdReduceStatus;

  private MockTaskAttemptListenerImpl listener;

  public static class MockTaskAttemptListenerImpl
      extends TaskAttemptListenerImpl {

    public MockTaskAttemptListenerImpl(AppContext context,
        JobTokenSecretManager jobTokenSecretManager,
        RMHeartbeatHandler rmHeartbeatHandler, AMPreemptionPolicy policy) {

      super(context, jobTokenSecretManager, rmHeartbeatHandler, policy);
    }

    public MockTaskAttemptListenerImpl(AppContext context,
        JobTokenSecretManager jobTokenSecretManager,
        RMHeartbeatHandler rmHeartbeatHandler,
        TaskHeartbeatHandler hbHandler,
        AMPreemptionPolicy policy) {

      super(context, jobTokenSecretManager, rmHeartbeatHandler, policy);
      this.taskHeartbeatHandler = hbHandler;
    }
    
    @Override
    protected void registerHeartbeatHandler(Configuration conf) {
      //Empty
    }

    @Override
    protected void startRpcServer() {
      //Empty
    }
    
    @Override
    protected void stopRpcServer() {
      //Empty
    }
  }

  @After
  public void after() throws IOException {
    if (listener != null) {
      listener.close();
      listener = null;
    }
  }

  @Test  (timeout=5000)
  public void testGetTask() throws IOException {
    configureMocks();
    startListener(false);

    // Verify ask before registration.
    //The JVM ID has not been registered yet so we should kill it.
    JvmContext context = new JvmContext();

    context.jvmId = id; 
    JvmTask result = listener.getTask(context);
    assertNotNull(result);
    assertTrue(result.shouldDie);

    // Verify ask after registration but before launch. 
    // Don't kill, should be null.
    //Now put a task with the ID
    listener.registerPendingTask(task, wid);
    result = listener.getTask(context);
    assertNull(result);
    // Unregister for more testing.
    listener.unregister(attemptId, wid);

    // Verify ask after registration and launch
    //Now put a task with the ID
    listener.registerPendingTask(task, wid);
    listener.registerLaunchedTask(attemptId, wid);
    verify(hbHandler).register(attemptId);
    result = listener.getTask(context);
    assertNotNull(result);
    assertFalse(result.shouldDie);
    // Don't unregister yet for more testing.

    //Verify that if we call it again a second time we are told to die.
    result = listener.getTask(context);
    assertNotNull(result);
    assertTrue(result.shouldDie);

    listener.unregister(attemptId, wid);

    // Verify after unregistration.
    result = listener.getTask(context);
    assertNotNull(result);
    assertTrue(result.shouldDie);

    // test JVMID
    JVMId jvmid = JVMId.forName("jvm_001_002_m_004");
    assertNotNull(jvmid);
    try {
      JVMId.forName("jvm_001_002_m_004_006");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(),
          "TaskId string : jvm_001_002_m_004_006 is not properly formed");
    }

  }

  @Test (timeout=5000)
  public void testJVMId() {

    JVMId jvmid = new JVMId("test", 1, true, 2);
    JVMId jvmid1 = JVMId.forName("jvm_test_0001_m_000002");
    // test compare methot should be the same
    assertEquals(0, jvmid.compareTo(jvmid1));
  }

  @Test (timeout=10000)
  public void testGetMapCompletionEvents() throws IOException {
    TaskAttemptCompletionEvent[] empty = {};
    TaskAttemptCompletionEvent[] taskEvents = {
        createTce(0, true, TaskAttemptCompletionEventStatus.OBSOLETE),
        createTce(1, false, TaskAttemptCompletionEventStatus.FAILED),
        createTce(2, true, TaskAttemptCompletionEventStatus.SUCCEEDED),
        createTce(3, false, TaskAttemptCompletionEventStatus.FAILED) };
    TaskAttemptCompletionEvent[] mapEvents = { taskEvents[0], taskEvents[2] };
    Job mockJob = mock(Job.class);
    when(mockJob.getTaskAttemptCompletionEvents(0, 100))
      .thenReturn(taskEvents);
    when(mockJob.getTaskAttemptCompletionEvents(0, 2))
      .thenReturn(Arrays.copyOfRange(taskEvents, 0, 2));
    when(mockJob.getTaskAttemptCompletionEvents(2, 100))
      .thenReturn(Arrays.copyOfRange(taskEvents, 2, 4));
    when(mockJob.getMapAttemptCompletionEvents(0, 100)).thenReturn(
        TypeConverter.fromYarn(mapEvents));
    when(mockJob.getMapAttemptCompletionEvents(0, 2)).thenReturn(
        TypeConverter.fromYarn(mapEvents));
    when(mockJob.getMapAttemptCompletionEvents(2, 100)).thenReturn(
        TypeConverter.fromYarn(empty));

    configureMocks();
    when(appCtx.getJob(any(JobId.class))).thenReturn(mockJob);

    listener = new MockTaskAttemptListenerImpl(
        appCtx, secret, rmHeartbeatHandler, policy) {
      @Override
      protected void registerHeartbeatHandler(Configuration conf) {
        taskHeartbeatHandler = hbHandler;
      }
    };
    Configuration conf = new Configuration();
    listener.init(conf);
    listener.start();

    JobID jid = new JobID("12345", 1);
    TaskAttemptID tid = new TaskAttemptID("12345", 1, TaskType.REDUCE, 1, 0);
    MapTaskCompletionEventsUpdate update =
        listener.getMapCompletionEvents(jid, 0, 100, tid);
    assertEquals(2, update.events.length);
    update = listener.getMapCompletionEvents(jid, 0, 2, tid);
    assertEquals(2, update.events.length);
    update = listener.getMapCompletionEvents(jid, 2, 100, tid);
    assertEquals(0, update.events.length);
  }

  private static TaskAttemptCompletionEvent createTce(int eventId,
      boolean isMap, TaskAttemptCompletionEventStatus status) {
    JobId jid = MRBuilderUtils.newJobId(12345, 1, 1);
    TaskId tid = MRBuilderUtils.newTaskId(jid, 0,
        isMap ? org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP
            : org.apache.hadoop.mapreduce.v2.api.records.TaskType.REDUCE);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(tid, 0);
    RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
    TaskAttemptCompletionEvent tce = recordFactory
        .newRecordInstance(TaskAttemptCompletionEvent.class);
    tce.setEventId(eventId);
    tce.setAttemptId(attemptId);
    tce.setStatus(status);
    return tce;
  }

  @Test (timeout=10000)
  public void testCommitWindow() throws IOException {
    SystemClock clock = SystemClock.getInstance();

    configureMocks();

    org.apache.hadoop.mapreduce.v2.app.job.Task mockTask =
        mock(org.apache.hadoop.mapreduce.v2.app.job.Task.class);
    when(mockTask.canCommit(any(TaskAttemptId.class))).thenReturn(true);
    Job mockJob = mock(Job.class);
    when(mockJob.getTask(any(TaskId.class))).thenReturn(mockTask);
    when(appCtx.getJob(any(JobId.class))).thenReturn(mockJob);
    when(appCtx.getClock()).thenReturn(clock);

    listener = new MockTaskAttemptListenerImpl(
        appCtx, secret, rmHeartbeatHandler, policy) {
      @Override
      protected void registerHeartbeatHandler(Configuration conf) {
        taskHeartbeatHandler = hbHandler;
      }
    };

    Configuration conf = new Configuration();
    listener.init(conf);
    listener.start();

    // verify commit not allowed when RM heartbeat has not occurred recently
    TaskAttemptID tid = new TaskAttemptID("12345", 1, TaskType.REDUCE, 1, 0);
    boolean canCommit = listener.canCommit(tid);
    assertFalse(canCommit);
    verify(mockTask, never()).canCommit(any(TaskAttemptId.class));

    // verify commit allowed when RM heartbeat is recent
    when(rmHeartbeatHandler.getLastHeartbeatTime())
      .thenReturn(clock.getTime());
    canCommit = listener.canCommit(tid);
    assertTrue(canCommit);
    verify(mockTask, times(1)).canCommit(any(TaskAttemptId.class));
  }

  @Test
  public void testCheckpointIDTracking()
    throws IOException, InterruptedException{
    SystemClock clock = SystemClock.getInstance();

    configureMocks();

    org.apache.hadoop.mapreduce.v2.app.job.Task mockTask =
        mock(org.apache.hadoop.mapreduce.v2.app.job.Task.class);
    when(mockTask.canCommit(any(TaskAttemptId.class))).thenReturn(true);
    Job mockJob = mock(Job.class);
    when(mockJob.getTask(any(TaskId.class))).thenReturn(mockTask);
    when(appCtx.getJob(any(JobId.class))).thenReturn(mockJob);
    when(appCtx.getClock()).thenReturn(clock);

    listener = new MockTaskAttemptListenerImpl(
        appCtx, secret, rmHeartbeatHandler, policy) {
      @Override
      protected void registerHeartbeatHandler(Configuration conf) {
        taskHeartbeatHandler = hbHandler;
      }
    };

    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.TASK_PREEMPTION, true);
    //conf.setBoolean("preemption.reduce", true);

    listener.init(conf);
    listener.start();

    TaskAttemptID tid = new TaskAttemptID("12345", 1, TaskType.REDUCE, 1, 0);

    List<Path> partialOut = new ArrayList<Path>();
    partialOut.add(new Path("/prev1"));
    partialOut.add(new Path("/prev2"));

    Counters counters = mock(Counters.class);
    final long CBYTES = 64L * 1024 * 1024;
    final long CTIME = 4344L;
    final Path CLOC = new Path("/test/1");
    Counter cbytes = mock(Counter.class);
    when(cbytes.getValue()).thenReturn(CBYTES);
    Counter ctime = mock(Counter.class);
    when(ctime.getValue()).thenReturn(CTIME);
    when(counters.findCounter(eq(EnumCounter.CHECKPOINT_BYTES)))
        .thenReturn(cbytes);
    when(counters.findCounter(eq(EnumCounter.CHECKPOINT_MS)))
        .thenReturn(ctime);

    // propagating a taskstatus that contains a checkpoint id
    TaskCheckpointID incid = new TaskCheckpointID(new FSCheckpointID(
          CLOC), partialOut, counters);
    listener.setCheckpointID(
        org.apache.hadoop.mapred.TaskID.downgrade(tid.getTaskID()), incid);

    // and try to get it back
    CheckpointID outcid = listener.getCheckpointID(tid.getTaskID());
    TaskCheckpointID tcid = (TaskCheckpointID) outcid;
    assertEquals(CBYTES, tcid.getCheckpointBytes());
    assertEquals(CTIME, tcid.getCheckpointTime());
    assertTrue(partialOut.containsAll(tcid.getPartialCommittedOutput()));
    assertTrue(tcid.getPartialCommittedOutput().containsAll(partialOut));

    //assert it worked
    assert outcid == incid;
  }

  @Test
  public void testStatusUpdateProgress()
      throws IOException, InterruptedException {
    configureMocks();
    startListener(true);
    verify(hbHandler).register(attemptId);

    // make sure a ping doesn't report progress
    AMFeedback feedback = listener.statusUpdate(attemptID, null);
    assertTrue(feedback.getTaskFound());
    verify(hbHandler, never()).progressing(eq(attemptId));

    // make sure a status update does report progress
    MapTaskStatus mockStatus = new MapTaskStatus(attemptID, 0.0f, 1,
        TaskStatus.State.RUNNING, "", "RUNNING", "", TaskStatus.Phase.MAP,
        new Counters());
    feedback = listener.statusUpdate(attemptID, mockStatus);
    assertTrue(feedback.getTaskFound());
    verify(hbHandler).progressing(eq(attemptId));
  }

  @Test
  public void testSingleStatusUpdate()
      throws IOException, InterruptedException {
    configureMocks();
    startListener(true);

    listener.statusUpdate(attemptID, firstReduceStatus);

    verify(ea).handle(eventCaptor.capture());
    TaskAttemptStatusUpdateEvent updateEvent =
        (TaskAttemptStatusUpdateEvent) eventCaptor.getValue();

    TaskAttemptStatus status = updateEvent.getTaskAttemptStatusRef().get();
    assertTrue(status.fetchFailedMaps.contains(TASKATTEMPTID1));
    assertEquals(1, status.fetchFailedMaps.size());
    assertEquals(Phase.SHUFFLE, status.phase);
  }

  @Test
  public void testStatusUpdateEventCoalescing()
      throws IOException, InterruptedException {
    configureMocks();
    startListener(true);

    listener.statusUpdate(attemptID, firstReduceStatus);
    listener.statusUpdate(attemptID, secondReduceStatus);

    verify(ea).handle(any(Event.class));
    ConcurrentMap<TaskAttemptId,
        AtomicReference<TaskAttemptStatus>> attemptIdToStatus =
        listener.getAttemptIdToStatus();
    TaskAttemptStatus status = attemptIdToStatus.get(attemptId).get();

    assertTrue(status.fetchFailedMaps.contains(TASKATTEMPTID1));
    assertTrue(status.fetchFailedMaps.contains(TASKATTEMPTID2));
    assertEquals(2, status.fetchFailedMaps.size());
    assertEquals(Phase.SORT, status.phase);
  }

  @Test
  public void testCoalescedStatusUpdatesCleared()
      throws IOException, InterruptedException {
    // First two events are coalesced, the third is not
    configureMocks();
    startListener(true);

    listener.statusUpdate(attemptID, firstReduceStatus);
    listener.statusUpdate(attemptID, secondReduceStatus);
    ConcurrentMap<TaskAttemptId,
        AtomicReference<TaskAttemptStatus>> attemptIdToStatus =
        listener.getAttemptIdToStatus();
    attemptIdToStatus.get(attemptId).set(null);
    listener.statusUpdate(attemptID, thirdReduceStatus);

    verify(ea, times(2)).handle(eventCaptor.capture());
    TaskAttemptStatusUpdateEvent updateEvent =
        (TaskAttemptStatusUpdateEvent) eventCaptor.getValue();

    TaskAttemptStatus status = updateEvent.getTaskAttemptStatusRef().get();
    assertNull(status.fetchFailedMaps);
    assertEquals(Phase.REDUCE, status.phase);
  }

  @Test(expected = IllegalStateException.class)
  public void testStatusUpdateFromUnregisteredTask()
      throws IOException, InterruptedException{
    configureMocks();
    startListener(false);

    listener.statusUpdate(attemptID, firstReduceStatus);
  }

  private void configureMocks() {
    firstReduceStatus = new ReduceTaskStatus(attemptID, 0.0f, 1,
        TaskStatus.State.RUNNING, "", "RUNNING", "", TaskStatus.Phase.SHUFFLE,
        new Counters());
    firstReduceStatus.addFetchFailedMap(TaskAttemptID.forName(ATTEMPT1_ID));

    secondReduceStatus = new ReduceTaskStatus(attemptID, 0.0f, 1,
        TaskStatus.State.RUNNING, "", "RUNNING", "", TaskStatus.Phase.SORT,
        new Counters());
    secondReduceStatus.addFetchFailedMap(TaskAttemptID.forName(ATTEMPT2_ID));

    thirdReduceStatus = new ReduceTaskStatus(attemptID, 0.0f, 1,
        TaskStatus.State.RUNNING, "", "RUNNING", "",
        TaskStatus.Phase.REDUCE, new Counters());

    when(dispatcher.getEventHandler()).thenReturn(ea);
    when(appCtx.getEventHandler()).thenReturn(ea);
    policy = new CheckpointAMPreemptionPolicy();
    policy.init(appCtx);
    listener = new MockTaskAttemptListenerImpl(appCtx, secret,
          rmHeartbeatHandler, hbHandler, policy);
    id = new JVMId("foo", 1, true, 1);
    wid = new WrappedJvmID(id.getJobId(), id.isMap, id.getId());
    attemptID = new TaskAttemptID("1", 1, TaskType.MAP, 1, 1);
    attemptId = TypeConverter.toYarn(attemptID);
  }

  private void startListener(boolean registerTask) {
    Configuration conf = new Configuration();

    listener.init(conf);
    listener.start();

    if (registerTask) {
      listener.registerPendingTask(task, wid);
      listener.registerLaunchedTask(attemptId, wid);
    }
  }
}
