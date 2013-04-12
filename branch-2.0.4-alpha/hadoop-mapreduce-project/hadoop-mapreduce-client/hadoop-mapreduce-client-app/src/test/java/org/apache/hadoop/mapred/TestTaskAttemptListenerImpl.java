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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;

public class TestTaskAttemptListenerImpl {
  public static class MockTaskAttemptListenerImpl extends TaskAttemptListenerImpl {

    public MockTaskAttemptListenerImpl(AppContext context,
        JobTokenSecretManager jobTokenSecretManager,
        RMHeartbeatHandler rmHeartbeatHandler,
        TaskHeartbeatHandler hbHandler) {
      super(context, jobTokenSecretManager, rmHeartbeatHandler);
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
  
  @Test
  public void testGetTask() throws IOException {
    AppContext appCtx = mock(AppContext.class);
    JobTokenSecretManager secret = mock(JobTokenSecretManager.class); 
    RMHeartbeatHandler rmHeartbeatHandler =
        mock(RMHeartbeatHandler.class);
    TaskHeartbeatHandler hbHandler = mock(TaskHeartbeatHandler.class);
    MockTaskAttemptListenerImpl listener = 
      new MockTaskAttemptListenerImpl(appCtx, secret,
          rmHeartbeatHandler, hbHandler);
    Configuration conf = new Configuration();
    listener.init(conf);
    listener.start();
    JVMId id = new JVMId("foo",1, true, 1);
    WrappedJvmID wid = new WrappedJvmID(id.getJobId(), id.isMap, id.getId());

    // Verify ask before registration.
    //The JVM ID has not been registered yet so we should kill it.
    JvmContext context = new JvmContext();
    context.jvmId = id; 
    JvmTask result = listener.getTask(context);
    assertNotNull(result);
    assertTrue(result.shouldDie);

    // Verify ask after registration but before launch. 
    // Don't kill, should be null.
    TaskAttemptId attemptID = mock(TaskAttemptId.class);
    Task task = mock(Task.class);
    //Now put a task with the ID
    listener.registerPendingTask(task, wid);
    result = listener.getTask(context);
    assertNull(result);
    // Unregister for more testing.
    listener.unregister(attemptID, wid);

    // Verify ask after registration and launch
    //Now put a task with the ID
    listener.registerPendingTask(task, wid);
    listener.registerLaunchedTask(attemptID, wid);
    verify(hbHandler).register(attemptID);
    result = listener.getTask(context);
    assertNotNull(result);
    assertFalse(result.shouldDie);
    // Don't unregister yet for more testing.

    //Verify that if we call it again a second time we are told to die.
    result = listener.getTask(context);
    assertNotNull(result);
    assertTrue(result.shouldDie);

    listener.unregister(attemptID, wid);

    // Verify after unregistration.
    result = listener.getTask(context);
    assertNotNull(result);
    assertTrue(result.shouldDie);

    listener.stop();
  }

  @Test
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

    AppContext appCtx = mock(AppContext.class);
    when(appCtx.getJob(any(JobId.class))).thenReturn(mockJob);
    JobTokenSecretManager secret = mock(JobTokenSecretManager.class);
    RMHeartbeatHandler rmHeartbeatHandler =
        mock(RMHeartbeatHandler.class);
    final TaskHeartbeatHandler hbHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptListenerImpl listener =
        new TaskAttemptListenerImpl(appCtx, secret, rmHeartbeatHandler) {
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
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    TaskAttemptCompletionEvent tce = recordFactory
        .newRecordInstance(TaskAttemptCompletionEvent.class);
    tce.setEventId(eventId);
    tce.setAttemptId(attemptId);
    tce.setStatus(status);
    return tce;
  }

  @Test
  public void testCommitWindow() throws IOException {
    SystemClock clock = new SystemClock();

    org.apache.hadoop.mapreduce.v2.app.job.Task mockTask =
        mock(org.apache.hadoop.mapreduce.v2.app.job.Task.class);
    when(mockTask.canCommit(any(TaskAttemptId.class))).thenReturn(true);
    Job mockJob = mock(Job.class);
    when(mockJob.getTask(any(TaskId.class))).thenReturn(mockTask);
    AppContext appCtx = mock(AppContext.class);
    when(appCtx.getJob(any(JobId.class))).thenReturn(mockJob);
    when(appCtx.getClock()).thenReturn(clock);
    JobTokenSecretManager secret = mock(JobTokenSecretManager.class);
    RMHeartbeatHandler rmHeartbeatHandler =
        mock(RMHeartbeatHandler.class);
    final TaskHeartbeatHandler hbHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptListenerImpl listener =
        new TaskAttemptListenerImpl(appCtx, secret, rmHeartbeatHandler) {
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
    when(rmHeartbeatHandler.getLastHeartbeatTime()).thenReturn(clock.getTime());
    canCommit = listener.canCommit(tid);
    assertTrue(canCommit);
    verify(mockTask, times(1)).canCommit(any(TaskAttemptId.class));

    listener.stop();
  }
}
