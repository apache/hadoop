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

package org.apache.hadoop.mapreduce.jobhistory;

import static junit.framework.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;
import org.mockito.Mockito;

public class TestJobHistoryEventHandler {


  private static final Log LOG = LogFactory
      .getLog(TestJobHistoryEventHandler.class);

  @Test
  public void testFirstFlushOnCompletionEvent() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, t.workDir);
    conf.setLong(MRJobConfig.MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
        60 * 1000l);
    conf.setInt(MRJobConfig.MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER, 10);
    conf.setInt(MRJobConfig.MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS, 10);
    conf.setInt(
        MRJobConfig.MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD, 200);

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000)));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

      for (int i = 0; i < 100; i++) {
        queueEvent(jheh, new JobHistoryEvent(t.jobId, new TaskStartedEvent(
            t.taskID, 0, TaskType.MAP, "")));
      }
      handleNextNEvents(jheh, 100);
      verify(mockWriter, times(0)).flush();

      // First completion event, but min-queue-size for batching flushes is 10
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new TaskFinishedEvent(
          t.taskID, null, 0, TaskType.MAP, "", null)));
      verify(mockWriter).flush();

    } finally {
      jheh.stop();
      verify(mockWriter).close();
    }
  }

  @Test
  public void testMaxUnflushedCompletionEvents() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, t.workDir);
    conf.setLong(MRJobConfig.MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
        60 * 1000l);
    conf.setInt(MRJobConfig.MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER, 10);
    conf.setInt(MRJobConfig.MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS, 10);
    conf.setInt(
        MRJobConfig.MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD, 5);

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000)));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

      for (int i = 0 ; i < 100 ; i++) {
        queueEvent(jheh, new JobHistoryEvent(t.jobId, new TaskFinishedEvent(
            t.taskID, null, 0, TaskType.MAP, "", null)));
      }

      handleNextNEvents(jheh, 9);
      verify(mockWriter, times(0)).flush();

      handleNextNEvents(jheh, 1);
      verify(mockWriter).flush();
      
      handleNextNEvents(jheh, 50);
      verify(mockWriter, times(6)).flush();
      
    } finally {
      jheh.stop();
      verify(mockWriter).close();
    }
  }
  
  @Test
  public void testUnflushedTimer() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, t.workDir);
    conf.setLong(MRJobConfig.MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
        2 * 1000l); //2 seconds.
    conf.setInt(MRJobConfig.MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER, 10);
    conf.setInt(MRJobConfig.MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS, 100);
    conf.setInt(
        MRJobConfig.MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD, 5);

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000)));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

      for (int i = 0 ; i < 100 ; i++) {
        queueEvent(jheh, new JobHistoryEvent(t.jobId, new TaskFinishedEvent(
            t.taskID, null, 0, TaskType.MAP, "", null)));
      }

      handleNextNEvents(jheh, 9);
      verify(mockWriter, times(0)).flush();

      Thread.sleep(2 * 4 * 1000l); // 4 seconds should be enough. Just be safe.
      verify(mockWriter).flush();
    } finally {
      jheh.stop();
      verify(mockWriter).close();
    }
  }
  
  @Test
  public void testBatchedFlushJobEndMultiplier() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, t.workDir);
    conf.setLong(MRJobConfig.MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
        60 * 1000l); //2 seconds.
    conf.setInt(MRJobConfig.MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER, 3);
    conf.setInt(MRJobConfig.MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS, 10);
    conf.setInt(
        MRJobConfig.MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD, 0);

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000)));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

      for (int i = 0 ; i < 100 ; i++) {
        queueEvent(jheh, new JobHistoryEvent(t.jobId, new TaskFinishedEvent(
            t.taskID, null, 0, TaskType.MAP, "", null)));
      }
      queueEvent(jheh, new JobHistoryEvent(t.jobId, new JobFinishedEvent(
          TypeConverter.fromYarn(t.jobId), 0, 10, 10, 0, 0, null, null, new Counters())));

      handleNextNEvents(jheh, 29);
      verify(mockWriter, times(0)).flush();

      handleNextNEvents(jheh, 72);
      verify(mockWriter, times(4)).flush(); //3 * 30 + 1 for JobFinished
    } finally {
      jheh.stop();
      verify(mockWriter).close();
    }
  }

  private void queueEvent(JHEvenHandlerForTest jheh, JobHistoryEvent event) {
    jheh.handle(event);
  }

  private void handleEvent(JHEvenHandlerForTest jheh, JobHistoryEvent event)
      throws InterruptedException {
    jheh.handle(event);
    jheh.handleEvent(jheh.eventQueue.take());
  }

  private void handleNextNEvents(JHEvenHandlerForTest jheh, int numEvents)
      throws InterruptedException {
    for (int i = 0; i < numEvents; i++) {
      jheh.handleEvent(jheh.eventQueue.take());
    }
  }

  private String setupTestWorkDir() {
    File testWorkDir = new File("target", this.getClass().getCanonicalName());
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(testWorkDir.getAbsolutePath()), true);
      return testWorkDir.getAbsolutePath();
    } catch (Exception e) {
      LOG.warn("Could not cleanup", e);
      throw new YarnException("could not cleanup test dir", e);
    }
  }

  private AppContext mockAppContext(ApplicationId appId) {
    JobId jobId = TypeConverter.toYarn(TypeConverter.fromYarn(appId));
    AppContext mockContext = mock(AppContext.class);
    Job mockJob = mock(Job.class);
    when(mockJob.getTotalMaps()).thenReturn(10);
    when(mockJob.getTotalReduces()).thenReturn(10);
    when(mockJob.getName()).thenReturn("mockjob");
    when(mockContext.getJob(jobId)).thenReturn(mockJob);
    when(mockContext.getApplicationID()).thenReturn(appId);
    return mockContext;
  }
  

  private class TestParams {
    String workDir = setupTestWorkDir();
    ApplicationId appId = BuilderUtils.newApplicationId(200, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    TaskID taskID = TaskID.forName("task_200707121733_0003_m_000005");
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    AppContext mockAppContext = mockAppContext(appId);
  }

  private JobHistoryEvent getEventToEnqueue(JobId jobId) {
    JobHistoryEvent toReturn = Mockito.mock(JobHistoryEvent.class);
    HistoryEvent he = Mockito.mock(HistoryEvent.class);
    Mockito.when(he.getEventType()).thenReturn(EventType.JOB_STATUS_CHANGED);
    Mockito.when(toReturn.getHistoryEvent()).thenReturn(he);
    Mockito.when(toReturn.getJobID()).thenReturn(jobId);
    return toReturn;
  }

  @Test
  /**
   * Tests that in case of SIGTERM, the JHEH stops without processing its event
   * queue (because we must stop quickly lest we get SIGKILLed) and processes
   * a JobUnsuccessfulEvent for jobs which were still running (so that they may
   * show up in the JobHistoryServer)
   */
  public void testSigTermedFunctionality() throws IOException {
    AppContext mockedContext = Mockito.mock(AppContext.class);
    JHEventHandlerForSigtermTest jheh =
      new JHEventHandlerForSigtermTest(mockedContext, 0);

    JobId jobId = Mockito.mock(JobId.class);
    jheh.addToFileMap(jobId);

    //Submit 4 events and check that they're handled in the absence of a signal
    final int numEvents = 4;
    JobHistoryEvent events[] = new JobHistoryEvent[numEvents];
    for(int i=0; i < numEvents; ++i) {
      events[i] = getEventToEnqueue(jobId);
      jheh.handle(events[i]);
    }
    jheh.stop();
    //Make sure events were handled
    assertTrue("handleEvent should've been called only 4 times but was "
      + jheh.eventsHandled, jheh.eventsHandled == 4);

    //Create a new jheh because the last stop closed the eventWriter etc.
    jheh = new JHEventHandlerForSigtermTest(mockedContext, 0);

    // Make constructor of JobUnsuccessfulCompletionEvent pass
    Job job = Mockito.mock(Job.class);
    Mockito.when(mockedContext.getJob(jobId)).thenReturn(job);
    // Make TypeConverter(JobID) pass
    ApplicationId mockAppId = Mockito.mock(ApplicationId.class);
    Mockito.when(mockAppId.getClusterTimestamp()).thenReturn(1000l);
    Mockito.when(jobId.getAppId()).thenReturn(mockAppId);

    jheh.addToFileMap(jobId);
    jheh.setForcejobCompletion(true);
    for(int i=0; i < numEvents; ++i) {
      events[i] = getEventToEnqueue(jobId);
      jheh.handle(events[i]);
    }
    jheh.stop();
    //Make sure events were handled, 4 + 1 finish event
    assertTrue("handleEvent should've been called only 5 times but was "
        + jheh.eventsHandled, jheh.eventsHandled == 5);
    assertTrue("Last event handled wasn't JobUnsuccessfulCompletionEvent",
        jheh.lastEventHandled.getHistoryEvent()
        instanceof JobUnsuccessfulCompletionEvent);
  }
}

class JHEvenHandlerForTest extends JobHistoryEventHandler {

  private EventWriter eventWriter;
  volatile int handleEventCompleteCalls = 0;
  volatile int handleEventStartedCalls = 0;

  public JHEvenHandlerForTest(AppContext context, int startCount) {
    super(context, startCount);
  }

  @Override
  public void start() {
  }
  
  @Override
  protected EventWriter createEventWriter(Path historyFilePath)
      throws IOException {
    this.eventWriter = mock(EventWriter.class);
    return this.eventWriter;
  }

  @Override
  protected void closeEventWriter(JobId jobId) {
  }
  
  public EventWriter getEventWriter() {
    return this.eventWriter;
  }
}

/**
 * Class to help with testSigTermedFunctionality
 */
class JHEventHandlerForSigtermTest extends JobHistoryEventHandler {
  private MetaInfo metaInfo;
  public JHEventHandlerForSigtermTest(AppContext context, int startCount) {
    super(context, startCount);
  }

  public void addToFileMap(JobId jobId) {
    metaInfo = Mockito.mock(MetaInfo.class);
    Mockito.when(metaInfo.isWriterActive()).thenReturn(true);
    fileMap.put(jobId, metaInfo);
  }

  JobHistoryEvent lastEventHandled;
  int eventsHandled = 0;
  @Override
  protected void handleEvent(JobHistoryEvent event) {
    this.lastEventHandled = event;
    this.eventsHandled++;
  }
}