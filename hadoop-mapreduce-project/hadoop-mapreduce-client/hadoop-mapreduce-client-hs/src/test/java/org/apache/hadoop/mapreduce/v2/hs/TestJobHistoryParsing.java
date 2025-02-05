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

package org.apache.hadoop.mapreduce.v2.hs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.
    NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptFailEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.EventReader;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.HistoryViewer;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.AMInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.TestJobHistoryEvents.MRAppWithHistory;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.RackResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJobHistoryParsing {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestJobHistoryParsing.class);

  private static final String RACK_NAME = "/MyRackName";

  private ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  public static class MyResolver implements DNSToSwitchMapping {
    @Override
    public List<String> resolve(List<String> names) {
      return Arrays.asList(new String[] { RACK_NAME });
    }

    @Override
    public void reloadCachedMappings() {
    }

    @Override
    public void reloadCachedMappings(List<String> names) {	
    }
  }

  @Test
  @Timeout(value = 50)
  public void testJobInfo() throws Exception {
    JobInfo info = new JobInfo();
    assertEquals("NORMAL", info.getPriority());
    info.printAll();
  }

  @Test
  @Timeout(value = 300)
  public void testHistoryParsing() throws Exception {
    LOG.info("STARTING testHistoryParsing()");
    try {
      checkHistoryParsing(2, 1, 2);
    } finally {
      LOG.info("FINISHED testHistoryParsing()");
    }
  }

  @Test
  @Timeout(value = 50)
  public void testHistoryParsingWithParseErrors() throws Exception {
    LOG.info("STARTING testHistoryParsingWithParseErrors()");
    try {
      checkHistoryParsing(3, 0, 2);
    } finally {
      LOG.info("FINISHED testHistoryParsingWithParseErrors()");
    }
  }

  private static String getJobSummary(FileContext fc, Path path)
      throws IOException {
    Path qPath = fc.makeQualified(path);
    FSDataInputStream in = fc.open(qPath);
    String jobSummaryString = in.readUTF();
    in.close();
    return jobSummaryString;
  }

  private void checkHistoryParsing(final int numMaps, final int numReduces,
      final int numSuccessfulMaps) throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    long amStartTimeEst = System.currentTimeMillis();
    conf.setClass(
        NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    RackResolver.init(conf);
    MRApp app = new MRAppWithHistory(numMaps, numReduces, true, this.getClass()
        .getName(), true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobId jobId = job.getID();
    LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
    app.waitForState(job, JobState.SUCCEEDED);

    // make sure all events are flushed
    app.waitForState(Service.STATE.STOPPED);

    String jobhistoryDir = JobHistoryUtils
        .getHistoryIntermediateDoneDirForUser(conf);

    FileContext fc = null;
    try {
      fc = FileContext.getFileContext(conf);
    } catch (IOException ioe) {
      LOG.info("Can not get FileContext", ioe);
      throw (new Exception("Can not get File Context"));
    }

    if (numMaps == numSuccessfulMaps) {
      String summaryFileName = JobHistoryUtils
          .getIntermediateSummaryFileName(jobId);
      Path summaryFile = new Path(jobhistoryDir, summaryFileName);
      String jobSummaryString = getJobSummary(fc, summaryFile);
      assertNotNull(jobSummaryString);
      assertTrue(jobSummaryString.contains("resourcesPerMap=100"));
      assertTrue(jobSummaryString.contains("resourcesPerReduce=100"));

      Map<String, String> jobSummaryElements = new HashMap<String, String>();
      StringTokenizer strToken = new StringTokenizer(jobSummaryString, ",");
      while (strToken.hasMoreTokens()) {
        String keypair = strToken.nextToken();
        jobSummaryElements.put(keypair.split("=")[0], keypair.split("=")[1]);
      }

      assertEquals(jobId.toString(), jobSummaryElements.get("jobId"),
          "JobId does not match");
      assertEquals("test", jobSummaryElements.get("jobName"), "JobName does not match");
      assertTrue(Long.parseLong(jobSummaryElements.get("submitTime")) != 0,
          "submitTime should not be 0");
      assertTrue(Long.parseLong(jobSummaryElements.get("launchTime")) != 0,
          "launchTime should not be 0");
      assertTrue(Long.parseLong(jobSummaryElements.get("firstMapTaskLaunchTime")) != 0,
          "firstMapTaskLaunchTime should not be 0");
      assertTrue(Long.parseLong(jobSummaryElements.get("firstReduceTaskLaunchTime")) != 0,
          "firstReduceTaskLaunchTime should not be 0");
      assertTrue(Long.parseLong(jobSummaryElements.get("finishTime")) != 0,
          "finishTime should not be 0");
      assertEquals(numSuccessfulMaps, Integer.parseInt(jobSummaryElements.get("numMaps")),
          "Mismatch in num map slots");
      assertEquals(numReduces, Integer.parseInt(jobSummaryElements.get("numReduces")),
          "Mismatch in num reduce slots");
      assertEquals(System.getProperty("user.name"), jobSummaryElements.get("user"),
          "User does not match");
      assertEquals("default", jobSummaryElements.get("queue"), "Queue does not match");
      assertEquals("SUCCEEDED", jobSummaryElements.get("status"), "Status does not match");
    }

    JobHistory jobHistory = new JobHistory();
    jobHistory.init(conf);
    HistoryFileInfo fileInfo = jobHistory.getJobFileInfo(jobId);
    JobInfo jobInfo;
    long numFinishedMaps;

    synchronized (fileInfo) {
      Path historyFilePath = fileInfo.getHistoryFile();
      FSDataInputStream in = null;
      LOG.info("JobHistoryFile is: " + historyFilePath);
      try {
        in = fc.open(fc.makeQualified(historyFilePath));
      } catch (IOException ioe) {
        LOG.info("Can not open history file: " + historyFilePath, ioe);
        throw (new Exception("Can not open History File"));
      }

      JobHistoryParser parser = new JobHistoryParser(in);
      final EventReader realReader = new EventReader(in);
      EventReader reader = Mockito.mock(EventReader.class);
      if (numMaps == numSuccessfulMaps) {
        reader = realReader;
      } else {
        final AtomicInteger numFinishedEvents = new AtomicInteger(0); // Hack!
        Mockito.when(reader.getNextEvent()).thenAnswer(
            new Answer<HistoryEvent>() {
              public HistoryEvent answer(InvocationOnMock invocation)
                  throws IOException {
                HistoryEvent event = realReader.getNextEvent();
                if (event instanceof TaskFinishedEvent) {
                  numFinishedEvents.incrementAndGet();
                }

                if (numFinishedEvents.get() <= numSuccessfulMaps) {
                  return event;
                } else {
                  throw new IOException("test");
                }
              }
            });
      }

      jobInfo = parser.parse(reader);

      numFinishedMaps = computeFinishedMaps(jobInfo, numMaps, numSuccessfulMaps);

      if (numFinishedMaps != numMaps) {
        Exception parseException = parser.getParseException();
        assertNotNull(parseException, "Didn't get expected parse exception");
      }
    }

    assertEquals(System.getProperty("user.name"),
        jobInfo.getUsername(), "Incorrect username ");
    assertEquals("test", jobInfo.getJobname(), "Incorrect jobName");
    assertEquals("default", jobInfo.getJobQueueName(),
        "Incorrect queuename");
    assertEquals("test", jobInfo.getJobConfPath(), "incorrect conf path");
    assertEquals(numSuccessfulMaps,
        numFinishedMaps, "incorrect finishedMap ");
    assertEquals(numReduces,
        jobInfo.getSucceededReduces(), "incorrect finishedReduces ");
    assertEquals(job.isUber(),
        jobInfo.getUberized(), "incorrect uberized ");
    Map<TaskID, TaskInfo> allTasks = jobInfo.getAllTasks();
    int totalTasks = allTasks.size();
    assertEquals((numMaps + numReduces), totalTasks,
        "total number of tasks is incorrect");

    // Verify aminfo
    assertEquals(1, jobInfo.getAMInfos().size());
    assertEquals(MRApp.NM_HOST, jobInfo.getAMInfos().get(0)
        .getNodeManagerHost());
    AMInfo amInfo = jobInfo.getAMInfos().get(0);
    assertEquals(MRApp.NM_PORT, amInfo.getNodeManagerPort());
    assertEquals(MRApp.NM_HTTP_PORT, amInfo.getNodeManagerHttpPort());
    assertEquals(1, amInfo.getAppAttemptId().getAttemptId());
    assertEquals(amInfo.getAppAttemptId(), amInfo.getContainerId()
        .getApplicationAttemptId());
    assertTrue(amInfo.getStartTime() <= System.currentTimeMillis()
        && amInfo.getStartTime() >= amStartTimeEst);

    ContainerId fakeCid = MRApp.newContainerId(-1, -1, -1, -1);
    // Assert at taskAttempt level
    for (TaskInfo taskInfo : allTasks.values()) {
      int taskAttemptCount = taskInfo.getAllTaskAttempts().size();
      assertEquals(1, taskAttemptCount, "total number of task attempts");
      TaskAttemptInfo taInfo = taskInfo.getAllTaskAttempts().values()
          .iterator().next();
      assertNotNull(taInfo.getContainerId());
      // Verify the wrong ctor is not being used. Remove after mrv1 is removed.
      assertNotEquals(taInfo.getContainerId(), fakeCid);
    }

    // Deep compare Job and JobInfo
    for (Task task : job.getTasks().values()) {
      TaskInfo taskInfo = allTasks.get(TypeConverter.fromYarn(task.getID()));
      assertNotNull(taskInfo, "TaskInfo not found");
      for (TaskAttempt taskAttempt : task.getAttempts().values()) {
        TaskAttemptInfo taskAttemptInfo = taskInfo.getAllTaskAttempts().get(
            TypeConverter.fromYarn((taskAttempt.getID())));
        assertNotNull(taskAttemptInfo, "TaskAttemptInfo not found");
        assertEquals(taskAttempt.getShufflePort(), taskAttemptInfo.getShufflePort(),
            "Incorrect shuffle port for task attempt");
        if (numMaps == numSuccessfulMaps) {
          assertEquals(MRApp.NM_HOST, taskAttemptInfo.getHostname());
          assertEquals(MRApp.NM_PORT, taskAttemptInfo.getPort());

          // Verify rack-name
          assertEquals(taskAttemptInfo.getRackname(), RACK_NAME,
              "rack-name is incorrect");
        }
      }
    }

    // test output for HistoryViewer
    PrintStream stdps = System.out;
    try {
      System.setOut(new PrintStream(outContent));
      HistoryViewer viewer;
      synchronized (fileInfo) {
        viewer = new HistoryViewer(fc.makeQualified(
            fileInfo.getHistoryFile()).toString(), conf, true);
      }
      viewer.print();

      for (TaskInfo taskInfo : allTasks.values()) {

        String test = (taskInfo.getTaskStatus() == null ? "" : taskInfo
            .getTaskStatus())
            + " "
            + taskInfo.getTaskType()
            + " task list for " + taskInfo.getTaskId().getJobID();
        assertTrue(outContent.toString().indexOf(test) > 0);
        assertTrue(outContent.toString().indexOf(
            taskInfo.getTaskId().toString()) > 0);
      }
    } finally {
      System.setOut(stdps);

    }
  }

  // Computes finished maps similar to RecoveryService...
  private long computeFinishedMaps(JobInfo jobInfo, int numMaps,
      int numSuccessfulMaps) {
    if (numMaps == numSuccessfulMaps) {
      return jobInfo.getSucceededMaps();
    }

    long numFinishedMaps = 0;
    Map<org.apache.hadoop.mapreduce.TaskID, TaskInfo> taskInfos = jobInfo
        .getAllTasks();
    for (TaskInfo taskInfo : taskInfos.values()) {
      if (TaskState.SUCCEEDED.toString().equals(taskInfo.getTaskStatus())) {
        ++numFinishedMaps;
      }
    }
    return numFinishedMaps;
  }

  @Test
  @Timeout(value = 30)
  public void testHistoryParsingForFailedAttempts() throws Exception {
    LOG.info("STARTING testHistoryParsingForFailedAttempts");
    try {
      Configuration conf = new Configuration();
      conf.setClass(
          NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          MyResolver.class, DNSToSwitchMapping.class);
      RackResolver.init(conf);
      MRApp app = new MRAppWithHistoryWithFailedAttempt(2, 1, true, this
          .getClass().getName(), true);
      app.submit(conf);
      Job job = app.getContext().getAllJobs().values().iterator().next();
      JobId jobId = job.getID();
      app.waitForState(job, JobState.SUCCEEDED);

      // make sure all events are flushed
      app.waitForState(Service.STATE.STOPPED);

      JobHistory jobHistory = new JobHistory();
      jobHistory.init(conf);
      HistoryFileInfo fileInfo = jobHistory.getJobFileInfo(jobId);
      
      JobHistoryParser parser;
      JobInfo jobInfo;
      synchronized (fileInfo) {
        Path historyFilePath = fileInfo.getHistoryFile();
        FSDataInputStream in = null;
        FileContext fc = null;
        try {
          fc = FileContext.getFileContext(conf);
          in = fc.open(fc.makeQualified(historyFilePath));
        } catch (IOException ioe) {
          LOG.info("Can not open history file: " + historyFilePath, ioe);
          throw (new Exception("Can not open History File"));
        }

        parser = new JobHistoryParser(in);
        jobInfo = parser.parse();
      }
      Exception parseException = parser.getParseException();
      assertNull(parseException,
          "Caught an expected exception " + parseException);
      int noOffailedAttempts = 0;
      Map<TaskID, TaskInfo> allTasks = jobInfo.getAllTasks();
      for (Task task : job.getTasks().values()) {
        TaskInfo taskInfo = allTasks.get(TypeConverter.fromYarn(task.getID()));
        for (TaskAttempt taskAttempt : task.getAttempts().values()) {
          TaskAttemptInfo taskAttemptInfo = taskInfo.getAllTaskAttempts().get(
              TypeConverter.fromYarn((taskAttempt.getID())));
          // Verify rack-name for all task attempts
          assertThat(taskAttemptInfo.getRackname())
              .withFailMessage("rack-name is incorrect").isEqualTo(RACK_NAME);
          if (taskAttemptInfo.getTaskStatus().equals("FAILED")) {
            noOffailedAttempts++;
          }
        }
      }
      assertEquals(2, noOffailedAttempts, "No of Failed tasks doesn't match.");
    } finally {
      LOG.info("FINISHED testHistoryParsingForFailedAttempts");
    }
  }

  @Test
  @Timeout(value = 30)
  public void testHistoryParsingForKilledAndFailedAttempts() throws Exception {
    MRApp app = null;
    JobHistory jobHistory = null;
    LOG.info("STARTING testHistoryParsingForKilledAndFailedAttempts");
    try {
      Configuration conf = new Configuration();
      conf.setClass(
          NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          MyResolver.class, DNSToSwitchMapping.class);
      conf.set(JHAdminConfig.MR_HS_JHIST_FORMAT, "json");
      // "CommitterEventHandler" thread could be slower in some cases,
      // which might cause a failed map/reduce task to fail the job
      // immediately (see JobImpl.checkJobAfterTaskCompletion()). If there are
      // killed events in progress, those will not be counted. Instead,
      // we allow a 50% failure rate, so the job will always succeed and kill
      // events will not be ignored.
      conf.setInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 50);
      conf.setInt(MRJobConfig.REDUCE_FAILURES_MAXPERCENT, 50);
      RackResolver.init(conf);
      app = new MRAppWithHistoryWithFailedAndKilledTask(3, 3, true, this
          .getClass().getName(), true);
      app.submit(conf);
      Job job = app.getContext().getAllJobs().values().iterator().next();
      JobId jobId = job.getID();
      app.waitForState(job, JobState.SUCCEEDED);

      // make sure all events are flushed
      app.waitForState(Service.STATE.STOPPED);

      jobHistory = new JobHistory();
      jobHistory.init(conf);
      HistoryFileInfo fileInfo = jobHistory.getJobFileInfo(jobId);

      JobHistoryParser parser;
      JobInfo jobInfo;
      synchronized (fileInfo) {
        Path historyFilePath = fileInfo.getHistoryFile();
        FSDataInputStream in = null;
        FileContext fc = null;
        try {
          fc = FileContext.getFileContext(conf);
          in = fc.open(fc.makeQualified(historyFilePath));
        } catch (IOException ioe) {
          LOG.info("Can not open history file: " + historyFilePath, ioe);
          throw (new Exception("Can not open History File"));
        }

        parser = new JobHistoryParser(in);
        jobInfo = parser.parse();
      }
      Exception parseException = parser.getParseException();
      assertNull(parseException,
          "Caught an expected exception " + parseException);

      assertEquals(1, jobInfo.getFailedMaps(), "FailedMaps");
      assertEquals(1, jobInfo.getKilledMaps(), "KilledMaps");
      assertEquals(1, jobInfo.getFailedReduces(), "FailedReduces");
      assertEquals(1, jobInfo.getKilledReduces(), "KilledReduces");
    } finally {
      LOG.info("FINISHED testHistoryParsingForKilledAndFailedAttempts");
      if (app != null) {
        app.close();
      }
      if (jobHistory != null) {
        jobHistory.close();
      }
    }
  }

  @Test
  @Timeout(value = 60)
  public void testCountersForFailedTask() throws Exception {
    LOG.info("STARTING testCountersForFailedTask");
    try {
      Configuration conf = new Configuration();
      conf.setClass(
          NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          MyResolver.class, DNSToSwitchMapping.class);
      RackResolver.init(conf);
      MRApp app = new MRAppWithHistoryWithFailedTask(2, 1, true, this
          .getClass().getName(), true);
      app.submit(conf);
      Job job = app.getContext().getAllJobs().values().iterator().next();
      JobId jobId = job.getID();
      app.waitForState(job, JobState.FAILED);

      // make sure all events are flushed
      app.waitForState(Service.STATE.STOPPED);

      JobHistory jobHistory = new JobHistory();
      jobHistory.init(conf);

      HistoryFileInfo fileInfo = jobHistory.getJobFileInfo(jobId);
      
      JobHistoryParser parser;
      JobInfo jobInfo;
      synchronized (fileInfo) {
        Path historyFilePath = fileInfo.getHistoryFile();
        FSDataInputStream in = null;
        FileContext fc = null;
        try {
          fc = FileContext.getFileContext(conf);
          in = fc.open(fc.makeQualified(historyFilePath));
        } catch (IOException ioe) {
          LOG.info("Can not open history file: " + historyFilePath, ioe);
          throw (new Exception("Can not open History File"));
        }

        parser = new JobHistoryParser(in);
        jobInfo = parser.parse();
      }
      Exception parseException = parser.getParseException();
      assertNull(parseException, "Caught an expected exception " + parseException);
      for (Map.Entry<TaskID, TaskInfo> entry : jobInfo.getAllTasks().entrySet()) {
        TaskId yarnTaskID = TypeConverter.toYarn(entry.getKey());
        CompletedTask ct = new CompletedTask(yarnTaskID, entry.getValue());
        assertNotNull(ct.getReport().getCounters(),
            "completed task report has null counters");
      }
      final List<String> originalDiagnostics = job.getDiagnostics();
      final String historyError = jobInfo.getErrorInfo();
      assertTrue(originalDiagnostics != null && !originalDiagnostics.isEmpty(),
          "No original diagnostics for a failed job");
      assertNotNull(historyError, "No history error info for a failed job");
      for (String diagString : originalDiagnostics) {
        assertTrue(historyError.contains(diagString));
      }
    } finally {
      LOG.info("FINISHED testCountersForFailedTask");
    }
  }

  @Test
  @Timeout(value = 60)
  public void testDiagnosticsForKilledJob() throws Exception {
    LOG.info("STARTING testDiagnosticsForKilledJob");
    try {
      final Configuration conf = new Configuration();
      conf.setClass(
          NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          MyResolver.class, DNSToSwitchMapping.class);
      RackResolver.init(conf);
      MRApp app = new MRAppWithHistoryWithJobKilled(2, 1, true, this
          .getClass().getName(), true);
      app.submit(conf);
      Job job = app.getContext().getAllJobs().values().iterator().next();
      JobId jobId = job.getID();
      app.waitForState(job, JobState.KILLED);

      // make sure all events are flushed
      app.waitForState(Service.STATE.STOPPED);

      JobHistory jobHistory = new JobHistory();
      jobHistory.init(conf);

      HistoryFileInfo fileInfo = jobHistory.getJobFileInfo(jobId);

      JobHistoryParser parser;
      JobInfo jobInfo;
      synchronized (fileInfo) {
        Path historyFilePath = fileInfo.getHistoryFile();
        FSDataInputStream in = null;
        FileContext fc = null;
        try {
          fc = FileContext.getFileContext(conf);
          in = fc.open(fc.makeQualified(historyFilePath));
        } catch (IOException ioe) {
          LOG.info("Can not open history file: " + historyFilePath, ioe);
          throw (new Exception("Can not open History File"));
        }

        parser = new JobHistoryParser(in);
        jobInfo = parser.parse();
      }
      Exception parseException = parser.getParseException();
      assertNull(parseException, "Caught an expected exception " + parseException);
      final List<String> originalDiagnostics = job.getDiagnostics();
      final String historyError = jobInfo.getErrorInfo();
      assertTrue(originalDiagnostics != null && !originalDiagnostics.isEmpty(),
          "No original diagnostics for a failed job");
      assertNotNull(historyError, "No history error info for a failed job ");
      for (String diagString : originalDiagnostics) {
        assertTrue(historyError.contains(diagString));
      }
      assertTrue(historyError.contains(JobImpl.JOB_KILLED_DIAG),
          "No killed message in diagnostics");
    } finally {
      LOG.info("FINISHED testDiagnosticsForKilledJob");
    }
  }

  @Test
  @Timeout(value = 50)
  public void testScanningOldDirs() throws Exception {
    LOG.info("STARTING testScanningOldDirs");
    try {
      Configuration conf = new Configuration();
      conf.setClass(
          NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          MyResolver.class, DNSToSwitchMapping.class);
      RackResolver.init(conf);
      MRApp app = new MRAppWithHistory(1, 1, true, this.getClass().getName(),
          true);
      app.submit(conf);
      Job job = app.getContext().getAllJobs().values().iterator().next();
      JobId jobId = job.getID();
      LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
      app.waitForState(job, JobState.SUCCEEDED);

      // make sure all events are flushed
      app.waitForState(Service.STATE.STOPPED);

      HistoryFileManagerForTest hfm = new HistoryFileManagerForTest();
      hfm.init(conf);
      HistoryFileInfo fileInfo = hfm.getFileInfo(jobId);
      assertNotNull(fileInfo, "Unable to locate job history");

      // force the manager to "forget" the job
      hfm.deleteJobFromJobListCache(fileInfo);
      final int msecPerSleep = 10;
      int msecToSleep = 10 * 1000;
      while (fileInfo.isMovePending() && msecToSleep > 0) {
        assertFalse(fileInfo.didMoveFail());
        msecToSleep -= msecPerSleep;
        Thread.sleep(msecPerSleep);
      }
      assertTrue(msecToSleep > 0, "Timeout waiting for history move");

      fileInfo = hfm.getFileInfo(jobId);
      hfm.stop();
      assertNotNull(fileInfo, "Unable to locate old job history");
      assertTrue(hfm.moveToDoneExecutor.isTerminated(),
          "HistoryFileManager not shutdown properly");
    } finally {
      LOG.info("FINISHED testScanningOldDirs");
    }
  }

  static class MRAppWithHistoryWithFailedAttempt extends MRAppWithHistory {

    public MRAppWithHistoryWithFailedAttempt(int maps, int reduces,
        boolean autoComplete, String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      if (attemptID.getTaskId().getId() == 0 && attemptID.getId() == 0) {
        getContext().getEventHandler().handle(
            new TaskAttemptFailEvent(attemptID));
      } else {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_DONE));
      }
    }
  }

  static class MRAppWithHistoryWithFailedTask extends MRAppWithHistory {

    public MRAppWithHistoryWithFailedTask(int maps, int reduces,
        boolean autoComplete, String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      if (attemptID.getTaskId().getId() == 0) {
        getContext().getEventHandler().handle(
            new TaskAttemptFailEvent(attemptID));
      } else {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_DONE));
      }
    }
  }

  static class MRAppWithHistoryWithFailedAndKilledTask
    extends MRAppWithHistory {

    MRAppWithHistoryWithFailedAndKilledTask(int maps, int reduces,
        boolean autoComplete, String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      final int taskId = attemptID.getTaskId().getId();
      final TaskType taskType = attemptID.getTaskId().getTaskType();

      // map #0 --> kill
      // reduce #0 --> fail
      if (taskType == TaskType.MAP && taskId == 0) {
        getContext().getEventHandler().handle(
            new TaskEvent(attemptID.getTaskId(), TaskEventType.T_KILL));
      } else if (taskType == TaskType.MAP && taskId == 1) {
        getContext().getEventHandler().handle(
            new TaskAttemptFailEvent(attemptID));
      } else if (taskType == TaskType.REDUCE && taskId == 0) {
        getContext().getEventHandler().handle(
            new TaskAttemptFailEvent(attemptID));
      } else if (taskType == TaskType.REDUCE && taskId == 1) {
        getContext().getEventHandler().handle(
            new TaskEvent(attemptID.getTaskId(), TaskEventType.T_KILL));
      } else {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_DONE));
      }
    }
  }

  static class MRAppWithHistoryWithJobKilled extends MRAppWithHistory {

    public MRAppWithHistoryWithJobKilled(int maps, int reduces,
        boolean autoComplete, String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      if (attemptID.getTaskId().getId() == 0) {
        getContext().getEventHandler().handle(
            new JobEvent(attemptID.getTaskId().getJobId(),
                JobEventType.JOB_KILL));
      } else {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_DONE));
      }
    }
  }

  static class HistoryFileManagerForTest extends HistoryFileManager {
    void deleteJobFromJobListCache(HistoryFileInfo fileInfo) {
      jobListCache.delete(fileInfo);
    }
  }

  public static void main(String[] args) throws Exception {
    TestJobHistoryParsing t = new TestJobHistoryParsing();
    t.testHistoryParsing();
    t.testHistoryParsingForFailedAttempts();
  }

  /**
   * Test clean old history files. Files should be deleted after 1 week by
   * default.
   */
  @Test
  @Timeout(value = 15)
  public void testDeleteFileInfo() throws Exception {
    LOG.info("STARTING testDeleteFileInfo");
    try {
      Configuration conf = new Configuration();

      conf.setClass(
          NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          MyResolver.class, DNSToSwitchMapping.class);

      RackResolver.init(conf);
      MRApp app = new MRAppWithHistory(1, 1, true, this.getClass().getName(),
          true);
      app.submit(conf);
      Job job = app.getContext().getAllJobs().values().iterator().next();
      JobId jobId = job.getID();

      app.waitForState(job, JobState.SUCCEEDED);

      // make sure all events are flushed
      app.waitForState(Service.STATE.STOPPED);

      HistoryFileManager hfm = new HistoryFileManager();
      hfm.init(conf);
      HistoryFileInfo fileInfo = hfm.getFileInfo(jobId);
      hfm.initExisting();
      // wait for move files form the done_intermediate directory to the gone
      // directory
      while (fileInfo.isMovePending()) {
        Thread.sleep(300);
      }

      assertNotNull(hfm.jobListCache.values());

      // try to remove fileInfo
      hfm.clean();
      // check that fileInfo does not deleted
      assertFalse(fileInfo.isDeleted());
      // correct live time
      hfm.setMaxHistoryAge(-1);
      hfm.clean();
      hfm.stop();
      assertTrue(hfm.moveToDoneExecutor.isTerminated(), "Thread pool shutdown");
      // should be deleted !
      assertTrue(fileInfo.isDeleted(), "file should be deleted ");

    } finally {
      LOG.info("FINISHED testDeleteFileInfo");
    }
  }

  /**
   * Simple test some methods of JobHistory
   */
  @Test
  @Timeout(value = 20)
  public void testJobHistoryMethods() throws Exception {
    LOG.info("STARTING testJobHistoryMethods");
    try {
      Configuration configuration = new Configuration();
      configuration
          .setClass(
              NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
              MyResolver.class, DNSToSwitchMapping.class);

      RackResolver.init(configuration);
      MRApp app = new MRAppWithHistory(1, 1, true, this.getClass().getName(),
          true);
      app.submit(configuration);
      Job job = app.getContext().getAllJobs().values().iterator().next();
      JobId jobId = job.getID();
      LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
      app.waitForState(job, JobState.SUCCEEDED);
      // make sure job history events are handled
      app.waitForState(Service.STATE.STOPPED);

      JobHistory jobHistory = new JobHistory();
      jobHistory.init(configuration);
      // Method getAllJobs
      assertEquals(1, jobHistory.getAllJobs().size());
      // and with ApplicationId
      assertEquals(1, jobHistory.getAllJobs(app.getAppID()).size());

      JobsInfo jobsinfo = jobHistory.getPartialJobs(0L, 10L, null, "default",
          0L, System.currentTimeMillis() + 1, 0L,
          System.currentTimeMillis() + 1, JobState.SUCCEEDED);

      assertEquals(1, jobsinfo.getJobs().size());
      assertNotNull(jobHistory.getApplicationAttemptId());
      // test Application Id
      assertEquals("application_0_0000", jobHistory.getApplicationID().toString());
      assertEquals("Job History Server", jobHistory.getApplicationName());
      // method does not work
      assertNull(jobHistory.getEventHandler());
      // method does not work
      assertNull(jobHistory.getClock());
      // method does not work
      assertNull(jobHistory.getClusterInfo());

    } finally {
      LOG.info("FINISHED testJobHistoryMethods");
    }
  }

  /**
   * Simple test PartialJob
   */
  @Test
  @Timeout(value = 3)
  public void testPartialJob() throws Exception {
    JobId jobId = new JobIdPBImpl();
    jobId.setId(0);
    JobIndexInfo jii = new JobIndexInfo(0L, System.currentTimeMillis(), "user",
        "jobName", jobId, 3, 2, "JobStatus");
    PartialJob test = new PartialJob(jii, jobId);
    
    assertEquals(1.0f, test.getProgress(), 0.001f);
    assertNull(test.getAllCounters());
    assertNull(test.getTasks());
    assertNull(test.getTasks(TaskType.MAP));
    assertNull(test.getTask(new TaskIdPBImpl()));

    assertNull(test.getTaskAttemptCompletionEvents(0, 100));
    assertNull(test.getMapAttemptCompletionEvents(0, 100));
    assertTrue(test.checkAccess(UserGroupInformation.getCurrentUser(), null));
    assertNull(test.getAMInfos());

  }

  @Test
  public void testMultipleFailedTasks() throws Exception {
    JobHistoryParser parser =
        new JobHistoryParser(Mockito.mock(FSDataInputStream.class));
    EventReader reader = Mockito.mock(EventReader.class);
    final AtomicInteger numEventsRead = new AtomicInteger(0); // Hack!
    final org.apache.hadoop.mapreduce.TaskType taskType =
        org.apache.hadoop.mapreduce.TaskType.MAP;
    final TaskID[] tids = new TaskID[2];
    final JobID jid = new JobID("1", 1);
    tids[0] = new TaskID(jid, taskType, 0);
    tids[1] = new TaskID(jid, taskType, 1);
    Mockito.when(reader.getNextEvent()).thenAnswer(
        new Answer<HistoryEvent>() {
          public HistoryEvent answer(InvocationOnMock invocation)
              throws IOException {
            // send two task start and two task fail events for tasks 0 and 1
            int eventId = numEventsRead.getAndIncrement();
            TaskID tid = tids[eventId & 0x1];
            if (eventId < 2) {
              return new TaskStartedEvent(tid, 0, taskType, "");
            }
            if (eventId < 4) {
              TaskFailedEvent tfe = new TaskFailedEvent(tid, 0, taskType,
                  "failed", "FAILED", null, new Counters());
              tfe.setDatum(tfe.getDatum());
              return tfe;
            }
            if (eventId < 5) {
              JobUnsuccessfulCompletionEvent juce =
                  new JobUnsuccessfulCompletionEvent(jid, 100L, 2, 0,
                          0, 0, 0, 0,
                      "JOB_FAILED", Collections.singletonList(
                          "Task failed: " + tids[0].toString()));
              return juce;
            }
            return null;
          }
        });
    JobInfo info = parser.parse(reader);
    assertTrue(info.getErrorInfo().contains(tids[0].toString()),
        "Task 0 not implicated");
  }

  @Test
  public void testFailedJobHistoryWithoutDiagnostics() throws Exception {
    final Path histPath = new Path(getClass().getClassLoader().getResource(
        "job_1393307629410_0001-1393307687476-user-Sleep+job-1393307723835-0-0-FAILED-default-1393307693920.jhist")
        .getFile());
    final FileSystem lfs = FileSystem.getLocal(new Configuration());
    final FSDataInputStream fsdis = lfs.open(histPath);
    try {
      JobHistoryParser parser = new JobHistoryParser(fsdis);
      JobInfo info = parser.parse();
      assertEquals(info.getJobId(), JobID.forName("job_1393307629410_0001"),
          "History parsed jobId incorrectly");
      assertEquals("", info.getErrorInfo(), "Default diagnostics incorrect");
    } finally {
      fsdis.close();
    }
  }
  
  /**
   * Test compatibility of JobHistoryParser with 2.0.3-alpha history files
   * @throws IOException
   */
  @Test
  public void testTaskAttemptUnsuccessfulCompletionWithoutCounters203() throws IOException 
    { 
      Path histPath = new Path(getClass().getClassLoader().getResource(
        "job_2.0.3-alpha-FAILED.jhist").getFile());
      JobHistoryParser parser = new JobHistoryParser(FileSystem.getLocal
          (new Configuration()), histPath);
      JobInfo jobInfo = parser.parse(); 
      LOG.info(" job info: " + jobInfo.getJobname() + " "
          + jobInfo.getSucceededMaps() + " "
          + jobInfo.getTotalMaps() + " "
          + jobInfo.getJobId() ) ;
    }
  
  /**
   * Test compatibility of JobHistoryParser with 2.4.0 history files
   * @throws IOException
   */
  @Test
  public void testTaskAttemptUnsuccessfulCompletionWithoutCounters240() throws IOException 
    {
      Path histPath = new Path(getClass().getClassLoader().getResource(
        "job_2.4.0-FAILED.jhist").getFile());
      JobHistoryParser parser = new JobHistoryParser(FileSystem.getLocal
          (new Configuration()), histPath);
      JobInfo jobInfo = parser.parse(); 
      LOG.info(" job info: " + jobInfo.getJobname() + " "
        + jobInfo.getSucceededMaps() + " "
        + jobInfo.getTotalMaps() + " "
        + jobInfo.getJobId() );
    }

  /**
   * Test compatibility of JobHistoryParser with 0.23.9 history files
   * @throws IOException
   */
  @Test
  public void testTaskAttemptUnsuccessfulCompletionWithoutCounters0239() throws IOException 
    {
      Path histPath = new Path(getClass().getClassLoader().getResource(
          "job_0.23.9-FAILED.jhist").getFile());
      JobHistoryParser parser = new JobHistoryParser(FileSystem.getLocal
          (new Configuration()), histPath);
      JobInfo jobInfo = parser.parse(); 
      LOG.info(" job info: " + jobInfo.getJobname() + " "
        + jobInfo.getSucceededMaps() + " "
        + jobInfo.getTotalMaps() + " " 
        + jobInfo.getJobId() ) ;
      }
}