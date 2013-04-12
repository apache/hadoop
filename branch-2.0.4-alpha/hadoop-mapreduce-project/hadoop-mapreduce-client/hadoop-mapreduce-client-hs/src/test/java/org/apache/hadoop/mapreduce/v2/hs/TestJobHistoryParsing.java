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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.EventReader;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.AMInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinishedEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.TestJobHistoryEvents.MRAppWithHistory;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestJobHistoryParsing {
  private static final Log LOG = LogFactory.getLog(TestJobHistoryParsing.class);

  private static final String RACK_NAME = "/MyRackName";

  public static class MyResolver implements DNSToSwitchMapping {
    @Override
    public List<String> resolve(List<String> names) {
      return Arrays.asList(new String[]{RACK_NAME});
    }
  }

  @Test
  public void testJobInfo() throws Exception {
    JobInfo info = new JobInfo();
    Assert.assertEquals("NORMAL", info.getPriority());
    info.printAll();
  }

  @Test
  public void testHistoryParsing() throws Exception {
    LOG.info("STARTING testHistoryParsing()");
    try {
      checkHistoryParsing(2, 1, 2);
    } finally {
      LOG.info("FINISHED testHistoryParsing()");
    }
  }
  
  @Test
  public void testHistoryParsingWithParseErrors() throws Exception {
    LOG.info("STARTING testHistoryParsingWithParseErrors()");
    try {
      checkHistoryParsing(3, 0, 2);
    } finally {
      LOG.info("FINISHED testHistoryParsingWithParseErrors()");
    }
  }
  
  private static String getJobSummary(FileContext fc, Path path) throws IOException {
    Path qPath = fc.makeQualified(path);
    FSDataInputStream in = fc.open(qPath);
    String jobSummaryString = in.readUTF();
    in.close();
    return jobSummaryString;
  }
  
  private void checkHistoryParsing(final int numMaps, final int numReduces,
      final int numSuccessfulMaps) 
  throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    long amStartTimeEst = System.currentTimeMillis();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    RackResolver.init(conf);
    MRApp app = 
        new MRAppWithHistory(numMaps, numReduces, true, 
            this.getClass().getName(), true);
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
      Assert.assertNotNull(jobSummaryString);
      Assert.assertTrue(jobSummaryString.contains("resourcesPerMap=100"));
      Assert.assertTrue(jobSummaryString.contains("resourcesPerReduce=100"));

      Map<String, String> jobSummaryElements = new HashMap<String, String>();
      StringTokenizer strToken = new StringTokenizer(jobSummaryString, ",");
      while (strToken.hasMoreTokens()) {
        String keypair = strToken.nextToken();
        jobSummaryElements.put(keypair.split("=")[0], keypair.split("=")[1]);
      }

      Assert.assertEquals("JobId does not match", jobId.toString(),
          jobSummaryElements.get("jobId"));
      Assert.assertEquals("JobName does not match", "test",
          jobSummaryElements.get("jobName"));
      Assert.assertTrue("submitTime should not be 0",
          Long.parseLong(jobSummaryElements.get("submitTime")) != 0);
      Assert.assertTrue("launchTime should not be 0",
          Long.parseLong(jobSummaryElements.get("launchTime")) != 0);
      Assert.assertTrue("firstMapTaskLaunchTime should not be 0",
          Long.parseLong(jobSummaryElements.get("firstMapTaskLaunchTime")) != 0);
      Assert
      .assertTrue(
          "firstReduceTaskLaunchTime should not be 0",
          Long.parseLong(jobSummaryElements.get("firstReduceTaskLaunchTime")) != 0);
      Assert.assertTrue("finishTime should not be 0",
          Long.parseLong(jobSummaryElements.get("finishTime")) != 0);
      Assert.assertEquals("Mismatch in num map slots", numSuccessfulMaps,
          Integer.parseInt(jobSummaryElements.get("numMaps")));
      Assert.assertEquals("Mismatch in num reduce slots", numReduces,
          Integer.parseInt(jobSummaryElements.get("numReduces")));
      Assert.assertEquals("User does not match", System.getProperty("user.name"),
          jobSummaryElements.get("user"));
      Assert.assertEquals("Queue does not match", "default",
          jobSummaryElements.get("queue"));
      Assert.assertEquals("Status does not match", "SUCCEEDED",
          jobSummaryElements.get("status"));
    }

    JobHistory jobHistory = new JobHistory();
    jobHistory.init(conf);
    HistoryFileInfo fileInfo = jobHistory.getJobFileInfo(jobId);
    JobInfo jobInfo;
    long numFinishedMaps;
    
    synchronized(fileInfo) {
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
        final AtomicInteger numFinishedEvents = new AtomicInteger(0);  // Hack!
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
            }
        );
      }

      jobInfo = parser.parse(reader);

      numFinishedMaps = 
        computeFinishedMaps(jobInfo, numMaps, numSuccessfulMaps);

      if (numFinishedMaps != numMaps) {
        Exception parseException = parser.getParseException();
        Assert.assertNotNull("Didn't get expected parse exception", 
            parseException);
      }
    }
    
    Assert.assertEquals("Incorrect username ", System.getProperty("user.name"),
        jobInfo.getUsername());
    Assert.assertEquals("Incorrect jobName ", "test", jobInfo.getJobname());
    Assert.assertEquals("Incorrect queuename ", "default",
        jobInfo.getJobQueueName());
    Assert
        .assertEquals("incorrect conf path", "test", jobInfo.getJobConfPath());
    Assert.assertEquals("incorrect finishedMap ", numSuccessfulMaps, 
        numFinishedMaps);
    Assert.assertEquals("incorrect finishedReduces ", numReduces,
        jobInfo.getFinishedReduces());
    Assert.assertEquals("incorrect uberized ", job.isUber(),
        jobInfo.getUberized());
    Map<TaskID, TaskInfo> allTasks = jobInfo.getAllTasks();
    int totalTasks = allTasks.size();
    Assert.assertEquals("total number of tasks is incorrect  ", 
        (numMaps+numReduces), totalTasks);

    // Verify aminfo
    Assert.assertEquals(1, jobInfo.getAMInfos().size());
    Assert.assertEquals(MRApp.NM_HOST, jobInfo.getAMInfos().get(0)
        .getNodeManagerHost());
    AMInfo amInfo = jobInfo.getAMInfos().get(0);
    Assert.assertEquals(MRApp.NM_PORT, amInfo.getNodeManagerPort());
    Assert.assertEquals(MRApp.NM_HTTP_PORT, amInfo.getNodeManagerHttpPort());
    Assert.assertEquals(1, amInfo.getAppAttemptId().getAttemptId());
    Assert.assertEquals(amInfo.getAppAttemptId(), amInfo.getContainerId()
        .getApplicationAttemptId());
    Assert.assertTrue(amInfo.getStartTime() <= System.currentTimeMillis()
        && amInfo.getStartTime() >= amStartTimeEst);

    ContainerId fakeCid = BuilderUtils.newContainerId(-1, -1, -1, -1);
    // Assert at taskAttempt level
    for (TaskInfo taskInfo : allTasks.values()) {
      int taskAttemptCount = taskInfo.getAllTaskAttempts().size();
      Assert
          .assertEquals("total number of task attempts ", 1, taskAttemptCount);
      TaskAttemptInfo taInfo = taskInfo.getAllTaskAttempts().values()
          .iterator().next();
      Assert.assertNotNull(taInfo.getContainerId());
      // Verify the wrong ctor is not being used. Remove after mrv1 is removed.
      Assert.assertFalse(taInfo.getContainerId().equals(fakeCid));
    }

    // Deep compare Job and JobInfo
    for (Task task : job.getTasks().values()) {
      TaskInfo taskInfo = allTasks.get(
          TypeConverter.fromYarn(task.getID()));
      Assert.assertNotNull("TaskInfo not found", taskInfo);
      for (TaskAttempt taskAttempt : task.getAttempts().values()) {
        TaskAttemptInfo taskAttemptInfo = taskInfo.getAllTaskAttempts().get(
            TypeConverter.fromYarn((taskAttempt.getID())));
        Assert.assertNotNull("TaskAttemptInfo not found", taskAttemptInfo);
        Assert.assertEquals("Incorrect shuffle port for task attempt",
            taskAttempt.getShufflePort(), taskAttemptInfo.getShufflePort());
        if (numMaps == numSuccessfulMaps) {
          Assert.assertEquals(MRApp.NM_HOST, taskAttemptInfo.getHostname());
          Assert.assertEquals(MRApp.NM_PORT, taskAttemptInfo.getPort());
          
          // Verify rack-name
          Assert.assertEquals("rack-name is incorrect", taskAttemptInfo
              .getRackname(), RACK_NAME);
        }
      }
    }
  }
  
  // Computes finished maps similar to RecoveryService...
  private long computeFinishedMaps(JobInfo jobInfo, 
      int numMaps, int numSuccessfulMaps) {
    if (numMaps == numSuccessfulMaps) {
      return jobInfo.getFinishedMaps();
    }
    
    long numFinishedMaps = 0;
    Map<org.apache.hadoop.mapreduce.TaskID, TaskInfo> taskInfos = 
        jobInfo.getAllTasks();
    for (TaskInfo taskInfo : taskInfos.values()) {
      if (TaskState.SUCCEEDED.toString().equals(taskInfo.getTaskStatus())) {
        ++numFinishedMaps;
      }
    }
    return numFinishedMaps;
  }
  
  @Test
  public void testHistoryParsingForFailedAttempts() throws Exception {
    LOG.info("STARTING testHistoryParsingForFailedAttempts");
    try {
    Configuration conf = new Configuration();
    conf
        .setClass(
            CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            MyResolver.class, DNSToSwitchMapping.class);
    RackResolver.init(conf);
    MRApp app = new MRAppWithHistoryWithFailedAttempt(2, 1, true, this.getClass().getName(),
        true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobId jobId = job.getID();
    app.waitForState(job, JobState.SUCCEEDED);
    
    // make sure all events are flushed
    app.waitForState(Service.STATE.STOPPED);

    String jobhistoryDir = JobHistoryUtils
        .getHistoryIntermediateDoneDirForUser(conf);
    JobHistory jobHistory = new JobHistory();
    jobHistory.init(conf);

    JobIndexInfo jobIndexInfo = jobHistory.getJobFileInfo(jobId)
        .getJobIndexInfo();
    String jobhistoryFileName = FileNameIndexUtils
        .getDoneFileName(jobIndexInfo);

    Path historyFilePath = new Path(jobhistoryDir, jobhistoryFileName);
    FSDataInputStream in = null;
    FileContext fc = null;
    try {
      fc = FileContext.getFileContext(conf);
      in = fc.open(fc.makeQualified(historyFilePath));
    } catch (IOException ioe) {
      LOG.info("Can not open history file: " + historyFilePath, ioe);
      throw (new Exception("Can not open History File"));
    }

    JobHistoryParser parser = new JobHistoryParser(in);
    JobInfo jobInfo = parser.parse();
    Exception parseException = parser.getParseException();
    Assert.assertNull("Caught an expected exception " + parseException, 
        parseException);
    int noOffailedAttempts = 0;
    Map<TaskID, TaskInfo> allTasks = jobInfo.getAllTasks();
    for (Task task : job.getTasks().values()) {
      TaskInfo taskInfo = allTasks.get(TypeConverter.fromYarn(task.getID()));
      for (TaskAttempt taskAttempt : task.getAttempts().values()) {
        TaskAttemptInfo taskAttemptInfo = taskInfo.getAllTaskAttempts().get(
            TypeConverter.fromYarn((taskAttempt.getID())));
        // Verify rack-name for all task attempts
        Assert.assertEquals("rack-name is incorrect", taskAttemptInfo
            .getRackname(), RACK_NAME);
        if (taskAttemptInfo.getTaskStatus().equals("FAILED")) {
          noOffailedAttempts++;
        }
      }
    }
    Assert.assertEquals("No of Failed tasks doesn't match.", 2, noOffailedAttempts);
    } finally {
      LOG.info("FINISHED testHistoryParsingForFailedAttempts");
    }
  }
  
  @Test (timeout=5000)
  public void testCountersForFailedTask() throws Exception {
    LOG.info("STARTING testCountersForFailedTask");
    try {
    Configuration conf = new Configuration();
    conf
        .setClass(
            CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            MyResolver.class, DNSToSwitchMapping.class);
    RackResolver.init(conf);
    MRApp app = new MRAppWithHistoryWithFailedTask(2, 1, true,
        this.getClass().getName(), true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobId jobId = job.getID();
    app.waitForState(job, JobState.FAILED);

    // make sure all events are flushed
    app.waitForState(Service.STATE.STOPPED);

    String jobhistoryDir = JobHistoryUtils
        .getHistoryIntermediateDoneDirForUser(conf);
    JobHistory jobHistory = new JobHistory();
    jobHistory.init(conf);

    JobIndexInfo jobIndexInfo = jobHistory.getJobFileInfo(jobId)
        .getJobIndexInfo();
    String jobhistoryFileName = FileNameIndexUtils
        .getDoneFileName(jobIndexInfo);

    Path historyFilePath = new Path(jobhistoryDir, jobhistoryFileName);
    FSDataInputStream in = null;
    FileContext fc = null;
    try {
      fc = FileContext.getFileContext(conf);
      in = fc.open(fc.makeQualified(historyFilePath));
    } catch (IOException ioe) {
      LOG.info("Can not open history file: " + historyFilePath, ioe);
      throw (new Exception("Can not open History File"));
    }

    JobHistoryParser parser = new JobHistoryParser(in);
    JobInfo jobInfo = parser.parse();
    Exception parseException = parser.getParseException();
    Assert.assertNull("Caught an expected exception " + parseException,
        parseException);
    for (Map.Entry<TaskID,TaskInfo> entry : jobInfo.getAllTasks().entrySet()) {
      TaskId yarnTaskID = TypeConverter.toYarn(entry.getKey());
      CompletedTask ct = new CompletedTask(yarnTaskID, entry.getValue());
      Assert.assertNotNull("completed task report has null counters",
          ct.getReport().getCounters());
      //Make sure all the completedTask has counters, and the counters are not empty
      Assert.assertTrue(ct.getReport().getCounters()
          .getAllCounterGroups().size() > 0);
    }
    } finally {
      LOG.info("FINISHED testCountersForFailedTask");
    }
  }

  @Test
  public void testScanningOldDirs() throws Exception {
    LOG.info("STARTING testScanningOldDirs");
    try {
    Configuration conf = new Configuration();
    conf
        .setClass(
            CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            MyResolver.class, DNSToSwitchMapping.class);
    RackResolver.init(conf);
    MRApp app =
        new MRAppWithHistory(1, 1, true,
            this.getClass().getName(), true);
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
    Assert.assertNotNull("Unable to locate job history", fileInfo);

    // force the manager to "forget" the job
    hfm.deleteJobFromJobListCache(fileInfo);
    final int msecPerSleep = 10;
    int msecToSleep = 10 * 1000;
    while (fileInfo.isMovePending() && msecToSleep > 0) {
      Assert.assertTrue(!fileInfo.didMoveFail());
      msecToSleep -= msecPerSleep;
      Thread.sleep(msecPerSleep);
    }
    Assert.assertTrue("Timeout waiting for history move", msecToSleep > 0);

    fileInfo = hfm.getFileInfo(jobId);
    Assert.assertNotNull("Unable to locate old job history", fileInfo);
   } finally {
      LOG.info("FINISHED testScanningOldDirs");
    }
  }

  static class MRAppWithHistoryWithFailedAttempt extends MRAppWithHistory {

    public MRAppWithHistoryWithFailedAttempt(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      if (attemptID.getTaskId().getId() == 0 && attemptID.getId() == 0) {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_FAILMSG));
      } else {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_DONE));
      }
    }
  }

  static class MRAppWithHistoryWithFailedTask extends MRAppWithHistory {

    public MRAppWithHistoryWithFailedTask(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      if (attemptID.getTaskId().getId() == 0) {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_FAILMSG));
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
}
