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
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.AMInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.hs.TestJobHistoryEvents.MRAppWithHistory;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

public class TestJobHistoryParsing {
  private static final Log LOG = LogFactory.getLog(TestJobHistoryParsing.class);
  @Test
  public void testHistoryParsing() throws Exception {
    Configuration conf = new Configuration();
    long amStartTimeEst = System.currentTimeMillis();
    MRApp app = new MRAppWithHistory(2, 1, true, this.getClass().getName(), true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobId jobId = job.getID();
    LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
    app.waitForState(job, JobState.SUCCEEDED);
    
    //make sure all events are flushed
    app.waitForState(Service.STATE.STOPPED);
    
    String jobhistoryDir = JobHistoryUtils.getHistoryIntermediateDoneDirForUser(conf);
    JobHistory jobHistory = new JobHistory();
    jobHistory.init(conf);
    
    JobIndexInfo jobIndexInfo = jobHistory.getJobMetaInfo(jobId).getJobIndexInfo();
    String jobhistoryFileName = FileNameIndexUtils.getDoneFileName(jobIndexInfo);
    
    Path historyFilePath = new Path(jobhistoryDir, jobhistoryFileName);
    FSDataInputStream in = null;
    LOG.info("JobHistoryFile is: " + historyFilePath);
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
    
    Assert.assertEquals ("Incorrect username ",
        "mapred", jobInfo.getUsername());
    Assert.assertEquals("Incorrect jobName ",
        "test", jobInfo.getJobname());
    Assert.assertEquals("Incorrect queuename ",
        "default", jobInfo.getJobQueueName());
    Assert.assertEquals("incorrect conf path",
        "test", jobInfo.getJobConfPath());
    Assert.assertEquals("incorrect finishedMap ",
        2, jobInfo.getFinishedMaps());
    Assert.assertEquals("incorrect finishedReduces ",
        1, jobInfo.getFinishedReduces());
    Assert.assertEquals("incorrect uberized ",
        job.isUber(), jobInfo.getUberized());
    int totalTasks = jobInfo.getAllTasks().size();
    Assert.assertEquals("total number of tasks is incorrect  ", 3, totalTasks);
    
    // Verify aminfo
    Assert.assertEquals(1, jobInfo.getAMInfos().size());
    Assert.assertEquals("testhost", jobInfo.getAMInfos().get(0)
        .getNodeManagerHost());
    AMInfo amInfo = jobInfo.getAMInfos().get(0);
    Assert.assertEquals(3333, amInfo.getNodeManagerHttpPort());
    Assert.assertEquals(1, amInfo.getAppAttemptId().getAttemptId());
    Assert.assertEquals(amInfo.getAppAttemptId(), amInfo.getContainerId()
        .getApplicationAttemptId());
    Assert.assertTrue(amInfo.getStartTime() <= System.currentTimeMillis()
        && amInfo.getStartTime() >= amStartTimeEst);

    ContainerId fakeCid = BuilderUtils.newContainerId(-1, -1, -1, -1);
    //Assert at taskAttempt level
    for (TaskInfo taskInfo : jobInfo.getAllTasks().values()) {
      int taskAttemptCount = taskInfo.getAllTaskAttempts().size();
      Assert.assertEquals("total number of task attempts ",
          1, taskAttemptCount);
      TaskAttemptInfo taInfo =
          taskInfo.getAllTaskAttempts().values().iterator().next();
      Assert.assertNotNull(taInfo.getContainerId());
      //Verify the wrong ctor is not being used. Remove after mrv1 is removed.
      Assert.assertFalse(taInfo.getContainerId().equals(fakeCid));
    }

    // Deep compare Job and JobInfo
    for (Task task : job.getTasks().values()) {
      TaskInfo taskInfo = jobInfo.getAllTasks().get(
          TypeConverter.fromYarn(task.getID()));
      Assert.assertNotNull("TaskInfo not found", taskInfo);
      for (TaskAttempt taskAttempt : task.getAttempts().values()) {
        TaskAttemptInfo taskAttemptInfo =
          taskInfo.getAllTaskAttempts().get(
              TypeConverter.fromYarn((taskAttempt.getID())));
        Assert.assertNotNull("TaskAttemptInfo not found", taskAttemptInfo);
        Assert.assertEquals("Incorrect shuffle port for task attempt",
            taskAttempt.getShufflePort(), taskAttemptInfo.getShufflePort());
      }
    }

    String summaryFileName = JobHistoryUtils
        .getIntermediateSummaryFileName(jobId);
    Path summaryFile = new Path(jobhistoryDir, summaryFileName);
    String jobSummaryString = jobHistory.getJobSummary(fc, summaryFile);
    Assert.assertNotNull(jobSummaryString);

    Map<String, String> jobSummaryElements = new HashMap<String, String>();
    StringTokenizer strToken = new StringTokenizer(jobSummaryString, ",");
    while (strToken.hasMoreTokens()) {
      String keypair = strToken.nextToken();
      jobSummaryElements.put(keypair.split("=")[0], keypair.split("=")[1]);

    }

    Assert.assertEquals("JobId does not match", jobId.toString(),
        jobSummaryElements.get("jobId"));
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
    Assert.assertEquals("Mismatch in num map slots", 2,
        Integer.parseInt(jobSummaryElements.get("numMaps")));
    Assert.assertEquals("Mismatch in num reduce slots", 1,
        Integer.parseInt(jobSummaryElements.get("numReduces")));
    Assert.assertEquals("User does not match", "mapred",
        jobSummaryElements.get("user"));
    Assert.assertEquals("Queue does not match", "default",
        jobSummaryElements.get("queue"));
    Assert.assertEquals("Status does not match", "SUCCEEDED",
        jobSummaryElements.get("status"));
  }

  public static void main(String[] args) throws Exception {
    TestJobHistoryParsing t = new TestJobHistoryParsing();
    t.testHistoryParsing();
  }
}
