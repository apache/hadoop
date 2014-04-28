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

package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.CompletedJob;
import org.apache.hadoop.mapreduce.v2.hs.TestJobHistoryEntities;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.junit.Test;


public class TestJobInfo {

  @Test(timeout = 10000)
  public void testAverageMergeTime() throws IOException {
    String historyFileName =
        "job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
    String confFileName =
        "job_1329348432655_0001_conf.xml";
    Configuration conf = new Configuration();
    JobACLsManager jobAclsMgr = new JobACLsManager(conf);
    Path fulleHistoryPath =
        new Path(TestJobHistoryEntities.class.getClassLoader()
            .getResource(historyFileName)
            .getFile());
    Path fullConfPath =
        new Path(TestJobHistoryEntities.class.getClassLoader()
            .getResource(confFileName)
            .getFile());

    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);

    JobId jobId = MRBuilderUtils.newJobId(1329348432655l, 1, 1);
    CompletedJob completedJob =
        new CompletedJob(conf, jobId, fulleHistoryPath, true, "user",
            info, jobAclsMgr);
    JobInfo jobInfo = new JobInfo(completedJob);
    // There are 2 tasks with merge time of 45 and 55 respectively. So average
    // merge time should be 50.
    Assert.assertEquals(50L, jobInfo.getAvgMergeTime().longValue());
  }
  
  @Test
  public void testAverageReduceTime() {
	  
    Job job = mock(CompletedJob.class);
    final Task task1 = mock(Task.class);
    final Task task2 = mock(Task.class);
  
    JobId  jobId = MRBuilderUtils.newJobId(1L, 1, 1);
  
    final TaskId taskId1 = MRBuilderUtils.newTaskId(jobId, 1, TaskType.REDUCE);
    final TaskId taskId2 = MRBuilderUtils.newTaskId(jobId, 2, TaskType.REDUCE);
  
    final TaskAttemptId taskAttemptId1  = MRBuilderUtils.
    		newTaskAttemptId(taskId1, 1);
    final TaskAttemptId taskAttemptId2  = MRBuilderUtils.
    		newTaskAttemptId(taskId2, 2);
  
    final TaskAttempt taskAttempt1 = mock(TaskAttempt.class);
    final TaskAttempt taskAttempt2 = mock(TaskAttempt.class);
  
    JobReport jobReport = mock(JobReport.class);
  
    when(taskAttempt1.getState()).thenReturn(TaskAttemptState.SUCCEEDED);
    when(taskAttempt1.getLaunchTime()).thenReturn(0L);
    when(taskAttempt1.getShuffleFinishTime()).thenReturn(4L);
    when(taskAttempt1.getSortFinishTime()).thenReturn(6L);
    when(taskAttempt1.getFinishTime()).thenReturn(8L);
  
    when(taskAttempt2.getState()).thenReturn(TaskAttemptState.SUCCEEDED);
    when(taskAttempt2.getLaunchTime()).thenReturn(5L);
    when(taskAttempt2.getShuffleFinishTime()).thenReturn(10L);
    when(taskAttempt2.getSortFinishTime()).thenReturn(22L);
    when(taskAttempt2.getFinishTime()).thenReturn(42L);
  
  
    when(task1.getType()).thenReturn(TaskType.REDUCE);
    when(task2.getType()).thenReturn(TaskType.REDUCE);
    when(task1.getAttempts()).thenReturn
      (new HashMap<TaskAttemptId, TaskAttempt>() 
    		{{put(taskAttemptId1,taskAttempt1); }});
    when(task2.getAttempts()).thenReturn
      (new HashMap<TaskAttemptId, TaskAttempt>() 
    		  {{put(taskAttemptId2,taskAttempt2); }});
  
    when(job.getTasks()).thenReturn
      (new HashMap<TaskId, Task>() 
    		{{ put(taskId1,task1); put(taskId2, task2);  }});
    when(job.getID()).thenReturn(jobId);
  
    when(job.getReport()).thenReturn(jobReport);
  
    when(job.getName()).thenReturn("TestJobInfo");	  
    when(job.getState()).thenReturn(JobState.SUCCEEDED);	  
  
    JobInfo jobInfo = new JobInfo(job);
  
    Assert.assertEquals(11L, jobInfo.getAvgReduceTime().longValue());
  }  
}
