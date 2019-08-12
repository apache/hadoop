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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
public class TestJobHistoryEntities {

  private final String historyFileName =
      "job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
  private final String historyFileNameZeroReduceTasks =
    "job_1416424547277_0002-1416424775281-root-TeraGen-1416424785433-2-0-SUCCEEDED-default-1416424779349.jhist";
  private final String confFileName = "job_1329348432655_0001_conf.xml";
  private final Configuration conf = new Configuration();
  private final JobACLsManager jobAclsManager = new JobACLsManager(conf);
  private boolean loadTasks;
  private JobId jobId = MRBuilderUtils.newJobId(1329348432655l, 1, 1);
  Path fullHistoryPath =
    new Path(this.getClass().getClassLoader().getResource(historyFileName)
        .getFile());
  Path fullHistoryPathZeroReduces =
    new Path(this.getClass().getClassLoader().getResource(historyFileNameZeroReduceTasks)
        .getFile());
  Path fullConfPath =
    new Path(this.getClass().getClassLoader().getResource(confFileName)
        .getFile());
  private CompletedJob completedJob;

  public TestJobHistoryEntities(boolean loadTasks) throws Exception {
    this.loadTasks = loadTasks;
  }

  @Parameters
  public static Collection<Object[]> data() {
    List<Object[]> list = new ArrayList<Object[]>(2);
    list.add(new Object[] { true });
    list.add(new Object[] { false });
    return list;
  }

  /* Verify some expected values based on the history file */
  @Test (timeout=100000)
  public void testCompletedJob() throws Exception {
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    when(info.getHistoryFile()).thenReturn(fullHistoryPath);
    //Re-initialize to verify the delayed load.
    completedJob =
      new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user",
          info, jobAclsManager);
    //Verify tasks loaded based on loadTask parameter.
    assertEquals(loadTasks, completedJob.tasksLoaded.get());
    assertEquals(1, completedJob.getAMInfos().size());
    assertEquals(10, completedJob.getCompletedMaps());
    assertEquals(1, completedJob.getCompletedReduces());
    assertEquals(12, completedJob.getTasks().size());
    //Verify tasks loaded at this point.
    assertThat(completedJob.tasksLoaded.get()).isTrue();
    assertEquals(10, completedJob.getTasks(TaskType.MAP).size());
    assertEquals(2, completedJob.getTasks(TaskType.REDUCE).size());
    assertEquals("user", completedJob.getUserName());
    assertEquals(JobState.SUCCEEDED, completedJob.getState());
    JobReport jobReport = completedJob.getReport();
    assertEquals("user", jobReport.getUser());
    assertEquals(JobState.SUCCEEDED, jobReport.getJobState());
    assertEquals(fullHistoryPath.toString(), jobReport.getHistoryFile());
  }
  
  @Test (timeout=100000)
  public void testCopmletedJobReportWithZeroTasks() throws Exception {
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    when(info.getHistoryFile()).thenReturn(fullHistoryPathZeroReduces);
    completedJob =
      new CompletedJob(conf, jobId, fullHistoryPathZeroReduces, loadTasks, "user",
          info, jobAclsManager);
    JobReport jobReport = completedJob.getReport();
    // Make sure that the number reduces (completed and total) are equal to zero.
    assertEquals(0, completedJob.getTotalReduces());
    assertEquals(0, completedJob.getCompletedReduces());
    // Verify that the reduce progress is 1.0 (not NaN)
    assertEquals(1.0, jobReport.getReduceProgress(), 0.001);
    assertEquals(fullHistoryPathZeroReduces.toString(),
        jobReport.getHistoryFile());
  }

  @Test (timeout=10000)
  public void testCompletedTask() throws Exception {
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    completedJob =
      new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user",
          info, jobAclsManager);
    TaskId mt1Id = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    TaskId rt1Id = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    
    Map<TaskId, Task> mapTasks = completedJob.getTasks(TaskType.MAP);
    Map<TaskId, Task> reduceTasks = completedJob.getTasks(TaskType.REDUCE);
    assertEquals(10, mapTasks.size());
    assertEquals(2, reduceTasks.size());
    
    Task mt1 = mapTasks.get(mt1Id);
    assertEquals(1, mt1.getAttempts().size());
    assertEquals(TaskState.SUCCEEDED, mt1.getState());
    TaskReport mt1Report = mt1.getReport();
    assertEquals(TaskState.SUCCEEDED, mt1Report.getTaskState());
    assertEquals(mt1Id, mt1Report.getTaskId());
    Task rt1 = reduceTasks.get(rt1Id);
    assertEquals(1, rt1.getAttempts().size());
    assertEquals(TaskState.SUCCEEDED, rt1.getState());
    TaskReport rt1Report = rt1.getReport();
    assertEquals(TaskState.SUCCEEDED, rt1Report.getTaskState());
    assertEquals(rt1Id, rt1Report.getTaskId());
  }

  @Test (timeout=10000)
  public void testCompletedTaskAttempt() throws Exception {
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    completedJob =
      new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user",
          info, jobAclsManager);
    TaskId mt1Id = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    TaskId rt1Id = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    TaskAttemptId mta1Id = MRBuilderUtils.newTaskAttemptId(mt1Id, 0);
    TaskAttemptId rta1Id = MRBuilderUtils.newTaskAttemptId(rt1Id, 0);
    
    Task mt1 = completedJob.getTask(mt1Id);
    Task rt1 = completedJob.getTask(rt1Id);
    
    TaskAttempt mta1 = mt1.getAttempt(mta1Id);
    assertEquals(TaskAttemptState.SUCCEEDED, mta1.getState());
    assertEquals("localhost:45454", mta1.getAssignedContainerMgrAddress());
    assertEquals("localhost:9999", mta1.getNodeHttpAddress());
    TaskAttemptReport mta1Report = mta1.getReport();
    assertEquals(TaskAttemptState.SUCCEEDED, mta1Report.getTaskAttemptState());
    assertEquals("localhost", mta1Report.getNodeManagerHost());
    assertEquals(45454, mta1Report.getNodeManagerPort());
    assertEquals(9999, mta1Report.getNodeManagerHttpPort());
    
    TaskAttempt rta1 = rt1.getAttempt(rta1Id);
    assertEquals(TaskAttemptState.SUCCEEDED, rta1.getState());
    assertEquals("localhost:45454", rta1.getAssignedContainerMgrAddress());
    assertEquals("localhost:9999", rta1.getNodeHttpAddress());
    TaskAttemptReport rta1Report = rta1.getReport();
    assertEquals(TaskAttemptState.SUCCEEDED, rta1Report.getTaskAttemptState());
    assertEquals("localhost", rta1Report.getNodeManagerHost());
    assertEquals(45454, rta1Report.getNodeManagerPort());
    assertEquals(9999, rta1Report.getNodeManagerHttpPort());
  }
  /**
   * Simple test of some methods of CompletedJob
   * @throws Exception
   */
  @Test (timeout=30000)
  public void testGetTaskAttemptCompletionEvent() throws Exception{
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    completedJob =
      new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user",
          info, jobAclsManager);
    TaskCompletionEvent[] events= completedJob.getMapAttemptCompletionEvents(0,1000);
    assertEquals(10, completedJob.getMapAttemptCompletionEvents(0,10).length);
    int currentEventId=0;
    for (TaskCompletionEvent taskAttemptCompletionEvent : events) {
      int eventId= taskAttemptCompletionEvent.getEventId();
      assertTrue(eventId>=currentEventId);
      currentEventId=eventId;
    }
    assertNull(completedJob.loadConfFile() );
    // job name
    assertEquals("Sleep job",completedJob.getName());
    // queue name
    assertEquals("default",completedJob.getQueueName());
    // progress
    assertEquals(1.0, completedJob.getProgress(),0.001);
    // 12 rows in answer
    assertEquals(12,completedJob.getTaskAttemptCompletionEvents(0,1000).length);
    // select first 10 rows
    assertEquals(10,completedJob.getTaskAttemptCompletionEvents(0,10).length);
    // select 5-10 rows include 5th
    assertEquals(7,completedJob.getTaskAttemptCompletionEvents(5,10).length);

    // without errors
    assertEquals(1,completedJob.getDiagnostics().size());
    assertEquals("",completedJob.getDiagnostics().get(0));

    assertEquals(0, completedJob.getJobACLs().size());

  }

  @Test (timeout=30000)
  public void testCompletedJobWithDiagnostics() throws Exception {
    final String jobError = "Job Diagnostics";
    JobInfo jobInfo = spy(new JobInfo());
    when(jobInfo.getErrorInfo()).thenReturn(jobError);
    when(jobInfo.getJobStatus()).thenReturn(JobState.FAILED.toString());
    when(jobInfo.getAMInfos()).thenReturn(Collections.<JobHistoryParser.AMInfo>emptyList());
    final JobHistoryParser mockParser = mock(JobHistoryParser.class);
    when(mockParser.parse()).thenReturn(jobInfo);
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    when(info.getHistoryFile()).thenReturn(fullHistoryPath);
    CompletedJob job =
      new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user",
          info, jobAclsManager) {
            @Override
            protected JobHistoryParser createJobHistoryParser(
                Path historyFileAbsolute) throws IOException {
               return mockParser;
            }
    };
    assertEquals(jobError, job.getReport().getDiagnostics());
  }
}
