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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
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
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMParams;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.AppForTest;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HsTaskPage.AttemptsBlock;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import org.apache.hadoop.yarn.webapp.Params;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsPage;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Test some HtmlBlock classes
 */

public class TestBlocks {
  private ByteArrayOutputStream data = new ByteArrayOutputStream();

  /**
   * test HsTasksBlock's rendering.
   */
  @Test
  public void testHsTasksBlock() {

    Task task = getTask(0);

    Map<TaskId, Task> tasks = new HashMap<TaskId, Task>();
    tasks.put(task.getID(), task);

    AppContext ctx = mock(AppContext.class);
    AppForTest app = new AppForTest(ctx);
    Job job = mock(Job.class);
    when(job.getTasks()).thenReturn(tasks);

    app.setJob(job);

    HsTasksBlockForTest block = new HsTasksBlockForTest(app);

    block.addParameter(AMParams.TASK_TYPE, "r");

    PrintWriter pWriter = new PrintWriter(data);
    Block html = new BlockForTest(new HtmlBlockForTest(), pWriter, 0, false);

    block.render(html);
    pWriter.flush();
    // should be printed information about task
    assertTrue(data.toString().contains("task_0_0001_r_000000"));
    assertTrue(data.toString().contains("SUCCEEDED"));
    assertTrue(data.toString().contains("100001"));
    assertTrue(data.toString().contains("100011"));
    assertTrue(data.toString().contains(""));
  }

  /**
   * test AttemptsBlock's rendering.
   */
  @Test
  public void testAttemptsBlock() {
    AppContext ctx = mock(AppContext.class);
    AppForTest app = new AppForTest(ctx);

    Task task = getTask(0);
    Map<TaskAttemptId, TaskAttempt> attempts = new HashMap<TaskAttemptId, TaskAttempt>();
    TaskAttempt attempt = mock(TaskAttempt.class);
    TaskAttemptId taId = new TaskAttemptIdPBImpl();
    taId.setId(0);
    taId.setTaskId(task.getID());
    when(attempt.getID()).thenReturn(taId);
    when(attempt.getNodeHttpAddress()).thenReturn("Node address");

    ApplicationId appId = ApplicationIdPBImpl.newInstance(0, 5);
    ApplicationAttemptId appAttemptId = ApplicationAttemptIdPBImpl.newInstance(appId, 1);

    ContainerId containerId = ContainerIdPBImpl.newInstance(appAttemptId, 1);
    when(attempt.getAssignedContainerID()).thenReturn(containerId);

    when(attempt.getAssignedContainerMgrAddress()).thenReturn(
            "assignedContainerMgrAddress");
    when(attempt.getNodeRackName()).thenReturn("nodeRackName");

    final long taStartTime = 100002L;
    final long taFinishTime = 100012L;
    final long taShuffleFinishTime = 100010L;
    final long taSortFinishTime = 100011L;
    final TaskAttemptState taState = TaskAttemptState.SUCCEEDED;

    when(attempt.getLaunchTime()).thenReturn(taStartTime);
    when(attempt.getFinishTime()).thenReturn(taFinishTime);
    when(attempt.getShuffleFinishTime()).thenReturn(taShuffleFinishTime);
    when(attempt.getSortFinishTime()).thenReturn(taSortFinishTime);
    when(attempt.getState()).thenReturn(taState);

    TaskAttemptReport taReport = mock(TaskAttemptReport.class);
    when(taReport.getStartTime()).thenReturn(taStartTime);
    when(taReport.getFinishTime()).thenReturn(taFinishTime);
    when(taReport.getShuffleFinishTime()).thenReturn(taShuffleFinishTime);
    when(taReport.getSortFinishTime()).thenReturn(taSortFinishTime);
    when(taReport.getContainerId()).thenReturn(containerId);
    when(taReport.getProgress()).thenReturn(1.0f);
    when(taReport.getStateString()).thenReturn("Processed 128/128 records");
    when(taReport.getTaskAttemptState()).thenReturn(taState);
    when(taReport.getDiagnosticInfo()).thenReturn("");

    when(attempt.getReport()).thenReturn(taReport);

    attempts.put(taId, attempt);
    when(task.getAttempts()).thenReturn(attempts);

    app.setTask(task);
    Job job = mock(Job.class);
    when(job.getUserName()).thenReturn("User");
    app.setJob(job);

    AttemptsBlockForTest block = new AttemptsBlockForTest(app);
    block.addParameter(AMParams.TASK_TYPE, "r");

    PrintWriter pWriter = new PrintWriter(data);
    Block html = new BlockForTest(new HtmlBlockForTest(), pWriter, 0, false);

    block.render(html);
    pWriter.flush();
    // should be printed information about attempts
    assertTrue(data.toString().contains("0 attempt_0_0001_r_000000_0"));
    assertTrue(data.toString().contains("SUCCEEDED"));
    assertTrue(data.toString().contains(
            "_0005_01_000001:attempt_0_0001_r_000000_0:User:"));
    assertTrue(data.toString().contains("100002"));
    assertTrue(data.toString().contains("100010"));
    assertTrue(data.toString().contains("100011"));
    assertTrue(data.toString().contains("100012"));
  }

  /**
   * test HsJobsBlock's rendering.
   */
  @Test
  public void testHsJobsBlock() {
    AppContext ctx = mock(AppContext.class);
    Map<JobId, Job> jobs = new HashMap<JobId, Job>();
    Job job = getJob();
    jobs.put(job.getID(), job);
    when(ctx.getAllJobs()).thenReturn(jobs);

    HsJobsBlock block = new HsJobsBlockForTest(ctx);
    PrintWriter pWriter = new PrintWriter(data);
    Block html = new BlockForTest(new HtmlBlockForTest(), pWriter, 0, false);
    block.render(html);

    pWriter.flush();
    assertTrue(data.toString().contains("JobName"));
    assertTrue(data.toString().contains("UserName"));
    assertTrue(data.toString().contains("QueueName"));
    assertTrue(data.toString().contains("SUCCEEDED"));
  }
  /**
   * test HsController
   */

  @Test
  public void testHsController() throws Exception {
    AppContext ctx = mock(AppContext.class);
    ApplicationId appId = ApplicationIdPBImpl.newInstance(0,5);
    
    when(ctx.getApplicationID()).thenReturn(appId);

    AppForTest app = new AppForTest(ctx);
    Configuration config = new Configuration();
    RequestContext requestCtx = mock(RequestContext.class);
    HsControllerForTest controller = new HsControllerForTest(app, config,
            requestCtx);
    controller.index();
    assertEquals("JobHistory", controller.get(Params.TITLE, ""));
    assertEquals(HsJobPage.class, controller.jobPage());
    assertEquals(HsCountersPage.class, controller.countersPage());
    assertEquals(HsTasksPage.class, controller.tasksPage());
    assertEquals(HsTaskPage.class, controller.taskPage());
    assertEquals(HsAttemptsPage.class, controller.attemptsPage());

    controller.set(AMParams.JOB_ID, "job_01_01");
    controller.set(AMParams.TASK_ID, "task_01_01_m01_01");
    controller.set(AMParams.TASK_TYPE, "m");
    controller.set(AMParams.ATTEMPT_STATE, "State");

    Job job = mock(Job.class);
    Task task = mock(Task.class);
    when(job.getTask(any(TaskId.class))).thenReturn(task);
    JobId jobID = MRApps.toJobID("job_01_01");
    when(ctx.getJob(jobID)).thenReturn(job);
    when(job.checkAccess(any(UserGroupInformation.class), any(JobACL.class)))
            .thenReturn(true);

    controller.job();
    assertEquals(HsJobPage.class, controller.getClazz());
    controller.jobCounters();
    assertEquals(HsCountersPage.class, controller.getClazz());
    controller.taskCounters();
    assertEquals(HsCountersPage.class, controller.getClazz());
    controller.tasks();
    assertEquals(HsTasksPage.class, controller.getClazz());
    controller.task();
    assertEquals(HsTaskPage.class, controller.getClazz());
    controller.attempts();
    assertEquals(HsAttemptsPage.class, controller.getClazz());

    assertEquals(HsConfPage.class, controller.confPage());
    assertEquals(HsAboutPage.class, controller.aboutPage());
    controller.about();
    assertEquals(HsAboutPage.class, controller.getClazz());
    controller.logs();
    assertEquals(HsLogsPage.class, controller.getClazz());
    controller.nmlogs();
    assertEquals(AggregatedLogsPage.class, controller.getClazz());

    assertEquals(HsSingleCounterPage.class, controller.singleCounterPage());
    controller.singleJobCounter();
    assertEquals(HsSingleCounterPage.class, controller.getClazz());
    controller.singleTaskCounter();
    assertEquals(HsSingleCounterPage.class, controller.getClazz());


  }

  private static class HsControllerForTest extends HsController {

    static private Map<String, String> params = new HashMap<String, String>();
    private Class<?> clazz;
    ByteArrayOutputStream data = new ByteArrayOutputStream();

    public void set(String name, String value) {
      params.put(name, value);
    }

    public String get(String key, String defaultValue) {
      String value = params.get(key);
      return value == null ? defaultValue : value;
    }

    HsControllerForTest(App app, Configuration configuration, RequestContext ctx) {
      super(app, configuration, ctx);
    }

    @Override
    public HttpServletRequest request() {
      HttpServletRequest result = mock(HttpServletRequest.class);
      when(result.getRemoteUser()).thenReturn("User");
      return result;
    }

    public HttpServletResponse response() {
      HttpServletResponse result = mock(HttpServletResponse.class);
      try {
        when(result.getWriter()).thenReturn(new PrintWriter(data));
      } catch (IOException ignored) {

      }

      return result;
    }

    protected void render(Class<? extends View> cls) {
      clazz = cls;
    }

    public Class<?> getClazz() {
      return clazz;
    }
  }

  private Job getJob() {
    Job job = mock(Job.class);

    JobId jobId = new JobIdPBImpl();

    ApplicationId appId = ApplicationIdPBImpl.newInstance(System.currentTimeMillis(),4);
    jobId.setAppId(appId);
    jobId.setId(1);
    when(job.getID()).thenReturn(jobId);

    JobReport report = mock(JobReport.class);
    when(report.getStartTime()).thenReturn(100010L);
    when(report.getFinishTime()).thenReturn(100015L);

    when(job.getReport()).thenReturn(report);
    when(job.getName()).thenReturn("JobName");
    when(job.getUserName()).thenReturn("UserName");
    when(job.getQueueName()).thenReturn("QueueName");
    when(job.getState()).thenReturn(JobState.SUCCEEDED);
    when(job.getTotalMaps()).thenReturn(3);
    when(job.getCompletedMaps()).thenReturn(2);
    when(job.getTotalReduces()).thenReturn(2);
    when(job.getCompletedReduces()).thenReturn(1);
    when(job.getCompletedReduces()).thenReturn(1);
    return job;
  }


  private Task getTask(long timestamp) {
    
    JobId jobId = new JobIdPBImpl();
    jobId.setId(0);
    jobId.setAppId(ApplicationIdPBImpl.newInstance(timestamp,1));

    TaskId taskId = new TaskIdPBImpl();
    taskId.setId(0);
    taskId.setTaskType(TaskType.REDUCE);
    taskId.setJobId(jobId);
    Task task = mock(Task.class);
    when(task.getID()).thenReturn(taskId);
    TaskReport report = mock(TaskReport.class);
    when(report.getProgress()).thenReturn(0.7f);
    when(report.getTaskState()).thenReturn(TaskState.SUCCEEDED);
    when(report.getStartTime()).thenReturn(100001L);
    when(report.getFinishTime()).thenReturn(100011L);

    when(task.getReport()).thenReturn(report);
    when(task.getType()).thenReturn(TaskType.REDUCE);
    return task;
  }

  private class HsJobsBlockForTest extends HsJobsBlock {
    HsJobsBlockForTest(AppContext appCtx) {
      super(appCtx);
    }

    @Override
    public String url(String... parts) {
      String result = "url://";
      for (String string : parts) {
        result += string + ":";
      }
      return result;
    }

  }

  private class AttemptsBlockForTest extends AttemptsBlock {
    private final Map<String, String> params = new HashMap<String, String>();

    public void addParameter(String name, String value) {
      params.put(name, value);
    }

    public String $(String key, String defaultValue) {
      String value = params.get(key);
      return value == null ? defaultValue : value;
    }

    public AttemptsBlockForTest(App ctx) {
      super(ctx);
    }

    @Override
    public String url(String... parts) {
      String result = "url://";
      for (String string : parts) {
        result += string + ":";
      }
      return result;
    }

  }

  private class HsTasksBlockForTest extends HsTasksBlock {
    private final Map<String, String> params = new HashMap<String, String>();

    public void addParameter(String name, String value) {
      params.put(name, value);
    }

    public String $(String key, String defaultValue) {
      String value = params.get(key);
      return value == null ? defaultValue : value;
    }

    @Override
    public String url(String... parts) {
      String result = "url://";
      for (String string : parts) {
        result += string + ":";
      }
      return result;
    }

    public HsTasksBlockForTest(App app) {
      super(app);
    }
  }

  private class HtmlBlockForTest extends HtmlBlock {

    @Override
    protected void render(Block html) {

    }
  }

}
