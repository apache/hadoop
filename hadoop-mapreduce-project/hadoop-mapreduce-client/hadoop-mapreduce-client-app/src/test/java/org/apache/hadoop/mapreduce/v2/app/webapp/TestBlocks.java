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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class TestBlocks {
  private ByteArrayOutputStream data = new ByteArrayOutputStream();

  /**
   * Test rendering for ConfBlock
   */
  @Test
  public void testConfigurationBlock() throws Exception {
    AppContext ctx = mock(AppContext.class);
    Job job = mock(Job.class);
    Path path = new Path("conf");
    Configuration configuration = new Configuration();
    configuration.set("Key for test", "Value for test");
    when(job.getConfFile()).thenReturn(path);
    when(job.loadConfFile()).thenReturn(configuration);

    when(ctx.getJob(any(JobId.class))).thenReturn(job);


    ConfBlockForTest configurationBlock = new ConfBlockForTest(ctx);
    PrintWriter pWriter = new PrintWriter(data);
    Block html = new BlockForTest(new HtmlBlockForTest(), pWriter, 0, false);

    configurationBlock.render(html);
    pWriter.flush();
    assertTrue(data.toString().contains(
            "Sorry, can't do anything without a JobID"));

    configurationBlock.addParameter(AMParams.JOB_ID, "job_01_01");
    data.reset();
    configurationBlock.render(html);
    pWriter.flush();
    assertTrue(data.toString().contains("Key for test"));

    assertTrue(data.toString().contains("Value for test"));

  }

  /**
   * Test rendering for TasksBlock
   */
  @Test
  public void testTasksBlock() throws Exception {

    ApplicationId appId = ApplicationIdPBImpl.newInstance(0, 1);
    JobId jobId = new JobIdPBImpl();
    jobId.setId(0);
    jobId.setAppId(appId);

    TaskId taskId = new TaskIdPBImpl();
    taskId.setId(0);
    taskId.setTaskType(TaskType.MAP);
    taskId.setJobId(jobId);
    Task task = mock(Task.class);
    when(task.getID()).thenReturn(taskId);
    TaskReport report = mock(TaskReport.class);
    when(report.getProgress()).thenReturn(0.7f);
    when(report.getTaskState()).thenReturn(TaskState.SUCCEEDED);
    when(report.getStartTime()).thenReturn(100001L);
    when(report.getFinishTime()).thenReturn(100011L);


    when(task.getReport()).thenReturn(report);
    when(task.getType()).thenReturn(TaskType.MAP);


    Map<TaskId, Task> tasks = new HashMap<TaskId, Task>();
    tasks.put(taskId, task);
    AppContext ctx = mock(AppContext.class);
    Job job = mock(Job.class);
    when(job.getTasks()).thenReturn(tasks);


    App app = new App(ctx);
    app.setJob(job);
    TasksBlockForTest taskBlock = new TasksBlockForTest(app);
    taskBlock.addParameter(AMParams.TASK_TYPE, "m");

    PrintWriter pWriter = new PrintWriter(data);
    Block html = new BlockForTest(new HtmlBlockForTest(), pWriter, 0, false);

    taskBlock.render(html);
    pWriter.flush();
    assertTrue(data.toString().contains("task_0_0001_m_000000"));
    assertTrue(data.toString().contains("70.00"));
    assertTrue(data.toString().contains("SUCCEEDED"));
    assertTrue(data.toString().contains("100001"));
    assertTrue(data.toString().contains("100011"));


  }

  private class ConfBlockForTest extends ConfBlock {
    private final Map<String, String> params = new HashMap<String, String>();

    public void addParameter(String name, String value) {
      params.put(name, value);
    }

    @Override
    public String $(String key, String defaultValue) {
      String value = params.get(key);
      return value == null ? defaultValue : value;
    }

    ConfBlockForTest(AppContext appCtx) {
      super(appCtx);
    }

  }

  private class HtmlBlockForTest extends HtmlBlock {

    @Override
    protected void render(Block html) {

    }
  }

}
