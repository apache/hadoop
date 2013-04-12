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

package org.apache.hadoop.mapreduce.v2.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.Test;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestRecovery {

  private static final Log LOG = LogFactory.getLog(TestRecovery.class);
  private static Path outputDir = new Path(new File("target", 
      TestRecovery.class.getName()).getAbsolutePath() + 
      Path.SEPARATOR + "out");
  private static String partFile = "part-r-00000";
  private Text key1 = new Text("key1");
  private Text key2 = new Text("key2");
  private Text val1 = new Text("val1");
  private Text val2 = new Text("val2");

  /**
   * AM with 2 maps and 1 reduce. For 1st map, one attempt fails, one attempt
   * completely disappears because of failed launch, one attempt gets killed and
   * one attempt succeeds. AM crashes after the first tasks finishes and
   * recovers completely and succeeds in the second generation.
   * 
   * @throws Exception
   */
  @Test
  public void testCrashed() throws Exception {

    int runCount = 0;
    long am1StartTimeEst = System.currentTimeMillis();
    MRApp app = new MRAppWithHistory(2, 1, false, this.getClass().getName(), true, ++runCount);
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    long jobStartTime = job.getReport().getStartTime();
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task mapTask2 = it.next();
    Task reduceTask = it.next();
    
    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    TaskAttempt task1Attempt1 = mapTask1.getAttempts().values().iterator().next();
    TaskAttempt task2Attempt = mapTask2.getAttempts().values().iterator().next();
    
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt1, TaskAttemptState.RUNNING);
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);
    
    // reduces must be in NEW state
    Assert.assertEquals("Reduce Task state not correct",
        TaskState.RUNNING, reduceTask.getReport().getTaskState());

    /////////// Play some games with the TaskAttempts of the first task //////
    //send the fail signal to the 1st map task attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt1.getID(),
            TaskAttemptEventType.TA_FAILMSG));
    
    app.waitForState(task1Attempt1, TaskAttemptState.FAILED);

    int timeOut = 0;
    while (mapTask1.getAttempts().size() != 2 && timeOut++ < 10) {
      Thread.sleep(2000);
      LOG.info("Waiting for next attempt to start");
    }
    Assert.assertEquals(2, mapTask1.getAttempts().size());
    Iterator<TaskAttempt> itr = mapTask1.getAttempts().values().iterator();
    itr.next();
    TaskAttempt task1Attempt2 = itr.next();
    
    // This attempt will automatically fail because of the way ContainerLauncher
    // is setup
    // This attempt 'disappears' from JobHistory and so causes MAPREDUCE-3846
    app.getContext().getEventHandler().handle(
      new TaskAttemptEvent(task1Attempt2.getID(),
        TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED));
    app.waitForState(task1Attempt2, TaskAttemptState.FAILED);

    timeOut = 0;
    while (mapTask1.getAttempts().size() != 3 && timeOut++ < 10) {
      Thread.sleep(2000);
      LOG.info("Waiting for next attempt to start");
    }
    Assert.assertEquals(3, mapTask1.getAttempts().size());
    itr = mapTask1.getAttempts().values().iterator();
    itr.next();
    itr.next();
    TaskAttempt task1Attempt3 = itr.next();
    
    app.waitForState(task1Attempt3, TaskAttemptState.RUNNING);

    //send the kill signal to the 1st map 3rd attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt3.getID(),
            TaskAttemptEventType.TA_KILL));
    
    app.waitForState(task1Attempt3, TaskAttemptState.KILLED);

    timeOut = 0;
    while (mapTask1.getAttempts().size() != 4 && timeOut++ < 10) {
      Thread.sleep(2000);
      LOG.info("Waiting for next attempt to start");
    }
    Assert.assertEquals(4, mapTask1.getAttempts().size());
    itr = mapTask1.getAttempts().values().iterator();
    itr.next();
    itr.next();
    itr.next();
    TaskAttempt task1Attempt4 = itr.next();
    
    app.waitForState(task1Attempt4, TaskAttemptState.RUNNING);

    //send the done signal to the 1st map 4th attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt4.getID(),
            TaskAttemptEventType.TA_DONE));

    /////////// End of games with the TaskAttempts of the first task //////

    //wait for first map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    long task1StartTime = mapTask1.getReport().getStartTime();
    long task1FinishTime = mapTask1.getReport().getFinishTime();
    
    //stop the app
    app.stop();

    //rerun
    //in rerun the 1st map will be recovered from previous run
    long am2StartTimeEst = System.currentTimeMillis();
    app = new MRAppWithHistory(2, 1, false, this.getClass().getName(), false, ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    mapTask2 = it.next();
    reduceTask = it.next();
    
    // first map will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    task2Attempt = mapTask2.getAttempts().values().iterator().next();
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);
    
    //send the done signal to the 2nd map task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapTask2.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait to get it completed
    app.waitForState(mapTask2, TaskState.SUCCEEDED);
    
    //wait for reduce to be running before sending done
    app.waitForState(reduceTask, TaskState.RUNNING);
    //send the done signal to the reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduceTask.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    Assert.assertEquals("Job Start time not correct",
        jobStartTime, job.getReport().getStartTime());
    Assert.assertEquals("Task Start time not correct",
        task1StartTime, mapTask1.getReport().getStartTime());
    Assert.assertEquals("Task Finish time not correct",
        task1FinishTime, mapTask1.getReport().getFinishTime());
    Assert.assertEquals(2, job.getAMInfos().size());
    int attemptNum = 1;
    // Verify AMInfo
    for (AMInfo amInfo : job.getAMInfos()) {
      Assert.assertEquals(attemptNum++, amInfo.getAppAttemptId()
          .getAttemptId());
      Assert.assertEquals(amInfo.getAppAttemptId(), amInfo.getContainerId()
          .getApplicationAttemptId());
      Assert.assertEquals(MRApp.NM_HOST, amInfo.getNodeManagerHost());
      Assert.assertEquals(MRApp.NM_PORT, amInfo.getNodeManagerPort());
      Assert.assertEquals(MRApp.NM_HTTP_PORT, amInfo.getNodeManagerHttpPort());
    }
    long am1StartTimeReal = job.getAMInfos().get(0).getStartTime();
    long am2StartTimeReal = job.getAMInfos().get(1).getStartTime();
    Assert.assertTrue(am1StartTimeReal >= am1StartTimeEst
        && am1StartTimeReal <= am2StartTimeEst);
    Assert.assertTrue(am2StartTimeReal >= am2StartTimeEst
        && am2StartTimeReal <= System.currentTimeMillis());
    // TODO Add verification of additional data from jobHistory - whatever was
    // available in the failed attempt should be available here
  }

  @Test
  public void testMultipleCrashes() throws Exception {

    int runCount = 0;
    MRApp app =
        new MRAppWithHistory(2, 1, false, this.getClass().getName(), true,
          ++runCount);
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task mapTask2 = it.next();
    Task reduceTask = it.next();
    
    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    TaskAttempt task1Attempt1 = mapTask1.getAttempts().values().iterator().next();
    TaskAttempt task2Attempt = mapTask2.getAttempts().values().iterator().next();
    
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt1, TaskAttemptState.RUNNING);
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);
    
    // reduces must be in NEW state
    Assert.assertEquals("Reduce Task state not correct",
        TaskState.RUNNING, reduceTask.getReport().getTaskState());

    //send the done signal to the 1st map
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
          task1Attempt1.getID(),
          TaskAttemptEventType.TA_DONE));

    //wait for first map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    
    // Crash the app
    app.stop();

    //rerun
    //in rerun the 1st map will be recovered from previous run
    app =
        new MRAppWithHistory(2, 1, false, this.getClass().getName(), false,
          ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    mapTask2 = it.next();
    reduceTask = it.next();
    
    // first map will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    task2Attempt = mapTask2.getAttempts().values().iterator().next();
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);
    
    //send the done signal to the 2nd map task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapTask2.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait to get it completed
    app.waitForState(mapTask2, TaskState.SUCCEEDED);

    // Crash the app again.
    app.stop();

    //rerun
    //in rerun the 1st and 2nd map will be recovered from previous run
    app =
        new MRAppWithHistory(2, 1, false, this.getClass().getName(), false,
          ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    mapTask2 = it.next();
    reduceTask = it.next();
    
    // The maps will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    app.waitForState(mapTask2, TaskState.SUCCEEDED);

    //wait for reduce to be running before sending done
    app.waitForState(reduceTask, TaskState.RUNNING);
    //send the done signal to the reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduceTask.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
  }

  @Test
  public void testOutputRecovery() throws Exception {
    int runCount = 0;
    MRApp app = new MRAppWithHistory(1, 2, false, this.getClass().getName(),
        true, ++runCount);
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task reduceTask1 = it.next();
    
    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    
    TaskAttempt task1Attempt1 = mapTask1.getAttempts().values().iterator()
        .next();
    
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt1, TaskAttemptState.RUNNING);
  
    //send the done signal to the map
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt1.getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait for map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    // Verify the shuffle-port
    Assert.assertEquals(5467, task1Attempt1.getShufflePort());
    
    app.waitForState(reduceTask1, TaskState.RUNNING);
    TaskAttempt reduce1Attempt1 = reduceTask1.getAttempts().values().iterator().next();
    
    // write output corresponding to reduce1
    writeOutput(reduce1Attempt1, conf);
    
    //send the done signal to the 1st reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduce1Attempt1.getID(),
            TaskAttemptEventType.TA_DONE));

    //wait for first reduce task to complete
    app.waitForState(reduceTask1, TaskState.SUCCEEDED);
    
    //stop the app before the job completes.
    app.stop();

    //rerun
    //in rerun the map will be recovered from previous run
    app = new MRAppWithHistory(1, 2, false, this.getClass().getName(), false,
        ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    reduceTask1 = it.next();
    Task reduceTask2 = it.next();
    
    // map will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    // Verify the shuffle-port after recovery
    task1Attempt1 = mapTask1.getAttempts().values().iterator().next();
    Assert.assertEquals(5467, task1Attempt1.getShufflePort());
    
    // first reduce will be recovered, no need to send done
    app.waitForState(reduceTask1, TaskState.SUCCEEDED); 
    
    app.waitForState(reduceTask2, TaskState.RUNNING);
    
    TaskAttempt reduce2Attempt = reduceTask2.getAttempts().values()
        .iterator().next();
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(reduce2Attempt, TaskAttemptState.RUNNING);
    
   //send the done signal to the 2nd reduce task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduce2Attempt.getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait to get it completed
    app.waitForState(reduceTask2, TaskState.SUCCEEDED);
    
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    validateOutput();
  }

  @Test
  public void testOutputRecoveryMapsOnly() throws Exception {
    int runCount = 0;
    MRApp app = new MRAppWithHistory(2, 1, false, this.getClass().getName(),
        true, ++runCount);
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task mapTask2 = it.next();
    Task reduceTask1 = it.next();
    
    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    
    TaskAttempt task1Attempt1 = mapTask1.getAttempts().values().iterator()
        .next();
    
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt1, TaskAttemptState.RUNNING);
  
    // write output corresponding to map1 (This is just to validate that it is
    //no included in the output)
    writeBadOutput(task1Attempt1, conf);
    
    //send the done signal to the map
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt1.getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait for map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    // Verify the shuffle-port
    Assert.assertEquals(5467, task1Attempt1.getShufflePort());

    //stop the app before the job completes.
    app.stop();
    
    //rerun
    //in rerun the map will be recovered from previous run
    app = new MRAppWithHistory(2, 1, false, this.getClass().getName(), false,
        ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    mapTask2 = it.next();
    reduceTask1 = it.next();
    
    // map will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    // Verify the shuffle-port after recovery
    task1Attempt1 = mapTask1.getAttempts().values().iterator().next();
    Assert.assertEquals(5467, task1Attempt1.getShufflePort());
    
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    TaskAttempt task2Attempt1 = mapTask2.getAttempts().values().iterator()
    .next();

    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task2Attempt1, TaskAttemptState.RUNNING);

    //send the done signal to the map
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task2Attempt1.getID(),
            TaskAttemptEventType.TA_DONE));

    //wait for map task to complete
    app.waitForState(mapTask2, TaskState.SUCCEEDED);

    // Verify the shuffle-port
    Assert.assertEquals(5467, task2Attempt1.getShufflePort());
    
    app.waitForState(reduceTask1, TaskState.RUNNING);
    TaskAttempt reduce1Attempt1 = reduceTask1.getAttempts().values().iterator().next();
    
    // write output corresponding to reduce1
    writeOutput(reduce1Attempt1, conf);
    
    //send the done signal to the 1st reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduce1Attempt1.getID(),
            TaskAttemptEventType.TA_DONE));

    //wait for first reduce task to complete
    app.waitForState(reduceTask1, TaskState.SUCCEEDED);
    
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    validateOutput();
  }
  
  @Test
  public void testRecoveryWithOldCommiter() throws Exception {
    int runCount = 0;
    MRApp app = new MRAppWithHistory(1, 2, false, this.getClass().getName(),
        true, ++runCount);
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", false);
    conf.setBoolean("mapred.reducer.new-api", false);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task reduceTask1 = it.next();
    
    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    
    TaskAttempt task1Attempt1 = mapTask1.getAttempts().values().iterator()
        .next();
    
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt1, TaskAttemptState.RUNNING);
  
    //send the done signal to the map
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt1.getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait for map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    // Verify the shuffle-port
    Assert.assertEquals(5467, task1Attempt1.getShufflePort());
    
    app.waitForState(reduceTask1, TaskState.RUNNING);
    TaskAttempt reduce1Attempt1 = reduceTask1.getAttempts().values().iterator().next();
    
    // write output corresponding to reduce1
    writeOutput(reduce1Attempt1, conf);
    
    //send the done signal to the 1st reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduce1Attempt1.getID(),
            TaskAttemptEventType.TA_DONE));

    //wait for first reduce task to complete
    app.waitForState(reduceTask1, TaskState.SUCCEEDED);
    
    //stop the app before the job completes.
    app.stop();

    //rerun
    //in rerun the map will be recovered from previous run
    app = new MRAppWithHistory(1, 2, false, this.getClass().getName(), false,
        ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean("mapred.mapper.new-api", false);
    conf.setBoolean("mapred.reducer.new-api", false);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    reduceTask1 = it.next();
    Task reduceTask2 = it.next();
    
    // map will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    // Verify the shuffle-port after recovery
    task1Attempt1 = mapTask1.getAttempts().values().iterator().next();
    Assert.assertEquals(5467, task1Attempt1.getShufflePort());
    
    // first reduce will be recovered, no need to send done
    app.waitForState(reduceTask1, TaskState.SUCCEEDED); 
    
    app.waitForState(reduceTask2, TaskState.RUNNING);
    
    TaskAttempt reduce2Attempt = reduceTask2.getAttempts().values()
        .iterator().next();
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(reduce2Attempt, TaskAttemptState.RUNNING);
    
   //send the done signal to the 2nd reduce task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduce2Attempt.getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait to get it completed
    app.waitForState(reduceTask2, TaskState.SUCCEEDED);
    
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    validateOutput();
  }

  /**
   * AM with 2 maps and 1 reduce. For 1st map, one attempt fails, one attempt
   * completely disappears because of failed launch, one attempt gets killed and
   * one attempt succeeds. AM crashes after the first tasks finishes and
   * recovers completely and succeeds in the second generation.
   * 
   * @throws Exception
   */
  @Test
  public void testSpeculative() throws Exception {

    int runCount = 0;
    long am1StartTimeEst = System.currentTimeMillis();
    MRApp app = new MRAppWithHistory(2, 1, false, this.getClass().getName(), true, ++runCount);
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    long jobStartTime = job.getReport().getStartTime();
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());

    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task mapTask2 = it.next();
    Task reduceTask = it.next();

    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    app.waitForState(mapTask2, TaskState.RUNNING);

    // Launch a Speculative Task for the first Task
    app.getContext().getEventHandler().handle(
        new TaskEvent(mapTask1.getID(), TaskEventType.T_ADD_SPEC_ATTEMPT));
    int timeOut = 0;
    while (mapTask1.getAttempts().size() != 2 && timeOut++ < 10) {
      Thread.sleep(1000);
      LOG.info("Waiting for next attempt to start");
    }
    Iterator<TaskAttempt> t1it = mapTask1.getAttempts().values().iterator();
    TaskAttempt task1Attempt1 = t1it.next();
    TaskAttempt task1Attempt2 = t1it.next();
    TaskAttempt task2Attempt = mapTask2.getAttempts().values().iterator().next();

    ContainerId t1a2contId = task1Attempt2.getAssignedContainerID();

    LOG.info(t1a2contId.toString());
    LOG.info(task1Attempt1.getID().toString());
    LOG.info(task1Attempt2.getID().toString());

    // Launch container for speculative attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptContainerLaunchedEvent(task1Attempt2.getID(), runCount));

    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt1, TaskAttemptState.RUNNING);
    app.waitForState(task1Attempt2, TaskAttemptState.RUNNING);
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);

    // reduces must be in NEW state
    Assert.assertEquals("Reduce Task state not correct",
        TaskState.RUNNING, reduceTask.getReport().getTaskState());

    //send the done signal to the map 1 attempt 1
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt1.getID(),
            TaskAttemptEventType.TA_DONE));

    app.waitForState(task1Attempt1, TaskAttemptState.SUCCEEDED);

    //wait for first map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    long task1StartTime = mapTask1.getReport().getStartTime();
    long task1FinishTime = mapTask1.getReport().getFinishTime();

    //stop the app
    app.stop();

    //rerun
    //in rerun the 1st map will be recovered from previous run
    long am2StartTimeEst = System.currentTimeMillis();
    app = new MRAppWithHistory(2, 1, false, this.getClass().getName(), false, ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean("mapred.reducer.new-api", true);
    conf.set(FileOutputFormat.OUTDIR, outputDir.toString());
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    mapTask2 = it.next();
    reduceTask = it.next();

    // first map will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);

    app.waitForState(mapTask2, TaskState.RUNNING);

    task2Attempt = mapTask2.getAttempts().values().iterator().next();
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);

    //send the done signal to the 2nd map task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapTask2.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));

    //wait to get it completed
    app.waitForState(mapTask2, TaskState.SUCCEEDED);

    //wait for reduce to be running before sending done
    app.waitForState(reduceTask, TaskState.RUNNING);

    //send the done signal to the reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduceTask.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));

    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    Assert.assertEquals("Job Start time not correct",
        jobStartTime, job.getReport().getStartTime());
    Assert.assertEquals("Task Start time not correct",
        task1StartTime, mapTask1.getReport().getStartTime());
    Assert.assertEquals("Task Finish time not correct",
        task1FinishTime, mapTask1.getReport().getFinishTime());
    Assert.assertEquals(2, job.getAMInfos().size());
    int attemptNum = 1;
    // Verify AMInfo
    for (AMInfo amInfo : job.getAMInfos()) {
      Assert.assertEquals(attemptNum++, amInfo.getAppAttemptId()
          .getAttemptId());
      Assert.assertEquals(amInfo.getAppAttemptId(), amInfo.getContainerId()
          .getApplicationAttemptId());
      Assert.assertEquals(MRApp.NM_HOST, amInfo.getNodeManagerHost());
      Assert.assertEquals(MRApp.NM_PORT, amInfo.getNodeManagerPort());
      Assert.assertEquals(MRApp.NM_HTTP_PORT, amInfo.getNodeManagerHttpPort());
    }
    long am1StartTimeReal = job.getAMInfos().get(0).getStartTime();
    long am2StartTimeReal = job.getAMInfos().get(1).getStartTime();
    Assert.assertTrue(am1StartTimeReal >= am1StartTimeEst
        && am1StartTimeReal <= am2StartTimeEst);
    Assert.assertTrue(am2StartTimeReal >= am2StartTimeEst
        && am2StartTimeReal <= System.currentTimeMillis());

  }

  private void writeBadOutput(TaskAttempt attempt, Configuration conf)
  throws Exception {
  TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, 
      TypeConverter.fromYarn(attempt.getID()));
 
  TextOutputFormat<?, ?> theOutputFormat = new TextOutputFormat();
  RecordWriter theRecordWriter = theOutputFormat
      .getRecordWriter(tContext);
  
  NullWritable nullWritable = NullWritable.get();
  try {
    theRecordWriter.write(key2, val2);
    theRecordWriter.write(null, nullWritable);
    theRecordWriter.write(null, val2);
    theRecordWriter.write(nullWritable, val1);
    theRecordWriter.write(key1, nullWritable);
    theRecordWriter.write(key2, null);
    theRecordWriter.write(null, null);
    theRecordWriter.write(key1, val1);
  } finally {
    theRecordWriter.close(tContext);
  }
  
  OutputFormat outputFormat = ReflectionUtils.newInstance(
      tContext.getOutputFormatClass(), conf);
  OutputCommitter committer = outputFormat.getOutputCommitter(tContext);
  committer.commitTask(tContext);
}
  
  
  private void writeOutput(TaskAttempt attempt, Configuration conf)
    throws Exception {
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, 
        TypeConverter.fromYarn(attempt.getID()));
    
    TextOutputFormat<?, ?> theOutputFormat = new TextOutputFormat();
    RecordWriter theRecordWriter = theOutputFormat
        .getRecordWriter(tContext);
    
    NullWritable nullWritable = NullWritable.get();
    try {
      theRecordWriter.write(key1, val1);
      theRecordWriter.write(null, nullWritable);
      theRecordWriter.write(null, val1);
      theRecordWriter.write(nullWritable, val2);
      theRecordWriter.write(key2, nullWritable);
      theRecordWriter.write(key1, null);
      theRecordWriter.write(null, null);
      theRecordWriter.write(key2, val2);
    } finally {
      theRecordWriter.close(tContext);
    }
    
    OutputFormat outputFormat = ReflectionUtils.newInstance(
        tContext.getOutputFormatClass(), conf);
    OutputCommitter committer = outputFormat.getOutputCommitter(tContext);
    committer.commitTask(tContext);
  }

  private void validateOutput() throws IOException {
    File expectedFile = new File(new Path(outputDir, partFile).toString());
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(key1).append('\t').append(val1).append("\n");
    expectedOutput.append(val1).append("\n");
    expectedOutput.append(val2).append("\n");
    expectedOutput.append(key2).append("\n");
    expectedOutput.append(key1).append("\n");
    expectedOutput.append(key2).append('\t').append(val2).append("\n");
    String output = slurp(expectedFile);
    Assert.assertEquals(output, expectedOutput.toString());
  }

  public static String slurp(File f) throws IOException {
    int len = (int) f.length();
    byte[] buf = new byte[len];
    FileInputStream in = new FileInputStream(f);
    String contents = null;
    try {
      in.read(buf, 0, len);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }


  static class MRAppWithHistory extends MRApp {
    public MRAppWithHistory(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart, int startCount) {
      super(maps, reduces, autoComplete, testName, cleanOnStart, startCount);
    }

    @Override
    protected ContainerLauncher createContainerLauncher(AppContext context) {
      MockContainerLauncher launcher = new MockContainerLauncher() {
        @Override
        public void handle(ContainerLauncherEvent event) {
          TaskAttemptId taskAttemptID = event.getTaskAttemptID();
          // Pass everything except the 2nd attempt of the first task.
          if (taskAttemptID.getId() != 1
              || taskAttemptID.getTaskId().getId() != 0) {
            super.handle(event);
          }
        }
      };
      launcher.shufflePort = 5467;
      return launcher;
    }

    @Override
    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context, 
          getStartCount());
      return eventHandler;
    }
  }

  public static void main(String[] arg) throws Exception {
    TestRecovery test = new TestRecovery();
    test.testCrashed();
  }
}
