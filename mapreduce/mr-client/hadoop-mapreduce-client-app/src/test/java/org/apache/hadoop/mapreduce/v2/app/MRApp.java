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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.MRConstants;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleaner;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleanupEvent;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHConfig;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;


/**
 * Mock MRAppMaster. Doesn't start RPC servers.
 * No threads are started except of the event Dispatcher thread.
 */
public class MRApp extends MRAppMaster {
  private static final Log LOG = LogFactory.getLog(MRApp.class);

  int maps;
  int reduces;

  private File testWorkDir;
  private Path testAbsPath;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  //if true, tasks complete automatically as soon as they are launched
  protected boolean autoComplete = false;

  static ApplicationId applicationId;

  static {
    applicationId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class);
    applicationId.setClusterTimestamp(0);
    applicationId.setId(0);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName, boolean cleanOnStart) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, 1);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName, boolean cleanOnStart, int startCount) {
    super(applicationId, startCount);
    this.testWorkDir = new File("target", testName);
    testAbsPath = new Path(testWorkDir.getAbsolutePath());
    LOG.info("PathUsed: " + testAbsPath);
    if (cleanOnStart) {
      testAbsPath = new Path(testWorkDir.getAbsolutePath());
      try {
        FileContext.getLocalFSFileContext().delete(testAbsPath, true);
      } catch (Exception e) {
        LOG.warn("COULD NOT CLEANUP: " + testAbsPath, e);
        throw new YarnException("could not cleanup test dir", e);
      }
    }
    
    this.maps = maps;
    this.reduces = reduces;
    this.autoComplete = autoComplete;
  }

  public Job submit(Configuration conf) throws Exception {
    String user = conf.get(MRJobConfig.USER_NAME, "mapred");
    conf.set(MRJobConfig.USER_NAME, user);
    conf.set(MRConstants.APPS_STAGING_DIR_KEY, testAbsPath.toString());
    conf.setBoolean(JHConfig.CREATE_HISTORY_INTERMEDIATE_BASE_DIR_KEY, true);
    //TODO: fix the bug where the speculator gets events with 
    //not-fully-constructed objects. For now, disable speculative exec
    LOG.info("****DISABLING SPECULATIVE EXECUTION*****");
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

    init(conf);
    start();
    DefaultMetricsSystem.shutdown();
    Job job = getContext().getAllJobs().values().iterator().next();
    return job;
  }

  public void waitForState(TaskAttempt attempt, 
      TaskAttemptState finalState) throws Exception {
    int timeoutSecs = 0;
    TaskAttemptReport report = attempt.getReport();
    while (!finalState.equals(report.getTaskAttemptState()) &&
        timeoutSecs++ < 20) {
      System.out.println("TaskAttempt State is : " + report.getTaskAttemptState() +
          " Waiting for state : " + finalState +
          "   progress : " + report.getProgress());
      report = attempt.getReport();
      Thread.sleep(500);
    }
    System.out.println("TaskAttempt State is : " + report.getTaskAttemptState());
    Assert.assertEquals("TaskAttempt state is not correct (timedout)",
        finalState, 
        report.getTaskAttemptState());
  }

  public void waitForState(Task task, TaskState finalState) throws Exception {
    int timeoutSecs = 0;
    TaskReport report = task.getReport();
    while (!finalState.equals(report.getTaskState()) &&
        timeoutSecs++ < 20) {
      System.out.println("Task State is : " + report.getTaskState() +
          " Waiting for state : " + finalState +
          "   progress : " + report.getProgress());
      report = task.getReport();
      Thread.sleep(500);
    }
    System.out.println("Task State is : " + report.getTaskState());
    Assert.assertEquals("Task state is not correct (timedout)", finalState, 
        report.getTaskState());
  }

  public void waitForState(Job job, JobState finalState) throws Exception {
    int timeoutSecs = 0;
    JobReport report = job.getReport();
    while (!finalState.equals(report.getJobState()) &&
        timeoutSecs++ < 20) {
      System.out.println("Job State is : " + report.getJobState() +
          " Waiting for state : " + finalState +
          "   map progress : " + report.getMapProgress() + 
          "   reduce progress : " + report.getReduceProgress());
      report = job.getReport();
      Thread.sleep(500);
    }
    System.out.println("Job State is : " + report.getJobState());
    Assert.assertEquals("Job state is not correct (timedout)", finalState, 
        job.getState());
  }

  public void waitForState(Service.STATE finalState) throws Exception {
    int timeoutSecs = 0;
    while (!finalState.equals(getServiceState()) && timeoutSecs++ < 20) {
      System.out.println("MRApp State is : " + getServiceState()
          + " Waiting for state : " + finalState);
      Thread.sleep(500);
    }
    System.out.println("MRApp State is : " + getServiceState());
    Assert.assertEquals("MRApp state is not correct (timedout)", finalState,
        getServiceState());
  }

  public void verifyCompleted() {
    for (Job job : getContext().getAllJobs().values()) {
      JobReport jobReport = job.getReport();
      System.out.println("Job start time :" + jobReport.getStartTime());
      System.out.println("Job finish time :" + jobReport.getFinishTime());
      Assert.assertTrue("Job start time is not less than finish time",
          jobReport.getStartTime() <= jobReport.getFinishTime());
      Assert.assertTrue("Job finish time is in future",
          jobReport.getFinishTime() <= System.currentTimeMillis());
      for (Task task : job.getTasks().values()) {
        TaskReport taskReport = task.getReport();
        System.out.println("Task start time : " + taskReport.getStartTime());
        System.out.println("Task finish time : " + taskReport.getFinishTime());
        Assert.assertTrue("Task start time is not less than finish time",
            taskReport.getStartTime() <= taskReport.getFinishTime());
        for (TaskAttempt attempt : task.getAttempts().values()) {
          TaskAttemptReport attemptReport = attempt.getReport();
          Assert.assertTrue("Attempt start time is not less than finish time",
              attemptReport.getStartTime() <= attemptReport.getFinishTime());
        }
      }
    }
  }

  @Override
  protected Job createJob(Configuration conf, Credentials fsTokens) {
    Job newJob = new TestJob(getAppID(), getDispatcher().getEventHandler(),
                             getTaskAttemptListener(), getContext().getClock());
    ((AppContext) getContext()).getAllJobs().put(newJob.getID(), newJob);

    getDispatcher().register(JobFinishEvent.Type.class,
        new EventHandler<JobFinishEvent>() {
          @Override
          public void handle(JobFinishEvent event) {
            stop();
          }
        });

    return newJob;
  }

  @Override
  protected TaskAttemptListener createTaskAttemptListener(AppContext context) {
    return new TaskAttemptListener(){
      @Override
      public InetSocketAddress getAddress() {
        return null;
      }
      @Override
      public void register(TaskAttemptId attemptID, 
          org.apache.hadoop.mapred.Task task, WrappedJvmID jvmID) {}
      @Override
      public void unregister(TaskAttemptId attemptID, WrappedJvmID jvmID) {
      }
    };
  }

  @Override
  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
      AppContext context) {//disable history
    return new EventHandler<JobHistoryEvent>() {
      @Override
      public void handle(JobHistoryEvent event) {
      }
    };
  }
  
  @Override
  protected ContainerLauncher createContainerLauncher(AppContext context,
                                                      boolean isLocal) {
    return new MockContainerLauncher();
  }

  class MockContainerLauncher implements ContainerLauncher {
    @Override
    public void handle(ContainerLauncherEvent event) {
      switch (event.getType()) {
      case CONTAINER_REMOTE_LAUNCH:
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(event.getTaskAttemptID(),
                TaskAttemptEventType.TA_CONTAINER_LAUNCHED));
        
        attemptLaunched(event.getTaskAttemptID());
        break;
      case CONTAINER_REMOTE_CLEANUP:
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(event.getTaskAttemptID(),
                TaskAttemptEventType.TA_CONTAINER_CLEANED));
        break;
      }
    }
  }

  protected void attemptLaunched(TaskAttemptId attemptID) {
    if (autoComplete) {
      // send the done event
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(attemptID,
              TaskAttemptEventType.TA_DONE));
    }
  }

  @Override
  protected ContainerAllocator createContainerAllocator(
      ClientService clientService, AppContext context, boolean isLocal) {
    return new ContainerAllocator(){
      private int containerCount;
      @Override
      public void handle(ContainerAllocatorEvent event) {
        ContainerId cId = recordFactory.newRecordInstance(ContainerId.class);
        cId.setAppId(getContext().getApplicationID());
        cId.setId(containerCount++);
        Container container = recordFactory.newRecordInstance(Container.class);
        container.setId(cId);
        container.setNodeId(recordFactory.newRecordInstance(NodeId.class));
        container.setContainerManagerAddress("dummy");
        container.setContainerToken(null);
        container.setNodeHttpAddress("localhost:9999");
        getContext().getEventHandler().handle(
            new TaskAttemptContainerAssignedEvent(event.getAttemptID(),
                container));
      }
    };
  }

  @Override
  protected TaskCleaner createTaskCleaner(AppContext context) {
    return new TaskCleaner() {
      @Override
      public void handle(TaskCleanupEvent event) {
        //send the cleanup done event
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(event.getAttemptID(),
                TaskAttemptEventType.TA_CLEANUP_DONE));
      }
    };
  }

  @Override
  protected ClientService createClientService(AppContext context) {
    return new ClientService(){
      @Override
      public InetSocketAddress getBindAddress() {
        return null;
      }

      @Override
      public int getHttpPort() {
        return -1;
      }
    };
  }

  class TestJob extends JobImpl {
    //override the init transition
    StateMachineFactory<JobImpl, JobState, JobEventType, JobEvent> localFactory
        = stateMachineFactory.addTransition(JobState.NEW,
            EnumSet.of(JobState.INITED, JobState.FAILED),
            JobEventType.JOB_INIT,
            // This is abusive.
            new TestInitTransition(getConfig(), maps, reduces));

    private final StateMachine<JobState, JobEventType, JobEvent>
        localStateMachine;

    @Override
    protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
      return localStateMachine;
    }

    public TestJob(ApplicationId appID, EventHandler eventHandler,
        TaskAttemptListener taskAttemptListener, Clock clock) {
      super(appID, new Configuration(), eventHandler, taskAttemptListener,
          new JobTokenSecretManager(), new Credentials(), clock, getStartCount(), 
          getCompletedTaskFromPreviousRun(), metrics);

      // This "this leak" is okay because the retained pointer is in an
      //  instance variable.
      localStateMachine = localFactory.make(this);
    }
  }

  //Override InitTransition to not look for split files etc
  static class TestInitTransition extends JobImpl.InitTransition {
    private Configuration config;
    private int maps;
    private int reduces;
    TestInitTransition(Configuration config, int maps, int reduces) {
      this.config = config;
      this.maps = maps;
      this.reduces = reduces;
    }
    @Override
    protected void setup(JobImpl job) throws IOException {
      job.conf = config;
      job.conf.setInt(MRJobConfig.NUM_REDUCES, reduces);
      job.remoteJobConfFile = new Path("test");
    }
    @Override
    protected TaskSplitMetaInfo[] createSplits(JobImpl job, JobId jobId) {
      TaskSplitMetaInfo[] splits = new TaskSplitMetaInfo[maps];
      for (int i = 0; i < maps ; i++) {
        splits[i] = new TaskSplitMetaInfo();
      }
      return splits;
    }
  }

}
 
