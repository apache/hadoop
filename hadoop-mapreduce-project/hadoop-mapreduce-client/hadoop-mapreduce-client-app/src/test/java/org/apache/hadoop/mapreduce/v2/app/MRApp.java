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
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventHandler;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.TaskStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;


/**
 * Mock MRAppMaster. Doesn't start RPC servers.
 * No threads are started except of the event Dispatcher thread.
 */
@SuppressWarnings("unchecked")
public class MRApp extends MRAppMaster {
  private static final Log LOG = LogFactory.getLog(MRApp.class);

  int maps;
  int reduces;

  private File testWorkDir;
  private Path testAbsPath;
  private ClusterInfo clusterInfo;
  
  // Queue to pretend the RM assigned us
  private String assignedQueue;

  public static String NM_HOST = "localhost";
  public static int NM_PORT = 1234;
  public static int NM_HTTP_PORT = 8042;

  //if true, tasks complete automatically as soon as they are launched
  protected boolean autoComplete = false;

  static ApplicationId applicationId;

  static {
    applicationId = ApplicationId.newInstance(0, 0);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, Clock clock) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, 1, clock, null);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, Clock clock, boolean unregistered) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, 1, clock,
        unregistered);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, 1);
  }
  
  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, String assignedQueue) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, 1,
        new SystemClock(), assignedQueue);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, boolean unregistered) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, 1, unregistered);
  }

  @Override
  protected void initJobCredentialsAndUGI(Configuration conf) {
    // Fake a shuffle secret that normally is provided by the job client.
    String shuffleSecret = "fake-shuffle-secret";
    TokenCache.setShuffleSecretKey(shuffleSecret.getBytes(), getCredentials());
  }

  private static ApplicationAttemptId getApplicationAttemptId(
      ApplicationId applicationId, int startCount) {
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, startCount);
    return applicationAttemptId;
  }
  
  private static ContainerId getContainerId(ApplicationId applicationId,
      int startCount) {
    ApplicationAttemptId appAttemptId =
        getApplicationAttemptId(applicationId, startCount);
    ContainerId containerId =
        ContainerId.newContainerId(appAttemptId, startCount);
    return containerId;
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, startCount,
        new SystemClock(), null);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount, boolean unregistered) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, startCount,
        new SystemClock(), unregistered);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount, Clock clock, boolean unregistered) {
    this(getApplicationAttemptId(applicationId, startCount), getContainerId(
      applicationId, startCount), maps, reduces, autoComplete, testName,
      cleanOnStart, startCount, clock, unregistered, null);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount, Clock clock, String assignedQueue) {
    this(getApplicationAttemptId(applicationId, startCount), getContainerId(
      applicationId, startCount), maps, reduces, autoComplete, testName,
      cleanOnStart, startCount, clock, true, assignedQueue);
  }

  public MRApp(ApplicationAttemptId appAttemptId, ContainerId amContainerId,
      int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount, boolean unregistered) {
    this(appAttemptId, amContainerId, maps, reduces, autoComplete, testName,
        cleanOnStart, startCount, new SystemClock(), unregistered, null);
  }

  public MRApp(ApplicationAttemptId appAttemptId, ContainerId amContainerId,
      int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount) {
    this(appAttemptId, amContainerId, maps, reduces, autoComplete, testName,
        cleanOnStart, startCount, new SystemClock(), true, null);
  }

  public MRApp(ApplicationAttemptId appAttemptId, ContainerId amContainerId,
      int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount, Clock clock, boolean unregistered,
      String assignedQueue) {
    super(appAttemptId, amContainerId, NM_HOST, NM_PORT, NM_HTTP_PORT, clock,
        System.currentTimeMillis());
    this.testWorkDir = new File("target", testName);
    testAbsPath = new Path(testWorkDir.getAbsolutePath());
    LOG.info("PathUsed: " + testAbsPath);
    if (cleanOnStart) {
      testAbsPath = new Path(testWorkDir.getAbsolutePath());
      try {
        FileContext.getLocalFSFileContext().delete(testAbsPath, true);
      } catch (Exception e) {
        LOG.warn("COULD NOT CLEANUP: " + testAbsPath, e);
        throw new YarnRuntimeException("could not cleanup test dir", e);
      }
    }

    this.maps = maps;
    this.reduces = reduces;
    this.autoComplete = autoComplete;
    // If safeToReportTerminationToUser is set to true, we can verify whether
    // the job can reaches the final state when MRAppMaster shuts down.
    this.successfullyUnregistered.set(unregistered);
    this.assignedQueue = assignedQueue;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    try {
      //Create the staging directory if it does not exist
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      Path stagingDir = MRApps.getStagingAreaDir(conf, user);
      FileSystem fs = getFileSystem(conf);
      fs.mkdirs(stagingDir);
    } catch (Exception e) {
      throw new YarnRuntimeException("Error creating staging dir", e);
    }
    
    super.serviceInit(conf);
    if (this.clusterInfo != null) {
      getContext().getClusterInfo().setMaxContainerCapability(
          this.clusterInfo.getMaxContainerCapability());
    } else {
      getContext().getClusterInfo().setMaxContainerCapability(
          Resource.newInstance(10240, 1));
    }
  }

  public Job submit(Configuration conf) throws Exception {
    //TODO: fix the bug where the speculator gets events with 
    //not-fully-constructed objects. For now, disable speculative exec
    return submit(conf, false, false);
  }

  public Job submit(Configuration conf, boolean mapSpeculative,
      boolean reduceSpeculative) throws Exception {
    String user = conf.get(MRJobConfig.USER_NAME, UserGroupInformation
        .getCurrentUser().getShortUserName());
    conf.set(MRJobConfig.USER_NAME, user);
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, testAbsPath.toString());
    conf.setBoolean(MRJobConfig.MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR, true);
    // TODO: fix the bug where the speculator gets events with
    // not-fully-constructed objects. For now, disable speculative exec
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, mapSpeculative);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, reduceSpeculative);

    init(conf);
    start();
    DefaultMetricsSystem.shutdown();
    Job job = getContext().getAllJobs().values().iterator().next();
    if (assignedQueue != null) {
      job.setQueueName(assignedQueue);
    }

    // Write job.xml
    String jobFile = MRApps.getJobFile(conf, user,
        TypeConverter.fromYarn(job.getID()));
    LOG.info("Writing job conf to " + jobFile);
    new File(jobFile).getParentFile().mkdirs();
    conf.writeXml(new FileOutputStream(jobFile));

    return job;
  }

  public void waitForInternalState(JobImpl job,
      JobStateInternal finalState) throws Exception {
    int timeoutSecs = 0;
    JobStateInternal iState = job.getInternalState();
    while (!finalState.equals(iState) && timeoutSecs++ < 20) {
      System.out.println("Job Internal State is : " + iState
          + " Waiting for Internal state : " + finalState);
      Thread.sleep(500);
      iState = job.getInternalState();
    }
    System.out.println("Task Internal State is : " + iState);
    Assert.assertEquals("Task Internal state is not correct (timedout)",
        finalState, iState);
  }

  public void waitForInternalState(TaskImpl task,
      TaskStateInternal finalState) throws Exception {
    int timeoutSecs = 0;
    TaskReport report = task.getReport();
    TaskStateInternal iState = task.getInternalState();
    while (!finalState.equals(iState) && timeoutSecs++ < 20) {
      System.out.println("Task Internal State is : " + iState
          + " Waiting for Internal state : " + finalState + "   progress : "
          + report.getProgress());
      Thread.sleep(500);
      report = task.getReport();
      iState = task.getInternalState();
    }
    System.out.println("Task Internal State is : " + iState);
    Assert.assertEquals("Task Internal state is not correct (timedout)",
        finalState, iState);
  }

  public void waitForInternalState(TaskAttemptImpl attempt,
      TaskAttemptStateInternal finalState) throws Exception {
    int timeoutSecs = 0;
    TaskAttemptReport report = attempt.getReport();
    TaskAttemptStateInternal iState = attempt.getInternalState();
    while (!finalState.equals(iState) && timeoutSecs++ < 20) {
      System.out.println("TaskAttempt Internal State is : " + iState
          + " Waiting for Internal state : " + finalState + "   progress : "
          + report.getProgress());
      Thread.sleep(500);
      report = attempt.getReport();
      iState = attempt.getInternalState();
    }
    System.out.println("TaskAttempt Internal State is : " + iState);
    Assert.assertEquals("TaskAttempt Internal state is not correct (timedout)",
        finalState, iState);
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
      System.out.println("Task State for " + task.getID() + " is : "
          + report.getTaskState() + " Waiting for state : " + finalState
          + "   progress : " + report.getProgress());
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
    if (finalState == Service.STATE.STOPPED) {
       Assert.assertTrue("Timeout while waiting for MRApp to stop",
           waitForServiceToStop(20 * 1000));
    } else {
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
  protected Job createJob(Configuration conf, JobStateInternal forcedState, 
      String diagnostic) {
    UserGroupInformation currentUser = null;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    Job newJob = new TestJob(getJobId(), getAttemptID(), conf, 
    		getDispatcher().getEventHandler(),
            getTaskAttemptListener(), getContext().getClock(),
            getCommitter(), isNewApiCommitter(),
            currentUser.getUserName(), getContext(),
            forcedState, diagnostic);
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
        return NetUtils.createSocketAddr("localhost:54321");
      }
      @Override
      public void registerLaunchedTask(TaskAttemptId attemptID,
          WrappedJvmID jvmID) {
      }
      @Override
      public void unregister(TaskAttemptId attemptID, WrappedJvmID jvmID) {
      }
      @Override
      public void registerPendingTask(org.apache.hadoop.mapred.Task task,
          WrappedJvmID jvmID) {
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
  protected ContainerLauncher createContainerLauncher(AppContext context) {
    return new MockContainerLauncher();
  }

  protected class MockContainerLauncher implements ContainerLauncher {

    //We are running locally so set the shuffle port to -1 
    int shufflePort = -1;

    public MockContainerLauncher() {
    }

    @Override
    public void handle(ContainerLauncherEvent event) {
      switch (event.getType()) {
      case CONTAINER_REMOTE_LAUNCH:
        containerLaunched(event.getTaskAttemptID(), shufflePort);
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

  protected void containerLaunched(TaskAttemptId attemptID, int shufflePort) {
    getContext().getEventHandler().handle(
      new TaskAttemptContainerLaunchedEvent(attemptID,
          shufflePort));
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
      ClientService clientService, final AppContext context) {
    return new MRAppContainerAllocator();
  }

  protected class MRAppContainerAllocator
      implements ContainerAllocator, RMHeartbeatHandler {
    private int containerCount;

     @Override
      public void handle(ContainerAllocatorEvent event) {
        ContainerId cId =
            ContainerId.newContainerId(getContext().getApplicationAttemptId(),
              containerCount++);
        NodeId nodeId = NodeId.newInstance(NM_HOST, NM_PORT);
        Resource resource = Resource.newInstance(1234, 2);
        ContainerTokenIdentifier containerTokenIdentifier =
            new ContainerTokenIdentifier(cId, nodeId.toString(), "user",
            resource, System.currentTimeMillis() + 10000, 42, 42,
            Priority.newInstance(0), 0);
        Token containerToken = newContainerToken(nodeId, "password".getBytes(),
              containerTokenIdentifier);
        Container container = Container.newInstance(cId, nodeId,
            NM_HOST + ":" + NM_HTTP_PORT, resource, null, containerToken);
        JobID id = TypeConverter.fromYarn(applicationId);
        JobId jobId = TypeConverter.toYarn(id);
        getContext().getEventHandler().handle(new JobHistoryEvent(jobId, 
            new NormalizedResourceEvent(
                org.apache.hadoop.mapreduce.TaskType.REDUCE,
            100)));
        getContext().getEventHandler().handle(new JobHistoryEvent(jobId, 
            new NormalizedResourceEvent(
                org.apache.hadoop.mapreduce.TaskType.MAP,
            100)));
        getContext().getEventHandler().handle(
            new TaskAttemptContainerAssignedEvent(event.getAttemptID(),
                container, null));
      }

    @Override
    public long getLastHeartbeatTime() {
      return getContext().getClock().getTime();
    }

    @Override
    public void runOnNextHeartbeat(Runnable callback) {
      callback.run();
    }
  }

  @Override
  protected EventHandler<CommitterEvent> createCommitterEventHandler(
      AppContext context, final OutputCommitter committer) {
    // create an output committer with the task methods stubbed out
    OutputCommitter stubbedCommitter = new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) throws IOException {
        committer.setupJob(jobContext);
      }
      @SuppressWarnings("deprecation")
      @Override
      public void cleanupJob(JobContext jobContext) throws IOException {
        committer.cleanupJob(jobContext);
      }
      @Override
      public void commitJob(JobContext jobContext) throws IOException {
        committer.commitJob(jobContext);
      }
      @Override
      public void abortJob(JobContext jobContext, State state)
          throws IOException {
        committer.abortJob(jobContext, state);
      }

      @Override
      public boolean isRecoverySupported(JobContext jobContext) throws IOException{
        return committer.isRecoverySupported(jobContext);
      }

      @SuppressWarnings("deprecation")
      @Override
      public boolean isRecoverySupported() {
        return committer.isRecoverySupported();
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext)
          throws IOException {
      }
      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext)
          throws IOException {
        return false;
      }
      @Override
      public void commitTask(TaskAttemptContext taskContext)
          throws IOException {
      }
      @Override
      public void abortTask(TaskAttemptContext taskContext)
          throws IOException {
      }
      @Override
      public void recoverTask(TaskAttemptContext taskContext)
          throws IOException {
      }
    };

    return new CommitterEventHandler(context, stubbedCommitter,
        getRMHeartbeatHandler());
  }

  @Override
  protected ClientService createClientService(AppContext context) {
    return new MRClientService(context) {
      @Override
      public InetSocketAddress getBindAddress() {
        return NetUtils.createSocketAddr("localhost:9876");
      }

      @Override
      public int getHttpPort() {
        return -1;
      }
    };
  }

  public void setClusterInfo(ClusterInfo clusterInfo) {
    // Only useful if set before a job is started.
    if (getServiceState() == Service.STATE.NOTINITED
        || getServiceState() == Service.STATE.INITED) {
      this.clusterInfo = clusterInfo;
    } else {
      throw new IllegalStateException(
          "ClusterInfo can only be set before the App is STARTED");
    }
  }

  class TestJob extends JobImpl {
    //override the init transition
    private final TestInitTransition initTransition = new TestInitTransition(
        maps, reduces);
    StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent> localFactory
        = stateMachineFactory.addTransition(JobStateInternal.NEW,
            EnumSet.of(JobStateInternal.INITED, JobStateInternal.FAILED),
            JobEventType.JOB_INIT,
            // This is abusive.
            initTransition);

    private final StateMachine<JobStateInternal, JobEventType, JobEvent>
        localStateMachine;

    @Override
    protected StateMachine<JobStateInternal, JobEventType, JobEvent> getStateMachine() {
      return localStateMachine;
    }

    @SuppressWarnings("rawtypes")
    public TestJob(JobId jobId, ApplicationAttemptId applicationAttemptId,
        Configuration conf, EventHandler eventHandler,
        TaskAttemptListener taskAttemptListener, Clock clock,
        OutputCommitter committer, boolean newApiCommitter,
        String user, AppContext appContext,
        JobStateInternal forcedState, String diagnostic) {
      super(jobId, getApplicationAttemptId(applicationId, getStartCount()),
          conf, eventHandler, taskAttemptListener,
          new JobTokenSecretManager(), new Credentials(), clock,
          getCompletedTaskFromPreviousRun(), metrics, committer,
          newApiCommitter, user, System.currentTimeMillis(), getAllAMInfos(),
          appContext, forcedState, diagnostic);

      // This "this leak" is okay because the retained pointer is in an
      //  instance variable.
      localStateMachine = localFactory.make(this);
    }
  }

  //Override InitTransition to not look for split files etc
  static class TestInitTransition extends JobImpl.InitTransition {
    private int maps;
    private int reduces;
    TestInitTransition(int maps, int reduces) {
      this.maps = maps;
      this.reduces = reduces;
    }
    @Override
    protected void setup(JobImpl job) throws IOException {
      super.setup(job);
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

  public static Token newContainerToken(NodeId nodeId,
      byte[] password, ContainerTokenIdentifier tokenIdentifier) {
    // RPC layer client expects ip:port as service for tokens
    InetSocketAddress addr =
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
    // NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
    Token containerToken =
        Token.newInstance(tokenIdentifier.getBytes(),
          ContainerTokenIdentifier.KIND.toString(), password, SecurityUtil
            .buildTokenService(addr).toString());
    return containerToken;
  }


  public static ContainerId newContainerId(int appId, int appAttemptId,
      long timestamp, int containerId) {
    ApplicationId applicationId = ApplicationId.newInstance(timestamp, appId);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, appAttemptId);
    return ContainerId.newContainerId(applicationAttemptId, containerId);
  }

  public static ContainerTokenIdentifier newContainerTokenIdentifier(
      Token containerToken) throws IOException {
    org.apache.hadoop.security.token.Token<ContainerTokenIdentifier> token =
        new org.apache.hadoop.security.token.Token<ContainerTokenIdentifier>(
            containerToken.getIdentifier()
                .array(), containerToken.getPassword().array(), new Text(
                containerToken.getKind()),
            new Text(containerToken.getService()));
    return token.decodeIdentifier();
  }
}
 
