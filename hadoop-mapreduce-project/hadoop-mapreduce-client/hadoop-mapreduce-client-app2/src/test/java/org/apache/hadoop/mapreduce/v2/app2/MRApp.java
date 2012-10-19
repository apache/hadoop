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

package org.apache.hadoop.mapreduce.v2.app2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.ContainerHeartbeatHandler;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app2.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app2.job.Task;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventKillRequest;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptRemoteStartEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app2.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app2.job.impl.TaskAttemptImpl;
import org.apache.hadoop.mapreduce.v2.app2.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerTALaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventTAEnded;
import org.apache.hadoop.mapreduce.v2.app2.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app2.rm.ContainerRequestor;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorContainerDeAllocateRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerRequestor;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainer;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerAssignTAEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventCompleted;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventLaunched;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerState;
import org.apache.hadoop.mapreduce.v2.app2.taskclean.TaskCleaner;
import org.apache.hadoop.mapreduce.v2.app2.taskclean.TaskCleanupEvent;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;


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
  private volatile boolean exited = false;

  public static String NM_HOST = "localhost";
  public static int NM_PORT = 1234;
  public static int NM_HTTP_PORT = 8042;

  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  //if true, tasks complete automatically as soon as they are launched
  protected boolean autoComplete = false;

  static ApplicationId applicationId;

  // TODO: Look at getting rid of this. Each test should generate it's own id, 
  // or have it provided.. Using a custom id without updating this causes problems.
  static {
    applicationId = recordFactory.newRecordInstance(ApplicationId.class);
    applicationId.setClusterTimestamp(0);
    applicationId.setId(0);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, Clock clock) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, 1, clock);
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, 1);
  }
  
  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount) {
    this(maps, reduces, autoComplete, testName, cleanOnStart, startCount,
        new SystemClock());
  }

  public MRApp(int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount, Clock clock) {
    this(getApplicationAttemptId(applicationId, startCount), getContainerId(
      applicationId, startCount), maps, reduces, autoComplete, testName,
      cleanOnStart, startCount, clock);
  }

  public MRApp(ApplicationAttemptId appAttemptId, ContainerId amContainerId,
      int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount) {
    this(appAttemptId, amContainerId, maps, reduces, autoComplete, testName,
        cleanOnStart, startCount, new SystemClock());
  }

  public MRApp(ApplicationAttemptId appAttemptId, ContainerId amContainerId,
      int maps, int reduces, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount, Clock clock) {
    super(appAttemptId, amContainerId, NM_HOST, NM_PORT, NM_HTTP_PORT, clock, System
        .currentTimeMillis());
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

    applicationId = appAttemptId.getApplicationId();
    this.maps = maps;
    this.reduces = reduces;
    this.autoComplete = autoComplete;
  }

  
  
  @Override
  public void init(Configuration conf) {
    super.init(conf);
    if (this.clusterInfo != null) {
      getContext().getClusterInfo().setMinContainerCapability(
          this.clusterInfo.getMinContainerCapability());
      getContext().getClusterInfo().setMaxContainerCapability(
          this.clusterInfo.getMaxContainerCapability());
    } else {
      getContext().getClusterInfo().setMinContainerCapability(
          BuilderUtils.newResource(1024));
      getContext().getClusterInfo().setMaxContainerCapability(
          BuilderUtils.newResource(10240));
    }
    // XXX Any point doing this here. Otherwise move to an overridden createDispatcher()
//    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, false);
  }

  public Job submit(Configuration conf) throws Exception {
    String user = conf.get(MRJobConfig.USER_NAME, UserGroupInformation
      .getCurrentUser().getShortUserName());
    conf.set(MRJobConfig.USER_NAME, user);
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, testAbsPath.toString());
    conf.setBoolean(MRJobConfig.MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR, true);
    //TODO: fix the bug where the speculator gets events with 
    //not-fully-constructed objects. For now, disable speculative exec
    LOG.info("****DISABLING SPECULATIVE EXECUTION*****");
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

    init(conf);
    start();
    DefaultMetricsSystem.shutdown();
    Job job = getContext().getAllJobs().values().iterator().next();

    // Write job.xml
    String jobFile = MRApps.getJobFile(conf, user,
      TypeConverter.fromYarn(job.getID()));
    LOG.info("Writing job conf to " + jobFile);
    new File(jobFile).getParentFile().mkdirs();
    conf.writeXml(new FileOutputStream(jobFile));

    return job;
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
      System.out.println("TaskAttempt State for " + attempt.getID() + " is : " + 
          report.getTaskAttemptState() +
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

  public void waitForAMExit() throws Exception {
    int timeoutSecs = 0;
    while (!exited && timeoutSecs ++ < 20) {
      System.out.println("Waiting for AM exit");
      Thread.sleep(500);
    }
    System.out.print("AM Exit State is: " + exited);
    Assert.assertEquals("AM did not exit (timedout)", true, exited);
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
  protected void downloadTokensAndSetupUGI(Configuration conf) {
  }

  private static ApplicationAttemptId getApplicationAttemptId(
      ApplicationId applicationId, int startCount) {
    ApplicationAttemptId applicationAttemptId =
        recordFactory.newRecordInstance(ApplicationAttemptId.class);
    applicationAttemptId.setApplicationId(applicationId);
    applicationAttemptId.setAttemptId(startCount);
    return applicationAttemptId;
  }
  
  private static ContainerId getContainerId(ApplicationId applicationId,
      int startCount) {
    ApplicationAttemptId appAttemptId =
        getApplicationAttemptId(applicationId, startCount);
    ContainerId containerId =
        BuilderUtils.newContainerId(appAttemptId, startCount);
    return containerId;
  }
  
  @Override
  protected Job createJob(Configuration conf) {
    UserGroupInformation currentUser = null;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }
    Job newJob = new TestJob(getJobId(), getAttemptID(), conf, 
    		getDispatcher().getEventHandler(),
            getTaskAttemptListener(), getContext().getClock(), getCommitter(),
            isNewApiCommitter(), currentUser.getUserName(),
            getTaskHeartbeatHandler(), getContext());
    ((AppContext) getContext()).getAllJobs().put(newJob.getID(), newJob);

    getDispatcher().register(JobFinishEvent.Type.class, new MRAppJobFinishHandler());

    return newJob;
  }
  
  protected class MRAppJobFinishHandler extends JobFinishEventHandlerCR {
    
    @Override
    protected void exit() {
      exited = true;
    }
    
    @Override
    protected void maybeSendJobEndNotification() {
    }
  }

  @Override
  protected TaskAttemptListener createTaskAttemptListener(AppContext context,
      TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh) {
    return new TaskAttemptListener(){

      @Override
      public InetSocketAddress getAddress() {
        return NetUtils.createSocketAddr("localhost:54321");
      }
      
      @Override
      public void registerRunningJvm(WrappedJvmID jvmID, ContainerId containerId) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void unregisterRunningJvm(WrappedJvmID jvmID) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void unregisterTaskAttempt(TaskAttemptId attemptID) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void registerTaskAttempt(TaskAttemptId attemptId,
          WrappedJvmID jvmId) {
        // TODO Auto-generated method stub
        
      }
    };
  }
  
  @Override
  protected TaskHeartbeatHandler createTaskHeartbeatHandler(AppContext context,
      Configuration conf) {
    return new TaskHeartbeatHandler(context, maps) {

      @Override
      public void init(Configuration conf) {
      }

      @Override
      public void start() {
      }

      @Override
      public void stop() {
      }
    };
  }

  @Override
  protected ContainerHeartbeatHandler createContainerHeartbeatHandler(
      AppContext context, Configuration conf) {
    return new ContainerHeartbeatHandler(context, 1) {
      @Override
      public void init(Configuration conf) {
      }

      @Override
      public void start() {
      }

      @Override
      public void stop() {
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

  // appAcls and attemptToContainerIdMap shared between various mocks.
  private Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
  private Map<TaskAttemptId, ContainerId> attemptToContainerIdMap = new HashMap<TaskAttemptId, ContainerId>();
  
  protected class MockContainerLauncher implements ContainerLauncher {

    //We are running locally so set the shuffle port to -1 
    int shufflePort = -1;

    public MockContainerLauncher() {
    }

    @Override
    public void handle(NMCommunicatorEvent event) {
      switch (event.getType()) {
      case CONTAINER_LAUNCH_REQUEST:
        LOG.info("XXX: Handling CONTAINER_LAUNCH_REQUEST for: " + event.getContainerId());
        
        AMContainer amContainer = getContext().getAllContainers().get(event.getContainerId());
        TaskAttemptId attemptIdForContainer = amContainer.getQueuedTaskAttempts().iterator().next();
        // Container Launched.
        getContext().getEventHandler().handle(
            new AMContainerEventLaunched(event.getContainerId(), shufflePort));
        
        // Simulate a TaskPull from the remote task.
        getContext().getEventHandler().handle(
            new AMContainerEvent(event.getContainerId(),
                AMContainerEventType.C_PULL_TA));
         
        // Simulate a TaskAttemptStartedEvent to the TaskAtetmpt.
        // Maybe simulate a completed task.
        getContext().getEventHandler().handle(
            new TaskAttemptRemoteStartEvent(attemptIdForContainer, event.getContainerId(), appAcls,
                shufflePort));
        attemptLaunched(attemptIdForContainer);

        break;
      case CONTAINER_STOP_REQUEST:
        ContainerStatus cs = Records.newRecord(ContainerStatus.class);
        cs.setContainerId(event.getContainerId());
        getContext().getEventHandler().handle(new AMContainerEventCompleted(cs));
        break;
      }
    }
  }

  protected void attemptLaunched(TaskAttemptId attemptId) {
    if (autoComplete) {
      // send the done event
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(attemptId, TaskAttemptEventType.TA_DONE));
    }
  }

  @Override
  protected ContainerRequestor createContainerRequestor(
      ClientService clientService, AppContext appContext) {
    return new MRAppContainerRequestor(clientService, appContext);
  }
  
  protected class MRAppContainerRequestor extends RMContainerRequestor {

    int numReleaseRequests;
    
    public MRAppContainerRequestor(ClientService clientService,
        AppContext context) {
      super(clientService, context);
    }
    
    @Override public void init(Configuration conf) {}
    @Override public void start() {}
    @Override public void stop() {}
    //TODO XXX: getApplicationAcls, getJob
    
    @Override public void addContainerReq(ContainerRequest req) {}
    @Override public void decContainerReq(ContainerRequest req) {}

    public void handle(RMCommunicatorEvent rawEvent) {
      LOG.info("XXX: MRAppContainerRequestor handling event of type:" + rawEvent.getType() + ", event: " + rawEvent + ", for containerId: ");
      switch (rawEvent.getType()) {
      case CONTAINER_DEALLOCATE:
        numReleaseRequests++;
        ContainerStatus cs = Records.newRecord(ContainerStatus.class);
        cs.setContainerId(((RMCommunicatorContainerDeAllocateRequestEvent)rawEvent).getContainerId());
        getContext().getEventHandler().handle(new AMContainerEventCompleted(cs));
        LOG.info("XXX: Sending out C_COMPLETE for containerId: " + cs.getContainerId());
        break;
      default:
        LOG.warn("Invalid event of type: " + rawEvent.getType() + ", Event: "
            + rawEvent);
        break;
      }
    }

    public int getNumReleaseRequests() {
      return numReleaseRequests;
    }
  }
 
  @Override
  protected ContainerAllocator createAMScheduler(ContainerRequestor requestor,
      AppContext appContext) {
    return new MRAppAMScheduler();
  }

  protected class MRAppAMScheduler extends AbstractService implements ContainerAllocator{
    private int containerCount;
    
    MRAppAMScheduler() {
      super(MRAppAMScheduler.class.getSimpleName());
    }
    
    public void start() {}
    public void init(Configuration conf) {}
    public void stop() {}
    
    @Override
    public void handle(AMSchedulerEvent rawEvent) {
      LOG.info("XXX: MRAppAMScheduler handling event of type:" + rawEvent.getType() + ", event: " + rawEvent);
      switch (rawEvent.getType()) {
      case S_TA_LAUNCH_REQUEST:
        AMSchedulerTALaunchRequestEvent lEvent = (AMSchedulerTALaunchRequestEvent)rawEvent;
        ContainerId cId = Records.newRecord(ContainerId.class);
        cId.setApplicationAttemptId(getContext().getApplicationAttemptId());
        cId.setId(containerCount++);
        NodeId nodeId = BuilderUtils.newNodeId(NM_HOST, NM_PORT);
        Container container = BuilderUtils.newContainer(cId, nodeId,
            NM_HOST + ":" + NM_HTTP_PORT, null, null, null);
        
        getContext().getAllContainers().addContainerIfNew(container);
        getContext().getAllNodes().nodeSeen(nodeId);
        
        JobID id = TypeConverter.fromYarn(applicationId);
        JobId jobId = TypeConverter.toYarn(id);
        getContext().getEventHandler().handle(new JobHistoryEvent(jobId, 
            new NormalizedResourceEvent(TaskType.REDUCE, 100)));
        getContext().getEventHandler().handle(new JobHistoryEvent(jobId, 
            new NormalizedResourceEvent(TaskType.MAP, 100)));
        
        attemptToContainerIdMap.put(lEvent.getAttemptID(), cId);
        if (getContext().getAllContainers().get(cId).getState() == AMContainerState.ALLOCATED) {
          LOG.info("XXX: Sending launch request for container: " + cId
              + " for taskAttemptId: " + lEvent.getAttemptID());
          AMContainerLaunchRequestEvent lrEvent = new AMContainerLaunchRequestEvent(
              cId, jobId, lEvent.getAttemptID().getTaskId().getTaskType(),
              lEvent.getJobToken(), lEvent.getCredentials(), false,
              new JobConf(getContext().getJob(jobId).getConf()));
          getContext().getEventHandler().handle(lrEvent);
        }
        LOG.info("XXX: Assigning attempt [" + lEvent.getAttemptID()
            + "] to Container [" + cId + "]");
        getContext().getEventHandler().handle(
            new AMContainerAssignTAEvent(cId, lEvent.getAttemptID(), lEvent
                .getRemoteTask()));

        break;
      case S_TA_ENDED:
        // Send out a Container_stop_request.
        AMSchedulerEventTAEnded sEvent = (AMSchedulerEventTAEnded) rawEvent;
        LOG.info("XXX: Handling S_TA_ENDED for attemptId:"
            + sEvent.getAttemptID() + " with state: " + sEvent.getState());
        switch (sEvent.getState()) {
        case FAILED:
        case KILLED:
          getContext().getEventHandler().handle(
              new AMContainerEvent(attemptToContainerIdMap.get(sEvent
                  .getAttemptID()), AMContainerEventType.C_STOP_REQUEST));
          break;
        case SUCCEEDED:
          // No re-use in MRApp. Stop the container.
          getContext().getEventHandler().handle(
              new AMContainerEvent(attemptToContainerIdMap.get(sEvent
                  .getAttemptID()), AMContainerEventType.C_STOP_REQUEST));
          break;
        default:
          throw new YarnException("Unexpected state: " + sEvent.getState());
        }
      case S_CONTAINERS_ALLOCATED:
        break;
      case S_CONTAINER_COMPLETED:
        break;
      default:
          break;
      }
    }
  }

  @Override
  protected TaskCleaner createTaskCleaner(AppContext context) {
    return new TaskCleaner() {
      @Override
      public void handle(TaskCleanupEvent event) {
        //send the cleanup done event
//        getContext().getEventHandler().handle(
//            new TaskAttemptEvent(event.getAttemptID(),
//                TaskAttemptEventType.TA_CLEANUP_DONE));
        
        // TODO XXX: Kindof equivalent to saying the container is complete / released.
      }
    };
  }

  @Override
  protected ClientService createClientService(AppContext context) {
    return new ClientService(){
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
        TaskAttemptListener taskAttemptListener,  Clock clock,
        OutputCommitter committer, boolean newApiCommitter, String user,
        TaskHeartbeatHandler thh, AppContext appContext) {
      super(jobId, getApplicationAttemptId(applicationId, getStartCount()),
          conf, eventHandler, taskAttemptListener,
          new JobTokenSecretManager(), new Credentials(), clock,
          getCompletedTaskFromPreviousRun(), metrics, committer,
          newApiCommitter, user, System.currentTimeMillis(), getAllAMInfos(),
          thh, appContext);

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
  

  private TaskAttemptStatus createTaskAttemptStatus(TaskAttemptId taskAttemptId,
      TaskAttemptState finalState) {
    TaskAttemptStatus tas = new TaskAttemptStatus();
    tas.id = taskAttemptId;
    tas.progress = 1.0f;
    tas.phase = Phase.CLEANUP;
    tas.stateString = finalState.name();
    tas.taskState = finalState;
    Counters counters = new Counters();
    tas.counters = counters;
    return tas;
  }

  private void sendStatusUpdate(TaskAttemptId taskAttemptId,
      TaskAttemptState finalState) {
    TaskAttemptStatus tas = createTaskAttemptStatus(taskAttemptId, finalState);
    getContext().getEventHandler().handle(
        new TaskAttemptStatusUpdateEvent(taskAttemptId, tas));
  }

  /*
   * Helper method to move a task attempt into a final state.
   */
  // TODO maybe rename to something like succeedTaskAttempt
  public void sendFinishToTaskAttempt(TaskAttemptId taskAttemptId,
      TaskAttemptState finalState, boolean sendStatusUpdate) throws Exception {
    if (sendStatusUpdate) {
      sendStatusUpdate(taskAttemptId, finalState);
    }
    if (finalState == TaskAttemptState.SUCCEEDED) {
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptId,
              TaskAttemptEventType.TA_DONE));
    } else if (finalState == TaskAttemptState.KILLED) {
      getContext().getEventHandler()
          .handle(new TaskAttemptEventKillRequest(taskAttemptId,
                  "Kill requested"));
    } else if (finalState == TaskAttemptState.FAILED) {
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptId,
              TaskAttemptEventType.TA_FAIL_REQUEST));
    }
  }
}
 
