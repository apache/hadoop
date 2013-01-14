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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalContainerLauncher;
import org.apache.hadoop.mapred.TaskAttemptListenerImpl;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.AMStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.EventReader;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryCopyService;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventHandler;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherImpl;
import org.apache.hadoop.mapreduce.v2.app.local.LocalContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.recover.Recovery;
import org.apache.hadoop.mapreduce.v2.app.recover.RecoveryService;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.Speculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * The Map-Reduce Application Master.
 * The state machine is encapsulated in the implementation of Job interface.
 * All state changes happens via Job interface. Each event 
 * results in a Finite State Transition in Job.
 * 
 * MR AppMaster is the composition of loosely coupled services. The services 
 * interact with each other via events. The components resembles the 
 * Actors model. The component acts on received event and send out the 
 * events to other components.
 * This keeps it highly concurrent with no or minimal synchronization needs.
 * 
 * The events are dispatched by a central Dispatch mechanism. All components
 * register to the Dispatcher.
 * 
 * The information is shared across different components using AppContext.
 */

@SuppressWarnings("rawtypes")
public class MRAppMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(MRAppMaster.class);

  /**
   * Priority of the MRAppMaster shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private Clock clock;
  private final long startTime;
  private final long appSubmitTime;
  private String appName;
  private final ApplicationAttemptId appAttemptID;
  private final ContainerId containerID;
  private final String nmHost;
  private final int nmPort;
  private final int nmHttpPort;
  protected final MRAppMetrics metrics;
  private Map<TaskId, TaskInfo> completedTasksFromPreviousRun;
  private List<AMInfo> amInfos;
  private AppContext context;
  private Dispatcher dispatcher;
  private ClientService clientService;
  private Recovery recoveryServ;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private EventHandler<CommitterEvent> committerEventHandler;
  private Speculator speculator;
  private TaskAttemptListener taskAttemptListener;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private JobId jobId;
  private boolean newApiCommitter;
  private OutputCommitter committer;
  private JobEventDispatcher jobEventDispatcher;
  private JobHistoryEventHandler jobHistoryEventHandler;
  private boolean inRecovery = false;
  private SpeculatorEventDispatcher speculatorEventDispatcher;

  private Job job;
  private Credentials fsTokens = new Credentials(); // Filled during init
  protected UserGroupInformation currentUser; // Will be setup during init

  private volatile boolean isLastAMRetry = false;
  //Something happened and we should shut down right after we start up.
  boolean errorHappenedShutDown = false;
  private String shutDownMessage = null;
  JobStateInternal forcedState = null;

  public MRAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        new SystemClock(), appSubmitTime);
  }

  public MRAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime) {
    super(MRAppMaster.class.getName());
    this.clock = clock;
    this.startTime = clock.getTime();
    this.appSubmitTime = appSubmitTime;
    this.appAttemptID = applicationAttemptId;
    this.containerID = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    this.metrics = MRAppMetrics.create();
    LOG.info("Created MRAppMaster for application " + applicationAttemptId);
  }

  @Override
  public void init(final Configuration conf) {
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    downloadTokensAndSetupUGI(conf);
    
    //TODO this is a hack, we really need the RM to inform us when we
    // are the last one.  This would allow us to configure retries on
    // a per application basis.
    int numAMRetries = conf.getInt(YarnConfiguration.RM_AM_MAX_RETRIES, 
        YarnConfiguration.DEFAULT_RM_AM_MAX_RETRIES);
    isLastAMRetry = appAttemptID.getAttemptId() >= numAMRetries;
    LOG.info("AM Retries: " + numAMRetries + 
        " attempt num: " + appAttemptID.getAttemptId() +
        " is last retry: " + isLastAMRetry);
    
    
    context = new RunningAppContext(conf);

    // Job name is the same as the app name util we support DAG of jobs
    // for an app later
    appName = conf.get(MRJobConfig.JOB_NAME, "<missing app name>");

    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, appAttemptID.getAttemptId());
     
    newApiCommitter = false;
    jobId = MRBuilderUtils.newJobId(appAttemptID.getApplicationId(),
        appAttemptID.getApplicationId().getId());
    int numReduceTasks = conf.getInt(MRJobConfig.NUM_REDUCES, 0);
    if ((numReduceTasks > 0 && 
        conf.getBoolean("mapred.reducer.new-api", false)) ||
          (numReduceTasks == 0 && 
           conf.getBoolean("mapred.mapper.new-api", false)))  {
      newApiCommitter = true;
      LOG.info("Using mapred newApiCommitter.");
    }
    
    boolean copyHistory = false;
    try {
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      Path stagingDir = MRApps.getStagingAreaDir(conf, user);
      FileSystem fs = getFileSystem(conf);
      boolean stagingExists = fs.exists(stagingDir);
      Path startCommitFile = MRApps.getStartJobCommitFile(conf, user, jobId);
      boolean commitStarted = fs.exists(startCommitFile);
      Path endCommitSuccessFile = MRApps.getEndJobCommitSuccessFile(conf, user, jobId);
      boolean commitSuccess = fs.exists(endCommitSuccessFile);
      Path endCommitFailureFile = MRApps.getEndJobCommitFailureFile(conf, user, jobId);
      boolean commitFailure = fs.exists(endCommitFailureFile);
      if(!stagingExists) {
        isLastAMRetry = true;
        errorHappenedShutDown = true;
        forcedState = JobStateInternal.ERROR;
        shutDownMessage = "Staging dir does not exist " + stagingDir;
        LOG.fatal(shutDownMessage);
      } else if (commitStarted) {
        //A commit was started so this is the last time, we just need to know
        // what result we will use to notify, and how we will unregister
        errorHappenedShutDown = true;
        isLastAMRetry = true;
        copyHistory = true;
        if (commitSuccess) {
          shutDownMessage = "We crashed after successfully committing. Recovering.";
          forcedState = JobStateInternal.SUCCEEDED;
        } else if (commitFailure) {
          shutDownMessage = "We crashed after a commit failure.";
          forcedState = JobStateInternal.FAILED;
        } else {
          //The commit is still pending, commit error
          shutDownMessage = "We crashed durring a commit";
          forcedState = JobStateInternal.ERROR;
        }
      }
    } catch (IOException e) {
      throw new YarnException("Error while initializing", e);
    }
    
    if (errorHappenedShutDown) {
      dispatcher = createDispatcher();
      addIfService(dispatcher);
      
      NoopEventHandler eater = new NoopEventHandler();
      //We do not have a JobEventDispatcher in this path
      dispatcher.register(JobEventType.class, eater);

      EventHandler<JobHistoryEvent> historyService = null;
      if (copyHistory) {
        historyService = 
          createJobHistoryHandler(context);
        dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
            historyService);
      } else {
        dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
            eater);
      }
      
      // service to allocate containers from RM (if non-uber) or to fake it (uber)
      containerAllocator = createContainerAllocator(null, context);
      addIfService(containerAllocator);
      dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);

      if (copyHistory) {
        // Add the staging directory cleaner before the history server but after
        // the container allocator so the staging directory is cleaned after
        // the history has been flushed but before unregistering with the RM.
        addService(createStagingDirCleaningService());

        // Add the JobHistoryEventHandler last so that it is properly stopped first.
        // This will guarantee that all history-events are flushed before AM goes
        // ahead with shutdown.
        // Note: Even though JobHistoryEventHandler is started last, if any
        // component creates a JobHistoryEvent in the meanwhile, it will be just be
        // queued inside the JobHistoryEventHandler 
        addIfService(historyService);
        

        JobHistoryCopyService cpHist = new JobHistoryCopyService(appAttemptID,
            dispatcher.getEventHandler());
        addIfService(cpHist);
      }
    } else {
      committer = createOutputCommitter(conf);
      boolean recoveryEnabled = conf.getBoolean(
          MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
      boolean recoverySupportedByCommitter = committer.isRecoverySupported();
      if (recoveryEnabled && recoverySupportedByCommitter
          && appAttemptID.getAttemptId() > 1) {
        LOG.info("Recovery is enabled. "
            + "Will try to recover from previous life on best effort basis.");
        recoveryServ = createRecoveryService(context);
        addIfService(recoveryServ);
        dispatcher = recoveryServ.getDispatcher();
        clock = recoveryServ.getClock();
        inRecovery = true;
      } else {
        LOG.info("Not starting RecoveryService: recoveryEnabled: "
            + recoveryEnabled + " recoverySupportedByCommitter: "
            + recoverySupportedByCommitter + " ApplicationAttemptID: "
            + appAttemptID.getAttemptId());
        dispatcher = createDispatcher();
        addIfService(dispatcher);
      }

      //service to handle requests from JobClient
      clientService = createClientService(context);
      addIfService(clientService);
      
      containerAllocator = createContainerAllocator(clientService, context);
      
      //service to handle the output committer
      committerEventHandler = createCommitterEventHandler(context, committer);
      addIfService(committerEventHandler);

      //service to handle requests to TaskUmbilicalProtocol
      taskAttemptListener = createTaskAttemptListener(context);
      addIfService(taskAttemptListener);

      //service to log job history events
      EventHandler<JobHistoryEvent> historyService = 
        createJobHistoryHandler(context);
      dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
          historyService);

      this.jobEventDispatcher = new JobEventDispatcher();

      //register the event dispatchers
      dispatcher.register(JobEventType.class, jobEventDispatcher);
      dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
      dispatcher.register(TaskAttemptEventType.class, 
          new TaskAttemptEventDispatcher());
      dispatcher.register(CommitterEventType.class, committerEventHandler);

      if (conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
          || conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
        //optional service to speculate on task attempts' progress
        speculator = createSpeculator(conf, context);
        addIfService(speculator);
      }

      speculatorEventDispatcher = new SpeculatorEventDispatcher(conf);
      dispatcher.register(Speculator.EventType.class,
          speculatorEventDispatcher);

      // service to allocate containers from RM (if non-uber) or to fake it (uber)
      addIfService(containerAllocator);
      dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);

      // corresponding service to launch allocated containers via NodeManager
      containerLauncher = createContainerLauncher(context);
      addIfService(containerLauncher);
      dispatcher.register(ContainerLauncher.EventType.class, containerLauncher);

      // Add the staging directory cleaner before the history server but after
      // the container allocator so the staging directory is cleaned after
      // the history has been flushed but before unregistering with the RM.
      addService(createStagingDirCleaningService());

      // Add the JobHistoryEventHandler last so that it is properly stopped first.
      // This will guarantee that all history-events are flushed before AM goes
      // ahead with shutdown.
      // Note: Even though JobHistoryEventHandler is started last, if any
      // component creates a JobHistoryEvent in the meanwhile, it will be just be
      // queued inside the JobHistoryEventHandler 
      addIfService(historyService);
    }
    
    super.init(conf);
  } // end of init()
  
  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  private OutputCommitter createOutputCommitter(Configuration conf) {
    OutputCommitter committer = null;

    LOG.info("OutputCommitter set in config "
        + conf.get("mapred.output.committer.class"));

    if (newApiCommitter) {
      org.apache.hadoop.mapreduce.v2.api.records.TaskId taskID = MRBuilderUtils
          .newTaskId(jobId, 0, TaskType.MAP);
      org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID = MRBuilderUtils
          .newTaskAttemptId(taskID, 0);
      TaskAttemptContext taskContext = new TaskAttemptContextImpl(conf,
          TypeConverter.fromYarn(attemptID));
      OutputFormat outputFormat;
      try {
        outputFormat = ReflectionUtils.newInstance(taskContext
            .getOutputFormatClass(), conf);
        committer = outputFormat.getOutputCommitter(taskContext);
      } catch (Exception e) {
        throw new YarnException(e);
      }
    } else {
      committer = ReflectionUtils.newInstance(conf.getClass(
          "mapred.output.committer.class", FileOutputCommitter.class,
          org.apache.hadoop.mapred.OutputCommitter.class), conf);
    }
    LOG.info("OutputCommitter is " + committer.getClass().getName());
    return committer;
  }

  protected boolean keepJobFiles(JobConf conf) {
    return (conf.getKeepTaskFilesPattern() != null || conf
        .getKeepFailedTaskFiles());
  }
  
  /**
   * Create the default file System for this job.
   * @param conf the conf object
   * @return the default filesystem for this job
   * @throws IOException
   */
  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }
  
  /**
   * clean up staging directories for the job.
   * @throws IOException
   */
  public void cleanupStagingDir() throws IOException {
    /* make sure we clean the staging files */
    String jobTempDir = null;
    FileSystem fs = getFileSystem(getConfig());
    try {
      if (!keepJobFiles(new JobConf(getConfig()))) {
        jobTempDir = getConfig().get(MRJobConfig.MAPREDUCE_JOB_DIR);
        if (jobTempDir == null) {
          LOG.warn("Job Staging directory is null");
          return;
        }
        Path jobTempDirPath = new Path(jobTempDir);
        LOG.info("Deleting staging directory " + FileSystem.getDefaultUri(getConfig()) +
            " " + jobTempDir);
        fs.delete(jobTempDirPath, true);
      }
    } catch(IOException io) {
      LOG.error("Failed to cleanup staging dir " + jobTempDir, io);
    }
  }
  
  /**
   * Exit call. Just in a function call to enable testing.
   */
  protected void sysexit() {
    System.exit(0);
  }

  @VisibleForTesting
  public void shutDownJob() {
    // job has finished
    // this is the only job, so shut down the Appmaster
    // note in a workflow scenario, this may lead to creation of a new
    // job (FIXME?)
    // Send job-end notification
    if (getConfig().get(MRJobConfig.MR_JOB_END_NOTIFICATION_URL) != null) {
      try {
        LOG.info("Job end notification started for jobID : "
            + job.getReport().getJobId());
        JobEndNotifier notifier = new JobEndNotifier();
        notifier.setConf(getConfig());
        notifier.notify(job.getReport());
      } catch (InterruptedException ie) {
        LOG.warn("Job end notification interrupted for jobID : "
            + job.getReport().getJobId(), ie);
      }
    }

    // TODO:currently just wait for some time so clients can know the
    // final states. Will be removed once RM come on.
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      //We are finishing cleanly so this is the last retry
      isLastAMRetry = true;
      // Stop all services
      // This will also send the final report to the ResourceManager
      LOG.info("Calling stop for all the services");
      MRAppMaster.this.stop();

    } catch (Throwable t) {
      LOG.warn("Graceful stop failed ", t);
    }

    //Bring the process down by force.
    //Not needed after HADOOP-7140
    LOG.info("Exiting MR AppMaster..GoodBye!");
    sysexit();   
  }
 
  private class JobFinishEventHandler implements EventHandler<JobFinishEvent> {
    @Override
    public void handle(JobFinishEvent event) {
      // Create a new thread to shutdown the AM. We should not do it in-line
      // to avoid blocking the dispatcher itself.
      new Thread() {
        
        @Override
        public void run() {
          shutDownJob();
        }
      }.start();
    }
  }
  
  /**
   * create an event handler that handles the job finish event.
   * @return the job finish event handler.
   */
  protected EventHandler<JobFinishEvent> createJobFinishEventHandler() {
    return new JobFinishEventHandler();
  }

  /**
   * Create the recovery service.
   * @return an instance of the recovery service.
   */
  protected Recovery createRecoveryService(AppContext appContext) {
    return new RecoveryService(appContext.getApplicationAttemptId(),
        appContext.getClock(), getCommitter(), isNewApiCommitter());
  }

  /** Create and initialize (but don't start) a single job. 
   * @param forcedState a state to force the job into or null for normal operation. 
   * @param diagnostic a diagnostic message to include with the job.
   */
  protected Job createJob(Configuration conf, JobStateInternal forcedState, 
      String diagnostic) {

    // create single job
    Job newJob =
        new JobImpl(jobId, appAttemptID, conf, dispatcher.getEventHandler(),
            taskAttemptListener, jobTokenSecretManager, fsTokens, clock,
            completedTasksFromPreviousRun, metrics, newApiCommitter,
            currentUser.getUserName(), appSubmitTime, amInfos, context, 
            forcedState, diagnostic);
    ((RunningAppContext) context).jobs.put(newJob.getID(), newJob);

    dispatcher.register(JobFinishEvent.Type.class,
        createJobFinishEventHandler());     
    return newJob;
  } // end createJob()


  /**
   * Obtain the tokens needed by the job and put them in the UGI
   * @param conf
   */
  protected void downloadTokensAndSetupUGI(Configuration conf) {

    try {
      this.currentUser = UserGroupInformation.getCurrentUser();

      // Read the file-system tokens from the localized tokens-file.
      Path jobSubmitDir = 
          FileContext.getLocalFSFileContext().makeQualified(
              new Path(new File(MRJobConfig.JOB_SUBMIT_DIR)
                  .getAbsolutePath()));
      Path jobTokenFile = 
          new Path(jobSubmitDir, MRJobConfig.APPLICATION_TOKENS_FILE);
      fsTokens.addAll(Credentials.readTokenStorageFile(jobTokenFile, conf));
      LOG.info("jobSubmitDir=" + jobSubmitDir + " jobTokenFile="
          + jobTokenFile);
      currentUser.addCredentials(fsTokens); // For use by AppMaster itself.
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
      AppContext context) {
    this.jobHistoryEventHandler = new JobHistoryEventHandler(context,
      getStartCount());
    return this.jobHistoryEventHandler;
  }

  protected AbstractService createStagingDirCleaningService() {
    return new StagingDirCleaningService();
  }

  protected Speculator createSpeculator(Configuration conf, AppContext context) {
    Class<? extends Speculator> speculatorClass;

    try {
      speculatorClass
          // "yarn.mapreduce.job.speculator.class"
          = conf.getClass(MRJobConfig.MR_AM_JOB_SPECULATOR,
                          DefaultSpeculator.class,
                          Speculator.class);
      Constructor<? extends Speculator> speculatorConstructor
          = speculatorClass.getConstructor
               (Configuration.class, AppContext.class);
      Speculator result = speculatorConstructor.newInstance(conf, context);

      return result;
    } catch (InstantiationException ex) {
      LOG.error("Can't make a speculator -- check "
          + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
      throw new YarnException(ex);
    } catch (IllegalAccessException ex) {
      LOG.error("Can't make a speculator -- check "
          + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
      throw new YarnException(ex);
    } catch (InvocationTargetException ex) {
      LOG.error("Can't make a speculator -- check "
          + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
      throw new YarnException(ex);
    } catch (NoSuchMethodException ex) {
      LOG.error("Can't make a speculator -- check "
          + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
      throw new YarnException(ex);
    }
  }

  protected TaskAttemptListener createTaskAttemptListener(AppContext context) {
    TaskAttemptListener lis =
        new TaskAttemptListenerImpl(context, jobTokenSecretManager,
            getRMHeartbeatHandler());
    return lis;
  }

  protected EventHandler<CommitterEvent> createCommitterEventHandler(
      AppContext context, OutputCommitter committer) {
    return new CommitterEventHandler(context, committer,
        getRMHeartbeatHandler());
  }

  protected ContainerAllocator createContainerAllocator(
      final ClientService clientService, final AppContext context) {
    return new ContainerAllocatorRouter(clientService, context);
  }

  protected RMHeartbeatHandler getRMHeartbeatHandler() {
    return (RMHeartbeatHandler) containerAllocator;
  }

  protected ContainerLauncher
      createContainerLauncher(final AppContext context) {
    return new ContainerLauncherRouter(context);
  }

  //TODO:should have an interface for MRClientService
  protected ClientService createClientService(AppContext context) {
    return new MRClientService(context);
  }

  public ApplicationId getAppID() {
    return appAttemptID.getApplicationId();
  }

  public ApplicationAttemptId getAttemptID() {
    return appAttemptID;
  }

  public JobId getJobId() {
    return jobId;
  }

  public OutputCommitter getCommitter() {
    return committer;
  }

  public boolean isNewApiCommitter() {
    return newApiCommitter;
  }

  public int getStartCount() {
    return appAttemptID.getAttemptId();
  }

  public AppContext getContext() {
    return context;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public Map<TaskId, TaskInfo> getCompletedTaskFromPreviousRun() {
    return completedTasksFromPreviousRun;
  }

  public List<AMInfo> getAllAMInfos() {
    return amInfos;
  }
  
  public ContainerAllocator getContainerAllocator() {
    return containerAllocator;
  }
  
  public ContainerLauncher getContainerLauncher() {
    return containerLauncher;
  }

  public TaskAttemptListener getTaskAttemptListener() {
    return taskAttemptListener;
  }

  /**
   * By the time life-cycle of this router starts, job-init would have already
   * happened.
   */
  private final class ContainerAllocatorRouter extends AbstractService
      implements ContainerAllocator, RMHeartbeatHandler {
    private final ClientService clientService;
    private final AppContext context;
    private ContainerAllocator containerAllocator;

    ContainerAllocatorRouter(ClientService clientService,
        AppContext context) {
      super(ContainerAllocatorRouter.class.getName());
      this.clientService = clientService;
      this.context = context;
    }

    @Override
    public synchronized void start() {
      if (job.isUber()) {
        this.containerAllocator = new LocalContainerAllocator(
            this.clientService, this.context, nmHost, nmPort, nmHttpPort
            , containerID);
      } else {
        this.containerAllocator = new RMContainerAllocator(
            this.clientService, this.context);
      }
      ((Service)this.containerAllocator).init(getConfig());
      ((Service)this.containerAllocator).start();
      super.start();
    }

    @Override
    public synchronized void stop() {
      ((Service)this.containerAllocator).stop();
      super.stop();
    }

    @Override
    public void handle(ContainerAllocatorEvent event) {
      this.containerAllocator.handle(event);
    }

    public void setSignalled(boolean isSignalled) {
      ((RMCommunicator) containerAllocator).setSignalled(isSignalled);
    }
    
    public void setShouldUnregister(boolean shouldUnregister) {
      ((RMCommunicator) containerAllocator).setShouldUnregister(shouldUnregister);
    }

    @Override
    public long getLastHeartbeatTime() {
      return ((RMCommunicator) containerAllocator).getLastHeartbeatTime();
    }

    @Override
    public void runOnNextHeartbeat(Runnable callback) {
      ((RMCommunicator) containerAllocator).runOnNextHeartbeat(callback);
    }
  }

  /**
   * By the time life-cycle of this router starts, job-init would have already
   * happened.
   */
  private final class ContainerLauncherRouter extends AbstractService
      implements ContainerLauncher {
    private final AppContext context;
    private ContainerLauncher containerLauncher;

    ContainerLauncherRouter(AppContext context) {
      super(ContainerLauncherRouter.class.getName());
      this.context = context;
    }

    @Override
    public synchronized void start() {
      if (job.isUber()) {
        this.containerLauncher = new LocalContainerLauncher(context,
            (TaskUmbilicalProtocol) taskAttemptListener);
      } else {
        this.containerLauncher = new ContainerLauncherImpl(context);
      }
      ((Service)this.containerLauncher).init(getConfig());
      ((Service)this.containerLauncher).start();
      super.start();
    }

    @Override
    public void handle(ContainerLauncherEvent event) {
        this.containerLauncher.handle(event);
    }

    @Override
    public synchronized void stop() {
      ((Service)this.containerLauncher).stop();
      super.stop();
    }
  }

  private final class StagingDirCleaningService extends AbstractService {
    StagingDirCleaningService() {
      super(StagingDirCleaningService.class.getName());
    }

    @Override
    public synchronized void stop() {
      try {
        if(isLastAMRetry) {
          cleanupStagingDir();
        } else {
          LOG.info("Skipping cleaning up the staging dir. "
              + "assuming AM will be retried.");
        }
      } catch (IOException io) {
        LOG.error("Failed to cleanup staging dir: ", io);
      }
      super.stop();
    }
  }

  private class RunningAppContext implements AppContext {

    private final Map<JobId, Job> jobs = new ConcurrentHashMap<JobId, Job>();
    private final Configuration conf;
    private final ClusterInfo clusterInfo = new ClusterInfo();

    public RunningAppContext(Configuration config) {
      this.conf = config;
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appAttemptID.getApplicationId();
    }

    @Override
    public String getApplicationName() {
      return appName;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public Job getJob(JobId jobID) {
      return jobs.get(jobID);
    }

    @Override
    public Map<JobId, Job> getAllJobs() {
      return jobs;
    }

    @Override
    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    @Override
    public CharSequence getUser() {
      return this.conf.get(MRJobConfig.USER_NAME);
    }

    @Override
    public Clock getClock() {
      return clock;
    }
    
    @Override
    public ClusterInfo getClusterInfo() {
      return this.clusterInfo;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void start() {

    amInfos = new LinkedList<AMInfo>();

    // Pull completedTasks etc from recovery
    if (inRecovery) {
      completedTasksFromPreviousRun = recoveryServ.getCompletedTasks();
      amInfos = recoveryServ.getAMInfos();
    } else {
      // Get the amInfos anyways irrespective of whether recovery is enabled or
      // not IF this is not the first AM generation
      if (appAttemptID.getAttemptId() != 1) {
        amInfos.addAll(readJustAMInfos());
      }
    }

    // Current an AMInfo for the current AM generation.
    AMInfo amInfo =
        MRBuilderUtils.newAMInfo(appAttemptID, startTime, containerID, nmHost,
            nmPort, nmHttpPort);
    amInfos.add(amInfo);

    // /////////////////// Create the job itself.
    job = createJob(getConfig(), forcedState, shutDownMessage);

    // End of creating the job.

    // Send out an MR AM inited event for this AM and all previous AMs.
    for (AMInfo info : amInfos) {
      dispatcher.getEventHandler().handle(
          new JobHistoryEvent(job.getID(), new AMStartedEvent(info
              .getAppAttemptId(), info.getStartTime(), info.getContainerId(),
              info.getNodeManagerHost(), info.getNodeManagerPort(), info
                  .getNodeManagerHttpPort())));
    }

    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("MRAppMaster");

    if (!errorHappenedShutDown) {
      // create a job event for job intialization
      JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
      // Send init to the job (this does NOT trigger job execution)
      // This is a synchronous call, not an event through dispatcher. We want
      // job-init to be done completely here.
      jobEventDispatcher.handle(initJobEvent);


      // JobImpl's InitTransition is done (call above is synchronous), so the
      // "uber-decision" (MR-1220) has been made.  Query job and switch to
      // ubermode if appropriate (by registering different container-allocator
      // and container-launcher services/event-handlers).

      if (job.isUber()) {
        speculatorEventDispatcher.disableSpeculation();
        LOG.info("MRAppMaster uberizing job " + job.getID()
            + " in local container (\"uber-AM\") on node "
            + nmHost + ":" + nmPort + ".");
      } else {
        // send init to speculator only for non-uber jobs. 
        // This won't yet start as dispatcher isn't started yet.
        dispatcher.getEventHandler().handle(
            new SpeculatorEvent(job.getID(), clock.getTime()));
        LOG.info("MRAppMaster launching normal, non-uberized, multi-container "
            + "job " + job.getID() + ".");
      }
    }

    //start all the components
    super.start();

    // All components have started, start the job.
    startJobs();
  }

  private List<AMInfo> readJustAMInfos() {
    List<AMInfo> amInfos = new ArrayList<AMInfo>();
    FSDataInputStream inputStream = null;
    try {
      inputStream =
          RecoveryService.getPreviousJobHistoryFileStream(getConfig(),
            appAttemptID);
      EventReader jobHistoryEventReader = new EventReader(inputStream);

      // All AMInfos are contiguous. Track when the first AMStartedEvent
      // appears.
      boolean amStartedEventsBegan = false;

      HistoryEvent event;
      while ((event = jobHistoryEventReader.getNextEvent()) != null) {
        if (event.getEventType() == EventType.AM_STARTED) {
          if (!amStartedEventsBegan) {
            // First AMStartedEvent.
            amStartedEventsBegan = true;
          }
          AMStartedEvent amStartedEvent = (AMStartedEvent) event;
          amInfos.add(MRBuilderUtils.newAMInfo(
            amStartedEvent.getAppAttemptId(), amStartedEvent.getStartTime(),
            amStartedEvent.getContainerId(),
            StringInterner.weakIntern(amStartedEvent.getNodeManagerHost()),
            amStartedEvent.getNodeManagerPort(),
            amStartedEvent.getNodeManagerHttpPort()));
        } else if (amStartedEventsBegan) {
          // This means AMStartedEvents began and this event is a
          // non-AMStarted event.
          // No need to continue reading all the other events.
          break;
        }
      }
    } catch (IOException e) {
      LOG.warn("Could not parse the old history file. "
          + "Will not have old AMinfos ", e);
    } finally {
      if (inputStream != null) {
        IOUtils.closeQuietly(inputStream);
      }
    }
    return amInfos;
  }

  /**
   * This can be overridden to instantiate multiple jobs and create a 
   * workflow.
   *
   * TODO:  Rework the design to actually support this.  Currently much of the
   * job stuff has been moved to init() above to support uberization (MR-1220).
   * In a typical workflow, one presumably would want to uberize only a subset
   * of the jobs (the "small" ones), which is awkward with the current design.
   */
  @SuppressWarnings("unchecked")
  protected void startJobs() {
    /** create a job-start event to get this ball rolling */
    JobEvent startJobEvent = new JobEvent(job.getID(), JobEventType.JOB_START);
    /** send the job-start event. this triggers the job execution. */
    dispatcher.getEventHandler().handle(startJobEvent);
  }

  private class JobEventDispatcher implements EventHandler<JobEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(JobEvent event) {
      ((EventHandler<JobEvent>)context.getJob(event.getJobId())).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      Task task = context.getJob(event.getTaskID().getJobId()).getTask(
          event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher 
          implements EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      Job job = context.getJob(event.getTaskAttemptID().getTaskId().getJobId());
      Task task = job.getTask(event.getTaskAttemptID().getTaskId());
      TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());
      ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
    }
  }

  private class SpeculatorEventDispatcher implements
      EventHandler<SpeculatorEvent> {
    private final Configuration conf;
    private volatile boolean disabled;
    public SpeculatorEventDispatcher(Configuration config) {
      this.conf = config;
    }
    @Override
    public void handle(SpeculatorEvent event) {
      if (disabled) {
        return;
      }

      TaskId tId = event.getTaskID();
      TaskType tType = null;
      /* event's TaskId will be null if the event type is JOB_CREATE or
       * ATTEMPT_STATUS_UPDATE
       */
      if (tId != null) {
        tType = tId.getTaskType(); 
      }
      boolean shouldMapSpec =
              conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false);
      boolean shouldReduceSpec =
              conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

      /* The point of the following is to allow the MAP and REDUCE speculative
       * config values to be independent:
       * IF spec-exec is turned on for maps AND the task is a map task
       * OR IF spec-exec is turned on for reduces AND the task is a reduce task
       * THEN call the speculator to handle the event.
       */
      if ( (shouldMapSpec && (tType == null || tType == TaskType.MAP))
        || (shouldReduceSpec && (tType == null || tType == TaskType.REDUCE))) {
        // Speculator IS enabled, direct the event to there.
        speculator.handle(event);
      }
    }

    public void disableSpeculation() {
      disabled = true;
    }

  }

  /**
   * Eats events that are not needed in some error cases.
   */
  private static class NoopEventHandler implements EventHandler<Event> {

    @Override
    public void handle(Event event) {
      //Empty
    }
  }
  
  private static void validateInputParam(String value, String param)
      throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  public static void main(String[] args) {
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      String containerIdStr =
          System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV);
      String nodeHostString = System.getenv(ApplicationConstants.NM_HOST_ENV);
      String nodePortString = System.getenv(ApplicationConstants.NM_PORT_ENV);
      String nodeHttpPortString =
          System.getenv(ApplicationConstants.NM_HTTP_PORT_ENV);
      String appSubmitTimeStr =
          System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      
      validateInputParam(containerIdStr,
          ApplicationConstants.AM_CONTAINER_ID_ENV);
      validateInputParam(nodeHostString, ApplicationConstants.NM_HOST_ENV);
      validateInputParam(nodePortString, ApplicationConstants.NM_PORT_ENV);
      validateInputParam(nodeHttpPortString,
          ApplicationConstants.NM_HTTP_PORT_ENV);
      validateInputParam(appSubmitTimeStr,
          ApplicationConstants.APP_SUBMIT_TIME_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId =
          containerId.getApplicationAttemptId();
      long appSubmitTime = Long.parseLong(appSubmitTimeStr);
      
      MRAppMaster appMaster =
          new MRAppMaster(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), appSubmitTime);
      ShutdownHookManager.get().addShutdownHook(
        new MRAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      conf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());
      conf.set(MRJobConfig.USER_NAME, jobUserName);
      // Do not automatically close FileSystem objects so that in case of
      // SIGTERM I have a chance to write out the job history. I'll be closing
      // the objects myself.
      conf.setBoolean("fs.automatic.close", false);
      // set job classloader if configured
      MRApps.setJobClassLoader(conf);
      initAndStartAppMaster(appMaster, conf, jobUserName);
    } catch (Throwable t) {
      LOG.fatal("Error starting MRAppMaster", t);
      System.exit(1);
    }
  }

  // The shutdown hook that runs when a signal is received AND during normal
  // close of the JVM.
  static class MRAppMasterShutdownHook implements Runnable {
    MRAppMaster appMaster;
    MRAppMasterShutdownHook(MRAppMaster appMaster) {
      this.appMaster = appMaster;
    }
    public void run() {
      LOG.info("MRAppMaster received a signal. Signaling RMCommunicator and "
        + "JobHistoryEventHandler.");

      // Notify the JHEH and RMCommunicator that a SIGTERM has been received so
      // that they don't take too long in shutting down
      if(appMaster.containerAllocator instanceof ContainerAllocatorRouter) {
        ((ContainerAllocatorRouter) appMaster.containerAllocator)
        .setSignalled(true);
        ((ContainerAllocatorRouter) appMaster.containerAllocator)
        .setShouldUnregister(appMaster.isLastAMRetry);
      }
      
      if(appMaster.jobHistoryEventHandler != null) {
        appMaster.jobHistoryEventHandler
          .setForcejobCompletion(appMaster.isLastAMRetry);
      }
      appMaster.stop();
    }
  }

  protected static void initAndStartAppMaster(final MRAppMaster appMaster,
      final YarnConfiguration conf, String jobUserName) throws IOException,
      InterruptedException {
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
    appMasterUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        appMaster.init(conf);
        appMaster.start();
        if(appMaster.errorHappenedShutDown) {
          throw new IOException("Was asked to shut down.");
        }
        return null;
      }
    });
  }
}
