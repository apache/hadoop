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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalContainerLauncher;
import org.apache.hadoop.mapred.TaskAttemptListenerImpl;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.jobhistory.AMStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.EventReader;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryCopyService;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
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
import org.apache.hadoop.mapreduce.v2.app.job.event.JobStartEvent;
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
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.NoopAMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.Speculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.log4j.LogManager;

import com.google.common.annotations.VisibleForTesting;

import javax.crypto.KeyGenerator;

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
  public static final String INTERMEDIATE_DATA_ENCRYPTION_ALGO = "HmacSHA1";

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
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private EventHandler<CommitterEvent> committerEventHandler;
  private Speculator speculator;
  protected TaskAttemptListener taskAttemptListener;
  protected JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private JobId jobId;
  private boolean newApiCommitter;
  private ClassLoader jobClassLoader;
  private OutputCommitter committer;
  private JobEventDispatcher jobEventDispatcher;
  private JobHistoryEventHandler jobHistoryEventHandler;
  private SpeculatorEventDispatcher speculatorEventDispatcher;
  private AMPreemptionPolicy preemptionPolicy;
  private byte[] encryptedSpillKey;

  // After a task attempt completes from TaskUmbilicalProtocol's point of view,
  // it will be transitioned to finishing state.
  // taskAttemptFinishingMonitor is just a timer for attempts in finishing
  // state. If the attempt stays in finishing state for too long,
  // taskAttemptFinishingMonitor will notify the attempt via TA_TIMED_OUT
  // event.
  private TaskAttemptFinishingMonitor taskAttemptFinishingMonitor;

  private Job job;
  private Credentials jobCredentials = new Credentials(); // Filled during init
  protected UserGroupInformation currentUser; // Will be setup during init

  @VisibleForTesting
  protected volatile boolean isLastAMRetry = false;
  //Something happened and we should shut down right after we start up.
  boolean errorHappenedShutDown = false;
  private String shutDownMessage = null;
  JobStateInternal forcedState = null;
  private final ScheduledExecutorService logSyncer;

  private long recoveredJobStartTime = -1L;
  private static boolean mainStarted = false;

  @VisibleForTesting
  protected AtomicBoolean successfullyUnregistered =
      new AtomicBoolean(false);

  public MRAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        SystemClock.getInstance(), appSubmitTime);
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
    logSyncer = TaskLog.createLogSyncer();
    LOG.info("Created MRAppMaster for application " + applicationAttemptId);
  }
  protected TaskAttemptFinishingMonitor createTaskAttemptFinishingMonitor(
      EventHandler eventHandler) {
    TaskAttemptFinishingMonitor monitor =
        new TaskAttemptFinishingMonitor(eventHandler);
    return monitor;
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    // create the job classloader if enabled
    createJobClassLoader(conf);

    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    initJobCredentialsAndUGI(conf);

    dispatcher = createDispatcher();
    addIfService(dispatcher);
    taskAttemptFinishingMonitor = createTaskAttemptFinishingMonitor(dispatcher.getEventHandler());
    addIfService(taskAttemptFinishingMonitor);
    context = new RunningAppContext(conf, taskAttemptFinishingMonitor);

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
    committer = createOutputCommitter(conf);
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
        LOG.info("Attempt num: " + appAttemptID.getAttemptId() +
            " is last retry: " + isLastAMRetry +
            " because the staging dir doesn't exist.");
        errorHappenedShutDown = true;
        forcedState = JobStateInternal.ERROR;
        shutDownMessage = "Staging dir does not exist " + stagingDir;
        LOG.fatal(shutDownMessage);
      } else if (commitStarted) {
        //A commit was started so this is the last time, we just need to know
        // what result we will use to notify, and how we will unregister
        errorHappenedShutDown = true;
        isLastAMRetry = true;
        LOG.info("Attempt num: " + appAttemptID.getAttemptId() +
            " is last retry: " + isLastAMRetry +
            " because a commit was started.");
        copyHistory = true;
        if (commitSuccess) {
          shutDownMessage =
              "Job commit succeeded in a prior MRAppMaster attempt " +
              "before it crashed. Recovering.";
          forcedState = JobStateInternal.SUCCEEDED;
        } else if (commitFailure) {
          shutDownMessage =
              "Job commit failed in a prior MRAppMaster attempt " +
              "before it crashed. Not retrying.";
          forcedState = JobStateInternal.FAILED;
        } else {
          if (isCommitJobRepeatable()) {
            // cleanup previous half done commits if committer supports
            // repeatable job commit.
            errorHappenedShutDown = false;
            cleanupInterruptedCommit(conf, fs, startCommitFile);
          } else {
            //The commit is still pending, commit error
            shutDownMessage =
                "Job commit from a prior MRAppMaster attempt is " +
                "potentially in progress. Preventing multiple commit executions";
            forcedState = JobStateInternal.ERROR;
          }
        }
      }
    } catch (IOException e) {
      throw new YarnRuntimeException("Error while initializing", e);
    }

    if (errorHappenedShutDown) {
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

      if (copyHistory) {
        // Now that there's a FINISHING state for application on RM to give AMs
        // plenty of time to clean up after unregister it's safe to clean staging
        // directory after unregistering with RM. So, we start the staging-dir
        // cleaner BEFORE the ContainerAllocator so that on shut-down,
        // ContainerAllocator unregisters first and then the staging-dir cleaner
        // deletes staging directory.
        addService(createStagingDirCleaningService());
      }

      // service to allocate containers from RM (if non-uber) or to fake it (uber)
      containerAllocator = createContainerAllocator(null, context);
      addIfService(containerAllocator);
      dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);

      if (copyHistory) {
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

      //service to handle requests from JobClient
      clientService = createClientService(context);
      // Init ClientService separately so that we stop it separately, since this
      // service needs to wait some time before it stops so clients can know the
      // final states
      clientService.init(conf);
      
      containerAllocator = createContainerAllocator(clientService, context);
      
      //service to handle the output committer
      committerEventHandler = createCommitterEventHandler(context, committer);
      addIfService(committerEventHandler);

      //policy handling preemption requests from RM
      callWithJobClassLoader(conf, new Action<Void>() {
        public Void call(Configuration conf) {
          preemptionPolicy = createPreemptionPolicy(conf);
          preemptionPolicy.init(context);
          return null;
        }
      });

      //service to handle requests to TaskUmbilicalProtocol
      taskAttemptListener = createTaskAttemptListener(context, preemptionPolicy);
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

      // Now that there's a FINISHING state for application on RM to give AMs
      // plenty of time to clean up after unregister it's safe to clean staging
      // directory after unregistering with RM. So, we start the staging-dir
      // cleaner BEFORE the ContainerAllocator so that on shut-down,
      // ContainerAllocator unregisters first and then the staging-dir cleaner
      // deletes staging directory.
      addService(createStagingDirCleaningService());

      // service to allocate containers from RM (if non-uber) or to fake it (uber)
      addIfService(containerAllocator);
      dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);

      // corresponding service to launch allocated containers via NodeManager
      containerLauncher = createContainerLauncher(context);
      addIfService(containerLauncher);
      dispatcher.register(ContainerLauncher.EventType.class, containerLauncher);

      // Add the JobHistoryEventHandler last so that it is properly stopped first.
      // This will guarantee that all history-events are flushed before AM goes
      // ahead with shutdown.
      // Note: Even though JobHistoryEventHandler is started last, if any
      // component creates a JobHistoryEvent in the meanwhile, it will be just be
      // queued inside the JobHistoryEventHandler 
      addIfService(historyService);
    }
    super.serviceInit(conf);
  } // end of init()
  
  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  private boolean isCommitJobRepeatable() throws IOException {
    boolean isRepeatable = false;
    Configuration conf = getConfig();
    if (committer != null) {
      final JobContext jobContext = getJobContextFromConf(conf);

      isRepeatable = callWithJobClassLoader(conf,
          new ExceptionAction<Boolean>() {
            public Boolean call(Configuration conf) throws IOException {
              return committer.isCommitJobRepeatable(jobContext);
            }
          });
    }
    return isRepeatable;
  }

  private JobContext getJobContextFromConf(Configuration conf) {
    if (newApiCommitter) {
      return new JobContextImpl(conf, TypeConverter.fromYarn(getJobId()));
    } else {
      return new org.apache.hadoop.mapred.JobContextImpl(
          new JobConf(conf), TypeConverter.fromYarn(getJobId()));
    }
  }

  private void cleanupInterruptedCommit(Configuration conf,
      FileSystem fs, Path startCommitFile) throws IOException {
    LOG.info("Delete startJobCommitFile in case commit is not finished as " +
        "successful or failed.");
    fs.delete(startCommitFile, false);
  }

  private OutputCommitter createOutputCommitter(Configuration conf) {
    return callWithJobClassLoader(conf, new Action<OutputCommitter>() {
      public OutputCommitter call(Configuration conf) {
        OutputCommitter committer = null;

        LOG.info("OutputCommitter set in config "
            + conf.get("mapred.output.committer.class"));

        if (newApiCommitter) {
          org.apache.hadoop.mapreduce.v2.api.records.TaskId taskID =
              MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
          org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
              MRBuilderUtils.newTaskAttemptId(taskID, 0);
          TaskAttemptContext taskContext = new TaskAttemptContextImpl(conf,
              TypeConverter.fromYarn(attemptID));
          OutputFormat outputFormat;
          try {
            outputFormat = ReflectionUtils.newInstance(taskContext
                .getOutputFormatClass(), conf);
            committer = outputFormat.getOutputCommitter(taskContext);
          } catch (Exception e) {
            throw new YarnRuntimeException(e);
          }
        } else {
          committer = ReflectionUtils.newInstance(conf.getClass(
              "mapred.output.committer.class", FileOutputCommitter.class,
              org.apache.hadoop.mapred.OutputCommitter.class), conf);
        }
        LOG.info("OutputCommitter is " + committer.getClass().getName());
        return committer;
      }
    });
  }

  protected AMPreemptionPolicy createPreemptionPolicy(Configuration conf) {
    return ReflectionUtils.newInstance(conf.getClass(
          MRJobConfig.MR_AM_PREEMPTION_POLICY,
          NoopAMPreemptionPolicy.class, AMPreemptionPolicy.class), conf);
  }

  private boolean isJobNamePatternMatch(JobConf conf, String jobTempDir) {
    // Matched staging files should be preserved after job is finished.
    if (conf.getKeepTaskFilesPattern() != null && jobTempDir != null) {
      String jobFileName = Paths.get(jobTempDir).getFileName().toString();
      Pattern pattern = Pattern.compile(conf.getKeepTaskFilesPattern());
      Matcher matcher = pattern.matcher(jobFileName);
      return matcher.find();
    } else {
      return false;
    }
  }

  private boolean isKeepFailedTaskFiles(JobConf conf) {
    // TODO: Decide which failed task files that should
    // be kept are in application log directory.
    return conf.getKeepFailedTaskFiles();
  }

  protected boolean keepJobFiles(JobConf conf, String jobTempDir) {
    return isJobNamePatternMatch(conf, jobTempDir)
            || isKeepFailedTaskFiles(conf);
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

  protected Credentials getCredentials() {
    return jobCredentials;
  }

  /**
   * clean up staging directories for the job.
   * @throws IOException
   */
  public void cleanupStagingDir() throws IOException {
    /* make sure we clean the staging files */
    String jobTempDir = getConfig().get(MRJobConfig.MAPREDUCE_JOB_DIR);
    FileSystem fs = getFileSystem(getConfig());
    try {
      if (!keepJobFiles(new JobConf(getConfig()), jobTempDir)) {
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

    try {
      //if isLastAMRetry comes as true, should never set it to false
      if ( !isLastAMRetry){
        if (((JobImpl)job).getInternalState() != JobStateInternal.REBOOT) {
          LOG.info("Job finished cleanly, recording last MRAppMaster retry");
          isLastAMRetry = true;
        }
      }
      notifyIsLastAMRetry(isLastAMRetry);
      // Stop all services
      // This will also send the final report to the ResourceManager
      LOG.info("Calling stop for all the services");
      MRAppMaster.this.stop();

      if (isLastAMRetry) {
        // Send job-end notification when it is safe to report termination to
        // users and it is the last AM retry
        if (getConfig().get(MRJobConfig.MR_JOB_END_NOTIFICATION_URL) != null) {
          try {
            LOG.info("Job end notification started for jobID : "
                + job.getReport().getJobId());
            JobEndNotifier notifier = new JobEndNotifier();
            notifier.setConf(getConfig());
            JobReport report = job.getReport();
            // If unregistration fails, the final state is unavailable. However,
            // at the last AM Retry, the client will finally be notified FAILED
            // from RM, so we should let users know FAILED via notifier as well
            if (!context.hasSuccessfullyUnregistered()) {
              report.setJobState(JobState.FAILED);
            }
            notifier.notify(report);
          } catch (InterruptedException ie) {
            LOG.warn("Job end notification interrupted for jobID : "
                + job.getReport().getJobId(), ie);
          }
        }
      }

      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      clientService.stop();
    } catch (Throwable t) {
      LOG.warn("Graceful stop failed. Exiting.. ", t);
      exitMRAppMaster(1, t);
    }
    exitMRAppMaster(0, null);
  }

  /** MRAppMaster exit method which has been instrumented for both runtime and
   *  unit testing.
   * If the main thread has not been started, this method was called from a
   * test. In that case, configure the ExitUtil object to not exit the JVM.
   *
   * @param status integer indicating exit status
   * @param t throwable exception that could be null
   */
  private void exitMRAppMaster(int status, Throwable t) {
    if (!mainStarted) {
      ExitUtil.disableSystemExit();
    }
    try {
      if (t != null) {
        ExitUtil.terminate(status, t);
      } else {
        ExitUtil.terminate(status);
      }
    } catch (ExitUtil.ExitException ee) {
      // ExitUtil.ExitException is only thrown from the ExitUtil test code when
      // SystemExit has been disabled. It is always thrown in in the test code,
      // even when no error occurs. Ignore the exception so that tests don't
      // need to handle it.
    }
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

  /** Create and initialize (but don't start) a single job. 
   * @param forcedState a state to force the job into or null for normal operation. 
   * @param diagnostic a diagnostic message to include with the job.
   */
  protected Job createJob(Configuration conf, JobStateInternal forcedState, 
      String diagnostic) {

    // create single job
    Job newJob =
        new JobImpl(jobId, appAttemptID, conf, dispatcher.getEventHandler(),
            taskAttemptListener, jobTokenSecretManager, jobCredentials, clock,
            completedTasksFromPreviousRun, metrics,
            committer, newApiCommitter,
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
  protected void initJobCredentialsAndUGI(Configuration conf) {

    try {
      this.currentUser = UserGroupInformation.getCurrentUser();
      this.jobCredentials = ((JobConf)conf).getCredentials();
      if (CryptoUtils.isEncryptedSpillEnabled(conf)) {
        int keyLen = conf.getInt(
                MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS,
                MRJobConfig
                        .DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS);
        KeyGenerator keyGen =
                KeyGenerator.getInstance(INTERMEDIATE_DATA_ENCRYPTION_ALGO);
        keyGen.init(keyLen);
        encryptedSpillKey = keyGen.generateKey().getEncoded();
      } else {
        encryptedSpillKey = new byte[] {0};
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new YarnRuntimeException(e);
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

  protected Speculator createSpeculator(Configuration conf,
      final AppContext context) {
    return callWithJobClassLoader(conf, new Action<Speculator>() {
      public Speculator call(Configuration conf) {
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
          throw new YarnRuntimeException(ex);
        } catch (IllegalAccessException ex) {
          LOG.error("Can't make a speculator -- check "
              + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
          throw new YarnRuntimeException(ex);
        } catch (InvocationTargetException ex) {
          LOG.error("Can't make a speculator -- check "
              + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
          throw new YarnRuntimeException(ex);
        } catch (NoSuchMethodException ex) {
          LOG.error("Can't make a speculator -- check "
              + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
          throw new YarnRuntimeException(ex);
        }
      }
    });
  }

  protected TaskAttemptListener createTaskAttemptListener(AppContext context,
      AMPreemptionPolicy preemptionPolicy) {
    TaskAttemptListener lis =
        new TaskAttemptListenerImpl(context, jobTokenSecretManager,
            getRMHeartbeatHandler(), preemptionPolicy, encryptedSpillKey);
    return lis;
  }

  protected EventHandler<CommitterEvent> createCommitterEventHandler(
      AppContext context, OutputCommitter committer) {
    return new CommitterEventHandler(context, committer,
        getRMHeartbeatHandler(), jobClassLoader);
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

  public Boolean isLastAMRetry() {
    return isLastAMRetry;
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
    protected void serviceStart() throws Exception {
      if (job.isUber()) {
        MRApps.setupDistributedCacheLocal(getConfig());
        this.containerAllocator = new LocalContainerAllocator(
            this.clientService, this.context, nmHost, nmPort, nmHttpPort
            , containerID);
      } else {
        this.containerAllocator = new RMContainerAllocator(
            this.clientService, this.context, preemptionPolicy);
      }
      ((Service)this.containerAllocator).init(getConfig());
      ((Service)this.containerAllocator).start();
      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      ServiceOperations.stop((Service) this.containerAllocator);
      super.serviceStop();
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
    protected void serviceStart() throws Exception {
      if (job.isUber()) {
        this.containerLauncher = new LocalContainerLauncher(context,
            (TaskUmbilicalProtocol) taskAttemptListener, jobClassLoader);
        ((LocalContainerLauncher) this.containerLauncher)
                .setEncryptedSpillKey(encryptedSpillKey);
      } else {
        this.containerLauncher = new ContainerLauncherImpl(context);
      }
      ((Service)this.containerLauncher).init(getConfig());
      ((Service)this.containerLauncher).start();
      super.serviceStart();
    }

    @Override
    public void handle(ContainerLauncherEvent event) {
        this.containerLauncher.handle(event);
    }

    @Override
    protected void serviceStop() throws Exception {
      ServiceOperations.stop((Service) this.containerLauncher);
      super.serviceStop();
    }
  }

  private final class StagingDirCleaningService extends AbstractService {
    StagingDirCleaningService() {
      super(StagingDirCleaningService.class.getName());
    }

    @Override
    protected void serviceStop() throws Exception {
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
      super.serviceStop();
    }
  }

  public class RunningAppContext implements AppContext {

    private final Map<JobId, Job> jobs = new ConcurrentHashMap<JobId, Job>();
    private final Configuration conf;
    private final ClusterInfo clusterInfo = new ClusterInfo();
    private final ClientToAMTokenSecretManager clientToAMTokenSecretManager;

    private final TaskAttemptFinishingMonitor taskAttemptFinishingMonitor;

    public RunningAppContext(Configuration config,
        TaskAttemptFinishingMonitor taskAttemptFinishingMonitor) {
      this.conf = config;
      this.clientToAMTokenSecretManager =
          new ClientToAMTokenSecretManager(appAttemptID, null);
      this.taskAttemptFinishingMonitor = taskAttemptFinishingMonitor;
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

    @Override
    public Set<String> getBlacklistedNodes() {
      return ((RMContainerRequestor) containerAllocator).getBlacklistedNodes();
    }
    
    @Override
    public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
      return clientToAMTokenSecretManager;
    }

    @Override
    public boolean isLastAMRetry(){
      return isLastAMRetry;
    }

    @Override
    public boolean hasSuccessfullyUnregistered() {
      return successfullyUnregistered.get();
    }

    public void markSuccessfulUnregistration() {
      successfullyUnregistered.set(true);
    }

    public void resetIsLastAMRetry() {
      isLastAMRetry = false;
    }

    @Override
    public String getNMHostname() {
      return nmHost;
    }

    @Override
    public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
      return taskAttemptFinishingMonitor;
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  protected void serviceStart() throws Exception {

    amInfos = new LinkedList<AMInfo>();
    completedTasksFromPreviousRun = new HashMap<TaskId, TaskInfo>();
    processRecovery();

    // Current an AMInfo for the current AM generation.
    AMInfo amInfo =
        MRBuilderUtils.newAMInfo(appAttemptID, startTime, containerID, nmHost,
            nmPort, nmHttpPort);

    // /////////////////// Create the job itself.
    job = createJob(getConfig(), forcedState, shutDownMessage);

    // End of creating the job.

    // Send out an MR AM inited event for all previous AMs.
    for (AMInfo info : amInfos) {
      dispatcher.getEventHandler().handle(
          new JobHistoryEvent(job.getID(), new AMStartedEvent(info
              .getAppAttemptId(), info.getStartTime(), info.getContainerId(),
              info.getNodeManagerHost(), info.getNodeManagerPort(), info
                  .getNodeManagerHttpPort(), appSubmitTime)));
    }

    // Send out an MR AM inited event for this AM.
    dispatcher.getEventHandler().handle(
        new JobHistoryEvent(job.getID(), new AMStartedEvent(amInfo
            .getAppAttemptId(), amInfo.getStartTime(), amInfo.getContainerId(),
            amInfo.getNodeManagerHost(), amInfo.getNodeManagerPort(), amInfo
                .getNodeManagerHttpPort(), this.forcedState == null ? null
                    : this.forcedState.toString(), appSubmitTime)));
    amInfos.add(amInfo);

    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("MRAppMaster");

    boolean initFailed = false;
    if (!errorHappenedShutDown) {
      // create a job event for job intialization
      JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
      // Send init to the job (this does NOT trigger job execution)
      // This is a synchronous call, not an event through dispatcher. We want
      // job-init to be done completely here.
      jobEventDispatcher.handle(initJobEvent);

      // If job is still not initialized, an error happened during
      // initialization. Must complete starting all of the services so failure
      // events can be processed.
      initFailed = (((JobImpl)job).getInternalState() != JobStateInternal.INITED);

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
      // Start ClientService here, since it's not initialized if
      // errorHappenedShutDown is true
      clientService.start();
    }
    //start all the components
    super.serviceStart();

    // finally set the job classloader
    MRApps.setClassLoader(jobClassLoader, getConfig());
    // set job classloader if configured
    Limits.init(getConfig());

    if (initFailed) {
      JobEvent initFailedEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT_FAILED);
      jobEventDispatcher.handle(initFailedEvent);
    } else {
      // All components have started, start the job.
      startJobs();
    }
  }

  protected void shutdownTaskLog() {
    TaskLog.syncLogsShutdown(logSyncer);
  }

  @Override
  public void stop() {
    super.stop();
    shutdownTaskLog();
  }

  private boolean isRecoverySupported() throws IOException {
    boolean isSupported = false;
    Configuration conf = getConfig();
    if (committer != null) {
      final JobContext _jobContext = getJobContextFromConf(conf);
      isSupported = callWithJobClassLoader(conf,
          new ExceptionAction<Boolean>() {
            public Boolean call(Configuration conf) throws IOException {
              return committer.isRecoverySupported(_jobContext);
            }
      });
    }
    return isSupported;
  }

  private void processRecovery() throws IOException{
    if (appAttemptID.getAttemptId() == 1) {
      return;  // no need to recover on the first attempt
    }

    boolean recoveryEnabled = getConfig().getBoolean(
        MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE,
        MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE_DEFAULT);

    boolean recoverySupportedByCommitter = isRecoverySupported();

    // If a shuffle secret was not provided by the job client then this app
    // attempt will generate one.  However that disables recovery if there
    // are reducers as the shuffle secret would be app attempt specific.
    int numReduceTasks = getConfig().getInt(MRJobConfig.NUM_REDUCES, 0);
    boolean shuffleKeyValidForRecovery =
        TokenCache.getShuffleSecretKey(jobCredentials) != null;

    if (recoveryEnabled && recoverySupportedByCommitter
        && (numReduceTasks <= 0 || shuffleKeyValidForRecovery)) {
      LOG.info("Recovery is enabled. "
          + "Will try to recover from previous life on best effort basis.");
      try {
        parsePreviousJobHistory();
      } catch (IOException e) {
        LOG.warn("Unable to parse prior job history, aborting recovery", e);
        // try to get just the AMInfos
        amInfos.addAll(readJustAMInfos());
      }
    } else {
      LOG.info("Will not try to recover. recoveryEnabled: "
            + recoveryEnabled + " recoverySupportedByCommitter: "
            + recoverySupportedByCommitter + " numReduceTasks: "
            + numReduceTasks + " shuffleKeyValidForRecovery: "
            + shuffleKeyValidForRecovery + " ApplicationAttemptID: "
            + appAttemptID.getAttemptId());
      // Get the amInfos anyways whether recovery is enabled or not
      amInfos.addAll(readJustAMInfos());
    }
  }

  private static FSDataInputStream getPreviousJobHistoryStream(
      Configuration conf, ApplicationAttemptId appAttemptId)
      throws IOException {
    Path historyFile = JobHistoryUtils.getPreviousJobHistoryPath(conf,
        appAttemptId);
    LOG.info("Previous history file is at " + historyFile);
    return historyFile.getFileSystem(conf).open(historyFile);
  }

  private void parsePreviousJobHistory() throws IOException {
    FSDataInputStream in = getPreviousJobHistoryStream(getConfig(),
        appAttemptID);
    JobHistoryParser parser = new JobHistoryParser(in);
    JobInfo jobInfo = parser.parse();
    Exception parseException = parser.getParseException();
    if (parseException != null) {
      LOG.info("Got an error parsing job-history file" +
          ", ignoring incomplete events.", parseException);
    }
    Map<org.apache.hadoop.mapreduce.TaskID, TaskInfo> taskInfos = jobInfo
        .getAllTasks();
    for (TaskInfo taskInfo : taskInfos.values()) {
      if (TaskState.SUCCEEDED.toString().equals(taskInfo.getTaskStatus())) {
        Iterator<Entry<TaskAttemptID, TaskAttemptInfo>> taskAttemptIterator =
            taskInfo.getAllTaskAttempts().entrySet().iterator();
        while (taskAttemptIterator.hasNext()) {
          Map.Entry<TaskAttemptID, TaskAttemptInfo> currentEntry = taskAttemptIterator.next();
          if (!jobInfo.getAllCompletedTaskAttempts().containsKey(currentEntry.getKey())) {
            taskAttemptIterator.remove();
          }
        }
        completedTasksFromPreviousRun
            .put(TypeConverter.toYarn(taskInfo.getTaskId()), taskInfo);
        LOG.info("Read from history task "
            + TypeConverter.toYarn(taskInfo.getTaskId()));
      }
    }
    LOG.info("Read completed tasks from history "
        + completedTasksFromPreviousRun.size());
    recoveredJobStartTime = jobInfo.getLaunchTime();

    // recover AMInfos
    List<JobHistoryParser.AMInfo> jhAmInfoList = jobInfo.getAMInfos();
    if (jhAmInfoList != null) {
      for (JobHistoryParser.AMInfo jhAmInfo : jhAmInfoList) {
        AMInfo amInfo = MRBuilderUtils.newAMInfo(jhAmInfo.getAppAttemptId(),
            jhAmInfo.getStartTime(), jhAmInfo.getContainerId(),
            jhAmInfo.getNodeManagerHost(), jhAmInfo.getNodeManagerPort(),
            jhAmInfo.getNodeManagerHttpPort());
        amInfos.add(amInfo);
      }
    }
  }

  private List<AMInfo> readJustAMInfos() {
    List<AMInfo> amInfos = new ArrayList<AMInfo>();
    FSDataInputStream inputStream = null;
    try {
      inputStream = getPreviousJobHistoryStream(getConfig(), appAttemptID);
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
    JobEvent startJobEvent = new JobStartEvent(job.getID(),
        recoveredJobStartTime);
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
    public void handle(final SpeculatorEvent event) {
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
        callWithJobClassLoader(conf, new Action<Void>() {
          public Void call(Configuration conf) {
            speculator.handle(event);
            return null;
          }
        });
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
      mainStarted = true;
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      String containerIdStr =
          System.getenv(Environment.CONTAINER_ID.name());
      String nodeHostString = System.getenv(Environment.NM_HOST.name());
      String nodePortString = System.getenv(Environment.NM_PORT.name());
      String nodeHttpPortString =
          System.getenv(Environment.NM_HTTP_PORT.name());
      String appSubmitTimeStr =
          System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      
      validateInputParam(containerIdStr,
          Environment.CONTAINER_ID.name());
      validateInputParam(nodeHostString, Environment.NM_HOST.name());
      validateInputParam(nodePortString, Environment.NM_PORT.name());
      validateInputParam(nodeHttpPortString,
          Environment.NM_HTTP_PORT.name());
      validateInputParam(appSubmitTimeStr,
          ApplicationConstants.APP_SUBMIT_TIME_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId =
          containerId.getApplicationAttemptId();
      if (applicationAttemptId != null) {
        CallerContext.setCurrent(new CallerContext.Builder(
            "mr_appmaster_" + applicationAttemptId.toString()).build());
      }
      long appSubmitTime = Long.parseLong(appSubmitTimeStr);
      
      
      MRAppMaster appMaster =
          new MRAppMaster(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), appSubmitTime);
      ShutdownHookManager.get().addShutdownHook(
        new MRAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);
      JobConf conf = new JobConf(new YarnConfiguration());
      conf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
      
      MRWebAppUtil.initialize(conf);
      // log the system properties
      String systemPropsToLog = MRApps.getSystemPropertiesToLog(conf);
      if (systemPropsToLog != null) {
        LOG.info(systemPropsToLog);
      }

      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());
      conf.set(MRJobConfig.USER_NAME, jobUserName);
      initAndStartAppMaster(appMaster, conf, jobUserName);
    } catch (Throwable t) {
      LOG.fatal("Error starting MRAppMaster", t);
      ExitUtil.terminate(1, t);
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
      }
      appMaster.notifyIsLastAMRetry(appMaster.isLastAMRetry);
      appMaster.stop();
    }
  }

  public void notifyIsLastAMRetry(boolean isLastAMRetry){
    if(containerAllocator instanceof ContainerAllocatorRouter) {
      LOG.info("Notify RMCommunicator isAMLastRetry: " + isLastAMRetry);
      ((ContainerAllocatorRouter) containerAllocator)
        .setShouldUnregister(isLastAMRetry);
    }
    if(jobHistoryEventHandler != null) {
      LOG.info("Notify JHEH isAMLastRetry: " + isLastAMRetry);
      jobHistoryEventHandler.setForcejobCompletion(isLastAMRetry);
    }
  }

  protected static void initAndStartAppMaster(final MRAppMaster appMaster,
      final JobConf conf, String jobUserName) throws IOException,
      InterruptedException {
    UserGroupInformation.setConfiguration(conf);
    // Security framework already loaded the tokens into current UGI, just use
    // them
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    LOG.info("Executing with tokens:");
    for (Token<?> token : credentials.getAllTokens()) {
      LOG.info(token);
    }
    
    UserGroupInformation appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
    appMasterUgi.addCredentials(credentials);

    // Now remove the AM->RM token so tasks don't have it
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    conf.getCredentials().addAll(credentials);
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

  /**
   * Creates a job classloader based on the configuration if the job classloader
   * is enabled. It is a no-op if the job classloader is not enabled.
   */
  private void createJobClassLoader(Configuration conf) throws IOException {
    jobClassLoader = MRApps.createJobClassLoader(conf);
  }

  /**
   * Executes the given action with the job classloader set as the configuration
   * classloader as well as the thread context class loader if the job
   * classloader is enabled. After the call, the original classloader is
   * restored.
   *
   * If the job classloader is enabled and the code needs to load user-supplied
   * classes via configuration or thread context classloader, this method should
   * be used in order to load them.
   *
   * @param conf the configuration on which the classloader will be set
   * @param action the callable action to be executed
   */
  <T> T callWithJobClassLoader(Configuration conf, Action<T> action) {
    // if the job classloader is enabled, we may need it to load the (custom)
    // classes; we make the job classloader available and unset it once it is
    // done
    ClassLoader currentClassLoader = conf.getClassLoader();
    boolean setJobClassLoader =
        jobClassLoader != null && currentClassLoader != jobClassLoader;
    if (setJobClassLoader) {
      MRApps.setClassLoader(jobClassLoader, conf);
    }
    try {
      return action.call(conf);
    } finally {
      if (setJobClassLoader) {
        // restore the original classloader
        MRApps.setClassLoader(currentClassLoader, conf);
      }
    }
  }

  /**
   * Executes the given action that can throw a checked exception with the job
   * classloader set as the configuration classloader as well as the thread
   * context class loader if the job classloader is enabled. After the call, the
   * original classloader is restored.
   *
   * If the job classloader is enabled and the code needs to load user-supplied
   * classes via configuration or thread context classloader, this method should
   * be used in order to load them.
   *
   * @param conf the configuration on which the classloader will be set
   * @param action the callable action to be executed
   * @throws IOException if the underlying action throws an IOException
   * @throws YarnRuntimeException if the underlying action throws an exception
   * other than an IOException
   */
  <T> T callWithJobClassLoader(Configuration conf, ExceptionAction<T> action)
      throws IOException {
    // if the job classloader is enabled, we may need it to load the (custom)
    // classes; we make the job classloader available and unset it once it is
    // done
    ClassLoader currentClassLoader = conf.getClassLoader();
    boolean setJobClassLoader =
        jobClassLoader != null && currentClassLoader != jobClassLoader;
    if (setJobClassLoader) {
      MRApps.setClassLoader(jobClassLoader, conf);
    }
    try {
      return action.call(conf);
    } catch (IOException e) {
      throw e;
    } catch (YarnRuntimeException e) {
      throw e;
    } catch (Exception e) {
      // wrap it with a YarnRuntimeException
      throw new YarnRuntimeException(e);
    } finally {
      if (setJobClassLoader) {
        // restore the original classloader
        MRApps.setClassLoader(currentClassLoader, conf);
      }
    }
  }

  /**
   * Action to be wrapped with setting and unsetting the job classloader
   */
  private static interface Action<T> {
    T call(Configuration conf);
  }

  private static interface ExceptionAction<T> {
    T call(Configuration conf) throws Exception;
  }

  protected void shutdownLogManager() {
    LogManager.shutdown();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    shutdownLogManager();
  }

  public ClientService getClientService() {
    return clientService;
  }
}
