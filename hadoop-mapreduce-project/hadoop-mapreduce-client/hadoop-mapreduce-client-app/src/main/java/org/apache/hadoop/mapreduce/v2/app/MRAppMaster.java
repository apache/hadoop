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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalContainerLauncher;
import org.apache.hadoop.mapred.TaskAttemptListenerImpl;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
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
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.Speculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleaner;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleanerImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;

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

public class MRAppMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(MRAppMaster.class);

  private Clock clock;
  private final long startTime = System.currentTimeMillis();
  private String appName;
  private final ApplicationAttemptId appAttemptID;
  protected final MRAppMetrics metrics;
  private Set<TaskId> completedTasksFromPreviousRun;
  private AppContext context;
  private Dispatcher dispatcher;
  private ClientService clientService;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private TaskCleaner taskCleaner;
  private Speculator speculator;
  private TaskAttemptListener taskAttemptListener;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private JobEventDispatcher jobEventDispatcher;

  private Job job;
  
  public MRAppMaster(ApplicationAttemptId applicationAttemptId) {
    this(applicationAttemptId, new SystemClock());
  }

  public MRAppMaster(ApplicationAttemptId applicationAttemptId, Clock clock) {
    super(MRAppMaster.class.getName());
    this.clock = clock;
    this.appAttemptID = applicationAttemptId;
    this.metrics = MRAppMetrics.create();
    LOG.info("Created MRAppMaster for application " + applicationAttemptId);
  }

  @Override
  public void init(final Configuration conf) {
    context = new RunningAppContext(conf);

    // Job name is the same as the app name util we support DAG of jobs
    // for an app later
    appName = conf.get(MRJobConfig.JOB_NAME, "<missing app name>");

    if (conf.getBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, false)
         && appAttemptID.getAttemptId() > 1) {
      LOG.info("Recovery is enabled. Will try to recover from previous life.");
      Recovery recoveryServ = new RecoveryService(appAttemptID, clock);
      addIfService(recoveryServ);
      dispatcher = recoveryServ.getDispatcher();
      clock = recoveryServ.getClock();
      completedTasksFromPreviousRun = recoveryServ.getCompletedTasks();
    } else {
      dispatcher = new AsyncDispatcher();
      addIfService(dispatcher);
    }

    //service to handle requests to TaskUmbilicalProtocol
    taskAttemptListener = createTaskAttemptListener(context);
    addIfService(taskAttemptListener);

    //service to do the task cleanup
    taskCleaner = createTaskCleaner(context);
    addIfService(taskCleaner);

    //service to handle requests from JobClient
    clientService = createClientService(context);
    addIfService(clientService);

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
    dispatcher.register(TaskCleaner.EventType.class, taskCleaner);
    
    if (conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
        || conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
      //optional service to speculate on task attempts' progress
      speculator = createSpeculator(conf, context);
      addIfService(speculator);
    }

    dispatcher.register(Speculator.EventType.class,
        new SpeculatorEventDispatcher(conf));

    // service to allocate containers from RM (if non-uber) or to fake it (uber)
    containerAllocator = createContainerAllocator(clientService, context);
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

    super.init(conf);
  } // end of init()

  /** Create and initialize (but don't start) a single job. */
  protected Job createJob(Configuration conf) {

    // ////////// Obtain the tokens needed by the job. //////////
    Credentials fsTokens = new Credentials();
    UserGroupInformation currentUser = null;

    try {
      currentUser = UserGroupInformation.getCurrentUser();

      if (UserGroupInformation.isSecurityEnabled()) {
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

        for (Token<? extends TokenIdentifier> tk : fsTokens.getAllTokens()) {
          LOG.info(" --- DEBUG: Token of kind " + tk.getKind()
              + "in current ugi in the AppMaster for service "
              + tk.getService());
          currentUser.addToken(tk); // For use by AppMaster itself.
        }
      }
    } catch (IOException e) {
      throw new YarnException(e);
    }
    // ////////// End of obtaining the tokens needed by the job. //////////

    // create single job
    Job newJob = new JobImpl(appAttemptID, conf, dispatcher.getEventHandler(),
        taskAttemptListener, jobTokenSecretManager, fsTokens, clock,
        completedTasksFromPreviousRun, metrics, currentUser.getUserName());
    ((RunningAppContext) context).jobs.put(newJob.getID(), newJob);

    dispatcher.register(JobFinishEvent.Type.class,
        new EventHandler<JobFinishEvent>() {
          @Override
          public void handle(JobFinishEvent event) {
            // job has finished
            // this is the only job, so shut down the Appmaster
            // note in a workflow scenario, this may lead to creation of a new
            // job (FIXME?)

            // TODO:currently just wait for some time so clients can know the
            // final states. Will be removed once RM come on.
            try {
              Thread.sleep(5000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            LOG.info("Calling stop for all the services");
            try {
              stop();
            } catch (Throwable t) {
              LOG.warn("Graceful stop failed ", t);
            }
            //TODO: this is required because rpc server does not shut down
            // in spite of calling server.stop().
            //Bring the process down by force.
            //Not needed after HADOOP-7140
            LOG.info("Exiting MR AppMaster..GoodBye!");
            System.exit(0);
          }
        });

    return newJob;
  } // end createJob()

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
      AppContext context) {
    JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context, 
        getStartCount());
    return eventHandler;
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
        new TaskAttemptListenerImpl(context, jobTokenSecretManager);
    return lis;
  }

  protected TaskCleaner createTaskCleaner(AppContext context) {
    return new TaskCleanerImpl(context);
  }

  protected ContainerAllocator createContainerAllocator(
      final ClientService clientService, final AppContext context) {
    return new ContainerAllocatorRouter(clientService, context);
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

  public int getStartCount() {
    return appAttemptID.getAttemptId();
  }

  public AppContext getContext() {
    return context;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public Set<TaskId> getCompletedTaskFromPreviousRun() {
    return completedTasksFromPreviousRun;
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
      implements ContainerAllocator {
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
            this.clientService, this.context);
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

  private class RunningAppContext implements AppContext {

    private final Map<JobId, Job> jobs = new ConcurrentHashMap<JobId, Job>();
    private final Configuration conf;

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
  }

  @Override
  public void start() {

    ///////////////////// Create the job itself.
    job = createJob(getConfig());
    // End of creating the job.

    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("MRAppMaster");

    // create a job event for job intialization
    JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
    // Send init to the job (this does NOT trigger job execution)
    // This is a synchronous call, not an event through dispatcher. We want
    // job-init to be done completely here.
    jobEventDispatcher.handle(initJobEvent);

    // send init to speculator. This won't yest start as dispatcher isn't
    // started yet.
    dispatcher.getEventHandler().handle(
        new SpeculatorEvent(job.getID(), clock.getTime()));

    // JobImpl's InitTransition is done (call above is synchronous), so the
    // "uber-decision" (MR-1220) has been made.  Query job and switch to
    // ubermode if appropriate (by registering different container-allocator
    // and container-launcher services/event-handlers).

    if (job.isUber()) {
      LOG.info("MRAppMaster uberizing job " + job.getID()
               + " in local container (\"uber-AM\").");
    } else {
      LOG.info("MRAppMaster launching normal, non-uberized, multi-container "
               + "job " + job.getID() + ".");
    }

    //start all the components
    super.start();

    // All components have started, start the job.
    startJobs();
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
  protected void startJobs() {
    /** create a job-start event to get this ball rolling */
    JobEvent startJobEvent = new JobEvent(job.getID(), JobEventType.JOB_START);
    /** send the job-start event. this triggers the job execution. */
    dispatcher.getEventHandler().handle(startJobEvent);
  }

  private class JobEventDispatcher implements EventHandler<JobEvent> {
    @Override
    public void handle(JobEvent event) {
      ((EventHandler<JobEvent>)context.getJob(event.getJobId())).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @Override
    public void handle(TaskEvent event) {
      Task task = context.getJob(event.getTaskID().getJobId()).getTask(
          event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher 
          implements EventHandler<TaskAttemptEvent> {
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
    public SpeculatorEventDispatcher(Configuration config) {
      this.conf = config;
    }
    @Override
    public void handle(SpeculatorEvent event) {
      if (conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
          || conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
        // Speculator IS enabled, direct the event to there.
        speculator.handle(event);
      }
    }
  }

  public static void main(String[] args) {
    try {
      String applicationAttemptIdStr = System
          .getenv(ApplicationConstants.APPLICATION_ATTEMPT_ID_ENV);
      if (applicationAttemptIdStr == null) {
        String msg = ApplicationConstants.APPLICATION_ATTEMPT_ID_ENV
            + " is null";
        LOG.error(msg);
        throw new IOException(msg);
      }
      ApplicationAttemptId applicationAttemptId = ConverterUtils
          .toApplicationAttemptId(applicationAttemptIdStr);
      MRAppMaster appMaster = new MRAppMaster(applicationAttemptId);
      Runtime.getRuntime().addShutdownHook(
          new CompositeServiceShutdownHook(appMaster));
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      conf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
      conf.set(MRJobConfig.USER_NAME, 
          System.getProperty("user.name")); 
      UserGroupInformation.setConfiguration(conf);
      appMaster.init(conf);
      appMaster.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting MRAppMaster", t);
      System.exit(1);
    }
  } 
}
