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
import org.apache.hadoop.mapreduce.v2.MRConstants;
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
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherImpl;
import org.apache.hadoop.mapreduce.v2.app.local.LocalContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.recover.Recovery;
import org.apache.hadoop.mapreduce.v2.app.recover.RecoveryService;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;

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
  private final int startCount;
  private final ApplicationId appID;
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

  private Job job;
  
  public MRAppMaster(ApplicationId applicationId, int startCount) {
    this(applicationId, new SystemClock(), startCount);
  }

  public MRAppMaster(ApplicationId applicationId, Clock clock, int startCount) {
    super(MRAppMaster.class.getName());
    this.clock = clock;
    this.appID = applicationId;
    this.appAttemptID = RecordFactoryProvider.getRecordFactory(null)
        .newRecordInstance(ApplicationAttemptId.class);
    this.appAttemptID.setApplicationId(appID);
    this.appAttemptID.setAttemptId(startCount);
    this.startCount = startCount;
    this.metrics = MRAppMetrics.create();
    LOG.info("Created MRAppMaster for application " + applicationId);
  }

  @Override
  public void init(final Configuration conf) {
    context = new RunningAppContext();

    // Job name is the same as the app name util we support DAG of jobs
    // for an app later
    appName = conf.get(MRJobConfig.JOB_NAME, "<missing app name>");

    if (conf.getBoolean(AMConstants.RECOVERY_ENABLE, false)
         && startCount > 1) {
      LOG.info("Recovery is enabled. Will try to recover from previous life.");
      Recovery recoveryServ = new RecoveryService(appID, clock, startCount);
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
    addIfService(historyService);

    JobEventDispatcher synchronousJobEventDispatcher = new JobEventDispatcher();

    //register the event dispatchers
    dispatcher.register(JobEventType.class, synchronousJobEventDispatcher);
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class, 
        new TaskAttemptEventDispatcher());
    dispatcher.register(TaskCleaner.EventType.class, taskCleaner);
    dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
        historyService);
    
    if (conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
        || conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
      //optional service to speculate on task attempts' progress
      speculator = createSpeculator(conf, context);
      addIfService(speculator);
    }

    dispatcher.register(Speculator.EventType.class,
        new SpeculatorEventDispatcher());

    Credentials fsTokens = new Credentials();

    UserGroupInformation currentUser = null;

    try {
      currentUser = UserGroupInformation.getCurrentUser();

      if (UserGroupInformation.isSecurityEnabled()) {
        // Read the file-system tokens from the localized tokens-file.
        Path jobSubmitDir = 
            FileContext.getLocalFSFileContext().makeQualified(
                new Path(new File(MRConstants.JOB_SUBMIT_DIR)
                    .getAbsolutePath()));
        Path jobTokenFile = 
            new Path(jobSubmitDir, MRConstants.APPLICATION_TOKENS_FILE);
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

    super.init(conf);

    //---- start of what used to be startJobs() code:

    Configuration config = getConfig();

    job = createJob(config, fsTokens, currentUser.getUserName());

    /** create a job event for job intialization */
    JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
    /** send init to the job (this does NOT trigger job execution) */
    synchronousJobEventDispatcher.handle(initJobEvent);

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

    // service to allocate containers from RM (if non-uber) or to fake it (uber)
    containerAllocator =
        createContainerAllocator(clientService, context, job.isUber());
    addIfService(containerAllocator);
    dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);
    if (containerAllocator instanceof Service) {
      ((Service) containerAllocator).init(config);
    }

    // corresponding service to launch allocated containers via NodeManager
    containerLauncher = createContainerLauncher(context, job.isUber());
    addIfService(containerLauncher);
    dispatcher.register(ContainerLauncher.EventType.class, containerLauncher);
    if (containerLauncher instanceof Service) {
      ((Service) containerLauncher).init(config);
    }

  } // end of init()

  /** Create and initialize (but don't start) a single job. 
   * @param fsTokens */
  protected Job createJob(Configuration conf, Credentials fsTokens, 
      String user) {

    // create single job
    Job newJob = new JobImpl(appID, conf, dispatcher.getEventHandler(),
        taskAttemptListener, jobTokenSecretManager, fsTokens, clock, startCount,
        completedTasksFromPreviousRun, metrics, user);
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
          = conf.getClass(AMConstants.SPECULATOR_CLASS,
                          DefaultSpeculator.class,
                          Speculator.class);
      Constructor<? extends Speculator> speculatorConstructor
          = speculatorClass.getConstructor
               (Configuration.class, AppContext.class);
      Speculator result = speculatorConstructor.newInstance(conf, context);

      return result;
    } catch (InstantiationException ex) {
      LOG.error("Can't make a speculator -- check "
          + AMConstants.SPECULATOR_CLASS + " " + ex);
      throw new YarnException(ex);
    } catch (IllegalAccessException ex) {
      LOG.error("Can't make a speculator -- check "
          + AMConstants.SPECULATOR_CLASS + " " + ex);
      throw new YarnException(ex);
    } catch (InvocationTargetException ex) {
      LOG.error("Can't make a speculator -- check "
          + AMConstants.SPECULATOR_CLASS + " " + ex);
      throw new YarnException(ex);
    } catch (NoSuchMethodException ex) {
      LOG.error("Can't make a speculator -- check "
          + AMConstants.SPECULATOR_CLASS + " " + ex);
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
      ClientService clientService, AppContext context, boolean isLocal) {
    //return new StaticContainerAllocator(context);
    return isLocal
        ? new LocalContainerAllocator(clientService, context)
        : new RMContainerAllocator(clientService, context);
  }

  protected ContainerLauncher createContainerLauncher(AppContext context,
                                                      boolean isLocal) {
    return isLocal
        ? new LocalContainerLauncher(context,
            (TaskUmbilicalProtocol) taskAttemptListener)
        : new ContainerLauncherImpl(context);
  }

  //TODO:should have an interface for MRClientService
  protected ClientService createClientService(AppContext context) {
    return new MRClientService(context);
  }

  public ApplicationId getAppID() {
    return appID;
  }

  public int getStartCount() {
    return startCount;
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

  class RunningAppContext implements AppContext {

    private Map<JobId, Job> jobs = new ConcurrentHashMap<JobId, Job>();

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appID;
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
      return getConfig().get(MRJobConfig.USER_NAME);
    }

    @Override
    public Clock getClock() {
      return clock;
    }
  }

  @Override
  public void start() {
    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("MRAppMaster");

    startJobs();
    //start all the components
    super.start();
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
    @Override
    public void handle(SpeculatorEvent event) {
      if (getConfig().getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
          || getConfig().getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
        // Speculator IS enabled, direct the event to there.
        speculator.handle(event);
      }
    }
  }

  public static void main(String[] args) {
    try {
      //Configuration.addDefaultResource("job.xml");
      ApplicationId applicationId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class);
      
      applicationId.setClusterTimestamp(Long.valueOf(args[0]));
      applicationId.setId(Integer.valueOf(args[1]));
      int failCount = Integer.valueOf(args[2]);
      MRAppMaster appMaster = new MRAppMaster(applicationId, failCount);
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      conf.addResource(new Path(MRConstants.JOB_CONF_FILE));
      conf.set(MRJobConfig.USER_NAME, 
          System.getProperty("user.name")); 
      UserGroupInformation.setConfiguration(conf);
      appMaster.init(conf);
      appMaster.start();
    } catch (Throwable t) {
      LOG.error("Caught throwable. Exiting:", t);
      System.exit(1);
    }
  } 
}
