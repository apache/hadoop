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
 package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import javax.crypto.SecretKey;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.TaskController.DebugScriptContext;
import org.apache.hadoop.mapred.TaskController.JobInitializationContext;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.TaskController.TaskControllerPathDeletionContext;
import org.apache.hadoop.mapred.TaskController.TaskControllerTaskPathDeletionContext;
import org.apache.hadoop.mapred.TaskController.TaskControllerJobPathDeletionContext;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleHeader;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.mapreduce.util.MemoryCalculatorPlugin;
import org.apache.hadoop.mapreduce.util.ResourceCalculatorPlugin;
import org.apache.hadoop.mapreduce.util.ProcfsBasedProcessTree;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;

/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 *******************************************************/
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskTracker 
    implements MRConstants, TaskUmbilicalProtocol, Runnable, TTConfig {
  /**
   * @deprecated
   */
  @Deprecated
  static final String MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY =
    "mapred.tasktracker.vmem.reserved";
  /**
   * @deprecated
   */
  @Deprecated
  static final String MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY =
    "mapred.tasktracker.pmem.reserved";


  static final long WAIT_FOR_DONE = 3 * 1000;
  private int httpPort;

  static enum State {NORMAL, STALE, INTERRUPTED, DENIED}

  static{
    ConfigUtil.loadResources();
  }

  public static final Log LOG =
    LogFactory.getLog(TaskTracker.class);

  public static final String MR_CLIENTTRACE_FORMAT =
    "src: %s" +     // src IP
    ", dest: %s" +  // dst IP
    ", maps: %s" + // number of maps
    ", op: %s" +    // operation
    ", reduceID: %s" + // reduce id
    ", duration: %s"; // duration

  public static final Log ClientTraceLog =
    LogFactory.getLog(TaskTracker.class.getName() + ".clienttrace");

  // Job ACLs file is created by TaskTracker under userlogs/$jobid directory for
  // each job at job localization time. This will be used by TaskLogServlet for
  // authorizing viewing of task logs of that job
  static String jobACLsFile = "job-acls.xml";

  volatile boolean running = true;

  private LocalDirAllocator localDirAllocator;
  String taskTrackerName;
  String localHostname;
  InetSocketAddress jobTrackAddr;
    
  InetSocketAddress taskReportAddress;

  Server taskReportServer = null;
  InterTrackerProtocol jobClient;

  private TrackerDistributedCacheManager distributedCacheManager;
    
  // last heartbeat response received
  short heartbeatResponseId = -1;
  
  static final String TASK_CLEANUP_SUFFIX = ".cleanup";

  /*
   * This is the last 'status' report sent by this tracker to the JobTracker.
   * 
   * If the rpc call succeeds, this 'status' is cleared-out by this tracker;
   * indicating that a 'fresh' status report be generated; in the event the
   * rpc calls fails for whatever reason, the previous status report is sent
   * again.
   */
  TaskTrackerStatus status = null;
  
  // The system-directory on HDFS where job files are stored 
  Path systemDirectory = null;
  
  // The filesystem where job files are stored
  FileSystem systemFS = null;
  
  private final HttpServer server;
    
  volatile boolean shuttingDown = false;
    
  Map<TaskAttemptID, TaskInProgress> tasks = new HashMap<TaskAttemptID, TaskInProgress>();
  /**
   * Map from taskId -> TaskInProgress.
   */
  Map<TaskAttemptID, TaskInProgress> runningTasks = null;
  Map<JobID, RunningJob> runningJobs = new TreeMap<JobID, RunningJob>();
  private final JobTokenSecretManager jobTokenSecretManager 
    = new JobTokenSecretManager();

  volatile int mapTotal = 0;
  volatile int reduceTotal = 0;
  boolean justStarted = true;
  boolean justInited = true;
  // Mark reduce tasks that are shuffling to rollback their events index
  Set<TaskAttemptID> shouldReset = new HashSet<TaskAttemptID>();
    
  //dir -> DF
  Map<String, DF> localDirsDf = new HashMap<String, DF>();
  long minSpaceStart = 0;
  //must have this much space free to start new tasks
  boolean acceptNewTasks = true;
  long minSpaceKill = 0;
  //if we run under this limit, kill one task
  //and make sure we never receive any new jobs
  //until all the old tasks have been cleaned up.
  //this is if a machine is so full it's only good
  //for serving map output to the other nodes

  static Random r = new Random();
  public static final String SUBDIR = "taskTracker";
  static final String DISTCACHEDIR = "distcache";
  static final String JOBCACHE = "jobcache";
  static final String OUTPUT = "output";
  private static final String JARSDIR = "jars";
  static final String LOCAL_SPLIT_FILE = "split.dta";
  static final String LOCAL_SPLIT_META_FILE = "split.info";
  static final String JOBFILE = "job.xml";
  static final String JOB_TOKEN_FILE="jobToken"; //localized file

  static final String JOB_LOCAL_DIR = MRJobConfig.JOB_LOCAL_DIR;

  private JobConf fConf;
  private FileSystem localFs;

  private Localizer localizer;

  private int maxMapSlots;
  private int maxReduceSlots;
  private int failures;

  private ACLsManager aclsManager;
  
  // Performance-related config knob to send an out-of-band heartbeat
  // on task completion
  private volatile boolean oobHeartbeatOnTaskCompletion;
  
  // Track number of completed tasks to send an out-of-band heartbeat
  private IntWritable finishedCount = new IntWritable(0);
  
  private MapEventsFetcherThread mapEventsFetcher;
  int workerThreads;
  CleanupQueue directoryCleanupThread;
  private volatile JvmManager jvmManager;
  UserLogCleaner taskLogCleanupThread;
  private TaskMemoryManagerThread taskMemoryManager;
  private boolean taskMemoryManagerEnabled = true;
  private long totalVirtualMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long mapSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long reduceSlotSizeMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;
  private long reservedPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private ResourceCalculatorPlugin resourceCalculatorPlugin = null;

  /**
   * the minimum interval between jobtracker polls
   */
  private volatile int heartbeatInterval =
    JTConfig.JT_HEARTBEAT_INTERVAL_MIN_DEFAULT;
  /**
   * Number of maptask completion events locations to poll for at one time
   */  
  private int probe_sample_size = 500;

  private IndexCache indexCache;

  private MRAsyncDiskService asyncDiskService;
  
  MRAsyncDiskService getAsyncDiskService() {
    return asyncDiskService;
  }

  void setAsyncDiskService(MRAsyncDiskService asyncDiskService) {
    this.asyncDiskService = asyncDiskService;
  }

  /**
  * Handle to the specific instance of the {@link TaskController} class
  */
  private TaskController taskController;
  
  /**
   * Handle to the specific instance of the {@link NodeHealthCheckerService}
   */
  private NodeHealthCheckerService healthChecker;
  
  /*
   * A list of commitTaskActions for whom commit response has been received 
   */
  private List<TaskAttemptID> commitResponses = 
            Collections.synchronizedList(new ArrayList<TaskAttemptID>());

  private ShuffleServerMetrics shuffleServerMetrics;
  /** This class contains the methods that should be used for metrics-reporting
   * the specific metrics for shuffle. The TaskTracker is actually a server for
   * the shuffle and hence the name ShuffleServerMetrics.
   */
  class ShuffleServerMetrics implements Updater {
    private MetricsRecord shuffleMetricsRecord = null;
    private int serverHandlerBusy = 0;
    private long outputBytes = 0;
    private int failedOutputs = 0;
    private int successOutputs = 0;
    private int exceptionsCaught = 0;
    ShuffleServerMetrics(JobConf conf) {
      MetricsContext context = MetricsUtil.getContext("mapred");
      shuffleMetricsRecord = 
                           MetricsUtil.createRecord(context, "shuffleOutput");
      this.shuffleMetricsRecord.setTag("sessionId", conf.getSessionId());
      context.registerUpdater(this);
    }
    synchronized void serverHandlerBusy() {
      ++serverHandlerBusy;
    }
    synchronized void serverHandlerFree() {
      --serverHandlerBusy;
    }
    synchronized void outputBytes(long bytes) {
      outputBytes += bytes;
    }
    synchronized void failedOutput() {
      ++failedOutputs;
    }
    synchronized void successOutput() {
      ++successOutputs;
    }
    synchronized void exceptionsCaught() {
      ++exceptionsCaught;
    }
    public void doUpdates(MetricsContext unused) {
      synchronized (this) {
        if (workerThreads != 0) {
          shuffleMetricsRecord.setMetric("shuffle_handler_busy_percent", 
              100*((float)serverHandlerBusy/workerThreads));
        } else {
          shuffleMetricsRecord.setMetric("shuffle_handler_busy_percent", 0);
        }
        shuffleMetricsRecord.incrMetric("shuffle_output_bytes", 
                                        outputBytes);
        shuffleMetricsRecord.incrMetric("shuffle_failed_outputs", 
                                        failedOutputs);
        shuffleMetricsRecord.incrMetric("shuffle_success_outputs", 
                                        successOutputs);
        shuffleMetricsRecord.incrMetric("shuffle_exceptions_caught",
                                        exceptionsCaught);
        outputBytes = 0;
        failedOutputs = 0;
        successOutputs = 0;
        exceptionsCaught = 0;
      }
      shuffleMetricsRecord.update();
    }
  }
  

  
  
    
  private TaskTrackerInstrumentation myInstrumentation = null;

  public TaskTrackerInstrumentation getTaskTrackerInstrumentation() {
    return myInstrumentation;
  }

  // Currently used only in tests
  void setTaskTrackerInstrumentation(
      TaskTrackerInstrumentation trackerInstrumentation) {
    myInstrumentation = trackerInstrumentation;
  }

  /**
   * A list of tips that should be cleaned up.
   */
  private BlockingQueue<TaskTrackerAction> tasksToCleanup = 
    new LinkedBlockingQueue<TaskTrackerAction>();
    
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  /**
   * A daemon-thread that pulls tips off the list of things to cleanup.
   */
  private Thread taskCleanupThread = 
    new Thread(new Runnable() {
        public void run() {
          while (true) {
            try {
              TaskTrackerAction action = tasksToCleanup.take();
              if (action instanceof KillJobAction) {
                purgeJob((KillJobAction) action);
              } else if (action instanceof KillTaskAction) {
                processKillTaskAction((KillTaskAction) action);
              } else {
                LOG.error("Non-delete action given to cleanup thread: "
                          + action);
              }
            } catch (Throwable except) {
              LOG.warn(StringUtils.stringifyException(except));
            }
          }
        }
      }, "taskCleanup");

  void processKillTaskAction(KillTaskAction killAction) throws IOException {
    TaskInProgress tip;
    synchronized (TaskTracker.this) {
      tip = tasks.get(killAction.getTaskID());
    }
    LOG.info("Received KillTaskAction for task: " + 
             killAction.getTaskID());
    purgeTask(tip, false);
  }
  
  public TaskController getTaskController() {
    return taskController;
  }
  
  // Currently this is used only by tests
  void setTaskController(TaskController t) {
    taskController = t;
  }
  
  private RunningJob addTaskToJob(JobID jobId, 
                                  TaskInProgress tip) {
    synchronized (runningJobs) {
      RunningJob rJob = null;
      if (!runningJobs.containsKey(jobId)) {
        rJob = new RunningJob(jobId);
        rJob.localized = false;
        rJob.tasks = new HashSet<TaskInProgress>();
        runningJobs.put(jobId, rJob);
      } else {
        rJob = runningJobs.get(jobId);
      }
      synchronized (rJob) {
        rJob.tasks.add(tip);
      }
      runningJobs.notify(); //notify the fetcher thread
      return rJob;
    }
  }

  private void removeTaskFromJob(JobID jobId, TaskInProgress tip) {
    synchronized (runningJobs) {
      RunningJob rjob = runningJobs.get(jobId);
      if (rjob == null) {
        LOG.warn("Unknown job " + jobId + " being deleted.");
      } else {
        synchronized (rjob) {
          rjob.tasks.remove(tip);
        }
      }
    }
  }

  JobTokenSecretManager getJobTokenSecretManager() {
    return jobTokenSecretManager;
  }
  
  RunningJob getRunningJob(JobID jobId) {
    return runningJobs.get(jobId);
  }

  Localizer getLocalizer() {
    return localizer;
  }

  void setLocalizer(Localizer l) {
    localizer = l;
  }

  public static String getUserDir(String user) {
    return TaskTracker.SUBDIR + Path.SEPARATOR + user;
  }

  public static String getPrivateDistributedCacheDir(String user) {
    return getUserDir(user) + Path.SEPARATOR + TaskTracker.DISTCACHEDIR;
  }
  
  public static String getPublicDistributedCacheDir() {
    return TaskTracker.SUBDIR + Path.SEPARATOR + TaskTracker.DISTCACHEDIR;
  }

  public static String getJobCacheSubdir(String user) {
    return getUserDir(user) + Path.SEPARATOR + TaskTracker.JOBCACHE;
  }

  public static String getLocalJobDir(String user, String jobid) {
    return getJobCacheSubdir(user) + Path.SEPARATOR + jobid;
  }

  static String getLocalJobConfFile(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.JOBFILE;
  }
  
  static String getLocalJobTokenFile(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.JOB_TOKEN_FILE;
  }


  static String getTaskConfFile(String user, String jobid, String taskid,
      boolean isCleanupAttempt) {
    return getLocalTaskDir(user, jobid, taskid, isCleanupAttempt)
        + Path.SEPARATOR + TaskTracker.JOBFILE;
  }

  static String getJobJarsDir(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.JARSDIR;
  }

  static String getJobJarFile(String user, String jobid) {
    return getJobJarsDir(user, jobid) + Path.SEPARATOR + "job.jar";
  }

  static String getJobWorkDir(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + MRConstants.WORKDIR;
  }

  static String getLocalSplitMetaFile(String user, String jobid, String taskid){
    return TaskTracker.getLocalTaskDir(user, jobid, taskid) + Path.SEPARATOR
        + TaskTracker.LOCAL_SPLIT_META_FILE;
  }

  static String getLocalSplitFile(String user, String jobid, String taskid) {
    return TaskTracker.getLocalTaskDir(user, jobid, taskid) + Path.SEPARATOR
           + TaskTracker.LOCAL_SPLIT_FILE;
  }
  
  static String getIntermediateOutputDir(String user, String jobid,
      String taskid) {
    return getLocalTaskDir(user, jobid, taskid) + Path.SEPARATOR
        + TaskTracker.OUTPUT;
  }

  static String getLocalTaskDir(String user, String jobid, String taskid) {
    return getLocalTaskDir(user, jobid, taskid, false);
  }

  public static String getLocalTaskDir(String user, String jobid, String taskid,
      boolean isCleanupAttempt) {
    String taskDir = getLocalJobDir(user, jobid) + Path.SEPARATOR + taskid;
    if (isCleanupAttempt) {
      taskDir = taskDir + TASK_CLEANUP_SUFFIX;
    }
    return taskDir;
  }

  static String getTaskWorkDir(String user, String jobid, String taskid,
      boolean isCleanupAttempt) {
    String dir = getLocalTaskDir(user, jobid, taskid, isCleanupAttempt);
    return dir + Path.SEPARATOR + MRConstants.WORKDIR;
  }

  String getPid(TaskAttemptID tid) {
    TaskInProgress tip = tasks.get(tid);
    if (tip != null) {
      return jvmManager.getPid(tip.getTaskRunner());  
    }
    return null;
  }
  
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(TaskUmbilicalProtocol.class.getName())) {
      return TaskUmbilicalProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol for task tracker: " +
                            protocol);
    }
  }
    
  
  int getHttpPort() {
    return httpPort;
  }

  /**
   * Do the real constructor work here.  It's in a separate method
   * so we can call it again and "recycle" the object after calling
   * close().
   */
  synchronized void initialize() throws IOException, InterruptedException {

    LOG.info("Starting tasktracker with owner as " +
        aclsManager.getMROwner().getShortUserName());

    localFs = FileSystem.getLocal(fConf);
    // use configured nameserver & interface to get local hostname
    if (fConf.get(TT_HOST_NAME) != null) {
      this.localHostname = fConf.get(TT_HOST_NAME);
    }
    if (localHostname == null) {
      this.localHostname =
      DNS.getDefaultHost
      (fConf.get(TT_DNS_INTERFACE,"default"),
       fConf.get(TT_DNS_NAMESERVER,"default"));
    }
 
    // Check local disk, start async disk service, and clean up all 
    // local directories.
    checkLocalDirs(this.fConf.getLocalDirs());
    setAsyncDiskService(new MRAsyncDiskService(fConf));
    getAsyncDiskService().cleanupAllVolumes();

    // Clear out state tables
    this.tasks.clear();
    this.runningTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    this.runningJobs = new TreeMap<JobID, RunningJob>();
    this.mapTotal = 0;
    this.reduceTotal = 0;
    this.acceptNewTasks = true;
    this.status = null;

    this.minSpaceStart = this.fConf.getLong(TT_LOCAL_DIR_MINSPACE_START, 0L);
    this.minSpaceKill = this.fConf.getLong(TT_LOCAL_DIR_MINSPACE_KILL, 0L);
    //tweak the probe sample size (make it a function of numCopiers)
    probe_sample_size = 
      this.fConf.getInt(TT_MAX_TASK_COMPLETION_EVENTS_TO_POLL, 500);
    
    // Set up TaskTracker instrumentation
    this.myInstrumentation = createInstrumentation(this, fConf);
    
    // bind address
    InetSocketAddress socAddr = NetUtils.createSocketAddr(
        fConf.get(TT_REPORT_ADDRESS, "127.0.0.1:0"));
    String bindAddress = socAddr.getHostName();
    int tmpPort = socAddr.getPort();
    
    this.jvmManager = new JvmManager(this);

    // RPC initialization
    int max = maxMapSlots > maxReduceSlots ?
                       maxMapSlots : maxReduceSlots;
    //set the num handlers to max*2 since canCommit may wait for the duration
    //of a heartbeat RPC
    this.taskReportServer = RPC.getServer(this.getClass(), this, bindAddress,
        tmpPort, 2 * max, false, this.fConf, this.jobTokenSecretManager);

    // Set service-level authorization security policy
    if (this.fConf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            this.fConf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                MapReducePolicyProvider.class, PolicyProvider.class), 
            this.fConf));
      this.taskReportServer.refreshServiceAcl(fConf, policyProvider);
    }

    this.taskReportServer.start();

    // get the assigned address
    this.taskReportAddress = taskReportServer.getListenerAddress();
    this.fConf.set(TT_REPORT_ADDRESS,
        taskReportAddress.getHostName() + ":" + taskReportAddress.getPort());
    LOG.info("TaskTracker up at: " + this.taskReportAddress);

    this.taskTrackerName = "tracker_" + localHostname + ":" + taskReportAddress;
    LOG.info("Starting tracker " + taskTrackerName);

    Class<? extends TaskController> taskControllerClass = fConf.getClass(
        TT_TASK_CONTROLLER, DefaultTaskController.class, TaskController.class);
    taskController = (TaskController) ReflectionUtils.newInstance(
        taskControllerClass, fConf);


    // setup and create jobcache directory with appropriate permissions
    taskController.setup();

    // Initialize DistributedCache
    this.distributedCacheManager = 
        new TrackerDistributedCacheManager(this.fConf, taskController,
        asyncDiskService);
    this.distributedCacheManager.startCleanupThread();

    this.jobClient = (InterTrackerProtocol) 
    UserGroupInformation.getLoginUser().doAs(
        new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        return RPC.waitForProxy(InterTrackerProtocol.class,
            InterTrackerProtocol.versionID, 
            jobTrackAddr, fConf);  
      }
    }); 
    this.justInited = true;
    this.running = true;    
    // start the thread that will fetch map task completion events
    this.mapEventsFetcher = new MapEventsFetcherThread();
    mapEventsFetcher.setDaemon(true);
    mapEventsFetcher.setName(
                             "Map-events fetcher for all reduce tasks " + "on " + 
                             taskTrackerName);
    mapEventsFetcher.start();

    Class<? extends ResourceCalculatorPlugin> clazz =
        fConf.getClass(TT_RESOURCE_CALCULATOR_PLUGIN,
            null, ResourceCalculatorPlugin.class);
    resourceCalculatorPlugin = ResourceCalculatorPlugin
            .getResourceCalculatorPlugin(clazz, fConf);
    LOG.info(" Using ResourceCalculatorPlugin : " + resourceCalculatorPlugin);
    initializeMemoryManagement();

    setIndexCache(new IndexCache(this.fConf));

    //clear old user logs
    taskLogCleanupThread.clearOldUserLogs(this.fConf);

    mapLauncher = new TaskLauncher(TaskType.MAP, maxMapSlots);
    reduceLauncher = new TaskLauncher(TaskType.REDUCE, maxReduceSlots);
    mapLauncher.start();
    reduceLauncher.start();

    // create a localizer instance
    setLocalizer(new Localizer(localFs, fConf.getLocalDirs(), taskController));

    //Start up node health checker service.
    if (shouldStartHealthMonitor(this.fConf)) {
      startHealthMonitor(this.fConf);
    }
    
    oobHeartbeatOnTaskCompletion = 
      fConf.getBoolean(TT_OUTOFBAND_HEARBEAT, false);
  }

  /**
   * Are ACLs for authorization checks enabled on the MR cluster ?
   */
  boolean areACLsEnabled() {
    return fConf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
  }

  public static Class<?>[] getInstrumentationClasses(Configuration conf) {
    return conf.getClasses(TT_INSTRUMENTATION, TaskTrackerMetricsInst.class);
  }

  public static void setInstrumentationClass(
    Configuration conf, Class<? extends TaskTrackerInstrumentation> t) {
    conf.setClass(TT_INSTRUMENTATION,
        t, TaskTrackerInstrumentation.class);
  }
  
  public static TaskTrackerInstrumentation createInstrumentation(
      TaskTracker tt, Configuration conf) {
    try {
      Class<?>[] instrumentationClasses = getInstrumentationClasses(conf);
      if (instrumentationClasses.length == 0) {
        LOG.error("Empty string given for " + TT_INSTRUMENTATION + 
            " property -- will use default instrumentation class instead");
        return new TaskTrackerMetricsInst(tt);
      } else if (instrumentationClasses.length == 1) {
        // Just one instrumentation class given; create it directly
        Class<?> cls = instrumentationClasses[0];
        java.lang.reflect.Constructor<?> c =
          cls.getConstructor(new Class[] {TaskTracker.class} );
        return (TaskTrackerInstrumentation) c.newInstance(tt);
      } else {
        // Multiple instrumentation classes given; use a composite object
        List<TaskTrackerInstrumentation> instrumentations =
          new ArrayList<TaskTrackerInstrumentation>();
        for (Class<?> cls: instrumentationClasses) {
          java.lang.reflect.Constructor<?> c =
            cls.getConstructor(new Class[] {TaskTracker.class} );
          TaskTrackerInstrumentation inst =
            (TaskTrackerInstrumentation) c.newInstance(tt);
          instrumentations.add(inst);
        }
        return new CompositeTaskTrackerInstrumentation(tt, instrumentations);
      }
    } catch(Exception e) {
      // Reflection can throw lots of exceptions -- handle them all by 
      // falling back on the default.
      LOG.error("Failed to initialize TaskTracker metrics", e);
      return new TaskTrackerMetricsInst(tt);
    }
  }

  /**
   * Removes all contents of temporary storage.  Called upon
   * startup, to remove any leftovers from previous run.
   * 
   * Use MRAsyncDiskService.moveAndDeleteAllVolumes instead.
   * @see org.apache.hadoop.mapreduce.util.MRAsyncDiskService#cleanupAllVolumes()
   */
  @Deprecated
  public void cleanupStorage() throws IOException {
    this.fConf.deleteLocalFiles();
  }

  // Object on wait which MapEventsFetcherThread is going to wait.
  private Object waitingOn = new Object();

  private class MapEventsFetcherThread extends Thread {

    private List <FetchStatus> reducesInShuffle() {
      List <FetchStatus> fList = new ArrayList<FetchStatus>();
      for (Map.Entry <JobID, RunningJob> item : runningJobs.entrySet()) {
        RunningJob rjob = item.getValue();
        JobID jobId = item.getKey();
        FetchStatus f;
        synchronized (rjob) {
          f = rjob.getFetchStatus();
          for (TaskInProgress tip : rjob.tasks) {
            Task task = tip.getTask();
            if (!task.isMapTask()) {
              if (((ReduceTask)task).getPhase() == 
                  TaskStatus.Phase.SHUFFLE) {
                if (rjob.getFetchStatus() == null) {
                  //this is a new job; we start fetching its map events
                  f = new FetchStatus(jobId, 
                                      ((ReduceTask)task).getNumMaps());
                  rjob.setFetchStatus(f);
                }
                f = rjob.getFetchStatus();
                fList.add(f);
                break; //no need to check any more tasks belonging to this
              }
            }
          }
        }
      }
      //at this point, we have information about for which of
      //the running jobs do we need to query the jobtracker for map 
      //outputs (actually map events).
      return fList;
    }
      
    @Override
    public void run() {
      LOG.info("Starting thread: " + this.getName());
        
      while (running) {
        try {
          List <FetchStatus> fList = null;
          synchronized (runningJobs) {
            while (((fList = reducesInShuffle()).size()) == 0) {
              try {
                runningJobs.wait();
              } catch (InterruptedException e) {
                LOG.info("Shutting down: " + this.getName());
                return;
              }
            }
          }
          // now fetch all the map task events for all the reduce tasks
          // possibly belonging to different jobs
          boolean fetchAgain = false; //flag signifying whether we want to fetch
                                      //immediately again.
          for (FetchStatus f : fList) {
            long currentTime = System.currentTimeMillis();
            try {
              //the method below will return true when we have not 
              //fetched all available events yet
              if (f.fetchMapCompletionEvents(currentTime)) {
                fetchAgain = true;
              }
            } catch (Exception e) {
              LOG.warn(
                       "Ignoring exception that fetch for map completion" +
                       " events threw for " + f.jobId + " threw: " +
                       StringUtils.stringifyException(e)); 
            }
            if (!running) {
              break;
            }
          }
          synchronized (waitingOn) {
            try {
              if (!fetchAgain) {
                waitingOn.wait(heartbeatInterval);
              }
            } catch (InterruptedException ie) {
              LOG.info("Shutting down: " + this.getName());
              return;
            }
          }
        } catch (Exception e) {
          LOG.info("Ignoring exception "  + e.getMessage());
        }
      }
    } 
  }

  private class FetchStatus {
    /** The next event ID that we will start querying the JobTracker from*/
    private IntWritable fromEventId;
    /** This is the cache of map events for a given job */ 
    private List<TaskCompletionEvent> allMapEvents;
    /** What jobid this fetchstatus object is for*/
    private JobID jobId;
    private long lastFetchTime;
    private boolean fetchAgain;
     
    public FetchStatus(JobID jobId, int numMaps) {
      this.fromEventId = new IntWritable(0);
      this.jobId = jobId;
      this.allMapEvents = new ArrayList<TaskCompletionEvent>(numMaps);
    }
      
    /**
     * Reset the events obtained so far.
     */
    public void reset() {
      // Note that the sync is first on fromEventId and then on allMapEvents
      synchronized (fromEventId) {
        synchronized (allMapEvents) {
          fromEventId.set(0); // set the new index for TCE
          allMapEvents.clear();
        }
      }
    }
    
    public TaskCompletionEvent[] getMapEvents(int fromId, int max) {
        
      TaskCompletionEvent[] mapEvents = 
        TaskCompletionEvent.EMPTY_ARRAY;
      boolean notifyFetcher = false; 
      synchronized (allMapEvents) {
        if (allMapEvents.size() > fromId) {
          int actualMax = Math.min(max, (allMapEvents.size() - fromId));
          List <TaskCompletionEvent> eventSublist = 
            allMapEvents.subList(fromId, actualMax + fromId);
          mapEvents = eventSublist.toArray(mapEvents);
        } else {
          // Notify Fetcher thread. 
          notifyFetcher = true;
        }
      }
      if (notifyFetcher) {
        synchronized (waitingOn) {
          waitingOn.notify();
        }
      }
      return mapEvents;
    }
      
    public boolean fetchMapCompletionEvents(long currTime) throws IOException {
      if (!fetchAgain && (currTime - lastFetchTime) < heartbeatInterval) {
        return false;
      }
      int currFromEventId = 0;
      synchronized (fromEventId) {
        currFromEventId = fromEventId.get();
        List <TaskCompletionEvent> recentMapEvents = 
          queryJobTracker(fromEventId, jobId, jobClient);
        synchronized (allMapEvents) {
          allMapEvents.addAll(recentMapEvents);
        }
        lastFetchTime = currTime;
        if (fromEventId.get() - currFromEventId >= probe_sample_size) {
          //return true when we have fetched the full payload, indicating
          //that we should fetch again immediately (there might be more to
          //fetch
          fetchAgain = true;
          return true;
        }
      }
      fetchAgain = false;
      return false;
    }
  }

  private static LocalDirAllocator lDirAlloc = 
                              new LocalDirAllocator(MRConfig.LOCAL_DIR);

  // intialize the job directory
  RunningJob localizeJob(TaskInProgress tip
                           ) throws IOException, InterruptedException {
    Task t = tip.getTask();
    JobID jobId = t.getJobID();
    RunningJob rjob = addTaskToJob(jobId, tip);

    // Initialize the user directories if needed.
    getLocalizer().initializeUserDirs(t.getUser());

    synchronized (rjob) {
      if (!rjob.localized) {
       
        JobConf localJobConf = localizeJobFiles(t, rjob);
        // initialize job log directory
        initializeJobLogDir(jobId, localJobConf);

        // Now initialize the job via task-controller so as to set
        // ownership/permissions of jars, job-work-dir. Note that initializeJob
        // should be the last call after every other directory/file to be
        // directly under the job directory is created.
        JobInitializationContext context = new JobInitializationContext();
        context.jobid = jobId;
        context.user = t.getUser();
        context.workDir = new File(localJobConf.get(JOB_LOCAL_DIR));
        taskController.initializeJob(context);

        rjob.jobConf = localJobConf;
        rjob.keepJobFiles = ((localJobConf.getKeepTaskFilesPattern() != null) ||
                             localJobConf.getKeepFailedTaskFiles());
        rjob.localized = true;
      }
    }
    return rjob;
  }

  private FileSystem getFS(final Path filePath, JobID jobId, 
      final Configuration conf) throws IOException, InterruptedException {
    RunningJob rJob = runningJobs.get(jobId);
    FileSystem userFs = 
      rJob.ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return filePath.getFileSystem(conf);
      }});
    return userFs;
  }
  
  /**
   * Localize the job on this tasktracker. Specifically
   * <ul>
   * <li>Cleanup and create job directories on all disks</li>
   * <li>Download the job config file job.xml from the FS</li>
   * <li>Create the job work directory and set {@link TaskTracker#JOB_LOCAL_DIR}
   * in the configuration.
   * <li>Download the job jar file job.jar from the FS, unjar it and set jar
   * file in the configuration.</li>
   * </ul>
   * 
   * @param t task whose job has to be localized on this TT
   * @return the modified job configuration to be used for all the tasks of this
   *         job as a starting point.
   * @throws IOException
   */
  JobConf localizeJobFiles(Task t, RunningJob rjob)
      throws IOException, InterruptedException {
    JobID jobId = t.getJobID();
    String userName = t.getUser();

    // Initialize the job directories
    FileSystem localFs = FileSystem.getLocal(fConf);
    getLocalizer().initializeJobDirs(userName, jobId);
    // save local copy of JobToken file
    String localJobTokenFile = localizeJobTokenFile(t.getUser(), jobId);
    rjob.ugi = UserGroupInformation.createRemoteUser(t.getUser());

    Credentials ts = TokenCache.loadTokens(localJobTokenFile, fConf);
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(ts);
    if (jt != null) { //could be null in the case of some unit tests
      getJobTokenSecretManager().addTokenForJob(jobId.toString(), jt);
    }
    for (Token<? extends TokenIdentifier> token : ts.getAllTokens()) {
      rjob.ugi.addToken(token);
    }
    // Download the job.xml for this job from the system FS
    Path localJobFile =
        localizeJobConfFile(new Path(t.getJobFile()), userName, jobId);

    JobConf localJobConf = new JobConf(localJobFile);
    //WE WILL TRUST THE USERNAME THAT WE GOT FROM THE JOBTRACKER
    //AS PART OF THE TASK OBJECT
    localJobConf.setUser(userName);
    
    // set the location of the token file into jobConf to transfer 
    // the name to TaskRunner
    localJobConf.set(TokenCache.JOB_TOKENS_FILENAME,
        localJobTokenFile);
    

    // create the 'job-work' directory: job-specific shared directory for use as
    // scratch space by all tasks of the same job running on this TaskTracker. 
    Path workDir =
        lDirAlloc.getLocalPathForWrite(getJobWorkDir(userName, jobId
            .toString()), fConf);
    if (!localFs.mkdirs(workDir)) {
      throw new IOException("Mkdirs failed to create " 
                  + workDir.toString());
    }
    System.setProperty(JOB_LOCAL_DIR, workDir.toUri().getPath());
    localJobConf.set(JOB_LOCAL_DIR, workDir.toUri().getPath());
    // Download the job.jar for this job from the system FS
    localizeJobJarFile(userName, jobId, localFs, localJobConf);
    
    return localJobConf;
  }

  // Create job userlog dir.
  // Create job acls file in job log dir, if needed.
  void initializeJobLogDir(JobID jobId, JobConf localJobConf)
      throws IOException {
    // remove it from tasklog cleanup thread first,
    // it might be added there because of tasktracker reinit or restart
    taskLogCleanupThread.unmarkJobFromLogDeletion(jobId);
    localizer.initializeJobLogDir(jobId);

    if (areACLsEnabled()) {
      // Create job-acls.xml file in job userlog dir and write the needed
      // info for authorization of users for viewing task logs of this job.
      writeJobACLs(localJobConf, TaskLog.getJobDir(jobId));
    }
  }

  /**
   *  Creates job-acls.xml under the given directory logDir and writes
   *  job-view-acl, queue-admins-acl, jobOwner name and queue name into this
   *  file.
   *  queue name is the queue to which the job was submitted to.
   *  queue-admins-acl is the queue admins ACL of the queue to which this
   *  job was submitted to.
   * @param conf   job configuration
   * @param logDir job userlog dir
   * @throws IOException
   */
  private static void writeJobACLs(JobConf conf, File logDir)
      throws IOException {
    File aclFile = new File(logDir, jobACLsFile);
    JobConf aclConf = new JobConf(false);

    // set the job view acl in aclConf
    String jobViewACL = conf.get(MRJobConfig.JOB_ACL_VIEW_JOB, " ");
    aclConf.set(MRJobConfig.JOB_ACL_VIEW_JOB, jobViewACL);

    // set the job queue name in aclConf
    String queue = conf.getQueueName();
    aclConf.setQueueName(queue);

    // set the queue admins acl in aclConf
    String qACLName = toFullPropertyName(queue,
        QueueACL.ADMINISTER_JOBS.getAclName());
    String queueAdminsACL = conf.get(qACLName, " ");
    aclConf.set(qACLName, queueAdminsACL);

    // set jobOwner as user.name in aclConf
    String jobOwner = conf.getUser();
    aclConf.set("user.name", jobOwner);

    FileOutputStream out;
    try {
      out = SecureIOUtils.createForWrite(aclFile, 0600);
    } catch (SecureIOUtils.AlreadyExistsException aee) {
      LOG.warn("Job ACL file already exists at " + aclFile, aee);
      return;
    }
    try {
      aclConf.writeXml(out);
    } finally {
      out.close();
    }
  }

  /**
   * Download the job configuration file from the FS.
   * 
   * @param t Task whose job file has to be downloaded
   * @param jobId jobid of the task
   * @return the local file system path of the downloaded file.
   * @throws IOException
   */
  private Path localizeJobConfFile(Path jobFile, String user, JobID jobId)
      throws IOException, InterruptedException {
    final JobConf conf = new JobConf(getJobConf());
    FileSystem userFs = getFS(jobFile, jobId, conf);
    // Get sizes of JobFile
    // sizes are -1 if they are not present.
    FileStatus status = null;
    long jobFileSize = -1;
    try {
      status = userFs.getFileStatus(jobFile);
      jobFileSize = status.getLen();
    } catch(FileNotFoundException fe) {
      jobFileSize = -1;
    }

    Path localJobFile =
        lDirAlloc.getLocalPathForWrite(getLocalJobConfFile(user, jobId.toString()),
            jobFileSize, fConf);

    // Download job.xml
    userFs.copyToLocalFile(jobFile, localJobFile);
    return localJobFile;
  }

  /**
   * Download the job jar file from FS to the local file system and unjar it.
   * Set the local jar file in the passed configuration.
   * 
   * @param jobId
   * @param localFs
   * @param localJobConf
   * @throws IOException
   */
  private void localizeJobJarFile(String user, JobID jobId, FileSystem localFs,
      JobConf localJobConf)
      throws IOException, InterruptedException {
    // copy Jar file to the local FS and unjar it.
    String jarFile = localJobConf.getJar();
    FileStatus status = null;
    long jarFileSize = -1;
    if (jarFile != null) {
      Path jarFilePath = new Path(jarFile);
      FileSystem fs = getFS(jarFilePath, jobId, localJobConf);
      try {
        status = fs.getFileStatus(jarFilePath);
        jarFileSize = status.getLen();
      } catch (FileNotFoundException fe) {
        jarFileSize = -1;
      }
      // Here we check for five times the size of jarFileSize to accommodate for
      // unjarring the jar file in the jars directory
      Path localJarFile =
          lDirAlloc.getLocalPathForWrite(
              getJobJarFile(user, jobId.toString()), 5 * jarFileSize, fConf);

      // Download job.jar
      fs.copyToLocalFile(jarFilePath, localJarFile);

      localJobConf.setJar(localJarFile.toString());

      // Un-jar the parts of the job.jar that need to be added to the classpath
      RunJar.unJar(
        new File(localJarFile.toString()),
        new File(localJarFile.getParent().toString()),
        localJobConf.getJarUnpackPattern());
    }
  }

  protected void launchTaskForJob(TaskInProgress tip, JobConf jobConf,
      UserGroupInformation ugi) throws IOException {
    synchronized (tip) {
      tip.setJobConf(jobConf);
      tip.setUGI(ugi);
      tip.launchTask();
    }
  }
    
  public synchronized void shutdown() throws IOException {
    shuttingDown = true;
    close();
    if (this.server != null) {
      try {
        LOG.info("Shutting down StatusHttpServer");
        this.server.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down TaskTracker", e);
      }
    }
  }
  /**
   * Close down the TaskTracker and all its components.  We must also shutdown
   * any running tasks or threads, and cleanup disk space.  A new TaskTracker
   * within the same process space might be restarted, so everything must be
   * clean.
   */
  public synchronized void close() throws IOException {
    //
    // Kill running tasks.  Do this in a 2nd vector, called 'tasksToClose',
    // because calling jobHasFinished() may result in an edit to 'tasks'.
    //
    TreeMap<TaskAttemptID, TaskInProgress> tasksToClose =
      new TreeMap<TaskAttemptID, TaskInProgress>();
    tasksToClose.putAll(tasks);
    for (TaskInProgress tip : tasksToClose.values()) {
      tip.jobHasFinished(false);
    }

    this.running = false;

    if (asyncDiskService != null) {
      // Clear local storage
      asyncDiskService.cleanupAllVolumes();
      
      // Shutdown all async deletion threads with up to 10 seconds of delay
      asyncDiskService.shutdown();
      try {
        if (!asyncDiskService.awaitTermination(10000)) {
          asyncDiskService.shutdownNow();
          asyncDiskService = null;
        }
      } catch (InterruptedException e) {
        asyncDiskService.shutdownNow();
        asyncDiskService = null;
      }
    }
    
    // Shutdown the fetcher thread
    this.mapEventsFetcher.interrupt();
    
    //stop the launchers
    this.mapLauncher.interrupt();
    this.reduceLauncher.interrupt();
    
    this.distributedCacheManager.stopCleanupThread();
    jvmManager.stop();
    
    // shutdown RPC connections
    RPC.stopProxy(jobClient);

    // wait for the fetcher thread to exit
    for (boolean done = false; !done; ) {
      try {
        this.mapEventsFetcher.join();
        done = true;
      } catch (InterruptedException e) {
      }
    }
    
    if (taskReportServer != null) {
      taskReportServer.stop();
      taskReportServer = null;
    }
    if (healthChecker != null) {
      //stop node health checker service
      healthChecker.stop();
      healthChecker = null;
    }
  }

  /**
   * For testing
   */
  TaskTracker() {
    server = null;
  }

  void setConf(JobConf conf) {
    fConf = conf;
  }

  /**
   * Start with the local machine name, and the default JobTracker
   */
  public TaskTracker(JobConf conf) throws IOException, InterruptedException {
    fConf = conf;
    maxMapSlots = conf.getInt(TT_MAP_SLOTS, 2);
    maxReduceSlots = conf.getInt(TT_REDUCE_SLOTS, 2);
    aclsManager = new ACLsManager(fConf, new JobACLsManager(fConf), null);
    this.jobTrackAddr = JobTracker.getAddress(conf);
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(
        conf.get(TT_HTTP_ADDRESS, "0.0.0.0:50060"));
    String httpBindAddress = infoSocAddr.getHostName();
    int httpPort = infoSocAddr.getPort();
    this.server = new HttpServer("task", httpBindAddress, httpPort,
        httpPort == 0, conf, aclsManager.getAdminsAcl());
    workerThreads = conf.getInt(TT_HTTP_THREADS, 40);
    this.shuffleServerMetrics = new ShuffleServerMetrics(conf);
    server.setThreads(1, workerThreads);
    // let the jsp pages get to the task tracker, config, and other relevant
    // objects
    FileSystem local = FileSystem.getLocal(conf);
    this.localDirAllocator = new LocalDirAllocator(MRConfig.LOCAL_DIR);
    server.setAttribute("task.tracker", this);
    server.setAttribute("local.file.system", local);
    server.setAttribute("conf", conf);
    server.setAttribute("log", LOG);
    server.setAttribute("localDirAllocator", localDirAllocator);
    server.setAttribute("shuffleServerMetrics", shuffleServerMetrics);
    String exceptionStackRegex = conf.get(JTConfig.SHUFFLE_EXCEPTION_STACK_REGEX);
    String exceptionMsgRegex = conf.get(JTConfig.SHUFFLE_EXCEPTION_MSG_REGEX);
    server.setAttribute("exceptionStackRegex", exceptionStackRegex);
    server.setAttribute("exceptionMsgRegex", exceptionMsgRegex);
    server.addInternalServlet("mapOutput", "/mapOutput", MapOutputServlet.class);
    server.addServlet("taskLog", "/tasklog", TaskLogServlet.class);
    server.start();
    this.httpPort = server.getPort();
    checkJettyPort(httpPort);
    // create task log cleanup thread
    setTaskLogCleanupThread(new UserLogCleaner(fConf));

    UserGroupInformation.setConfiguration(fConf);
    SecurityUtil.login(fConf, TTConfig.TT_KEYTAB_FILE, TTConfig.TT_USER_NAME);

    initialize();
  }

  private void checkJettyPort(int port) throws IOException { 
    //See HADOOP-4744
    if (port < 0) {
      shuttingDown = true;
      throw new IOException("Jetty problem. Jetty didn't bind to a " +
      		"valid port");
    }
  }
  
  private void startCleanupThreads() throws IOException {
    taskCleanupThread.setDaemon(true);
    taskCleanupThread.start();
    directoryCleanupThread = new CleanupQueue();
    // start tasklog cleanup thread
    taskLogCleanupThread.setDaemon(true);
    taskLogCleanupThread.start();
  }

  // only used by tests
  void setCleanupThread(CleanupQueue c) {
    directoryCleanupThread = c;
  }
  
  CleanupQueue getCleanupThread() {
    return directoryCleanupThread;
  }

  UserLogCleaner getTaskLogCleanupThread() {
    return this.taskLogCleanupThread;
  }

  void setTaskLogCleanupThread(UserLogCleaner t) {
    this.taskLogCleanupThread = t;
  }

  void setIndexCache(IndexCache cache) {
    this.indexCache = cache;
  }
  
  /**
   * The connection to the JobTracker, used by the TaskRunner 
   * for locating remote files.
   */
  public InterTrackerProtocol getJobClient() {
    return jobClient;
  }
        
  /** Return the port at which the tasktracker bound to */
  public synchronized InetSocketAddress getTaskTrackerReportAddress() {
    return taskReportAddress;
  }
    
  /** Queries the job tracker for a set of outputs ready to be copied
   * @param fromEventId the first event ID we want to start from, this is
   * modified by the call to this method
   * @param jobClient the job tracker
   * @return a set of locations to copy outputs from
   * @throws IOException
   */  
  private List<TaskCompletionEvent> queryJobTracker(IntWritable fromEventId,
                                                    JobID jobId,
                                                    InterTrackerProtocol jobClient)
    throws IOException {

    TaskCompletionEvent t[] = jobClient.getTaskCompletionEvents(
                                                                jobId,
                                                                fromEventId.get(),
                                                                probe_sample_size);
    //we are interested in map task completion events only. So store
    //only those
    List <TaskCompletionEvent> recentMapEvents = 
      new ArrayList<TaskCompletionEvent>();
    for (int i = 0; i < t.length; i++) {
      if (t[i].isMapTask()) {
        recentMapEvents.add(t[i]);
      }
    }
    fromEventId.set(fromEventId.get() + t.length);
    return recentMapEvents;
  }

  /**
   * Main service loop.  Will stay in this loop forever.
   */
  State offerService() throws Exception {
    long lastHeartbeat = 0;

    while (running && !shuttingDown) {
      try {
        long now = System.currentTimeMillis();

        long waitTime = heartbeatInterval - (now - lastHeartbeat);
        if (waitTime > 0) {
          // sleeps for the wait time or
          // until there are empty slots to schedule tasks
          synchronized (finishedCount) {
            if (finishedCount.get() == 0) {
              finishedCount.wait(waitTime);
            }
            finishedCount.set(0);
          }
        }

        // If the TaskTracker is just starting up:
        // 1. Verify the buildVersion
        // 2. Get the system directory & filesystem
        if(justInited) {
          String jobTrackerBV = jobClient.getBuildVersion();
          if(!VersionInfo.getBuildVersion().equals(jobTrackerBV)) {
            String msg = "Shutting down. Incompatible buildVersion." +
            "\nJobTracker's: " + jobTrackerBV + 
            "\nTaskTracker's: "+ VersionInfo.getBuildVersion();
            LOG.error(msg);
            try {
              jobClient.reportTaskTrackerError(taskTrackerName, null, msg);
            } catch(Exception e ) {
              LOG.info("Problem reporting to jobtracker: " + e);
            }
            return State.DENIED;
          }
          
          String dir = jobClient.getSystemDir();
          if (dir == null) {
            throw new IOException("Failed to get system directory");
          }
          systemDirectory = new Path(dir);
          systemFS = systemDirectory.getFileSystem(fConf);
        }
        
        // Send the heartbeat and process the jobtracker's directives
        HeartbeatResponse heartbeatResponse = transmitHeartBeat(now);

        // Note the time when the heartbeat returned, use this to decide when to send the
        // next heartbeat   
        lastHeartbeat = System.currentTimeMillis();
        
        TaskTrackerAction[] actions = heartbeatResponse.getActions();
        if(LOG.isDebugEnabled()) {
          LOG.debug("Got heartbeatResponse from JobTracker with responseId: " + 
                    heartbeatResponse.getResponseId() + " and " + 
                    ((actions != null) ? actions.length : 0) + " actions");
        }
        if (reinitTaskTracker(actions)) {
          return State.STALE;
        }
            
        // resetting heartbeat interval from the response.
        heartbeatInterval = heartbeatResponse.getHeartbeatInterval();
        justStarted = false;
        justInited = false;
        if (actions != null){ 
          for(TaskTrackerAction action: actions) {
            if (action instanceof LaunchTaskAction) {
              addToTaskQueue((LaunchTaskAction)action);
            } else if (action instanceof CommitTaskAction) {
              CommitTaskAction commitAction = (CommitTaskAction)action;
              if (!commitResponses.contains(commitAction.getTaskID())) {
                LOG.info("Received commit task action for " + 
                          commitAction.getTaskID());
                commitResponses.add(commitAction.getTaskID());
              }
            } else {
              tasksToCleanup.put(action);
            }
          }
        }
        markUnresponsiveTasks();
        killOverflowingTasks();
            
        //we've cleaned up, resume normal operation
        if (!acceptNewTasks && isIdle()) {
          acceptNewTasks=true;
        }
        //The check below may not be required every iteration but we are 
        //erring on the side of caution here. We have seen many cases where
        //the call to jetty's getLocalPort() returns different values at 
        //different times. Being a real paranoid here.
        checkJettyPort(server.getPort());
      } catch (InterruptedException ie) {
        LOG.info("Interrupted. Closing down.");
        return State.INTERRUPTED;
      } catch (DiskErrorException de) {
        String msg = "Exiting task tracker for disk error:\n" +
          StringUtils.stringifyException(de);
        LOG.error(msg);
        synchronized (this) {
          jobClient.reportTaskTrackerError(taskTrackerName, 
                                           "DiskErrorException", msg);
        }
        return State.STALE;
      } catch (RemoteException re) {
        String reClass = re.getClassName();
        if (DisallowedTaskTrackerException.class.getName().equals(reClass)) {
          LOG.info("Tasktracker disallowed by JobTracker.");
          return State.DENIED;
        }
      } catch (Exception except) {
        String msg = "Caught exception: " + 
          StringUtils.stringifyException(except);
        LOG.error(msg);
      }
    }

    return State.NORMAL;
  }

  private long previousUpdate = 0;

  /**
   * Build and transmit the heart beat to the JobTracker
   * @param now current time
   * @return false if the tracker was unknown
   * @throws IOException
   */
  HeartbeatResponse transmitHeartBeat(long now) throws IOException {
    // Send Counters in the status once every COUNTER_UPDATE_INTERVAL
    boolean sendCounters;
    if (now > (previousUpdate + COUNTER_UPDATE_INTERVAL)) {
      sendCounters = true;
      previousUpdate = now;
    }
    else {
      sendCounters = false;
    }

    // 
    // Check if the last heartbeat got through... 
    // if so then build the heartbeat information for the JobTracker;
    // else resend the previous status information.
    //
    if (status == null) {
      synchronized (this) {
        status = new TaskTrackerStatus(taskTrackerName, localHostname, 
                                       httpPort, 
                                       cloneAndResetRunningTaskStatuses(
                                         sendCounters), 
                                       failures, 
                                       maxMapSlots,
                                       maxReduceSlots); 
      }
    } else {
      LOG.info("Resending 'status' to '" + jobTrackAddr.getHostName() +
               "' with reponseId '" + heartbeatResponseId);
    }
      
    //
    // Check if we should ask for a new Task
    //
    boolean askForNewTask;
    long localMinSpaceStart;
    synchronized (this) {
      askForNewTask = 
        ((status.countOccupiedMapSlots() < maxMapSlots || 
          status.countOccupiedReduceSlots() < maxReduceSlots) && 
         acceptNewTasks); 
      localMinSpaceStart = minSpaceStart;
    }
    if (askForNewTask) {
      checkLocalDirs(fConf.getLocalDirs());
      askForNewTask = enoughFreeSpace(localMinSpaceStart);
      long freeDiskSpace = getFreeSpace();
      long totVmem = getTotalVirtualMemoryOnTT();
      long totPmem = getTotalPhysicalMemoryOnTT();
      long availableVmem = getAvailableVirtualMemoryOnTT();
      long availablePmem = getAvailablePhysicalMemoryOnTT();
      long cumuCpuTime = getCumulativeCpuTimeOnTT();
      long cpuFreq = getCpuFrequencyOnTT();
      int numCpu = getNumProcessorsOnTT();
      float cpuUsage = getCpuUsageOnTT();

      status.getResourceStatus().setAvailableSpace(freeDiskSpace);
      status.getResourceStatus().setTotalVirtualMemory(totVmem);
      status.getResourceStatus().setTotalPhysicalMemory(totPmem);
      status.getResourceStatus().setMapSlotMemorySizeOnTT(
          mapSlotMemorySizeOnTT);
      status.getResourceStatus().setReduceSlotMemorySizeOnTT(
          reduceSlotSizeMemoryOnTT);
      status.getResourceStatus().setAvailableVirtualMemory(availableVmem); 
      status.getResourceStatus().setAvailablePhysicalMemory(availablePmem);
      status.getResourceStatus().setCumulativeCpuTime(cumuCpuTime);
      status.getResourceStatus().setCpuFrequency(cpuFreq);
      status.getResourceStatus().setNumProcessors(numCpu);
      status.getResourceStatus().setCpuUsage(cpuUsage);
    }
    //add node health information
    
    TaskTrackerHealthStatus healthStatus = status.getHealthStatus();
    synchronized (this) {
      if (healthChecker != null) {
        healthChecker.setHealthStatus(healthStatus);
      } else {
        healthStatus.setNodeHealthy(true);
        healthStatus.setLastReported(0L);
        healthStatus.setHealthReport("");
      }
    }
    //
    // Xmit the heartbeat
    //
    HeartbeatResponse heartbeatResponse = jobClient.heartbeat(status, 
                                                              justStarted,
                                                              justInited,
                                                              askForNewTask, 
                                                              heartbeatResponseId);
      
    //
    // The heartbeat got through successfully!
    //
    heartbeatResponseId = heartbeatResponse.getResponseId();
      
    synchronized (this) {
      for (TaskStatus taskStatus : status.getTaskReports()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            !taskStatus.inTaskCleanupPhase()) {
          if (taskStatus.getIsMap()) {
            mapTotal--;
          } else {
            reduceTotal--;
          }
          try {
            myInstrumentation.completeTask(taskStatus.getTaskID());
          } catch (MetricsException me) {
            LOG.warn("Caught: " + StringUtils.stringifyException(me));
          }
          runningTasks.remove(taskStatus.getTaskID());
        }
      }
      
      // Clear transient status information which should only
      // be sent once to the JobTracker
      for (TaskInProgress tip: runningTasks.values()) {
        tip.getStatus().clearStatus();
      }
    }

    // Force a rebuild of 'status' on the next iteration
    status = null;                                

    return heartbeatResponse;
  }

  /**
   * Return the total virtual memory available on this TaskTracker.
   * @return total size of virtual memory.
   */
  long getTotalVirtualMemoryOnTT() {
    return totalVirtualMemoryOnTT;
  }

  /**
   * Return the total physical memory available on this TaskTracker.
   * @return total size of physical memory.
   */
  long getTotalPhysicalMemoryOnTT() {
    return totalPhysicalMemoryOnTT;
  }

  /**
   * Return the free virtual memory available on this TaskTracker.
   * @return total size of free virtual memory.
   */
  long getAvailableVirtualMemoryOnTT() {
    long availableVirtualMemoryOnTT = TaskTrackerStatus.UNAVAILABLE;
    if (resourceCalculatorPlugin != null) {
      availableVirtualMemoryOnTT =
              resourceCalculatorPlugin.getAvailableVirtualMemorySize();
    }
    return availableVirtualMemoryOnTT;
  }

  /**
   * Return the free physical memory available on this TaskTracker.
   * @return total size of free physical memory in bytes
   */
  long getAvailablePhysicalMemoryOnTT() {
    long availablePhysicalMemoryOnTT = TaskTrackerStatus.UNAVAILABLE;
    if (resourceCalculatorPlugin != null) {
      availablePhysicalMemoryOnTT =
              resourceCalculatorPlugin.getAvailablePhysicalMemorySize();
    }
    return availablePhysicalMemoryOnTT;
  }

  /**
   * Return the cumulative CPU used time on this TaskTracker since system is on
   * @return cumulative CPU used time in millisecond
   */
  long getCumulativeCpuTimeOnTT() {
    long cumulativeCpuTime = TaskTrackerStatus.UNAVAILABLE;
    if (resourceCalculatorPlugin != null) {
      cumulativeCpuTime = resourceCalculatorPlugin.getCumulativeCpuTime();
    }
    return cumulativeCpuTime;
  }

  /**
   * Return the number of Processors on this TaskTracker
   * @return number of processors
   */
  int getNumProcessorsOnTT() {
    int numProcessors = TaskTrackerStatus.UNAVAILABLE;
    if (resourceCalculatorPlugin != null) {
      numProcessors = resourceCalculatorPlugin.getNumProcessors();
    }
    return numProcessors;
  }

  /**
   * Return the CPU frequency of this TaskTracker
   * @return CPU frequency in kHz
   */
  long getCpuFrequencyOnTT() {
    long cpuFrequency = TaskTrackerStatus.UNAVAILABLE;
    if (resourceCalculatorPlugin != null) {
      cpuFrequency = resourceCalculatorPlugin.getCpuFrequency();
    }
    return cpuFrequency;
  }

  /**
   * Return the CPU usage in % of this TaskTracker
   * @return CPU usage in %
   */
  float getCpuUsageOnTT() {
    float cpuUsage = TaskTrackerStatus.UNAVAILABLE;
    if (resourceCalculatorPlugin != null) {
      cpuUsage = resourceCalculatorPlugin.getCpuUsage();
    }
    return cpuUsage;
  }
  
  long getTotalMemoryAllottedForTasksOnTT() {
    return totalMemoryAllottedForTasks;
  }

  /**
   * @return The amount of physical memory that will not be used for running
   *         tasks in bytes. Returns JobConf.DISABLED_MEMORY_LIMIT if it is not
   *         configured.
   */
  long getReservedPhysicalMemoryOnTT() {
    return reservedPhysicalMemoryOnTT;
  }

  /**
   * Check if the jobtracker directed a 'reset' of the tasktracker.
   * 
   * @param actions the directives of the jobtracker for the tasktracker.
   * @return <code>true</code> if tasktracker is to be reset, 
   *         <code>false</code> otherwise.
   */
  private boolean reinitTaskTracker(TaskTrackerAction[] actions) {
    if (actions != null) {
      for (TaskTrackerAction action : actions) {
        if (action.getActionId() == 
            TaskTrackerAction.ActionType.REINIT_TRACKER) {
          LOG.info("Received ReinitTrackerAction from JobTracker");
          return true;
        }
      }
    }
    return false;
  }
    
  /**
   * Kill any tasks that have not reported progress in the last X seconds.
   */
  private synchronized void markUnresponsiveTasks() throws IOException {
    long now = System.currentTimeMillis();
    for (TaskInProgress tip: runningTasks.values()) {
      if (tip.getRunState() == TaskStatus.State.RUNNING ||
          tip.getRunState() == TaskStatus.State.COMMIT_PENDING ||
          tip.isCleaningup()) {
        // Check the per-job timeout interval for tasks;
        // an interval of '0' implies it is never timed-out
        long jobTaskTimeout = tip.getTaskTimeout();
        if (jobTaskTimeout == 0) {
          continue;
        }
          
        // Check if the task has not reported progress for a 
        // time-period greater than the configured time-out
        long timeSinceLastReport = now - tip.getLastProgressReport();
        if (timeSinceLastReport > jobTaskTimeout && !tip.wasKilled) {
          String msg = 
            "Task " + tip.getTask().getTaskID() + " failed to report status for " 
            + (timeSinceLastReport / 1000) + " seconds. Killing!";
          LOG.info(tip.getTask().getTaskID() + ": " + msg);
          ReflectionUtils.logThreadInfo(LOG, "lost task", 30);
          tip.reportDiagnosticInfo(msg);
          myInstrumentation.timedoutTask(tip.getTask().getTaskID());
          dumpTaskStack(tip);
          purgeTask(tip, true);
        }
      }
    }
  }

  /**
   * Builds list of PathDeletionContext objects for the given paths
   */
  private static PathDeletionContext[] buildPathDeletionContexts(FileSystem fs,
      Path[] paths) {
    int i = 0;
    PathDeletionContext[] contexts = new PathDeletionContext[paths.length];

    for (Path p : paths) {
      contexts[i++] = new PathDeletionContext(fs, p.toUri().getPath());
    }
    return contexts;
  }

  /**
   * Builds list of {@link TaskControllerJobPathDeletionContext} objects for a 
   * job each pointing to the job's jobLocalDir.
   * @param fs    : FileSystem in which the dirs to be deleted
   * @param paths : mapred-local-dirs
   * @param id    : {@link JobID} of the job for which the local-dir needs to 
   *                be cleaned up.
   * @param user  : Job owner's username
   * @param taskController : the task-controller to be used for deletion of
   *                         jobLocalDir
   */
  static PathDeletionContext[] buildTaskControllerJobPathDeletionContexts(
      FileSystem fs, Path[] paths, JobID id, String user,
      TaskController taskController)
      throws IOException {
    int i = 0;
    PathDeletionContext[] contexts =
                          new TaskControllerPathDeletionContext[paths.length];

    for (Path p : paths) {
      contexts[i++] = new TaskControllerJobPathDeletionContext(fs, p, id, user,
                                                               taskController);
    }
    return contexts;
  } 
  
  /**
   * Builds list of TaskControllerTaskPathDeletionContext objects for a task
   * @param fs    : FileSystem in which the dirs to be deleted
   * @param paths : mapred-local-dirs
   * @param task  : the task whose taskDir or taskWorkDir is going to be deleted
   * @param isWorkDir : the dir to be deleted is workDir or taskDir
   * @param taskController : the task-controller to be used for deletion of
   *                         taskDir or taskWorkDir
   */
  static PathDeletionContext[] buildTaskControllerTaskPathDeletionContexts(
      FileSystem fs, Path[] paths, Task task, boolean isWorkDir,
      TaskController taskController)
      throws IOException {
    int i = 0;
    PathDeletionContext[] contexts =
                          new TaskControllerPathDeletionContext[paths.length];

    for (Path p : paths) {
      contexts[i++] = new TaskControllerTaskPathDeletionContext(fs, p, task,
                          isWorkDir, taskController);
    }
    return contexts;
  }

  /**
   * Send a signal to a stuck task commanding it to dump stack traces
   * to stderr before we kill it with purgeTask().
   *
   * @param tip {@link TaskInProgress} to dump stack traces.
   */
  private void dumpTaskStack(TaskInProgress tip) {
    TaskRunner runner = tip.getTaskRunner();
    if (null == runner) {
      return; // tip is already abandoned.
    }

    JvmManager jvmMgr = runner.getJvmManager();
    jvmMgr.dumpStack(runner);
  }

  /**
   * The task tracker is done with this job, so we need to clean up.
   * @param action The action with the job
   * @throws IOException
   */
  synchronized void purgeJob(KillJobAction action) throws IOException {
    JobID jobId = action.getJobID();
    LOG.info("Received 'KillJobAction' for job: " + jobId);
    RunningJob rjob = null;
    synchronized (runningJobs) {
      rjob = runningJobs.get(jobId);
    }
      
    if (rjob == null) {
      LOG.warn("Unknown job " + jobId + " being deleted.");
    } else {
      synchronized (rjob) {            
        // Add this tips of this job to queue of tasks to be purged 
        for (TaskInProgress tip : rjob.tasks) {
          tip.jobHasFinished(false);
          Task t = tip.getTask();
          if (t.isMapTask()) {
            indexCache.removeMap(tip.getTask().getTaskID().toString());
          }
        }
        // Delete the job directory for this  
        // task if the job is done/failed
        if (!rjob.keepJobFiles) {
          removeJobFiles(rjob.jobConf.getUser(), rjob.getJobID());
        }
        // add job to taskLogCleanupThread
        long now = System.currentTimeMillis();
        taskLogCleanupThread.markJobLogsForDeletion(now, rjob.jobConf,
            rjob.jobid);

        // Remove this job 
        rjob.tasks.clear();
        // Close all FileSystems for this job
        try {
          FileSystem.closeAllForUGI(rjob.getUGI());
        } catch (IOException ie) {
          LOG.warn("Ignoring exception " + StringUtils.stringifyException(ie) + 
              " while closing FileSystem for " + rjob.getUGI());
        }
      }
    }

    synchronized(runningJobs) {
      runningJobs.remove(jobId);
    }
    getJobTokenSecretManager().removeTokenForJob(jobId.toString());
  }

  /**
   * This job's files are no longer needed on this TT, remove them.
   * 
   * @param rjob
   * @throws IOException
   */
  void removeJobFiles(String user, JobID jobId)
      throws IOException {
    PathDeletionContext[] contexts = 
      buildTaskControllerJobPathDeletionContexts(localFs, 
          getLocalFiles(fConf, ""), jobId, user, taskController);
    directoryCleanupThread.addToQueue(contexts);
  }

  /**
   * Remove the tip and update all relevant state.
   * 
   * @param tip {@link TaskInProgress} to be removed.
   * @param wasFailure did the task fail or was it killed?
   */
  private void purgeTask(TaskInProgress tip, boolean wasFailure) 
  throws IOException {
    if (tip != null) {
      LOG.info("About to purge task: " + tip.getTask().getTaskID());
        
      // Remove the task from running jobs, 
      // removing the job if it's the last task
      removeTaskFromJob(tip.getTask().getJobID(), tip);
      tip.jobHasFinished(wasFailure);
      if (tip.getTask().isMapTask()) {
        indexCache.removeMap(tip.getTask().getTaskID().toString());
      }
    }
  }

  /** Check if we're dangerously low on disk space
   * If so, kill jobs to free up space and make sure
   * we don't accept any new tasks
   * Try killing the reduce jobs first, since I believe they
   * use up most space
   * Then pick the one with least progress
   */
  private void killOverflowingTasks() throws IOException {
    long localMinSpaceKill;
    synchronized(this){
      localMinSpaceKill = minSpaceKill;  
    }
    if (!enoughFreeSpace(localMinSpaceKill)) {
      acceptNewTasks=false; 
      //we give up! do not accept new tasks until
      //all the ones running have finished and they're all cleared up
      synchronized (this) {
        TaskInProgress killMe = findTaskToKill(null);

        if (killMe!=null) {
          String msg = "Tasktracker running out of space." +
            " Killing task.";
          LOG.info(killMe.getTask().getTaskID() + ": " + msg);
          killMe.reportDiagnosticInfo(msg);
          purgeTask(killMe, false);
        }
      }
    }
  }

  /**
   * Pick a task to kill to free up memory/disk-space 
   * @param tasksToExclude tasks that are to be excluded while trying to find a
   *          task to kill. If null, all runningTasks will be searched.
   * @return the task to kill or null, if one wasn't found
   */
  synchronized TaskInProgress findTaskToKill(List<TaskAttemptID> tasksToExclude) {
    TaskInProgress killMe = null;
    for (Iterator it = runningTasks.values().iterator(); it.hasNext();) {
      TaskInProgress tip = (TaskInProgress) it.next();

      if (tasksToExclude != null
          && tasksToExclude.contains(tip.getTask().getTaskID())) {
        // exclude this task
        continue;
      }

      if ((tip.getRunState() == TaskStatus.State.RUNNING ||
           tip.getRunState() == TaskStatus.State.COMMIT_PENDING) &&
          !tip.wasKilled) {
                
        if (killMe == null) {
          killMe = tip;

        } else if (!tip.getTask().isMapTask()) {
          //reduce task, give priority
          if (killMe.getTask().isMapTask() || 
              (tip.getTask().getProgress().get() < 
               killMe.getTask().getProgress().get())) {

            killMe = tip;
          }

        } else if (killMe.getTask().isMapTask() &&
                   tip.getTask().getProgress().get() < 
                   killMe.getTask().getProgress().get()) {
          //map task, only add if the progress is lower

          killMe = tip;
        }
      }
    }
    return killMe;
  }

  /**
   * Check if any of the local directories has enough
   * free space  (more than minSpace)
   * 
   * If not, do not try to get a new task assigned 
   * @return
   * @throws IOException 
   */
  private boolean enoughFreeSpace(long minSpace) throws IOException {
    if (minSpace == 0) {
      return true;
    }
    return minSpace < getFreeSpace();
  }
  
  private long getFreeSpace() throws IOException {
    long biggestSeenSoFar = 0;
    String[] localDirs = fConf.getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      DF df = null;
      if (localDirsDf.containsKey(localDirs[i])) {
        df = localDirsDf.get(localDirs[i]);
      } else {
        df = new DF(new File(localDirs[i]), fConf);
        localDirsDf.put(localDirs[i], df);
      }

      long availOnThisVol = df.getAvailable();
      if (availOnThisVol > biggestSeenSoFar) {
        biggestSeenSoFar = availOnThisVol;
      }
    }
    
    //Should ultimately hold back the space we expect running tasks to use but 
    //that estimate isn't currently being passed down to the TaskTrackers    
    return biggestSeenSoFar;
  }
    
  private TaskLauncher mapLauncher;
  private TaskLauncher reduceLauncher;
  public JvmManager getJvmManagerInstance() {
    return jvmManager;
  }

  // called from unit test  
  void setJvmManagerInstance(JvmManager jvmManager) {
    this.jvmManager = jvmManager;
  }

  private void addToTaskQueue(LaunchTaskAction action) {
    if (action.getTask().isMapTask()) {
      mapLauncher.addToTaskQueue(action);
    } else {
      reduceLauncher.addToTaskQueue(action);
    }
  }

  // This method is called from unit tests
  int getFreeSlots(boolean isMap) {
    if (isMap) {
      return mapLauncher.numFreeSlots.get();
    } else {
      return reduceLauncher.numFreeSlots.get();
    }
  }

  class TaskLauncher extends Thread {
    private IntWritable numFreeSlots;
    private final int maxSlots;
    private List<TaskInProgress> tasksToLaunch;

    public TaskLauncher(TaskType taskType, int numSlots) {
      this.maxSlots = numSlots;
      this.numFreeSlots = new IntWritable(numSlots);
      this.tasksToLaunch = new LinkedList<TaskInProgress>();
      setDaemon(true);
      setName("TaskLauncher for " + taskType + " tasks");
    }

    public void addToTaskQueue(LaunchTaskAction action) {
      synchronized (tasksToLaunch) {
        TaskInProgress tip = registerTask(action, this);
        tasksToLaunch.add(tip);
        tasksToLaunch.notifyAll();
      }
    }
    
    public void cleanTaskQueue() {
      tasksToLaunch.clear();
    }
    
    public void addFreeSlots(int numSlots) {
      synchronized (numFreeSlots) {
        numFreeSlots.set(numFreeSlots.get() + numSlots);
        assert (numFreeSlots.get() <= maxSlots);
        LOG.info("addFreeSlot : current free slots : " + numFreeSlots.get());
        numFreeSlots.notifyAll();
      }
    }
    
    void notifySlots() {
      synchronized (numFreeSlots) {
        numFreeSlots.notifyAll();
      }      
    }
    
    int getNumWaitingTasksToLaunch() {
      synchronized (tasksToLaunch) {
        return tasksToLaunch.size();
      }
    }
    
    public void run() {
      while (!Thread.interrupted()) {
        try {
          TaskInProgress tip;
          Task task;
          synchronized (tasksToLaunch) {
            while (tasksToLaunch.isEmpty()) {
              tasksToLaunch.wait();
            }
            //get the TIP
            tip = tasksToLaunch.remove(0);
            task = tip.getTask();
            LOG.info("Trying to launch : " + tip.getTask().getTaskID() + 
                     " which needs " + task.getNumSlotsRequired() + " slots");
          }
          //wait for free slots to run
          synchronized (numFreeSlots) {
            boolean canLaunch = true;
            while (numFreeSlots.get() < task.getNumSlotsRequired()) {
              //Make sure that there is no kill task action for this task!
              //We are not locking tip here, because it would reverse the
              //locking order!
              //Also, Lock for the tip is not required here! because :
              // 1. runState of TaskStatus is volatile
              // 2. Any notification is not missed because notification is
              // synchronized on numFreeSlots. So, while we are doing the check,
              // if the tip is half way through the kill(), we don't miss
              // notification for the following wait(). 
              if (!tip.canBeLaunched()) {
                //got killed externally while still in the launcher queue
                LOG.info("Not blocking slots for " + task.getTaskID()
                    + " as it got killed externally. Task's state is "
                    + tip.getRunState());
                canLaunch = false;
                break;
              }              
              LOG.info("TaskLauncher : Waiting for " + task.getNumSlotsRequired() + 
                       " to launch " + task.getTaskID() + ", currently we have " + 
                       numFreeSlots.get() + " free slots");
              numFreeSlots.wait();
            }
            if (!canLaunch) {
              continue;
            }
            LOG.info("In TaskLauncher, current free slots : " + numFreeSlots.get()+
                     " and trying to launch "+tip.getTask().getTaskID() + 
                     " which needs " + task.getNumSlotsRequired() + " slots");
            numFreeSlots.set(numFreeSlots.get() - task.getNumSlotsRequired());
            assert (numFreeSlots.get() >= 0);
          }
          synchronized (tip) {
            //to make sure that there is no kill task action for this
            if (!tip.canBeLaunched()) {
              //got killed externally while still in the launcher queue
              LOG.info("Not launching task " + task.getTaskID() + " as it got"
                + " killed externally. Task's state is " + tip.getRunState());
              addFreeSlots(task.getNumSlotsRequired());
              continue;
            }
            tip.slotTaken = true;
          }
          //got a free slot. launch the task
          startNewTask(tip);
        } catch (InterruptedException e) { 
          return; // ALL DONE
        } catch (Throwable th) {
          LOG.error("TaskLauncher error " + 
              StringUtils.stringifyException(th));
        }
      }
    }
  }
  private TaskInProgress registerTask(LaunchTaskAction action, 
      TaskLauncher launcher) {
    Task t = action.getTask();
    LOG.info("LaunchTaskAction (registerTask): " + t.getTaskID() +
             " task's state:" + t.getState());
    TaskInProgress tip = new TaskInProgress(t, this.fConf, launcher);
    synchronized (this) {
      tasks.put(t.getTaskID(), tip);
      runningTasks.put(t.getTaskID(), tip);
      boolean isMap = t.isMapTask();
      if (isMap) {
        mapTotal++;
      } else {
        reduceTotal++;
      }
    }
    return tip;
  }
  /**
   * Start a new task.
   * All exceptions are handled locally, so that we don't mess up the
   * task tracker.
   */
  void startNewTask(final TaskInProgress tip) {
    Thread launchThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          RunningJob rjob = localizeJob(tip);
          // Localization is done. Neither rjob.jobConf nor rjob.ugi can be null
          launchTaskForJob(tip, new JobConf(rjob.getJobConf()), rjob.ugi); 
        } catch (Throwable e) {
          String msg = ("Error initializing " + tip.getTask().getTaskID() + 
                        ":\n" + StringUtils.stringifyException(e));
          LOG.warn(msg);
          tip.reportDiagnosticInfo(msg);
          try {
            tip.kill(true);
            tip.cleanup(true);
          } catch (IOException ie2) {
            LOG.info("Error cleaning up " + tip.getTask().getTaskID() + ":\n" +
                     StringUtils.stringifyException(ie2));          
          }
          if (e instanceof Error) {
            LOG.error("TaskLauncher error " +
                StringUtils.stringifyException(e));
          }
        }
      }
    });
    launchThread.start();
        
  }
  
  void addToMemoryManager(TaskAttemptID attemptId, boolean isMap, 
                          JobConf conf) {
    if (!isTaskMemoryManagerEnabled()) {
      return; // Skip this if TaskMemoryManager is not enabled.
    }
    // Obtain physical memory limits from the job configuration
    long physicalMemoryLimit =
      conf.getLong(isMap ? MRJobConfig.MAP_MEMORY_PHYSICAL_MB :
                   MRJobConfig.REDUCE_MEMORY_PHYSICAL_MB,
                   JobConf.DISABLED_MEMORY_LIMIT);
    if (physicalMemoryLimit > 0) {
      physicalMemoryLimit *= 1024L * 1024L;
    }

    // Obtain virtual memory limits from the job configuration
    long virtualMemoryLimit = isMap ?
      conf.getMemoryForMapTask() * 1024 * 1024 :
      conf.getMemoryForReduceTask() * 1024 * 1024;

    taskMemoryManager.addTask(attemptId, virtualMemoryLimit,
                              physicalMemoryLimit);
  }

  void removeFromMemoryManager(TaskAttemptID attemptId) {
    // Remove the entry from taskMemoryManagerThread's data structures.
    if (isTaskMemoryManagerEnabled()) {
      taskMemoryManager.removeTask(attemptId);
    }
  }

  /** 
   * Notify the tasktracker to send an out-of-band heartbeat.
   */
  private void notifyTTAboutTaskCompletion() {
    if (oobHeartbeatOnTaskCompletion) {
      synchronized (finishedCount) {
        int value = finishedCount.get();
        finishedCount.set(value+1);
        finishedCount.notify();
      }
    }
  }
  
  /**
   * The server retry loop.  
   * This while-loop attempts to connect to the JobTracker.  It only 
   * loops when the old TaskTracker has gone bad (its state is
   * stale somehow) and we need to reinitialize everything.
   */
  public void run() {
    try {
      startCleanupThreads();
      boolean denied = false;
      while (running && !shuttingDown && !denied) {
        boolean staleState = false;
        try {
          // This while-loop attempts reconnects if we get network errors
          while (running && !staleState && !shuttingDown && !denied) {
            try {
              State osState = offerService();
              if (osState == State.STALE) {
                staleState = true;
              } else if (osState == State.DENIED) {
                denied = true;
              }
            } catch (Exception ex) {
              if (!shuttingDown) {
                LOG.info("Lost connection to JobTracker [" +
                         jobTrackAddr + "].  Retrying...", ex);
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
              }
            }
          }
        } finally {
          close();
        }
        if (shuttingDown) { return; }
        LOG.warn("Reinitializing local state");
        initialize();
      }
      if (denied) {
        shutdown();
      }
    } catch (IOException iex) {
      LOG.error("Got fatal exception while reinitializing TaskTracker: " +
                StringUtils.stringifyException(iex));
      return;
    }
    catch (InterruptedException i) {
      LOG.error("Got interrupted while reinitializing TaskTracker: " + 
          i.getMessage());
      return;
    }
  }
    
  ///////////////////////////////////////////////////////
  // TaskInProgress maintains all the info for a Task that
  // lives at this TaskTracker.  It maintains the Task object,
  // its TaskStatus, and the TaskRunner.
  ///////////////////////////////////////////////////////
  class TaskInProgress {
    Task task;
    long lastProgressReport;
    StringBuffer diagnosticInfo = new StringBuffer();
    private TaskRunner runner;
    volatile boolean done = false;
    volatile boolean wasKilled = false;
    private JobConf defaultJobConf;
    private JobConf localJobConf;
    private boolean keepFailedTaskFiles;
    private boolean alwaysKeepTaskFiles;
    private TaskStatus taskStatus; 
    private long taskTimeout;
    private String debugCommand;
    private volatile boolean slotTaken = false;
    private TaskLauncher launcher;

    // The ugi of the user who is running the job. This contains all the tokens
    // too which will be populated during job-localization
    private UserGroupInformation ugi;

    UserGroupInformation getUGI() {
      return ugi;
    }

    void setUGI(UserGroupInformation userUGI) {
      ugi = userUGI;
    }

    /**
     */
    public TaskInProgress(Task task, JobConf conf) {
      this(task, conf, null);
    }
    
    public TaskInProgress(Task task, JobConf conf, TaskLauncher launcher) {
      this.task = task;
      this.launcher = launcher;
      this.lastProgressReport = System.currentTimeMillis();
      this.defaultJobConf = conf;
      localJobConf = null;
      taskStatus = TaskStatus.createTaskStatus(task.isMapTask(), task.getTaskID(), 
                                               0.0f, 
                                               task.getNumSlotsRequired(),
                                               task.getState(),
                                               diagnosticInfo.toString(), 
                                               "initializing",  
                                               getName(), 
                                               task.isTaskCleanupTask() ? 
                                                 TaskStatus.Phase.CLEANUP :  
                                               task.isMapTask()? TaskStatus.Phase.MAP:
                                               TaskStatus.Phase.SHUFFLE,
                                               task.getCounters()); 
      taskTimeout = (10 * 60 * 1000);
    }
        
    void localizeTask(Task task) throws IOException{

      FileSystem localFs = FileSystem.getLocal(fConf);

      // create taskDirs on all the disks.
      getLocalizer().initializeAttemptDirs(task.getUser(),
          task.getJobID().toString(), task.getTaskID().toString(),
          task.isTaskCleanupTask());

      // create the working-directory of the task 
      Path cwd =
          lDirAlloc.getLocalPathForWrite(getTaskWorkDir(task.getUser(), task
              .getJobID().toString(), task.getTaskID().toString(), task
              .isTaskCleanupTask()), defaultJobConf);
      if (!localFs.mkdirs(cwd)) {
        throw new IOException("Mkdirs failed to create " 
                    + cwd.toString());
      }

      localJobConf.set(LOCAL_DIR,
                       fConf.get(LOCAL_DIR));

      if (fConf.get(TT_HOST_NAME) != null) {
        localJobConf.set(TT_HOST_NAME, fConf.get(TT_HOST_NAME));
      }
            
      keepFailedTaskFiles = localJobConf.getKeepFailedTaskFiles();

      // Do the task-type specific localization
      task.localizeConfiguration(localJobConf);
      
      List<String[]> staticResolutions = NetUtils.getAllStaticResolutions();
      if (staticResolutions != null && staticResolutions.size() > 0) {
        StringBuffer str = new StringBuffer();

        for (int i = 0; i < staticResolutions.size(); i++) {
          String[] hostToResolved = staticResolutions.get(i);
          str.append(hostToResolved[0]+"="+hostToResolved[1]);
          if (i != staticResolutions.size() - 1) {
            str.append(',');
          }
        }
        localJobConf.set(TT_STATIC_RESOLUTIONS, str.toString());
      }
      if (task.isMapTask()) {
        debugCommand = localJobConf.getMapDebugScript();
      } else {
        debugCommand = localJobConf.getReduceDebugScript();
      }
      String keepPattern = localJobConf.getKeepTaskFilesPattern();
      if (keepPattern != null) {
        alwaysKeepTaskFiles = 
          Pattern.matches(keepPattern, task.getTaskID().toString());
      } else {
        alwaysKeepTaskFiles = false;
      }
      if (debugCommand != null || localJobConf.getProfileEnabled() ||
          alwaysKeepTaskFiles || keepFailedTaskFiles) {
        //disable jvm reuse
        localJobConf.setNumTasksToExecutePerJvm(1);
      }
      task.setConf(localJobConf);
    }
        
    /**
     */
    public Task getTask() {
      return task;
    }
    
    TaskRunner getTaskRunner() {
      return runner;
    }

    void setTaskRunner(TaskRunner rnr) {
      this.runner = rnr;
    }

    public synchronized void setJobConf(JobConf lconf){
      this.localJobConf = lconf;
      keepFailedTaskFiles = localJobConf.getKeepFailedTaskFiles();
      taskTimeout = localJobConf.getLong(MRJobConfig.TASK_TIMEOUT, 
                                         10 * 60 * 1000);
    }
        
    public synchronized JobConf getJobConf() {
      return localJobConf;
    }
        
    /**
     */
    public synchronized TaskStatus getStatus() {
      taskStatus.setDiagnosticInfo(diagnosticInfo.toString());
      if (diagnosticInfo.length() > 0) {
        diagnosticInfo = new StringBuffer();
      }
      
      return taskStatus;
    }

    /**
     * Kick off the task execution
     */
    public synchronized void launchTask() throws IOException {
      if (this.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED ||
          this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
          this.taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN) {
        localizeTask(task);
        if (this.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED) {
          this.taskStatus.setRunState(TaskStatus.State.RUNNING);
        }
        setTaskRunner(task.createRunner(TaskTracker.this, this));
        this.runner.start();
        this.taskStatus.setStartTime(System.currentTimeMillis());
      } else {
        LOG.info("Not launching task: " + task.getTaskID() + 
            " since it's state is " + this.taskStatus.getRunState());
      }
    }

    boolean isCleaningup() {
   	  return this.taskStatus.inTaskCleanupPhase();
    }
    
    // checks if state has been changed for the task to be launched
    boolean canBeLaunched() {
      return (getRunState() == TaskStatus.State.UNASSIGNED ||
          getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
          getRunState() == TaskStatus.State.KILLED_UNCLEAN);
    }

    /**
     * The task is reporting its progress
     */
    public synchronized void reportProgress(TaskStatus taskStatus) 
    {
      LOG.info(task.getTaskID() + " " + taskStatus.getProgress() + 
          "% " + taskStatus.getStateString());
      // task will report its state as
      // COMMIT_PENDING when it is waiting for commit response and 
      // when it is committing.
      // cleanup attempt will report its state as FAILED_UNCLEAN/KILLED_UNCLEAN
      if (this.done || 
          (this.taskStatus.getRunState() != TaskStatus.State.RUNNING &&
          this.taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
          !isCleaningup()) ||
          ((this.taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING ||
           this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
           this.taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN) &&
           taskStatus.getRunState() == TaskStatus.State.RUNNING)) {
        //make sure we ignore progress messages after a task has 
        //invoked TaskUmbilicalProtocol.done() or if the task has been
        //KILLED/FAILED/FAILED_UNCLEAN/KILLED_UNCLEAN
        //Also ignore progress update if the state change is from 
        //COMMIT_PENDING/FAILED_UNCLEAN/KILLED_UNCLEA to RUNNING
        LOG.info(task.getTaskID() + " Ignoring status-update since " +
                 ((this.done) ? "task is 'done'" : 
                                ("runState: " + this.taskStatus.getRunState()))
                 ); 
        return;
      }
      
      this.taskStatus.statusUpdate(taskStatus);
      this.lastProgressReport = System.currentTimeMillis();
    }

    /**
     */
    public long getLastProgressReport() {
      return lastProgressReport;
    }

    /**
     */
    public TaskStatus.State getRunState() {
      return taskStatus.getRunState();
    }

    /**
     * The task's configured timeout.
     * 
     * @return the task's configured timeout.
     */
    public long getTaskTimeout() {
      return taskTimeout;
    }
        
    /**
     * The task has reported some diagnostic info about its status
     */
    public synchronized void reportDiagnosticInfo(String info) {
      this.diagnosticInfo.append(info);
    }
    
    public synchronized void reportNextRecordRange(SortedRanges.Range range) {
      this.taskStatus.setNextRecordRange(range);
    }

    /**
     * The task is reporting that it's done running
     */
    public synchronized void reportDone() {
      if (isCleaningup()) {
        if (this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
          this.taskStatus.setRunState(TaskStatus.State.FAILED);
        } else if (this.taskStatus.getRunState() == 
                   TaskStatus.State.KILLED_UNCLEAN) {
          this.taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      } else {
        this.taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
      }
      this.taskStatus.setProgress(1.0f);
      this.taskStatus.setFinishTime(System.currentTimeMillis());
      this.done = true;
      jvmManager.taskFinished(runner);
      runner.signalDone();
      LOG.info("Task " + task.getTaskID() + " is done.");
      LOG.info("reported output size for " + task.getTaskID() +  "  was " + taskStatus.getOutputSize());
      myInstrumentation.statusUpdate(task, taskStatus);
    }
    
    public boolean wasKilled() {
      return wasKilled;
    }

    /**
     * A task is reporting in as 'done'.
     * 
     * We need to notify the tasktracker to send an out-of-band heartbeat.
     * If isn't <code>commitPending</code>, we need to finalize the task
     * and release the slot it's occupied.
     * 
     * @param commitPending is the task-commit pending?
     */
    void reportTaskFinished(boolean commitPending) {
      if (!commitPending) {
        try {
          taskFinished(); 
        } finally {
          releaseSlot();
        }
      }
      notifyTTAboutTaskCompletion();
    }

    /* State changes:
     * RUNNING/COMMIT_PENDING -> FAILED_UNCLEAN/FAILED/KILLED_UNCLEAN/KILLED
     * FAILED_UNCLEAN -> FAILED
     * KILLED_UNCLEAN -> KILLED 
     */
    private void setTaskFailState(boolean wasFailure) {
      // go FAILED_UNCLEAN -> FAILED and KILLED_UNCLEAN -> KILLED always
      if (taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
        taskStatus.setRunState(TaskStatus.State.FAILED);
      } else if (taskStatus.getRunState() == 
                 TaskStatus.State.KILLED_UNCLEAN) {
        taskStatus.setRunState(TaskStatus.State.KILLED);
      } else if (task.isMapOrReduce() && 
                 taskStatus.getPhase() != TaskStatus.Phase.CLEANUP) {
        if (wasFailure) {
          taskStatus.setRunState(TaskStatus.State.FAILED_UNCLEAN);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED_UNCLEAN);
        }
      } else {
        if (wasFailure) {
          taskStatus.setRunState(TaskStatus.State.FAILED);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      }
    }
    
    /**
     * The task has actually finished running.
     */
    public void taskFinished() {
      long start = System.currentTimeMillis();

      //
      // Wait until task reports as done.  If it hasn't reported in,
      // wait for a second and try again.
      //
      while (!done && (System.currentTimeMillis() - start < WAIT_FOR_DONE)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }

      //
      // Change state to success or failure, depending on whether
      // task was 'done' before terminating
      //
      boolean needCleanup = false;
      synchronized (this) {
        // Remove the task from MemoryManager, if the task SUCCEEDED or FAILED.
        // KILLED tasks are removed in method kill(), because Kill 
        // would result in launching a cleanup attempt before 
        // TaskRunner returns; if remove happens here, it would remove
        // wrong task from memory manager.
        if (done || !wasKilled) {
          removeFromMemoryManager(task.getTaskID());
        }
        if (!done) {
          if (!wasKilled) {
            failures += 1;
            setTaskFailState(true);
            // call the script here for the failed tasks.
            if (debugCommand != null) {
              try {
                runDebugScript();
              } catch (Exception e) {
                String msg =
                    "Debug-script could not be run successfully : "
                        + StringUtils.stringifyException(e);
                LOG.warn(msg);
                reportDiagnosticInfo(msg);
              }
            }
          }
          taskStatus.setProgress(0.0f);
        }
        this.taskStatus.setFinishTime(System.currentTimeMillis());
        needCleanup = (taskStatus.getRunState() == TaskStatus.State.FAILED || 
                taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
                taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN || 
                taskStatus.getRunState() == TaskStatus.State.KILLED);
      }

      //
      // If the task has failed, or if the task was killAndCleanup()'ed,
      // we should clean up right away.  We only wait to cleanup
      // if the task succeeded, and its results might be useful
      // later on to downstream job processing.
      //
      if (needCleanup) {
        removeTaskFromJob(task.getJobID(), this);
      }

      cleanup(needCleanup);
    }

    /**
     * Run the debug-script now. Because debug-script can be user code, we use
     * {@link TaskController} to execute the debug script.
     * 
     * @throws IOException
     */
    private void runDebugScript() throws IOException {
      String taskStdout ="";
      String taskStderr ="";
      String taskSyslog ="";
      String jobConf = task.getJobFile();
      try {
        // get task's stdout file 
        taskStdout = FileUtil
            .makeShellPath(TaskLog.getRealTaskLogFileLocation(task.getTaskID(),
                task.isTaskCleanupTask(), TaskLog.LogName.STDOUT));
        // get task's stderr file
        taskStderr = FileUtil
            .makeShellPath(TaskLog.getRealTaskLogFileLocation(task.getTaskID(),
                task.isTaskCleanupTask(), TaskLog.LogName.STDERR));
        // get task's syslog file
        taskSyslog = FileUtil
            .makeShellPath(TaskLog.getRealTaskLogFileLocation(task.getTaskID(),
                task.isTaskCleanupTask(), TaskLog.LogName.SYSLOG));
      } catch(Exception e){
        LOG.warn("Exception finding task's stdout/err/syslog files", e);
      }
      File workDir = new File(lDirAlloc.getLocalPathToRead(
          TaskTracker.getLocalTaskDir(task.getUser(), task.getJobID()
              .toString(), task.getTaskID().toString(), task
              .isTaskCleanupTask())
              + Path.SEPARATOR + MRConstants.WORKDIR, localJobConf).toString());
      // Build the command  
      File stdout = TaskLog.getTaskLogFile(task.getTaskID(), task
          .isTaskCleanupTask(), TaskLog.LogName.DEBUGOUT);
      // add pipes program as argument if it exists.
      String program ="";
      String executable = Submitter.getExecutable(localJobConf);
      if ( executable != null) {
        try {
          program = new URI(executable).getFragment();
        } catch (URISyntaxException ur) {
          LOG.warn("Problem in the URI fragment for pipes executable");
        }     
      }
      String [] debug = debugCommand.split(" ");
      List<String> vargs = new ArrayList<String>();
      for (String component : debug) {
        vargs.add(component);
      }
      vargs.add(taskStdout);
      vargs.add(taskStderr);
      vargs.add(taskSyslog);
      vargs.add(jobConf);
      vargs.add(program);
      DebugScriptContext context = 
        new TaskController.DebugScriptContext();
      context.args = vargs;
      context.stdout = stdout;
      context.workDir = workDir;
      context.task = task;
      getTaskController().runDebugScript(context);
      // add the lines of debug out to diagnostics
      int num = localJobConf.getInt(MRJobConfig.TASK_DEBUGOUT_LINES, -1);
      addDiagnostics(FileUtil.makeShellPath(stdout), num, "DEBUG OUT");
    }

    /**
     * Add last 'num' lines of the given file to the diagnostics.
     * if num =-1, all the lines of file are added to the diagnostics.
     * @param file The file from which to collect diagnostics.
     * @param num The number of lines to be sent to diagnostics.
     * @param tag The tag is printed before the diagnostics are printed. 
     */
    public void addDiagnostics(String file, int num, String tag) {
      RandomAccessFile rafile = null;
      try {
        rafile = new RandomAccessFile(file,"r");
        int no_lines =0;
        String line = null;
        StringBuffer tail = new StringBuffer();
        tail.append("\n-------------------- "+tag+"---------------------\n");
        String[] lines = null;
        if (num >0) {
          lines = new String[num];
        }
        while ((line = rafile.readLine()) != null) {
          no_lines++;
          if (num >0) {
            if (no_lines <= num) {
              lines[no_lines-1] = line;
            }
            else { // shift them up
              for (int i=0; i<num-1; ++i) {
                lines[i] = lines[i+1];
              }
              lines[num-1] = line;
            }
          }
          else if (num == -1) {
            tail.append(line); 
            tail.append("\n");
          }
        }
        int n = no_lines > num ?num:no_lines;
        if (num >0) {
          for (int i=0;i<n;i++) {
            tail.append(lines[i]);
            tail.append("\n");
          }
        }
        if(n!=0)
          reportDiagnosticInfo(tail.toString());
      } catch (FileNotFoundException fnfe){
        LOG.warn("File "+file+ " not found");
      } catch (IOException ioe){
        LOG.warn("Error reading file "+file);
      } finally {
         try {
           if (rafile != null) {
             rafile.close();
           }
         } catch (IOException ioe) {
           LOG.warn("Error closing file "+file);
         }
      }
    }
    
    /**
     * We no longer need anything from this task, as the job has
     * finished.  If the task is still running, kill it and clean up.
     * 
     * @param wasFailure did the task fail, as opposed to was it killed by
     *                   the framework
     */
    public void jobHasFinished(boolean wasFailure) throws IOException {
      // Kill the task if it is still running
      synchronized(this){
        if (getRunState() == TaskStatus.State.RUNNING ||
            getRunState() == TaskStatus.State.UNASSIGNED ||
            getRunState() == TaskStatus.State.COMMIT_PENDING ||
            isCleaningup()) {
          kill(wasFailure);
        }
      }
      
      // Cleanup on the finished task
      cleanup(true);
    }

    /**
     * Something went wrong and the task must be killed.
     * 
     * @param wasFailure was it a failure (versus a kill request)?
     */
    public synchronized void kill(boolean wasFailure) throws IOException {
      if (taskStatus.getRunState() == TaskStatus.State.RUNNING ||
          taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING ||
          isCleaningup()) {
        wasKilled = true;
        if (wasFailure) {
          failures += 1;
        }
        // runner could be null if task-cleanup attempt is not localized yet
        if (runner != null) {
          runner.kill();
        }
        setTaskFailState(wasFailure);
      } else if (taskStatus.getRunState() == TaskStatus.State.UNASSIGNED) {
        if (wasFailure) {
          failures += 1;
          taskStatus.setRunState(TaskStatus.State.FAILED);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      }
      taskStatus.setFinishTime(System.currentTimeMillis());
      removeFromMemoryManager(task.getTaskID());
      releaseSlot();
      myInstrumentation.statusUpdate(task, taskStatus);
      notifyTTAboutTaskCompletion();
    }
    
    private synchronized void releaseSlot() {
      if (slotTaken) {
        if (launcher != null) {
          launcher.addFreeSlots(task.getNumSlotsRequired());
        }
        slotTaken = false;
      } else {
        // wake up the launcher. it may be waiting to block slots for this task.
        if (launcher != null) {
          launcher.notifySlots();
        }        
      }
    }

    /**
     * The map output has been lost.
     */
    private synchronized void mapOutputLost(String failure
                                           ) throws IOException {
      if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING || 
          taskStatus.getRunState() == TaskStatus.State.SUCCEEDED) {
        // change status to failure
        LOG.info("Reporting output lost:"+task.getTaskID());
        taskStatus.setRunState(TaskStatus.State.FAILED);
        taskStatus.setProgress(0.0f);
        reportDiagnosticInfo("Map output lost, rescheduling: " + 
                             failure);
        runningTasks.put(task.getTaskID(), this);
        mapTotal++;
        myInstrumentation.statusUpdate(task, taskStatus);
      } else {
        LOG.warn("Output already reported lost:"+task.getTaskID());
      }
    }

    /**
     * We no longer need anything from this task.  Either the 
     * controlling job is all done and the files have been copied
     * away, or the task failed and we don't need the remains.
     * Any calls to cleanup should not lock the tip first.
     * cleanup does the right thing- updates tasks in Tasktracker
     * by locking tasktracker first and then locks the tip.
     * 
     * if needCleanup is true, the whole task directory is cleaned up.
     * otherwise the current working directory of the task 
     * i.e. &lt;taskid&gt;/work is cleaned up.
     */
    void cleanup(boolean needCleanup) {
      TaskAttemptID taskId = task.getTaskID();
      LOG.debug("Cleaning up " + taskId);


      synchronized (TaskTracker.this) {
        if (needCleanup) {
          // see if tasks data structure is holding this tip.
          // tasks could hold the tip for cleanup attempt, if cleanup attempt 
          // got launched before this method.
          if (tasks.get(taskId) == this) {
            tasks.remove(taskId);
          }
        }
        synchronized (this){
          if (alwaysKeepTaskFiles ||
              (taskStatus.getRunState() == TaskStatus.State.FAILED && 
               keepFailedTaskFiles)) {
            return;
          }
        }
      }
      synchronized (this) {
        // localJobConf could be null if localization has not happened
        // then no cleanup will be required.
        if (localJobConf == null) {
          return;
        }
        try {
          removeTaskFiles(needCleanup, taskId);
        } catch (Throwable ie) {
          LOG.info("Error cleaning up task runner: "
              + StringUtils.stringifyException(ie));
        }
      }
    }

    /**
     * Some or all of the files from this task are no longer required. Remove
     * them via CleanupQueue.
     * 
     * @param needCleanup
     * @param taskId
     * @throws IOException 
     */
    void removeTaskFiles(boolean needCleanup, TaskAttemptID taskId)
        throws IOException {
      if (needCleanup) {
        if (runner != null) {
          // cleans up the output directory of the task (where map outputs
          // and reduce inputs get stored)
          runner.close();
        }

        if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
          // No jvm reuse, remove everything
          PathDeletionContext[] contexts =
            buildTaskControllerTaskPathDeletionContexts(localFs,
                getLocalFiles(fConf, ""), task, false/* not workDir */,
                taskController);
          directoryCleanupThread.addToQueue(contexts);
        } else {
          // Jvm reuse. We don't delete the workdir since some other task
          // (running in the same JVM) might be using the dir. The JVM
          // running the tasks would clean the workdir per a task in the
          // task process itself.
          String localTaskDir =
            getLocalTaskDir(task.getUser(), task.getJobID().toString(), taskId
                .toString(), task.isTaskCleanupTask());
          PathDeletionContext[] contexts = buildPathDeletionContexts(
              localFs, getLocalFiles(defaultJobConf, localTaskDir +
                         Path.SEPARATOR + TaskTracker.JOBFILE));
          directoryCleanupThread.addToQueue(contexts);
        }
      } else {
        if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
          PathDeletionContext[] contexts =
            buildTaskControllerTaskPathDeletionContexts(localFs,
              getLocalFiles(fConf, ""), task, true /* workDir */,
              taskController);
          directoryCleanupThread.addToQueue(contexts);
        }
      }
    }
        
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof TaskInProgress) &&
        task.getTaskID().equals
        (((TaskInProgress) obj).getTask().getTaskID());
    }
        
    @Override
    public int hashCode() {
      return task.getTaskID().hashCode();
    }
  }

  /**
   * Check that the current UGI is the JVM authorized to report
   * for this particular job.
   *
   * @throws IOException for unauthorized access
   */
  private void ensureAuthorizedJVM(JobID jobId) throws IOException {
    String currentJobId = 
      UserGroupInformation.getCurrentUser().getUserName();
    if (!currentJobId.equals(jobId.toString())) {
      throw new IOException ("JVM with " + currentJobId + 
          " is not authorized for " + jobId);
    }
  }

    
  // ///////////////////////////////////////////////////////////////
  // TaskUmbilicalProtocol
  /////////////////////////////////////////////////////////////////

  /**
   * Called upon startup by the child process, to fetch Task data.
   */
  public synchronized JvmTask getTask(JvmContext context) 
  throws IOException {
    ensureAuthorizedJVM(context.jvmId.getJobId());
    JVMId jvmId = context.jvmId;
    
    // save pid of task JVM sent by child
    jvmManager.setPidToJvm(jvmId, context.pid);
    
    LOG.debug("JVM with ID : " + jvmId + " asked for a task");
    if (!jvmManager.isJvmKnown(jvmId)) {
      LOG.info("Killing unknown JVM " + jvmId);
      return new JvmTask(null, true);
    }
    RunningJob rjob = runningJobs.get(jvmId.getJobId());
    if (rjob == null) { //kill the JVM since the job is dead
      LOG.info("Killing JVM " + jvmId + " since job " + jvmId.getJobId() +
               " is dead");
      jvmManager.killJvm(jvmId);
      return new JvmTask(null, true);
    }
    TaskInProgress tip = jvmManager.getTaskForJvm(jvmId);
    if (tip == null) {
      return new JvmTask(null, false);
    }
    if (tasks.get(tip.getTask().getTaskID()) != null) { //is task still present
      LOG.info("JVM with ID: " + jvmId + " given task: " + 
          tip.getTask().getTaskID());
      return new JvmTask(tip.getTask(), false);
    } else {
      LOG.info("Killing JVM with ID: " + jvmId + " since scheduled task: " + 
          tip.getTask().getTaskID() + " is " + tip.taskStatus.getRunState());
      return new JvmTask(null, true);
    }
  }

  /**
   * Called periodically to report Task progress, from 0.0 to 1.0.
   */
  public synchronized boolean statusUpdate(TaskAttemptID taskid, 
                                              TaskStatus taskStatus) 
  throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportProgress(taskStatus);
      myInstrumentation.statusUpdate(tip.getTask(), taskStatus);
      return true;
    } else {
      LOG.warn("Progress from unknown child task: "+taskid);
      return false;
    }
  }

  /**
   * Called when the task dies before completion, and we want to report back
   * diagnostic info
   */
  public synchronized void reportDiagnosticInfo(TaskAttemptID taskid, String info) throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    internalReportDiagnosticInfo(taskid, info);
  }

  /**
   * Same as reportDiagnosticInfo but does not authorize caller. This is used
   * internally within MapReduce, whereas reportDiagonsticInfo may be called
   * via RPC.
   */
  synchronized void internalReportDiagnosticInfo(TaskAttemptID taskid, String info) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportDiagnosticInfo(info);
    } else {
      LOG.warn("Error from unknown child task: "+taskid+". Ignored.");
    }
  }
  
  public synchronized void reportNextRecordRange(TaskAttemptID taskid, 
      SortedRanges.Range range) throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportNextRecordRange(range);
    } else {
      LOG.warn("reportNextRecordRange from unknown child task: "+taskid+". " +
      		"Ignored.");
    }
  }

  /** Child checking to see if we're alive.  Normally does nothing.*/
  public synchronized boolean ping(TaskAttemptID taskid) throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    return tasks.get(taskid) != null;
  }

  /**
   * Task is reporting that it is in commit_pending
   * and it is waiting for the commit Response
   */
  public synchronized void commitPending(TaskAttemptID taskid,
                                         TaskStatus taskStatus) 
  throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    LOG.info("Task " + taskid + " is in commit-pending," +"" +
             " task state:" +taskStatus.getRunState());
    statusUpdate(taskid, taskStatus);
    reportTaskFinished(taskid, true);
  }
  
  /**
   * Child checking whether it can commit 
   */
  public synchronized boolean canCommit(TaskAttemptID taskid) {
    return commitResponses.contains(taskid); //don't remove it now
  }
  
  /**
   * The task is done.
   */
  public synchronized void done(TaskAttemptID taskid) 
  throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    commitResponses.remove(taskid);
    if (tip != null) {
      tip.reportDone();
    } else {
      LOG.warn("Unknown child task done: "+taskid+". Ignored.");
    }
  }


  /** 
   * A reduce-task failed to shuffle the map-outputs. Kill the task.
   */  
  public synchronized void shuffleError(TaskAttemptID taskId, String message) 
  throws IOException { 
    ensureAuthorizedJVM(taskId.getJobID());
    LOG.fatal("Task: " + taskId + " - Killed due to Shuffle Failure: " + message);
    TaskInProgress tip = runningTasks.get(taskId);
    tip.reportDiagnosticInfo("Shuffle Error: " + message);
    purgeTask(tip, true);
  }

  /** 
   * A child task had a local filesystem error. Kill the task.
   */  
  public synchronized void fsError(TaskAttemptID taskId, String message) 
  throws IOException {
    ensureAuthorizedJVM(taskId.getJobID());
    internalFsError(taskId, message);
  }

  /**
   * Version of fsError() that does not do authorization checks, called by
   * the TaskRunner.
   */
  synchronized void internalFsError(TaskAttemptID taskId, String message)
  throws IOException {
    LOG.fatal("Task: " + taskId + " - Killed due to FSError: " + message);
    TaskInProgress tip = runningTasks.get(taskId);
    tip.reportDiagnosticInfo("FSError: " + message);
    purgeTask(tip, true);
  }

  /** 
   * A child task had a fatal error. Kill the task.
   */  
  public synchronized void fatalError(TaskAttemptID taskId, String msg) 
  throws IOException {
    ensureAuthorizedJVM(taskId.getJobID());
    LOG.fatal("Task: " + taskId + " - exited : " + msg);
    TaskInProgress tip = runningTasks.get(taskId);
    tip.reportDiagnosticInfo("Error: " + msg);
    purgeTask(tip, true);
  }

  public synchronized MapTaskCompletionEventsUpdate getMapCompletionEvents(
      JobID jobId, int fromEventId, int maxLocs, TaskAttemptID id) 
  throws IOException {
    TaskCompletionEvent[]mapEvents = TaskCompletionEvent.EMPTY_ARRAY;
    synchronized (shouldReset) {
      if (shouldReset.remove(id)) {
        return new MapTaskCompletionEventsUpdate(mapEvents, true);
      }
    }
    RunningJob rjob;
    synchronized (runningJobs) {
      rjob = runningJobs.get(jobId);          
      if (rjob != null) {
        synchronized (rjob) {
          FetchStatus f = rjob.getFetchStatus();
          if (f != null) {
            mapEvents = f.getMapEvents(fromEventId, maxLocs);
          }
        }
      }
    }
    return new MapTaskCompletionEventsUpdate(mapEvents, false);
  }
    
  /////////////////////////////////////////////////////
  //  Called by TaskTracker thread after task process ends
  /////////////////////////////////////////////////////
  /**
   * The task is no longer running.  It may not have completed successfully
   */
  void reportTaskFinished(TaskAttemptID taskid, boolean commitPending) {
    TaskInProgress tip;
    synchronized (this) {
      tip = tasks.get(taskid);
    }
    if (tip != null) {
      tip.reportTaskFinished(commitPending);
    } else {
      LOG.warn("Unknown child task finished: "+taskid+". Ignored.");
    }
  }
  

  /**
   * A completed map task's output has been lost.
   */
  public synchronized void mapOutputLost(TaskAttemptID taskid,
                                         String errorMsg) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.mapOutputLost(errorMsg);
    } else {
      LOG.warn("Unknown child with bad map output: "+taskid+". Ignored.");
    }
  }
    
  /**
   *  The datastructure for initializing a job
   */
  static class RunningJob{
    private JobID jobid; 
    private JobConf jobConf;
    // keep this for later use
    volatile Set<TaskInProgress> tasks;
    boolean localized;
    boolean keepJobFiles;
    UserGroupInformation ugi;
    FetchStatus f;
    RunningJob(JobID jobid) {
      this.jobid = jobid;
      localized = false;
      tasks = new HashSet<TaskInProgress>();
      keepJobFiles = false;
    }
      
    JobID getJobID() {
      return jobid;
    }
    
    UserGroupInformation getUGI() {
      return ugi;
    }
      
    void setFetchStatus(FetchStatus f) {
      this.f = f;
    }
      
    FetchStatus getFetchStatus() {
      return f;
    }

    JobConf getJobConf() {
      return jobConf;
    }
  }

  /**
   * Get the name for this task tracker.
   * @return the string like "tracker_mymachine:50010"
   */
  String getName() {
    return taskTrackerName;
  }
    
  private synchronized List<TaskStatus> cloneAndResetRunningTaskStatuses(
                                          boolean sendCounters) {
    List<TaskStatus> result = new ArrayList<TaskStatus>(runningTasks.size());
    for(TaskInProgress tip: runningTasks.values()) {
      TaskStatus status = tip.getStatus();
      status.setIncludeCounters(sendCounters);
      // send counters for finished or failed tasks and commit pending tasks
      if (status.getRunState() != TaskStatus.State.RUNNING) {
        status.setIncludeCounters(true);
      }
      result.add((TaskStatus)status.clone());
      status.clearStatus();
    }
    return result;
  }
  /**
   * Get the list of tasks that will be reported back to the 
   * job tracker in the next heartbeat cycle.
   * @return a copy of the list of TaskStatus objects
   */
  synchronized List<TaskStatus> getRunningTaskStatuses() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(runningTasks.size());
    for(TaskInProgress tip: runningTasks.values()) {
      result.add(tip.getStatus());
    }
    return result;
  }

  /**
   * Get the list of stored tasks on this task tracker.
   * @return
   */
  synchronized List<TaskStatus> getNonRunningTasks() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(tasks.size());
    for(Map.Entry<TaskAttemptID, TaskInProgress> task: tasks.entrySet()) {
      if (!runningTasks.containsKey(task.getKey())) {
        result.add(task.getValue().getStatus());
      }
    }
    return result;
  }


  /**
   * Get the list of tasks from running jobs on this task tracker.
   * @return a copy of the list of TaskStatus objects
   */
  synchronized List<TaskStatus> getTasksFromRunningJobs() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(tasks.size());
    for (Map.Entry <JobID, RunningJob> item : runningJobs.entrySet()) {
      RunningJob rjob = item.getValue();
      synchronized (rjob) {
        for (TaskInProgress tip : rjob.tasks) {
          result.add(tip.getStatus());
        }
      }
    }
    return result;
  }
  
  /**
   * Get the default job conf for this tracker.
   */
  JobConf getJobConf() {
    return fConf;
  }
    
  /**
   * Check if the given local directories
   * (and parent directories, if necessary) can be created.
   * @param localDirs where the new TaskTracker should keep its local files.
   * @throws DiskErrorException if all local directories are not writable
   */
  private static void checkLocalDirs(String[] localDirs) 
    throws DiskErrorException {
    boolean writable = false;
        
    if (localDirs != null) {
      for (int i = 0; i < localDirs.length; i++) {
        try {
          DiskChecker.checkDir(new File(localDirs[i]));
          writable = true;
        } catch(DiskErrorException e) {
          LOG.warn("Task Tracker local " + e.getMessage());
        }
      }
    }

    if (!writable)
      throw new DiskErrorException(
                                   "all local directories are not writable");
  }
    
  /**
   * Is this task tracker idle?
   * @return has this task tracker finished and cleaned up all of its tasks?
   */
  public synchronized boolean isIdle() {
    return tasks.isEmpty() && tasksToCleanup.isEmpty();
  }
    
  /**
   * Start the TaskTracker, point toward the indicated JobTracker
   */
  public static void main(String argv[]) throws Exception {
    StringUtils.startupShutdownMessage(TaskTracker.class, argv, LOG);
    if (argv.length != 0) {
      System.out.println("usage: TaskTracker");
      System.exit(-1);
    }
    try {
      JobConf conf=new JobConf();
      // enable the server to track time spent waiting on locks
      ReflectionUtils.setContentionTracing
        (conf.getBoolean(TT_CONTENTION_TRACKING, false));
      new TaskTracker(conf).run();
    } catch (Throwable e) {
      LOG.error("Can not start task tracker because "+
                StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  /**
   * This class is used in TaskTracker's Jetty to serve the map outputs
   * to other nodes.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class MapOutputServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final int MAX_BYTES_TO_READ = 64 * 1024;
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      long start = System.currentTimeMillis();
      String mapIds = request.getParameter("map");
      String reduceId = request.getParameter("reduce");
      String jobId = request.getParameter("job");

      LOG.debug("Shuffle started for maps (mapIds=" + mapIds + ") to reduce " + 
               reduceId);

      if (jobId == null) {
        throw new IOException("job parameter is required");
      }

      if (mapIds == null || reduceId == null) {
        throw new IOException("map and reduce parameters are required");
      }
      
      ServletContext context = getServletContext();
      int reduce = Integer.parseInt(reduceId);
      DataOutputStream outStream = null;
 
      ShuffleServerMetrics shuffleMetrics =
        (ShuffleServerMetrics) context.getAttribute("shuffleServerMetrics");
      TaskTracker tracker = 
        (TaskTracker) context.getAttribute("task.tracker");
      String exceptionStackRegex =
        (String) context.getAttribute("exceptionStackRegex");
      String exceptionMsgRegex =
        (String) context.getAttribute("exceptionMsgRegex");

      verifyRequest(request, response, tracker, jobId);
      
      int numMaps = 0;
      try {
        shuffleMetrics.serverHandlerBusy();
        response.setContentType("application/octet-stream");

        outStream = new DataOutputStream(response.getOutputStream());
        //use the same buffersize as used for reading the data from disk
        response.setBufferSize(MAX_BYTES_TO_READ);
        JobConf conf = (JobConf) context.getAttribute("conf");
        LocalDirAllocator lDirAlloc = 
          (LocalDirAllocator)context.getAttribute("localDirAllocator");
        FileSystem rfs = ((LocalFileSystem)
            context.getAttribute("local.file.system")).getRaw();

        // Split the map ids, send output for one map at a time
        StringTokenizer itr = new StringTokenizer(mapIds, ",");
        while(itr.hasMoreTokens()) {
          String mapId = itr.nextToken();
          ++numMaps;
          sendMapFile(jobId, mapId, reduce, conf, outStream,
                      tracker, lDirAlloc, shuffleMetrics, rfs);
        }
      } catch (IOException ie) {
        Log log = (Log) context.getAttribute("log");
        String errorMsg = ("getMapOutputs(" + mapIds + "," + reduceId + 
                           ") failed");
        log.warn(errorMsg, ie);
        checkException(ie, exceptionMsgRegex, exceptionStackRegex, shuffleMetrics);
        response.sendError(HttpServletResponse.SC_GONE, errorMsg);
        shuffleMetrics.failedOutput();
        throw ie;
      } finally {
        shuffleMetrics.serverHandlerFree();
      }
      outStream.close();
      shuffleMetrics.successOutput();
      long timeElapsed = (System.currentTimeMillis()-start);
      LOG.info("Shuffled " + numMaps
          + "maps (mapIds=" + mapIds + ") to reduce "
          + reduceId + " in " + timeElapsed + "s");

      if (ClientTraceLog.isInfoEnabled()) {
        ClientTraceLog.info(String.format(MR_CLIENTTRACE_FORMAT,
            request.getLocalAddr() + ":" + request.getLocalPort(),
            request.getRemoteAddr() + ":" + request.getRemotePort(),
            numMaps, "MAPRED_SHUFFLE", reduceId,
            timeElapsed));
      }
    }

    protected void checkException(IOException ie, String exceptionMsgRegex,
        String exceptionStackRegex, ShuffleServerMetrics shuffleMetrics) {
      // parse exception to see if it looks like a regular expression you
      // configure. If both msgRegex and StackRegex set then make sure both
      // match, otherwise only the one set has to match.
      if (exceptionMsgRegex != null) {
        String msg = ie.getMessage();
        if (msg == null || !msg.matches(exceptionMsgRegex)) {
          return;
        }
      }
      if (exceptionStackRegex != null
          && !checkStackException(ie, exceptionStackRegex)) {
        return;
      }
      shuffleMetrics.exceptionsCaught();
    }

    private boolean checkStackException(IOException ie,
        String exceptionStackRegex) {
      StackTraceElement[] stack = ie.getStackTrace();

      for (StackTraceElement elem : stack) {
        String stacktrace = elem.toString();
        if (stacktrace.matches(exceptionStackRegex)) {
          return true;
        }
      }
      return false;
    }

    private void sendMapFile(String jobId, String mapId,
                             int reduce,
                             Configuration conf,
                             DataOutputStream outStream,
                             TaskTracker tracker,
                             LocalDirAllocator lDirAlloc,
                             ShuffleServerMetrics shuffleMetrics,
                             FileSystem localfs
                             ) throws IOException {
      
      LOG.debug("sendMapFile called for " + mapId + " to reduce " + reduce);
      
      // true iff IOException was caused by attempt to access input
      boolean isInputException = false;
      FileInputStream mapOutputIn = null;
      byte[] buffer = new byte[MAX_BYTES_TO_READ];
      long totalRead = 0;

      String userName = null;
      String runAsUserName = null;
      synchronized (tracker.runningJobs) {
        RunningJob rjob = tracker.runningJobs.get(JobID.forName(jobId));
        if (rjob == null) {
          throw new IOException("Unknown job " + jobId + "!!");
        }
        userName = rjob.jobConf.getUser();
        runAsUserName = tracker.getTaskController().getRunAsUser(rjob.jobConf);
      }
      // Index file
      Path indexFileName =
          lDirAlloc.getLocalPathToRead(TaskTracker.getIntermediateOutputDir(
              userName, jobId, mapId)
              + "/file.out.index", conf);

      // Map-output file
      Path mapOutputFileName =
          lDirAlloc.getLocalPathToRead(TaskTracker.getIntermediateOutputDir(
              userName, jobId, mapId)
              + "/file.out", conf);

      /**
       * Read the index file to get the information about where the map-output
       * for the given reducer is available.
       */
      IndexRecord info = 
        tracker.indexCache.getIndexInformation(mapId, reduce, indexFileName,
            runAsUserName);

      try {
        /**
         * Read the data from the single map-output file and
         * send it to the reducer.
         */
        //open the map-output file
        mapOutputIn = SecureIOUtils.openForRead(
            new File(mapOutputFileName.toUri().getPath()), runAsUserName, null);
        //seek to the correct offset for the reduce
        IOUtils.skipFully(mapOutputIn, info.startOffset);
        
        // write header for each map output
        ShuffleHeader header = new ShuffleHeader(mapId, info.partLength,
            info.rawLength, reduce);
        header.write(outStream);

        // read the map-output and stream it out
        isInputException = true;
        long rem = info.partLength;
        if (rem == 0) {
          throw new IOException("Illegal partLength of 0 for mapId " + mapId + 
                                " to reduce " + reduce);
        }
        int len =
          mapOutputIn.read(buffer, 0, (int)Math.min(rem, MAX_BYTES_TO_READ));
        long now = 0;
        while (len >= 0) {
          rem -= len;
          try {
            shuffleMetrics.outputBytes(len);
            
            if (len > 0) {
              outStream.write(buffer, 0, len);
            } else {
              LOG.info("Skipped zero-length read of map " + mapId + 
                       " to reduce " + reduce);
            }
            
          } catch (IOException ie) {
            isInputException = false;
            throw ie;
          }
          totalRead += len;
          if (rem == 0) {
            break;
          }
          len =
            mapOutputIn.read(buffer, 0, (int)Math.min(rem, MAX_BYTES_TO_READ));
        }
        try {
          outStream.flush();
        } catch (IOException ie) {
          isInputException = false;
          throw ie;
        }
      } catch (IOException ie) {
        String errorMsg = "error on sending map " + mapId + " to reduce " + 
                          reduce;
        if (isInputException) {
          tracker.mapOutputLost(TaskAttemptID.forName(mapId), errorMsg + 
                                StringUtils.stringifyException(ie));
        }
        throw new IOException(errorMsg, ie);
      } finally {
        if (mapOutputIn != null) {
          try {
            mapOutputIn.close();
          } catch (IOException ioe) {
            LOG.info("problem closing map output file", ioe);
          }
        }
      }
      
      LOG.info("Sent out " + totalRead + " bytes to reduce " + reduce + 
          " from map: " + mapId + " given " + info.partLength + "/" + 
          info.rawLength);
    }
    
    /**
     * verify that request has correct HASH for the url
     * and also add a field to reply header with hash of the HASH
     * @param request
     * @param response
     * @param jt the job token
     * @throws IOException
     */
    private void verifyRequest(HttpServletRequest request, 
        HttpServletResponse response, TaskTracker tracker, String jobId) 
    throws IOException {
      SecretKey tokenSecret = tracker.getJobTokenSecretManager()
          .retrieveTokenSecret(jobId);
      // string to encrypt
      String enc_str = SecureShuffleUtils.buildMsgFrom(request);
      
      // hash from the fetcher
      String urlHashStr = request.getHeader(SecureShuffleUtils.HTTP_HEADER_URL_HASH);
      if(urlHashStr == null) {
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        throw new IOException("fetcher cannot be authenticated");
      }
      int len = urlHashStr.length();
      LOG.debug("verifying request. enc_str="+enc_str+"; hash=..."+
          urlHashStr.substring(len-len/2, len-1)); // half of the hash for debug

      // verify - throws exception
      try {
        SecureShuffleUtils.verifyReply(urlHashStr, enc_str, tokenSecret);
      } catch (IOException ioe) {
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        throw ioe;
      }
      
      // verification passed - encode the reply
      String reply = SecureShuffleUtils.generateHash(urlHashStr.getBytes(), tokenSecret);
      response.addHeader(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH, reply);
      
      len = reply.length();
      LOG.debug("Fetcher request verfied. enc_str="+enc_str+";reply="
          +reply.substring(len-len/2, len-1));
    }
  }
  
  // get the full paths of the directory in all the local disks.
  Path[] getLocalFiles(JobConf conf, String subdir) throws IOException{
    String[] localDirs = conf.getLocalDirs();
    Path[] paths = new Path[localDirs.length];
    FileSystem localFs = FileSystem.getLocal(conf);
    boolean subdirNeeded = (subdir != null) && (subdir.length() > 0);
    for (int i = 0; i < localDirs.length; i++) {
      paths[i] = (subdirNeeded) ? new Path(localDirs[i], subdir)
                                : new Path(localDirs[i]);
      paths[i] = paths[i].makeQualified(localFs);
    }
    return paths;
  }

  FileSystem getLocalFileSystem(){
    return localFs;
  }

  // only used by tests
  void setLocalFileSystem(FileSystem fs){
    localFs = fs;
  }

  int getMaxCurrentMapTasks() {
    return maxMapSlots;
  }
  
  int getMaxCurrentReduceTasks() {
    return maxReduceSlots;
  }

  //called from unit test
  synchronized void setMaxMapSlots(int mapSlots) {
    maxMapSlots = mapSlots;
  }

  //called from unit test
  synchronized void setMaxReduceSlots(int reduceSlots) {
    maxReduceSlots = reduceSlots;
  }

  /**
   * Is the TaskMemoryManager Enabled on this system?
   * @return true if enabled, false otherwise.
   */
  public boolean isTaskMemoryManagerEnabled() {
    return taskMemoryManagerEnabled;
  }
  
  public TaskMemoryManagerThread getTaskMemoryManager() {
    return taskMemoryManager;
  }

  /**
   * Normalize the negative values in configuration
   * 
   * @param val
   * @return normalized val
   */
  private long normalizeMemoryConfigValue(long val) {
    if (val < 0) {
      val = JobConf.DISABLED_MEMORY_LIMIT;
    }
    return val;
  }

  /**
   * Memory-related setup
   */
  private void initializeMemoryManagement() {

    //handling @deprecated
    if (fConf.get(MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY));
    }

    // Use TT_MEMORY_CALCULATOR_PLUGIN if it is configured.
    Class<? extends MemoryCalculatorPlugin> clazz = 
        fConf.getClass(TT_MEMORY_CALCULATOR_PLUGIN, 
            null, MemoryCalculatorPlugin.class); 
    MemoryCalculatorPlugin memoryCalculatorPlugin = (clazz == null ?
        null : MemoryCalculatorPlugin.getMemoryCalculatorPlugin(clazz, fConf)); 
    if (memoryCalculatorPlugin != null || resourceCalculatorPlugin != null) {
      totalVirtualMemoryOnTT = (memoryCalculatorPlugin == null ?
          resourceCalculatorPlugin.getVirtualMemorySize() :
          memoryCalculatorPlugin.getVirtualMemorySize());
      if (totalVirtualMemoryOnTT <= 0) {
        LOG.warn("TaskTracker's totalVmem could not be calculated. "
            + "Setting it to " + JobConf.DISABLED_MEMORY_LIMIT);
        totalVirtualMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
      }
      totalPhysicalMemoryOnTT = (memoryCalculatorPlugin == null ?
          resourceCalculatorPlugin.getPhysicalMemorySize() :
          memoryCalculatorPlugin.getPhysicalMemorySize());
      if (totalPhysicalMemoryOnTT <= 0) {
        LOG.warn("TaskTracker's totalPmem could not be calculated. "
            + "Setting it to " + JobConf.DISABLED_MEMORY_LIMIT);
        totalPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
      }
    }

    mapSlotMemorySizeOnTT =
        fConf.getLong(
            MAPMEMORY_MB,
            JobConf.DISABLED_MEMORY_LIMIT);
    reduceSlotSizeMemoryOnTT =
        fConf.getLong(
            REDUCEMEMORY_MB,
            JobConf.DISABLED_MEMORY_LIMIT);
    totalMemoryAllottedForTasks =
        maxMapSlots * mapSlotMemorySizeOnTT + maxReduceSlots
            * reduceSlotSizeMemoryOnTT;
    if (totalMemoryAllottedForTasks < 0) {
      //adding check for the old keys which might be used by the administrator
      //while configuration of the memory monitoring on TT
      long memoryAllotedForSlot = fConf.normalizeMemoryConfigValue(
          fConf.getLong(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY, 
              JobConf.DISABLED_MEMORY_LIMIT));
      long limitVmPerTask = fConf.normalizeMemoryConfigValue(
          fConf.getLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, 
              JobConf.DISABLED_MEMORY_LIMIT));
      if(memoryAllotedForSlot == JobConf.DISABLED_MEMORY_LIMIT) {
        totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT; 
      } else {
        if(memoryAllotedForSlot > limitVmPerTask) {
          LOG.info("DefaultMaxVmPerTask is mis-configured. " +
          		"It shouldn't be greater than task limits");
          totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;
        } else {
          totalMemoryAllottedForTasks = (maxMapSlots + 
              maxReduceSlots) *  (memoryAllotedForSlot/(1024 * 1024));
        }
      }
    }
    if (totalMemoryAllottedForTasks > totalPhysicalMemoryOnTT) {
      LOG.info("totalMemoryAllottedForTasks > totalPhysicalMemoryOnTT."
          + " Thrashing might happen.");
    } else if (totalMemoryAllottedForTasks > totalVirtualMemoryOnTT) {
      LOG.info("totalMemoryAllottedForTasks > totalVirtualMemoryOnTT."
          + " Thrashing might happen.");
    }

    reservedPhysicalMemoryOnTT =
      fConf.getLong(TTConfig.TT_RESERVED_PHYSCIALMEMORY_MB,
                    JobConf.DISABLED_MEMORY_LIMIT);
    reservedPhysicalMemoryOnTT =
      reservedPhysicalMemoryOnTT == JobConf.DISABLED_MEMORY_LIMIT ?
      JobConf.DISABLED_MEMORY_LIMIT :
      reservedPhysicalMemoryOnTT * 1024 * 1024; // normalize to bytes

    // start the taskMemoryManager thread only if enabled
    setTaskMemoryManagerEnabledFlag();
    if (isTaskMemoryManagerEnabled()) {
      taskMemoryManager = new TaskMemoryManagerThread(this);
      taskMemoryManager.setDaemon(true);
      taskMemoryManager.start();
    }
  }

  void setTaskMemoryManagerEnabledFlag() {
    if (!ProcfsBasedProcessTree.isAvailable()) {
      LOG.info("ProcessTree implementation is missing on this system. "
          + "TaskMemoryManager is disabled.");
      taskMemoryManagerEnabled = false;
      return;
    }

    if (reservedPhysicalMemoryOnTT == JobConf.DISABLED_MEMORY_LIMIT
        && totalMemoryAllottedForTasks == JobConf.DISABLED_MEMORY_LIMIT) {
      taskMemoryManagerEnabled = false;
      LOG.warn("TaskTracker's totalMemoryAllottedForTasks is -1 and " +
               "reserved physical memory is not configured. " +
               "TaskMemoryManager is disabled.");
      return;
    }

    taskMemoryManagerEnabled = true;
  }

  /**
   * Clean-up the task that TaskMemoryMangerThread requests to do so.
   * @param tid
   * @param wasFailure mark the task as failed or killed. 'failed' if true,
   *          'killed' otherwise
   * @param diagnosticMsg
   */
  synchronized void cleanUpOverMemoryTask(TaskAttemptID tid, boolean wasFailure,
      String diagnosticMsg) {
    TaskInProgress tip = runningTasks.get(tid);
    if (tip != null) {
      tip.reportDiagnosticInfo(diagnosticMsg);
      try {
        purgeTask(tip, wasFailure); // Marking it as failed/killed.
      } catch (IOException ioe) {
        LOG.warn("Couldn't purge the task of " + tid + ". Error : " + ioe);
      }
    }
  }
  
  /**
   * Wrapper method used by TaskTracker to check if {@link  NodeHealthCheckerService}
   * can be started
   * @param conf configuration used to check if service can be started
   * @return true if service can be started
   */
  private boolean shouldStartHealthMonitor(Configuration conf) {
    return NodeHealthCheckerService.shouldRun(conf);
  }
  
  /**
   * Wrapper method used to start {@link NodeHealthCheckerService} for 
   * Task Tracker
   * @param conf Configuration used by the service.
   */
  private void startHealthMonitor(Configuration conf) {
    healthChecker = new NodeHealthCheckerService(conf);
    healthChecker.start();
  }

  TrackerDistributedCacheManager getTrackerDistributedCacheManager() {
    return distributedCacheManager;
  }
  
    /**
     * Download the job-token file from the FS and save on local fs.
     * @param user
     * @param jobId
     * @param jobConf
     * @return the local file system path of the downloaded file.
     * @throws IOException
     */
    private String localizeJobTokenFile(String user, JobID jobId)
        throws IOException {
      // check if the tokenJob file is there..
      Path skPath = new Path(systemDirectory, 
          jobId.toString()+"/"+TokenCache.JOB_TOKEN_HDFS_FILE);
      
      FileStatus status = null;
      long jobTokenSize = -1;
      status = systemFS.getFileStatus(skPath); //throws FileNotFoundException
      jobTokenSize = status.getLen();
      
      Path localJobTokenFile =
          lDirAlloc.getLocalPathForWrite(getLocalJobTokenFile(user, 
              jobId.toString()), jobTokenSize, fConf);
      String localJobTokenFileStr = localJobTokenFile.toUri().getPath();
      LOG.debug("localizingJobTokenFile from sd="+skPath.toUri().getPath() + 
          " to " + localJobTokenFileStr);
      
      // Download job_token
      systemFS.copyToLocalFile(skPath, localJobTokenFile);      
      return localJobTokenFileStr;
    }

    JobACLsManager getJobACLsManager() {
      return aclsManager.getJobACLsManager();
    }
    
    ACLsManager getACLsManager() {
      return aclsManager;
    }
}
