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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.TreeMap;
import java.util.Vector;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.mapreduce.server.tasktracker.*;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.*;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.TaskController.DeletionContext;
import org.apache.hadoop.mapred.TaskLog.LogFileDetail;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.MemoryCalculatorPlugin;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.security.Credentials;

/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 *******************************************************/
public class TaskTracker implements MRConstants, TaskUmbilicalProtocol,
    Runnable, TaskTrackerMXBean {
  
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

  static final String CONF_VERSION_KEY = "mapreduce.tasktracker.conf.version";
  static final String CONF_VERSION_DEFAULT = "default";

  static final long WAIT_FOR_DONE = 3 * 1000;
  private int httpPort;

  static enum State {NORMAL, STALE, INTERRUPTED, DENIED}

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  public static final Log LOG =
    LogFactory.getLog(TaskTracker.class);

  public static final String MR_CLIENTTRACE_FORMAT =
        "src: %s" +     // src IP
        ", dest: %s" +  // dst IP
        ", bytes: %s" + // byte count
        ", op: %s" +    // operation
        ", cliID: %s" +  // task id
        ", duration: %s"; // duration
  public static final Log ClientTraceLog =
    LogFactory.getLog(TaskTracker.class.getName() + ".clienttrace");

  //Job ACLs file is created by TaskController under userlogs/$jobid directory
  //for each job at job localization time. This will be used by TaskLogServlet 
  //for authorizing viewing of task logs of that job
  static String jobACLsFile = "job-acls.xml";

  volatile boolean running = true;

  private LocalDirAllocator localDirAllocator;
  private String[] localdirs;
  String taskTrackerName;
  String localHostname;
  InetSocketAddress jobTrackAddr;
    
  InetSocketAddress taskReportAddress;

  Server taskReportServer = null;
  InterTrackerProtocol jobClient;
  
  private TrackerDistributedCacheManager distributedCacheManager;
    
  // last heartbeat response recieved
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
  private FileSystem localFs = null;
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

  JobTokenSecretManager getJobTokenSecretManager() {
    return jobTokenSecretManager;
  }

  RunningJob getRunningJob(JobID jobId) {
    return runningJobs.get(jobId);
  }

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
  static final String JARSDIR = "jars";
  static final String LOCAL_SPLIT_FILE = "split.info";
  static final String JOBFILE = "job.xml";
  static final String TT_PRIVATE_DIR = "ttprivate";
  static final String JVM_EXTRA_ENV_FILE = "jvm.extra.env";

  static final String JOB_LOCAL_DIR = "job.local.dir";
  static final String JOB_TOKEN_FILE="jobToken"; //localized file

  private JobConf fConf;
  private JobConf originalConf;
  private Localizer localizer;
  private int maxMapSlots;
  private int maxReduceSlots;
  private int failures;

  private ACLsManager aclsManager;
  
  // Performance-related config knob to send an out-of-band heartbeat
  // on task completion
  static final String TT_OUTOFBAND_HEARBEAT =
    "mapreduce.tasktracker.outofband.heartbeat";
  private volatile boolean oobHeartbeatOnTaskCompletion;
  
  // Track number of completed tasks to send an out-of-band heartbeat
  private IntWritable finishedCount = new IntWritable(0);
  
  private MapEventsFetcherThread mapEventsFetcher;
  final int workerThreads;
  CleanupQueue directoryCleanupThread;
  private volatile JvmManager jvmManager;
  
  private TaskMemoryManagerThread taskMemoryManager;
  private boolean taskMemoryManagerEnabled = true;
  private long totalVirtualMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long mapSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long reduceSlotSizeMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;

  private UserLogManager userLogManager;

  static final String MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY =
      "mapred.tasktracker.memory_calculator_plugin";

  /**
   * the minimum interval between jobtracker polls
   */
  private volatile int heartbeatInterval = HEARTBEAT_INTERVAL_MIN;
  /**
   * Number of maptask completion events locations to poll for at one time
   */  
  private int probe_sample_size = 500;

  private IndexCache indexCache;

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

  private ShuffleServerInstrumentation shuffleServerMetrics;

  private TaskTrackerInstrumentation myInstrumentation = null;

  public TaskTrackerInstrumentation getTaskTrackerInstrumentation() {
    return myInstrumentation;
  }
  
  /**
   * A list of tips that should be cleaned up.
   */
  private BlockingQueue<TaskTrackerAction> tasksToCleanup = 
    new LinkedBlockingQueue<TaskTrackerAction>();
    
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
    LOG.info("Received KillTaskAction for task: " + killAction.getTaskID());
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

  UserLogManager getUserLogManager() {
    return this.userLogManager;
  }

  void setUserLogManager(UserLogManager u) {
    this.userLogManager = u;
  }

  public static String getUserDir(String user) {
    return TaskTracker.SUBDIR + Path.SEPARATOR + user;
  } 

  Localizer getLocalizer() {
    return localizer;
  }

  void setLocalizer(Localizer l) {
    localizer = l;
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
  
  static String getPrivateDirJobConfFile(String user, String jobid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + getLocalJobConfFile(user, jobid);
  }

  static String getTaskConfFile(String user, String jobid, String taskid,
      boolean isCleanupAttempt) {
    return getLocalTaskDir(user, jobid, taskid, isCleanupAttempt)
    + Path.SEPARATOR + TaskTracker.JOBFILE;
  }
  
  static String getPrivateDirTaskScriptLocation(String user, String jobid, 
     String taskid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + 
           getLocalTaskDir(user, jobid, taskid);
  }

  static String getJobJarsDir(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.JARSDIR;
  }

  public static String getJobJarFile(String user, String jobid) {
    return getJobJarsDir(user, jobid) + Path.SEPARATOR + "job.jar";
  }
  
  static String getJobWorkDir(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + MRConstants.WORKDIR;
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

  public static String getLocalTaskDir(String user, String jobid, 
      String taskid) {
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

  static String getLocalJobTokenFile(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.JOB_TOKEN_FILE;
  }
  
  static String getPrivateDirJobTokenFile(String user, String jobid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + 
           getLocalJobTokenFile(user, jobid); 
  }
  
  static String getPrivateDirForJob(String user, String jobid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + getLocalJobDir(user, jobid) ;
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

  /**
   * Delete all of the user directories.
   * @param conf the TT configuration
   * @throws IOException
   */
  private void deleteUserDirectories(Configuration conf) throws IOException {
    for(String root: localdirs) {
      for(FileStatus status: localFs.listStatus(new Path(root, SUBDIR))) {
        String owner = status.getOwner();
        String path = status.getPath().getName();
        if (path.endsWith("/" + owner)) {
          taskController.deleteAsUser(owner, "");
        }
      }
    }
  }

  public static final String TT_USER_NAME = "mapreduce.tasktracker.kerberos.principal";
  public static final String TT_KEYTAB_FILE =
    "mapreduce.tasktracker.keytab.file";  
  /**
   * Do the real constructor work here.  It's in a separate method
   * so we can call it again and "recycle" the object after calling
   * close().
   */
  synchronized void initialize() throws IOException, InterruptedException {
    this.fConf = new JobConf(originalConf);
    
    LOG.info("Starting tasktracker with owner as "
        + getMROwner().getShortUserName());

    localFs = FileSystem.getLocal(fConf);
    if (fConf.get("slave.host.name") != null) {
      this.localHostname = fConf.get("slave.host.name");
    }
    if (localHostname == null) {
      this.localHostname =
      DNS.getDefaultHost
      (fConf.get("mapred.tasktracker.dns.interface","default"),
       fConf.get("mapred.tasktracker.dns.nameserver","default"));
    }
 
    //check local disk
    checkLocalDirs((localdirs = this.fConf.getLocalDirs()));
    deleteUserDirectories(fConf);
    fConf.deleteLocalFiles(SUBDIR);
    final FsPermission ttdir = FsPermission.createImmutable((short) 0755);
    for (String s : localdirs) {
      localFs.mkdirs(new Path(s, SUBDIR), ttdir);
    }
    fConf.deleteLocalFiles(TT_PRIVATE_DIR);
    final FsPermission priv = FsPermission.createImmutable((short) 0700);
    for (String s : localdirs) {
      localFs.mkdirs(new Path(s, TT_PRIVATE_DIR), priv);
    }

    // Clear out state tables
    this.tasks.clear();
    this.runningTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    this.runningJobs = new TreeMap<JobID, RunningJob>();
    this.mapTotal = 0;
    this.reduceTotal = 0;
    this.acceptNewTasks = true;
    this.status = null;

    this.minSpaceStart = this.fConf.getLong("mapred.local.dir.minspacestart", 0L);
    this.minSpaceKill = this.fConf.getLong("mapred.local.dir.minspacekill", 0L);
    //tweak the probe sample size (make it a function of numCopiers)
    probe_sample_size = this.fConf.getInt("mapred.tasktracker.events.batchsize", 500);

    createInstrumentation();

    // bind address
    String address = 
      NetUtils.getServerAddress(fConf,
                                "mapred.task.tracker.report.bindAddress", 
                                "mapred.task.tracker.report.port", 
                                "mapred.task.tracker.report.address");
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
    String bindAddress = socAddr.getHostName();
    int tmpPort = socAddr.getPort();
    
    this.jvmManager = new JvmManager(this);

    // Set service-level authorization security policy
    if (this.fConf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            this.fConf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                MapReducePolicyProvider.class, PolicyProvider.class), 
            this.fConf));
      ServiceAuthorizationManager.refresh(fConf, policyProvider);
    }
    
    // RPC initialization
    int max = maxMapSlots > maxReduceSlots ? 
                       maxMapSlots : maxReduceSlots;
    //set the num handlers to max*2 since canCommit may wait for the duration
    //of a heartbeat RPC
    this.taskReportServer = RPC.getServer(this, bindAddress,
        tmpPort, 2 * max, false, this.fConf, this.jobTokenSecretManager);
    this.taskReportServer.start();

    // get the assigned address
    this.taskReportAddress = taskReportServer.getListenerAddress();
    this.fConf.set("mapred.task.tracker.report.address",
        taskReportAddress.getHostName() + ":" + taskReportAddress.getPort());
    LOG.info("TaskTracker up at: " + this.taskReportAddress);

    this.taskTrackerName = "tracker_" + localHostname + ":" + taskReportAddress;
    LOG.info("Starting tracker " + taskTrackerName);

    // Initialize DistributedCache
    this.distributedCacheManager = new TrackerDistributedCacheManager(
        this.fConf, taskController);

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

    initializeMemoryManagement();

    getUserLogManager().clearOldUserLogs(fConf);

    setIndexCache(new IndexCache(this.fConf));

    mapLauncher = new TaskLauncher(TaskType.MAP, maxMapSlots);
    reduceLauncher = new TaskLauncher(TaskType.REDUCE, maxReduceSlots);
    mapLauncher.start();
    reduceLauncher.start();

    // create a localizer instance
    setLocalizer(new Localizer(localFs, fConf.getLocalDirs()));

    //Start up node health checker service.
    if (shouldStartHealthMonitor(this.fConf)) {
      startHealthMonitor(this.fConf);
    }
    
    oobHeartbeatOnTaskCompletion = 
      fConf.getBoolean(TT_OUTOFBAND_HEARBEAT, false);
  }

  private void createInstrumentation() {
    Class<? extends TaskTrackerInstrumentation> metricsInst =
        getInstrumentationClass(fConf);
    LOG.debug("instrumentation class="+ metricsInst);
    if (metricsInst == null) {
      myInstrumentation = TaskTrackerInstrumentation.create(this);
      return;
    }
    try {
      java.lang.reflect.Constructor<? extends TaskTrackerInstrumentation> c =
        metricsInst.getConstructor(new Class<?>[] {TaskTracker.class} );
      this.myInstrumentation = c.newInstance(this);
    } catch(Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by
      //falling back on the default.
      LOG.error("failed to initialize taskTracker metrics", e);
      this.myInstrumentation = TaskTrackerInstrumentation.create(this);
    }

  }

  UserGroupInformation getMROwner() {
    return aclsManager.getMROwner();
  }

  /**
   * Are ACLs for authorization checks enabled on the TT ?
   */
  boolean areACLsEnabled() {
    return fConf.getBoolean(JobConf.MR_ACLS_ENABLED, false);
  }

  static Class<? extends TaskTrackerInstrumentation> getInstrumentationClass(
    Configuration conf) {
    return conf.getClass("mapred.tasktracker.instrumentation", null,
                         TaskTrackerInstrumentation.class);
  }

  static void setInstrumentationClass(
    Configuration conf, Class<? extends TaskTrackerInstrumentation> t) {
    conf.setClass("mapred.tasktracker.instrumentation",
        t, TaskTrackerInstrumentation.class);
  }
  
  /** 
   * Removes all contents of temporary storage.  Called upon 
   * startup, to remove any leftovers from previous run.
   */
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
                              new LocalDirAllocator("mapred.local.dir");

  // intialize the job directory
  RunningJob localizeJob(TaskInProgress tip) 
  throws IOException, InterruptedException {
    Task t = tip.getTask();
    JobID jobId = t.getJobID();
    RunningJob rjob = addTaskToJob(jobId, tip);

    synchronized (rjob) {
      if (!rjob.localized) {
        Path localJobConfPath = initializeJob(t, rjob);
        JobConf localJobConf = new JobConf(localJobConfPath);
        //to be doubly sure, overwrite the user in the config with the one the TT 
        //thinks it is
        localJobConf.setUser(t.getUser());
        //also reset the #tasks per jvm
        resetNumTasksPerJvm(localJobConf);
        //set the base jobconf path in rjob; all tasks will use
        //this as the base path when they run
        rjob.localizedJobConf = localJobConfPath;
        rjob.jobConf = localJobConf;  
        rjob.keepJobFiles = ((localJobConf.getKeepTaskFilesPattern() != null) ||
                             localJobConf.getKeepFailedTaskFiles());
 
        rjob.localized = true;
      }
    }
    return rjob;
  }

  /**
   * Localize the job on this tasktracker. Specifically
   * <ul>
   * <li>Cleanup and create job directories on all disks</li>
   * <li>Download the credentials file</li>
   * <li>Download the job config file job.xml from the FS</li>
   * <li>Invokes the {@link TaskController} to do the rest of the job 
   * initialization</li>
   * </ul>
   *
   * @param t task whose job has to be localized on this TT
   * @param rjob the {@link RunningJob}
   * @return the path to the job configuration to be used for all the tasks
   *         of this job as a starting point.
   * @throws IOException
   */
  Path initializeJob(final Task t, RunningJob rjob)
  throws IOException, InterruptedException {
    final JobID jobId = t.getJobID();

    final Path jobFile = new Path(t.getJobFile());
    final String userName = t.getUser();
    final Configuration conf = getJobConf();

    // save local copy of JobToken file
    final String localJobTokenFile = localizeJobTokenFile(t.getUser(), jobId);
    rjob.ugi = UserGroupInformation.createRemoteUser(t.getUser());

    Credentials ts = TokenCache.loadTokens(localJobTokenFile, conf);
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(ts);
    if (jt != null) { //could be null in the case of some unit tests
      getJobTokenSecretManager().addTokenForJob(jobId.toString(), jt);
    }
    for (Token<? extends TokenIdentifier> token : ts.getAllTokens()) {
      rjob.ugi.addToken(token);
    }

    FileSystem userFs = getFS(jobFile, jobId, conf);

    // Download the job.xml for this job from the system FS
    final Path localJobFile =
      localizeJobConfFile(new Path(t.getJobFile()), userName, userFs, jobId);
    JobConf localJobConf = new JobConf(localJobFile);

    // Setup the public distributed cache
    TaskDistributedCacheManager taskDistributedCacheManager =
      getTrackerDistributedCacheManager()
     .newTaskDistributedCacheManager(jobId, localJobConf);
    rjob.distCacheMgr = taskDistributedCacheManager;
    taskDistributedCacheManager.setupCache(TaskTracker.getPublicDistributedCacheDir(),
        TaskTracker.getPrivateDistributedCacheDir(userName));

    // Set some config values
    localJobConf.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY,
        getJobConf().get(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
    if (conf.get("slave.host.name") != null) {
      localJobConf.set("slave.host.name", conf.get("slave.host.name"));
    }
    resetNumTasksPerJvm(localJobConf);
    localJobConf.setUser(t.getUser());

    // write back the config (this config will have the updates that the
    // distributed cache manager makes as well)
    JobLocalizer.writeLocalJobFile(localJobFile, localJobConf);

    /**
      * Now initialize the job via task-controller to do the rest of the
      * job-init. Do this within a doAs since the distributed cache is also set
      * up in {@link TaskController#initializeJob(String, JobID, Path, Path)}
      * To support potential authenticated HDFS accesses, we need the tokens
      */
    rjob.ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        try {
          taskController.initializeJob(t.getUser(), jobId.toString(), 
              new Path(localJobTokenFile), localJobFile, TaskTracker.this);
        } catch (IOException e) {
          try {
            // called holding lock on RunningJob
            removeJobFiles(t.getUser(), jobId);
          } catch (IOException e2) {
            LOG.warn("Failed to add " + jobId + " to cleanup queue", e2);
          }
          throw e;
        }
        return null;
      }
    });
    //search for the conf that the initializeJob created
    //need to look up certain configs from this conf, like
    //the distributed cache, profiling, etc. ones
    Path initializedConf = lDirAlloc.getLocalPathToRead(getLocalJobConfFile(
           userName, jobId.toString()), getJobConf());
    return initializedConf;
  }
  
  /** If certain configs are enabled, the jvm-reuse should be disabled
   * @param localJobConf
   */
  static void resetNumTasksPerJvm(JobConf localJobConf) {
    boolean debugEnabled = false;
    if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
      return;
    }
    if (localJobConf.getMapDebugScript() != null || 
        localJobConf.getReduceDebugScript() != null) {
      debugEnabled = true;
    }
    String keepPattern = localJobConf.getKeepTaskFilesPattern();
    
    if (debugEnabled || localJobConf.getProfileEnabled() ||
        keepPattern != null || localJobConf.getKeepFailedTaskFiles()) {
      //disable jvm reuse
      localJobConf.setNumTasksToExecutePerJvm(1);
    }
  }

  // Remove the log dir from the tasklog cleanup thread
  void saveLogDir(JobID jobId, JobConf localJobConf)
      throws IOException {
    // remove it from tasklog cleanup thread first,
    // it might be added there because of tasktracker reinit or restart
    JobStartedEvent jse = new JobStartedEvent(jobId);
    getUserLogManager().addLogEvent(jse);
  }

  
  /**
   * Download the job configuration file from the FS.
   *
   * @param jobFile the original location of the configuration file
   * @param user the user in question
   * @param userFs the FileSystem created on behalf of the user
   * @param jobId jobid in question
   * @return the local file system path of the downloaded file.
   * @throws IOException
   */
  private Path localizeJobConfFile(Path jobFile, String user, 
      FileSystem userFs, JobID jobId)
  throws IOException {
    // Get sizes of JobFile and JarFile
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
      lDirAlloc.getLocalPathForWrite(getPrivateDirJobConfFile(user,
          jobId.toString()), jobFileSize, fConf);

    // Download job.xml
    userFs.copyToLocalFile(jobFile, localJobFile);
    return localJobFile;
  }

  private void launchTaskForJob(TaskInProgress tip, JobConf jobConf,
                                RunningJob rjob) throws IOException {
    synchronized (tip) {
      tip.setJobConf(jobConf);
      tip.setUGI(rjob.ugi);
      tip.launchTask(rjob);
    }
  }
    
  public synchronized void shutdown() throws IOException, InterruptedException {
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
   * @throws InterruptedException 
   */
  public synchronized void close() throws IOException, InterruptedException {
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

    // Clear local storage
    cleanupStorage();
        
    // Shutdown the fetcher thread
    this.mapEventsFetcher.interrupt();
    
    //stop the launchers
    this.mapLauncher.interrupt();
    this.reduceLauncher.interrupt();

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
    workerThreads = 0;
  }

  void setConf(JobConf conf) {
    fConf = conf;
  }

  /**
   * Start with the local machine name, and the default JobTracker
   */
  public TaskTracker(JobConf conf) throws IOException, InterruptedException {
    originalConf = conf;
    maxMapSlots = conf.getInt(
                  "mapred.tasktracker.map.tasks.maximum", 2);
    maxReduceSlots = conf.getInt(
                  "mapred.tasktracker.reduce.tasks.maximum", 2);
    UserGroupInformation.setConfiguration(originalConf);
    aclsManager = new ACLsManager(conf, new JobACLsManager(conf), null);
    this.jobTrackAddr = JobTracker.getAddress(conf);
    String infoAddr = 
      NetUtils.getServerAddress(conf,
                                "tasktracker.http.bindAddress", 
                                "tasktracker.http.port",
                                "mapred.task.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String httpBindAddress = infoSocAddr.getHostName();
    int httpPort = infoSocAddr.getPort();
    this.server = new HttpServer("task", httpBindAddress, httpPort,
        httpPort == 0, conf, aclsManager.getAdminsAcl());
    workerThreads = conf.getInt("tasktracker.http.threads", 40);
    server.setThreads(1, workerThreads);
    // let the jsp pages get to the task tracker, config, and other relevant
    // objects
    FileSystem local = FileSystem.getLocal(conf);
    this.localDirAllocator = new LocalDirAllocator("mapred.local.dir");
    Class<? extends TaskController> taskControllerClass = 
      conf.getClass("mapred.task.tracker.task-controller", 
                     DefaultTaskController.class, TaskController.class);
   taskController = 
     (TaskController) ReflectionUtils.newInstance(taskControllerClass, conf);
   taskController.setup(localDirAllocator);

    // create user log manager
    setUserLogManager(new UserLogManager(conf, taskController));
    SecurityUtil.login(originalConf, TT_KEYTAB_FILE, TT_USER_NAME);

    initialize();
    this.shuffleServerMetrics = ShuffleServerInstrumentation.create(this);
    server.setAttribute("task.tracker", this);
    server.setAttribute("local.file.system", local);
    server.setAttribute("conf", conf);
    server.setAttribute("log", LOG);
    server.setAttribute("localDirAllocator", localDirAllocator);
    server.setAttribute("shuffleServerMetrics", shuffleServerMetrics);
    server.addInternalServlet("mapOutput", "/mapOutput", MapOutputServlet.class);
    server.addServlet("taskLog", "/tasklog", TaskLogServlet.class);
    server.start();
    this.httpPort = server.getPort();
    checkJettyPort(httpPort);
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
  }

  // only used by tests
  void setCleanupThread(CleanupQueue c) {
    directoryCleanupThread = c;
  }
  
  CleanupQueue getCleanupThread() {
    return directoryCleanupThread;
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
      if (t[i].isMap) {
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
        
        
        // Check if the map-event list needs purging
        Set<JobID> jobs = heartbeatResponse.getRecoveredJobs();
        if (jobs.size() > 0) {
          synchronized (this) {
            // purge the local map events list
            for (JobID job : jobs) {
              RunningJob rjob;
              synchronized (runningJobs) {
                rjob = runningJobs.get(job);          
                if (rjob != null) {
                  synchronized (rjob) {
                    FetchStatus f = rjob.getFetchStatus();
                    if (f != null) {
                      f.reset();
                    }
                  }
                }
              }
            }

            // Mark the reducers in shuffle for rollback
            synchronized (shouldReset) {
              for (Map.Entry<TaskAttemptID, TaskInProgress> entry 
                   : runningTasks.entrySet()) {
                if (entry.getValue().getStatus().getPhase() == Phase.SHUFFLE) {
                  this.shouldReset.add(entry.getKey());
                }
              }
            }
          }
        }
        
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

  void setIndexCache(IndexCache cache) {
    this.indexCache = cache;
  }

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

      status.getResourceStatus().setAvailableSpace(freeDiskSpace);
      status.getResourceStatus().setTotalVirtualMemory(totVmem);
      status.getResourceStatus().setTotalPhysicalMemory(totPmem);
      status.getResourceStatus().setMapSlotMemorySizeOnTT(
          mapSlotMemorySizeOnTT);
      status.getResourceStatus().setReduceSlotMemorySizeOnTT(
          reduceSlotSizeMemoryOnTT);
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
          myInstrumentation.completeTask(taskStatus.getTaskID());
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

  long getTotalMemoryAllottedForTasksOnTT() {
    return totalMemoryAllottedForTasks;
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
          LOG.info("Recieved RenitTrackerAction from JobTracker");
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
          purgeTask(tip, true);
        }
      }
    }
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
        if (!rjob.keepJobFiles && rjob.localized) {
          removeJobFiles(rjob.jobConf.getUser(), rjob.getJobID());
        }
        // add job to user log manager
        long now = System.currentTimeMillis();
        JobCompletedEvent jca = new JobCompletedEvent(rjob
            .getJobID(), now, UserLogCleaner.getUserlogRetainHours(rjob
            .getJobConf()));
        getUserLogManager().addLogEvent(jca);

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
  void removeJobFiles(String user, JobID jobId) throws IOException {
    String userDir = getUserDir(user);
    String jobDir = getLocalJobDir(user, jobId.toString());
    PathDeletionContext jobCleanup = 
      new TaskController.DeletionContext(getTaskController(), false, user, 
                                         jobDir.substring(userDir.length()));
    directoryCleanupThread.addToQueue(jobCleanup);
    
    for (String str : localdirs) {
      Path ttPrivateJobDir = FileSystem.getLocal(fConf).makeQualified(
        new Path(str, TaskTracker.getPrivateDirForJob(user, jobId.toString())));
      PathDeletionContext ttPrivateJobCleanup =
        new CleanupQueue.PathDeletionContext(ttPrivateJobDir, fConf);
      directoryCleanupThread.addToQueue(ttPrivateJobCleanup);
    }
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
   * @throws InterruptedException 
   */
  void startNewTask(TaskInProgress tip) throws InterruptedException {
    try {
      RunningJob rjob = localizeJob(tip);
      tip.getTask().setJobFile(rjob.localizedJobConf.toString());
      // Localization is done. Neither rjob.jobConf nor rjob.ugi can be null
      launchTaskForJob(tip, new JobConf(rjob.jobConf), rjob); 
    } catch (Throwable e) {
      String msg = ("Error initializing " + tip.getTask().getTaskID() + 
                    ":\n" + StringUtils.stringifyException(e));
      LOG.warn(msg);
      tip.reportDiagnosticInfo(msg);
      try {
        tip.kill(true);
        tip.cleanup(true);
      } catch (IOException ie2) {
        LOG.info("Error cleaning up " + tip.getTask().getTaskID(), ie2);
      } catch (InterruptedException ie2) {
        LOG.info("Error cleaning up " + tip.getTask().getTaskID(), ie2);
      }
        
      // Careful! 
      // This might not be an 'Exception' - don't handle 'Error' here!
      if (e instanceof Error) {
        throw ((Error) e);
      }
    }
  }
  
  void addToMemoryManager(TaskAttemptID attemptId, boolean isMap,
                          JobConf conf) {
    if (isTaskMemoryManagerEnabled()) {
      taskMemoryManager.addTask(attemptId, 
          isMap ? conf
              .getMemoryForMapTask() * 1024 * 1024L : conf
              .getMemoryForReduceTask() * 1024 * 1024L);
    }
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
      getUserLogManager().start();
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
    private JobConf ttConf;
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
      this.ttConf = conf;
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

      // Do the task-type specific localization
//TODO: are these calls really required
      task.localizeConfiguration(localJobConf);
      
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
      taskTimeout = localJobConf.getLong("mapred.task.timeout", 
                                         10 * 60 * 1000);
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
    public synchronized void launchTask(RunningJob rjob) throws IOException {
      if (this.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED ||
          this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
          this.taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN) {
        localizeTask(task);
        if (this.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED) {
          this.taskStatus.setRunState(TaskStatus.State.RUNNING);
        }
        setTaskRunner(task.createRunner(TaskTracker.this, this, rjob));
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
      
      /** check for counter limits and fail the task in case limits are exceeded **/
      Counters taskCounters = taskStatus.getCounters();
      if (taskCounters.size() > Counters.MAX_COUNTER_LIMIT ||
          taskCounters.getGroupNames().size() > Counters.MAX_GROUP_LIMIT) {
        LOG.warn("Killing task " + task.getTaskID() + ": " +
        		"Exceeded limit on counters.");
        try { 
          reportDiagnosticInfo("Error: Exceeded counter limits - " +
          		"Counters=" + taskCounters.size() + " Limit=" 
              + Counters.MAX_COUNTER_LIMIT  + ". " + 
              "Groups=" + taskCounters.getGroupNames().size() + " Limit=" +
              Counters.MAX_GROUP_LIMIT);
          kill(true);
        } catch (IOException e) {
          LOG.error("Error killing task " + task.getTaskID(), e);
        } catch (InterruptedException e) {
          LOG.error("Error killing task " + task.getTaskID(), e);
        }
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
        taskFinished();
        releaseSlot();
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
              String taskStdout ="";
              String taskStderr ="";
              String taskSyslog ="";
              String jobConf = task.getJobFile();
              try {
                Map<LogName, LogFileDetail> allFilesDetails = TaskLog
                    .getAllLogsFileDetails(task.getTaskID(), task
                        .isTaskCleanupTask());
                // get task's stdout file
                taskStdout =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.STDOUT).location,
                        LogName.STDOUT);
                // get task's stderr file
                taskStderr =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.STDERR).location,
                        LogName.STDERR);
                // get task's syslog file
                taskSyslog =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.SYSLOG).location,
                        LogName.SYSLOG);
              } catch(IOException e){
                LOG.warn("Exception finding task's stdout/err/syslog files");
              }
              File workDir = null;
              try {
                workDir =
                    new File(lDirAlloc.getLocalPathToRead(
                        TaskTracker.getLocalTaskDir(task.getUser(), task
                            .getJobID().toString(), task.getTaskID()
                            .toString(), task.isTaskCleanupTask())
                            + Path.SEPARATOR + MRConstants.WORKDIR,
                        localJobConf).toString());
              } catch (IOException e) {
                LOG.warn("Working Directory of the task " + task.getTaskID() +
                                " doesnt exist. Caught exception " +
                          StringUtils.stringifyException(e));
              }
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
              Vector<String> vargs = new Vector<String>();
              for (String component : debug) {
                vargs.add(component);
              }
              vargs.add(taskStdout);
              vargs.add(taskStderr);
              vargs.add(taskSyslog);
              vargs.add(jobConf);
              vargs.add(program);
              try {
                List<String>  wrappedCommand = TaskLog.captureDebugOut
                                                          (vargs, stdout);
                // run the script.
                try {
                  runScript(wrappedCommand, workDir);
                } catch (IOException ioe) {
                  LOG.warn("runScript failed with: " + StringUtils.
                                                      stringifyException(ioe));
                }
              } catch(IOException e) {
                LOG.warn("Error in preparing wrapped debug command");
              }

              // add all lines of debug out to diagnostics
              try {
                int num = localJobConf.getInt("mapred.debug.out.lines", -1);
                addDiagnostics(FileUtil.makeShellPath(stdout),num,"DEBUG OUT");
              } catch(IOException ioe) {
                LOG.warn("Exception in add diagnostics!");
              }

              // Debug-command is run. Do the post-debug-script-exit debug-logs
              // processing. Truncate the logs.
              JvmFinishedEvent jvmFinished = new JvmFinishedEvent(new JVMInfo(
                  TaskLog.getAttemptDir(task.getTaskID(), task
                      .isTaskCleanupTask()), Arrays.asList(task)));
              getUserLogManager().addLogEvent(jvmFinished);
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
      try {
        cleanup(needCleanup);
      } catch (IOException ie) {
      }

    }
    

    /**
     * Runs the script given in args
     * @param args script name followed by its argumnets
     * @param dir current working directory.
     * @throws IOException
     */
    public void runScript(List<String> args, File dir) throws IOException {
      ShellCommandExecutor shexec = 
              new ShellCommandExecutor(args.toArray(new String[0]), dir);
      shexec.execute();
      int exitCode = shexec.getExitCode();
      if (exitCode != 0) {
        throw new IOException("Task debug script exit with nonzero status of " 
                              + exitCode + ".");
      }
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
          try {
            kill(wasFailure);
          } catch (InterruptedException e) {
            throw new IOException("Interrupted while killing " +
                getTask().getTaskID(), e);
          }
        }
      }
      
      // Cleanup on the finished task
      cleanup(true);
    }

    /**
     * Something went wrong and the task must be killed.
     * @param wasFailure was it a failure (versus a kill request)?
     * @throws InterruptedException 
     */
    public synchronized void kill(boolean wasFailure
                                  ) throws IOException, InterruptedException {
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
    void cleanup(boolean needCleanup) throws IOException {
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
          removeTaskFiles(needCleanup);
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
     * @param removeOutputs remove outputs as well as output
     * @param taskId
     * @throws IOException 
     */
    void removeTaskFiles(boolean removeOutputs) throws IOException {
      if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
        String user = ugi.getShortUserName();
        int userDirLen = TaskTracker.getUserDir(user).length();
        String jobId = task.getJobID().toString();
        String taskId = task.getTaskID().toString();
        boolean cleanup = task.isTaskCleanupTask();
        String taskDir;
        if (!removeOutputs) {
          taskDir = TaskTracker.getTaskWorkDir(user, jobId, taskId, cleanup);
        } else {
          taskDir = TaskTracker.getLocalTaskDir(user, jobId, taskId, cleanup);
        }
        PathDeletionContext item =
          new TaskController.DeletionContext(taskController, false, user,
                                             taskDir.substring(userDirLen));          
        directoryCleanupThread.addToQueue(item);
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
  
  private void authorizeJVM(JobID jobId) throws IOException {
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
    authorizeJVM(context.jvmId.getJobId());
    JVMId jvmId = context.jvmId;
    LOG.debug("JVM with ID : " + jvmId + " asked for a task");
    // save pid of task JVM sent by child
    jvmManager.setPidToJvm(jvmId, context.pid);
    if (!jvmManager.isJvmKnown(jvmId)) {
      LOG.info("Killing unknown JVM " + jvmId);
      return new JvmTask(null, true);
    }
    RunningJob rjob = runningJobs.get(jvmId.getJobId());
    if (rjob == null) { //kill the JVM since the job is dead
      LOG.info("Killing JVM " + jvmId + " since job " + jvmId.getJobId() +
               " is dead");
      try {
        jvmManager.killJvm(jvmId);
      } catch (InterruptedException e) {
        LOG.warn("Failed to kill " + jvmId, e);
      }
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
    authorizeJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportProgress(taskStatus);
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
    authorizeJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportDiagnosticInfo(info);
    } else {
      LOG.warn("Error from unknown child task: "+taskid+". Ignored.");
    }
  }
  
  public synchronized void reportNextRecordRange(TaskAttemptID taskid, 
      SortedRanges.Range range) throws IOException {
    authorizeJVM(taskid.getJobID());
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
    authorizeJVM(taskid.getJobID());
    return tasks.get(taskid) != null;
  }

  /**
   * Task is reporting that it is in commit_pending
   * and it is waiting for the commit Response
   */
  public synchronized void commitPending(TaskAttemptID taskid,
                                         TaskStatus taskStatus) 
  throws IOException {
    authorizeJVM(taskid.getJobID());
    LOG.info("Task " + taskid + " is in commit-pending," +"" +
             " task state:" +taskStatus.getRunState());
    statusUpdate(taskid, taskStatus);
    reportTaskFinished(taskid, true);
  }
  
  /**
   * Child checking whether it can commit 
   */
  public synchronized boolean canCommit(TaskAttemptID taskid) 
  throws IOException {
    authorizeJVM(taskid.getJobID());
    return commitResponses.contains(taskid); //don't remove it now
  }
  
  /**
   * The task is done.
   */
  public synchronized void done(TaskAttemptID taskid) 
  throws IOException {
    authorizeJVM(taskid.getJobID());
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
    authorizeJVM(taskId.getJobID());
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
    authorizeJVM(taskId.getJobID());
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
    authorizeJVM(taskId.getJobID());
    LOG.fatal("Task: " + taskId + " - Killed : " + msg);
    TaskInProgress tip = runningTasks.get(taskId);
    tip.reportDiagnosticInfo("Error: " + msg);
    purgeTask(tip, true);
  }

  public synchronized MapTaskCompletionEventsUpdate getMapCompletionEvents(
      JobID jobId, int fromEventId, int maxLocs, TaskAttemptID id) 
  throws IOException {
    authorizeJVM(jobId);
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
    private Path localizedJobConf;
    // keep this for later use
    volatile Set<TaskInProgress> tasks;
    boolean localized;
    boolean keepJobFiles;
    UserGroupInformation ugi;
    FetchStatus f;
    TaskDistributedCacheManager distCacheMgr;
    
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
        (conf.getBoolean("tasktracker.contention.tracking", false));
      DefaultMetricsSystem.initialize("TaskTracker");
      TaskTracker tt = new TaskTracker(conf);
      MBeans.register("TaskTracker", "TaskTrackerInfo", tt);
      tt.run();
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
  public static class MapOutputServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final int MAX_BYTES_TO_READ = 64 * 1024;
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      String mapId = request.getParameter("map");
      String reduceId = request.getParameter("reduce");
      String jobId = request.getParameter("job");

      if (jobId == null) {
        throw new IOException("job parameter is required");
      }

      if (mapId == null || reduceId == null) {
        throw new IOException("map and reduce parameters are required");
      }
      ServletContext context = getServletContext();
      int reduce = Integer.parseInt(reduceId);
      byte[] buffer = new byte[MAX_BYTES_TO_READ];
      // true iff IOException was caused by attempt to access input
      boolean isInputException = true;
      OutputStream outStream = null;
      FileInputStream mapOutputIn = null;
 
      long totalRead = 0;
      ShuffleServerInstrumentation shuffleMetrics =
        (ShuffleServerInstrumentation) context.getAttribute("shuffleServerMetrics");
      TaskTracker tracker = 
        (TaskTracker) context.getAttribute("task.tracker");

      verifyRequest(request, response, tracker, jobId);

      long startTime = 0;
      try {
        shuffleMetrics.serverHandlerBusy();
        if(ClientTraceLog.isInfoEnabled())
          startTime = System.nanoTime();
        outStream = response.getOutputStream();
        JobConf conf = (JobConf) context.getAttribute("conf");
        LocalDirAllocator lDirAlloc = 
          (LocalDirAllocator)context.getAttribute("localDirAllocator");
        FileSystem rfs = ((LocalFileSystem)
            context.getAttribute("local.file.system")).getRaw();

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
         * Read the index file to get the information about where
         * the map-output for the given reducer is available. 
         */
        IndexRecord info = 
          tracker.indexCache.getIndexInformation(mapId, reduce,indexFileName, 
              runAsUserName);
          
        //set the custom "from-map-task" http header to the map task from which
        //the map output data is being transferred
        response.setHeader(FROM_MAP_TASK, mapId);
        
        //set the custom "Raw-Map-Output-Length" http header to 
        //the raw (decompressed) length
        response.setHeader(RAW_MAP_OUTPUT_LENGTH,
            Long.toString(info.rawLength));

        //set the custom "Map-Output-Length" http header to 
        //the actual number of bytes being transferred
        response.setHeader(MAP_OUTPUT_LENGTH,
            Long.toString(info.partLength));

        //set the custom "for-reduce-task" http header to the reduce task number
        //for which this map output is being transferred
        response.setHeader(FOR_REDUCE_TASK, Integer.toString(reduce));
        
        //use the same buffersize as used for reading the data from disk
        response.setBufferSize(MAX_BYTES_TO_READ);
        
        /**
         * Read the data from the sigle map-output file and
         * send it to the reducer.
         */
        //open the map-output file
        mapOutputIn = SecureIOUtils.openForRead(
            new File(mapOutputFileName.toUri().getPath()), runAsUserName, null);

        //seek to the correct offset for the reduce
        mapOutputIn.skip(info.startOffset);
        long rem = info.partLength;
        int len =
          mapOutputIn.read(buffer, 0, (int)Math.min(rem, MAX_BYTES_TO_READ));
        while (rem > 0 && len >= 0) {
          rem -= len;
          try {
            shuffleMetrics.outputBytes(len);
            outStream.write(buffer, 0, len);
            outStream.flush();
          } catch (IOException ie) {
            isInputException = false;
            throw ie;
          }
          totalRead += len;
          len =
            mapOutputIn.read(buffer, 0, (int)Math.min(rem, MAX_BYTES_TO_READ));
        }

        LOG.info("Sent out " + totalRead + " bytes for reduce: " + reduce + 
                 " from map: " + mapId + " given " + info.partLength + "/" + 
                 info.rawLength);
      } catch (IOException ie) {
        Log log = (Log) context.getAttribute("log");
        String errorMsg = ("getMapOutput(" + mapId + "," + reduceId + 
                           ") failed :\n"+
                           StringUtils.stringifyException(ie));
        log.warn(errorMsg);
        if (isInputException) {
          tracker.mapOutputLost(TaskAttemptID.forName(mapId), errorMsg);
        }
        response.sendError(HttpServletResponse.SC_GONE, errorMsg);
        shuffleMetrics.failedOutput();
        throw ie;
      } finally {
        if (null != mapOutputIn) {
          mapOutputIn.close();
        }
        final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
        shuffleMetrics.serverHandlerFree();
        if (ClientTraceLog.isInfoEnabled()) {
          ClientTraceLog.info(String.format(MR_CLIENTTRACE_FORMAT,
                request.getLocalAddr() + ":" + request.getLocalPort(),
                request.getRemoteAddr() + ":" + request.getRemotePort(),
                totalRead, "MAPRED_SHUFFLE", mapId, endTime-startTime));
        }
      }
      outStream.close();
      shuffleMetrics.successOutput();
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
        throw new IOException("fetcher cannot be authenticated " + 
            request.getRemoteHost());
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

    Class<? extends MemoryCalculatorPlugin> clazz =
        fConf.getClass(MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
            null, MemoryCalculatorPlugin.class);
    MemoryCalculatorPlugin memoryCalculatorPlugin =
        MemoryCalculatorPlugin
            .getMemoryCalculatorPlugin(clazz, fConf);
    LOG.info(" Using MemoryCalculatorPlugin : " + memoryCalculatorPlugin);

    if (memoryCalculatorPlugin != null) {
      totalVirtualMemoryOnTT = memoryCalculatorPlugin.getVirtualMemorySize();
      if (totalVirtualMemoryOnTT <= 0) {
        LOG.warn("TaskTracker's totalVmem could not be calculated. "
            + "Setting it to " + JobConf.DISABLED_MEMORY_LIMIT);
        totalVirtualMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
      }
      totalPhysicalMemoryOnTT = memoryCalculatorPlugin.getPhysicalMemorySize();
      if (totalPhysicalMemoryOnTT <= 0) {
        LOG.warn("TaskTracker's totalPmem could not be calculated. "
            + "Setting it to " + JobConf.DISABLED_MEMORY_LIMIT);
        totalPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
      }
    }

    mapSlotMemorySizeOnTT =
        fConf.getLong(
            JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT);
    reduceSlotSizeMemoryOnTT =
        fConf.getLong(
            JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
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

    if (totalMemoryAllottedForTasks == JobConf.DISABLED_MEMORY_LIMIT) {
      taskMemoryManagerEnabled = false;
      LOG.warn("TaskTracker's totalMemoryAllottedForTasks is -1."
          + " TaskMemoryManager is disabled.");
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
          lDirAlloc.getLocalPathForWrite(getPrivateDirJobTokenFile(user, 
              jobId.toString()), jobTokenSize, fConf);
    
      String localJobTokenFileStr = localJobTokenFile.toUri().getPath();
      if(LOG.isDebugEnabled())
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

  // Begin MXBean implementation
  @Override
  public String getHostname() {
    return localHostname;
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion() +", r"+ VersionInfo.getRevision();
  }

  @Override
  public String getConfigVersion() {
    return originalConf.get(CONF_VERSION_KEY, CONF_VERSION_DEFAULT);
  }

  @Override
  public String getJobTrackerUrl() {
    return originalConf.get("mapred.job.tracker");
  }

  @Override
  public int getRpcPort() {
    return taskReportAddress.getPort();
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  @Override
  public boolean isHealthy() {
    boolean healthy = true;
    TaskTrackerHealthStatus hs = new TaskTrackerHealthStatus();
    if (healthChecker != null) {
      healthChecker.setHealthStatus(hs);
      healthy = hs.isNodeHealthy();
    }    
    return healthy;
  }

  @Override
  public String getTasksInfoJson() {
    return getTasksInfo().toJson();
  }

  InfoMap getTasksInfo() {
    InfoMap map = new InfoMap();
    int failed = 0;
    int commitPending = 0;
    for (TaskStatus st : getNonRunningTasks()) {
      if (st.getRunState() == TaskStatus.State.FAILED ||
          st.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
        ++failed;
      } else if (st.getRunState() == TaskStatus.State.COMMIT_PENDING) {
        ++commitPending;
      }
    }
    map.put("running", runningTasks.size());
    map.put("failed", failed);
    map.put("commit_pending", commitPending);
    return map;
  }
  // End MXBean implemenation

  @Override
  public void 
  updatePrivateDistributedCacheSizes(org.apache.hadoop.mapreduce.JobID jobId,
                                     long[] sizes
                                     ) throws IOException {
    distributedCacheManager.setArchiveSizes(jobId, sizes);
  }

}
