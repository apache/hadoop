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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.MemoryCalculatorPlugin;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 *******************************************************/
public class TaskTracker 
             implements MRConstants, TaskUmbilicalProtocol, Runnable {
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
        ", cliID: %s";  // task id
  public static final Log ClientTraceLog =
    LogFactory.getLog(TaskTracker.class.getName() + ".clienttrace");

  volatile boolean running = true;

  private LocalDirAllocator localDirAllocator;
  String taskTrackerName;
  String localHostname;
  InetSocketAddress jobTrackAddr;
    
  InetSocketAddress taskReportAddress;

  Server taskReportServer = null;
  InterTrackerProtocol jobClient;
    
  // last heartbeat response recieved
  short heartbeatResponseId = -1;

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
  Map<JobID, RunningJob> runningJobs = null;
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
  private static final String SUBDIR = "taskTracker";
  private static final String CACHEDIR = "archive";
  private static final String JOBCACHE = "jobcache";
  private static final String PID = "pid";
  private static final String OUTPUT = "output";
  private JobConf originalConf;
  private JobConf fConf;
  private int maxCurrentMapTasks;
  private int maxCurrentReduceTasks;
  private int failures;
  private MapEventsFetcherThread mapEventsFetcher;
  int workerThreads;
  private CleanupQueue directoryCleanupThread;
  volatile JvmManager jvmManager;
  
  private TaskMemoryManagerThread taskMemoryManager;
  private boolean taskMemoryManagerEnabled = true;
  private long totalVirtualMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long mapSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long reduceSlotSizeMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;

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
  private class ShuffleServerMetrics implements Updater {
    private MetricsRecord shuffleMetricsRecord = null;
    private int serverHandlerBusy = 0;
    private long outputBytes = 0;
    private int failedOutputs = 0;
    private int successOutputs = 0;
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
        outputBytes = 0;
        failedOutputs = 0;
        successOutputs = 0;
      }
      shuffleMetricsRecord.update();
    }
  }
  

  
  
    
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
                TaskInProgress tip;
                KillTaskAction killAction = (KillTaskAction) action;
                synchronized (TaskTracker.this) {
                  tip = tasks.get(killAction.getTaskID());
                }
                LOG.info("Received KillTaskAction for task: " + 
                         killAction.getTaskID());
                purgeTask(tip, false);
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

  static String getCacheSubdir() {
    return TaskTracker.SUBDIR + Path.SEPARATOR + TaskTracker.CACHEDIR;
  }

  static String getJobCacheSubdir() {
    return TaskTracker.SUBDIR + Path.SEPARATOR + TaskTracker.JOBCACHE;
  }

  static String getLocalJobDir(String jobid) {
	return getJobCacheSubdir() + Path.SEPARATOR + jobid; 
  }

  static String getLocalTaskDir(String jobid, String taskid) {
	return getLocalTaskDir(jobid, taskid, false) ; 
  }

  static String getIntermediateOutputDir(String jobid, String taskid) {
	return getLocalTaskDir(jobid, taskid) 
           + Path.SEPARATOR + TaskTracker.OUTPUT ; 
  }

  static String getLocalTaskDir(String jobid, 
                                String taskid, 
                                boolean isCleanupAttempt) {
	String taskDir = getLocalJobDir(jobid) + Path.SEPARATOR + taskid;
	if (isCleanupAttempt) { 
      taskDir = taskDir + ".cleanup";
	}
	return taskDir;
  }

  static String getPidFile(String jobid, 
                           String taskid, 
                           boolean isCleanup) {
    return  getLocalTaskDir(jobid, taskid, isCleanup)
            + Path.SEPARATOR + PID;
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
   * Do the real constructor work here.  It's in a separate method
   * so we can call it again and "recycle" the object after calling
   * close().
   */
  synchronized void initialize() throws IOException {
    // use configured nameserver & interface to get local hostname
    this.fConf = new JobConf(originalConf);
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
    checkLocalDirs(this.fConf.getLocalDirs());
    fConf.deleteLocalFiles(SUBDIR);

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
    
    Class<? extends TaskTrackerInstrumentation> metricsInst = getInstrumentationClass(fConf);
    try {
      java.lang.reflect.Constructor<? extends TaskTrackerInstrumentation> c =
        metricsInst.getConstructor(new Class[] {TaskTracker.class} );
      this.myInstrumentation = c.newInstance(this);
    } catch(Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by 
      //falling back on the default.
      LOG.error("failed to initialize taskTracker metrics", e);
      this.myInstrumentation = new TaskTrackerMetricsInst(this);
    }
    
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
      SecurityUtil.setPolicy(new ConfiguredPolicy(this.fConf, policyProvider));
    }
    
    // RPC initialization
    int max = maxCurrentMapTasks > maxCurrentReduceTasks ? 
                       maxCurrentMapTasks : maxCurrentReduceTasks;
    //set the num handlers to max*2 since canCommit may wait for the duration
    //of a heartbeat RPC
    this.taskReportServer =
      RPC.getServer(this, bindAddress, tmpPort, 2 * max, false, this.fConf);
    this.taskReportServer.start();

    // get the assigned address
    this.taskReportAddress = taskReportServer.getListenerAddress();
    this.fConf.set("mapred.task.tracker.report.address",
        taskReportAddress.getHostName() + ":" + taskReportAddress.getPort());
    LOG.info("TaskTracker up at: " + this.taskReportAddress);

    this.taskTrackerName = "tracker_" + localHostname + ":" + taskReportAddress;
    LOG.info("Starting tracker " + taskTrackerName);

    // Clear out temporary files that might be lying around
    DistributedCache.purgeCache(this.fConf);
    cleanupStorage();

    this.jobClient = (InterTrackerProtocol) 
      RPC.waitForProxy(InterTrackerProtocol.class,
                       InterTrackerProtocol.versionID, 
                       jobTrackAddr, this.fConf);
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

    this.indexCache = new IndexCache(this.fConf);

    mapLauncher = new TaskLauncher(maxCurrentMapTasks);
    reduceLauncher = new TaskLauncher(maxCurrentReduceTasks);
    mapLauncher.start();
    reduceLauncher.start();
  }

  public static Class<? extends TaskTrackerInstrumentation> getInstrumentationClass(
    Configuration conf) {
    return conf.getClass("mapred.tasktracker.instrumentation",
        TaskTrackerMetricsInst.class, TaskTrackerInstrumentation.class);
  }

  public static void setInstrumentationClass(
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

  private LocalDirAllocator lDirAlloc = 
                              new LocalDirAllocator("mapred.local.dir");

  // intialize the job directory
  private void localizeJob(TaskInProgress tip) throws IOException {
    Path localJarFile = null;
    Task t = tip.getTask();
    JobID jobId = t.getJobID();
    Path jobFile = new Path(t.getJobFile());
    // Get sizes of JobFile and JarFile
    // sizes are -1 if they are not present.
    FileStatus status = null;
    long jobFileSize = -1;
    try {
      status = systemFS.getFileStatus(jobFile);
      jobFileSize = status.getLen();
    } catch(FileNotFoundException fe) {
      jobFileSize = -1;
    }
    Path localJobFile = lDirAlloc.getLocalPathForWrite(
                                    getLocalJobDir(jobId.toString())
                                    + Path.SEPARATOR + "job.xml",
                                    jobFileSize, fConf);
    RunningJob rjob = addTaskToJob(jobId, tip);
    synchronized (rjob) {
      if (!rjob.localized) {
  
        FileSystem localFs = FileSystem.getLocal(fConf);
        // this will happen on a partial execution of localizeJob.
        // Sometimes the job.xml gets copied but copying job.jar
        // might throw out an exception
        // we should clean up and then try again
        Path jobDir = localJobFile.getParent();
        if (localFs.exists(jobDir)){
          localFs.delete(jobDir, true);
          boolean b = localFs.mkdirs(jobDir);
          if (!b)
            throw new IOException("Not able to create job directory "
                                  + jobDir.toString());
        }
        systemFS.copyToLocalFile(jobFile, localJobFile);
        JobConf localJobConf = new JobConf(localJobFile);
        
        // create the 'work' directory
        // job-specific shared directory for use as scratch space 
        Path workDir = lDirAlloc.getLocalPathForWrite(
                         (getLocalJobDir(jobId.toString())
                         + Path.SEPARATOR + "work"), fConf);
        if (!localFs.mkdirs(workDir)) {
          throw new IOException("Mkdirs failed to create " 
                      + workDir.toString());
        }
        System.setProperty("job.local.dir", workDir.toString());
        localJobConf.set("job.local.dir", workDir.toString());
        
        // copy Jar file to the local FS and unjar it.
        String jarFile = localJobConf.getJar();
        long jarFileSize = -1;
        if (jarFile != null) {
          Path jarFilePath = new Path(jarFile);
          try {
            status = systemFS.getFileStatus(jarFilePath);
            jarFileSize = status.getLen();
          } catch(FileNotFoundException fe) {
            jarFileSize = -1;
          }
          // Here we check for and we check five times the size of jarFileSize
          // to accommodate for unjarring the jar file in work directory 
          localJarFile = new Path(lDirAlloc.getLocalPathForWrite(
                                     getLocalJobDir(jobId.toString())
                                     + Path.SEPARATOR + "jars",
                                     5 * jarFileSize, fConf), "job.jar");
          if (!localFs.mkdirs(localJarFile.getParent())) {
            throw new IOException("Mkdirs failed to create jars directory "); 
          }
          systemFS.copyToLocalFile(jarFilePath, localJarFile);
          localJobConf.setJar(localJarFile.toString());
          OutputStream out = localFs.create(localJobFile);
          try {
            localJobConf.writeXml(out);
          } finally {
            out.close();
          }
          // also unjar the job.jar files 
          RunJar.unJar(new File(localJarFile.toString()),
                       new File(localJarFile.getParent().toString()));
        }
        rjob.keepJobFiles = ((localJobConf.getKeepTaskFilesPattern() != null) ||
                             localJobConf.getKeepFailedTaskFiles());
        rjob.localized = true;
        rjob.jobConf = localJobConf;
      }
    }
    launchTaskForJob(tip, new JobConf(rjob.jobConf)); 
  }

  private void launchTaskForJob(TaskInProgress tip, JobConf jobConf) throws IOException{
    synchronized (tip) {
      tip.setJobConf(jobConf);
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
  }

  /**
   * Start with the local machine name, and the default JobTracker
   */
  public TaskTracker(JobConf conf) throws IOException {
    originalConf = conf;
    maxCurrentMapTasks = conf.getInt(
                  "mapred.tasktracker.map.tasks.maximum", 2);
    maxCurrentReduceTasks = conf.getInt(
                  "mapred.tasktracker.reduce.tasks.maximum", 2);
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
        httpPort == 0, conf);
    workerThreads = conf.getInt("tasktracker.http.threads", 40);
    this.shuffleServerMetrics = new ShuffleServerMetrics(conf);
    server.setThreads(1, workerThreads);
    // let the jsp pages get to the task tracker, config, and other relevant
    // objects
    FileSystem local = FileSystem.getLocal(conf);
    this.localDirAllocator = new LocalDirAllocator("mapred.local.dir");
    server.setAttribute("task.tracker", this);
    server.setAttribute("local.file.system", local);
    server.setAttribute("conf", conf);
    server.setAttribute("log", LOG);
    server.setAttribute("localDirAllocator", localDirAllocator);
    server.setAttribute("shuffleServerMetrics", shuffleServerMetrics);
    server.addInternalServlet("mapOutput", "/mapOutput", MapOutputServlet.class);
    server.addInternalServlet("taskLog", "/tasklog", TaskLogServlet.class);
    server.start();
    this.httpPort = server.getPort();
    checkJettyPort(httpPort);
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
          // sleeps for the wait time
          Thread.sleep(waitTime);
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

  /**
   * Build and transmit the heart beat to the JobTracker
   * @param now current time
   * @return false if the tracker was unknown
   * @throws IOException
   */
  private HeartbeatResponse transmitHeartBeat(long now) throws IOException {
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
                                       maxCurrentMapTasks,
                                       maxCurrentReduceTasks); 
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
      askForNewTask = (status.countMapTasks() < maxCurrentMapTasks || 
                       status.countReduceTasks() < maxCurrentReduceTasks) &&
                      acceptNewTasks; 
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
  private synchronized void purgeJob(KillJobAction action) throws IOException {
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
        if (!rjob.keepJobFiles){
          directoryCleanupThread.addToQueue(fConf, getLocalFiles(fConf, 
            getLocalJobDir(rjob.getJobID().toString())));
        }
        // Remove this job 
        rjob.tasks.clear();
      }
    }

    synchronized(runningJobs) {
      runningJobs.remove(jobId);
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
    
  /**
   * Try to get the size of output for this task.
   * Returns -1 if it can't be found.
   * @return
   */
  long tryToGetOutputSize(TaskAttemptID taskId, JobConf conf) {
    
    try{
      TaskInProgress tip;
      synchronized(this) {
        tip = tasks.get(taskId);
      }
      if(tip == null)
         return -1;
      
      if (!tip.getTask().isMapTask() || 
          tip.getRunState() != TaskStatus.State.SUCCEEDED) {
        return -1;
      }
      
      MapOutputFile mapOutputFile = new MapOutputFile();
      mapOutputFile.setJobId(taskId.getJobID());
      mapOutputFile.setConf(conf);
      
      Path tmp_output =  mapOutputFile.getOutputFile(taskId);
      if(tmp_output == null)
        return 0;
      FileSystem localFS = FileSystem.getLocal(conf);
      FileStatus stat = localFS.getFileStatus(tmp_output);
      if(stat == null)
        return 0;
      else
        return stat.getLen();
    } catch(IOException e) {
      LOG.info(e);
      return -1;
    }
  }

  private TaskLauncher mapLauncher;
  private TaskLauncher reduceLauncher;
  public JvmManager getJvmManagerInstance() {
    return jvmManager;
  }
  
  private void addToTaskQueue(LaunchTaskAction action) {
    if (action.getTask().isMapTask()) {
      mapLauncher.addToTaskQueue(action);
    } else {
      reduceLauncher.addToTaskQueue(action);
    }
  }
  
  private class TaskLauncher extends Thread {
    private IntWritable numFreeSlots;
    private final int maxSlots;
    private List<TaskInProgress> tasksToLaunch;

    public TaskLauncher(int numSlots) {
      this.maxSlots = numSlots;
      this.numFreeSlots = new IntWritable(numSlots);
      this.tasksToLaunch = new LinkedList<TaskInProgress>();
      setDaemon(true);
      setName("TaskLauncher for task");
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
    
    public void addFreeSlot() {
      synchronized (numFreeSlots) {
        numFreeSlots.set(numFreeSlots.get() + 1);
        assert (numFreeSlots.get() <= maxSlots);
        LOG.info("addFreeSlot : current free slots : " + numFreeSlots.get());
        numFreeSlots.notifyAll();
      }
    }
    
    public void run() {
      while (!Thread.interrupted()) {
        try {
          TaskInProgress tip;
          synchronized (tasksToLaunch) {
            while (tasksToLaunch.isEmpty()) {
              tasksToLaunch.wait();
            }
            //get the TIP
            tip = tasksToLaunch.remove(0);
            LOG.info("Trying to launch : " + tip.getTask().getTaskID());
          }
          //wait for a slot to run
          synchronized (numFreeSlots) {
            while (numFreeSlots.get() == 0) {
              numFreeSlots.wait();
            }
            LOG.info("In TaskLauncher, current free slots : " + numFreeSlots.get()+
                " and trying to launch "+tip.getTask().getTaskID());
            numFreeSlots.set(numFreeSlots.get() - 1);
            assert (numFreeSlots.get() >= 0);
          }
          synchronized (tip) {
            //to make sure that there is no kill task action for this
            if (tip.getRunState() != TaskStatus.State.UNASSIGNED &&
                tip.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
                tip.getRunState() != TaskStatus.State.KILLED_UNCLEAN) {
              //got killed externally while still in the launcher queue
              addFreeSlot();
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
  private void startNewTask(TaskInProgress tip) {
    try {
      localizeJob(tip);
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
        
      // Careful! 
      // This might not be an 'Exception' - don't handle 'Error' here!
      if (e instanceof Error) {
        throw ((Error) e);
      }
    }
  }
  
  void addToMemoryManager(TaskAttemptID attemptId, boolean isMap,
                          JobConf conf, 
                          String pidFile) {
    if (isTaskMemoryManagerEnabled()) {
      taskMemoryManager.addTask(attemptId, 
          isMap ? conf
              .getMemoryForMapTask() * 1024 * 1024L : conf
              .getMemoryForReduceTask() * 1024 * 1024L, pidFile);
    }
  }

  void removeFromMemoryManager(TaskAttemptID attemptId) {
    // Remove the entry from taskMemoryManagerThread's data structures.
    if (isTaskMemoryManagerEnabled()) {
      taskMemoryManager.removeTask(attemptId);
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
        
    private void localizeTask(Task task) throws IOException{

      Path localTaskDir = 
        lDirAlloc.getLocalPathForWrite(
          TaskTracker.getLocalTaskDir(task.getJobID().toString(), 
            task.getTaskID().toString(), task.isTaskCleanupTask()), 
          defaultJobConf );
      
      FileSystem localFs = FileSystem.getLocal(fConf);
      if (!localFs.mkdirs(localTaskDir)) {
        throw new IOException("Mkdirs failed to create " 
                    + localTaskDir.toString());
      }

      // create symlink for ../work if it already doesnt exist
      String workDir = lDirAlloc.getLocalPathToRead(
                         TaskTracker.getLocalJobDir(task.getJobID().toString())
                         + Path.SEPARATOR  
                         + "work", defaultJobConf).toString();
      String link = localTaskDir.getParent().toString() 
                      + Path.SEPARATOR + "work";
      File flink = new File(link);
      if (!flink.exists())
        FileUtil.symLink(workDir, link);
      
      // create the working-directory of the task 
      Path cwd = lDirAlloc.getLocalPathForWrite(
                   getLocalTaskDir(task.getJobID().toString(), 
                      task.getTaskID().toString(), task.isTaskCleanupTask()) 
                   + Path.SEPARATOR + MRConstants.WORKDIR,
                   defaultJobConf);
      if (!localFs.mkdirs(cwd)) {
        throw new IOException("Mkdirs failed to create " 
                    + cwd.toString());
      }

      Path localTaskFile = new Path(localTaskDir, "job.xml");
      task.setJobFile(localTaskFile.toString());
      localJobConf.set("mapred.local.dir",
                       fConf.get("mapred.local.dir"));
      if (fConf.get("slave.host.name") != null) {
        localJobConf.set("slave.host.name",
                         fConf.get("slave.host.name"));
      }
            
      localJobConf.set("mapred.task.id", task.getTaskID().toString());
      keepFailedTaskFiles = localJobConf.getKeepFailedTaskFiles();

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
        localJobConf.set("hadoop.net.static.resolutions", str.toString());
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
      if (isTaskMemoryManagerEnabled()) {
        localJobConf.setBoolean("task.memory.mgmt.enabled", true);
      }
      OutputStream out = localFs.create(localTaskFile);
      try {
        localJobConf.writeXml(out);
      } finally {
        out.close();
      }
      task.setConf(localJobConf);
    }
        
    /**
     */
    public Task getTask() {
      return task;
    }
    
    public TaskRunner getTaskRunner() {
      return runner;
    }

    public synchronized void setJobConf(JobConf lconf){
      this.localJobConf = lconf;
      keepFailedTaskFiles = localJobConf.getKeepFailedTaskFiles();
      taskTimeout = localJobConf.getLong("mapred.task.timeout", 
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
        this.runner = task.createRunner(TaskTracker.this, this);
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

    }
    
    public boolean wasKilled() {
      return wasKilled;
    }

    void reportTaskFinished() {
      taskFinished();
      releaseSlot();
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
                // get task's stdout file 
                taskStdout = FileUtil.makeShellPath(
                    TaskLog.getRealTaskLogFileLocation
                                  (task.getTaskID(), TaskLog.LogName.STDOUT));
                // get task's stderr file 
                taskStderr = FileUtil.makeShellPath(
                    TaskLog.getRealTaskLogFileLocation
                                  (task.getTaskID(), TaskLog.LogName.STDERR));
                // get task's syslog file 
                taskSyslog = FileUtil.makeShellPath(
                    TaskLog.getRealTaskLogFileLocation
                                  (task.getTaskID(), TaskLog.LogName.SYSLOG));
              } catch(IOException e){
                LOG.warn("Exception finding task's stdout/err/syslog files");
              }
              File workDir = null;
              try {
                workDir = new File(lDirAlloc.getLocalPathToRead(
                                     TaskTracker.getLocalTaskDir( 
                                       task.getJobID().toString(), 
                                       task.getTaskID().toString(),
                                       task.isTaskCleanupTask())
                                     + Path.SEPARATOR + MRConstants.WORKDIR,
                                     localJobConf). toString());
              } catch (IOException e) {
                LOG.warn("Working Directory of the task " + task.getTaskID() +
                		 "doesnt exist. Caught exception " +
                          StringUtils.stringifyException(e));
              }
              // Build the command  
              File stdout = TaskLog.getRealTaskLogFileLocation(
                                   task.getTaskID(), TaskLog.LogName.DEBUGOUT);
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
          kill(wasFailure);
        }
      }
      
      // Cleanup on the finished task
      cleanup(true);
    }

    /**
     * Something went wrong and the task must be killed.
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
      removeFromMemoryManager(task.getTaskID());
      releaseSlot();
    }
    
    private synchronized void releaseSlot() {
      if (slotTaken) {
        if (launcher != null) {
          launcher.addFreeSlot();
        }
        slotTaken = false;
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
        try {
          // localJobConf could be null if localization has not happened
          // then no cleanup will be required.
          if (localJobConf == null) {
            return;
          }
          String taskDir = getLocalTaskDir(task.getJobID().toString(),
                             taskId.toString(), task.isTaskCleanupTask());
          if (needCleanup) {
            if (runner != null) {
              //cleans up the output directory of the task (where map outputs 
              //and reduce inputs get stored)
              runner.close();
            }
            //We don't delete the workdir
            //since some other task (running in the same JVM) 
            //might be using the dir. The JVM running the tasks would clean
            //the workdir per a task in the task process itself.
            if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
              directoryCleanupThread.addToQueue(defaultJobConf,
                  getLocalFiles(defaultJobConf,
                  taskDir));
            }  
            
            else {
              directoryCleanupThread.addToQueue(defaultJobConf,
                  getLocalFiles(defaultJobConf,
                taskDir+"/job.xml"));
            }
          } else {
            if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
              directoryCleanupThread.addToQueue(defaultJobConf,
                  getLocalFiles(defaultJobConf,
                  taskDir+"/work"));
            }  
          }
        } catch (Throwable ie) {
          LOG.info("Error cleaning up task runner: " + 
                   StringUtils.stringifyException(ie));
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

    
  // ///////////////////////////////////////////////////////////////
  // TaskUmbilicalProtocol
  /////////////////////////////////////////////////////////////////

  /**
   * Called upon startup by the child process, to fetch Task data.
   */
  public synchronized JvmTask getTask(JVMId jvmId) 
  throws IOException {
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
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportDiagnosticInfo(info);
    } else {
      LOG.warn("Error from unknown child task: "+taskid+". Ignored.");
    }
  }
  
  public synchronized void reportNextRecordRange(TaskAttemptID taskid, 
      SortedRanges.Range range) throws IOException {
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
    return tasks.get(taskid) != null;
  }

  /**
   * Task is reporting that it is in commit_pending
   * and it is waiting for the commit Response
   */
  public synchronized void commitPending(TaskAttemptID taskid,
                                         TaskStatus taskStatus) 
  throws IOException {
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
    LOG.fatal("Task: " + taskId + " - Killed : " + msg);
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
      if (!commitPending) {
        tip.reportTaskFinished();
      }
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
      
    void setFetchStatus(FetchStatus f) {
      this.f = f;
    }
      
    FetchStatus getFetchStatus() {
      return f;
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
      status.setOutputSize(tryToGetOutputSize(status.getTaskID(), fConf));
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
  public static class MapOutputServlet extends HttpServlet {
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
      FSDataInputStream mapOutputIn = null;
 
      long totalRead = 0;
      ShuffleServerMetrics shuffleMetrics =
        (ShuffleServerMetrics) context.getAttribute("shuffleServerMetrics");
      TaskTracker tracker = 
        (TaskTracker) context.getAttribute("task.tracker");

      try {
        shuffleMetrics.serverHandlerBusy();
        outStream = response.getOutputStream();
        JobConf conf = (JobConf) context.getAttribute("conf");
        LocalDirAllocator lDirAlloc = 
          (LocalDirAllocator)context.getAttribute("localDirAllocator");
        FileSystem rfs = ((LocalFileSystem)
            context.getAttribute("local.file.system")).getRaw();

        // Index file
        Path indexFileName = lDirAlloc.getLocalPathToRead(
            TaskTracker.getIntermediateOutputDir(jobId, mapId)
            + "/file.out.index", conf);
        
        // Map-output file
        Path mapOutputFileName = lDirAlloc.getLocalPathToRead(
            TaskTracker.getIntermediateOutputDir(jobId, mapId)
            + "/file.out", conf);

        /**
         * Read the index file to get the information about where
         * the map-output for the given reducer is available. 
         */
        IndexRecord info = 
          tracker.indexCache.getIndexInformation(mapId, reduce,indexFileName);
          
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
        mapOutputIn = rfs.open(mapOutputFileName);

        //seek to the correct offset for the reduce
        mapOutputIn.seek(info.startOffset);
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
        shuffleMetrics.serverHandlerFree();
        if (ClientTraceLog.isInfoEnabled()) {
          ClientTraceLog.info(String.format(MR_CLIENTTRACE_FORMAT,
                request.getLocalAddr() + ":" + request.getLocalPort(),
                request.getRemoteAddr() + ":" + request.getRemotePort(),
                totalRead, "MAPRED_SHUFFLE", mapId));
        }
      }
      outStream.close();
      shuffleMetrics.successOutput();
    }
  }

  // get the full paths of the directory in all the local disks.
  private Path[] getLocalFiles(JobConf conf, String subdir) throws IOException{
    String[] localDirs = conf.getLocalDirs();
    Path[] paths = new Path[localDirs.length];
    FileSystem localFs = FileSystem.getLocal(conf);
    for (int i = 0; i < localDirs.length; i++) {
      paths[i] = new Path(localDirs[i], subdir);
      paths[i] = paths[i].makeQualified(localFs);
    }
    return paths;
  }

  int getMaxCurrentMapTasks() {
    return maxCurrentMapTasks;
  }
  
  int getMaxCurrentReduceTasks() {
    return maxCurrentReduceTasks;
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
        (MemoryCalculatorPlugin) MemoryCalculatorPlugin
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
        maxCurrentMapTasks * mapSlotMemorySizeOnTT + maxCurrentReduceTasks
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
          totalMemoryAllottedForTasks = (maxCurrentMapTasks + 
              maxCurrentReduceTasks) *  (memoryAllotedForSlot/(1024 * 1024));
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

  private void setTaskMemoryManagerEnabledFlag() {
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
}
