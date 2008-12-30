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


import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 *******************************************************/
public class JobTracker implements MRConstants, InterTrackerProtocol, JobSubmissionProtocol {
  static long TASKTRACKER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  static long RETIRE_JOB_INTERVAL;
  static long RETIRE_JOB_CHECK_INTERVAL;
  static float TASK_ALLOC_EPSILON;
  static float PAD_FRACTION;
  static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
  public static enum State { INITIALIZING, RUNNING }
  State state = State.INITIALIZING;
  private static final int SYSTEM_DIR_CLEANUP_RETRY_PERIOD = 10000;

  private DNSToSwitchMapping dnsToSwitchMapping;
  private NetworkTopology clusterMap = new NetworkTopology();
  private int numTaskCacheLevels; // the max level to which we cache tasks
  private Set<Node> nodesAtMaxLevel = new HashSet<Node>();

  // system directories are world-wide readable and owner readable
  final static FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0733); // rwx-wx-wx

  /**
   * A client tried to submit a job before the Job Tracker was ready.
   */
  public static class IllegalStateException extends IOException {
    public IllegalStateException(String msg) {
      super(msg);
    }
  }

  /**
   * The maximum no. of 'completed' (successful/failed/killed)
   * jobs kept in memory per-user. 
   */
  final int MAX_COMPLETE_USER_JOBS_IN_MEMORY;

  private int nextJobId = 1;

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.JobTracker");
    
  /**
   * Start the JobTracker with given configuration.
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the JobTracker is up and running if the user passes the port as
   * <code>zero</code>.
   *   
   * @param conf configuration for the JobTracker.
   * @throws IOException
   */
  public static JobTracker startTracker(JobConf conf
                                        ) throws IOException,
                                                 InterruptedException {
    JobTracker result = null;
    while (true) {
      try {
        result = new JobTracker(conf);
        break;
      } catch (VersionMismatch e) {
        throw e;
      } catch (BindException e) {
        throw e;
      } catch (UnknownHostException e) {
        throw e;
      } catch (IOException e) {
        LOG.warn("Error starting tracker: " + 
                 StringUtils.stringifyException(e));
      }
      Thread.sleep(1000);
    }
    if (result != null) {
      JobEndNotifier.startNotifier();
    }
    return result;
  }

  public void stopTracker() throws IOException {
    JobEndNotifier.stopNotifier();
    close();
  }
    
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(InterTrackerProtocol.class.getName())) {
      return InterTrackerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())){
      return JobSubmissionProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to job tracker: " + protocol);
    }
  }
  /**
   * A thread to timeout tasks that have been assigned to task trackers,
   * but that haven't reported back yet.
   * Note that I included a stop() method, even though there is no place
   * where JobTrackers are cleaned up.
   */
  private class ExpireLaunchingTasks implements Runnable {
    /**
     * This is a map of the tasks that have been assigned to task trackers,
     * but that have not yet been seen in a status report.
     * map: task-id -> time-assigned 
     */
    private Map<TaskAttemptID, Long> launchingTasks =
      new LinkedHashMap<TaskAttemptID, Long>();
      
    public void run() {
      while (true) {
        try {
          // Every 3 minutes check for any tasks that are overdue
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL/3);
          long now = System.currentTimeMillis();
          LOG.debug("Starting launching task sweep");
          synchronized (JobTracker.this) {
            synchronized (launchingTasks) {
              Iterator<Map.Entry<TaskAttemptID, Long>> itr =
                launchingTasks.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<TaskAttemptID, Long> pair = itr.next();
                TaskAttemptID taskId = pair.getKey();
                long age = now - (pair.getValue()).longValue();
                LOG.info(taskId + " is " + age + " ms debug.");
                if (age > TASKTRACKER_EXPIRY_INTERVAL) {
                  LOG.info("Launching task " + taskId + " timed out.");
                  TaskInProgress tip = null;
                  tip = taskidToTIPMap.get(taskId);
                  if (tip != null) {
                    JobInProgress job = tip.getJob();
                    String trackerName = getAssignedTracker(taskId);
                    TaskTrackerStatus trackerStatus = 
                      getTaskTracker(trackerName);
                    // This might happen when the tasktracker has already
                    // expired and this thread tries to call failedtask
                    // again. expire tasktracker should have called failed
                    // task!
                    if (trackerStatus != null)
                      job.failedTask(tip, taskId, "Error launching task", 
                                     tip.isMapTask()? TaskStatus.Phase.MAP:
                                     TaskStatus.Phase.STARTING,
                                     TaskStatus.State.FAILED,
                                     trackerName, myMetrics);
                  }
                  itr.remove();
                } else {
                  // the tasks are sorted by start time, so once we find
                  // one that we want to keep, we are done for this cycle.
                  break;
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          // all done
          break;
        } catch (Exception e) {
          LOG.error("Expire Launching Task Thread got exception: " +
                    StringUtils.stringifyException(e));
        }
      }
    }
      
    public void addNewTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.put(taskName, 
                           System.currentTimeMillis());
      }
    }
      
    public void removeTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.remove(taskName);
      }
    }
  }
    
  ///////////////////////////////////////////////////////
  // Used to expire TaskTrackers that have gone down
  ///////////////////////////////////////////////////////
  class ExpireTrackers implements Runnable {
    public ExpireTrackers() {
    }
    /**
     * The run method lives for the life of the JobTracker, and removes TaskTrackers
     * that have not checked in for some time.
     */
    public void run() {
      while (true) {
        try {
          //
          // Thread runs periodically to check whether trackers should be expired.
          // The sleep interval must be no more than half the maximum expiry time
          // for a task tracker.
          //
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);

          //
          // Loop through all expired items in the queue
          //
          // Need to lock the JobTracker here since we are
          // manipulating it's data-structures via
          // ExpireTrackers.run -> JobTracker.lostTaskTracker ->
          // JobInProgress.failedTask -> JobTracker.markCompleteTaskAttempt
          // Also need to lock JobTracker before locking 'taskTracker' &
          // 'trackerExpiryQueue' to prevent deadlock:
          // @see {@link JobTracker.processHeartbeat(TaskTrackerStatus, boolean)} 
          synchronized (JobTracker.this) {
            synchronized (taskTrackers) {
              synchronized (trackerExpiryQueue) {
                long now = System.currentTimeMillis();
                TaskTrackerStatus leastRecent = null;
                while ((trackerExpiryQueue.size() > 0) &&
                       ((leastRecent = trackerExpiryQueue.first()) != null) &&
                       (now - leastRecent.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL)) {
                        
                  // Remove profile from head of queue
                  trackerExpiryQueue.remove(leastRecent);
                  String trackerName = leastRecent.getTrackerName();
                        
                  // Figure out if last-seen time should be updated, or if tracker is dead
                  TaskTrackerStatus newProfile = taskTrackers.get(leastRecent.getTrackerName());
                  // Items might leave the taskTracker set through other means; the
                  // status stored in 'taskTrackers' might be null, which means the
                  // tracker has already been destroyed.
                  if (newProfile != null) {
                    if (now - newProfile.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL) {
                      // Remove completely after marking the tasks as 'KILLED'
                      lostTaskTracker(leastRecent.getTrackerName());
                      updateTaskTrackerStatus(trackerName, null);
                    } else {
                      // Update time by inserting latest profile
                      trackerExpiryQueue.add(newProfile);
                    }
                  }
                }
              }
            }
          }
        } catch (InterruptedException iex) {
          break;
        } catch (Exception t) {
          LOG.error("Tracker Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }
        
  }

  ///////////////////////////////////////////////////////
  // Used to remove old finished Jobs that have been around for too long
  ///////////////////////////////////////////////////////
  class RetireJobs implements Runnable {
    public RetireJobs() {
    }

    /**
     * The run method lives for the life of the JobTracker,
     * and removes Jobs that are not still running, but which
     * finished a long time ago.
     */
    public void run() {
      while (true) {
        try {
          Thread.sleep(RETIRE_JOB_CHECK_INTERVAL);
          List<JobInProgress> retiredJobs = new ArrayList<JobInProgress>();
          long retireBefore = System.currentTimeMillis() - 
            RETIRE_JOB_INTERVAL;
          synchronized (jobsByPriority) {
            for(JobInProgress job: jobsByPriority) {
              if (job.getStatus().getRunState() != JobStatus.RUNNING &&
                  job.getStatus().getRunState() != JobStatus.PREP &&
                  (job.getFinishTime()  < retireBefore)) {
                retiredJobs.add(job);
              }
            }
          }
          if (!retiredJobs.isEmpty()) {
            synchronized (JobTracker.this) {
              synchronized (jobs) {
                synchronized (jobsByPriority) {
                  synchronized (jobInitQueue) {
                    for (JobInProgress job: retiredJobs) {
                      removeJobTasks(job);
                      jobs.remove(job.getProfile().getJobID());
                      jobInitQueue.remove(job);
                      jobsByPriority.remove(job);
                      String jobUser = job.getProfile().getUser();
                      synchronized (userToJobsMap) {
                        ArrayList<JobInProgress> userJobs =
                          userToJobsMap.get(jobUser);
                        synchronized (userJobs) {
                          userJobs.remove(job);
                        }
                        if (userJobs.isEmpty()) {
                          userToJobsMap.remove(jobUser);
                        }
                      }
                      LOG.info("Retired job with id: '" + 
                               job.getProfile().getJobID() + "' of user '" +
                               jobUser + "'");
                    }
                  }
                }
              }
            }
          }
        } catch (InterruptedException t) {
          break;
        } catch (Throwable t) {
          LOG.error("Error in retiring job:\n" +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////
  //  Used to init new jobs that have just been created
  /////////////////////////////////////////////////////////////////
  class JobInitThread implements Runnable {
    public JobInitThread() {
    }
    public void run() {
      JobInProgress job;
      while (true) {
        job = null;
        try {
          synchronized (jobInitQueue) {
            while (jobInitQueue.isEmpty()) {
              jobInitQueue.wait();
            }
            job = jobInitQueue.remove(0);
          }
          job.initTasks();
        } catch (InterruptedException t) {
          break;
        } catch (Throwable t) {
          LOG.error("Job initialization failed:\n" +
                    StringUtils.stringifyException(t));
          if (job != null) {
            job.kill();
          }
        }
      }
    }
  }

  static class JobTrackerMetrics implements Updater {
    private MetricsRecord metricsRecord = null;
    private int numMapTasksLaunched = 0;
    private int numMapTasksCompleted = 0;
    private int numReduceTasksLaunched = 0;
    private int numReduceTasksCompleted = 0;
    private int numJobsSubmitted = 0;
    private int numJobsCompleted = 0;
    private JobTracker tracker;
      
    JobTrackerMetrics(JobTracker tracker, JobConf conf) {
      String sessionId = conf.getSessionId();
      // Initiate JVM Metrics
      JvmMetrics.init("JobTracker", sessionId);
      // Create a record for map-reduce metrics
      MetricsContext context = MetricsUtil.getContext("mapred");
      metricsRecord = MetricsUtil.createRecord(context, "jobtracker");
      metricsRecord.setTag("sessionId", sessionId);
      this.tracker = tracker;
      context.registerUpdater(this);
    }
      
    /**
     * Since this object is a registered updater, this method will be called
     * periodically, e.g. every 5 seconds.
     */
    public void doUpdates(MetricsContext unused) {
      synchronized (this) {
        metricsRecord.incrMetric("maps_launched", numMapTasksLaunched);
        metricsRecord.incrMetric("maps_completed", numMapTasksCompleted);
        metricsRecord.incrMetric("reduces_launched", numReduceTasksLaunched);
        metricsRecord.incrMetric("reduces_completed", numReduceTasksCompleted);
        metricsRecord.incrMetric("jobs_submitted", numJobsSubmitted);
        metricsRecord.incrMetric("jobs_completed", numJobsCompleted);
              
        numMapTasksLaunched = 0;
        numMapTasksCompleted = 0;
        numReduceTasksLaunched = 0;
        numReduceTasksCompleted = 0;
        numJobsSubmitted = 0;
        numJobsCompleted = 0;
      }
      metricsRecord.update();

      if (tracker != null) {
        for (JobInProgress jip : tracker.getRunningJobs()) {
          jip.updateMetrics();
        }
      }
    }

    synchronized void launchMap() {
      ++numMapTasksLaunched;
    }
      
    synchronized void completeMap() {
      ++numMapTasksCompleted;
    }
      
    synchronized void launchReduce() {
      ++numReduceTasksLaunched;
    }
      
    synchronized void completeReduce() {
      ++numReduceTasksCompleted;
    }
      
    synchronized void submitJob() {
      ++numJobsSubmitted;
    }
      
    synchronized void completeJob() {
      ++numJobsCompleted;
    }
  }

  private JobTrackerMetrics myMetrics = null;
    
  /////////////////////////////////////////////////////////////////
  // The real JobTracker
  ////////////////////////////////////////////////////////////////
  int port;
  String localMachine;
  private String trackerIdentifier;
  long startTime;
  int totalSubmissions = 0;
  private int totalMapTaskCapacity;
  private int totalReduceTaskCapacity;
  private HostsFileReader hostsReader;

  //
  // Properties to maintain while running Jobs and Tasks:
  //
  // 1.  Each Task is always contained in a single Job.  A Job succeeds when all its 
  //     Tasks are complete.
  //
  // 2.  Every running or successful Task is assigned to a Tracker.  Idle Tasks are not.
  //
  // 3.  When a Tracker fails, all of its assigned Tasks are marked as failures.
  //
  // 4.  A Task might need to be reexecuted if it (or the machine it's hosted on) fails
  //     before the Job is 100% complete.  Sometimes an upstream Task can fail without
  //     reexecution if all downstream Tasks that require its output have already obtained
  //     the necessary files.
  //

  // All the known jobs.  (jobid->JobInProgress)
  Map<JobID, JobInProgress> jobs = new TreeMap<JobID, JobInProgress>();
  List<JobInProgress> jobsByPriority = new ArrayList<JobInProgress>();

  // (user -> list of JobInProgress)
  TreeMap<String, ArrayList<JobInProgress>> userToJobsMap =
    new TreeMap<String, ArrayList<JobInProgress>>();
    
  // All the known TaskInProgress items, mapped to by taskids (taskid->TIP)
  Map<TaskAttemptID, TaskInProgress> taskidToTIPMap =
    new TreeMap<TaskAttemptID, TaskInProgress>();

  // (taskid --> trackerID) 
  TreeMap<TaskAttemptID, String> taskidToTrackerMap = new TreeMap<TaskAttemptID, String>();

  // (trackerID->TreeSet of taskids running at that tracker)
  TreeMap<String, Set<TaskAttemptID>> trackerToTaskMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // (trackerID -> TreeSet of completed taskids running at that tracker)
  TreeMap<String, Set<TaskAttemptID>> trackerToMarkedTasksMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // (trackerID --> last sent HeartBeatResponse)
  Map<String, HeartbeatResponse> trackerToHeartbeatResponseMap = 
    new TreeMap<String, HeartbeatResponse>();

  // (hostname --> Node (NetworkTopology))
  Map<String, Node> hostnameToNodeMap = 
    Collections.synchronizedMap(new TreeMap<String, Node>());
  
  // Number of resolved entries
  int numResolved;
    
  //
  // Watch and expire TaskTracker objects using these structures.
  // We can map from Name->TaskTrackerStatus, or we can expire by time.
  //
  int totalMaps = 0;
  int totalReduces = 0;
  private HashMap<String, TaskTrackerStatus> taskTrackers =
    new HashMap<String, TaskTrackerStatus>();
  HashMap<String,Integer>uniqueHostsMap = new HashMap<String, Integer>();
  List<JobInProgress> jobInitQueue = new ArrayList<JobInProgress>();
  ExpireTrackers expireTrackers = new ExpireTrackers();
  Thread expireTrackersThread = null;
  RetireJobs retireJobs = new RetireJobs();
  Thread retireJobsThread = null;
  JobInitThread initJobs = new JobInitThread();
  Thread initJobsThread = null;
  ExpireLaunchingTasks expireLaunchingTasks = new ExpireLaunchingTasks();
  Thread expireLaunchingTaskThread = new Thread(expireLaunchingTasks,
                                                "expireLaunchingTasks");

  CompletedJobStatusStore completedJobStatusStore = null;
  Thread completedJobsStoreThread = null;

  /**
   * It might seem like a bug to maintain a TreeSet of status objects,
   * which can be updated at any time.  But that's not what happens!  We
   * only update status objects in the taskTrackers table.  Status objects
   * are never updated once they enter the expiry queue.  Instead, we wait
   * for them to expire and remove them from the expiry queue.  If a status
   * object has been updated in the taskTracker table, the latest status is 
   * reinserted.  Otherwise, we assume the tracker has expired.
   */
  TreeSet<TaskTrackerStatus> trackerExpiryQueue =
    new TreeSet<TaskTrackerStatus>(
                                   new Comparator<TaskTrackerStatus>() {
                                     public int compare(TaskTrackerStatus p1, TaskTrackerStatus p2) {
                                       if (p1.getLastSeen() < p2.getLastSeen()) {
                                         return -1;
                                       } else if (p1.getLastSeen() > p2.getLastSeen()) {
                                         return 1;
                                       } else {
                                         return (p1.getTrackerName().compareTo(p2.getTrackerName()));
                                       }
                                     }
                                   }
                                   );

  // Used to provide an HTML view on Job, Task, and TaskTracker structures
  StatusHttpServer infoServer;
  int infoPort;

  Server interTrackerServer;

  // Some jobs are stored in a local system directory.  We can delete
  // the files when we're done with the job.
  static final String SUBDIR = "jobTracker";
  FileSystem fs = null;
  Path systemDir = null;
  private JobConf conf;

  private Thread taskCommitThread;
  
  /**
   * Start the JobTracker process, listen on the indicated port
   */
  JobTracker(JobConf conf) throws IOException, InterruptedException {
    //
    // Grab some static constants
    //
    TASKTRACKER_EXPIRY_INTERVAL = 
      conf.getLong("mapred.tasktracker.expiry.interval", 10 * 60 * 1000);
    RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
    RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);
    TASK_ALLOC_EPSILON = conf.getFloat("mapred.jobtracker.taskalloc.loadbalance.epsilon", 0.2f);
    PAD_FRACTION = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                 0.01f);
    MAX_COMPLETE_USER_JOBS_IN_MEMORY = conf.getInt("mapred.jobtracker.completeuserjobs.maximum", 100);

    // This is a directory of temporary submission files.  We delete it
    // on startup, and can delete any files that we're done with
    this.conf = conf;
    JobConf jobConf = new JobConf(conf);

    // Read the hosts/exclude files to restrict access to the jobtracker.
    this.hostsReader = new HostsFileReader(conf.get("mapred.hosts", ""),
                                           conf.get("mapred.hosts.exclude", ""));
                                           
    // Set ports, start RPC servers, etc.
    InetSocketAddress addr = getAddress(conf);
    this.localMachine = addr.getHostName();
    this.port = addr.getPort();
    int handlerCount = conf.getInt("mapred.job.tracker.handler.count", 10);

    this.dnsToSwitchMapping = (DNSToSwitchMapping)ReflectionUtils.newInstance(
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);

    this.interTrackerServer = RPC.getServer(this, addr.getHostName(), addr.getPort(), handlerCount, false, conf);
    this.interTrackerServer.start();
    if (LOG.isDebugEnabled()) {
      Properties p = System.getProperties();
      for (Iterator it = p.keySet().iterator(); it.hasNext();) {
        String key = (String) it.next();
        String val = p.getProperty(key);
        LOG.debug("Property '" + key + "' is " + val);
      }
    }

    String infoAddr = 
      NetUtils.getServerAddress(conf, "mapred.job.tracker.info.bindAddress",
                                "mapred.job.tracker.info.port",
                                "mapred.job.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    infoServer = new StatusHttpServer("job", infoBindAddress, tmpInfoPort, 
                                      tmpInfoPort == 0);
    infoServer.setAttribute("job.tracker", this);
    // initialize history parameters.
    boolean historyInitialized = JobHistory.init(conf, this.localMachine);
    String historyLogDir = null;
    FileSystem historyFS = null;
    if (historyInitialized) {
      historyLogDir = conf.get("hadoop.job.history.location");
      infoServer.setAttribute("historyLogDir", historyLogDir);
      historyFS = new Path(historyLogDir).getFileSystem(conf);
      infoServer.setAttribute("fileSys", historyFS);
    }
    infoServer.start();

    this.startTime = System.currentTimeMillis();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
    trackerIdentifier = dateFormat.format(new Date());

    myMetrics = new JobTrackerMetrics(this, jobConf);
    
    // The rpc/web-server ports can be ephemeral ports... 
    // ... ensure we have the correct info
    this.port = interTrackerServer.getListenerAddress().getPort();
    this.conf.set("mapred.job.tracker", (this.localMachine + ":" + this.port));
    LOG.info("JobTracker up at: " + this.port);
    this.infoPort = this.infoServer.getPort();
    this.conf.set("mapred.job.tracker.http.address", 
        infoBindAddress + ":" + this.infoPort); 
    LOG.info("JobTracker webserver: " + this.infoServer.getPort());
    
    while (true) {
      try {
        // if we haven't contacted the namenode go ahead and do it
        if (fs == null) {
          fs = FileSystem.get(conf);
        }
        // clean up the system dir, which will only work if hdfs is out of 
        // safe mode
        if(systemDir == null) {
          systemDir = new Path(getSystemDir());    
        }
        fs.delete(systemDir, true);
        if (FileSystem.mkdirs(fs, systemDir, 
            new FsPermission(SYSTEM_DIR_PERMISSION))) {
          break;
        }
        LOG.error("Mkdirs failed to create " + systemDir);
      } catch (IOException ie) {
        if (ie instanceof RemoteException && 
            AccessControlException.class.getName().equals(
                ((RemoteException)ie).getClassName())) {
          throw ie;
        }
        LOG.info("problem cleaning system directory: " + systemDir, ie);
      }
      Thread.sleep(SYSTEM_DIR_CLEANUP_RETRY_PERIOD);
    }
    // Same with 'localDir' except it's always on the local disk.
    jobConf.deleteLocalFiles(SUBDIR);

    // Initialize history again if it is not initialized
    // because history was on dfs and namenode was in safemode.
    if (!historyInitialized) {
      JobHistory.init(conf, this.localMachine); 
      historyLogDir = conf.get("hadoop.job.history.location");
      infoServer.setAttribute("historyLogDir", historyLogDir);
      historyFS = new Path(historyLogDir).getFileSystem(conf);
      infoServer.setAttribute("fileSys", historyFS);
    }

    this.numTaskCacheLevels = conf.getInt("mapred.task.cache.levels", 
        NetworkTopology.DEFAULT_HOST_LEVEL);
    synchronized (this) {
      state = State.RUNNING;
    }

    //initializes the job status store
    completedJobStatusStore = new CompletedJobStatusStore(conf,fs);

    LOG.info("Starting RUNNING");
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String jobTrackerStr =
      conf.get("mapred.job.tracker", "localhost:8012");
    return NetUtils.createSocketAddr(jobTrackerStr);
  }

  /**
   * Run forever
   */
  public void offerService() throws InterruptedException {
    this.expireTrackersThread = new Thread(this.expireTrackers,
                                          "expireTrackers");
    this.expireTrackersThread.start();
    this.retireJobsThread = new Thread(this.retireJobs, "retireJobs");
    this.retireJobsThread.start();
    this.initJobsThread = new Thread(this.initJobs, "initJobs");
    this.initJobsThread.start();
    expireLaunchingTaskThread.start();
    this.taskCommitThread = new TaskCommitQueue();
    this.taskCommitThread.start();

    if (completedJobStatusStore.isActive()) {
      completedJobsStoreThread = new Thread(completedJobStatusStore,
                                            "completedjobsStore-housekeeper");
      completedJobsStoreThread.start();
    }

    this.interTrackerServer.join();
    LOG.info("Stopped interTrackerServer");
  }

  void close() throws IOException {
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.interTrackerServer != null) {
      LOG.info("Stopping interTrackerServer");
      this.interTrackerServer.stop();
    }
    if (this.expireTrackersThread != null && this.expireTrackersThread.isAlive()) {
      LOG.info("Stopping expireTrackers");
      this.expireTrackersThread.interrupt();
      try {
        this.expireTrackersThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.retireJobsThread != null && this.retireJobsThread.isAlive()) {
      LOG.info("Stopping retirer");
      this.retireJobsThread.interrupt();
      try {
        this.retireJobsThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.initJobsThread != null && this.initJobsThread.isAlive()) {
      LOG.info("Stopping initer");
      this.initJobsThread.interrupt();
      try {
        this.initJobsThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.expireLaunchingTaskThread != null && this.expireLaunchingTaskThread.isAlive()) {
      LOG.info("Stopping expireLaunchingTasks");
      this.expireLaunchingTaskThread.interrupt();
      try {
        this.expireLaunchingTaskThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.taskCommitThread != null) {
      LOG.info("Stopping TaskCommit thread");
      this.taskCommitThread.interrupt();
      try {
        this.taskCommitThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.completedJobsStoreThread != null &&
        this.completedJobsStoreThread.isAlive()) {
      LOG.info("Stopping completedJobsStore thread");
      this.completedJobsStoreThread.interrupt();
      try {
        this.completedJobsStoreThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    LOG.info("stopped all jobtracker services");
    return;
  }
    
  ///////////////////////////////////////////////////////
  // Maintain lookup tables; called by JobInProgress
  // and TaskInProgress
  ///////////////////////////////////////////////////////
  void createTaskEntry(TaskAttemptID taskid, String taskTracker, TaskInProgress tip) {
    LOG.info("Adding task '" + taskid + "' to tip " + tip.getTIPId() + ", for tracker '" + taskTracker + "'");

    // taskid --> tracker
    taskidToTrackerMap.put(taskid, taskTracker);

    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToTaskMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToTaskMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);

    // taskid --> TIP
    taskidToTIPMap.put(taskid, tip);
  }
    
  void removeTaskEntry(TaskAttemptID taskid) {
    // taskid --> tracker
    String tracker = taskidToTrackerMap.remove(taskid);

    // tracker --> taskid
    if (tracker != null) {
      Set<TaskAttemptID> trackerSet = trackerToTaskMap.get(tracker);
      if (trackerSet != null) {
        trackerSet.remove(taskid);
      }
    }

    // taskid --> TIP
    taskidToTIPMap.remove(taskid);
        
    LOG.debug("Removing task '" + taskid + "'");
  }
    
  /**
   * Mark a 'task' for removal later.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param taskTracker the tasktracker at which the 'task' was running
   * @param taskid completed (success/failure/killed) task
   */
  void markCompletedTaskAttempt(String taskTracker, TaskAttemptID taskid) {
    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToMarkedTasksMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToMarkedTasksMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);
      
    LOG.debug("Marked '" + taskid + "' from '" + taskTracker + "'");
  }

  /**
   * Mark all 'non-running' jobs of the job for pruning.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param job the completed job
   */
  void markCompletedJob(JobInProgress job) {
    for (TaskInProgress tip : job.getMapTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getReduceTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
  }
    
  /**
   * Remove all 'marked' tasks running on a given {@link TaskTracker}
   * from the {@link JobTracker}'s data-structures.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param taskTracker tasktracker whose 'non-running' tasks are to be purged
   */
  private void removeMarkedTasks(String taskTracker) {
    // Purge all the 'marked' tasks which were running at taskTracker
    Set<TaskAttemptID> markedTaskSet = 
      trackerToMarkedTasksMap.get(taskTracker);
    if (markedTaskSet != null) {
      for (TaskAttemptID taskid : markedTaskSet) {
        removeTaskEntry(taskid);
        LOG.info("Removed completed task '" + taskid + "' from '" + 
                 taskTracker + "'");
      }
      // Clear
      trackerToMarkedTasksMap.remove(taskTracker);
    }
  }
    
  /**
   * Call {@link #removeTaskEntry(String)} for each of the
   * job's tasks.
   * When the JobTracker is retiring the long-completed
   * job, either because it has outlived {@link #RETIRE_JOB_INTERVAL}
   * or the limit of {@link #MAX_COMPLETE_USER_JOBS_IN_MEMORY} jobs 
   * has been reached, we can afford to nuke all it's tasks; a little
   * unsafe, but practically feasible. 
   * 
   * @param job the job about to be 'retired'
   */
  synchronized private void removeJobTasks(JobInProgress job) { 
    for (TaskInProgress tip : job.getMapTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        removeTaskEntry(taskStatus.getTaskID());
      }
    }
    for (TaskInProgress tip : job.getReduceTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        removeTaskEntry(taskStatus.getTaskID());
      }
    }
  }
    
  /**
   * Safe clean-up all data structures at the end of the 
   * job (success/failure/killed).
   * Here we also ensure that for a given user we maintain 
   * information for only MAX_COMPLETE_USER_JOBS_IN_MEMORY jobs 
   * on the JobTracker.
   *  
   * @param job completed job.
   */
  synchronized void finalizeJob(JobInProgress job) {
    // Mark the 'non-running' tasks for pruning
    markCompletedJob(job);

    //persists the job info in DFS
    completedJobStatusStore.store(job);
    
    JobEndNotifier.registerNotification(job.getJobConf(), job.getStatus());

    // Purge oldest jobs and keep at-most MAX_COMPLETE_USER_JOBS_IN_MEMORY jobs of a given user
    // in memory; information about the purged jobs is available via
    // JobHistory.
    synchronized (jobs) {
      synchronized (jobsByPriority) {
        synchronized (jobInitQueue) {
          synchronized (userToJobsMap) {
            String jobUser = job.getProfile().getUser();
            if (!userToJobsMap.containsKey(jobUser)) {
              userToJobsMap.put(jobUser, 
                                new ArrayList<JobInProgress>());
            }
            ArrayList<JobInProgress> userJobs = 
              userToJobsMap.get(jobUser);
            synchronized (userJobs) {
              // Add the currently completed 'job'
              userJobs.add(job);

              // Check if we need to retire some jobs of this user
              while (userJobs.size() > 
                     MAX_COMPLETE_USER_JOBS_IN_MEMORY) {
                JobInProgress rjob = userJobs.get(0);
                  
                // Do not delete 'current'
                // finished job just yet.
                if (rjob == job) {
                  break;
                }
                  
                // Cleanup all datastructures
                int rjobRunState = 
                  rjob.getStatus().getRunState();
                if (rjobRunState == JobStatus.SUCCEEDED || 
                    rjobRunState == JobStatus.FAILED) {
                  // Ok, this call to removeTaskEntries
                  // is dangerous is some very very obscure
                  // cases; e.g. when rjob completed, hit
                  // MAX_COMPLETE_USER_JOBS_IN_MEMORY job
                  // limit and yet some task (taskid)
                  // wasn't complete!
                  removeJobTasks(rjob);
                    
                  userJobs.remove(0);
                  jobs.remove(rjob.getProfile().getJobID());
                  jobInitQueue.remove(rjob);
                  jobsByPriority.remove(rjob);
                    
                  LOG.info("Retired job with id: '" + 
                           rjob.getProfile().getJobID() + "' of user: '" +
                           jobUser + "'");
                } else {
                  // Do not remove jobs that aren't complete.
                  // Stop here, and let the next pass take
                  // care of purging jobs.
                  break;
                }
              }
            }
            if (userJobs.isEmpty()) {
              userToJobsMap.remove(jobUser);
            }
          }
        }
      }
    }
  }

  ///////////////////////////////////////////////////////
  // Accessors for objects that want info on jobs, tasks,
  // trackers, etc.
  ///////////////////////////////////////////////////////
  public int getTotalSubmissions() {
    return totalSubmissions;
  }
  public String getJobTrackerMachine() {
    return localMachine;
  }
  
  /**
   * Get the unique identifier (ie. timestamp) of this job tracker start.
   * @return a string with a unique identifier
   */
  public String getTrackerIdentifier() {
    return trackerIdentifier;
  }

  public int getTrackerPort() {
    return port;
  }
  public int getInfoPort() {
    return infoPort;
  }
  public long getStartTime() {
    return startTime;
  }
  public Vector<JobInProgress> runningJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.RUNNING) {
        v.add(jip);
      }
    }
    return v;
  }
  /**
   * Version that is called from a timer thread, and therefore needs to be
   * careful to synchronize.
   */
  public synchronized List<JobInProgress> getRunningJobs() {
    synchronized (jobs) {
      return runningJobs();
    }
  }
  public Vector<JobInProgress> failedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.FAILED) {
        v.add(jip);
      }
    }
    return v;
  }
  public Vector<JobInProgress> completedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.SUCCEEDED) {
        v.add(jip);
      }
    }
    return v;
  }
  public Collection taskTrackers() {
    synchronized (taskTrackers) {
      return taskTrackers.values();
    }
  }
  public TaskTrackerStatus getTaskTracker(String trackerID) {
    synchronized (taskTrackers) {
      return taskTrackers.get(trackerID);
    }
  }

  public Node resolveAndAddToTopology(String name) {
    List <String> tmpList = new ArrayList<String>(1);
    tmpList.add(name);
    List <String> rNameList = dnsToSwitchMapping.resolve(tmpList);
    String rName = rNameList.get(0);
    String networkLoc = NodeBase.normalize(rName);
    return addHostToNodeMapping(name, networkLoc);
  }
  
  private Node addHostToNodeMapping(String host, String networkLoc) {
    Node node;
    if ((node = clusterMap.getNode(networkLoc+"/"+host)) == null) {
      node = new NodeBase(host, networkLoc);
      clusterMap.add(node);
      if (node.getLevel() < getNumTaskCacheLevels()) {
        LOG.fatal("Got a host whose level is: " + node.getLevel() + "." 
                  + " Should get at least a level of value: " 
                  + getNumTaskCacheLevels());
        try {
          stopTracker();
        } catch (IOException ie) {
          LOG.warn("Exception encountered during shutdown: " 
                   + StringUtils.stringifyException(ie));
          System.exit(-1);
        }
      }
      hostnameToNodeMap.put(host, node);
      // Make an entry for the node at the max level in the cache
      nodesAtMaxLevel.add(getParentNode(node, getNumTaskCacheLevels() - 1));
    }
    return node;
  }

  /**
   * Returns a collection of nodes at the max level
   */
  public Collection<Node> getNodesAtMaxLevel() {
    return nodesAtMaxLevel;
  }

  public static Node getParentNode(Node node, int level) {
    for (int i = 0; i < level; ++i) {
      node = node.getParent();
    }
    return node;
  }

  /**
   * Return the Node in the network topology that corresponds to the hostname
   */
  public Node getNode(String name) {
    return hostnameToNodeMap.get(name);
  }
  public int getNumTaskCacheLevels() {
    return numTaskCacheLevels;
  }
  public int getNumResolvedTaskTrackers() {
    return numResolved;
  }
  ////////////////////////////////////////////////////
  // InterTrackerProtocol
  ////////////////////////////////////////////////////
  
  public String getBuildVersion() throws IOException{
    return VersionInfo.getBuildVersion();
  }

  /**
   * The periodic heartbeat mechanism between the {@link TaskTracker} and
   * the {@link JobTracker}.
   * 
   * The {@link JobTracker} processes the status information sent by the 
   * {@link TaskTracker} and responds with instructions to start/stop 
   * tasks or jobs, and also 'reset' instructions during contingencies. 
   */
  public synchronized HeartbeatResponse heartbeat(TaskTrackerStatus status, 
                                                  boolean initialContact, boolean acceptNewTasks, short responseId) 
    throws IOException {
    LOG.debug("Got heartbeat from: " + status.getTrackerName() + 
              " (initialContact: " + initialContact + 
              " acceptNewTasks: " + acceptNewTasks + ")" +
              " with responseId: " + responseId);

    // Make sure heartbeat is from a tasktracker allowed by the jobtracker.
    if (!acceptTaskTracker(status)) {
      throw new DisallowedTaskTrackerException(status);
    }

    // First check if the last heartbeat response got through
    String trackerName = status.getTrackerName();
    
    HeartbeatResponse prevHeartbeatResponse =
      trackerToHeartbeatResponseMap.get(trackerName);

    if (initialContact != true) {
      // If this isn't the 'initial contact' from the tasktracker,
      // there is something seriously wrong if the JobTracker has
      // no record of the 'previous heartbeat'; if so, ask the 
      // tasktracker to re-initialize itself.
      if (prevHeartbeatResponse == null) {
        LOG.warn("Serious problem, cannot find record of 'previous' " +
                 "heartbeat for '" + trackerName + 
                 "'; reinitializing the tasktracker");
        return new HeartbeatResponse(responseId, 
                                     new TaskTrackerAction[] {new ReinitTrackerAction()});

      }
                
      // It is completely safe to not process a 'duplicate' heartbeat from a 
      // {@link TaskTracker} since it resends the heartbeat when rpcs are lost - 
      // @see {@link TaskTracker.transmitHeartbeat()};
      // acknowledge it by re-sending the previous response to let the 
      // {@link TaskTracker} go forward. 
      if (prevHeartbeatResponse.getResponseId() != responseId) {
        LOG.info("Ignoring 'duplicate' heartbeat from '" + 
                 trackerName + "'; resending the previous 'lost' response");
        return prevHeartbeatResponse;
      }
    }
      
    // Register the tracker if its not registered
    if (getNode(trackerName) == null) {
      // Making the network location resolution inline .. 
      resolveAndAddToTopology(status.getHost());
    }
    
    // Process this heartbeat 
    short newResponseId = (short)(responseId + 1);
    if (!processHeartbeat(status, initialContact)) {
      if (prevHeartbeatResponse != null) {
        trackerToHeartbeatResponseMap.remove(trackerName);
      }

      return new HeartbeatResponse(newResponseId, 
                                   new TaskTrackerAction[] {new ReinitTrackerAction()});
    }
      
    // Initialize the response to be sent for the heartbeat
    HeartbeatResponse response = new HeartbeatResponse(newResponseId, null);
    List<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();
      
    // Check for new tasks to be executed on the tasktracker
    if (acceptNewTasks) {
      Task task = getNewTaskForTaskTracker(trackerName);
      if (task != null) {
        LOG.debug(trackerName + " -> LaunchTask: " + task.getTaskID());
        actions.add(new LaunchTaskAction(task));
      }
    }
      
    // Check for tasks to be killed
    List<TaskTrackerAction> killTasksList = getTasksToKill(trackerName);
    if (killTasksList != null) {
      actions.addAll(killTasksList);
    }
     
    // calculate next heartbeat interval and put in heartbeat response
    int nextInterval = getNextHeartbeatInterval();
    response.setHeartbeatInterval(nextInterval);
    response.setActions(
                        actions.toArray(new TaskTrackerAction[actions.size()]));
        
    // Update the trackerToHeartbeatResponseMap
    trackerToHeartbeatResponseMap.put(trackerName, response);

    // Done processing the hearbeat, now remove 'marked' tasks
    removeMarkedTasks(trackerName);
        
    return response;
  }
  
  /**
   * Calculates next heartbeat interval using cluster size.
   * Heartbeat interval is incremented 1second for every 50 nodes. 
   * @return next heartbeat interval.
   */
  private int getNextHeartbeatInterval() {
    // get the no of task trackers
    int clusterSize = getClusterStatus().getTaskTrackers();
    int heartbeatInterval =  Math.max(
                                1000 * (clusterSize / CLUSTER_INCREMENT + 1),
                                HEARTBEAT_INTERVAL_MIN) ;
    return heartbeatInterval;
  }

  /**
   * Return if the specified tasktracker is in the hosts list, 
   * if one was configured.  If none was configured, then this 
   * returns true.
   */
  private boolean inHostsList(TaskTrackerStatus status) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || hostsList.contains(status.getHost()));
  }

  /**
   * Return if the specified tasktracker is in the exclude list.
   */
  private boolean inExcludedHostsList(TaskTrackerStatus status) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return excludeList.contains(status.getHost());
  }

  /**
   * Returns true if the tasktracker is in the hosts list and 
   * not in the exclude list. 
   */
  private boolean acceptTaskTracker(TaskTrackerStatus status) {
    return (inHostsList(status) && !inExcludedHostsList(status));
  }
    
  /**
   * Update the last recorded status for the given task tracker.
   * It assumes that the taskTrackers are locked on entry.
   * @param trackerName The name of the tracker
   * @param status The new status for the task tracker
   * @return Was an old status found?
   */
  private boolean updateTaskTrackerStatus(String trackerName,
                                          TaskTrackerStatus status) {
    TaskTrackerStatus oldStatus = taskTrackers.get(trackerName);
    if (oldStatus != null) {
      totalMaps -= oldStatus.countMapTasks();
      totalReduces -= oldStatus.countReduceTasks();
      totalMapTaskCapacity -= oldStatus.getMaxMapTasks();
      totalReduceTaskCapacity -= oldStatus.getMaxReduceTasks();
      if (status == null) {
        taskTrackers.remove(trackerName);
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(oldStatus.getHost());
        numTaskTrackersInHost --;
        if (numTaskTrackersInHost > 0)  {
          uniqueHostsMap.put(oldStatus.getHost(), numTaskTrackersInHost);
        }
        else {
          uniqueHostsMap.remove(oldStatus.getHost());
        }
      }
    }
    if (status != null) {
      totalMaps += status.countMapTasks();
      totalReduces += status.countReduceTasks();
      totalMapTaskCapacity += status.getMaxMapTasks();
      totalReduceTaskCapacity += status.getMaxReduceTasks();
      boolean alreadyPresent = false;
      if (taskTrackers.containsKey(trackerName)) {
        alreadyPresent = true;
      }
      taskTrackers.put(trackerName, status);

      if (!alreadyPresent)  {
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(status.getHost());
        if (numTaskTrackersInHost == null) {
          numTaskTrackersInHost = 0;
        }
        numTaskTrackersInHost ++;
        uniqueHostsMap.put(status.getHost(), numTaskTrackersInHost);
      }
    }
    return oldStatus != null;
  }
    
  /**
   * Process incoming heartbeat messages from the task trackers.
   */
  private synchronized boolean processHeartbeat(
                                                TaskTrackerStatus trackerStatus, boolean initialContact) {
    String trackerName = trackerStatus.getTrackerName();
    trackerStatus.setLastSeen(System.currentTimeMillis());

    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        boolean seenBefore = updateTaskTrackerStatus(trackerName,
                                                     trackerStatus);
        if (initialContact) {
          // If it's first contact, then clear out 
          // any state hanging around
          if (seenBefore) {
            lostTaskTracker(trackerName);
          }
        } else {
          // If not first contact, there should be some record of the tracker
          if (!seenBefore) {
            LOG.warn("Status from unknown Tracker : " + trackerName);
            updateTaskTrackerStatus(trackerName, null);
            return false;
          }
        }

        if (initialContact) {
          trackerExpiryQueue.add(trackerStatus);
        }
      }
    }

    updateTaskStatuses(trackerStatus);
    
    return true;
  }

  /**
   * Returns a task we'd like the TaskTracker to execute right now.
   *
   * Eventually this function should compute load on the various TaskTrackers,
   * and incorporate knowledge of DFS file placement.  But for right now, it
   * just grabs a single item out of the pending task list and hands it back.
   */
  private synchronized Task getNewTaskForTaskTracker(String taskTracker
                                                     ) throws IOException {
    //
    // Compute average map and reduce task numbers across pool
    //
    int remainingReduceLoad = 0;
    int remainingMapLoad = 0;
    int numTaskTrackers;
    TaskTrackerStatus tts;

    synchronized (taskTrackers) {
      numTaskTrackers = taskTrackers.size();
      tts = taskTrackers.get(taskTracker);
    }
    if (tts == null) {
      LOG.warn("Unknown task tracker polling; ignoring: " + taskTracker);
      return null;
    }

    synchronized(jobsByPriority){
      for (Iterator it = jobsByPriority.iterator(); it.hasNext();) {
        JobInProgress job = (JobInProgress) it.next();
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          int totalMapTasks = job.desiredMaps();
          int totalReduceTasks = job.desiredReduces();
          remainingMapLoad += (totalMapTasks - job.finishedMaps());
          remainingReduceLoad += (totalReduceTasks - job.finishedReduces());
        }
      }   
    }

    int maxCurrentMapTasks = tts.getMaxMapTasks();
    int maxCurrentReduceTasks = tts.getMaxReduceTasks();
    // find out the maximum number of maps or reduces that we are willing
    // to run on any node.
    int maxMapLoad = 0;
    int maxReduceLoad = 0;
    if (numTaskTrackers > 0) {
      maxMapLoad = Math.min(maxCurrentMapTasks,
                            (int) Math.ceil((double) remainingMapLoad / 
                                            numTaskTrackers));
      maxReduceLoad = Math.min(maxCurrentReduceTasks,
                               (int) Math.ceil((double) remainingReduceLoad
                                               / numTaskTrackers));
    }
        
    //
    // Get map + reduce counts for the current tracker.
    //

    int numMaps = tts.countMapTasks();
    int numReduces = tts.countReduceTasks();

    //
    // In the below steps, we allocate first a map task (if appropriate),
    // and then a reduce task if appropriate.  We go through all jobs
    // in order of job arrival; jobs only get serviced if their 
    // predecessors are serviced, too.
    //

    //
    // We hand a task to the current taskTracker if the given machine 
    // has a workload that's less than the maximum load of that kind of
    // task.
    //
       
    synchronized (jobsByPriority) {
      if (numMaps < maxMapLoad) {

        int totalNeededMaps = 0;
        for (Iterator it = jobsByPriority.iterator(); it.hasNext();) {
          JobInProgress job = (JobInProgress) it.next();
          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }

          Task t = job.obtainNewMapTask(tts, numTaskTrackers,
                                        uniqueHostsMap.size());
          if (t != null) {
            expireLaunchingTasks.addNewTask(t.getTaskID());
            myMetrics.launchMap();
            return t;
          }

          //
          // Beyond the highest-priority task, reserve a little 
          // room for failures and speculative executions; don't 
          // schedule tasks to the hilt.
          //
          totalNeededMaps += job.desiredMaps();
          int padding = 0;
          if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
            padding = Math.min(maxCurrentMapTasks,
                               (int)(totalNeededMaps * PAD_FRACTION));
          }
          if (totalMaps + padding >= totalMapTaskCapacity) {
            break;
          }
        }
      }

      //
      // Same thing, but for reduce tasks
      //
      if (numReduces < maxReduceLoad) {

        int totalNeededReduces = 0;
        for (Iterator it = jobsByPriority.iterator(); it.hasNext();) {
          JobInProgress job = (JobInProgress) it.next();
          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
              job.numReduceTasks == 0) {
            continue;
          }

          Task t = job.obtainNewReduceTask(tts, numTaskTrackers, 
                                           uniqueHostsMap.size());
          if (t != null) {
            expireLaunchingTasks.addNewTask(t.getTaskID());
            myMetrics.launchReduce();
            return t;
          }

          //
          // Beyond the highest-priority task, reserve a little 
          // room for failures and speculative executions; don't 
          // schedule tasks to the hilt.
          //
          totalNeededReduces += job.desiredReduces();
          int padding = 0;
          if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
            padding = 
              Math.min(maxCurrentReduceTasks,
                       (int) (totalNeededReduces * PAD_FRACTION));
          }
          if (totalReduces + padding >= totalReduceTaskCapacity) {
            break;
          }
        }
      }
    }
    return null;
  }

  /**
   * A tracker wants to know if any of its Tasks have been
   * closed (because the job completed, whether successfully or not)
   */
  private synchronized List<TaskTrackerAction> getTasksToKill(
                                                              String taskTracker) {
    
    Set<TaskAttemptID> taskIds = trackerToTaskMap.get(taskTracker);
    if (taskIds != null) {
      List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
      Set<JobID> killJobIds = new TreeSet<JobID>(); 
      for (TaskAttemptID killTaskId : taskIds) {
        TaskInProgress tip = taskidToTIPMap.get(killTaskId);
        if (tip.shouldClose(killTaskId)) {
          // 
          // This is how the JobTracker ends a task at the TaskTracker.
          // It may be successfully completed, or may be killed in
          // mid-execution.
          //
          if (tip.getJob().getStatus().getRunState() == JobStatus.RUNNING) {
            killList.add(new KillTaskAction(killTaskId));
            LOG.debug(taskTracker + " -> KillTaskAction: " + killTaskId);
          } else {
            JobID killJobId = tip.getJob().getStatus().getJobID(); 
            killJobIds.add(killJobId);
          }
        }
      }
            
      for (JobID killJobId : killJobIds) {
        killList.add(new KillJobAction(killJobId));
        LOG.debug(taskTracker + " -> KillJobAction: " + killJobId);
      }

      return killList;
    }
    return null;
  }

  /**
   * Grab the local fs name
   */
  public synchronized String getFilesystemName() throws IOException {
    if (fs == null) {
      throw new IllegalStateException("FileSystem object not available yet");
    }
    return fs.getUri().toString();
  }


  public void reportTaskTrackerError(String taskTracker,
                                     String errorClass,
                                     String errorMessage) throws IOException {
    LOG.warn("Report from " + taskTracker + ": " + errorMessage);        
  }

  /**
   * Remove the job_ from jobids to get the unique string.
   */
  static String getJobUniqueString(String jobid) {
    return jobid.substring(4);
  }

  ////////////////////////////////////////////////////
  // JobSubmissionProtocol
  ////////////////////////////////////////////////////

  /**
   * Make sure the JobTracker is done initializing.
   */
  private synchronized void ensureRunning() throws IllegalStateException {
    if (state != State.RUNNING) {
      throw new IllegalStateException("Job tracker still initializing");
    }
  }

  /**
   * Allocates a new JobId string.
   */
  public synchronized JobID getNewJobId() throws IOException {
    ensureRunning();
    return new JobID(getTrackerIdentifier(), nextJobId++);
  }

  @Deprecated
  public JobStatus submitJob(String jobid) throws IOException {
    return submitJob(JobID.forName(jobid));
  }

  /**
   * JobTracker.submitJob() kicks off a new job.  
   *
   * Create a 'JobInProgress' object, which contains both JobProfile
   * and JobStatus.  Those two sub-objects are sometimes shipped outside
   * of the JobTracker.  But JobInProgress adds info that's useful for
   * the JobTracker alone.
   *
   * We add the JIP to the jobInitQueue, which is processed 
   * asynchronously to handle split-computation and build up
   * the right TaskTracker/Block mapping.
   */
  public synchronized JobStatus submitJob(JobID jobId) throws IOException {
    ensureRunning();
    if(jobs.containsKey(jobId)) {
      //job already running, don't start twice
      return jobs.get(jobId).getStatus();
    }
    
    totalSubmissions++;
    JobInProgress job = new JobInProgress(jobId, this, this.conf);
    synchronized (jobs) {
      synchronized (jobsByPriority) {
        synchronized (jobInitQueue) {
          jobs.put(job.getProfile().getJobID(), job);
          jobsByPriority.add(job);
          jobInitQueue.add(job);
          resortPriority();
          jobInitQueue.notifyAll();
        }
      }
    }
    myMetrics.submitJob();
    return job.getStatus();
  }

  /**
   * Sort jobs by priority and then by start time.
   */
  private synchronized void resortPriority() {
    Comparator<JobInProgress> comp = new Comparator<JobInProgress>() {
      public int compare(JobInProgress o1, JobInProgress o2) {
        int res = o1.getPriority().compareTo(o2.getPriority());
        if(res == 0) {
          if(o1.getStartTime() < o2.getStartTime())
            res = -1;
          else
            res = (o1.getStartTime()==o2.getStartTime() ? 0 : 1);
        }
          
        return res;
      }
    };
    
    synchronized(jobsByPriority) {
      Collections.sort(jobsByPriority, comp);
    }
    synchronized (jobInitQueue) {
      Collections.sort(jobInitQueue, comp);
    }
  }

  public synchronized ClusterStatus getClusterStatus() {
    synchronized (taskTrackers) {
      return new ClusterStatus(taskTrackers.size(),
                               totalMaps,
                               totalReduces,
                               totalMapTaskCapacity,
                               totalReduceTaskCapacity, 
                               state);          
    }
  }

  @Deprecated
  public void killJob(String id) {
    killJob(JobID.forName(id));
  }

  public synchronized void killJob(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    job.kill();
  }

  @Deprecated
  public JobProfile getJobProfile(String id) {
    return getJobProfile(JobID.forName(id));
  }

  public synchronized JobProfile getJobProfile(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getProfile();
    } else {
      return completedJobStatusStore.readJobProfile(jobid);
    }
  }
  
  @Deprecated
  public JobStatus getJobStatus(String id) {
    return getJobStatus(JobID.forName(id));
  }

  public synchronized JobStatus getJobStatus(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getStatus();
    } else {
      return completedJobStatusStore.readJobStatus(jobid);
    }
  }

  @Deprecated
  public Counters getJobCounters(String id) {
    return getJobCounters(JobID.forName(id));
  }

  public synchronized Counters getJobCounters(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getCounters();
    } else {
      return completedJobStatusStore.readCounters(jobid);
    }
  }
  
  @Deprecated
  public TaskReport[] getMapTaskReports(String jobid) {
    return getMapTaskReports(JobID.forName(jobid));
  }

  public synchronized TaskReport[] getMapTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null) {
      return new TaskReport[0];
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeMapTasks =
        job.reportTasksInProgress(true, true);
      for (Iterator it = completeMapTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteMapTasks =
        job.reportTasksInProgress(true, false);
      for (Iterator it = incompleteMapTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  @Deprecated
  public TaskReport[] getReduceTaskReports(String jobid) {
    return getReduceTaskReports(JobID.forName(jobid));
  }

  public synchronized TaskReport[] getReduceTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null) {
      return new TaskReport[0];
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector completeReduceTasks = job.reportTasksInProgress(false, true);
      for (Iterator it = completeReduceTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector incompleteReduceTasks = job.reportTasksInProgress(false, false);
      for (Iterator it = incompleteReduceTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  @Deprecated
  public TaskCompletionEvent[] getTaskCompletionEvents(String jobid, int fromid, 
                                                       int maxevents
                                                      ) throws IOException {
    return getTaskCompletionEvents(JobID.forName(jobid), fromid, maxevents);
  }

  /* 
   * Returns a list of TaskCompletionEvent for the given job, 
   * starting from fromEventId.
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getTaskCompletionEvents(java.lang.String, int, int)
   */
  public synchronized TaskCompletionEvent[] getTaskCompletionEvents(
      JobID jobid, int fromEventId, int maxEvents) throws IOException{
    TaskCompletionEvent[] events;

    JobInProgress job = this.jobs.get(jobid);
    if (null != job) {
      events = job.getTaskCompletionEvents(fromEventId, maxEvents);
    }
    else {
      events = completedJobStatusStore.readJobTaskCompletionEvents(jobid, fromEventId, maxEvents);
    }
    return events;
  }

  @Deprecated
  public String[] getTaskDiagnostics(String jobid, String tipid, 
                                     String taskid) throws IOException {
    return getTaskDiagnostics(TaskAttemptID.forName(taskid));
  }

  /**
   * Get the diagnostics for a given task
   * @param taskId the id of the task
   * @return an array of the diagnostic messages
   */
  public synchronized String[] getTaskDiagnostics(TaskAttemptID taskId)  
    throws IOException {
    
    JobID jobId = taskId.getJobID();
    TaskID tipId = taskId.getTaskID();
    JobInProgress job = jobs.get(jobId);
    if (job == null) {
      throw new IllegalArgumentException("Job " + jobId + " not found.");
    }
    TaskInProgress tip = job.getTaskInProgress(tipId);
    if (tip == null) {
      throw new IllegalArgumentException("TIP " + tipId + " not found.");
    }
    List<String> taskDiagnosticInfo = tip.getDiagnosticInfo(taskId);
    return ((taskDiagnosticInfo == null) ? null 
            : taskDiagnosticInfo.toArray(new String[0]));
  }
    
  /** Get all the TaskStatuses from the tipid. */
  TaskStatus[] getTaskStatuses(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? new TaskStatus[0] 
            : tip.getTaskStatuses());
  }

  /** Returns the TaskStatus for a particular taskid. */
  TaskStatus getTaskStatus(TaskAttemptID taskid) {
    TaskInProgress tip = getTip(taskid.getTaskID());
    return (tip == null ? null 
            : tip.getTaskStatus(taskid));
  }
    
  /**
   * Returns the counters for the specified task in progress.
   */
  Counters getTipCounters(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? null : tip.getCounters());
  }
    
  /**
   * Returns specified TaskInProgress, or null.
   */
  public TaskInProgress getTip(TaskID tipid) {
    JobInProgress job = jobs.get(tipid.getJobID());
    return (job == null ? null : job.getTaskInProgress(tipid));
  }

  @Deprecated
  public boolean killTask(String taskId, boolean shouldFail) throws IOException{
    return killTask(TaskAttemptID.forName(taskId), shouldFail);
  }

  /** Mark a Task to be killed */
  public synchronized boolean killTask(TaskAttemptID taskid, boolean shouldFail) throws IOException{
    TaskInProgress tip = taskidToTIPMap.get(taskid);
    if(tip != null) {
      return tip.killTask(taskid, shouldFail);
    }
    else {
      LOG.info("Kill task attempt failed since task " + taskid + " was not found");
      return false;
    }
  }

  @Deprecated
  public String getAssignedTracker(String taskid) {
    return getAssignedTracker(TaskAttemptID.forName(taskid));
  }

  /**
   * Get tracker name for a given task id.
   * @param taskId the name of the task
   * @return The name of the task tracker
   */
  public synchronized String getAssignedTracker(TaskAttemptID taskId) {
    return taskidToTrackerMap.get(taskId);
  }
    
  public JobStatus[] jobsToComplete() {
    Vector<JobStatus> v = new Vector<JobStatus>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.RUNNING 
          || status.getRunState() == JobStatus.PREP) {
        status.setStartTime(jip.getStartTime());
        status.setUsername(jip.getProfile().getUser());
        v.add(status);
      }
    }
    return v.toArray(new JobStatus[v.size()]);
  } 
  
  public JobStatus[] getAllJobs() {
    Vector<JobStatus> v = new Vector<JobStatus>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      status.setStartTime(jip.getStartTime());
      status.setUsername(jip.getProfile().getUser());
      v.add(status);
    }
    return v.toArray(new JobStatus[v.size()]);
  }
    
  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }
  
  ///////////////////////////////////////////////////////////////
  // JobTracker methods
  ///////////////////////////////////////////////////////////////
  @Deprecated
  public JobInProgress getJob(String jobid) {
    return getJob(JobID.forName(jobid));
  }

  public JobInProgress getJob(JobID jobid) {
    return jobs.get(jobid);
  }

  /**
   * Change the run-time priority of the given job.
   * @param jobId job id
   * @param priority new {@link JobPriority} for the job
   */
  synchronized void setJobPriority(JobID jobId, JobPriority priority) {
    JobInProgress job = jobs.get(jobId);
    if (job != null) {
      job.setPriority(priority);
      
      // Re-sort jobs to reflect this change
      resortPriority();
    } else {
      LOG.warn("Trying to change the priority of an unknown job: " + jobId);
    }
  }
  
  ////////////////////////////////////////////////////
  // Methods to track all the TaskTrackers
  ////////////////////////////////////////////////////
  /**
   * Accept and process a new TaskTracker profile.  We might
   * have known about the TaskTracker previously, or it might
   * be brand-new.  All task-tracker structures have already
   * been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void updateTaskStatuses(TaskTrackerStatus status) {
    String trackerName = status.getTrackerName();
    for (TaskStatus report : status.getTaskReports()) {
      report.setTaskTracker(trackerName);
      TaskAttemptID taskId = report.getTaskID();
      TaskInProgress tip = taskidToTIPMap.get(taskId);
      if (tip == null) {
        LOG.info("Serious problem.  While updating status, cannot find taskid " + report.getTaskID());
      } else {
        expireLaunchingTasks.removeTask(taskId);
        tip.getJob().updateTaskStatus(tip, report, myMetrics);
      }
      
      // Process 'failed fetch' notifications 
      List<TaskAttemptID> failedFetchMaps = report.getFetchFailedMaps();
      if (failedFetchMaps != null) {
        for (TaskAttemptID mapTaskId : failedFetchMaps) {
          TaskInProgress failedFetchMap = taskidToTIPMap.get(mapTaskId);
          
          if (failedFetchMap != null) {
            // Gather information about the map which has to be failed, if need be
            String failedFetchTrackerName = getAssignedTracker(mapTaskId);
            if (failedFetchTrackerName == null) {
              failedFetchTrackerName = "Lost task tracker";
            }
            failedFetchMap.getJob().fetchFailureNotification(failedFetchMap, 
                                                             mapTaskId, 
                                                             failedFetchTrackerName, 
                                                             myMetrics);
          }
        }
      }
    }
  }

  /**
   * We lost the task tracker!  All task-tracker structures have 
   * already been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void lostTaskTracker(String trackerName) {
    LOG.info("Lost tracker '" + trackerName + "'");
    Set<TaskAttemptID> lostTasks = trackerToTaskMap.get(trackerName);
    trackerToTaskMap.remove(trackerName);

    if (lostTasks != null) {
      // List of jobs which had any of their tasks fail on this tracker
      Set<JobInProgress> jobsWithFailures = new HashSet<JobInProgress>(); 
      for (TaskAttemptID taskId : lostTasks) {
        TaskInProgress tip = taskidToTIPMap.get(taskId);
        JobInProgress job = tip.getJob();

        // Completed reduce tasks never need to be failed, because 
        // their outputs go to dfs
        // And completed maps with zero reducers of the job 
        // never need to be failed. 
        if (!tip.isComplete() || 
            (tip.isMapTask() && job.desiredReduces() != 0)) {
          // if the job is done, we don't want to change anything
          if (job.getStatus().getRunState() == JobStatus.RUNNING) {
            job.failedTask(tip, taskId, ("Lost task tracker: " + trackerName), 
                           (tip.isMapTask() ? 
                               TaskStatus.Phase.MAP : 
                               TaskStatus.Phase.REDUCE), 
                           TaskStatus.State.KILLED, trackerName, myMetrics);
            jobsWithFailures.add(job);
          }
        } else {
          // Completed 'reduce' task and completed 'maps' with zero 
          // reducers of the job, not failed;
          // only removed from data-structures.
          markCompletedTaskAttempt(trackerName, taskId);
        }
      }
      
      // Penalize this tracker for each of the jobs which   
      // had any tasks running on it when it was 'lost' 
      for (JobInProgress job : jobsWithFailures) {
        job.addTrackerTaskFailure(trackerName);
      }
      
      // Purge 'marked' tasks, needs to be done  
      // here to prevent hanging references!
      removeMarkedTasks(trackerName);
    }
  }

  /**
   * Add a job's completed task (either successful or failed/killed) to the 
   * {@link TaskCommitQueue}. 
   * @param j completed task (either successful or failed/killed)
   */
  void addToCommitQueue(JobInProgress.JobWithTaskContext j) {
    ((TaskCommitQueue)taskCommitThread).addToQueue(j);
  }
  
  /**
   * A thread which does all of the {@link FileSystem}-related operations for
   * tasks. It picks the next task in the queue, promotes outputs of 
   * {@link TaskStatus.State#SUCCEEDED} tasks & discards outputs for 
   * {@link TaskStatus.State#FAILED} or {@link TaskStatus.State#KILLED} tasks.
   */
  private class TaskCommitQueue extends Thread {
    
    private LinkedBlockingQueue<JobInProgress.JobWithTaskContext> queue = 
            new LinkedBlockingQueue <JobInProgress.JobWithTaskContext>();
        
    public TaskCommitQueue() {
      setName("Task Commit Thread");
      setDaemon(true);
    }
    
    public void addToQueue(JobInProgress.JobWithTaskContext j) {
      while (true) { // loop until the element gets added
        try {
          queue.put(j);
          return;
        } catch (InterruptedException ie) {}
      }
    }
       
    @Override
    public void run() {
      int  batchCommitSize = conf.getInt("jobtracker.task.commit.batch.size", 
                                         5000); 
      while (!isInterrupted()) {
        try {
          ArrayList <JobInProgress.JobWithTaskContext> jobList = 
            new ArrayList<JobInProgress.JobWithTaskContext>(batchCommitSize);
          // Block if the queue is empty
          jobList.add(queue.take());  
          queue.drainTo(jobList, batchCommitSize);

          JobInProgress[] jobs = new JobInProgress[jobList.size()];
          TaskInProgress[] tips = new TaskInProgress[jobList.size()];
          TaskAttemptID[] taskids = new TaskAttemptID[jobList.size()];
          JobTrackerMetrics[] metrics = new JobTrackerMetrics[jobList.size()];

          Iterator<JobInProgress.JobWithTaskContext> iter = jobList.iterator();
          int count = 0;

          while (iter.hasNext()) {
            JobInProgress.JobWithTaskContext j = iter.next();
            jobs[count] = j.getJob();
            tips[count] = j.getTIP();
            taskids[count]= j.getTaskID();
            metrics[count] = j.getJobTrackerMetrics();
            ++count;
          }

          Task[] tasks = new Task[jobList.size()];
          TaskStatus[] status = new TaskStatus[jobList.size()];
          boolean[] isTipComplete = new boolean[jobList.size()];
          TaskStatus.State[] states = new TaskStatus.State[jobList.size()];

          synchronized (JobTracker.this) {
            for(int i = 0; i < jobList.size(); ++i) {
              synchronized (jobs[i]) {
                synchronized (tips[i]) {
                  status[i] = tips[i].getTaskStatus(taskids[i]);
                  tasks[i] = tips[i].getTask(taskids[i]);
                  states[i] = status[i].getRunState();
                  isTipComplete[i] = tips[i].isComplete();
                }
              }
            }
          }

          //For COMMIT_PENDING tasks, we save the task output in the dfs
          //as well as manipulate the JT datastructures to reflect a
          //successful task. This guarantees that we don't declare a task
          //as having succeeded until we have successfully completed the
          //dfs operations.
          //For failed tasks, we just do the dfs operations here. The
          //datastructures updates is done earlier as soon as the failure
          //is detected so that the JT can immediately schedule another
          //attempt for that task.

          Set<TaskID> seenTIPs = new HashSet<TaskID>();
          for(int index = 0; index < jobList.size(); ++index) {
            try {
              if (states[index] == TaskStatus.State.COMMIT_PENDING) {
                if (!isTipComplete[index]) {
                  if (!seenTIPs.contains(tips[index].getTIPId())) {
                    tasks[index].saveTaskOutput();
                    seenTIPs.add(tips[index].getTIPId());
                  } else {
                    // since other task of this tip has saved its output
                    isTipComplete[index] = true;
                  }
                }
              } else if (states[index] == TaskStatus.State.FAILED || 
                         states[index] == TaskStatus.State.KILLED) {
                try {
                  tasks[index].removeTaskOutput();
                } catch (IOException e) {
                  LOG.info("Failed to remove temporary directory of "
                           + status[index].getTaskID() + " with " 
                           + StringUtils.stringifyException(e));
                }
              }
            } catch (IOException ioe) {
              // Oops! Failed to copy the task's output to its final place;
              // fail the task!
              states[index] = TaskStatus.State.FAILED;
              synchronized (JobTracker.this) {
                String reason = "Failed to rename output with the exception: " 
                                + StringUtils.stringifyException(ioe);
                TaskStatus.Phase phase = (tips[index].isMapTask() 
                                          ? TaskStatus.Phase.MAP 
                                          : TaskStatus.Phase.REDUCE);
                jobs[index].failedTask(tips[index], status[index].getTaskID(), 
                                       reason, phase, TaskStatus.State.FAILED, 
                                       status[index].getTaskTracker(), null);
              }
              LOG.info("Failed to rename the output of " 
                       + status[index].getTaskID() + " with " 
                       + StringUtils.stringifyException(ioe));
            }
          }

          synchronized (JobTracker.this) {
            //do a check for the case where after the task went to
            //COMMIT_PENDING, it was lost. So although we would have
            //saved the task output, we cannot declare it a SUCCESS.
            for(int i = 0; i < jobList.size(); ++i) {
              TaskStatus newStatus = null;
              if(states[i] == TaskStatus.State.COMMIT_PENDING) {
                synchronized (jobs[i]) {
                  synchronized (tips[i]) {
                    status[i] = tips[i].getTaskStatus(taskids[i]);
                    if (!isTipComplete[i]) {
                      if (status[i].getRunState() 
                          != TaskStatus.State.COMMIT_PENDING) {
                        states[i] = TaskStatus.State.KILLED;
                      } else {
                        states[i] = TaskStatus.State.SUCCEEDED;
                      }
                    } else {
                      tips[i].addDiagnosticInfo(tasks[i].getTaskID(), 
                                                "Already completed  TIP");
                      states[i] = TaskStatus.State.KILLED;
                    }
                    //create new status if required. If the state changed 
                    //from COMMIT_PENDING to KILLED in the JobTracker, while 
                    //we were saving the output,the JT would have called 
                    //updateTaskStatus and we don't need to call it again
                    newStatus = (TaskStatus)status[i].clone();
                    newStatus.setRunState(states[i]);
                    newStatus.setProgress(
                        (states[i] == TaskStatus.State.SUCCEEDED) 
                        ? 1.0f 
                        : 0.0f);
                  }
                  if (newStatus != null) {
                    jobs[i].updateTaskStatus(tips[i], newStatus, metrics[i]);
                  }
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          break;
        }
        catch (Throwable t) {
          LOG.error(getName() + " got an exception: " +
                    StringUtils.stringifyException(t));
        }
      }
      
      LOG.warn(getName() + " exiting..."); 
    }
  }
  

  @Deprecated
  public String getLocalJobFilePath(String jobid) {
    return getLocalJobFilePath(JobID.forName(jobid));
  }

  /**
   * Get the localized job file path on the job trackers local file system
   * @param jobId id of the job
   * @return the path of the job conf file on the local file system
   */
  public static String getLocalJobFilePath(JobID jobId){
    return JobHistory.JobInfo.getLocalJobFilePath(jobId);
  }
  ////////////////////////////////////////////////////////////
  // main()
  ////////////////////////////////////////////////////////////

  /**
   * Start the JobTracker process.  This is used only for debugging.  As a rule,
   * JobTracker should be run as part of the DFS Namenode process.
   */
  public static void main(String argv[]
                          ) throws IOException, InterruptedException {
    StringUtils.startupShutdownMessage(JobTracker.class, argv, LOG);
    if (argv.length != 0) {
      System.out.println("usage: JobTracker");
      System.exit(-1);
    }
      
    try {
      JobTracker tracker = startTracker(new JobConf());
      tracker.offerService();
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

}
