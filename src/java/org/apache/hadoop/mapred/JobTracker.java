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
import java.net.InetSocketAddress;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;

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

  /**
   * The maximum no. of 'completed' (successful/failed/killed)
   * jobs kept in memory per-user. 
   */
  static final int MAX_COMPLETE_USER_JOBS_IN_MEMORY = 100;
    
  /**
   * Used for formatting the id numbers
   */
  private static NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setMinimumIntegerDigits(4);
    idFormat.setGroupingUsed(false);
  }

  private int nextJobId = 1;

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.JobTracker");

  private static JobTracker tracker = null;
  private static boolean runTracker = true;
    
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
  public static void startTracker(JobConf conf) throws IOException {
    if (tracker != null)
      throw new IOException("JobTracker already running.");
    runTracker = true;
    while (runTracker) {
      try {
        tracker = new JobTracker(conf);
        break;
      } catch (VersionMismatch v) {
        // Can't recover from a version mismatch. Avoid the retry loop and re-throw
        throw v;
      } catch (IOException e) {
        LOG.warn("Error starting tracker: " + 
                 StringUtils.stringifyException(e));
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    if (runTracker) {
      JobEndNotifier.startNotifier();
      tracker.offerService();
    }
  }

  public static JobTracker getTracker() {
    return tracker;
  }

  public static void stopTracker() throws IOException {
    runTracker = false;
    if (tracker != null) {
      JobEndNotifier.stopNotifier();
      tracker.close();
      tracker = null;
    }
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
    private volatile boolean shouldRun = true;
    /**
     * This is a map of the tasks that have been assigned to task trackers,
     * but that have not yet been seen in a status report.
     * map: task-id (String) -> time-assigned (Long)
     */
    private Map<String, Long> launchingTasks =
      new LinkedHashMap<String, Long>();
      
    public void run() {
      while (shouldRun) {
        try {
          // Every 3 minutes check for any tasks that are overdue
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL/3);
          long now = System.currentTimeMillis();
          LOG.debug("Starting launching task sweep");
          synchronized (JobTracker.this) {
            synchronized (launchingTasks) {
              Iterator<Map.Entry<String, Long>> itr =
                launchingTasks.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<String, Long> pair = itr.next();
                String taskId = pair.getKey();
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
                                     trackerStatus.getHost(), trackerName,
                                     myMetrics);
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
          return;
        } catch (Exception e) {
          LOG.error("Expire Launching Task Thread got exception: " +
                    StringUtils.stringifyException(e));
        }
      }
    }
      
    public void addNewTask(String taskName) {
      synchronized (launchingTasks) {
        launchingTasks.put(taskName, 
                           System.currentTimeMillis());
      }
    }
      
    public void removeTask(String taskName) {
      synchronized (launchingTasks) {
        launchingTasks.remove(taskName);
      }
    }
      
    public void stop() {
      shouldRun = false;
    }
  }
    
  ///////////////////////////////////////////////////////
  // Used to expire TaskTrackers that have gone down
  ///////////////////////////////////////////////////////
  class ExpireTrackers implements Runnable {
    boolean shouldRun = true;
    public ExpireTrackers() {
    }
    /**
     * The run method lives for the life of the JobTracker, and removes TaskTrackers
     * that have not checked in for some time.
     */
    public void run() {
      while (shouldRun) {
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
                      // Remove completely
                      updateTaskTrackerStatus(trackerName, null);
                      lostTaskTracker(leastRecent.getTrackerName(),
                                      leastRecent.getHost());
                    } else {
                      // Update time by inserting latest profile
                      trackerExpiryQueue.add(newProfile);
                    }
                  }
                }
              }
            }
          }
        } catch (Exception t) {
          LOG.error("Tracker Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }
        
    /**
     * Stop the tracker on next iteration
     */
    public void stopTracker() {
      shouldRun = false;
    }
  }

  ///////////////////////////////////////////////////////
  // Used to remove old finished Jobs that have been around for too long
  ///////////////////////////////////////////////////////
  class RetireJobs implements Runnable {
    boolean shouldRun = true;
    public RetireJobs() {
    }

    /**
     * The run method lives for the life of the JobTracker,
     * and removes Jobs that are not still running, but which
     * finished a long time ago.
     */
    public void run() {
      while (shouldRun) {
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
                      jobs.remove(job.getProfile().getJobId());
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
                               job.getProfile().getJobId() + "' of user '" +
                               jobUser + "'");
                    }
                  }
                }
              }
            }
          }
        } catch (InterruptedException t) {
          shouldRun = false;
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
    boolean shouldRun = true;
    public JobInitThread() {
    }
    public void run() {
      JobInProgress job;
      while (shouldRun) {
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
          shouldRun = false;
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
      
    JobTrackerMetrics(JobConf conf) {
      String sessionId = conf.getSessionId();
      // Initiate JVM Metrics
      JvmMetrics.init("JobTracker", sessionId);
      // Create a record for map-reduce metrics
      MetricsContext context = MetricsUtil.getContext("mapred");
      metricsRecord = MetricsUtil.createRecord(context, "jobtracker");
      metricsRecord.setTag("sessionId", sessionId);
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

  private int maxCurrentTasks;
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
  Map<String, JobInProgress> jobs = new TreeMap<String, JobInProgress>();
  List<JobInProgress> jobsByPriority = new ArrayList<JobInProgress>();

  // (user -> list of JobInProgress)
  TreeMap<String, ArrayList<JobInProgress>> userToJobsMap =
    new TreeMap<String, ArrayList<JobInProgress>>();
    
  // All the known TaskInProgress items, mapped to by taskids (taskid->TIP)
  Map<String, TaskInProgress> taskidToTIPMap =
    new TreeMap<String, TaskInProgress>();

  // (taskid --> trackerID) 
  TreeMap<String, String> taskidToTrackerMap = new TreeMap<String, String>();

  // (trackerID->TreeSet of taskids running at that tracker)
  TreeMap<String, Set<String>> trackerToTaskMap =
    new TreeMap<String, Set<String>>();

  // (trackerID -> TreeSet of completed taskids running at that tracker)
  TreeMap<String, Set<String>> trackerToMarkedTasksMap =
    new TreeMap<String, Set<String>>();

  // (trackerID --> last sent HeartBeatResponse)
  Map<String, HeartbeatResponse> trackerToHeartbeatResponseMap = 
    new TreeMap<String, HeartbeatResponse>();
    
  //
  // Watch and expire TaskTracker objects using these structures.
  // We can map from Name->TaskTrackerStatus, or we can expire by time.
  //
  int totalMaps = 0;
  int totalReduces = 0;
  private TreeMap<String, TaskTrackerStatus> taskTrackers =
    new TreeMap<String, TaskTrackerStatus>();
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
  String infoBindAddress;
  int infoPort;

  Server interTrackerServer;

  // Some jobs are stored in a local system directory.  We can delete
  // the files when we're done with the job.
  static final String SUBDIR = "jobTracker";
  FileSystem fs;
  Path systemDir;
  private JobConf conf;

  /**
   * Start the JobTracker process, listen on the indicated port
   */
  JobTracker(JobConf conf) throws IOException {
    //
    // Grab some static constants
    //
    TASKTRACKER_EXPIRY_INTERVAL = 
      conf.getLong("mapred.tasktracker.expiry.interval", 10 * 60 * 1000);
    maxCurrentTasks = conf.getInt("mapred.tasktracker.tasks.maximum", 2);
    RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
    RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);
    TASK_ALLOC_EPSILON = conf.getFloat("mapred.jobtracker.taskalloc.loadbalance.epsilon", 0.2f);
    PAD_FRACTION = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                 0.01f);

    // This is a directory of temporary submission files.  We delete it
    // on startup, and can delete any files that we're done with
    this.conf = conf;
    JobConf jobConf = new JobConf(conf);
    this.systemDir = jobConf.getSystemDir();
    this.fs = FileSystem.get(conf);
    fs.delete(systemDir);
    if (!fs.mkdirs(systemDir)) {
      throw new IOException("Mkdirs failed to create " + systemDir.toString());
    }

    // Same with 'localDir' except it's always on the local disk.
    jobConf.deleteLocalFiles(SUBDIR);

    // Read the hosts/exclude files to restrict access to the jobtracker.
    this.hostsReader = new HostsFileReader(conf.get("mapred.hosts", ""),
                                           conf.get("mapred.hosts.exclude", ""));
                                           
    // Set ports, start RPC servers, etc.
    InetSocketAddress addr = getAddress(conf);
    this.localMachine = addr.getHostName();
    this.port = addr.getPort();
    this.interTrackerServer = RPC.getServer(this, addr.getHostName(), addr.getPort(), 10, false, conf);
    this.interTrackerServer.start();
    Properties p = System.getProperties();
    for (Iterator it = p.keySet().iterator(); it.hasNext();) {
      String key = (String) it.next();
      String val = (String) p.getProperty(key);
      LOG.info("Property '" + key + "' is " + val);
    }

    this.infoPort = conf.getInt("mapred.job.tracker.info.port", 50030);
    this.infoBindAddress = conf.get("mapred.job.tracker.info.bindAddress","0.0.0.0");
    this.infoServer = new StatusHttpServer("job", infoBindAddress, infoPort, false);
    this.infoServer.start();

    this.startTime = System.currentTimeMillis();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
    trackerIdentifier = dateFormat.format(new Date());

    myMetrics = new JobTrackerMetrics(jobConf);
    
    this.expireTrackersThread = new Thread(this.expireTrackers,
                                           "expireTrackers");
    this.expireTrackersThread.start();
    this.retireJobsThread = new Thread(this.retireJobs, "retireJobs");
    this.retireJobsThread.start();
    this.initJobsThread = new Thread(this.initJobs, "initJobs");
    this.initJobsThread.start();
    expireLaunchingTaskThread.start();
        
    // The rpc/web-server ports can be ephemeral ports... 
    // ... ensure we have the correct info
    this.port = interTrackerServer.getListenerAddress().getPort();
    this.conf.set("mapred.job.tracker", (this.localMachine + ":" + this.port));
    LOG.info("JobTracker up at: " + this.port);
    this.infoPort = this.infoServer.getPort();
    this.conf.setInt("mapred.job.tracker.info.port", this.infoPort); 
    LOG.info("JobTracker webserver: " + this.infoServer.getPort());
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String jobTrackerStr =
      conf.get("mapred.job.tracker", "localhost:8012");
    int colon = jobTrackerStr.indexOf(":");
    if (colon < 0) {
      throw new RuntimeException("Bad mapred.job.tracker: "+jobTrackerStr);
    }
    String jobTrackerName = jobTrackerStr.substring(0, colon);
    int jobTrackerPort = Integer.parseInt(jobTrackerStr.substring(colon+1));
    return new InetSocketAddress(jobTrackerName, jobTrackerPort);
  }


  /**
   * Run forever
   */
  public void offerService() {
    try {
      this.interTrackerServer.join();
    } catch (InterruptedException ie) {
    }
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
    if (this.expireTrackers != null) {
      LOG.info("Stopping expireTrackers");
      this.expireTrackers.stopTracker();
      try {
        this.expireTrackersThread.interrupt();
        this.expireTrackersThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.retireJobs != null) {
      LOG.info("Stopping retirer");
      this.retireJobsThread.interrupt();
      try {
        this.retireJobsThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.initJobs != null) {
      LOG.info("Stopping initer");
      this.initJobsThread.interrupt();
      try {
        this.initJobsThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.expireLaunchingTaskThread != null) {
      LOG.info("Stopping expireLaunchingTasks");
      this.expireLaunchingTasks.stop();
      try {
        this.expireLaunchingTaskThread.interrupt();
        this.expireLaunchingTaskThread.join();
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
  void createTaskEntry(String taskid, String taskTracker, TaskInProgress tip) {
    LOG.info("Adding task '" + taskid + "' to tip " + tip.getTIPId() + ", for tracker '" + taskTracker + "'");

    // taskid --> tracker
    taskidToTrackerMap.put(taskid, taskTracker);

    // tracker --> taskid
    Set<String> taskset = trackerToTaskMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<String>();
      trackerToTaskMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);

    // taskid --> TIP
    taskidToTIPMap.put(taskid, tip);
  }
    
  void removeTaskEntry(String taskid) {
    // taskid --> tracker
    String tracker = taskidToTrackerMap.remove(taskid);

    // tracker --> taskid
    if (tracker != null) {
      Set<String> trackerSet = trackerToTaskMap.get(tracker);
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
  void markCompletedTaskAttempt(String taskTracker, String taskid) {
    // tracker --> taskid
    Set<String> taskset = trackerToMarkedTasksMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<String>();
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
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskId());
        }
      }
    }
    for (TaskInProgress tip : job.getReduceTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskId());
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
    Set<String> markedTaskSet = 
      trackerToMarkedTasksMap.get(taskTracker);
    if (markedTaskSet != null) {
      for (String taskid : markedTaskSet) {
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
        removeTaskEntry(taskStatus.getTaskId());
      }
    }
    for (TaskInProgress tip : job.getReduceTasks()) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        removeTaskEntry(taskStatus.getTaskId());
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
                  jobs.remove(rjob.getProfile().getJobId());
                  jobInitQueue.remove(rjob);
                  jobsByPriority.remove(rjob);
                    
                  LOG.info("Retired job with id: '" + 
                           rjob.getProfile().getJobId() + "' of user: '" +
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
      return (List<JobInProgress>) runningJobs();
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

  ////////////////////////////////////////////////////
  // InterTrackerProtocol
  ////////////////////////////////////////////////////

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
                
      // It is completely safe to ignore a 'duplicate' from a tracker
      // since we are guaranteed that the tracker sends the same 
      // 'heartbeat' when rpcs are lost. 
      // {@see TaskTracker.transmitHeartbeat()}
      if (prevHeartbeatResponse.getResponseId() != responseId) {
        LOG.info("Ignoring 'duplicate' heartbeat from '" + 
                 trackerName + "'");
        return prevHeartbeatResponse;
      }
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
        LOG.debug(trackerName + " -> LaunchTask: " + task.getTaskId());
        actions.add(new LaunchTaskAction(task));
      }
    }
      
    // Check for tasks to be killed
    List<TaskTrackerAction> killTasksList = getTasksToKill(trackerName);
    if (killTasksList != null) {
      actions.addAll(killTasksList);
    }
     
    response.setActions(
                        actions.toArray(new TaskTrackerAction[actions.size()]));
        
    // Update the trackerToHeartbeatResponseMap
    trackerToHeartbeatResponseMap.put(trackerName, response);

    // Done processing the hearbeat, now remove 'marked' tasks
    removeMarkedTasks(trackerName);
        
    return response;
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
      if (status == null) {
        taskTrackers.remove(trackerName);
      }
    }
    if (status != null) {
      totalMaps += status.countMapTasks();
      totalReduces += status.countReduceTasks();
      taskTrackers.put(trackerName, status);
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
            lostTaskTracker(trackerName, trackerStatus.getHost());
          }
        } else {
          // If not first contact, there should be some record of the tracker
          if (!seenBefore) {
            LOG.warn("Status from unknown Tracker : " + trackerName);
            taskTrackers.remove(trackerName); 
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
    int totalCapacity = numTaskTrackers * maxCurrentTasks;

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

    // find out the maximum number of maps or reduces that we are willing
    // to run on any node.
    int maxMapLoad = 0;
    int maxReduceLoad = 0;
    if (numTaskTrackers > 0) {
      maxMapLoad = Math.min(maxCurrentTasks,
                            (int) Math.ceil((double) remainingMapLoad / 
                                            numTaskTrackers));
      maxReduceLoad = Math.min(maxCurrentTasks,
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

          Task t = job.obtainNewMapTask(tts, numTaskTrackers);
          if (t != null) {
            expireLaunchingTasks.addNewTask(t.getTaskId());
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
            padding = Math.min(maxCurrentTasks,
                               (int)(totalNeededMaps * PAD_FRACTION));
          }
          if (totalMaps + padding >= totalCapacity) {
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

          Task t = job.obtainNewReduceTask(tts, numTaskTrackers);
          if (t != null) {
            expireLaunchingTasks.addNewTask(t.getTaskId());
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
              Math.min(maxCurrentTasks,
                       (int) (totalNeededReduces * PAD_FRACTION));
          }
          if (totalReduces + padding >= totalCapacity) {
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
    
    Set<String> taskIds = trackerToTaskMap.get(taskTracker);
    if (taskIds != null) {
      List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
      Set<String> killJobIds = new TreeSet<String>(); 
      for (String killTaskId : taskIds) {
        TaskInProgress tip = taskidToTIPMap.get(killTaskId);
        if (tip.shouldCloseForClosedJob(killTaskId)) {
          // 
          // This is how the JobTracker ends a task at the TaskTracker.
          // It may be successfully completed, or may be killed in
          // mid-execution.
          //
          if (tip.getJob().getStatus().getRunState() == JobStatus.RUNNING) {
            killList.add(new KillTaskAction(killTaskId));
            LOG.debug(taskTracker + " -> KillTaskAction: " + killTaskId);
          } else {
            String killJobId = tip.getJob().getStatus().getJobId(); 
            killJobIds.add(killJobId);
          }
        }
      }
            
      for (String killJobId : killJobIds) {
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
    return fs.getName();
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
   * Allocates a new JobId string.
   */
  public String getNewJobId() {
    synchronized (this) {
      return "job_" + getTrackerIdentifier() + "_" + 
             idFormat.format(nextJobId++);
    }
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
  public synchronized JobStatus submitJob(String jobFile) throws IOException {
    totalSubmissions++;
    JobInProgress job = new JobInProgress(jobFile, this, this.conf);
    synchronized (jobs) {
      synchronized (jobsByPriority) {
        synchronized (jobInitQueue) {
          jobs.put(job.getProfile().getJobId(), job);
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
  public void resortPriority() {
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
                               maxCurrentTasks);          
    }
  }
    
  public synchronized void killJob(String jobid) {
    JobInProgress job = jobs.get(jobid);
    job.kill();
  }

  public synchronized JobProfile getJobProfile(String jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getProfile();
    } else {
      return null;
    }
  }
  public synchronized JobStatus getJobStatus(String jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getStatus();
    } else {
      return null;
    }
  }
  public synchronized Counters getJobCounters(String jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      return job.getCounters();
    } else {
      return null;
    }
  }
  public synchronized TaskReport[] getMapTaskReports(String jobid) {
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

  public synchronized TaskReport[] getReduceTaskReports(String jobid) {
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
    
  /* 
   * Returns a list of TaskCompletionEvent for the given job, 
   * starting from fromEventId.
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getTaskCompletionEvents(java.lang.String, int, int)
   */
  public synchronized TaskCompletionEvent[] getTaskCompletionEvents(
                                                                    String jobid, int fromEventId, int maxEvents) throws IOException{
    TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
    JobInProgress job = this.jobs.get(jobid);
    if (null != job) {
      events = job.getTaskCompletionEvents(fromEventId, maxEvents);
    }
    return events;
  }

  /**
   * Get the diagnostics for a given task
   * @param jobId the id of the job
   * @param tipId the id of the tip 
   * @param taskId the id of the task
   * @return a list of the diagnostic messages
   */
  public synchronized List<String> getTaskDiagnostics(String jobId,
                                                      String tipId,
                                                      String taskId) {
    JobInProgress job = jobs.get(jobId);
    if (job == null) {
      throw new IllegalArgumentException("Job " + jobId + " not found.");
    }
    TaskInProgress tip = job.getTaskInProgress(tipId);
    if (tip == null) {
      throw new IllegalArgumentException("TIP " + tipId + " not found.");
    }
    return tip.getDiagnosticInfo(taskId);
  }
    
  /** Get all the TaskStatuses from the tipid. */
  TaskStatus[] getTaskStatuses(String jobid, String tipid) {
    TaskInProgress tip = getTip(jobid, tipid);
    return (tip == null ? new TaskStatus[0] 
            : tip.getTaskStatuses());
  }

  /** Returns the TaskStatus for a particular taskid. */
  TaskStatus getTaskStatus(String jobid, String tipid, String taskid) {
    TaskInProgress tip = getTip(jobid, tipid);
    return (tip == null ? null 
            : tip.getTaskStatus(taskid));
  }
    
  /**
   * Returns the counters for the specified task in progress.
   */
  Counters getTipCounters(String jobid, String tipid) {
    TaskInProgress tip = getTip(jobid, tipid);
    return (tip == null ? null : tip.getCounters());
  }
    
  /**
   * Returns specified TaskInProgress, or null.
   */
  private TaskInProgress getTip(String jobid, String tipid) {
    JobInProgress job = jobs.get(jobid);
    return (job == null ? null 
            : (TaskInProgress) job.getTaskInProgress(tipid));
  }
    
  /**
   * Get tracker name for a given task id.
   * @param taskId the name of the task
   * @return The name of the task tracker
   */
  public synchronized String getAssignedTracker(String taskId) {
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
    
  ///////////////////////////////////////////////////////////////
  // JobTracker methods
  ///////////////////////////////////////////////////////////////
  public JobInProgress getJob(String jobid) {
    return jobs.get(jobid);
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
    for (TaskStatus report : status.getTaskReports()) {
      report.setTaskTracker(status.getTrackerName());
      String taskId = report.getTaskId();
      TaskInProgress tip = taskidToTIPMap.get(taskId);
      if (tip == null) {
        LOG.info("Serious problem.  While updating status, cannot find taskid " + report.getTaskId());
      } else {
        expireLaunchingTasks.removeTask(taskId);
        tip.getJob().updateTaskStatus(tip, report, myMetrics);
      }
    }
  }

  /**
   * We lost the task tracker!  All task-tracker structures have 
   * already been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void lostTaskTracker(String trackerName, String hostname) {
    LOG.info("Lost tracker '" + trackerName + "'");
    Set<String> lostTasks = trackerToTaskMap.get(trackerName);
    trackerToTaskMap.remove(trackerName);

    if (lostTasks != null) {
      // List of jobs which had any of their tasks fail on this tracker
      Set<JobInProgress> jobsWithFailures = new HashSet<JobInProgress>(); 
      for (String taskId : lostTasks) {
        TaskInProgress tip = taskidToTIPMap.get(taskId);

        // Completed reduce tasks never need to be failed, because 
        // their outputs go to dfs
        if (tip.isMapTask() || !tip.isComplete()) {
          JobInProgress job = tip.getJob();
          // if the job is done, we don't want to change anything
          if (job.getStatus().getRunState() == JobStatus.RUNNING) {
            job.failedTask(tip, taskId, "Lost task tracker", 
                           (tip.isMapTask() ? 
                               TaskStatus.Phase.MAP : 
                               TaskStatus.Phase.REDUCE), 
                           TaskStatus.State.KILLED,
                           hostname, trackerName, myMetrics);
            jobsWithFailures.add(job);
          }
        } else if (!tip.isMapTask() && tip.isComplete()) {
          // Completed 'reduce' task, not failed;
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

  ////////////////////////////////////////////////////////////
  // main()
  ////////////////////////////////////////////////////////////

  /**
   * Start the JobTracker process.  This is used only for debugging.  As a rule,
   * JobTracker should be run as part of the DFS Namenode process.
   */
  public static void main(String argv[]) throws IOException, InterruptedException {
    StringUtils.startupShutdownMessage(JobTracker.class, argv, LOG);
    if (argv.length != 0) {
      System.out.println("usage: JobTracker");
      System.exit(-1);
    }
      
    try {
      startTracker(new JobConf());
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
