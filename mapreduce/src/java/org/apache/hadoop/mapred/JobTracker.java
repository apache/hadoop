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
import java.io.PrintWriter;
import java.io.Writer;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.mapred.AuditLogger.Constants;
import org.apache.hadoop.mapred.ClusterStatus.BlackListInfo;
import org.apache.hadoop.mapred.JobInProgress.KillInterruptedException;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapred.JobTrackerStatistics.TaskTrackerStat;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.DelegationTokenRenewal;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.State;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 *******************************************************/
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobTracker implements MRConstants, InterTrackerProtocol,
    ClientProtocol, TaskTrackerManager, RefreshUserMappingsProtocol,
    RefreshAuthorizationPolicyProtocol, AdminOperationsProtocol, JTConfig {

  static{
    ConfigUtil.loadResources();
  }

  private final long tasktrackerExpiryInterval;
  private final long DELEGATION_TOKEN_GC_INTERVAL = 3600000; // 1 hour
  private final DelegationTokenSecretManager secretManager;
  
  // The interval after which one fault of a tracker will be discarded,
  // if there are no faults during this. 
  private static long UPDATE_FAULTY_TRACKER_INTERVAL = 24 * 60 * 60 * 1000;
  // The maximum percentage of trackers in cluster added 
  // to the 'blacklist' across all the jobs.
  private static double MAX_BLACKLIST_PERCENT = 0.50;
  // A tracker is blacklisted across jobs only if number of 
  // blacklists are X% above the average number of blacklists.
  // X is the blacklist threshold here.
  private double AVERAGE_BLACKLIST_THRESHOLD = 0.50;
  // The maximum number of blacklists for a tracker after which the 
  // tracker could be blacklisted across all jobs
  private int MAX_BLACKLISTS_PER_TRACKER = 4;
  
  // Approximate number of heartbeats that could arrive JobTracker
  // in a second
  private int NUM_HEARTBEATS_IN_SECOND;
  private final int DEFAULT_NUM_HEARTBEATS_IN_SECOND = 100;
  private final int MIN_NUM_HEARTBEATS_IN_SECOND = 1;
  
  // Scaling factor for heartbeats, used for testing only
  private float HEARTBEATS_SCALING_FACTOR;
  private final float MIN_HEARTBEATS_SCALING_FACTOR = 0.01f;
  private final float DEFAULT_HEARTBEATS_SCALING_FACTOR = 1.0f;
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static enum State { INITIALIZING, RUNNING }
  State state = State.INITIALIZING;
  private static final int FS_ACCESS_RETRY_PERIOD = 10000;
  
  static final String JOB_INFO_FILE = "job-info";

  private DNSToSwitchMapping dnsToSwitchMapping;
  NetworkTopology clusterMap = new NetworkTopology();
  private int numTaskCacheLevels; // the max level to which we cache tasks
  /**
   * {@link #nodesAtMaxLevel} is using the keySet from {@link ConcurrentHashMap}
   * so that it can be safely written to and iterated on via 2 separate threads.
   * Note: It can only be iterated from a single thread which is feasible since
   *       the only iteration is done in {@link JobInProgress} under the 
   *       {@link JobTracker} lock.
   */
  private Set<Node> nodesAtMaxLevel = 
    Collections.newSetFromMap(new ConcurrentHashMap<Node, Boolean>());
  final TaskScheduler taskScheduler;
  private final List<JobInProgressListener> jobInProgressListeners =
    new CopyOnWriteArrayList<JobInProgressListener>();

  // system directory is completely owned by the JobTracker
  final static FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------

  // system files should have 700 permission
  final static FsPermission SYSTEM_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------
  
  private static Clock clock = null;
  
  static final Clock DEFAULT_CLOCK = new Clock();

  private final JobHistory jobHistory;
  
  private final JobTokenSecretManager jobTokenSecretManager 
    = new JobTokenSecretManager();
  
  JobTokenSecretManager getJobTokenSecretManager() {
    return jobTokenSecretManager;
  }

  private MRAsyncDiskService asyncDiskService;
  
  /**
   * Returns the delegation token secret manager instance in JobTracker.
   * 
   * @return DelegationTokenSecretManager object
   */
  public DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return secretManager;
  }
  
  /**
   * A client tried to submit a job before the Job Tracker was ready.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class IllegalStateException extends IOException {
 
    private static final long serialVersionUID = 1L;

    public IllegalStateException(String msg) {
      super(msg);
    }
  }

  private final AtomicInteger nextJobId = new AtomicInteger(1);

  public static final Log LOG = LogFactory.getLog(JobTracker.class);
    
  /**
   * Returns JobTracker's clock. Note that the correct clock implementation will
   * be obtained only when the JobTracker is initialized. If the JobTracker is
   * not initialized then the default clock i.e {@link Clock} is returned. 
   */
  static Clock getClock() {
    return clock == null ? DEFAULT_CLOCK : clock;
  }
  
  /**
   * Return the JT's job history handle.
   * @return the jobhistory handle
   */
  JobHistory getJobHistory() { return jobHistory; }
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
  public static JobTracker startTracker(JobConf conf) 
  throws IOException, InterruptedException {
    return startTracker(conf, DEFAULT_CLOCK);
  }

  static JobTracker startTracker(JobConf conf, Clock clock) 
  throws IOException, InterruptedException {
    return startTracker(conf, clock, generateNewIdentifier());
  }

  static JobTracker startTracker(JobConf conf, Clock clock, String identifier) 
  throws IOException, InterruptedException {
    JobTracker result = null;
    while (true) {
      try {
        result = new JobTracker(conf, clock, identifier);
        result.taskScheduler.setTaskTrackerManager(result);
        break;
      } catch (VersionMismatch e) {
        throw e;
      } catch (BindException e) {
        throw e;
      } catch (UnknownHostException e) {
        throw e;
      } catch (AccessControlException ace) {
        // in case of jobtracker not having right access
        // bail out
        throw ace;
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
    } else if (protocol.equals(ClientProtocol.class.getName())){
      return ClientProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(AdminOperationsProtocol.class.getName())){
      return AdminOperationsProtocol.versionID;
    } else if (protocol.equals(RefreshUserMappingsProtocol.class.getName())){
      return RefreshUserMappingsProtocol.versionID;
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
          Thread.sleep(tasktrackerExpiryInterval/3);
          long now = clock.getTime();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Starting launching task sweep");
          }
          synchronized (JobTracker.this) {
            synchronized (launchingTasks) {
              Iterator<Map.Entry<TaskAttemptID, Long>> itr =
                launchingTasks.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<TaskAttemptID, Long> pair = itr.next();
                TaskAttemptID taskId = pair.getKey();
                long age = now - (pair.getValue()).longValue();
                LOG.info(taskId + " is " + age + " ms debug.");
                if (age > tasktrackerExpiryInterval) {
                  LOG.info("Launching task " + taskId + " timed out.");
                  TaskInProgress tip = null;
                  tip = taskidToTIPMap.get(taskId);
                  if (tip != null) {
                    JobInProgress job = tip.getJob();
                    String trackerName = getAssignedTracker(taskId);
                    TaskTrackerStatus trackerStatus = 
                      getTaskTrackerStatus(trackerName); 
                      
                    // This might happen when the tasktracker has already
                    // expired and this thread tries to call failedtask
                    // again. expire tasktracker should have called failed
                    // task!
                    if (trackerStatus != null)
                      job.failedTask(tip, taskId, "Error launching task", 
                                     tip.isMapTask()? TaskStatus.Phase.MAP:
                                     TaskStatus.Phase.STARTING,
                                     TaskStatus.State.FAILED,
                                     trackerName);
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
                           clock.getTime());
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
          Thread.sleep(tasktrackerExpiryInterval / 3);
          checkExpiredTrackers();
        } catch (InterruptedException iex) {
          break;
        } catch (Exception t) {
          LOG.error("Tracker Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }
  
  void checkExpiredTrackers() {
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
          long now = clock.getTime();
          TaskTrackerStatus leastRecent = null;
          while ((trackerExpiryQueue.size() > 0) &&
              (leastRecent = trackerExpiryQueue.first()) != null &&
              ((now - leastRecent.getLastSeen()) > 
                  tasktrackerExpiryInterval)) {

            // Remove profile from head of queue
            trackerExpiryQueue.remove(leastRecent);
            String trackerName = leastRecent.getTrackerName();

            // Figure out if last-seen time should be updated, or if 
            // tracker is dead
            TaskTracker current = getTaskTracker(trackerName);
            TaskTrackerStatus newProfile = 
              (current == null ) ? null : current.getStatus();
            // Items might leave the taskTracker set through other means; the
            // status stored in 'taskTrackers' might be null, which means the
            // tracker has already been destroyed.
            if (newProfile != null) {
              if ((now - newProfile.getLastSeen()) >
                  tasktrackerExpiryInterval) {
                // Remove completely after marking the tasks as 'KILLED'
                removeTracker(current);
                // remove the mapping from the hosts list
                String hostname = newProfile.getHost();
                hostnameToTaskTracker.get(hostname).remove(trackerName);
              } else {
                // Update time by inserting latest profile
                trackerExpiryQueue.add(newProfile);
              }
            }
          }
        }
      }
    }
  }

  // Assumes JobTracker, taskTrackers and trackerExpiryQueue are locked on entry
  private void removeTracker(TaskTracker tracker) {
    lostTaskTracker(tracker);
    String trackerName = tracker.getStatus().getTrackerName();
    // tracker is lost, and if it is blacklisted, remove 
    // it from the count of blacklisted trackers in the cluster
    if (isBlacklisted(trackerName)) {
      faultyTrackers.decrBlackListedTrackers(1);
    }
    updateTaskTrackerStatus(trackerName, null);
    statistics.taskTrackerRemoved(trackerName);
    getInstrumentation().decTrackers(1);
  }

  public synchronized void retireJob(JobID jobid, String historyFile) {
    synchronized (jobs) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        JobStatus status = job.getStatus();
        
        //set the historyfile
        if (historyFile != null) {
          status.setHistoryFile(historyFile);
        }
        // clean up job files from the local disk
        job.cleanupLocalizedJobConf(job.getProfile().getJobID());

        //this configuration is primarily for testing
        //test cases can set this to false to validate job data structures on 
        //job completion
        boolean retireJob = 
          conf.getBoolean(JT_RETIREJOBS, true);

        if (retireJob) {
          //purge the job from memory
          removeJobTasks(job);
          jobs.remove(job.getProfile().getJobID());
          for (JobInProgressListener l : jobInProgressListeners) {
            l.jobRemoved(job);
          }

          String jobUser = job.getProfile().getUser();
          LOG.info("Retired job with id: '" + 
                   job.getProfile().getJobID() + "' of user '" +
                   jobUser + "'");

          //add the job status to retired cache
          retireJobs.addToCache(job.getStatus());
        }
      }
    }
  }

  ///////////////////////////////////////////////////////
  // Used to remove old finished Jobs that have been around for too long
  ///////////////////////////////////////////////////////
  class RetireJobs {
    private final Map<JobID, JobStatus> jobIDStatusMap = 
      new HashMap<JobID, JobStatus>();
    private final LinkedList<JobStatus> jobStatusQ = 
      new LinkedList<JobStatus>();
    public RetireJobs() {
    }

    synchronized void addToCache(JobStatus status) {
      status.setRetired();
      jobStatusQ.add(status);
      jobIDStatusMap.put(status.getJobID(), status);
      if (jobStatusQ.size() > retiredJobsCacheSize) {
        JobStatus removed = jobStatusQ.remove();
        jobIDStatusMap.remove(removed.getJobID());
        LOG.info("Retired job removed from cache " + removed.getJobID());
      }
    }

    synchronized JobStatus get(JobID jobId) {
      return jobIDStatusMap.get(jobId);
    }

    @SuppressWarnings("unchecked")
    synchronized LinkedList<JobStatus> getAll() {
      return (LinkedList<JobStatus>) jobStatusQ.clone();
    }
  }

  enum ReasonForBlackListing {
    EXCEEDING_FAILURES,
    NODE_UNHEALTHY
  }
  
  // The FaultInfo which indicates the number of faults of a tracker
  // and when the last fault occurred
  // and whether the tracker is blacklisted across all jobs or not
  private static class FaultInfo {
    static final String FAULT_FORMAT_STRING =  "%d failures on the tracker";
    int numFaults = 0;
    long lastUpdated;
    boolean blacklisted; 

    private boolean isHealthy;
    private HashMap<ReasonForBlackListing, String>rfbMap;

    FaultInfo(long time) {
      numFaults = 0;
      lastUpdated = time;
      blacklisted = false;
      rfbMap = new  HashMap<ReasonForBlackListing, String>();
    }

    void setFaultCount(int num) {
      numFaults = num;
    }

    void setLastUpdated(long timeStamp) {
      lastUpdated = timeStamp;
    }

    int getFaultCount() {
      return numFaults;
    }

    long getLastUpdated() {
      return lastUpdated;
    }
    
    boolean isBlacklisted() {
      return blacklisted;
    }
    
    void setBlacklist(ReasonForBlackListing rfb, 
        String trackerFaultReport) {
      blacklisted = true;
      this.rfbMap.put(rfb, trackerFaultReport);
    }

    public void setHealthy(boolean isHealthy) {
      this.isHealthy = isHealthy;
    }

    public boolean isHealthy() {
      return isHealthy;
    }
    
    public String getTrackerFaultReport() {
      StringBuffer sb = new StringBuffer();
      for(String reasons : rfbMap.values()) {
        sb.append(reasons);
        sb.append("\n");
      }
      if (sb.length() > 0) {
        sb.replace(sb.length()-1, sb.length(), "");
      }
      return sb.toString();
    }
    
    Set<ReasonForBlackListing> getReasonforblacklisting() {
      return this.rfbMap.keySet();
    }
    
    public void unBlacklist() {
      this.blacklisted = false;
      this.rfbMap.clear();
    }

    public boolean removeBlackListedReason(ReasonForBlackListing rfb) {
      String str = rfbMap.remove(rfb);
      return str!=null;
    }

    public void addBlackListedReason(ReasonForBlackListing rfb, String reason) {
      this.rfbMap.put(rfb, reason);
    }
    
  }

  private class FaultyTrackersInfo {
    // A map from hostName to its faults
    private Map<String, FaultInfo> potentiallyFaultyTrackers = 
              new HashMap<String, FaultInfo>();
    // This count gives the number of blacklisted trackers in the cluster 
    // at any time. This is maintained to avoid iteration over 
    // the potentiallyFaultyTrackers to get blacklisted trackers. And also
    // this count doesn't include blacklisted trackers which are lost, 
    // although the fault info is maintained for lost trackers.  
    private volatile int numBlacklistedTrackers = 0;

    /**
     * Increments faults(blacklist by job) for the tracker by one.
     * 
     * Adds the tracker to the potentially faulty list. 
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName 
     */
    void incrementFaults(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = getFaultInfo(hostName, true);
        long now = clock.getTime();
        int numFaults = fi.getFaultCount();
        ++numFaults;
        fi.setFaultCount(numFaults);
        fi.setLastUpdated(now);
        if (exceedsFaults(fi)) {
          LOG.info("Adding " + hostName + " to the blacklist"
              + " across all jobs");
          String reason = String.format(FaultInfo.FAULT_FORMAT_STRING,
              numFaults);
          blackListTracker(hostName, reason,
              ReasonForBlackListing.EXCEEDING_FAILURES);
        }
      }        
    }

    private void incrBlackListedTrackers(int count) {
      numBlacklistedTrackers += count;
      getInstrumentation().addBlackListedTrackers(count);
    }

    private void decrBlackListedTrackers(int count) {
      numBlacklistedTrackers -= count;
      getInstrumentation().decBlackListedTrackers(count);
    }

    private void blackListTracker(String hostName, String reason, ReasonForBlackListing rfb) {
      FaultInfo fi = getFaultInfo(hostName, true);
      boolean blackListed = fi.isBlacklisted();
      if (blackListed) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding blacklisted reason for tracker : " + hostName 
              + " Reason for blacklisting is : " + rfb);
        }
        if (!fi.getReasonforblacklisting().contains(rfb)) {
          LOG.info("Adding blacklisted reason for tracker : " + hostName
              + " Reason for blacklisting is : " + rfb);
        }
        fi.addBlackListedReason(rfb, reason);
      } else {
        LOG.info("Blacklisting tracker : " + hostName 
            + " Reason for blacklisting is : " + rfb);
        Set<TaskTracker> trackers = 
          hostnameToTaskTracker.get(hostName);
        synchronized (trackers) {
          for (TaskTracker tracker : trackers) {
            tracker.cancelAllReservations();
          }
        }
        removeHostCapacity(hostName);
        fi.setBlacklist(rfb, reason);
      }
    }
    
    private boolean canUnBlackListTracker(String hostName,
        ReasonForBlackListing rfb) {
      FaultInfo fi = getFaultInfo(hostName, false);
      if (fi == null) {
        return false;
      }
      
      Set<ReasonForBlackListing> rfbSet = fi.getReasonforblacklisting();
      return fi.isBlacklisted() && rfbSet.contains(rfb);
    }

    private void unBlackListTracker(String hostName,
        ReasonForBlackListing rfb) {
      // check if you can black list the tracker then call this methods
      FaultInfo fi = getFaultInfo(hostName, false);
      if (fi.removeBlackListedReason(rfb)) {
        if (fi.getReasonforblacklisting().isEmpty()) {
          addHostCapacity(hostName);
          LOG.info("Unblacklisting tracker : " + hostName);
          fi.unBlacklist();
          //We have unBlackListed tracker, so tracker should
          //definitely be healthy. Check fault count if fault count
          //is zero don't keep it memory.
          if (fi.numFaults == 0) {
            potentiallyFaultyTrackers.remove(hostName);
          }
        }
      }
    }
    
    // Assumes JobTracker is locked on entry.
    private FaultInfo getFaultInfo(String hostName, 
        boolean createIfNeccessary) {
      FaultInfo fi = null;
      synchronized (potentiallyFaultyTrackers) {
        fi = potentiallyFaultyTrackers.get(hostName);
        long now = clock.getTime();
        if (fi == null && createIfNeccessary) {
          fi = new FaultInfo(now);
          potentiallyFaultyTrackers.put(hostName, fi);
        }
      }
      return fi;
    }
    
    /**
     * Blacklists the tracker across all jobs if
     * <ol>
     * <li>#faults are more than 
     *     MAX_BLACKLISTS_PER_TRACKER (configurable) blacklists</li>
     * <li>#faults is 50% (configurable) above the average #faults</li>
     * <li>50% the cluster is not blacklisted yet </li>
     * </ol>
     */
    private boolean exceedsFaults(FaultInfo fi) {
      int faultCount = fi.getFaultCount();
      if (faultCount >= MAX_BLACKLISTS_PER_TRACKER) {
        // calculate avgBlackLists
        long clusterSize = getClusterStatus().getTaskTrackers();
        long sum = 0;
        for (FaultInfo f : potentiallyFaultyTrackers.values()) {
          sum += f.getFaultCount();
        }
        double avg = (double) sum / clusterSize;
            
        long totalCluster = clusterSize + numBlacklistedTrackers;
        if ((faultCount - avg) > (AVERAGE_BLACKLIST_THRESHOLD * avg) &&
            numBlacklistedTrackers < (totalCluster * MAX_BLACKLIST_PERCENT)) {
          return true;
        }
      }
      return false;
    }
    
    /**
     * Removes the tracker from blacklist and
     * from potentially faulty list, when it is restarted.
     * 
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName
     */
    void markTrackerHealthy(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = potentiallyFaultyTrackers.remove(hostName);
        if (fi != null && fi.isBlacklisted()) {
          LOG.info("Removing " + hostName + " from blacklist");
          addHostCapacity(hostName);
        }
      }
    }

    /**
     * Check whether tasks can be assigned to the tracker.
     *
     * One fault of the tracker is discarded if there
     * are no faults during one day. So, the tracker will get a 
     * chance again to run tasks of a job.
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName The tracker name
     * @param now The current time
     * 
     * @return true if the tracker is blacklisted 
     *         false otherwise
     */
    boolean shouldAssignTasksToTracker(String hostName, long now) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = potentiallyFaultyTrackers.get(hostName);
        if (fi != null &&
            (now - fi.getLastUpdated()) > UPDATE_FAULTY_TRACKER_INTERVAL) {
          int numFaults = fi.getFaultCount() - 1;
          fi.setFaultCount(numFaults);
          fi.setLastUpdated(now);
          if (canUnBlackListTracker(hostName, 
              ReasonForBlackListing.EXCEEDING_FAILURES)) {
            unBlackListTracker(hostName,
                ReasonForBlackListing.EXCEEDING_FAILURES);
          }
        }
        return (fi != null && fi.isBlacklisted());
      }
    }

    private void removeHostCapacity(String hostName) {
      synchronized (taskTrackers) {
        // remove the capacity of trackers on this host
        int numTrackersOnHost = 0;
        for (TaskTrackerStatus status : getStatusesOnHost(hostName)) {
          int mapSlots = status.getMaxMapSlots();
          totalMapTaskCapacity -= mapSlots;
          int reduceSlots = status.getMaxReduceSlots();
          totalReduceTaskCapacity -= reduceSlots;
          ++numTrackersOnHost;
          getInstrumentation().addBlackListedMapSlots(
              mapSlots);
          getInstrumentation().addBlackListedReduceSlots(
              reduceSlots);
        }
        // remove the host
        uniqueHostsMap.remove(hostName);
        incrBlackListedTrackers(numTrackersOnHost);
      }
    }
    
    // This is called on tracker's restart or after a day of blacklist.
    private void addHostCapacity(String hostName) {
      synchronized (taskTrackers) {
        int numTrackersOnHost = 0;
        // add the capacity of trackers on the host
        for (TaskTrackerStatus status : getStatusesOnHost(hostName)) {
          int mapSlots = status.getMaxMapSlots();
          totalMapTaskCapacity += mapSlots;
          int reduceSlots = status.getMaxReduceSlots();
          totalReduceTaskCapacity += reduceSlots;
          numTrackersOnHost++;
          getInstrumentation().decBlackListedMapSlots(mapSlots);
          getInstrumentation().decBlackListedReduceSlots(reduceSlots);
        }
        uniqueHostsMap.put(hostName,
                           numTrackersOnHost);
        decrBlackListedTrackers(numTrackersOnHost);
      }
    }

    /**
     * Whether a host is blacklisted across all the jobs. 
     * 
     * Assumes JobTracker is locked on the entry.
     * @param hostName
     * @return
     */
    boolean isBlacklisted(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.isBlacklisted();
        }
      }
      return false;
    }
    
    // Assumes JobTracker is locked on the entry.
    int getFaultCount(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.getFaultCount();
        }
      }
      return 0;
    }
    
    // Assumes JobTracker is locked on the entry.
    Set<ReasonForBlackListing> getReasonForBlackListing(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.getReasonforblacklisting();
        }
      }
      return null;
    }


    // Assumes JobTracker is locked on the entry.
    void setNodeHealthStatus(String hostName, boolean isHealthy, String reason) {
      FaultInfo fi = null;
      // If tracker is not healthy, create a fault info object
      // blacklist it.
      if (!isHealthy) {
        fi = getFaultInfo(hostName, true);
        fi.setHealthy(isHealthy);
        updateNodeHealthFailureStatistics(hostName, fi);
        synchronized (potentiallyFaultyTrackers) { 
          blackListTracker(hostName, reason,
              ReasonForBlackListing.NODE_UNHEALTHY);
        }
      } else {
        fi = getFaultInfo(hostName, false);
        if (fi == null) {
          return;
        } else {
          if (canUnBlackListTracker(hostName,
              ReasonForBlackListing.NODE_UNHEALTHY)) {
            unBlackListTracker(hostName, ReasonForBlackListing.NODE_UNHEALTHY);
          }
        }
      }
    }

    /**
     * Update the node health failure statistics of the given
     * host.
     * 
     * We increment the count only when the host transitions
     * from healthy -> unhealthy. 
     * 
     * @param hostName
     * @param fi Fault info object for the host.
     */
    private void updateNodeHealthFailureStatistics(String hostName, 
        FaultInfo fi) {
      //Check if the node was already blacklisted due to 
      //unhealthy reason. If so dont increment the count.
      if (!fi.getReasonforblacklisting().contains(
          ReasonForBlackListing.NODE_UNHEALTHY)) {
        Set<TaskTracker> trackers = hostnameToTaskTracker.get(hostName);
        synchronized (trackers) {
          for (TaskTracker t : trackers) {
            TaskTrackerStat stat = statistics.getTaskTrackerStat(
                t.getTrackerName());
            stat.incrHealthCheckFailed();
          }
        }
      }
    }
    
  }
  
  /**
   * Get all task tracker statuses on given host
   * 
   * Assumes JobTracker is locked on the entry
   * @param hostName
   * @return {@link java.util.List} of {@link TaskTrackerStatus}
   */
  private List<TaskTrackerStatus> getStatusesOnHost(String hostName) {
    List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus(); 
        if (hostName.equals(status.getHost())) {
          statuses.add(status);
        }
      }
    }
    return statuses;
  }
  
  ///////////////////////////////////////////////////////
  // Used to recover the jobs upon restart
  ///////////////////////////////////////////////////////
  class RecoveryManager {
    private Set<JobID> jobsToRecover; // set of jobs to be recovered
    private int recovered;
    private int restartCount = 0;
    private boolean shouldRecover = false;

    public RecoveryManager() {
      jobsToRecover = new TreeSet<JobID>();
    }

    public boolean contains(JobID id) {
      return jobsToRecover.contains(id);
    }

    int getRecovered() {
      return recovered;
    }

    void addJobForRecovery(JobID id) {
      jobsToRecover.add(id);
    }

    public boolean shouldRecover() {
      return shouldRecover;
    }

    Set<JobID> getJobsToRecover() {
      return jobsToRecover;
    }

    // add the job
    void addJobForRecovery(FileStatus status) throws IOException {
      recoveryManager.addJobForRecovery(JobID.forName(status.getPath().getName()));
      shouldRecover = true; // enable actual recovery if num-files > 1
    }

   
    Path getRestartCountFile() {
      return new Path(getSystemDir(), "jobtracker.info");
    }

    Path getTempRestartCountFile() {
      return new Path(getSystemDir(), "jobtracker.info.recover");
    }

    /**
     * Initialize the recovery process. It simply creates a jobtracker.info file
     * in the jobtracker's system directory and writes its restart count in it.
     * For the first start, the jobtracker writes '0' in it. Upon subsequent 
     * restarts the jobtracker replaces the count with its current count which 
     * is (old count + 1). The whole purpose of this api is to obtain restart 
     * counts across restarts to avoid attempt-id clashes.
     * 
     * Note that in between if the jobtracker.info files goes missing then the
     * jobtracker will disable recovery and continue. 
     *  
     */
    void updateRestartCount() throws IOException {
      Path restartFile = getRestartCountFile();
      Path tmpRestartFile = getTempRestartCountFile();
      FsPermission filePerm = new FsPermission(SYSTEM_FILE_PERMISSION);

      // read the count from the jobtracker info file
      if (fs.exists(restartFile)) {
        fs.delete(tmpRestartFile, false); // delete the tmp file
      } else if (fs.exists(tmpRestartFile)) {
        // if .rec exists then delete the main file and rename the .rec to main
        fs.rename(tmpRestartFile, restartFile); // rename .rec to main file
      } else {
        // For the very first time the jobtracker will create a jobtracker.info
        // file. If the jobtracker has restarted then disable recovery as files'
        // needed for recovery are missing.

        // disable recovery if this is a restart
        shouldRecover = false;

        // write the jobtracker.info file
        try {
          FSDataOutputStream out = FileSystem.create(fs, restartFile, 
                                                     filePerm);
          out.writeInt(0);
          out.close();
        } catch (IOException ioe) {
          LOG.warn("Writing to file " + restartFile + " failed!");
          LOG.warn("FileSystem is not ready yet!");
          fs.delete(restartFile, false);
          throw ioe;
        }
        return;
      }

      FSDataInputStream in = fs.open(restartFile);
      try {
        // read the old count
        restartCount = in.readInt();
        ++restartCount; // increment the restart count
      } catch (IOException ioe) {
        LOG.warn("System directory is garbled. Failed to read file " 
                 + restartFile);
        LOG.warn("Jobtracker recovery is not possible with garbled"
                 + " system directory! Please delete the system directory and"
                 + " restart the jobtracker. Note that deleting the system" 
                 + " directory will result in loss of all the running jobs.");
        throw new RuntimeException(ioe);
      } finally {
        if (in != null) {
          in.close();
        }
      }

      // Write back the new restart count and rename the old info file
      //TODO This is similar to jobhistory recovery, maybe this common code
      //      can be factored out.
      
      // write to the tmp file
      FSDataOutputStream out = FileSystem.create(fs, tmpRestartFile, filePerm);
      out.writeInt(restartCount);
      out.close();

      // delete the main file
      fs.delete(restartFile, false);
      
      // rename the .rec to main file
      fs.rename(tmpRestartFile, restartFile);
    }

    public void recover() {
      long recoveryProcessStartTime = clock.getTime();
      if (!shouldRecover()) {
        // clean up jobs structure
        jobsToRecover.clear();
        return;
      }

      LOG.info("Starting the recovery process for " + jobsToRecover.size() +
          " jobs ...");
      for (JobID jobId : jobsToRecover) {
        LOG.info("Submitting job "+ jobId);
        try {
          Path jobInfoFile = getSystemFileForJob(jobId);
          FSDataInputStream in = fs.open(jobInfoFile);
          JobInfo token = new JobInfo();
          token.readFields(in);
          in.close();
          UserGroupInformation ugi = 
            UserGroupInformation.createRemoteUser(token.getUser().toString());
          submitJob(token.getJobID(), restartCount, 
              ugi, token.getJobSubmitDir().toString(), true, null);
          recovered++;
        } catch (Exception e) {
          LOG.warn("Could not recover job " + jobId, e);
        }
      }
      recoveryDuration = clock.getTime() - recoveryProcessStartTime;
      hasRecovered = true;

      LOG.info("Recovery done! Recoverd " + recovered +" of "+ 
          jobsToRecover.size() + " jobs.");
      LOG.info("Recovery Duration (ms):" + recoveryDuration);
    }

  }

  private final JobTrackerInstrumentation myInstrumentation;
    
  /////////////////////////////////////////////////////////////////
  // The real JobTracker
  ////////////////////////////////////////////////////////////////
  int port;
  String localMachine;
  private final String trackerIdentifier;
  long startTime;
  int totalSubmissions = 0;
  private int totalMapTaskCapacity;
  private int totalReduceTaskCapacity;
  private final HostsFileReader hostsReader;
  
  // JobTracker recovery variables
  private volatile boolean hasRecovered = false;
  private volatile long recoveryDuration;

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
  Map<JobID, JobInProgress> jobs =  
    Collections.synchronizedMap(new TreeMap<JobID, JobInProgress>());

  // (trackerID --> list of jobs to cleanup)
  Map<String, Set<JobID>> trackerToJobsToCleanup = 
    new HashMap<String, Set<JobID>>();
  
  // (trackerID --> list of tasks to cleanup)
  Map<String, Set<TaskAttemptID>> trackerToTasksToCleanup = 
    new HashMap<String, Set<TaskAttemptID>>();
  
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
  
  // (hostname --> Set(tasktracker))
  // This is used to keep track of all trackers running on one host. While
  // decommissioning the host, all the trackers on the host will be lost.
  Map<String, Set<TaskTracker>> hostnameToTaskTracker = 
    Collections.synchronizedMap(new TreeMap<String, Set<TaskTracker>>());
  
  // Number of resolved entries
  int numResolved;
    
  private FaultyTrackersInfo faultyTrackers = new FaultyTrackersInfo();
  
  private JobTrackerStatistics statistics = 
    new JobTrackerStatistics();
  //
  // Watch and expire TaskTracker objects using these structures.
  // We can map from Name->TaskTrackerStatus, or we can expire by time.
  //
  int totalMaps = 0;
  int totalReduces = 0;
  private int occupiedMapSlots = 0;
  private int occupiedReduceSlots = 0;
  private int reservedMapSlots = 0;
  private int reservedReduceSlots = 0;
  private HashMap<String, TaskTracker> taskTrackers =
    new HashMap<String, TaskTracker>();
  Map<String,Integer>uniqueHostsMap = new ConcurrentHashMap<String, Integer>();
  ExpireTrackers expireTrackers = new ExpireTrackers();
  Thread expireTrackersThread = null;
  RetireJobs retireJobs = new RetireJobs();
  final int retiredJobsCacheSize;
  ExpireLaunchingTasks expireLaunchingTasks = new ExpireLaunchingTasks();
  Thread expireLaunchingTaskThread = new Thread(expireLaunchingTasks,
                                                "expireLaunchingTasks");

  final CompletedJobStatusStore completedJobStatusStore;
  Thread completedJobsStoreThread = null;
  final RecoveryManager recoveryManager;

  /**
   * It might seem like a bug to maintain a TreeSet of tasktracker objects,
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
  final HttpServer infoServer;
  int infoPort;

  Server interTrackerServer;

  // Some jobs are stored in a local system directory.  We can delete
  // the files when we're done with the job.
  static final String SUBDIR = "jobTracker";
  final LocalFileSystem localFs;
  FileSystem fs = null;
  Path systemDir = null;
  JobConf conf;

  private final ACLsManager aclsManager;

  long limitMaxMemForMapTasks;
  long limitMaxMemForReduceTasks;
  long memSizeForMapSlotOnJT;
  long memSizeForReduceSlotOnJT;

  private final QueueManager queueManager;

  //TO BE USED BY TEST CLASSES ONLY
  //ONLY BUILD THE STATE WHICH IS REQUIRED BY TESTS
  JobTracker() {
    hostsReader = null;
    retiredJobsCacheSize = 0;
    infoServer = null;
    queueManager = null;
    aclsManager = null;
    taskScheduler = null;
    trackerIdentifier = null;
    recoveryManager = null;
    jobHistory = null;
    completedJobStatusStore = null;
    tasktrackerExpiryInterval = 0;
    myInstrumentation = new JobTrackerMetricsInst(this, new JobConf());
    secretManager = null;
    localFs = null;
  }

  
  JobTracker(JobConf conf) 
  throws IOException,InterruptedException {
    this(conf, new Clock());
  }
  /**
   * Start the JobTracker process, listen on the indicated port
   */
  JobTracker(JobConf conf, Clock clock) 
  throws IOException, InterruptedException {
    this(conf, clock, generateNewIdentifier());
  }

  JobTracker(final JobConf conf, Clock newClock, String jobtrackerIndentifier) 
  throws IOException, InterruptedException {
    // Set ports, start RPC servers, setup security policy etc.
    InetSocketAddress addr = getAddress(conf);
    this.localMachine = addr.getHostName();
    this.port = addr.getPort();
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, JTConfig.JT_KEYTAB_FILE, JTConfig.JT_USER_NAME,
        localMachine);

    clock = newClock;
    
    long secretKeyInterval = 
      conf.getLong(MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_KEY, 
                   MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime =
      conf.getLong(MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                   MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval =
      conf.getLong(MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 
                   MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    secretManager = 
      new DelegationTokenSecretManager(secretKeyInterval,
                                       tokenMaxLifetime,
                                       tokenRenewInterval,
                                       DELEGATION_TOKEN_GC_INTERVAL);
    secretManager.startThreads();

    //
    // Grab some static constants
    //
    tasktrackerExpiryInterval = 
      conf.getLong(JT_TRACKER_EXPIRY_INTERVAL, 10 * 60 * 1000);
    retiredJobsCacheSize = conf.getInt(JT_RETIREJOB_CACHE_SIZE, 1000);
    MAX_BLACKLISTS_PER_TRACKER = 
      conf.getInt(JTConfig.JT_MAX_TRACKER_BLACKLISTS, 4);
    
    NUM_HEARTBEATS_IN_SECOND = 
      conf.getInt(JT_HEARTBEATS_IN_SECOND, DEFAULT_NUM_HEARTBEATS_IN_SECOND);
    if (NUM_HEARTBEATS_IN_SECOND < MIN_NUM_HEARTBEATS_IN_SECOND) {
      NUM_HEARTBEATS_IN_SECOND = DEFAULT_NUM_HEARTBEATS_IN_SECOND;
    }
    
    HEARTBEATS_SCALING_FACTOR = 
      conf.getFloat(JT_HEARTBEATS_SCALING_FACTOR, 
                    DEFAULT_HEARTBEATS_SCALING_FACTOR);
    if (HEARTBEATS_SCALING_FACTOR < MIN_HEARTBEATS_SCALING_FACTOR) {
      HEARTBEATS_SCALING_FACTOR = DEFAULT_HEARTBEATS_SCALING_FACTOR;
    }

    //This configuration is there solely for tuning purposes and 
    //once this feature has been tested in real clusters and an appropriate
    //value for the threshold has been found, this config might be taken out.
    AVERAGE_BLACKLIST_THRESHOLD = conf.getFloat(JTConfig.JT_AVG_BLACKLIST_THRESHOLD, 0.5f); 

    // This is a directory of temporary submission files.  We delete it
    // on startup, and can delete any files that we're done with
    this.conf = conf;
    JobConf jobConf = new JobConf(conf);

    initializeTaskMemoryRelatedConfig();

    // Read the hosts/exclude files to restrict access to the jobtracker.
    this.hostsReader = new HostsFileReader(conf.get(JTConfig.JT_HOSTS_FILENAME, ""),
                                           conf.get(JTConfig.JT_HOSTS_EXCLUDE_FILENAME, ""));

    Configuration clusterConf = new Configuration(this.conf);
    queueManager = new QueueManager(clusterConf);
    
    aclsManager = new ACLsManager(conf, new JobACLsManager(conf), queueManager);

    LOG.info("Starting jobtracker with owner as " +
        getMROwner().getShortUserName());

    // Create the scheduler
    Class<? extends TaskScheduler> schedulerClass
      = conf.getClass(JT_TASK_SCHEDULER,
          JobQueueTaskScheduler.class, TaskScheduler.class);
    taskScheduler = (TaskScheduler) ReflectionUtils.newInstance(schedulerClass, conf);
    
    int handlerCount = conf.getInt(JT_IPC_HANDLER_COUNT, 10);
    this.interTrackerServer = RPC.getServer(ClientProtocol.class,
                                            this,
                                            addr.getHostName(), 
                                            addr.getPort(), handlerCount, 
                                            false, conf, secretManager);

    // Set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      this.interTrackerServer.refreshServiceAcl(conf, new MapReducePolicyProvider());
    }

    if (LOG.isDebugEnabled()) {
      Properties p = System.getProperties();
      for (Iterator it = p.keySet().iterator(); it.hasNext();) {
        String key = (String) it.next();
        String val = p.getProperty(key);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Property '" + key + "' is " + val);
        }
      }
    }

    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(
        conf.get(JT_HTTP_ADDRESS, "0.0.0.0:50030"));
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.startTime = clock.getTime();
    infoServer = new HttpServer("job", infoBindAddress, tmpInfoPort, 
        tmpInfoPort == 0, conf, aclsManager.getAdminsAcl());
    infoServer.setAttribute("job.tracker", this);
    // initialize history parameters.
    jobHistory = new JobHistory();
    jobHistory.init(this, conf, this.localMachine, this.startTime);
    
    infoServer.addServlet("reducegraph", "/taskgraph", TaskGraphServlet.class);
    infoServer.start();
    
    this.trackerIdentifier = jobtrackerIndentifier;

    // Initialize instrumentation
    JobTrackerInstrumentation tmp;
    Class<? extends JobTrackerInstrumentation> metricsInst =
      getInstrumentationClass(jobConf);
    try {
      java.lang.reflect.Constructor<? extends JobTrackerInstrumentation> c =
        metricsInst.getConstructor(new Class[] {JobTracker.class, JobConf.class} );
      tmp = c.newInstance(this, jobConf);
    } catch(Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by 
      //falling back on the default.
      LOG.error("failed to initialize job tracker metrics", e);
      tmp = new JobTrackerMetricsInst(this, jobConf);
    }
    myInstrumentation = tmp;
    
    // The rpc/web-server ports can be ephemeral ports... 
    // ... ensure we have the correct info
    this.port = interTrackerServer.getListenerAddress().getPort();
    this.conf.set(JT_IPC_ADDRESS, (this.localMachine + ":" + this.port));
    this.localFs = FileSystem.getLocal(conf);
    LOG.info("JobTracker up at: " + this.port);
    this.infoPort = this.infoServer.getPort();
    this.conf.set(JT_HTTP_ADDRESS, 
        infoBindAddress + ":" + this.infoPort); 
    LOG.info("JobTracker webserver: " + this.infoServer.getPort());
    
    // start the recovery manager
    recoveryManager = new RecoveryManager();
    
    while (!Thread.currentThread().isInterrupted()) {
      try {
        // if we haven't contacted the namenode go ahead and do it
        if (fs == null) {
          fs = getMROwner().doAs(new PrivilegedExceptionAction<FileSystem>() {
            public FileSystem run() throws IOException {
              return FileSystem.get(conf);
          }});
        }
        // clean up the system dir, which will only work if hdfs is out of 
        // safe mode
        if (systemDir == null) {
          systemDir = new Path(getSystemDir());    
        }
        try {
          FileStatus systemDirStatus = fs.getFileStatus(systemDir);
          if (!systemDirStatus.getOwner().equals(
              getMROwner().getShortUserName())) {
            throw new AccessControlException("The systemdir " + systemDir + 
                " is not owned by " + getMROwner().getShortUserName());
          }
          if (!systemDirStatus.getPermission().equals(SYSTEM_DIR_PERMISSION)) {
            LOG.warn("Incorrect permissions on " + systemDir + 
                ". Setting it to " + SYSTEM_DIR_PERMISSION);
            fs.setPermission(systemDir,new FsPermission(SYSTEM_DIR_PERMISSION));
          }
        } catch (FileNotFoundException fnf) {} //ignore
        // Make sure that the backup data is preserved
        FileStatus[] systemDirData;
        try {
          systemDirData = fs.listStatus(this.systemDir);
        } catch (FileNotFoundException fnfe) {
          systemDirData = null;
        }
        
        // Check if the history is enabled .. as we can't have persistence with 
        // history disabled
        if (conf.getBoolean(JT_RESTART_ENABLED, false) 
            && systemDirData != null) {
          for (FileStatus status : systemDirData) {
            try {
              recoveryManager.addJobForRecovery(status);
            } catch (Throwable t) {
              LOG.warn("Failed to add the job " + status.getPath().getName(), 
                       t);
            }
          }
          
          // Check if there are jobs to be recovered
          if (recoveryManager.shouldRecover()) {
            break; // if there is something to recover else clean the sys dir
          }
        }
        if (!fs.exists(systemDir)) {
          LOG.info("Creating the system directory");
          if (FileSystem.mkdirs(fs, systemDir, 
                                new FsPermission(SYSTEM_DIR_PERMISSION))) {
            // success
            break;
          } else {
            LOG.error("Mkdirs failed to create " + systemDir);
          }
        } else {
          // It exists, just set permissions and clean the contents up
          LOG.info("Cleaning up the system directory");
          fs.setPermission(systemDir, new FsPermission(SYSTEM_DIR_PERMISSION));
          deleteContents(fs, systemDir);
          break;
        }
      } catch (AccessControlException ace) {
        LOG.warn("Failed to operate on " + JTConfig.JT_SYSTEM_DIR + "("
                 + systemDir.makeQualified(fs)
                 + ") because of permissions.");
        LOG.warn("This directory should exist and be owned by the user '" +
                 UserGroupInformation.getCurrentUser() + "'");
        LOG.warn("Bailing out ... ");
        throw ace;
      } catch (IOException ie) {
        LOG.info("problem cleaning system directory: " +
                 systemDir.makeQualified(fs), ie);
      }
      Thread.sleep(FS_ACCESS_RETRY_PERIOD);
    }
    
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
    
    // Same with 'localDir' except it's always on the local disk.
    asyncDiskService = new MRAsyncDiskService(FileSystem.getLocal(conf), conf.getLocalDirs());
    asyncDiskService.moveAndDeleteFromEachVolume(SUBDIR);

    // Initialize history DONE folder
    jobHistory.initDone(conf, fs);
    final String historyLogDir = 
      jobHistory.getCompletedJobHistoryLocation().toString();
    infoServer.setAttribute("historyLogDir", historyLogDir);
    FileSystem historyFS = getMROwner().doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws IOException {
        return new Path(historyLogDir).getFileSystem(conf);
      }
    });
    infoServer.setAttribute("fileSys", historyFS);

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass(
            CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, 
            ScriptBasedMapping.class, DNSToSwitchMapping.class),
         conf);
    this.numTaskCacheLevels = conf.getInt(JT_TASKCACHE_LEVELS, 
        NetworkTopology.DEFAULT_HOST_LEVEL);

    //initializes the job status store
    completedJobStatusStore = new CompletedJobStatusStore(conf, aclsManager);
  }

  /**
   * Recursively delete the contents of a directory without deleting the
   * directory itself.
   */
  private void deleteContents(FileSystem fs, Path dir) throws IOException {
    for (FileStatus stat : fs.listStatus(dir)) {
      if (!fs.delete(stat.getPath(), true)) {
        throw new IOException("Unable to delete " + stat.getPath());
      }
    }
  }

  private static SimpleDateFormat getDateFormat() {
    return new SimpleDateFormat("yyyyMMddHHmm");
  }

  private static String generateNewIdentifier() {
    return getDateFormat().format(new Date());
  }
  
  static boolean validateIdentifier(String id) {
    try {
      // the jobtracker id should be 'date' parseable
      getDateFormat().parse(id);
      return true;
    } catch (ParseException pe) {}
    return false;
  }

  static boolean validateJobNumber(String id) {
    try {
      // the job number should be integer parseable
      Integer.parseInt(id);
      return true;
    } catch (IllegalArgumentException pe) {}
    return false;
  }

  /**
   * Whether the JT has recovered upon restart
   */
  public boolean hasRecovered() {
    return hasRecovered;
  }

  /**
   * How long the jobtracker took to recover from restart.
   */
  public long getRecoveryDuration() {
    return recoveryDuration;
  }

  /**
   * Get JobTracker's FileSystem. This is the filesystem for mapreduce.system.dir.
   */
  FileSystem getFileSystem() {
    return fs;
  }

  /**
   * Get JobTracker's LocalFileSystem handle. This is used by jobs for 
   * localizing job files to the local disk.
   */
  LocalFileSystem getLocalFileSystem() throws IOException {
    return localFs;
  }

  TaskScheduler getScheduler() {
    return taskScheduler;
  }

  public static Class<? extends JobTrackerInstrumentation> getInstrumentationClass(Configuration conf) {
    return conf.getClass(JT_INSTRUMENTATION,
        JobTrackerMetricsInst.class, JobTrackerInstrumentation.class);
  }
  
  public static void setInstrumentationClass(Configuration conf, Class<? extends JobTrackerInstrumentation> t) {
    conf.setClass(JT_INSTRUMENTATION,
        t, JobTrackerInstrumentation.class);
  }

  JobTrackerInstrumentation getInstrumentation() {
    return myInstrumentation;
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String jobTrackerStr =
      conf.get(JT_IPC_ADDRESS, "localhost:8012");
    return NetUtils.createSocketAddr(jobTrackerStr);
  }

  void  startExpireTrackersThread() {
    this.expireTrackersThread = new Thread(this.expireTrackers, "expireTrackers");
    this.expireTrackersThread.start();
  }

  /**
   * Run forever
   */
  public void offerService() throws InterruptedException, IOException {
    // Prepare for recovery. This is done irrespective of the status of restart
    // flag.
    while (true) {
      try {
        recoveryManager.updateRestartCount();
        break;
      } catch (IOException ioe) {
        LOG.warn("Failed to initialize recovery manager. ", ioe);
        // wait for some time
        Thread.sleep(FS_ACCESS_RETRY_PERIOD);
        LOG.warn("Retrying...");
      }
    }

    taskScheduler.start();
    
    recoveryManager.recover();
    
    // refresh the node list as the recovery manager might have added 
    // disallowed trackers
    refreshHosts();
    
    startExpireTrackersThread();

    expireLaunchingTaskThread.start();

    if (completedJobStatusStore.isActive()) {
      completedJobsStoreThread = new Thread(completedJobStatusStore,
                                            "completedjobsStore-housekeeper");
      completedJobsStoreThread.start();
    }

    // start the inter-tracker server once the jt is ready
    this.interTrackerServer.start();
    
    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");
    
    this.interTrackerServer.join();
    LOG.info("Stopped interTrackerServer");
  }

  void close() throws IOException {
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        LOG.warn("Exception shutting down JobTracker", ex);
      }
    }
    if (this.interTrackerServer != null) {
      LOG.info("Stopping interTrackerServer");
      this.interTrackerServer.stop();
    }

    stopExpireTrackersThread();

    if (taskScheduler != null) {
      taskScheduler.terminate();
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
    
    if (jobHistory != null) {
      jobHistory.shutDown();
    }
    DelegationTokenRenewal.close();
    LOG.info("stopped all jobtracker services");
    return;
  }

  void stopExpireTrackersThread() {
    if (this.expireTrackersThread != null && this.expireTrackersThread.isAlive()) {
      LOG.info("Stopping expireTrackers");
      this.expireTrackersThread.interrupt();
      try {
        this.expireTrackersThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
  }

    
  ///////////////////////////////////////////////////////
  // Maintain lookup tables; called by JobInProgress
  // and TaskInProgress
  ///////////////////////////////////////////////////////
  void createTaskEntry(TaskAttemptID taskid, String taskTracker, TaskInProgress tip) {
    LOG.info("Adding task (" + tip.getAttemptType(taskid) + ") " + 
      "'"  + taskid + "' to tip " + 
      tip.getTIPId() + ", for tracker '" + taskTracker + "'");

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
    if (taskidToTIPMap.remove(taskid) != null) {
      // log the task removal in case of success
      LOG.info("Removing task '" + taskid + "'");
    }
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
      
    if (LOG.isDebugEnabled()) {
      LOG.debug("Marked '" + taskid + "' from '" + taskTracker + "'");
    }
  }

  /**
   * Mark all 'non-running' jobs of the job for pruning.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param job the completed job
   */
  void markCompletedJob(JobInProgress job) {
    for (TaskInProgress tip : job.getTasks(TaskType.JOB_SETUP)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getTasks(TaskType.MAP)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getTasks(TaskType.REDUCE)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
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
  void removeMarkedTasks(String taskTracker) {
    // Purge all the 'marked' tasks which were running at taskTracker
    Set<TaskAttemptID> markedTaskSet = 
      trackerToMarkedTasksMap.get(taskTracker);
    if (markedTaskSet != null) {
      for (TaskAttemptID taskid : markedTaskSet) {
        removeTaskEntry(taskid);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removed marked completed task '" + taskid + "' from '" + 
                    taskTracker + "'");
        }
      }
      // Clear
      trackerToMarkedTasksMap.remove(taskTracker);
    }
  }
    
  /**
   * Call {@link #removeTaskEntry(String)} for each of the
   * job's tasks.
   * When the job is retiring we can afford to nuke all it's tasks
   * 
   * @param job the job about to be 'retired'
   */
  synchronized void removeJobTasks(JobInProgress job) { 
    // iterate over all the task types
    for (TaskType type : TaskType.values()) {
      // iterate over all the tips of the type under consideration
      for (TaskInProgress tip : job.getTasks(type)) {
        // iterate over all the task-ids in the tip under consideration
        for (TaskAttemptID id : tip.getAllTaskAttemptIDs()) {
          // remove the task-id entry from the jobtracker
          removeTaskEntry(id);
        }
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

    // start the merge of log files
    JobID id = job.getStatus().getJobID();

    // mark the job as completed
    try {
      jobHistory.markCompleted(id);
    } catch (IOException ioe) {
      LOG.info("Failed to mark job " + id + " as completed!", ioe);
    }

    final JobTrackerInstrumentation metrics = getInstrumentation();
    metrics.finalizeJob(conf, id);
    
    // mark the job for cleanup at all the trackers
    addJobForCleanup(id);

    // add the blacklisted trackers to potentially faulty list
    if (job.getStatus().getRunState() == JobStatus.SUCCEEDED) {
      if (job.getNoOfBlackListedTrackers() > 0) {
        for (String hostName : job.getBlackListedTrackers()) {
          faultyTrackers.incrementFaults(hostName);
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
      if ((status.getRunState() == JobStatus.FAILED)
          || (status.getRunState() == JobStatus.KILLED)) {
        v.add(jip);
      }
    }
    return v;
  }

  public synchronized List<JobInProgress> getFailedJobs() {
    synchronized (jobs) {
      return failedJobs();
    }
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

  public synchronized List<JobInProgress> getCompletedJobs() {
    synchronized (jobs) {
      return completedJobs();
    }
  }

  /**
   * Get all the task trackers in the cluster
   * 
   * @return {@link Collection} of {@link TaskTrackerStatus} 
   */
  // lock to taskTrackers should hold JT lock first.
  public synchronized Collection<TaskTrackerStatus> taskTrackers() {
    Collection<TaskTrackerStatus> ttStatuses;
    synchronized (taskTrackers) {
      ttStatuses = 
        new ArrayList<TaskTrackerStatus>(taskTrackers.values().size());
      for (TaskTracker tt : taskTrackers.values()) {
        ttStatuses.add(tt.getStatus());
      }
    }
    return ttStatuses;
  }
  
  /**
   * Get the active task tracker statuses in the cluster
   *  
   * @return {@link Collection} of active {@link TaskTrackerStatus} 
   */
  // This method is synchronized to make sure that the locking order 
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers 
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public Collection<TaskTrackerStatus> activeTaskTrackers() {
    Collection<TaskTrackerStatus> activeTrackers = 
      new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for ( TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (!faultyTrackers.isBlacklisted(status.getHost())) {
          activeTrackers.add(status);
        }
      }
    }
    return activeTrackers;
  }
  
  /**
   * Get the active and blacklisted task tracker names in the cluster. The first
   * element in the returned list contains the list of active tracker names.
   * The second element in the returned list contains the list of blacklisted
   * tracker names. 
   */
  // This method is synchronized to make sure that the locking order 
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers 
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public List<List<String>> taskTrackerNames() {
    List<String> activeTrackers = 
      new ArrayList<String>();
    List<String> blacklistedTrackers = 
      new ArrayList<String>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (!faultyTrackers.isBlacklisted(status.getHost())) {
          activeTrackers.add(status.getTrackerName());
        } else {
          blacklistedTrackers.add(status.getTrackerName());
        }
      }
    }
    List<List<String>> result = new ArrayList<List<String>>(2);
    result.add(activeTrackers);
    result.add(blacklistedTrackers);
    return result;
  }
  
  /**
   * Get the blacklisted task tracker statuses in the cluster
   *  
   * @return {@link Collection} of blacklisted {@link TaskTrackerStatus} 
   */
  // This method is synchronized to make sure that the locking order 
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers 
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public Collection<TaskTrackerStatus> blacklistedTaskTrackers() {
    Collection<TaskTrackerStatus> blacklistedTrackers = 
      new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus(); 
        if (faultyTrackers.isBlacklisted(status.getHost())) {
          blacklistedTrackers.add(status);
        }
      }
    }    
    return blacklistedTrackers;
  }

  synchronized int getFaultCount(String hostName) {
    return faultyTrackers.getFaultCount(hostName);
  }
  
  /**
   * Get the number of blacklisted trackers across all the jobs
   * 
   * @return
   */
  int getBlacklistedTrackerCount() {
    return faultyTrackers.numBlacklistedTrackers;
  }

  /**
   * Whether the tracker is blacklisted or not
   * 
   * @param trackerID
   * 
   * @return true if blacklisted, false otherwise
   */
  synchronized public boolean isBlacklisted(String trackerID) {
    TaskTrackerStatus status = getTaskTrackerStatus(trackerID);
    if (status != null) {
      return faultyTrackers.isBlacklisted(status.getHost());
    }
    return false;
  }
  
  // lock to taskTrackers should hold JT lock first.
  synchronized public TaskTrackerStatus getTaskTrackerStatus(String trackerID) {
    TaskTracker taskTracker;
    synchronized (taskTrackers) {
      taskTracker = taskTrackers.get(trackerID);
    }
    return (taskTracker == null) ? null : taskTracker.getStatus();
  }

  // lock to taskTrackers should hold JT lock first.
  synchronized public TaskTracker getTaskTracker(String trackerID) {
    synchronized (taskTrackers) {
      return taskTrackers.get(trackerID);
    }
  }

  JobTrackerStatistics getStatistics() {
    return statistics;
  }
  /**
   * Adds a new node to the jobtracker. It involves adding it to the expiry
   * thread and adding it for resolution
   * 
   * Assumes JobTracker, taskTrackers and trackerExpiryQueue are locked on entry
   * 
   * @param status Task Tracker's status
   */
  void addNewTracker(TaskTracker taskTracker) {
    TaskTrackerStatus status = taskTracker.getStatus();
    trackerExpiryQueue.add(status);

    //  Register the tracker if its not registered
    String hostname = status.getHost();
    if (getNode(status.getTrackerName()) == null) {
      // Making the network location resolution inline .. 
      resolveAndAddToTopology(hostname);
    }

    // add it to the set of tracker per host
    Set<TaskTracker> trackers = hostnameToTaskTracker.get(hostname);
    if (trackers == null) {
      trackers = Collections.synchronizedSet(new HashSet<TaskTracker>());
      hostnameToTaskTracker.put(hostname, trackers);
    }
    statistics.taskTrackerAdded(status.getTrackerName());
    getInstrumentation().addTrackers(1);
    LOG.info("Adding tracker " + status.getTrackerName() + " to host " 
             + hostname);
    trackers.add(taskTracker);
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
    Node node = null;
    synchronized (nodesAtMaxLevel) {
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
  
  public int getNumberOfUniqueHosts() {
    return uniqueHostsMap.size();
  }
  
  public void addJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.add(listener);
  }

  public void removeJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.remove(listener);
  }
  
  // Update the listeners about the job
  // Assuming JobTracker is locked on entry.
  void updateJobInProgressListeners(JobChangeEvent event) {
    for (JobInProgressListener listener : jobInProgressListeners) {
      listener.jobUpdated(event);
    }
  }
  
  /**
   * Return the {@link QueueManager} associated with the JobTracker.
   */
  public QueueManager getQueueManager() {
    return queueManager;
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
                                                  boolean restarted,
                                                  boolean initialContact,
                                                  boolean acceptNewTasks, 
                                                  short responseId) 
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got heartbeat from: " + status.getTrackerName() + 
                " (restarted: " + restarted + 
                " initialContact: " + initialContact + 
                " acceptNewTasks: " + acceptNewTasks + ")" +
                " with responseId: " + responseId);
    }

    // Make sure heartbeat is from a tasktracker allowed by the jobtracker.
    if (!acceptTaskTracker(status)) {
      throw new DisallowedTaskTrackerException(status);
    }

    // First check if the last heartbeat response got through
    String trackerName = status.getTrackerName();
    long now = clock.getTime();
    boolean isBlacklisted = false;
    if (restarted) {
      faultyTrackers.markTrackerHealthy(status.getHost());
    } else {
      isBlacklisted = 
        faultyTrackers.shouldAssignTasksToTracker(status.getHost(), now);
    }
    
    HeartbeatResponse prevHeartbeatResponse =
      trackerToHeartbeatResponseMap.get(trackerName);

    if (initialContact != true) {
      // If this isn't the 'initial contact' from the tasktracker,
      // there is something seriously wrong if the JobTracker has
      // no record of the 'previous heartbeat'; if so, ask the 
      // tasktracker to re-initialize itself.
      if (prevHeartbeatResponse == null) {
        // This is the first heartbeat from the old tracker to the newly 
        // started JobTracker
        
        // Jobtracker might have restarted but no recovery is needed
        // otherwise this code should not be reached
        LOG.warn("Serious problem, cannot find record of 'previous' " +
                 "heartbeat for '" + trackerName + 
                 "'; reinitializing the tasktracker");
        return new HeartbeatResponse(responseId, 
            new TaskTrackerAction[] {new ReinitTrackerAction()});
      
      } else {
                
        // It is completely safe to not process a 'duplicate' heartbeat from a 
        // {@link TaskTracker} since it resends the heartbeat when rpcs are 
        // lost see {@link TaskTracker.transmitHeartbeat()};
        // acknowledge it by re-sending the previous response to let the 
        // {@link TaskTracker} go forward. 
        if (prevHeartbeatResponse.getResponseId() != responseId) {
          LOG.info("Ignoring 'duplicate' heartbeat from '" + 
              trackerName + "'; resending the previous 'lost' response");
          return prevHeartbeatResponse;
        }
      }
    }
      
    // Process this heartbeat 
    short newResponseId = (short)(responseId + 1);
    status.setLastSeen(now);
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
    isBlacklisted = faultyTrackers.isBlacklisted(status.getHost());
    // Check for new tasks to be executed on the tasktracker
    if (acceptNewTasks && !isBlacklisted) {
      TaskTrackerStatus taskTrackerStatus = getTaskTrackerStatus(trackerName) ;
      if (taskTrackerStatus == null) {
        LOG.warn("Unknown task tracker polling; ignoring: " + trackerName);
      } else {
        List<Task> tasks = getSetupAndCleanupTasks(taskTrackerStatus);
        if (tasks == null ) {
          tasks = taskScheduler.assignTasks(taskTrackers.get(trackerName));
        }
        if (tasks != null) {
          for (Task task : tasks) {
            expireLaunchingTasks.addNewTask(task.getTaskID());
            if (LOG.isDebugEnabled()) {
              LOG.debug(trackerName + " -> LaunchTask: " + task.getTaskID());
            }
            actions.add(new LaunchTaskAction(task));
          }
        }
      }
    }
      
    // Check for tasks to be killed
    List<TaskTrackerAction> killTasksList = getTasksToKill(trackerName);
    if (killTasksList != null) {
      actions.addAll(killTasksList);
    }
     
    // Check for jobs to be killed/cleanedup
    List<TaskTrackerAction> killJobsList = getJobsForCleanup(trackerName);
    if (killJobsList != null) {
      actions.addAll(killJobsList);
    }

    // Check for tasks whose outputs can be saved
    List<TaskTrackerAction> commitTasksList = getTasksToSave(status);
    if (commitTasksList != null) {
      actions.addAll(commitTasksList);
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
   * Heartbeat interval is incremented by 1 second for every 100 nodes by default. 
   * @return next heartbeat interval.
   */
  public int getNextHeartbeatInterval() {
    // get the no of task trackers
    int clusterSize = getClusterStatus().getTaskTrackers();
    int heartbeatInterval =  Math.max(
                                (int)(1000 * HEARTBEATS_SCALING_FACTOR *
                                      Math.ceil((double)clusterSize / 
                                                NUM_HEARTBEATS_IN_SECOND)),
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
  boolean updateTaskTrackerStatus(String trackerName,
                                          TaskTrackerStatus status) {
    TaskTracker tt = getTaskTracker(trackerName);
    TaskTrackerStatus oldStatus = (tt == null) ? null : tt.getStatus();
    if (oldStatus != null) {
      totalMaps -= oldStatus.countMapTasks();
      totalReduces -= oldStatus.countReduceTasks();
      occupiedMapSlots -= oldStatus.countOccupiedMapSlots();
      occupiedReduceSlots -= oldStatus.countOccupiedReduceSlots();
      getInstrumentation().decRunningMaps(oldStatus.countMapTasks());
      getInstrumentation().decRunningReduces(oldStatus.countReduceTasks());
      getInstrumentation().decOccupiedMapSlots(oldStatus.countOccupiedMapSlots());
      getInstrumentation().decOccupiedReduceSlots(oldStatus.countOccupiedReduceSlots());
      if (!faultyTrackers.isBlacklisted(oldStatus.getHost())) {
        int mapSlots = oldStatus.getMaxMapSlots();
        totalMapTaskCapacity -= mapSlots;
        int reduceSlots = oldStatus.getMaxReduceSlots();
        totalReduceTaskCapacity -= reduceSlots;
      }
      if (status == null) {
        taskTrackers.remove(trackerName);
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(oldStatus.getHost());
        if (numTaskTrackersInHost != null) {
          numTaskTrackersInHost --;
          if (numTaskTrackersInHost > 0)  {
            uniqueHostsMap.put(oldStatus.getHost(), numTaskTrackersInHost);
          }
          else {
            uniqueHostsMap.remove(oldStatus.getHost());
          }
        }
      }
    }
    if (status != null) {
      totalMaps += status.countMapTasks();
      totalReduces += status.countReduceTasks();
      occupiedMapSlots += status.countOccupiedMapSlots();
      occupiedReduceSlots += status.countOccupiedReduceSlots();
      getInstrumentation().addRunningMaps(status.countMapTasks());
      getInstrumentation().addRunningReduces(status.countReduceTasks());
      getInstrumentation().addOccupiedMapSlots(status.countOccupiedMapSlots());
      getInstrumentation().addOccupiedReduceSlots(status.countOccupiedReduceSlots());
      if (!faultyTrackers.isBlacklisted(status.getHost())) {
        int mapSlots = status.getMaxMapSlots();
        totalMapTaskCapacity += mapSlots;
        int reduceSlots = status.getMaxReduceSlots();
        totalReduceTaskCapacity += reduceSlots;
      }
      boolean alreadyPresent = false;
      TaskTracker taskTracker = taskTrackers.get(trackerName);
      if (taskTracker != null) {
        alreadyPresent = true;
      } else {
        taskTracker = new TaskTracker(trackerName);
      }
      
      taskTracker.setStatus(status);
      taskTrackers.put(trackerName, taskTracker);
      
      if (LOG.isDebugEnabled()) {
        int runningMaps = 0, runningReduces = 0;
        int commitPendingMaps = 0, commitPendingReduces = 0;
        int unassignedMaps = 0, unassignedReduces = 0;
        int miscMaps = 0, miscReduces = 0;
        List<TaskStatus> taskReports = status.getTaskReports();
        for (Iterator<TaskStatus> it = taskReports.iterator(); it.hasNext();) {
          TaskStatus ts =  it.next();
          boolean isMap = ts.getIsMap();
          TaskStatus.State state = ts.getRunState();
          if (state == TaskStatus.State.RUNNING) {
            if (isMap) { ++runningMaps; }
            else { ++runningReduces; }
          } else if (state == TaskStatus.State.UNASSIGNED) {
            if (isMap) { ++unassignedMaps; }
            else { ++unassignedReduces; }
          } else if (state == TaskStatus.State.COMMIT_PENDING) {
            if (isMap) { ++commitPendingMaps; }
            else { ++commitPendingReduces; }
          } else {
            if (isMap) { ++miscMaps; } 
            else { ++miscReduces; } 
          }
        }
        LOG.debug(trackerName + ": Status -" +
                  " running(m) = " + runningMaps + 
                  " unassigned(m) = " + unassignedMaps + 
                  " commit_pending(m) = " + commitPendingMaps +
                  " misc(m) = " + miscMaps +
                  " running(r) = " + runningReduces + 
                  " unassigned(r) = " + unassignedReduces + 
                  " commit_pending(r) = " + commitPendingReduces +
                  " misc(r) = " + miscReduces); 
      }

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
    getInstrumentation().setMapSlots(totalMapTaskCapacity);
    getInstrumentation().setReduceSlots(totalReduceTaskCapacity);
    return oldStatus != null;
  }
  
  // Increment the number of reserved slots in the cluster.
  // This method assumes the caller has JobTracker lock.
  void incrementReservations(TaskType type, int reservedSlots) {
    if (type.equals(TaskType.MAP)) {
      reservedMapSlots += reservedSlots;
    } else if (type.equals(TaskType.REDUCE)) {
      reservedReduceSlots += reservedSlots;
    }
  }

  // Decrement the number of reserved slots in the cluster.
  // This method assumes the caller has JobTracker lock.
  void decrementReservations(TaskType type, int reservedSlots) {
    if (type.equals(TaskType.MAP)) {
      reservedMapSlots -= reservedSlots;
    } else if (type.equals(TaskType.REDUCE)) {
      reservedReduceSlots -= reservedSlots;
    }
  }
  
  private void updateNodeHealthStatus(TaskTrackerStatus trackerStatus) {
    TaskTrackerHealthStatus status = trackerStatus.getHealthStatus();
    synchronized (faultyTrackers) {
      faultyTrackers.setNodeHealthStatus(trackerStatus.getHost(), 
          status.isNodeHealthy(), status.getHealthReport());
    }
  }
    
  /**
   * Process incoming heartbeat messages from the task trackers.
   */
  synchronized boolean processHeartbeat(
                                 TaskTrackerStatus trackerStatus, 
                                 boolean initialContact) {
    
    getInstrumentation().heartbeat();

    String trackerName = trackerStatus.getTrackerName();

    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        boolean seenBefore = updateTaskTrackerStatus(trackerName,
                                                     trackerStatus);
        TaskTracker taskTracker = getTaskTracker(trackerName);
        if (initialContact) {
          // If it's first contact, then clear out 
          // any state hanging around
          if (seenBefore) {
            lostTaskTracker(taskTracker);
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
          // if this is lost tracker that came back now, and if it blacklisted
          // increment the count of blacklisted trackers in the cluster
          if (isBlacklisted(trackerName)) {
            faultyTrackers.incrBlackListedTrackers(1);
          }
          addNewTracker(taskTracker);
        }
      }
    }

    updateTaskStatuses(trackerStatus);
    updateNodeHealthStatus(trackerStatus);
    
    return true;
  }

  /**
   * A tracker wants to know if any of its Tasks have been
   * closed (because the job completed, whether successfully or not)
   */
  synchronized List<TaskTrackerAction> getTasksToKill(String taskTracker) {
    
    Set<TaskAttemptID> taskIds = trackerToTaskMap.get(taskTracker);
    List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
    if (taskIds != null) {
      for (TaskAttemptID killTaskId : taskIds) {
        TaskInProgress tip = taskidToTIPMap.get(killTaskId);
        if (tip == null) {
          continue;
        }
        if (tip.shouldClose(killTaskId)) {
          // 
          // This is how the JobTracker ends a task at the TaskTracker.
          // It may be successfully completed, or may be killed in
          // mid-execution.
          //
          if (!tip.getJob().isComplete()) {
            killList.add(new KillTaskAction(killTaskId));
            if (LOG.isDebugEnabled()) {
              LOG.debug(taskTracker + " -> KillTaskAction: " + killTaskId);
            }
          }
        }
      }
    }
    
    // add the stray attempts for uninited jobs
    synchronized (trackerToTasksToCleanup) {
      Set<TaskAttemptID> set = trackerToTasksToCleanup.remove(taskTracker);
      if (set != null) {
        for (TaskAttemptID id : set) {
          killList.add(new KillTaskAction(id));
        }
      }
    }
    return killList;
  }

  /**
   * Add a job to cleanup for the tracker.
   */
  private void addJobForCleanup(JobID id) {
    for (String taskTracker : taskTrackers.keySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Marking job " + id
                  + " for cleanup by tracker " + taskTracker);
      }
      synchronized (trackerToJobsToCleanup) {
        Set<JobID> jobsToKill = trackerToJobsToCleanup.get(taskTracker);
        if (jobsToKill == null) {
          jobsToKill = new HashSet<JobID>();
          trackerToJobsToCleanup.put(taskTracker, jobsToKill);
        }
        jobsToKill.add(id);
      }
    }
  }
  
  /**
   * A tracker wants to know if any job needs cleanup because the job completed.
   */
  private List<TaskTrackerAction> getJobsForCleanup(String taskTracker) {
    Set<JobID> jobs = null;
    synchronized (trackerToJobsToCleanup) {
      jobs = trackerToJobsToCleanup.remove(taskTracker);
    }
    if (jobs != null) {
      // prepare the actions list
      List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
      for (JobID killJobId : jobs) {
        killList.add(new KillJobAction(killJobId));
        if (LOG.isDebugEnabled()) {
          LOG.debug(taskTracker + " -> KillJobAction: " + killJobId);
        }
      }

      return killList;
    }
    return null;
  }

  /**
   * A tracker wants to know if any of its Tasks can be committed 
   */
  synchronized List<TaskTrackerAction> getTasksToSave(
                                                 TaskTrackerStatus tts) {
    List<TaskStatus> taskStatuses = tts.getTaskReports();
    if (taskStatuses != null) {
      List<TaskTrackerAction> saveList = new ArrayList<TaskTrackerAction>();
      for (TaskStatus taskStatus : taskStatuses) {
        if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING) {
          TaskAttemptID taskId = taskStatus.getTaskID();
          TaskInProgress tip = taskidToTIPMap.get(taskId);
          if (tip == null) {
            continue;
          }
          if (tip.shouldCommit(taskId)) {
            saveList.add(new CommitTaskAction(taskId));
            if (LOG.isDebugEnabled()) {
              LOG.debug(tts.getTrackerName() + 
                        " -> CommitTaskAction: " + taskId);
            }
          }
        }
      }
      return saveList;
    }
    return null;
  }
  
  // returns cleanup tasks first, then setup tasks.
  synchronized List<Task> getSetupAndCleanupTasks(
    TaskTrackerStatus taskTracker) throws IOException {
    int maxMapTasks = taskTracker.getMaxMapSlots();
    int maxReduceTasks = taskTracker.getMaxReduceSlots();
    int numMaps = taskTracker.countOccupiedMapSlots();
    int numReduces = taskTracker.countOccupiedReduceSlots();
    int numTaskTrackers = getClusterStatus().getTaskTrackers();
    int numUniqueHosts = getNumberOfUniqueHosts();

    Task t = null;
    synchronized (jobs) {
      if (numMaps < maxMapTasks) {
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainTaskCleanupTask(taskTracker, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                  numUniqueHosts, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
      }
      if (numReduces < maxReduceTasks) {
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainTaskCleanupTask(taskTracker, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
      }
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

  /**
   * Returns a handle to the JobTracker's Configuration
   */
  public JobConf getConf() {
    return conf;
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
   * @deprecated use {@link #getNewJobID()} instead
   */
  @Deprecated
  public JobID getNewJobId() throws IOException {
    return JobID.downgrade(getNewJobID());
  }

  /**
   * Allocates a new JobId string.
   */
  public org.apache.hadoop.mapreduce.JobID getNewJobID() throws IOException {
    return new org.apache.hadoop.mapreduce.JobID
      (getTrackerIdentifier(), nextJobId.getAndIncrement());
  }

  /**
   * JobTracker.submitJob() kicks off a new job.  
   *
   * Create a 'JobInProgress' object, which contains both JobProfile
   * and JobStatus.  Those two sub-objects are sometimes shipped outside
   * of the JobTracker.  But JobInProgress adds info that's useful for
   * the JobTracker alone.
   */
  public synchronized 
  org.apache.hadoop.mapreduce.JobStatus 
    submitJob(org.apache.hadoop.mapreduce.JobID jobId, String jobSubmitDir,
              Credentials ts
              ) throws IOException, InterruptedException {
    return submitJob(JobID.downgrade(jobId), jobSubmitDir, ts);
  }
  
  /**
   * JobTracker.submitJob() kicks off a new job.  
   *
   * Create a 'JobInProgress' object, which contains both JobProfile
   * and JobStatus.  Those two sub-objects are sometimes shipped outside
   * of the JobTracker.  But JobInProgress adds info that's useful for
   * the JobTracker alone.
   * @deprecated Use 
   * {@link #submitJob(org.apache.hadoop.mapreduce.JobID, String, Credentials)}
   *  instead
   */
  @Deprecated
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
      throws IOException, InterruptedException {

    return submitJob(jobId, 0, UserGroupInformation.getCurrentUser(), 
                     jobSubmitDir, false, ts);
  }

  /**
   * Submits either a new job or a job from an earlier run.
   */
  private JobStatus submitJob(org.apache.hadoop.mapreduce.JobID jobID, 
			      int restartCount, UserGroupInformation ugi, 
			      String jobSubmitDir, boolean recovered, Credentials ts
			      )
      throws IOException, InterruptedException {

    JobID jobId = null;

    JobInfo jobInfo;

    synchronized (this) {
      jobId = JobID.downgrade(jobID);
      if (jobs.containsKey(jobId)) {
        // job already running, don't start twice
        return jobs.get(jobId).getStatus();
      }

      // the conversion from String to Text for the UGI's username will
      // not be required when we have the UGI to return us the username as
      // Text.
      jobInfo =
          new JobInfo(jobId, new Text(ugi.getShortUserName()), new Path(
              jobSubmitDir));
    }

    // Create the JobInProgress, temporarily unlock the JobTracker since
    // we are about to copy job.xml from HDFSJobInProgress
    JobInProgress job =
        new JobInProgress(this, this.conf, restartCount, jobInfo, ts);

    synchronized (this) {
      try {
        checkQueueValidity(job);
      } catch(IOException ioe) {
        LOG.error("Queue given for job " + job.getJobID() + " is not valid: " + ioe);
        throw ioe;
      }
      try {
        aclsManager.checkAccess(job, ugi, Operation.SUBMIT_JOB);
      } catch (AccessControlException ace) {
        LOG.warn("Access denied for user " + job.getJobConf().getUser()
            + ". Ignoring job " + jobId, ace);
        throw ace;
      }

      // Check the job if it cannot run in the cluster because of invalid memory
      // requirements.
      try {
        checkMemoryRequirements(job);
      } catch (IOException ioe) {
        throw ioe;
      }

      if (!recovered) {
        // Store the information in a file so that the job can be recovered
        // later (if at all)
        Path jobDir = getSystemDirectoryForJob(jobId);
        FileSystem.mkdirs(fs, jobDir, new FsPermission(SYSTEM_DIR_PERMISSION));
        FSDataOutputStream out = fs.create(getSystemFileForJob(jobId));
        jobInfo.write(out);
        out.close();
      }
      return addJob(jobId, job);
    }
  }

  /**
   * Adds a job to the jobtracker. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * @param jobId The id for the job submitted which needs to be added
   */
  synchronized JobStatus addJob(JobID jobId, JobInProgress job) {
    totalSubmissions++;

    synchronized (jobs) {
      synchronized (taskScheduler) {
        jobs.put(job.getProfile().getJobID(), job);
        for (JobInProgressListener listener : jobInProgressListeners) {
          try {
            listener.jobAdded(job);
          } catch (IOException ioe) {
            LOG.warn("Failed to add and so skipping the job : "
                + job.getJobID() + ". Exception : " + ioe);
          }
        }
      }
    }
    myInstrumentation.submitJob(job.getJobConf(), jobId);
    LOG.info("Job " + jobId + " added successfully for user '" 
             + job.getJobConf().getUser() + "' to queue '" 
             + job.getJobConf().getQueueName() + "'");

    return job.getStatus();
  }

  /**
   * For a JobInProgress that is being submitted, check whether 
   * queue that the job has been submitted to exists and is RUNNING.
   * @param job The JobInProgress object being submitted.
   * @throws IOException
   */
  public void checkQueueValidity(JobInProgress job) throws IOException {
    String queue = job.getProfile().getQueueName();
    if (!(queueManager.getLeafQueueNames().contains(queue))) {
        throw new IOException("Queue \"" + queue + "\" does not exist");
    }

    // check if queue is RUNNING
    if (!queueManager.isRunning(queue)) {
        throw new IOException("Queue \"" + queue + "\" is not running");
    }
  }

  /**
   * Are ACLs for authorization checks enabled on the MR cluster ?
   *
   * @return true if ACLs(job acls and queue acls) are enabled
   */
  boolean areACLsEnabled() {
    return conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
  }

  /**@deprecated use {@link #getClusterStatus(boolean)}*/
  @Deprecated
  public synchronized ClusterStatus getClusterStatus() {
    return getClusterStatus(false);
  }

  public synchronized ClusterStatus getClusterStatus(boolean detailed) {
    synchronized (taskTrackers) {
      if (detailed) {
        List<List<String>> trackerNames = taskTrackerNames();
        Collection<BlackListInfo> blackListedTrackers = getBlackListedTrackers();
        return new ClusterStatus(trackerNames.get(0),
            blackListedTrackers,
            tasktrackerExpiryInterval,
            totalMaps,
            totalReduces,
            totalMapTaskCapacity,
            totalReduceTaskCapacity, 
            state, getExcludedNodes().size()
            );
      } else {
        return new ClusterStatus(taskTrackers.size() - 
            getBlacklistedTrackerCount(),
            getBlacklistedTrackerCount(),
            tasktrackerExpiryInterval,
            totalMaps,
            totalReduces,
            totalMapTaskCapacity,
            totalReduceTaskCapacity, 
            state, getExcludedNodes().size());          
      }
    }
  }
  
  public synchronized ClusterMetrics getClusterMetrics() {
    return new ClusterMetrics(totalMaps,
      totalReduces, occupiedMapSlots, occupiedReduceSlots,
      reservedMapSlots, reservedReduceSlots,
      totalMapTaskCapacity, totalReduceTaskCapacity,
      totalSubmissions,
      taskTrackers.size() - getBlacklistedTrackerCount(), 
      getBlacklistedTrackerCount(), getExcludedNodes().size()) ;
  }

  /**
   * @deprecated Use {@link #getJobTrackerStatus()} instead.
   */
  @Deprecated
  public org.apache.hadoop.mapreduce.server.jobtracker.State 
      getJobTrackerState() {
    return org.apache.hadoop.mapreduce.server.jobtracker.
      State.valueOf(state.name());
  }
 
  @Override
  public JobTrackerStatus getJobTrackerStatus() {
    return JobTrackerStatus.valueOf(state.name());
  }
  
  public long getTaskTrackerExpiryInterval() {
    return tasktrackerExpiryInterval;
  }
  
  /** 
   * Get all active trackers in cluster. 
   * @return array of TaskTrackerInfo
   */
  public TaskTrackerInfo[] getActiveTrackers() 
  throws IOException, InterruptedException {
    List<String> activeTrackers = taskTrackerNames().get(0);
    TaskTrackerInfo[] info = new TaskTrackerInfo[activeTrackers.size()];
    for (int i = 0; i < activeTrackers.size(); i++) {
      info[i] = new TaskTrackerInfo(activeTrackers.get(i));
    }
    return info;
  }

  /** 
   * Get all blacklisted trackers in cluster. 
   * @return array of TaskTrackerInfo
   */
  public TaskTrackerInfo[] getBlacklistedTrackers() 
  throws IOException, InterruptedException {
    Collection<BlackListInfo> blackListed = getBlackListedTrackers();
    TaskTrackerInfo[] info = new TaskTrackerInfo[blackListed.size()];
    int i = 0;
    for (BlackListInfo binfo : blackListed) {
      info[i++] = new TaskTrackerInfo(binfo.getTrackerName(),
        binfo.getReasonForBlackListing(), binfo.getBlackListReport());
    }
    return info;
  }

  /**
   * @see ClientProtocol#killJob(org.apache.hadoop.mapreduce.JobID)
   */
  @Override
  public synchronized void killJob(org.apache.hadoop.mapreduce.JobID jobid) 
      throws IOException {
    killJob(JobID.downgrade(jobid));
  }
  
  /**
   * @deprecated Use {@link #killJob(org.apache.hadoop.mapreduce.JobID)} instead 
   */
  @Deprecated
  public synchronized void killJob(JobID jobid) throws IOException {
    if (null == jobid) {
      LOG.info("Null jobid object sent to JobTracker.killJob()");
      return;
    }
    
    JobInProgress job = jobs.get(jobid);
    
    if (null == job) {
      LOG.info("killJob(): JobId " + jobid.toString() + " is not a valid job");
      return;
    }

    // check both queue-level and job-level access
    aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
        Operation.KILL_JOB);

    killJob(job);
  }

  private synchronized void killJob(JobInProgress job) {
    LOG.info("Killing job " + job.getJobID());
    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    job.kill();
    
    // Inform the listeners if the job is killed
    // Note : 
    //   If the job is killed in the PREP state then the listeners will be 
    //   invoked
    //   If the job is killed in the RUNNING state then cleanup tasks will be 
    //   launched and the updateTaskStatuses() will take care of it
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()
        && newStatus.getRunState() == JobStatus.KILLED) {
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
            newStatus);
      updateJobInProgressListeners(event);
    }
  }

  /**
   * Initialize a job and inform the listeners about a state change, if any.
   * Other components in the framework should use this api to initialize a job.
   */
  public void initJob(JobInProgress job) {
    if (null == job) {
      LOG.info("Init on null job is not valid");
      return;
    }
	        
    try {
      JobStatus prevStatus = (JobStatus)job.getStatus().clone();
      LOG.info("Initializing " + job.getJobID());
      job.initTasks();
      // Here the job *should* be in the PREP state.
      // From here there are 3 ways :
      //  - job requires setup : the job remains in PREP state and 
      //    setup is launched to move the job in RUNNING state
      //  - job is complete (no setup required and no tasks) : complete 
      //    the job and move it to SUCCEEDED
      //  - job has tasks but doesnt require setup : make the job RUNNING.
      if (job.isJobEmpty()) { // is the job empty?
        completeEmptyJob(job); // complete it
      } else if (!job.isSetupCleanupRequired()) { // setup/cleanup not required
        job.completeSetup(); // complete setup and make job running
      }
      // Inform the listeners if the job state has changed
      // Note : 
      //   If job does not require setup, job state will be RUNNING
      //   If job is configured with 0 maps, 0 reduces and no setup-cleanup then 
      //   the job state will be SUCCEEDED
      //   otherwise, job state is PREP.
      JobStatus newStatus = (JobStatus)job.getStatus().clone();
      if (prevStatus.getRunState() != newStatus.getRunState()) {
        JobStatusChangeEvent event = 
          new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
              newStatus);
        synchronized (JobTracker.this) {
          updateJobInProgressListeners(event);
        }
      }
    } catch (KillInterruptedException kie) {
      //   If job was killed during initialization, job state will be KILLED
      LOG.error("Job initialization interrupted :\n" +
          StringUtils.stringifyException(kie));
      killJob(job);
    } catch (Throwable t) {
      //    If the job initialization is failed, job state will be FAILED
      LOG.error("Job initialization failed:\n" +
          StringUtils.stringifyException(t));
      failJob(job);
    }
  }

  // This simply marks the job as completed. Note that the caller is responsible
  // for raising events.
  private synchronized void completeEmptyJob(JobInProgress job) {
    job.completeEmptyJob();
  }
  
  /**
   * Fail a job and inform the listeners. Other components in the framework 
   * should use this to fail a job.
   */
  public synchronized void failJob(JobInProgress job) {
    if (null == job) {
      LOG.info("Fail on null job is not valid");
      return;
    }
         
    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    LOG.info("Failing job " + job.getJobID());
    job.fail();
     
    // Inform the listeners if the job state has changed
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()) {
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
            newStatus);
      updateJobInProgressListeners(event);
    }
  }

  /**
   * Set the priority of a job
   * @param jobid
   * @param priority
   * @throws IOException
   */
  public synchronized void setJobPriority(org.apache.hadoop.mapreduce.JobID 
      jobid, String priority) throws IOException {
    setJobPriority(JobID.downgrade(jobid), priority);
  }
  /**
   * Set the priority of a job
   * @param jobid id of the job
   * @param priority new priority of the job
   * @deprecated Use 
   * {@link #setJobPriority(org.apache.hadoop.mapreduce.JobID, String)} instead
   */
  @Deprecated
  public synchronized void setJobPriority(JobID jobid, 
                                          String priority)
                                          throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (null == job) {
        LOG.info("setJobPriority(): JobId " + jobid.toString()
            + " is not a valid job");
        return;
    }
    JobPriority newPriority = JobPriority.valueOf(priority);
    setJobPriority(jobid, newPriority);
  }
                           
  void storeCompletedJob(JobInProgress job) {
    //persists the job info in DFS
    completedJobStatusStore.store(job);
  }

  public JobProfile getJobProfile(org.apache.hadoop.mapreduce.JobID jobid) {
    return getJobProfile(JobID.downgrade(jobid));
  }
  
  /**
   * Check if the <code>job</code> has been initialized.
   * 
   * @param job {@link JobInProgress} to be checked
   * @return <code>true</code> if the job has been initialized,
   *         <code>false</code> otherwise
   */
  private boolean isJobInited(JobInProgress job) {
    return job.inited(); 
  }
  

  /**
   * @deprecated Use {@link #getJobProfile(org.apache.hadoop.mapreduce.JobID)} 
   * instead
   */
  @Deprecated
  public JobProfile getJobProfile(JobID jobid) {
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        // Safe to call JobInProgress.getProfile while holding the lock
        // on the JobTracker since it isn't a synchronized method
        return job.getProfile();
      } 
    }
    return completedJobStatusStore.readJobProfile(jobid);
  }

  /**
   * see {@link ClientProtocol#getJobStatus(org.apache.hadoop.mapreduce.JobID)}
   */
  @Override
  public JobStatus getJobStatus(org.apache.hadoop.mapreduce.JobID jobid) {
    return getJobStatus(JobID.downgrade(jobid));
  }

  /**
   * @deprecated Use 
   * {@link #getJobStatus(org.apache.hadoop.mapreduce.JobID)} instead
   */
  @Deprecated
  public JobStatus getJobStatus(JobID jobid) {
    if (null == jobid) {
      LOG.warn("JobTracker.getJobStatus() cannot get status for null jobid");
      return null;
    }
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        // Safe to call JobInProgress.getStatus while holding the lock
        // on the JobTracker since it isn't a synchronized method
        return job.getStatus();
      } else {
        JobStatus status = retireJobs.get(jobid);
        if (status != null) {
          return status;
        }
      }
    }
    return completedJobStatusStore.readJobStatus(jobid);
  }

  private static final org.apache.hadoop.mapreduce.Counters EMPTY_COUNTERS
      = new org.apache.hadoop.mapreduce.Counters();

  /**
   * see
   * {@link ClientProtocol#getJobCounters(org.apache.hadoop.mapreduce.JobID)}
   * 
   * @throws IOException
   * @throws AccessControlException
   */
  @Override
  public org.apache.hadoop.mapreduce.Counters getJobCounters(
      org.apache.hadoop.mapreduce.JobID jobid)
      throws AccessControlException, IOException {

    JobID oldJobID = JobID.downgrade(jobid);
    JobInProgress job;
    synchronized (this) {
      job = jobs.get(oldJobID);
    }

    if (job != null) {
      // check the job-access
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
                              Operation.VIEW_JOB_COUNTERS);

      if (!isJobInited(job)) {
        return EMPTY_COUNTERS;
      }

      Counters counters = job.getCounters();
      if (counters != null) {
        return new org.apache.hadoop.mapreduce.Counters(counters);
      }
      return null;
    } 

    Counters counters = completedJobStatusStore.readCounters(oldJobID);
    if (counters != null) {
      return new org.apache.hadoop.mapreduce.Counters(counters);
    }
    return null;
  }
  
  /**
   * @deprecated Use 
   * {@link #getJobCounters(org.apache.hadoop.mapreduce.JobID)} instead
   */
  @Deprecated
  public Counters getJobCounters(JobID jobid) {
    try {
      return Counters.downgrade(
          getJobCounters((org.apache.hadoop.mapreduce.JobID) jobid));
    } catch (AccessControlException e) {
      return null;
    } catch (IOException e) {
      return null;
    } 
  }

  private static final TaskReport[] EMPTY_TASK_REPORTS = new TaskReport[0];

  /**
   * @param jobid
   * @return array of TaskReport
   * @deprecated Use 
   * {@link #getTaskReports(org.apache.hadoop.mapreduce.JobID, TaskType)} 
   * instead
   */
  @Deprecated
  public synchronized TaskReport[] getMapTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
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

  /**
   * @param jobid
   * @return array of TaskReport
   * @deprecated Use 
   * {@link #getTaskReports(org.apache.hadoop.mapreduce.JobID, TaskType)} 
   * instead
   */
  @Deprecated
  public synchronized TaskReport[] getReduceTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
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

  /**
   * @param jobid
   * @return array of TaskReport
   * @deprecated Use 
   * {@link #getTaskReports(org.apache.hadoop.mapreduce.JobID, TaskType)} 
   * instead
   */
  @Deprecated
  public synchronized TaskReport[] getCleanupTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeTasks = job.reportCleanupTIPs(true);
      for (Iterator<TaskInProgress> it = completeTasks.iterator();
           it.hasNext();) {
        TaskInProgress tip = it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteTasks = job.reportCleanupTIPs(false);
      for (Iterator<TaskInProgress> it = incompleteTasks.iterator(); 
           it.hasNext();) {
        TaskInProgress tip = it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  
  }

  /**
   * @param jobid
   * @return array of TaskReport
   * @deprecated Use 
   * {@link #getTaskReports(org.apache.hadoop.mapreduce.JobID, TaskType)} 
   * instead
   */
  @Deprecated
  public synchronized TaskReport[] getSetupTaskReports(JobID jobid) {
    JobInProgress job = jobs.get(jobid);
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeTasks = job.reportSetupTIPs(true);
      for (Iterator<TaskInProgress> it = completeTasks.iterator();
           it.hasNext();) {
        TaskInProgress tip =  it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteTasks = job.reportSetupTIPs(false);
      for (Iterator<TaskInProgress> it = incompleteTasks.iterator(); 
           it.hasNext();) {
        TaskInProgress tip =  it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  /**
   * see
   * {@link ClientProtocol#getTaskReports(org.apache.hadoop.mapreduce.JobID, TaskType)}
   * @throws IOException 
   * @throws AccessControlException 
   */
  @Override
  public synchronized TaskReport[] getTaskReports(
      org.apache.hadoop.mapreduce.JobID jobid, TaskType type)
      throws AccessControlException, IOException {

    // Check authorization
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    } else { 
      return EMPTY_TASK_REPORTS;
    }

    switch (type) {
      case MAP :
        return getMapTaskReports(JobID.downgrade(jobid));
      case REDUCE :
        return getReduceTaskReports(JobID.downgrade(jobid));
      case JOB_CLEANUP:
        return getCleanupTaskReports(JobID.downgrade(jobid));
      case JOB_SETUP :
        return getSetupTaskReports(JobID.downgrade(jobid));
    }
    return EMPTY_TASK_REPORTS;
  }

  /* 
   * Returns a list of TaskCompletionEvent for the given job, 
   * starting from fromEventId.
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(
      org.apache.hadoop.mapreduce.JobID jobid, int fromEventId, int maxEvents)
      throws IOException {
    return getTaskCompletionEvents(JobID.downgrade(jobid),
      fromEventId, maxEvents);
  }
  
  /* 
   * Returns a list of TaskCompletionEvent for the given job, 
   * starting from fromEventId.
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getTaskCompletionEvents(java.lang.String, int, int)
   */
  @Deprecated
  public TaskCompletionEvent[] getTaskCompletionEvents(
    JobID jobid, int fromEventId, int maxEvents) throws IOException{

    JobInProgress job = this.jobs.get(jobid);
    if (null != job) {
      return job.inited() ? job.getTaskCompletionEvents(fromEventId, maxEvents)
	  : TaskCompletionEvent.EMPTY_ARRAY;
    }

    return completedJobStatusStore.readJobTaskCompletionEvents(jobid, fromEventId, maxEvents);
  }

  private static final String[] EMPTY_TASK_DIAGNOSTICS = new String[0];

  /**
   * Get the diagnostics for a given task
   * @param taskId the id of the task
   * @return an array of the diagnostic messages
   */
  public synchronized String[] getTaskDiagnostics(
      org.apache.hadoop.mapreduce.TaskAttemptID taskId)  
      throws IOException {
    return getTaskDiagnostics(TaskAttemptID.downgrade(taskId));
  }

  /**
   * Get the diagnostics for a given task
   * @param taskId the id of the task
   * @return an array of the diagnostic messages
   */
  @Deprecated
  public synchronized String[] getTaskDiagnostics(TaskAttemptID taskId)  
    throws IOException {
    List<String> taskDiagnosticInfo = null;
    JobID jobId = taskId.getJobID();
    TaskID tipId = taskId.getTaskID();
    JobInProgress job = jobs.get(jobId);
    if (job != null) {

      // check the access to the job.
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);

      if (isJobInited(job)) {
        TaskInProgress tip = job.getTaskInProgress(tipId);
        if (tip != null) {
          taskDiagnosticInfo = tip.getDiagnosticInfo(taskId);
        }
      }      
    }
    
    return ((taskDiagnosticInfo == null) ? EMPTY_TASK_DIAGNOSTICS :
             taskDiagnosticInfo.toArray(new String[taskDiagnosticInfo.size()]));
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
   * Returns the configured task scheduler for this job tracker.
   * @return the configured task scheduler
   */
  TaskScheduler getTaskScheduler() {
    return taskScheduler;
  }
  
  /**
   * Returns specified TaskInProgress, or null.
   */
  public TaskInProgress getTip(TaskID tipid) {
    JobInProgress job = jobs.get(tipid.getJobID());
    return (job == null ? null : job.getTaskInProgress(tipid));
  }

  /**
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#killTask(org.apache.hadoop.mapreduce.TaskAttemptID,
   *      boolean)
   */
  @Override
  public synchronized boolean killTask(
      org.apache.hadoop.mapreduce.TaskAttemptID taskid,
      boolean shouldFail) throws IOException {
    return killTask(TaskAttemptID.downgrade(taskid), shouldFail);
  }
  
  /** Mark a Task to be killed */
  @Deprecated
  public synchronized boolean killTask(TaskAttemptID taskid, 
      boolean shouldFail) throws IOException {
    TaskInProgress tip = taskidToTIPMap.get(taskid);
    if (tip != null) {

      // check both queue-level and job-level access
      aclsManager.checkAccess(tip.getJob(),
          UserGroupInformation.getCurrentUser(),
          shouldFail ? Operation.FAIL_TASK : Operation.KILL_TASK);

      return tip.killTask(taskid, shouldFail);
    }
    else {
      LOG.info("Kill task attempt failed since task " + taskid + " was not found");
      return false;
    }
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
    return getJobStatus(jobs.values(), true);
  } 

  /**
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getSystemDir()
   */
  public org.apache.hadoop.mapreduce.JobStatus[] getAllJobs() {
    List<JobStatus> list = new ArrayList<JobStatus>();
    list.addAll(Arrays.asList(getJobStatus(jobs.values(),false)));
    list.addAll(retireJobs.getAll());
    return list.toArray(new JobStatus[list.size()]);
  }
    
  /**
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(conf.get(JTConfig.JT_SYSTEM_DIR, "/tmp/hadoop/mapred/system"));
    return fs.makeQualified(sysDir).toString();
  }
  
  /**
   * @throws LoginException 
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getStagingAreaDir()
   */
  public String getStagingAreaDir() throws IOException {
    try {
      final String user =
          UserGroupInformation.getCurrentUser().getShortUserName();
      return getMROwner().doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws Exception {
          Path stagingRootDir = new Path(conf.get(JTConfig.JT_STAGING_AREA_ROOT, 
          "/tmp/hadoop/mapred/staging"));
          FileSystem fs = stagingRootDir.getFileSystem(conf);
          return fs.makeQualified(new Path(stagingRootDir, 
                                      user+"/.staging")).toString();
      }
      });
    } catch(InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /**
   * @see 
   * org.apache.hadoop.mapreduce.protocol.ClientProtocol#getJobHistoryDir()
   */
  public String getJobHistoryDir() {
    return jobHistory.getCompletedJobHistoryLocation().toString();
  }

  /**
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getQueueAdmins(String)
   */
  public AccessControlList getQueueAdmins(String queueName) throws IOException {
	  AccessControlList acl =
		  queueManager.getQueueACL(queueName, QueueACL.ADMINISTER_JOBS);
	  if (acl == null) {
		  acl = new AccessControlList(" ");
	  }
	  return acl;
  }

  ///////////////////////////////////////////////////////////////
  // JobTracker methods
  ///////////////////////////////////////////////////////////////
  public JobInProgress getJob(JobID jobid) {
    return jobs.get(jobid);
  }

  //Get the job directory in system directory
  Path getSystemDirectoryForJob(JobID id) {
    return new Path(getSystemDir(), id.toString());
  }
  
  //Get the job token file in system directory
  Path getSystemFileForJob(JobID id) {
    return new Path(getSystemDirectoryForJob(id)+"/" + JOB_INFO_FILE);
  }

  /**
   * Change the run-time priority of the given job.
   * 
   * @param jobId job id
   * @param priority new {@link JobPriority} for the job
   * @throws IOException
   * @throws AccessControlException
   */
  synchronized void setJobPriority(JobID jobId, JobPriority priority)
      throws AccessControlException, IOException {
    JobInProgress job = jobs.get(jobId);
    if (job != null) {

      // check both queue-level and job-level access
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.SET_JOB_PRIORITY);

      synchronized (taskScheduler) {
        JobStatus oldStatus = (JobStatus)job.getStatus().clone();
        job.setPriority(priority);
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        JobStatusChangeEvent event = 
          new JobStatusChangeEvent(job, EventType.PRIORITY_CHANGED, oldStatus, 
                                   newStatus);
        updateJobInProgressListeners(event);
      }
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
      
      // expire it
      expireLaunchingTasks.removeTask(taskId);
      
      JobInProgress job = getJob(taskId.getJobID());
      if (job == null) {
        // if job is not there in the cleanup list ... add it
        synchronized (trackerToJobsToCleanup) {
          Set<JobID> jobs = trackerToJobsToCleanup.get(trackerName);
          if (jobs == null) {
            jobs = new HashSet<JobID>();
            trackerToJobsToCleanup.put(trackerName, jobs);
          }
          jobs.add(taskId.getJobID());
        }
        continue;
      }
      
      if (!job.inited()) {
        // if job is not yet initialized ... kill the attempt
        synchronized (trackerToTasksToCleanup) {
          Set<TaskAttemptID> tasks = trackerToTasksToCleanup.get(trackerName);
          if (tasks == null) {
            tasks = new HashSet<TaskAttemptID>();
            trackerToTasksToCleanup.put(trackerName, tasks);
          }
          tasks.add(taskId);
        }
        continue;
      }

      TaskInProgress tip = taskidToTIPMap.get(taskId);
      
      if (tip != null) {
        // Update the job and inform the listeners if necessary
        JobStatus prevStatus = (JobStatus)job.getStatus().clone();
        // Clone TaskStatus object here, because JobInProgress
        // or TaskInProgress can modify this object and
        // the changes should not get reflected in TaskTrackerStatus.
        // An old TaskTrackerStatus is used later in countMapTasks, etc.
        job.updateTaskStatus(tip, (TaskStatus)report.clone());
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        
        // Update the listeners if an incomplete job completes
        if (prevStatus.getRunState() != newStatus.getRunState()) {
          JobStatusChangeEvent event = 
            new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, 
                                     prevStatus, newStatus);
          updateJobInProgressListeners(event);
        }
      } else {
        LOG.info("Serious problem.  While updating status, cannot find taskid " 
                 + report.getTaskID());
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
                                                             failedFetchTrackerName);
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
  void lostTaskTracker(TaskTracker taskTracker) {
    String trackerName = taskTracker.getTrackerName();
    LOG.info("Lost tracker '" + trackerName + "'");
    
    // remove the tracker from the local structures
    synchronized (trackerToJobsToCleanup) {
      trackerToJobsToCleanup.remove(trackerName);
    }
    
    synchronized (trackerToTasksToCleanup) {
      trackerToTasksToCleanup.remove(trackerName);
    }
    
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
            (tip.isMapTask() && !tip.isJobSetupTask() && 
             job.desiredReduces() != 0)) {
          // if the job is done, we don't want to change anything
          if (job.getStatus().getRunState() == JobStatus.RUNNING ||
              job.getStatus().getRunState() == JobStatus.PREP) {
            // the state will be KILLED_UNCLEAN, if the task(map or reduce) 
            // was RUNNING on the tracker
            TaskStatus.State killState = (tip.isRunningTask(taskId) && 
              !tip.isJobSetupTask() && !tip.isJobCleanupTask()) ? 
              TaskStatus.State.KILLED_UNCLEAN : TaskStatus.State.KILLED;
            job.failedTask(tip, taskId, ("Lost task tracker: " + trackerName), 
                           (tip.isMapTask() ? 
                               TaskStatus.Phase.MAP : 
                               TaskStatus.Phase.REDUCE), 
                            killState,
                            trackerName);
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
      // Also, remove any reserved slots on this tasktracker
      for (JobInProgress job : jobsWithFailures) {
        job.addTrackerTaskFailure(trackerName, taskTracker);
      }

      // Cleanup
      taskTracker.cancelAllReservations();

      // Purge 'marked' tasks, needs to be done  
      // here to prevent hanging references!
      removeMarkedTasks(trackerName);
    }
  }

  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.
   */
  public synchronized void refreshNodes() throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    // check access
    if (!aclsManager.isMRAdmin(UserGroupInformation.getCurrentUser())) {
      AuditLogger.logFailure(user, Constants.REFRESH_NODES,
          aclsManager.getAdminsAcl().toString(), Constants.JOBTRACKER,
          Constants.UNAUTHORIZED_USER);
      throw new AccessControlException(user + 
                                       " is not authorized to refresh nodes.");
    }
    
    AuditLogger.logSuccess(user, Constants.REFRESH_NODES, Constants.JOBTRACKER);
    // call the actual api
    refreshHosts();
  }

  UserGroupInformation getMROwner() {
    return aclsManager.getMROwner();
  }

  private synchronized void refreshHosts() throws IOException {
    // Reread the config to get HOSTS and HOSTS_EXCLUDE filenames.
    // Update the file names and refresh internal includes and excludes list
    LOG.info("Refreshing hosts information");
    Configuration conf = new Configuration();

    hostsReader.updateFileNames(conf.get(JTConfig.JT_HOSTS_FILENAME,""), 
                                conf.get(JTConfig.JT_HOSTS_EXCLUDE_FILENAME, ""));
    hostsReader.refresh();
    
    Set<String> excludeSet = new HashSet<String>();
    for(Map.Entry<String, TaskTracker> eSet : taskTrackers.entrySet()) {
      String trackerName = eSet.getKey();
      TaskTrackerStatus status = eSet.getValue().getStatus();
      // Check if not include i.e not in host list or in hosts list but excluded
      if (!inHostsList(status) || inExcludedHostsList(status)) {
          excludeSet.add(status.getHost()); // add to rejected trackers
      }
    }
    decommissionNodes(excludeSet);
  }

  // main decommission
  synchronized void decommissionNodes(Set<String> hosts) 
  throws IOException {  
    LOG.info("Decommissioning " + hosts.size() + " nodes");
    // create a list of tracker hostnames
    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        int trackersDecommissioned = 0;
        for (String host : hosts) {
          LOG.info("Decommissioning host " + host);
          Set<TaskTracker> trackers = hostnameToTaskTracker.remove(host);
          if (trackers != null) {
            for (TaskTracker tracker : trackers) {
              LOG.info("Decommission: Losing tracker " 
                       + tracker.getTrackerName() + " on host " + host);
              removeTracker(tracker);
            }
            trackersDecommissioned += trackers.size();
          }
          LOG.info("Host " + host + " is ready for decommissioning");
        }
        getInstrumentation().setDecommissionedTrackers(trackersDecommissioned);
      }
    }
  }

  /**
   * Returns a set of excluded nodes.
   */
  Collection<String> getExcludedNodes() {
    return hostsReader.getExcludedHosts();
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
    
    try {
      if (argv.length == 0) {
        JobTracker tracker = startTracker(new JobConf());
        tracker.offerService();
      }
      else {
        if ("-dumpConfiguration".equals(argv[0]) && argv.length == 1) {
          dumpConfiguration(new PrintWriter(System.out));
          System.out.println();
          Configuration conf = new Configuration();
          QueueManager.dumpConfiguration(new PrintWriter(System.out), conf);
        }
        else {
          System.out.println("usage: JobTracker [-dumpConfiguration]");
          System.exit(-1);
        }
      }
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  /**
   * Dumps the configuration properties in Json format
   * @param writer {@link}Writer object to which the output is written
   * @throws IOException
   */
  private static void dumpConfiguration(Writer writer) throws IOException {
    Configuration.dumpConfiguration(new JobConf(), writer);
    writer.write("\n");
  }

  /**
   * Gets the root level queues.
   *
   * @return array of QueueInfo object.
   * @throws java.io.IOException
   */
   @Override
  public QueueInfo[] getRootQueues() throws IOException {
    return getQueueInfoArray(queueManager.getRootQueues());
  }
 
  /**
   * Returns immediate children of queueName.
   *
   * @param queueName
   * @return array of QueueInfo which are children of queueName
   * @throws java.io.IOException
   */
  @Override
  public QueueInfo[] getChildQueues(String queueName) throws IOException {
    return getQueueInfoArray(queueManager.getChildQueues(queueName));
  }

  /**
   * Gets the root level queues.
   *
   * @return array of JobQueueInfo object.
   * @throws java.io.IOException
   */
   @Deprecated
  public JobQueueInfo[] getRootJobQueues() throws IOException {
    return queueManager.getRootQueues();
  }

  @Deprecated 
  public JobQueueInfo[] getJobQueues() throws IOException {
    return queueManager.getJobQueueInfos();
  }

  @Deprecated 
  public JobQueueInfo getQueueInfo(String queue) throws IOException {
    return queueManager.getJobQueueInfo(queue);
  }

  private QueueInfo[] getQueueInfoArray(JobQueueInfo[] queues) 
      throws IOException {
    for (JobQueueInfo queue : queues) {
      queue.setJobStatuses(getJobsFromQueue(queue.getQueueName()));
    }
    return queues;
  }
  
  @Override
  public QueueInfo[] getQueues() throws IOException {
    return getQueueInfoArray(queueManager.getJobQueueInfos());
  }

  @Override
  public QueueInfo getQueue(String queue) throws IOException {
    JobQueueInfo jqueue = queueManager.getJobQueueInfo(queue);
    if (jqueue != null) {
      jqueue.setJobStatuses(getJobsFromQueue(jqueue.getQueueName()));
    }
    return jqueue;
  }

  public org.apache.hadoop.mapreduce.JobStatus[] getJobsFromQueue(String queue) 
      throws IOException {
    Collection<JobInProgress> jips = null;
    if (queueManager.getLeafQueueNames().contains(queue)) {
      jips = taskScheduler.getJobs(queue);
    }
    return getJobStatus(jips,false);
  }
  
  @Override
  public org.apache.hadoop.mapreduce.QueueAclsInfo[] 
      getQueueAclsForCurrentUser() throws IOException {
    return queueManager.getQueueAcls(UserGroupInformation.getCurrentUser());
  }

  private synchronized JobStatus[] getJobStatus(Collection<JobInProgress> jips,
      boolean toComplete) {
    if (jips == null || jips.isEmpty()) {
      return new JobStatus[]{};
    }
    ArrayList<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for(JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();
      status.setStartTime(jip.getStartTime());
      status.setUsername(jip.getProfile().getUser());
      if (toComplete) {
        if (status.getRunState() == JobStatus.RUNNING || 
            status.getRunState() == JobStatus.PREP) {
          jobStatusList.add(status);
        }
      }else {
        jobStatusList.add(status);
      }
    }
    return  jobStatusList.toArray(
        new JobStatus[jobStatusList.size()]);
  }

  /**
   * Returns the confgiured maximum number of tasks for a single job
   */
  int getMaxTasksPerJob() {
    return conf.getInt(JT_TASKS_PER_JOB, -1);
  }
  
  @Override
  public void refreshServiceAcl() throws IOException {
    if (!conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }
    this.interTrackerServer.refreshServiceAcl(conf, new MapReducePolicyProvider());
  }

  @Override
  public void refreshQueues() throws IOException{
    LOG.info("Refreshing queue information. requested by : " + 
             UserGroupInformation.getCurrentUser().getShortUserName());
    synchronized (taskScheduler) {
      queueManager.refreshQueues(new Configuration(this.conf), taskScheduler
          .getQueueRefresher());
    }
  }
  
  private void initializeTaskMemoryRelatedConfig() {
    memSizeForMapSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            MAPMEMORY_MB,
            JobConf.DISABLED_MEMORY_LIMIT));
    memSizeForReduceSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            REDUCEMEMORY_MB,
            JobConf.DISABLED_MEMORY_LIMIT));

    if (conf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY)+
          " instead use "+JTConfig.JT_MAX_MAPMEMORY_MB+
          " and " + JTConfig.JT_MAX_REDUCEMEMORY_MB
      );

      limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      if (limitMaxMemForMapTasks != JobConf.DISABLED_MEMORY_LIMIT &&
        limitMaxMemForMapTasks >= 0) {
        limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
          limitMaxMemForMapTasks /
            (1024 * 1024); //Converting old values in bytes to MB
      }
    } else {
      limitMaxMemForMapTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JTConfig.JT_MAX_MAPMEMORY_MB,
            JobConf.DISABLED_MEMORY_LIMIT));
      limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JTConfig.JT_MAX_REDUCEMEMORY_MB,
            JobConf.DISABLED_MEMORY_LIMIT));
    }

    LOG.info(new StringBuilder().append("Scheduler configured with ").append(
        "(memSizeForMapSlotOnJT, memSizeForReduceSlotOnJT,").append(
        " limitMaxMemForMapTasks, limitMaxMemForReduceTasks) (").append(
        memSizeForMapSlotOnJT).append(", ").append(memSizeForReduceSlotOnJT)
        .append(", ").append(limitMaxMemForMapTasks).append(", ").append(
            limitMaxMemForReduceTasks).append(")"));
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() {
    LOG.info("Refreshing superuser proxy groups mapping ");

    ProxyUsers.refreshSuperUserGroupsConfiguration();
  }

  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " + 
             UserGroupInformation.getCurrentUser().getShortUserName());
    
    Groups.getUserToGroupsMappingService().refresh();
  }
  
  private boolean perTaskMemoryConfigurationSetOnJT() {
    if (limitMaxMemForMapTasks == JobConf.DISABLED_MEMORY_LIMIT
        || limitMaxMemForReduceTasks == JobConf.DISABLED_MEMORY_LIMIT
        || memSizeForMapSlotOnJT == JobConf.DISABLED_MEMORY_LIMIT
        || memSizeForReduceSlotOnJT == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
  }

  /**
   * Check the job if it has invalid requirements and throw and IOException if does have.
   * 
   * @param job
   * @throws IOException 
   */
  void checkMemoryRequirements(JobInProgress job)
      throws IOException {
    if (!perTaskMemoryConfigurationSetOnJT()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Per-Task memory configuration is not set on JT. "
                  + "Not checking the job for invalid memory requirements.");
      }
      return;
    }

    boolean invalidJob = false;
    String msg = "";
    long maxMemForMapTask = job.getMemoryForMapTask();
    long maxMemForReduceTask = job.getMemoryForReduceTask();

    if (maxMemForMapTask == JobConf.DISABLED_MEMORY_LIMIT
        || maxMemForReduceTask == JobConf.DISABLED_MEMORY_LIMIT) {
      invalidJob = true;
      msg = "Invalid job requirements.";
    }

    if (maxMemForMapTask > limitMaxMemForMapTasks
        || maxMemForReduceTask > limitMaxMemForReduceTasks) {
      invalidJob = true;
      msg = "Exceeds the cluster's max-memory-limit.";
    }

    if (invalidJob) {
      StringBuilder jobStr =
          new StringBuilder().append(job.getJobID().toString()).append("(")
              .append(maxMemForMapTask).append(" memForMapTasks ").append(
                  maxMemForReduceTask).append(" memForReduceTasks): ");
      LOG.warn(jobStr.toString() + msg);

      throw new IOException(jobStr.toString() + msg);
    }
  }
  
  synchronized String getFaultReport(String host) {
    FaultInfo fi = faultyTrackers.getFaultInfo(host, false);
    if (fi == null) {
      return "";
    }
    return fi.getTrackerFaultReport();
  }

  synchronized Set<ReasonForBlackListing> getReasonForBlackList(String host) {
    FaultInfo fi = faultyTrackers.getFaultInfo(host, false);
    if (fi == null) {
      return new HashSet<ReasonForBlackListing>();
    }
    return fi.getReasonforblacklisting();
  }
  
  synchronized Collection<BlackListInfo> getBlackListedTrackers() {
    Collection<BlackListInfo> blackListedTrackers = 
      new ArrayList<BlackListInfo>();
    for(TaskTrackerStatus tracker : blacklistedTaskTrackers()) {
      String hostName = tracker.getHost();
      BlackListInfo bi = new BlackListInfo();
      bi.setTrackerName(tracker.getTrackerName());
      Set<ReasonForBlackListing> rfbs = 
        getReasonForBlackList(hostName);
      StringBuffer sb = new StringBuffer();
      for(ReasonForBlackListing rfb : rfbs) {
        sb.append(rfb.toString());
        sb.append(",");
      }
      if (sb.length() > 0) {
        sb.replace(sb.length()-1, sb.length(), "");
      }
      bi.setReasonForBlackListing(sb.toString());
      bi.setBlackListReport(
          getFaultReport(hostName));
      blackListedTrackers.add(bi);
    }
    return blackListedTrackers;
  }
  
  /** Test method to increment the fault
   * This method is synchronized to make sure that the locking order 
   * "faultyTrackers.potentiallyFaultyTrackers lock followed by taskTrackers 
   * lock" is under JobTracker lock to avoid deadlocks.
   */
  synchronized void incrementFaults(String hostName) {
    faultyTrackers.incrementFaults(hostName);
  }

  JobTracker(final JobConf conf, Clock clock, boolean ignoredForSimulation) 
  throws IOException {
    this.clock = clock;
    this.conf = conf;
    trackerIdentifier = getDateFormat().format(new Date());

    if (fs == null) {
      fs = FileSystem.get(conf);
    }
    this.localFs = FileSystem.getLocal(conf);
    
    tasktrackerExpiryInterval = 
      conf.getLong("mapred.tasktracker.expiry.interval", 10 * 60 * 1000);
    retiredJobsCacheSize = 
      conf.getInt("mapred.job.tracker.retiredjobs.cache.size", 1000);

    // min time before retire
    MAX_BLACKLISTS_PER_TRACKER = 
        conf.getInt("mapred.max.tracker.blacklists", 4);
    NUM_HEARTBEATS_IN_SECOND = 
        conf.getInt("mapred.heartbeats.in.second", 100);
    
    // Set ports, start RPC servers, setup security policy etc.
    InetSocketAddress addr = getAddress(conf);
    this.localMachine = addr.getHostName();
    this.port = addr.getPort();
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, JTConfig.JT_KEYTAB_FILE, JTConfig.JT_USER_NAME,
        localMachine);
    
    secretManager = null;
    
    this.hostsReader = new HostsFileReader(conf.get(JTConfig.JT_HOSTS_FILENAME, ""),
        conf.get(JTConfig.JT_HOSTS_EXCLUDE_FILENAME, ""));
    // queue manager
    Configuration clusterConf = new Configuration(this.conf);
    queueManager = new QueueManager(clusterConf);

    aclsManager = new ACLsManager(conf, new JobACLsManager(conf), queueManager);

    LOG.info("Starting jobtracker with owner as " +
        getMROwner().getShortUserName());

    // Create the scheduler
    Class<? extends TaskScheduler> schedulerClass
      = conf.getClass(JTConfig.JT_TASK_SCHEDULER,
          JobQueueTaskScheduler.class, TaskScheduler.class);
    taskScheduler = 
      (TaskScheduler)ReflectionUtils.newInstance(schedulerClass, conf);

    // Create the jetty server
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(
        conf.get(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:50030"));
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.startTime = clock.getTime();
    infoServer = new HttpServer("job", infoBindAddress, tmpInfoPort, 
        tmpInfoPort == 0, conf);
    infoServer.setAttribute("job.tracker", this);
    
    // initialize history parameters.
    FileSystem historyFS = null;

    jobHistory = new JobHistory();
    final JobTracker jtFinal = this;
    try {
      historyFS = getMROwner().doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          jobHistory.init(jtFinal, conf, jtFinal.localMachine, jtFinal.startTime);
          jobHistory.initDone(conf, fs);
          final String historyLogDir = 
            jobHistory.getCompletedJobHistoryLocation().toString();
          infoServer.setAttribute("historyLogDir", historyLogDir);
          return new Path(historyLogDir).getFileSystem(conf);
        }
      });
    } catch (InterruptedException e1) {
      throw (IOException) new IOException().initCause(e1);
    }

    infoServer.setAttribute("fileSys", historyFS);
    infoServer.addServlet("reducegraph", "/taskgraph", TaskGraphServlet.class);
    infoServer.start();
    this.infoPort = this.infoServer.getPort();

    // Initialize instrumentation
    JobTrackerInstrumentation tmp;
    Class<? extends JobTrackerInstrumentation> metricsInst =
      getInstrumentationClass(conf);
    try {
      java.lang.reflect.Constructor<? extends JobTrackerInstrumentation> c =
        metricsInst.getConstructor(new Class[] {JobTracker.class, JobConf.class} );
      tmp = c.newInstance(this, conf);
    } catch(Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by 
      //falling back on the default.
      LOG.error("failed to initialize job tracker metrics", e);
      tmp = new JobTrackerMetricsInst(this, conf);
    }
    myInstrumentation = tmp;
    
    // start the recovery manager
    recoveryManager = new RecoveryManager();
    
    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass(
            CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, 
            ScriptBasedMapping.class, DNSToSwitchMapping.class), 
        conf);
    this.numTaskCacheLevels = conf.getInt("mapred.task.cache.levels", 
        NetworkTopology.DEFAULT_HOST_LEVEL);

    //initializes the job status store
    completedJobStatusStore = new CompletedJobStatusStore(conf, aclsManager);
  }

  /**
   * Get the path of the locally stored job file
   * @param jobId id of the job
   * @return the path of the job file on the local file system 
   */
  String getLocalJobFilePath(org.apache.hadoop.mapreduce.JobID jobId){
    return System.getProperty("hadoop.log.dir") + 
           File.separator + jobId + "_conf.xml";
  }

  /**
   * Discard a current delegation token.
   */
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                       ) throws IOException,
                                                InterruptedException {
    String user = UserGroupInformation.getCurrentUser().getUserName();
    secretManager.cancelToken(token, user);
  }

  /**
   * Get a new delegation token.
   */
  @Override
  public Token<DelegationTokenIdentifier> 
     getDelegationToken(Text renewer
                        )throws IOException, InterruptedException {
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be issued only with kerberos authentication");
    }
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Text owner = new Text(ugi.getUserName());
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    DelegationTokenIdentifier ident = 
      new DelegationTokenIdentifier(owner, renewer, realUser);
    return new Token<DelegationTokenIdentifier>(ident, secretManager);
  }

  /**
   * Renew a delegation token to extend its lifetime.
   */
  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                      ) throws IOException,
                                               InterruptedException {
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be renewed only with kerberos authentication");
    }
    String user = UserGroupInformation.getCurrentUser().getUserName();
    return secretManager.renewToken(token, user);
  }

  JobACLsManager getJobACLsManager() {
    return aclsManager.getJobACLsManager();
  }

  ACLsManager getACLsManager() {
    return aclsManager;
  }

  /**
   * 
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = UserGroupInformation
        .getRealAuthenticationMethod(UserGroupInformation.getCurrentUser());
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)) {
      return false;
    }
    return true;
  }
}
