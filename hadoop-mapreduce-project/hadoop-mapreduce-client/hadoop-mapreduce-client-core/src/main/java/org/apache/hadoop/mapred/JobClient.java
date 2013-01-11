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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus.BlackListInfo;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * <code>JobClient</code> is the primary interface for the user-job to interact
 * with the cluster.
 * 
 * <code>JobClient</code> provides facilities to submit jobs, track their 
 * progress, access component-tasks' reports/logs, get the Map-Reduce cluster
 * status information etc.
 * 
 * <p>The job submission process involves:
 * <ol>
 *   <li>
 *   Checking the input and output specifications of the job.
 *   </li>
 *   <li>
 *   Computing the {@link InputSplit}s for the job.
 *   </li>
 *   <li>
 *   Setup the requisite accounting information for the {@link DistributedCache} 
 *   of the job, if necessary.
 *   </li>
 *   <li>
 *   Copying the job's jar and configuration to the map-reduce system directory 
 *   on the distributed file-system. 
 *   </li>
 *   <li>
 *   Submitting the job to the cluster and optionally monitoring
 *   it's status.
 *   </li>
 * </ol></p>
 *  
 * Normally the user creates the application, describes various facets of the
 * job via {@link JobConf} and then uses the <code>JobClient</code> to submit 
 * the job and monitor its progress.
 * 
 * <p>Here is an example on how to use <code>JobClient</code>:</p>
 * <p><blockquote><pre>
 *     // Create a new JobConf
 *     JobConf job = new JobConf(new Configuration(), MyJob.class);
 *     
 *     // Specify various job-specific parameters     
 *     job.setJobName("myjob");
 *     
 *     job.setInputPath(new Path("in"));
 *     job.setOutputPath(new Path("out"));
 *     
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *
 *     // Submit the job, then poll for progress until the job is complete
 *     JobClient.runJob(job);
 * </pre></blockquote></p>
 * 
 * <h4 id="JobControl">Job Control</h4>
 * 
 * <p>At times clients would chain map-reduce jobs to accomplish complex tasks 
 * which cannot be done via a single map-reduce job. This is fairly easy since 
 * the output of the job, typically, goes to distributed file-system and that 
 * can be used as the input for the next job.</p>
 * 
 * <p>However, this also means that the onus on ensuring jobs are complete 
 * (success/failure) lies squarely on the clients. In such situations the 
 * various job-control options are:
 * <ol>
 *   <li>
 *   {@link #runJob(JobConf)} : submits the job and returns only after 
 *   the job has completed.
 *   </li>
 *   <li>
 *   {@link #submitJob(JobConf)} : only submits the job, then poll the 
 *   returned handle to the {@link RunningJob} to query status and make 
 *   scheduling decisions.
 *   </li>
 *   <li>
 *   {@link JobConf#setJobEndNotificationURI(String)} : setup a notification
 *   on job-completion, thus avoiding polling.
 *   </li>
 * </ol></p>
 * 
 * @see JobConf
 * @see ClusterStatus
 * @see Tool
 * @see DistributedCache
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobClient extends CLI {
  public static enum TaskStatusFilter { NONE, KILLED, FAILED, SUCCEEDED, ALL }
  private TaskStatusFilter taskOutputFilter = TaskStatusFilter.FAILED; 
  /* notes that get delegation token was called. Again this is hack for oozie 
   * to make sure we add history server delegation tokens to the credentials
   *  for the job. Since the api only allows one delegation token to be returned, 
   *  we have to add this hack.
   */
  private boolean getDelegationTokenCalled = false;
  /* do we need a HS delegation token for this client */
  static final String HS_DELEGATION_TOKEN_REQUIRED 
      = "mapreduce.history.server.delegationtoken.required";
  
  static{
    ConfigUtil.loadResources();
  }

  /**
   * A NetworkedJob is an implementation of RunningJob.  It holds
   * a JobProfile object to provide some info, and interacts with the
   * remote service to provide certain functionality.
   */
  static class NetworkedJob implements RunningJob {
    Job job;
    /**
     * We store a JobProfile and a timestamp for when we last
     * acquired the job profile.  If the job is null, then we cannot
     * perform any of the tasks.  The job might be null if the cluster
     * has completely forgotten about the job.  (eg, 24 hours after the
     * job completes.)
     */
    public NetworkedJob(JobStatus status, Cluster cluster) throws IOException {
      job = Job.getInstance(cluster, status, new JobConf(status.getJobFile()));
    }

    public NetworkedJob(Job job) throws IOException {
      this.job = job;
    }

    public Configuration getConfiguration() {
      return job.getConfiguration();
    }

    /**
     * An identifier for the job
     */
    public JobID getID() {
      return JobID.downgrade(job.getJobID());
    }
    
    /** @deprecated This method is deprecated and will be removed. Applications should 
     * rather use {@link #getID()}.*/
    @Deprecated
    public String getJobID() {
      return getID().toString();
    }
    
    /**
     * The user-specified job name
     */
    public String getJobName() {
      return job.getJobName();
    }

    /**
     * The name of the job file
     */
    public String getJobFile() {
      return job.getJobFile();
    }

    /**
     * A URL where the job's status can be seen
     */
    public String getTrackingURL() {
      return job.getTrackingURL();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of map work
     * completed.
     */
    public float mapProgress() throws IOException {
      try {
        return job.mapProgress();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of reduce work
     * completed.
     */
    public float reduceProgress() throws IOException {
      try {
        return job.reduceProgress();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of cleanup work
     * completed.
     */
    public float cleanupProgress() throws IOException {
      try {
        return job.cleanupProgress();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of setup work
     * completed.
     */
    public float setupProgress() throws IOException {
      try {
        return job.setupProgress();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * Returns immediately whether the whole job is done yet or not.
     */
    public synchronized boolean isComplete() throws IOException {
      try {
        return job.isComplete();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * True iff job completed successfully.
     */
    public synchronized boolean isSuccessful() throws IOException {
      try {
        return job.isSuccessful();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * Blocks until the job is finished
     */
    public void waitForCompletion() throws IOException {
      try {
        job.waitForCompletion(false);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      } catch (ClassNotFoundException ce) {
        throw new IOException(ce);
      }
    }

    /**
     * Tells the service to get the state of the current job.
     */
    public synchronized int getJobState() throws IOException {
      try {
        return job.getJobState().getValue();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    /**
     * Tells the service to terminate the current job.
     */
    public synchronized void killJob() throws IOException {
      try {
        job.killJob();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
   
    
    /** Set the priority of the job.
    * @param priority new priority of the job. 
    */
    public synchronized void setJobPriority(String priority) 
                                                throws IOException {
      try {
        job.setPriority(
          org.apache.hadoop.mapreduce.JobPriority.valueOf(priority));
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    /**
     * Kill indicated task attempt.
     * @param taskId the id of the task to kill.
     * @param shouldFail if true the task is failed and added to failed tasks list, otherwise
     * it is just killed, w/o affecting job failure status.
     */
    public synchronized void killTask(TaskAttemptID taskId,
        boolean shouldFail) throws IOException {
      try {
        if (shouldFail) {
          job.failTask(taskId);
        } else {
          job.killTask(taskId);
        }
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /** @deprecated Applications should rather use {@link #killTask(TaskAttemptID, boolean)}*/
    @Deprecated
    public synchronized void killTask(String taskId, boolean shouldFail) throws IOException {
      killTask(TaskAttemptID.forName(taskId), shouldFail);
    }
    
    /**
     * Fetch task completion events from cluster for this job. 
     */
    public synchronized TaskCompletionEvent[] getTaskCompletionEvents(
        int startFrom) throws IOException {
      try {
        org.apache.hadoop.mapreduce.TaskCompletionEvent[] acls = 
          job.getTaskCompletionEvents(startFrom, 10);
        TaskCompletionEvent[] ret = new TaskCompletionEvent[acls.length];
        for (int i = 0 ; i < acls.length; i++ ) {
          ret[i] = TaskCompletionEvent.downgrade(acls[i]);
        }
        return ret;
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * Dump stats to screen
     */
    @Override
    public String toString() {
      return job.toString();
    }
        
    /**
     * Returns the counters for this job
     */
    public Counters getCounters() throws IOException {
      try { 
        Counters result = null;
        org.apache.hadoop.mapreduce.Counters temp = job.getCounters();
        if(temp != null) {
          result = Counters.downgrade(temp);
        }
        return result;
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    @Override
    public String[] getTaskDiagnostics(TaskAttemptID id) throws IOException {
      try { 
        return job.getTaskDiagnostics(id);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    public String getHistoryUrl() throws IOException {
      try {
        return job.getHistoryUrl();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    public boolean isRetired() throws IOException {
      try {
        return job.isRetired();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    boolean monitorAndPrintJob() throws IOException, InterruptedException {
      return job.monitorAndPrintJob();
    }
    
    @Override
    public String getFailureInfo() throws IOException {
      try {
        return job.getStatus().getFailureInfo();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    @Override
    public JobStatus getJobStatus() throws IOException {
      try {
        return JobStatus.downgrade(job.getStatus());
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
  }

  /**
   * Ugi of the client. We store this ugi when the client is created and 
   * then make sure that the same ugi is used to run the various protocols.
   */
  UserGroupInformation clientUgi;
  
  /**
   * Create a job client.
   */
  public JobClient() {
  }
    
  /**
   * Build a job client with the given {@link JobConf}, and connect to the 
   * default cluster
   * 
   * @param conf the job configuration.
   * @throws IOException
   */
  public JobClient(JobConf conf) throws IOException {
    init(conf);
  }

  /**
   * Build a job client with the given {@link Configuration}, 
   * and connect to the default cluster
   * 
   * @param conf the configuration.
   * @throws IOException
   */
  public JobClient(Configuration conf) throws IOException {
    init(new JobConf(conf));
  }

  /**
   * Connect to the default cluster
   * @param conf the job configuration.
   * @throws IOException
   */
  public void init(JobConf conf) throws IOException {
    setConf(conf);
    cluster = new Cluster(conf);
    clientUgi = UserGroupInformation.getCurrentUser();
  }

  /**
   * Build a job client, connect to the indicated job tracker.
   * 
   * @param jobTrackAddr the job tracker to connect to.
   * @param conf configuration.
   */
  public JobClient(InetSocketAddress jobTrackAddr, 
                   Configuration conf) throws IOException {
    cluster = new Cluster(jobTrackAddr, conf);
    clientUgi = UserGroupInformation.getCurrentUser();
  }

  /**
   * Close the <code>JobClient</code>.
   */
  public synchronized void close() throws IOException {
    cluster.close();
  }

  /**
   * Get a filesystem handle.  We need this to prepare jobs
   * for submission to the MapReduce system.
   * 
   * @return the filesystem handle.
   */
  public synchronized FileSystem getFs() throws IOException {
    try { 
      return cluster.getFileSystem();
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /**
   * Get a handle to the Cluster
   */
  public Cluster getClusterHandle() {
    return cluster;
  }
  
  /**
   * Submit a job to the MR system.
   * 
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   * 
   * @param jobFile the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws FileNotFoundException
   * @throws InvalidJobConfException
   * @throws IOException
   */
  public RunningJob submitJob(String jobFile) throws FileNotFoundException, 
                                                     InvalidJobConfException, 
                                                     IOException {
    // Load in the submitted job details
    JobConf job = new JobConf(jobFile);
    return submitJob(job);
  }
    
  /**
   * Submit a job to the MR system.
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   * 
   * @param conf the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public RunningJob submitJob(final JobConf conf) throws FileNotFoundException,
                                                  IOException {
    try {
      conf.setBooleanIfUnset("mapred.mapper.new-api", false);
      conf.setBooleanIfUnset("mapred.reducer.new-api", false);
      if (getDelegationTokenCalled) {
        conf.setBoolean(HS_DELEGATION_TOKEN_REQUIRED, getDelegationTokenCalled);
        getDelegationTokenCalled = false;
      }
      Job job = clientUgi.doAs(new PrivilegedExceptionAction<Job> () {
        @Override
        public Job run() throws IOException, ClassNotFoundException, 
          InterruptedException {
          Job job = Job.getInstance(conf);
          job.submit();
          return job;
        }
      });
      // update our Cluster instance with the one created by Job for submission
      // (we can't pass our Cluster instance to Job, since Job wraps the config
      // instance, and the two configs would then diverge)
      cluster = job.getCluster();
      return new NetworkedJob(job);
    } catch (InterruptedException ie) {
      throw new IOException("interrupted", ie);
    }
  }

  private Job getJobUsingCluster(final JobID jobid) throws IOException,
  InterruptedException {
    return clientUgi.doAs(new PrivilegedExceptionAction<Job>() {
      public Job run() throws IOException, InterruptedException  {
       return cluster.getJob(jobid);
      }
    });
  }
  /**
   * Get an {@link RunningJob} object to track an ongoing job.  Returns
   * null if the id does not correspond to any known job.
   * 
   * @param jobid the jobid of the job.
   * @return the {@link RunningJob} handle to track the job, null if the 
   *         <code>jobid</code> doesn't correspond to any known job.
   * @throws IOException
   */
  public RunningJob getJob(final JobID jobid) throws IOException {
    try {
      
      Job job = getJobUsingCluster(jobid);
      if (job != null) {
        JobStatus status = JobStatus.downgrade(job.getStatus());
        if (status != null) {
          return new NetworkedJob(status, cluster);
        } 
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    return null;
  }

  /**@deprecated Applications should rather use {@link #getJob(JobID)}. 
   */
  @Deprecated
  public RunningJob getJob(String jobid) throws IOException {
    return getJob(JobID.forName(jobid));
  }
  
  private static final TaskReport[] EMPTY_TASK_REPORTS = new TaskReport[0];
  
  /**
   * Get the information of the current state of the map tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the map tips.
   * @throws IOException
   */
  public TaskReport[] getMapTaskReports(JobID jobId) throws IOException {
    return getTaskReports(jobId, TaskType.MAP);
  }
  
  private TaskReport[] getTaskReports(final JobID jobId, TaskType type) throws 
    IOException {
    try {
      Job j = getJobUsingCluster(jobId);
      if(j == null) {
        return EMPTY_TASK_REPORTS;
      }
      return TaskReport.downgradeArray(j.getTaskReports(type));
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /**@deprecated Applications should rather use {@link #getMapTaskReports(JobID)}*/
  @Deprecated
  public TaskReport[] getMapTaskReports(String jobId) throws IOException {
    return getMapTaskReports(JobID.forName(jobId));
  }
  
  /**
   * Get the information of the current state of the reduce tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the reduce tips.
   * @throws IOException
   */    
  public TaskReport[] getReduceTaskReports(JobID jobId) throws IOException {
    return getTaskReports(jobId, TaskType.REDUCE);
  }

  /**
   * Get the information of the current state of the cleanup tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the cleanup tips.
   * @throws IOException
   */    
  public TaskReport[] getCleanupTaskReports(JobID jobId) throws IOException {
    return getTaskReports(jobId, TaskType.JOB_CLEANUP);
  }

  /**
   * Get the information of the current state of the setup tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the setup tips.
   * @throws IOException
   */    
  public TaskReport[] getSetupTaskReports(JobID jobId) throws IOException {
    return getTaskReports(jobId, TaskType.JOB_SETUP);
  }

  
  /**@deprecated Applications should rather use {@link #getReduceTaskReports(JobID)}*/
  @Deprecated
  public TaskReport[] getReduceTaskReports(String jobId) throws IOException {
    return getReduceTaskReports(JobID.forName(jobId));
  }
  
  /**
   * Display the information about a job's tasks, of a particular type and
   * in a particular state
   * 
   * @param jobId the ID of the job
   * @param type the type of the task (map/reduce/setup/cleanup)
   * @param state the state of the task 
   * (pending/running/completed/failed/killed)
   * @throws IOException when there is an error communicating with the master
   * @throws IllegalArgumentException if an invalid type/state is passed
   */
  public void displayTasks(final JobID jobId, String type, String state) 
  throws IOException {
    try {
      Job job = getJobUsingCluster(jobId);
      super.displayTasks(job, type, state);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /**
   * Get status information about the Map-Reduce cluster.
   *  
   * @return the status information about the Map-Reduce cluster as an object
   *         of {@link ClusterStatus}.
   * @throws IOException
   */
  public ClusterStatus getClusterStatus() throws IOException {
    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<ClusterStatus>() {
        public ClusterStatus run()  throws IOException, InterruptedException {
          ClusterMetrics metrics = cluster.getClusterStatus();
          return new ClusterStatus(metrics.getTaskTrackerCount(),
              metrics.getBlackListedTaskTrackerCount(), cluster.getTaskTrackerExpiryInterval(),
              metrics.getOccupiedMapSlots(),
              metrics.getOccupiedReduceSlots(), metrics.getMapSlotCapacity(),
              metrics.getReduceSlotCapacity(),
              cluster.getJobTrackerStatus(),
              metrics.getDecommissionedTaskTrackerCount());
        }
      });
    }
      catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  private  Collection<String> arrayToStringList(TaskTrackerInfo[] objs) {
    Collection<String> list = new ArrayList<String>();
    for (TaskTrackerInfo info: objs) {
      list.add(info.getTaskTrackerName());
    }
    return list;
  }

  private  Collection<BlackListInfo> arrayToBlackListInfo(TaskTrackerInfo[] objs) {
    Collection<BlackListInfo> list = new ArrayList<BlackListInfo>();
    for (TaskTrackerInfo info: objs) {
      BlackListInfo binfo = new BlackListInfo();
      binfo.setTrackerName(info.getTaskTrackerName());
      binfo.setReasonForBlackListing(info.getReasonForBlacklist());
      binfo.setBlackListReport(info.getBlacklistReport());
      list.add(binfo);
    }
    return list;
  }

  /**
   * Get status information about the Map-Reduce cluster.
   *  
   * @param  detailed if true then get a detailed status including the
   *         tracker names
   * @return the status information about the Map-Reduce cluster as an object
   *         of {@link ClusterStatus}.
   * @throws IOException
   */
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<ClusterStatus>() {
        public ClusterStatus run() throws IOException, InterruptedException {
        ClusterMetrics metrics = cluster.getClusterStatus();
        return new ClusterStatus(arrayToStringList(cluster.getActiveTaskTrackers()),
          arrayToBlackListInfo(cluster.getBlackListedTaskTrackers()),
          cluster.getTaskTrackerExpiryInterval(), metrics.getOccupiedMapSlots(),
          metrics.getOccupiedReduceSlots(), metrics.getMapSlotCapacity(),
          metrics.getReduceSlotCapacity(), 
          cluster.getJobTrackerStatus());
        }
      });
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
    

  /** 
   * Get the jobs that are not completed and not failed.
   * 
   * @return array of {@link JobStatus} for the running/to-be-run jobs.
   * @throws IOException
   */
  public JobStatus[] jobsToComplete() throws IOException {
    List<JobStatus> stats = new ArrayList<JobStatus>();
    for (JobStatus stat : getAllJobs()) {
      if (!stat.isJobComplete()) {
        stats.add(stat);
      }
    }
    return stats.toArray(new JobStatus[0]);
  }

  /** 
   * Get the jobs that are submitted.
   * 
   * @return array of {@link JobStatus} for the submitted jobs.
   * @throws IOException
   */
  public JobStatus[] getAllJobs() throws IOException {
    try {
      org.apache.hadoop.mapreduce.JobStatus[] jobs = 
          clientUgi.doAs(new PrivilegedExceptionAction<
              org.apache.hadoop.mapreduce.JobStatus[]> () {
            public org.apache.hadoop.mapreduce.JobStatus[] run() 
                throws IOException, InterruptedException {
              return cluster.getAllJobStatuses();
            }
          });
      JobStatus[] stats = new JobStatus[jobs.length];
      for (int i = 0; i < jobs.length; i++) {
        stats[i] = JobStatus.downgrade(jobs[i]);
      }
      return stats;
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /** 
   * Utility that submits a job, then polls for progress until the job is
   * complete.
   * 
   * @param job the job configuration.
   * @throws IOException if the job fails
   */
  public static RunningJob runJob(JobConf job) throws IOException {
    JobClient jc = new JobClient(job);
    RunningJob rj = jc.submitJob(job);
    try {
      if (!jc.monitorAndPrintJob(job, rj)) {
        throw new IOException("Job failed!");
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    return rj;
  }
  
  /**
   * Monitor a job and print status in real-time as progress is made and tasks 
   * fail.
   * @param conf the job's configuration
   * @param job the job to track
   * @return true if the job succeeded
   * @throws IOException if communication to the JobTracker fails
   */
  public boolean monitorAndPrintJob(JobConf conf, 
                                    RunningJob job
  ) throws IOException, InterruptedException {
    return ((NetworkedJob)job).monitorAndPrintJob();
  }

  static String getTaskLogURL(TaskAttemptID taskId, String baseUrl) {
    return (baseUrl + "/tasklog?plaintext=true&attemptid=" + taskId); 
  }
  
  static Configuration getConfiguration(String jobTrackerSpec)
  {
    Configuration conf = new Configuration();
    if (jobTrackerSpec != null) {        
      if (jobTrackerSpec.indexOf(":") >= 0) {
        conf.set("mapred.job.tracker", jobTrackerSpec);
      } else {
        String classpathFile = "hadoop-" + jobTrackerSpec + ".xml";
        URL validate = conf.getResource(classpathFile);
        if (validate == null) {
          throw new RuntimeException(classpathFile + " not found on CLASSPATH");
        }
        conf.addResource(classpathFile);
      }
    }
    return conf;
  }

  /**
   * Sets the output filter for tasks. only those tasks are printed whose
   * output matches the filter. 
   * @param newValue task filter.
   */
  @Deprecated
  public void setTaskOutputFilter(TaskStatusFilter newValue){
    this.taskOutputFilter = newValue;
  }
    
  /**
   * Get the task output filter out of the JobConf.
   * 
   * @param job the JobConf to examine.
   * @return the filter level.
   */
  public static TaskStatusFilter getTaskOutputFilter(JobConf job) {
    return TaskStatusFilter.valueOf(job.get("jobclient.output.filter", 
                                            "FAILED"));
  }
    
  /**
   * Modify the JobConf to set the task output filter.
   * 
   * @param job the JobConf to modify.
   * @param newValue the value to set.
   */
  public static void setTaskOutputFilter(JobConf job, 
                                         TaskStatusFilter newValue) {
    job.set("jobclient.output.filter", newValue.toString());
  }
    
  /**
   * Returns task output filter.
   * @return task filter. 
   */
  @Deprecated
  public TaskStatusFilter getTaskOutputFilter(){
    return this.taskOutputFilter; 
  }

  protected long getCounter(org.apache.hadoop.mapreduce.Counters cntrs,
      String counterGroupName, String counterName) throws IOException {
    Counters counters = Counters.downgrade(cntrs);
    return counters.findCounter(counterGroupName, counterName).getValue();
  }

  /**
   * Get status information about the max available Maps in the cluster.
   *  
   * @return the max available Maps in the cluster
   * @throws IOException
   */
  public int getDefaultMaps() throws IOException {
    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<Integer>() {
        @Override
        public Integer run() throws IOException, InterruptedException {
          return cluster.getClusterStatus().getMapSlotCapacity();
        }
      });
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Get status information about the max available Reduces in the cluster.
   *  
   * @return the max available Reduces in the cluster
   * @throws IOException
   */
  public int getDefaultReduces() throws IOException {
    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<Integer>() {
        @Override
        public Integer run() throws IOException, InterruptedException {
          return cluster.getClusterStatus().getReduceSlotCapacity();
        }
      });
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Grab the jobtracker system directory path where job-specific files are to be placed.
   * 
   * @return the system directory where job-specific files are to be placed.
   */
  public Path getSystemDir() {
    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<Path>() {
        @Override
        public Path run() throws IOException, InterruptedException {
          return cluster.getSystemDir();
        }
      });
      } catch (IOException ioe) {
      return null;
    } catch (InterruptedException ie) {
      return null;
    }
  }

  private JobQueueInfo getJobQueueInfo(QueueInfo queue) {
    JobQueueInfo ret = new JobQueueInfo(queue);
    // make sure to convert any children
    if (queue.getQueueChildren().size() > 0) {
      List<JobQueueInfo> childQueues = new ArrayList<JobQueueInfo>(queue
          .getQueueChildren().size());
      for (QueueInfo child : queue.getQueueChildren()) {
        childQueues.add(getJobQueueInfo(child));
      }
      ret.setChildren(childQueues);
    }
    return ret;
  }

  private JobQueueInfo[] getJobQueueInfoArray(QueueInfo[] queues)
      throws IOException {
    JobQueueInfo[] ret = new JobQueueInfo[queues.length];
    for (int i = 0; i < queues.length; i++) {
      ret[i] = getJobQueueInfo(queues[i]);
    }
    return ret;
  }

  /**
   * Returns an array of queue information objects about root level queues
   * configured
   *
   * @return the array of root level JobQueueInfo objects
   * @throws IOException
   */
  public JobQueueInfo[] getRootQueues() throws IOException {
    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<JobQueueInfo[]>() {
        public JobQueueInfo[] run() throws IOException, InterruptedException {
          return getJobQueueInfoArray(cluster.getRootQueues());
        }
      });
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Returns an array of queue information objects about immediate children
   * of queue queueName.
   * 
   * @param queueName
   * @return the array of immediate children JobQueueInfo objects
   * @throws IOException
   */
  public JobQueueInfo[] getChildQueues(final String queueName) throws IOException {
    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<JobQueueInfo[]>() {
        public JobQueueInfo[] run() throws IOException, InterruptedException {
          return getJobQueueInfoArray(cluster.getChildQueues(queueName));
        }
      });
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /**
   * Return an array of queue information objects about all the Job Queues
   * configured.
   * 
   * @return Array of JobQueueInfo objects
   * @throws IOException
   */
  public JobQueueInfo[] getQueues() throws IOException {
    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<JobQueueInfo[]>() {
        public JobQueueInfo[] run() throws IOException, InterruptedException {
          return getJobQueueInfoArray(cluster.getQueues());
        }
      });
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /**
   * Gets all the jobs which were added to particular Job Queue
   * 
   * @param queueName name of the Job Queue
   * @return Array of jobs present in the job queue
   * @throws IOException
   */
  
  public JobStatus[] getJobsFromQueue(final String queueName) throws IOException {
    try {
      QueueInfo queue = clientUgi.doAs(new PrivilegedExceptionAction<QueueInfo>() {
        @Override
        public QueueInfo run() throws IOException, InterruptedException {
          return cluster.getQueue(queueName);
        }
      });
      if (queue == null) {
        return null;
      }
      org.apache.hadoop.mapreduce.JobStatus[] stats = 
        queue.getJobStatuses();
      JobStatus[] ret = new JobStatus[stats.length];
      for (int i = 0 ; i < stats.length; i++ ) {
        ret[i] = JobStatus.downgrade(stats[i]);
      }
      return ret;
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /**
   * Gets the queue information associated to a particular Job Queue
   * 
   * @param queueName name of the job queue.
   * @return Queue information associated to particular queue.
   * @throws IOException
   */
  public JobQueueInfo getQueueInfo(final String queueName) throws IOException {
    try {
      QueueInfo queueInfo = clientUgi.doAs(new 
          PrivilegedExceptionAction<QueueInfo>() {
        public QueueInfo run() throws IOException, InterruptedException {
          return cluster.getQueue(queueName);
        }
      });
      if (queueInfo != null) {
        return new JobQueueInfo(queueInfo);
      }
      return null;
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  /**
   * Gets the Queue ACLs for current user
   * @return array of QueueAclsInfo object for current user.
   * @throws IOException
   */
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException {
    try {
      org.apache.hadoop.mapreduce.QueueAclsInfo[] acls = 
        clientUgi.doAs(new 
            PrivilegedExceptionAction
            <org.apache.hadoop.mapreduce.QueueAclsInfo[]>() {
              public org.apache.hadoop.mapreduce.QueueAclsInfo[] run() 
              throws IOException, InterruptedException {
                return cluster.getQueueAclsForCurrentUser();
              }
        });
      QueueAclsInfo[] ret = new QueueAclsInfo[acls.length];
      for (int i = 0 ; i < acls.length; i++ ) {
        ret[i] = QueueAclsInfo.downgrade(acls[i]);
      }
      return ret;
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Get a delegation token for the user from the JobTracker.
   * @param renewer the user who can renew the token
   * @return the new token
   * @throws IOException
   */
  public Token<DelegationTokenIdentifier> 
    getDelegationToken(final Text renewer) throws IOException, InterruptedException {
    getDelegationTokenCalled = true;
    return clientUgi.doAs(new 
        PrivilegedExceptionAction<Token<DelegationTokenIdentifier>>() {
      public Token<DelegationTokenIdentifier> run() throws IOException, 
      InterruptedException {
        return cluster.getDelegationToken(renewer);
      }
    });
  }

  /**
   * Renew a delegation token
   * @param token the token to renew
   * @return true if the renewal went well
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use {@link Token#renew} instead
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                   ) throws InvalidToken, IOException, 
                                            InterruptedException {
    return token.renew(getConf());
  }

  /**
   * Cancel a delegation token from the JobTracker
   * @param token the token to cancel
   * @throws IOException
   * @deprecated Use {@link Token#cancel} instead
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                    ) throws InvalidToken, IOException, 
                                             InterruptedException {
    token.cancel(getConf());
  }

  /**
   */
  public static void main(String argv[]) throws Exception {
    int res = ToolRunner.run(new JobClient(), argv);
    System.exit(res);
  }
}

