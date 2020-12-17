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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ReservationId;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The job submitter's view of the Job.
 * 
 * <p>It allows the user to configure the
 * job, submit it, control its execution, and query the state. The set methods
 * only work until the job is submitted, afterwards they will throw an 
 * IllegalStateException. </p>
 * 
 * <p>
 * Normally the user creates the application, describes various facets of the
 * job via {@link Job} and then submits the job and monitor its progress.</p>
 * 
 * <p>Here is an example on how to submit a job:</p>
 * <p><blockquote><pre>
 *     // Create a new Job
 *     Job job = Job.getInstance();
 *     job.setJarByClass(MyJob.class);
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
 *     job.waitForCompletion(true);
 * </pre></blockquote>
 * 
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Job extends JobContextImpl implements JobContext, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Job.class);

  @InterfaceStability.Evolving
  public enum JobState {DEFINE, RUNNING};
  private static final long MAX_JOBSTATUS_AGE = 1000 * 2;
  public static final String OUTPUT_FILTER = "mapreduce.client.output.filter";
  /** Key in mapred-*.xml that sets completionPollInvervalMillis */
  public static final String COMPLETION_POLL_INTERVAL_KEY = 
    "mapreduce.client.completion.pollinterval";
  
  /** Default completionPollIntervalMillis is 5000 ms. */
  static final int DEFAULT_COMPLETION_POLL_INTERVAL = 5000;
  /** Key in mapred-*.xml that sets progMonitorPollIntervalMillis */
  public static final String PROGRESS_MONITOR_POLL_INTERVAL_KEY =
    "mapreduce.client.progressmonitor.pollinterval";
  /** Default progMonitorPollIntervalMillis is 1000 ms. */
  static final int DEFAULT_MONITOR_POLL_INTERVAL = 1000;

  public static final String USED_GENERIC_PARSER = 
      "mapreduce.client.genericoptionsparser.used";
  public static final String SUBMIT_REPLICATION = 
      "mapreduce.client.submit.file.replication";
  public static final int DEFAULT_SUBMIT_REPLICATION = 10;
  public static final String USE_WILDCARD_FOR_LIBJARS =
      "mapreduce.client.libjars.wildcard";
  public static final boolean DEFAULT_USE_WILDCARD_FOR_LIBJARS = true;

  @InterfaceStability.Evolving
  public enum TaskStatusFilter { NONE, KILLED, FAILED, SUCCEEDED, ALL }

  static {
    ConfigUtil.loadResources();
  }

  private JobState state = JobState.DEFINE;
  private JobStatus status;
  private long statustime;
  private Cluster cluster;
  private ReservationId reservationId;

  /**
   * @deprecated Use {@link #getInstance()}
   */
  @Deprecated
  public Job() throws IOException {
    this(new JobConf(new Configuration()));
  }

  /**
   * @deprecated Use {@link #getInstance(Configuration)}
   */
  @Deprecated
  public Job(Configuration conf) throws IOException {
    this(new JobConf(conf));
  }

  /**
   * @deprecated Use {@link #getInstance(Configuration, String)}
   */
  @Deprecated
  public Job(Configuration conf, String jobName) throws IOException {
    this(new JobConf(conf));
    setJobName(jobName);
  }

  Job(JobConf conf) throws IOException {
    super(conf, null);
    // propagate existing user credentials to job
    this.credentials.mergeAll(this.ugi.getCredentials());
    this.cluster = null;
  }

  Job(JobStatus status, JobConf conf) throws IOException {
    this(conf);
    setJobID(status.getJobID());
    this.status = status;
    state = JobState.RUNNING;
  }

      
  /**
   * Creates a new {@link Job} with no particular {@link Cluster} .
   * A Cluster will be created with a generic {@link Configuration}.
   * 
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   */
  public static Job getInstance() throws IOException {
    // create with a null Cluster
    return getInstance(new Configuration());
  }
      
  /**
   * Creates a new {@link Job} with no particular {@link Cluster} and a 
   * given {@link Configuration}.
   * 
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so 
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * A Cluster will be created from the conf parameter only when it's needed.
   * 
   * @param conf the configuration
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   */
  public static Job getInstance(Configuration conf) throws IOException {
    // create with a null Cluster
    JobConf jobConf = new JobConf(conf);
    return new Job(jobConf);
  }

      
  /**
   * Creates a new {@link Job} with no particular {@link Cluster} and a given jobName.
   * A Cluster will be created from the conf parameter only when it's needed.
   *
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so 
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * @param conf the configuration
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   */
  public static Job getInstance(Configuration conf, String jobName)
           throws IOException {
    // create with a null Cluster
    Job result = getInstance(conf);
    result.setJobName(jobName);
    return result;
  }
  
  /**
   * Creates a new {@link Job} with no particular {@link Cluster} and given
   * {@link Configuration} and {@link JobStatus}.
   * A Cluster will be created from the conf parameter only when it's needed.
   * 
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so 
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * @param status job status
   * @param conf job configuration
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   */
  public static Job getInstance(JobStatus status, Configuration conf) 
  throws IOException {
    return new Job(status, new JobConf(conf));
  }

  /**
   * Creates a new {@link Job} with no particular {@link Cluster}.
   * A Cluster will be created from the conf parameter only when it's needed.
   *
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so 
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * @param ignored
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   * @deprecated Use {@link #getInstance()}
   */
  @Deprecated
  public static Job getInstance(Cluster ignored) throws IOException {
    return getInstance();
  }
  
  /**
   * Creates a new {@link Job} with no particular {@link Cluster} and given
   * {@link Configuration}.
   * A Cluster will be created from the conf parameter only when it's needed.
   * 
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so 
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * @param ignored
   * @param conf job configuration
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   * @deprecated Use {@link #getInstance(Configuration)}
   */
  @Deprecated
  public static Job getInstance(Cluster ignored, Configuration conf) 
      throws IOException {
    return getInstance(conf);
  }
  
  /**
   * Creates a new {@link Job} with no particular {@link Cluster} and given
   * {@link Configuration} and {@link JobStatus}.
   * A Cluster will be created from the conf parameter only when it's needed.
   * 
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so 
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * @param cluster cluster
   * @param status job status
   * @param conf job configuration
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   */
  @Private
  public static Job getInstance(Cluster cluster, JobStatus status, 
      Configuration conf) throws IOException {
    Job job = getInstance(status, conf);
    job.setCluster(cluster);
    return job;
  }

  private void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state "+ this.state + 
                                      " instead of " + state);
    }

    if (state == JobState.RUNNING && cluster == null) {
      throw new IllegalStateException
        ("Job in state " + this.state
         + ", but it isn't attached to any job tracker!");
    }
  }

  /**
   * Some methods rely on having a recent job status object.  Refresh
   * it, if necessary
   */
  synchronized void ensureFreshStatus() 
      throws IOException {
    if (System.currentTimeMillis() - statustime > MAX_JOBSTATUS_AGE) {
      updateStatus();
    }
  }
    
  /** Some methods need to update status immediately. So, refresh
   * immediately
   * @throws IOException
   */
  synchronized void updateStatus() throws IOException {
    try {
      this.status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
        @Override
        public JobStatus run() throws IOException, InterruptedException {
          return cluster.getClient().getJobStatus(getJobID());
        }
      });
    }
    catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    if (this.status == null) {
      throw new IOException("Job status not available ");
    }
    this.statustime = System.currentTimeMillis();
  }
  
  public JobStatus getStatus() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status;
  }

  /**
   * Returns the current state of the Job.
   * 
   * @return JobStatus#State
   * @throws IOException
   * @throws InterruptedException
   */
  public JobStatus.State getJobState() 
      throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getState();
  }
  
  /**
   * Get the URL where some job progress information will be displayed.
   * 
   * @return the URL where some job progress information will be displayed.
   */
  public String getTrackingURL(){
    ensureState(JobState.RUNNING);
    return status.getTrackingUrl().toString();
  }

  /**
   * Get the path of the submitted job configuration.
   * 
   * @return the path of the submitted job configuration.
   */
  public String getJobFile() {
    ensureState(JobState.RUNNING);
    return status.getJobFile();
  }

  /**
   * Get start time of the job.
   * 
   * @return the start time of the job
   */
  public long getStartTime() {
    ensureState(JobState.RUNNING);
    return status.getStartTime();
  }

  /**
   * Get finish time of the job.
   * 
   * @return the finish time of the job
   */
  public long getFinishTime() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getFinishTime();
  }

  /**
   * Get scheduling info of the job.
   * 
   * @return the scheduling info of the job
   */
  public String getSchedulingInfo() {
    ensureState(JobState.RUNNING);
    return status.getSchedulingInfo();
  }

  /**
   * Get scheduling info of the job.
   * 
   * @return the priority info of the job
   */
  public JobPriority getPriority() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getPriority();
  }

  /**
   * The user-specified job name.
   */
  public String getJobName() {
    if (state == JobState.DEFINE || status == null) {
      return super.getJobName();
    }
    ensureState(JobState.RUNNING);
    return status.getJobName();
  }

  public String getHistoryUrl() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getHistoryFile();
  }

  public boolean isRetired() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.isRetired();
  }
  
  @Private
  public Cluster getCluster() {
    return cluster;
  }

  /** Only for mocks in unit tests. */
  @Private
  private void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Dump stats to screen.
   */
  @Override
  public String toString() {
    ensureState(JobState.RUNNING);
    String reasonforFailure = " ";
    int numMaps = 0;
    int numReduces = 0;
    try {
      updateStatus();
      if (status.getState().equals(JobStatus.State.FAILED))
        reasonforFailure = getTaskFailureEventString();
      numMaps = getTaskReports(TaskType.MAP).length;
      numReduces = getTaskReports(TaskType.REDUCE).length;
    } catch (IOException e) {
    } catch (InterruptedException ie) {
    }
    StringBuffer sb = new StringBuffer();
    sb.append("Job: ").append(status.getJobID()).append("\n");
    sb.append("Job File: ").append(status.getJobFile()).append("\n");
    sb.append("Job Tracking URL : ").append(status.getTrackingUrl());
    sb.append("\n");
    sb.append("Uber job : ").append(status.isUber()).append("\n");
    sb.append("Number of maps: ").append(numMaps).append("\n");
    sb.append("Number of reduces: ").append(numReduces).append("\n");
    sb.append("map() completion: ");
    sb.append(status.getMapProgress()).append("\n");
    sb.append("reduce() completion: ");
    sb.append(status.getReduceProgress()).append("\n");
    sb.append("Job state: ");
    sb.append(status.getState()).append("\n");
    sb.append("retired: ").append(status.isRetired()).append("\n");
    sb.append("reason for failure: ").append(reasonforFailure);
    return sb.toString();
  }

  /**
   * @return taskid which caused job failure
   * @throws IOException
   * @throws InterruptedException
   */
  String getTaskFailureEventString() throws IOException,
      InterruptedException {
    int failCount = 1;
    TaskCompletionEvent lastEvent = null;
    TaskCompletionEvent[] events = ugi.doAs(new 
        PrivilegedExceptionAction<TaskCompletionEvent[]>() {
          @Override
          public TaskCompletionEvent[] run() throws IOException,
          InterruptedException {
            return cluster.getClient().getTaskCompletionEvents(
                status.getJobID(), 0, 10);
          }
        });
    for (TaskCompletionEvent event : events) {
      if (event.getStatus().equals(TaskCompletionEvent.Status.FAILED)) {
        failCount++;
        lastEvent = event;
      }
    }
    if (lastEvent == null) {
      return "There are no failed tasks for the job. "
          + "Job is failed due to some other reason and reason "
          + "can be found in the logs.";
    }
    String[] taskAttemptID = lastEvent.getTaskAttemptId().toString().split("_", 2);
    String taskID = taskAttemptID[1].substring(0, taskAttemptID[1].length()-2);
    return (" task " + taskID + " failed " +
      failCount + " times " + "For details check tasktracker at: " +
      lastEvent.getTaskTrackerHttp());
  }

  /**
   * Get the information of the current state of the tasks of a job.
   * 
   * @param type Type of the task
   * @return the list of all of the map tips.
   * @throws IOException
   */
  public TaskReport[] getTaskReports(TaskType type) 
      throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    final TaskType tmpType = type;
    return ugi.doAs(new PrivilegedExceptionAction<TaskReport[]>() {
      public TaskReport[] run() throws IOException, InterruptedException {
        return cluster.getClient().getTaskReports(getJobID(), tmpType);
      }
    });
  }

  /**
   * Get the <i>progress</i> of the job's map-tasks, as a float between 0.0 
   * and 1.0.  When all map tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's map-tasks.
   * @throws IOException
   */
  public float mapProgress() throws IOException {
    ensureState(JobState.RUNNING);
    ensureFreshStatus();
    return status.getMapProgress();
  }

  /**
   * Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0 
   * and 1.0.  When all reduce tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's reduce-tasks.
   * @throws IOException
   */
  public float reduceProgress() throws IOException {
    ensureState(JobState.RUNNING);
    ensureFreshStatus();
    return status.getReduceProgress();
  }

  /**
   * Get the <i>progress</i> of the job's cleanup-tasks, as a float between 0.0 
   * and 1.0.  When all cleanup tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's cleanup-tasks.
   * @throws IOException
   */
  public float cleanupProgress() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    ensureFreshStatus();
    return status.getCleanupProgress();
  }

  /**
   * Get the <i>progress</i> of the job's setup-tasks, as a float between 0.0 
   * and 1.0.  When all setup tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's setup-tasks.
   * @throws IOException
   */
  public float setupProgress() throws IOException {
    ensureState(JobState.RUNNING);
    ensureFreshStatus();
    return status.getSetupProgress();
  }

  /**
   * Check if the job is finished or not. 
   * This is a non-blocking call.
   * 
   * @return <code>true</code> if the job is complete, else <code>false</code>.
   * @throws IOException
   */
  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.isJobComplete();
  }

  /**
   * Check if the job completed successfully. 
   * 
   * @return <code>true</code> if the job succeeded, else <code>false</code>.
   * @throws IOException
   */
  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getState() == JobStatus.State.SUCCEEDED;
  }

  /**
   * Kill the running job.  Blocks until all job tasks have been
   * killed as well.  If the job is no longer running, it simply returns.
   * 
   * @throws IOException
   */
  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    try {
      cluster.getClient().killJob(getJobID());
    }
    catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Set the priority of a running job.
   * @param jobPriority the new priority for the job.
   * @throws IOException
   */
  public void setPriority(JobPriority jobPriority) throws IOException,
      InterruptedException {
    if (state == JobState.DEFINE) {
      if (jobPriority == JobPriority.UNDEFINED_PRIORITY) {
        conf.setJobPriorityAsInteger(convertPriorityToInteger(jobPriority));
      } else {
        conf.setJobPriority(org.apache.hadoop.mapred.JobPriority
            .valueOf(jobPriority.name()));
      }
    } else {
      ensureState(JobState.RUNNING);
      final int tmpPriority = convertPriorityToInteger(jobPriority);
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException, InterruptedException {
          cluster.getClient()
              .setJobPriority(getJobID(), Integer.toString(tmpPriority));
          return null;
        }
      });
    }
  }

  /**
   * Set the priority of a running job.
   *
   * @param jobPriority
   *          the new priority for the job.
   * @throws IOException
   */
  public void setPriorityAsInteger(int jobPriority) throws IOException,
      InterruptedException {
    if (state == JobState.DEFINE) {
      conf.setJobPriorityAsInteger(jobPriority);
    } else {
      ensureState(JobState.RUNNING);
      final int tmpPriority = jobPriority;
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException, InterruptedException {
          cluster.getClient()
              .setJobPriority(getJobID(), Integer.toString(tmpPriority));
          return null;
        }
      });
    }
  }

  private int convertPriorityToInteger(JobPriority jobPriority) {
    switch (jobPriority) {
    case VERY_HIGH :
      return 5;
    case HIGH :
      return 4;
    case NORMAL :
      return 3;
    case LOW :
      return 2;
    case VERY_LOW :
      return 1;
    case DEFAULT :
      return 0;
    default:
      break;
    }
    // For UNDEFINED_PRIORITY, we can set it to default for better handling
    return 0;
  }

  /**
   * Get events indicating completion (success/failure) of component tasks.
   *  
   * @param startFrom index to start fetching events from
   * @param numEvents number of events to fetch
   * @return an array of {@link TaskCompletionEvent}s
   * @throws IOException
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(final int startFrom,
      final int numEvents) throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    return ugi.doAs(new PrivilegedExceptionAction<TaskCompletionEvent[]>() {
      @Override
      public TaskCompletionEvent[] run() throws IOException, InterruptedException {
        return cluster.getClient().getTaskCompletionEvents(getJobID(),
            startFrom, numEvents); 
      }
    });
  }

  /**
   * Get events indicating completion (success/failure) of component tasks.
   *  
   * @param startFrom index to start fetching events from
   * @return an array of {@link org.apache.hadoop.mapred.TaskCompletionEvent}s
   * @throws IOException
   */
  public org.apache.hadoop.mapred.TaskCompletionEvent[]
    getTaskCompletionEvents(final int startFrom) throws IOException {
    try {
      TaskCompletionEvent[] events = getTaskCompletionEvents(startFrom, 10);
      org.apache.hadoop.mapred.TaskCompletionEvent[] retEvents =
          new org.apache.hadoop.mapred.TaskCompletionEvent[events.length];
      for (int i = 0; i < events.length; i++) {
        retEvents[i] = org.apache.hadoop.mapred.TaskCompletionEvent.downgrade
            (events[i]);
      }
      return retEvents;
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Kill indicated task attempt.
   * @param taskId the id of the task to kill.
   * @param shouldFail if <code>true</code> the task is failed and added
   *                   to failed tasks list, otherwise it is just killed,
   *                   w/o affecting job failure status.
   */
  @Private
  public boolean killTask(final TaskAttemptID taskId,
                          final boolean shouldFail) throws IOException {
    ensureState(JobState.RUNNING);
    try {
      return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
        public Boolean run() throws IOException, InterruptedException {
          return cluster.getClient().killTask(taskId, shouldFail);
        }
      });
    }
    catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Kill indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void killTask(final TaskAttemptID taskId)
      throws IOException {
    killTask(taskId, false);
  }

  /**
   * Fail indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void failTask(final TaskAttemptID taskId)
      throws IOException {
    killTask(taskId, true);
  }

  /**
   * Gets the counters for this job. May return null if the job has been
   * retired and the job is no longer in the completed job store.
   * 
   * @return the counters for this job.
   * @throws IOException
   */
  public Counters getCounters() 
      throws IOException {
    ensureState(JobState.RUNNING);
    try {
      return ugi.doAs(new PrivilegedExceptionAction<Counters>() {
        @Override
        public Counters run() throws IOException, InterruptedException {
          return cluster.getClient().getJobCounters(getJobID());
        }
      });
    }
    catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Gets the diagnostic messages for a given task attempt.
   * @param taskid
   * @return the list of diagnostic messages for the task
   * @throws IOException
   */
  public String[] getTaskDiagnostics(final TaskAttemptID taskid) 
      throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    return ugi.doAs(new PrivilegedExceptionAction<String[]>() {
      @Override
      public String[] run() throws IOException, InterruptedException {
        return cluster.getClient().getTaskDiagnostics(taskid);
      }
    });
  }

  /**
   * Set the number of reduce tasks for the job.
   * @param tasks the number of reduce tasks
   * @throws IllegalStateException if the job is submitted
   */
  public void setNumReduceTasks(int tasks) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setNumReduceTasks(tasks);
  }

  /**
   * Set the current working directory for the default file system.
   * 
   * @param dir the new current working directory.
   * @throws IllegalStateException if the job is submitted
   */
  public void setWorkingDirectory(Path dir) throws IOException {
    ensureState(JobState.DEFINE);
    conf.setWorkingDirectory(dir);
  }

  /**
   * Set the {@link InputFormat} for the job.
   * @param cls the <code>InputFormat</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setInputFormatClass(Class<? extends InputFormat> cls
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(INPUT_FORMAT_CLASS_ATTR, cls, 
                  InputFormat.class);
  }

  /**
   * Set the {@link OutputFormat} for the job.
   * @param cls the <code>OutputFormat</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputFormatClass(Class<? extends OutputFormat> cls
                                   ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(OUTPUT_FORMAT_CLASS_ATTR, cls, 
                  OutputFormat.class);
  }

  /**
   * Set the {@link Mapper} for the job.
   * @param cls the <code>Mapper</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapperClass(Class<? extends Mapper> cls
                             ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(MAP_CLASS_ATTR, cls, Mapper.class);
  }

  /**
   * Set the Jar by finding where a given class came from.
   * @param cls the example class
   */
  public void setJarByClass(Class<?> cls) {
    ensureState(JobState.DEFINE);
    conf.setJarByClass(cls);
  }

  /**
   * Set the job jar 
   */
  public void setJar(String jar) {
    ensureState(JobState.DEFINE);
    conf.setJar(jar);
  }

  /**
   * Set the reported username for this job.
   * 
   * @param user the username for this job.
   */
  public void setUser(String user) {
    ensureState(JobState.DEFINE);
    conf.setUser(user);
  }

  /**
   * Set the combiner class for the job.
   * @param cls the combiner to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setCombinerClass(Class<? extends Reducer> cls
                               ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(COMBINE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Reducer} for the job.
   * @param cls the <code>Reducer</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setReducerClass(Class<? extends Reducer> cls
                              ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(REDUCE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Partitioner} for the job.
   * @param cls the <code>Partitioner</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setPartitionerClass(Class<? extends Partitioner> cls
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(PARTITIONER_CLASS_ATTR, cls, 
                  Partitioner.class);
  }

  /**
   * Set the key class for the map output data. This allows the user to
   * specify the map output key class to be different than the final output
   * value class.
   * 
   * @param theClass the map output key class.
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapOutputKeyClass(Class<?> theClass
                                   ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setMapOutputKeyClass(theClass);
  }

  /**
   * Set the value class for the map output data. This allows the user to
   * specify the map output value class to be different than the final output
   * value class.
   * 
   * @param theClass the map output value class.
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapOutputValueClass(Class<?> theClass
                                     ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setMapOutputValueClass(theClass);
  }

  /**
   * Set the key class for the job output data.
   * 
   * @param theClass the key class for the job output data.
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputKeyClass(Class<?> theClass
                                ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputKeyClass(theClass);
  }

  /**
   * Set the value class for job outputs.
   * 
   * @param theClass the value class for job outputs.
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputValueClass(Class<?> theClass
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputValueClass(theClass);
  }

  /**
   * Define the comparator that controls which keys are grouped together
   * for a single call to combiner,
   * {@link Reducer#reduce(Object, Iterable,
   * org.apache.hadoop.mapreduce.Reducer.Context)}
   *
   * @param cls the raw comparator to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setCombinerKeyGroupingComparatorClass(
      Class<? extends RawComparator> cls) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setCombinerKeyGroupingComparator(cls);
  }

  /**
   * Define the comparator that controls how the keys are sorted before they
   * are passed to the {@link Reducer}.
   * @param cls the raw comparator
   * @throws IllegalStateException if the job is submitted
   * @see #setCombinerKeyGroupingComparatorClass(Class)
   */
  public void setSortComparatorClass(Class<? extends RawComparator> cls
                                     ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputKeyComparatorClass(cls);
  }

  /**
   * Define the comparator that controls which keys are grouped together
   * for a single call to 
   * {@link Reducer#reduce(Object, Iterable, 
   *                       org.apache.hadoop.mapreduce.Reducer.Context)}
   * @param cls the raw comparator to use
   * @throws IllegalStateException if the job is submitted
   * @see #setCombinerKeyGroupingComparatorClass(Class)
   */
  public void setGroupingComparatorClass(Class<? extends RawComparator> cls
                                         ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputValueGroupingComparator(cls);
  }

  /**
   * Set the user-specified job name.
   * 
   * @param name the job's new name.
   * @throws IllegalStateException if the job is submitted
   */
  public void setJobName(String name) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setJobName(name);
  }

  /**
   * Turn speculative execution on or off for this job. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on, else <code>false</code>.
   */
  public void setSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setSpeculativeExecution(speculativeExecution);
  }

  /**
   * Turn speculative execution on or off for this job for map tasks. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on for map tasks,
   *                             else <code>false</code>.
   */
  public void setMapSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setMapSpeculativeExecution(speculativeExecution);
  }

  /**
   * Turn speculative execution on or off for this job for reduce tasks. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on for reduce tasks,
   *                             else <code>false</code>.
   */
  public void setReduceSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setReduceSpeculativeExecution(speculativeExecution);
  }

  /**
   * Specify whether job-setup and job-cleanup is needed for the job 
   * 
   * @param needed If <code>true</code>, job-setup and job-cleanup will be
   *               considered from {@link OutputCommitter} 
   *               else ignored.
   */
  public void setJobSetupCleanupNeeded(boolean needed) {
    ensureState(JobState.DEFINE);
    conf.setBoolean(SETUP_CLEANUP_NEEDED, needed);
  }

  /**
   * Set the given set of archives
   * @param archives The list of archives that need to be localized
   */
  public void setCacheArchives(URI[] archives) {
    ensureState(JobState.DEFINE);
    DistributedCache.setCacheArchives(archives, conf);
  }

  /**
   * Set the given set of files
   * @param files The list of files that need to be localized
   */
  public void setCacheFiles(URI[] files) {
    ensureState(JobState.DEFINE);
    DistributedCache.setCacheFiles(files, conf);
  }

  /**
   * Add a archives to be localized
   * @param uri The uri of the cache to be localized
   */
  public void addCacheArchive(URI uri) {
    ensureState(JobState.DEFINE);
    DistributedCache.addCacheArchive(uri, conf);
  }
  
  /**
   * Add a file to be localized
   * @param uri The uri of the cache to be localized
   */
  public void addCacheFile(URI uri) {
    ensureState(JobState.DEFINE);
    DistributedCache.addCacheFile(uri, conf);
  }

  /**
   * Add an file path to the current set of classpath entries It adds the file
   * to cache as well.
   * 
   * Files added with this method will not be unpacked while being added to the
   * classpath.
   * To add archives to classpath, use the {@link #addArchiveToClassPath(Path)}
   * method instead.
   *
   * @param file Path of the file to be added
   */
  public void addFileToClassPath(Path file)
    throws IOException {
    ensureState(JobState.DEFINE);
    DistributedCache.addFileToClassPath(file, conf, file.getFileSystem(conf));
  }

  /**
   * Add an archive path to the current set of classpath entries. It adds the
   * archive to cache as well.
   * 
   * Archive files will be unpacked and added to the classpath
   * when being distributed.
   *
   * @param archive Path of the archive to be added
   */
  public void addArchiveToClassPath(Path archive)
    throws IOException {
    ensureState(JobState.DEFINE);
    DistributedCache.addArchiveToClassPath(archive, conf, archive.getFileSystem(conf));
  }

  /**
   * Originally intended to enable symlinks, but currently symlinks cannot be
   * disabled.
   */
  @Deprecated
  public void createSymlink() {
    ensureState(JobState.DEFINE);
    DistributedCache.createSymlink(conf);
  }
  
  /** 
   * Expert: Set the number of maximum attempts that will be made to run a
   * map task.
   * 
   * @param n the number of attempts per map task.
   */
  public void setMaxMapAttempts(int n) {
    ensureState(JobState.DEFINE);
    conf.setMaxMapAttempts(n);
  }

  /** 
   * Expert: Set the number of maximum attempts that will be made to run a
   * reduce task.
   * 
   * @param n the number of attempts per reduce task.
   */
  public void setMaxReduceAttempts(int n) {
    ensureState(JobState.DEFINE);
    conf.setMaxReduceAttempts(n);
  }

  /**
   * Set whether the system should collect profiler information for some of 
   * the tasks in this job? The information is stored in the user log 
   * directory.
   * @param newValue true means it should be gathered
   */
  public void setProfileEnabled(boolean newValue) {
    ensureState(JobState.DEFINE);
    conf.setProfileEnabled(newValue);
  }

  /**
   * Set the profiler configuration arguments. If the string contains a '%s' it
   * will be replaced with the name of the profiling output file when the task
   * runs.
   *
   * This value is passed to the task child JVM on the command line.
   *
   * @param value the configuration string
   */
  public void setProfileParams(String value) {
    ensureState(JobState.DEFINE);
    conf.setProfileParams(value);
  }

  /**
   * Set the ranges of maps or reduces to profile. setProfileEnabled(true) 
   * must also be called.
   * @param newValue a set of integer ranges of the map ids
   */
  public void setProfileTaskRange(boolean isMap, String newValue) {
    ensureState(JobState.DEFINE);
    conf.setProfileTaskRange(isMap, newValue);
  }

  private void ensureNotSet(String attr, String msg) throws IOException {
    if (conf.get(attr) != null) {
      throw new IOException(attr + " is incompatible with " + msg + " mode.");
    }    
  }
  
  /**
   * Sets the flag that will allow the JobTracker to cancel the HDFS delegation
   * tokens upon job completion. Defaults to true.
   */
  public void setCancelDelegationTokenUponJobCompletion(boolean value) {
    ensureState(JobState.DEFINE);
    conf.setBoolean(JOB_CANCEL_DELEGATION_TOKEN, value);
  }

  /**
   * Default to the new APIs unless they are explicitly set or the old mapper or
   * reduce attributes are used.
   * @throws IOException if the configuration is inconsistent
   */
  private void setUseNewAPI() throws IOException {
    int numReduces = conf.getNumReduceTasks();
    String oldMapperClass = "mapred.mapper.class";
    String oldReduceClass = "mapred.reducer.class";
    conf.setBooleanIfUnset("mapred.mapper.new-api",
                           conf.get(oldMapperClass) == null);
    if (conf.getUseNewMapper()) {
      String mode = "new map API";
      ensureNotSet("mapred.input.format.class", mode);
      ensureNotSet(oldMapperClass, mode);
      if (numReduces != 0) {
        ensureNotSet("mapred.partitioner.class", mode);
       } else {
        ensureNotSet("mapred.output.format.class", mode);
      }      
    } else {
      String mode = "map compatibility";
      ensureNotSet(INPUT_FORMAT_CLASS_ATTR, mode);
      ensureNotSet(MAP_CLASS_ATTR, mode);
      if (numReduces != 0) {
        ensureNotSet(PARTITIONER_CLASS_ATTR, mode);
       } else {
        ensureNotSet(OUTPUT_FORMAT_CLASS_ATTR, mode);
      }
    }
    if (numReduces != 0) {
      conf.setBooleanIfUnset("mapred.reducer.new-api",
                             conf.get(oldReduceClass) == null);
      if (conf.getUseNewReducer()) {
        String mode = "new reduce API";
        ensureNotSet("mapred.output.format.class", mode);
        ensureNotSet(oldReduceClass, mode);   
      } else {
        String mode = "reduce compatibility";
        ensureNotSet(OUTPUT_FORMAT_CLASS_ATTR, mode);
        ensureNotSet(REDUCE_CLASS_ATTR, mode);   
      }
    }   
  }

  /**
   * Add a file to job config for shared cache processing. If shared cache is
   * enabled, it will return true, otherwise, return false. We don't check with
   * SCM here given application might not be able to provide the job id;
   * ClientSCMProtocol.use requires the application id. Job Submitter will read
   * the files from job config and take care of things.
   *
   * @param resource The resource that Job Submitter will process later using
   *          shared cache.
   * @param conf Configuration to add the resource to
   * @return whether the resource has been added to the configuration
   */
  @Unstable
  public static boolean addFileToSharedCache(URI resource, Configuration conf) {
    SharedCacheConfig scConfig = new SharedCacheConfig();
    scConfig.init(conf);
    if (scConfig.isSharedCacheFilesEnabled()) {
      String files = conf.get(MRJobConfig.FILES_FOR_SHARED_CACHE);
      conf.set(
          MRJobConfig.FILES_FOR_SHARED_CACHE,
          files == null ? resource.toString() : files + ","
              + resource.toString());
      return true;
    } else {
      return false;
    }
  }

  /**
   * Add a file to job config for shared cache processing. If shared cache is
   * enabled, it will return true, otherwise, return false. We don't check with
   * SCM here given application might not be able to provide the job id;
   * ClientSCMProtocol.use requires the application id. Job Submitter will read
   * the files from job config and take care of things. Job Submitter will also
   * add the file to classpath. Intended to be used by user code.
   *
   * @param resource The resource that Job Submitter will process later using
   *          shared cache.
   * @param conf Configuration to add the resource to
   * @return whether the resource has been added to the configuration
   */
  @Unstable
  public static boolean addFileToSharedCacheAndClasspath(URI resource,
      Configuration conf) {
    SharedCacheConfig scConfig = new SharedCacheConfig();
    scConfig.init(conf);
    if (scConfig.isSharedCacheLibjarsEnabled()) {
      String files =
          conf.get(MRJobConfig.FILES_FOR_CLASSPATH_AND_SHARED_CACHE);
      conf.set(
          MRJobConfig.FILES_FOR_CLASSPATH_AND_SHARED_CACHE,
          files == null ? resource.toString() : files + ","
              + resource.toString());
      return true;
    } else {
      return false;
    }
  }

  /**
   * Add an archive to job config for shared cache processing. If shared cache
   * is enabled, it will return true, otherwise, return false. We don't check
   * with SCM here given application might not be able to provide the job id;
   * ClientSCMProtocol.use requires the application id. Job Submitter will read
   * the files from job config and take care of things. Intended to be used by
   * user code.
   *
   * @param resource The resource that Job Submitter will process later using
   *          shared cache.
   * @param conf Configuration to add the resource to
   * @return whether the resource has been added to the configuration
   */
  @Unstable
  public static boolean addArchiveToSharedCache(URI resource,
      Configuration conf) {
    SharedCacheConfig scConfig = new SharedCacheConfig();
    scConfig.init(conf);
    if (scConfig.isSharedCacheArchivesEnabled()) {
      String files = conf.get(MRJobConfig.ARCHIVES_FOR_SHARED_CACHE);
      conf.set(
          MRJobConfig.ARCHIVES_FOR_SHARED_CACHE,
          files == null ? resource.toString() : files + ","
              + resource.toString());
      return true;
    } else {
      return false;
    }
  }

  /**
   * This is to set the shared cache upload policies for files. If the parameter
   * was previously set, this method will replace the old value with the new
   * provided map.
   *
   * @param conf Configuration which stores the shared cache upload policies
   * @param policies A map containing the shared cache upload policies for a set
   *          of resources. The key is the url of the resource and the value is
   *          the upload policy. True if it should be uploaded, false otherwise.
   */
  @Unstable
  public static void setFileSharedCacheUploadPolicies(Configuration conf,
      Map<String, Boolean> policies) {
    setSharedCacheUploadPolicies(conf, policies, true);
  }

  /**
   * This is to set the shared cache upload policies for archives. If the
   * parameter was previously set, this method will replace the old value with
   * the new provided map.
   *
   * @param conf Configuration which stores the shared cache upload policies
   * @param policies A map containing the shared cache upload policies for a set
   *          of resources. The key is the url of the resource and the value is
   *          the upload policy. True if it should be uploaded, false otherwise.
   */
  @Unstable
  public static void setArchiveSharedCacheUploadPolicies(Configuration conf,
      Map<String, Boolean> policies) {
    setSharedCacheUploadPolicies(conf, policies, false);
  }

  // We use a double colon because a colon is a reserved character in a URI and
  // there should not be two colons next to each other.
  private static final String DELIM = "::";

  /**
   * Set the shared cache upload policies config parameter. This is done by
   * serializing the provided map of shared cache upload policies into a config
   * parameter. If the parameter was previously set, this method will replace
   * the old value with the new provided map.
   *
   * @param conf Configuration which stores the shared cache upload policies
   * @param policies A map containing the shared cache upload policies for a set
   *          of resources. The key is the url of the resource and the value is
   *          the upload policy. True if it should be uploaded, false otherwise.
   * @param areFiles True if these policies are for files, false if they are for
   *          archives.
   */
  private static void setSharedCacheUploadPolicies(Configuration conf,
      Map<String, Boolean> policies, boolean areFiles) {
    String confParam = areFiles ?
        MRJobConfig.CACHE_FILES_SHARED_CACHE_UPLOAD_POLICIES :
        MRJobConfig.CACHE_ARCHIVES_SHARED_CACHE_UPLOAD_POLICIES;
    // If no policy is provided, we will reset the config by setting an empty
    // string value. In other words, cleaning up existing policies. This is
    // useful when we try to clean up shared cache upload policies for
    // non-application master tasks. See MAPREDUCE-7294 for details.
    if (policies == null || policies.size() == 0) {
      conf.set(confParam, "");
      return;
    }
    StringBuilder sb = new StringBuilder();
    policies.forEach((k,v) -> sb.append(k).append(DELIM).append(v).append(","));
    sb.deleteCharAt(sb.length() - 1);
    conf.set(confParam, sb.toString());
  }

  /**
   * Deserialize a map of shared cache upload policies from a config parameter.
   *
   * @param conf Configuration which stores the shared cache upload policies
   * @param areFiles True if these policies are for files, false if they are for
   *          archives.
   * @return A map containing the shared cache upload policies for a set of
   *         resources. The key is the url of the resource and the value is the
   *         upload policy. True if it should be uploaded, false otherwise.
   */
  private static Map<String, Boolean> getSharedCacheUploadPolicies(
      Configuration conf, boolean areFiles) {
    String confParam =
        areFiles ? MRJobConfig.CACHE_FILES_SHARED_CACHE_UPLOAD_POLICIES
            : MRJobConfig.CACHE_ARCHIVES_SHARED_CACHE_UPLOAD_POLICIES;
    Collection<String> policies = conf.getStringCollection(confParam);
    String[] policy;
    Map<String, Boolean> policyMap = new LinkedHashMap<String, Boolean>();
    for (String s : policies) {
      policy = s.split(DELIM);
      if (policy.length != 2) {
        LOG.error(confParam
            + " is mis-formatted, returning empty shared cache upload policies."
            + " Error on [" + s + "]");
        return new LinkedHashMap<String, Boolean>();
      }
      policyMap.put(policy[0], Boolean.parseBoolean(policy[1]));
    }
    return policyMap;
  }

  /**
   * This is to get the shared cache upload policies for files.
   *
   * @param conf Configuration which stores the shared cache upload policies
   * @return A map containing the shared cache upload policies for a set of
   *         resources. The key is the url of the resource and the value is the
   *         upload policy. True if it should be uploaded, false otherwise.
   */
  @Unstable
  public static Map<String, Boolean> getFileSharedCacheUploadPolicies(
      Configuration conf) {
    return getSharedCacheUploadPolicies(conf, true);
  }

  /**
   * This is to get the shared cache upload policies for archives.
   *
   * @param conf Configuration which stores the shared cache upload policies
   * @return A map containing the shared cache upload policies for a set of
   *         resources. The key is the url of the resource and the value is the
   *         upload policy. True if it should be uploaded, false otherwise.
   */
  @Unstable
  public static Map<String, Boolean> getArchiveSharedCacheUploadPolicies(
      Configuration conf) {
    return getSharedCacheUploadPolicies(conf, false);
  }

  /** Only for mocking via unit tests. */
  @Private
  @VisibleForTesting
  synchronized void connect()
          throws IOException, InterruptedException, ClassNotFoundException {
    if (cluster == null) {
      cluster = 
        ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
                   public Cluster run()
                          throws IOException, InterruptedException, 
                                 ClassNotFoundException {
                     return new Cluster(getConfiguration());
                   }
                 });
    }
  }

  boolean isConnected() {
    return cluster != null;
  }

  /** Only for mocking via unit tests. */
  @Private
  @VisibleForTesting
  JobSubmitter getJobSubmitter(FileSystem fs,
      ClientProtocol submitClient) throws IOException {
    return new JobSubmitter(fs, submitClient);
  }
  /**
   * Submit the job to the cluster and return immediately.
   * @throws IOException
   */
  public void submit() 
         throws IOException, InterruptedException, ClassNotFoundException {
    ensureState(JobState.DEFINE);
    setUseNewAPI();
    connect();
    final JobSubmitter submitter = 
        getJobSubmitter(cluster.getFileSystem(), cluster.getClient());
    status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
      public JobStatus run() throws IOException, InterruptedException, 
      ClassNotFoundException {
        return submitter.submitJobInternal(Job.this, cluster);
      }
    });
    state = JobState.RUNNING;
    LOG.info("The url to track the job: " + getTrackingURL());
   }
  
  /**
   * Submit the job to the cluster and wait for it to finish.
   * @param verbose print the progress to the user
   * @return true if the job succeeded
   * @throws IOException thrown if the communication with the 
   *         <code>JobTracker</code> is lost
   */
  public boolean waitForCompletion(boolean verbose
                                   ) throws IOException, InterruptedException,
                                            ClassNotFoundException {
    if (state == JobState.DEFINE) {
      submit();
    }
    if (verbose) {
      monitorAndPrintJob();
    } else {
      // get the completion poll interval from the client.
      int completionPollIntervalMillis = 
        Job.getCompletionPollInterval(cluster.getConf());
      while (!isComplete()) {
        try {
          Thread.sleep(completionPollIntervalMillis);
        } catch (InterruptedException ie) {
        }
      }
    }
    return isSuccessful();
  }
  
  /**
   * Monitor a job and print status in real-time as progress is made and tasks 
   * fail.
   * @return true if the job succeeded
   * @throws IOException if communication to the JobTracker fails
   */
  public boolean monitorAndPrintJob() 
      throws IOException, InterruptedException {
    String lastReport = null;
    Job.TaskStatusFilter filter;
    Configuration clientConf = getConfiguration();
    filter = Job.getTaskOutputFilter(clientConf);
    JobID jobId = getJobID();
    LOG.info("Running job: " + jobId);
    int eventCounter = 0;
    boolean profiling = getProfileEnabled();
    IntegerRanges mapRanges = getProfileTaskRange(true);
    IntegerRanges reduceRanges = getProfileTaskRange(false);
    int progMonitorPollIntervalMillis = 
      Job.getProgressPollInterval(clientConf);
    /* make sure to report full progress after the job is done */
    boolean reportedAfterCompletion = false;
    boolean reportedUberMode = false;
    while (!isComplete() || !reportedAfterCompletion) {
      if (isComplete()) {
        reportedAfterCompletion = true;
      } else {
        Thread.sleep(progMonitorPollIntervalMillis);
      }
      if (status.getState() == JobStatus.State.PREP) {
        continue;
      }      
      if (!reportedUberMode) {
        reportedUberMode = true;
        LOG.info("Job " + jobId + " running in uber mode : " + isUber());
      }      
      String report = 
        (" map " + StringUtils.formatPercent(mapProgress(), 0)+
            " reduce " + 
            StringUtils.formatPercent(reduceProgress(), 0));
      if (!report.equals(lastReport)) {
        LOG.info(report);
        lastReport = report;
      }

      TaskCompletionEvent[] events = 
        getTaskCompletionEvents(eventCounter, 10); 
      eventCounter += events.length;
      printTaskEvents(events, filter, profiling, mapRanges, reduceRanges);
    }
    boolean success = isSuccessful();
    if (success) {
      LOG.info("Job " + jobId + " completed successfully");
    } else {
      LOG.info("Job " + jobId + " failed with state " + status.getState() + 
          " due to: " + status.getFailureInfo());
    }
    Counters counters = getCounters();
    if (counters != null) {
      LOG.info(counters.toString());
    }
    return success;
  }

  private void printTaskEvents(TaskCompletionEvent[] events,
      Job.TaskStatusFilter filter, boolean profiling, IntegerRanges mapRanges,
      IntegerRanges reduceRanges) throws IOException, InterruptedException {
    for (TaskCompletionEvent event : events) {
      switch (filter) {
      case NONE:
        break;
      case SUCCEEDED:
        if (event.getStatus() == 
          TaskCompletionEvent.Status.SUCCEEDED) {
          LOG.info(event.toString());
        }
        break; 
      case FAILED:
        if (event.getStatus() == 
          TaskCompletionEvent.Status.FAILED) {
          LOG.info(event.toString());
          // Displaying the task diagnostic information
          TaskAttemptID taskId = event.getTaskAttemptId();
          String[] taskDiagnostics = getTaskDiagnostics(taskId); 
          if (taskDiagnostics != null) {
            for (String diagnostics : taskDiagnostics) {
              System.err.println(diagnostics);
            }
          }
        }
        break; 
      case KILLED:
        if (event.getStatus() == TaskCompletionEvent.Status.KILLED){
          LOG.info(event.toString());
        }
        break; 
      case ALL:
        LOG.info(event.toString());
        break;
      }
    }
  }

  /** The interval at which monitorAndPrintJob() prints status */
  public static int getProgressPollInterval(Configuration conf) {
    // Read progress monitor poll interval from config. Default is 1 second.
    int progMonitorPollIntervalMillis = conf.getInt(
      PROGRESS_MONITOR_POLL_INTERVAL_KEY, DEFAULT_MONITOR_POLL_INTERVAL);
    if (progMonitorPollIntervalMillis < 1) {
      LOG.warn(PROGRESS_MONITOR_POLL_INTERVAL_KEY + 
        " has been set to an invalid value; "
        + " replacing with " + DEFAULT_MONITOR_POLL_INTERVAL);
      progMonitorPollIntervalMillis = DEFAULT_MONITOR_POLL_INTERVAL;
    }
    return progMonitorPollIntervalMillis;
  }

  /** The interval at which waitForCompletion() should check. */
  public static int getCompletionPollInterval(Configuration conf) {
    int completionPollIntervalMillis = conf.getInt(
      COMPLETION_POLL_INTERVAL_KEY, DEFAULT_COMPLETION_POLL_INTERVAL);
    if (completionPollIntervalMillis < 1) { 
      LOG.warn(COMPLETION_POLL_INTERVAL_KEY + 
       " has been set to an invalid value; "
       + "replacing with " + DEFAULT_COMPLETION_POLL_INTERVAL);
      completionPollIntervalMillis = DEFAULT_COMPLETION_POLL_INTERVAL;
    }
    return completionPollIntervalMillis;
  }

  /**
   * Get the task output filter.
   * 
   * @param conf the configuration.
   * @return the filter level.
   */
  public static TaskStatusFilter getTaskOutputFilter(Configuration conf) {
    return TaskStatusFilter.valueOf(conf.get(Job.OUTPUT_FILTER, "FAILED"));
  }

  /**
   * Modify the Configuration to set the task output filter.
   * 
   * @param conf the Configuration to modify.
   * @param newValue the value to set.
   */
  public static void setTaskOutputFilter(Configuration conf, 
      TaskStatusFilter newValue) {
    conf.set(Job.OUTPUT_FILTER, newValue.toString());
  }

  public boolean isUber() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.isUber();
  }

  /**
   * Get the reservation to which the job is submitted to, if any
   *
   * @return the reservationId the identifier of the job's reservation, null if
   *         the job does not have any reservation associated with it
   */
  public ReservationId getReservationId() {
    return reservationId;
  }

  /**
   * Set the reservation to which the job is submitted to
   *
   * @param reservationId the reservationId to set
   */
  public void setReservationId(ReservationId reservationId) {
    this.reservationId = reservationId;
  }
  
  /**
   * Close the <code>Job</code>.
   * @throws IOException if fail to close.
   */
  @Override
  public void close() throws IOException {
    if (cluster != null) {
      cluster.close();
      cluster = null;
    }
  }
}
