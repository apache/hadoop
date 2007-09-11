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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*******************************************************
 * JobClient interacts with the JobTracker network interface.
 * This object implements the job-control interface, and
 * should be the primary method by which user programs interact
 * with the networked job system.
 *
 *******************************************************/
public class JobClient extends Configured implements MRConstants, Tool  {
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.JobClient");
  public static enum TaskStatusFilter { NONE, FAILED, SUCCEEDED, ALL }
  private TaskStatusFilter taskOutputFilter = TaskStatusFilter.FAILED; 

  static long MAX_JOBPROFILE_AGE = 1000 * 2;

  /**
   * A NetworkedJob is an implementation of RunningJob.  It holds
   * a JobProfile object to provide some info, and interacts with the
   * remote service to provide certain functionality.
   */
  class NetworkedJob implements RunningJob {
    JobProfile profile;
    JobStatus status;
    long statustime;

    /**
     * We store a JobProfile and a timestamp for when we last
     * acquired the job profile.  If the job is null, then we cannot
     * perform any of the tasks.  The job might be null if the JobTracker
     * has completely forgotten about the job.  (eg, 24 hours after the
     * job completes.)
     */
    public NetworkedJob(JobStatus job) throws IOException {
      this.status = job;
      this.profile = jobSubmitClient.getJobProfile(job.getJobId());
      this.statustime = System.currentTimeMillis();
    }

    /**
     * Some methods rely on having a recent job profile object.  Refresh
     * it, if necessary
     */
    synchronized void ensureFreshStatus() throws IOException {
      if (System.currentTimeMillis() - statustime > MAX_JOBPROFILE_AGE) {
        this.status = jobSubmitClient.getJobStatus(profile.getJobId());
        this.statustime = System.currentTimeMillis();
      }
    }

    /**
     * An identifier for the job
     */
    public String getJobID() {
      return profile.getJobId();
    }
    
    /**
     * The user-specified job name
     */
    public String getJobName() {
      return profile.getJobName();
    }

    /**
     * The name of the job file
     */
    public String getJobFile() {
      return profile.getJobFile();
    }

    /**
     * A URL where the job's status can be seen
     */
    public String getTrackingURL() {
      return profile.getURL().toString();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of map work
     * completed.
     */
    public float mapProgress() throws IOException {
      ensureFreshStatus();
      return status.mapProgress();
    }

    /**
     * A float between 0.0 and 1.0, indicating the % of reduce work
     * completed.
     */
    public float reduceProgress() throws IOException {
      ensureFreshStatus();
      return status.reduceProgress();
    }

    /**
     * Returns immediately whether the whole job is done yet or not.
     */
    public synchronized boolean isComplete() throws IOException {
      ensureFreshStatus();
      return (status.getRunState() == JobStatus.SUCCEEDED ||
              status.getRunState() == JobStatus.FAILED);
    }

    /**
     * True iff job completed successfully.
     */
    public synchronized boolean isSuccessful() throws IOException {
      ensureFreshStatus();
      return status.getRunState() == JobStatus.SUCCEEDED;
    }

    /**
     * Blocks until the job is finished
     */
    public void waitForCompletion() throws IOException {
      while (!isComplete()) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
        }
      }
    }

    /**
     * Tells the service to terminate the current job.
     */
    public synchronized void killJob() throws IOException {
      jobSubmitClient.killJob(getJobID());
    }
    
    /**
     * Kill indicated task attempt.
     * @param taskId the id of the task to kill.
     * @param shouldFail if true the task is failed and added to failed tasks list, otherwise
     * it is just killed, w/o affecting job failure status.
     */
    public synchronized void killTask(String taskId, boolean shouldFail) throws IOException {
      jobSubmitClient.killTask(taskId, shouldFail);
    }

    /**
     * Fetch task completion events from jobtracker for this job. 
     */
    public synchronized TaskCompletionEvent[] getTaskCompletionEvents(
                                                                      int startFrom) throws IOException{
      return jobSubmitClient.getTaskCompletionEvents(
                                                     getJobID(), startFrom, 10); 
    }

    /**
     * Dump stats to screen
     */
    @Override
    public String toString() {
      try {
        ensureFreshStatus();
      } catch (IOException e) {
      }
      return "Job: " + profile.getJobId() + "\n" + 
        "file: " + profile.getJobFile() + "\n" + 
        "tracking URL: " + profile.getURL() + "\n" + 
        "map() completion: " + status.mapProgress() + "\n" + 
        "reduce() completion: " + status.reduceProgress();
    }
        
    /**
     * Returns the counters for this job
     */
    public Counters getCounters() throws IOException {
      return jobSubmitClient.getJobCounters(getJobID());
    }
  }

  JobSubmissionProtocol jobSubmitClient;
  FileSystem fs = null;

  static Random r = new Random();

  /**
   * Build a job client, connect to the default job tracker
   */
  public JobClient() {
  }
    
  public JobClient(JobConf conf) throws IOException {
    setConf(conf);
    init(conf);
  }
    
  public void init(JobConf conf) throws IOException {
    String tracker = conf.get("mapred.job.tracker", "local");
    if ("local".equals(tracker)) {
      this.jobSubmitClient = new LocalJobRunner(conf);
    } else {
      this.jobSubmitClient = createProxy(JobTracker.getAddress(conf), conf);
    }        
  }

  /**
   * Create a proxy JobSubmissionProtocol that retries timeouts.
   * @param addr the address to connect to
   * @param conf the server's configuration
   * @return a proxy object that will retry timeouts
   * @throws IOException
   */
  private JobSubmissionProtocol createProxy(InetSocketAddress addr,
                                            Configuration conf
                                            ) throws IOException {
    JobSubmissionProtocol raw = (JobSubmissionProtocol) 
      RPC.getProxy(JobSubmissionProtocol.class,
                   JobSubmissionProtocol.versionID, addr, conf);
    RetryPolicy backoffPolicy =
      RetryPolicies.retryUpToMaximumCountWithProportionalSleep
      (5, 10, java.util.concurrent.TimeUnit.SECONDS);
    Map<Class<? extends Exception>, RetryPolicy> handlers = 
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    handlers.put(SocketTimeoutException.class, backoffPolicy);
    RetryPolicy backoffTimeOuts = 
      RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL,handlers);
    return (JobSubmissionProtocol)
      RetryProxy.create(JobSubmissionProtocol.class, raw, backoffTimeOuts);
  }

  /**
   * Build a job client, connect to the indicated job tracker.
   */
  public JobClient(InetSocketAddress jobTrackAddr, 
                   Configuration conf) throws IOException {
    jobSubmitClient = createProxy(jobTrackAddr, conf);
  }

  /**
   */
  public synchronized void close() throws IOException {
  }

  /**
   * Get a filesystem handle.  We need this to prepare jobs
   * for submission to the MapReduce system.
   */
  public synchronized FileSystem getFs() throws IOException {
    if (this.fs == null) {
      String fsName = jobSubmitClient.getFilesystemName();
      this.fs = FileSystem.getNamed(fsName, getConf());
    }
    return fs;
  }

  /**
   * Submit a job to the MR system
   */
  public RunningJob submitJob(String jobFile) throws FileNotFoundException, 
                                                     InvalidJobConfException, IOException {
    // Load in the submitted job details
    JobConf job = new JobConf(jobFile);
    return submitJob(job);
  }
    
   
  /**
   * Submit a job to the MR system
   */
  public RunningJob submitJob(JobConf job) throws FileNotFoundException, 
                                                  InvalidJobConfException, IOException {
    //
    // First figure out what fs the JobTracker is using.  Copy the
    // job to it, under a temporary name.  This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading 
    // semantics.  (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    String jobId = jobSubmitClient.getNewJobId();
    Path submitJobDir = new Path(job.getSystemDir(), jobId);
    FileSystem fs = getFs();
    LOG.debug("default FileSystem: " + fs.getUri());
    fs.delete(submitJobDir);    
    Path submitJobFile = new Path(submitJobDir, "job.xml");
    Path submitJarFile = new Path(submitJobDir, "job.jar");
    Path submitSplitFile = new Path(submitJobDir, "job.split");
        
    // set the timestamps of the archives and files
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    if (tarchives != null) {
      StringBuffer archiveTimestamps = 
        new StringBuffer(String.valueOf(DistributedCache.getTimestamp(job, tarchives[0])));
      for (int i = 1; i < tarchives.length; i++) {
        archiveTimestamps.append(",");
        archiveTimestamps.append(String.valueOf(DistributedCache.getTimestamp(job, tarchives[i])));
      }
      DistributedCache.setArchiveTimestamps(job, archiveTimestamps.toString());
    }

    URI[] tfiles = DistributedCache.getCacheFiles(job);
    if (tfiles != null) {
      StringBuffer fileTimestamps = 
        new StringBuffer(String.valueOf(DistributedCache.getTimestamp(job, tfiles[0])));
      for (int i = 1; i < tfiles.length; i++) {
        fileTimestamps.append(",");
        fileTimestamps.append(String.valueOf(DistributedCache.getTimestamp(job, tfiles[i])));
      }
      DistributedCache.setFileTimestamps(job, fileTimestamps.toString());
    }
       
    String originalJarPath = job.getJar();
    short replication = (short)job.getInt("mapred.submit.replication", 10);

    if (originalJarPath != null) {           // copy jar to JobTracker's fs
      // use jar name if job is not named. 
      if ("".equals(job.getJobName())){
        job.setJobName(new Path(originalJarPath).getName());
      }
      job.setJar(submitJarFile.toString());
      fs.copyFromLocalFile(new Path(originalJarPath), submitJarFile);
      fs.setReplication(submitJarFile, replication);
    }

    // Set the user's name and working directory
    String user = System.getProperty("user.name");
    job.setUser(user != null ? user : "Dr Who");
    if (job.getWorkingDirectory() == null) {
      job.setWorkingDirectory(fs.getWorkingDirectory());          
    }

    // Check the input specification 
    job.getInputFormat().validateInput(job);

    // Check the output specification
    job.getOutputFormat().checkOutputSpecs(fs, job);

    // Create the splits for the job
    LOG.debug("Creating splits at " + fs.makeQualified(submitSplitFile));
    InputSplit[] splits = 
      job.getInputFormat().getSplits(job, job.getNumMapTasks());
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new Comparator<InputSplit>() {
      public int compare(InputSplit a, InputSplit b) {
        try {
          long left = a.getLength();
          long right = b.getLength();
          if (left == right) {
            return 0;
          } else if (left < right) {
            return 1;
          } else {
            return -1;
          }
        } catch (IOException ie) {
          throw new RuntimeException("Problem getting input split size",
                                     ie);
        }
      }
    });
    // write the splits to a file for the job tracker
    FSDataOutputStream out = fs.create(submitSplitFile);
    try {
      writeSplitsFile(splits, out);
    } finally {
      out.close();
    }
    job.set("mapred.job.split.file", submitSplitFile.toString());
    job.setNumMapTasks(splits.length);
        
    // Write job file to JobTracker's fs        
    out = fs.create(submitJobFile, replication);
    try {
      job.write(out);
    } finally {
      out.close();
    }

    //
    // Now, actually submit the job (using the submit name)
    //
    JobStatus status = jobSubmitClient.submitJob(jobId);
    if (status != null) {
      return new NetworkedJob(status);
    } else {
      throw new IOException("Could not launch job");
    }
  }

  static class RawSplit implements Writable {
    private String splitClass;
    private BytesWritable bytes = new BytesWritable();
    private String[] locations;
      
    public void setBytes(byte[] data, int offset, int length) {
      bytes.set(data, offset, length);
    }

    public void setClassName(String className) {
      splitClass = className;
    }
      
    public String getClassName() {
      return splitClass;
    }
      
    public BytesWritable getBytes() {
      return bytes;
    }
      
    public void setLocations(String[] locations) {
      this.locations = locations;
    }
      
    public String[] getLocations() {
      return locations;
    }
      
    public void readFields(DataInput in) throws IOException {
      splitClass = Text.readString(in);
      bytes.readFields(in);
      int len = WritableUtils.readVInt(in);
      locations = new String[len];
      for(int i=0; i < len; ++i) {
        locations[i] = Text.readString(in);
      }
    }
      
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, splitClass);
      bytes.write(out);
      WritableUtils.writeVInt(out, locations.length);
      for(int i = 0; i < locations.length; i++) {
        Text.writeString(out, locations[i]);
      }        
    }
  }
    
  private static final int CURRENT_SPLIT_FILE_VERSION = 0;
  private static final byte[] SPLIT_FILE_HEADER = "SPL".getBytes();
    
  /** Create the list of input splits and write them out in a file for
   *the JobTracker. The format is:
   * <format version>
   * <numSplits>
   * for each split:
   *    <RawSplit>
   * @param splits the input splits to write out
   * @param out the stream to write to
   */
  private void writeSplitsFile(InputSplit[] splits, FSDataOutputStream out) throws IOException {
    out.write(SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(out, CURRENT_SPLIT_FILE_VERSION);
    WritableUtils.writeVInt(out, splits.length);
    DataOutputBuffer buffer = new DataOutputBuffer();
    RawSplit rawSplit = new RawSplit();
    for(InputSplit split: splits) {
      rawSplit.setClassName(split.getClass().getName());
      buffer.reset();
      split.write(buffer);
      rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
      rawSplit.setLocations(split.getLocations());
      rawSplit.write(out);
    }
  }

  /**
   * Read a splits file into a list of raw splits
   * @param in the stream to read from
   * @return the complete list of splits
   * @throws IOException
   */
  static RawSplit[] readSplitFile(DataInput in) throws IOException {
    byte[] header = new byte[SPLIT_FILE_HEADER.length];
    in.readFully(header);
    if (!Arrays.equals(SPLIT_FILE_HEADER, header)) {
      throw new IOException("Invalid header on split file");
    }
    int vers = WritableUtils.readVInt(in);
    if (vers != CURRENT_SPLIT_FILE_VERSION) {
      throw new IOException("Unsupported split version " + vers);
    }
    int len = WritableUtils.readVInt(in);
    RawSplit[] result = new RawSplit[len];
    for(int i=0; i < len; ++i) {
      result[i] = new RawSplit();
      result[i].readFields(in);
    }
    return result;
  }
    
  /**
   * Get an RunningJob object to track an ongoing job.  Returns
   * null if the id does not correspond to any known job.
   */
  public RunningJob getJob(String jobid) throws IOException {
    JobStatus status = jobSubmitClient.getJobStatus(jobid);
    if (status != null) {
      return new NetworkedJob(status);
    } else {
      return null;
    }
  }

  /**
   * Get the information of the current state of the map tasks of a job.
   * @param jobId the job to query
   * @return the list of all of the map tips
   */
  public TaskReport[] getMapTaskReports(String jobId) throws IOException {
    return jobSubmitClient.getMapTaskReports(jobId);
  }
    
  /**
   * Get the information of the current state of the reduce tasks of a job.
   * @param jobId the job to query
   * @return the list of all of the map tips
   */    
  public TaskReport[] getReduceTaskReports(String jobId) throws IOException {
    return jobSubmitClient.getReduceTaskReports(jobId);
  }
    
  public ClusterStatus getClusterStatus() throws IOException {
    return jobSubmitClient.getClusterStatus();
  }
    
  public JobStatus[] jobsToComplete() throws IOException {
    return jobSubmitClient.jobsToComplete();
  }
    
  /** Utility that submits a job, then polls for progress until the job is
   * complete. */
  public static RunningJob runJob(JobConf job) throws IOException {
    JobClient jc = new JobClient(job);
    boolean error = true;
    RunningJob running = null;
    String lastReport = null;
    final int MAX_RETRIES = 5;
    int retries = MAX_RETRIES;
    TaskStatusFilter filter;
    try {
      filter = getTaskOutputFilter(job);
    } catch(IllegalArgumentException e) {
      LOG.warn("Invalid Output filter : " + e.getMessage() + 
               " Valid values are : NONE, FAILED, SUCCEEDED, ALL");
      throw e;
    }
    try {
      running = jc.submitJob(job);
      String jobId = running.getJobID();
      LOG.info("Running job: " + jobId);
      int eventCounter = 0; 
        
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {}
        try {
          if (running.isComplete()) {
            break;
          }
          running = jc.getJob(jobId);
          String report = 
            (" map " + StringUtils.formatPercent(running.mapProgress(), 0)+
             " reduce " + 
             StringUtils.formatPercent(running.reduceProgress(), 0));
          if (!report.equals(lastReport)) {
            LOG.info(report);
            lastReport = report;
          }
            
          if (filter  != TaskStatusFilter.NONE){
            TaskCompletionEvent[] events = 
              running.getTaskCompletionEvents(eventCounter); 
            eventCounter += events.length;
            for(TaskCompletionEvent event : events){
              switch(filter){
              case SUCCEEDED:
                if (event.getTaskStatus() == 
                    TaskCompletionEvent.Status.SUCCEEDED){
                  LOG.info(event.toString());
                  displayTaskLogs(event.getTaskId(), event.getTaskTrackerHttp());
                }
                break; 
              case FAILED:
                if (event.getTaskStatus() == 
                    TaskCompletionEvent.Status.FAILED){
                  LOG.info(event.toString());
                  displayTaskLogs(event.getTaskId(), event.getTaskTrackerHttp());
                }
                break; 
              case ALL:
                LOG.info(event.toString());
                displayTaskLogs(event.getTaskId(), event.getTaskTrackerHttp());
                break;
              }
            }
          }
          retries = MAX_RETRIES;
        } catch (IOException ie) {
          if (--retries == 0) {
            LOG.warn("Final attempt failed, killing job.");
            throw ie;
          }
          LOG.info("Communication problem with server: " +
                   StringUtils.stringifyException(ie));
        }
      }
      if (!running.isSuccessful()) {
        throw new IOException("Job failed!");
      }
      LOG.info("Job complete: " + jobId);
      running.getCounters().log(LOG);
      error = false;
    } finally {
      if (error && (running != null)) {
        running.killJob();
      }
      jc.close();
    }
    return running;
  }

  private static void displayTaskLogs(String taskId, String baseUrl)
    throws IOException {
    // The tasktracker for a 'failed/killed' job might not be around...
    if (baseUrl != null) {
      // Copy tasks's stdout of the JobClient
      getTaskLogs(taskId, new URL(baseUrl+"&filter=stdout"), System.out);
        
      // Copy task's stderr to stderr of the JobClient 
      getTaskLogs(taskId, new URL(baseUrl+"&filter=stderr"), System.err);
    }
  }
    
  private static void getTaskLogs(String taskId, URL taskLogUrl, 
                                  OutputStream out) {
    try {
      URLConnection connection = taskLogUrl.openConnection();
      BufferedReader input = 
        new BufferedReader(new InputStreamReader(connection.getInputStream()));
      BufferedWriter output = 
        new BufferedWriter(new OutputStreamWriter(out));
      try {
        String logData = null;
        while ((logData = input.readLine()) != null) {
          if (logData.length() > 0) {
            output.write(taskId + ": " + logData + "\n");
            output.flush();
          }
        }
      } finally {
        input.close();
      }
    }catch(IOException ioe){
      LOG.warn("Error reading task output" + ioe.getMessage()); 
    }
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
   * Get the task output filter out of the JobConf
   * @param job the JobConf to examine
   * @return the filter level
   */
  public static TaskStatusFilter getTaskOutputFilter(JobConf job) {
    return TaskStatusFilter.valueOf(job.get("jobclient.output.filter", 
                                            "FAILED"));
  }
    
  /**
   * Modify the JobConf to set the task output filter
   * @param job the JobConf to modify
   * @param newValue the value to set
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

  /**
   * Display usage of the command-line tool and terminate execution
   */
  private void displayUsage() {
    System.out.printf("JobClient <command> <args>\n");
    System.out.printf("\t-submit\t<job-file>\n");
    System.out.printf("\t-status\t<job-id>\n");
    System.out.printf("\t-kill\t<job-id>\n");
    System.out.printf("\t-events\t<job-id> <from-event-#> <#-of-events>\n");
    System.out.printf("\t-list\n");
    System.out.printf("\t-kill-task <task-id>\n");
    System.out.printf("\t-fail-task <task-id>\n\n");
    ToolRunner.printGenericCommandUsage(System.out);
    throw new RuntimeException("JobClient: bad command-line arguments");
  }
    
  public int run(String[] argv) throws Exception {
    // process arguments
    String submitJobFile = null;
    String jobid = null;
    String taskid = null;
    int fromEvent = 0;
    int nEvents = 0;
    boolean getStatus = false;
    boolean killJob = false;
    boolean listEvents = false;
    boolean listJobs = false;
    boolean killTask = false;
    boolean failTask = false;
    
    if (argv.length < 1)
      displayUsage();

    if ("-submit".equals(argv[0])) {
      if (argv.length != 2)
        displayUsage();
      submitJobFile = argv[1];
    } else if ("-status".equals(argv[0])) {
      if (argv.length != 2)
        displayUsage();
      jobid = argv[1];
      getStatus = true;
    } else if ("-kill".equals(argv[0])) {
      if (argv.length != 2)
        displayUsage();
      jobid = argv[1];
      killJob = true;
    } else if ("-events".equals(argv[0])) {
      if (argv.length != 4)
        displayUsage();
      jobid = argv[1];
      fromEvent = Integer.parseInt(argv[2]);
      nEvents = Integer.parseInt(argv[3]);
      listEvents = true;
    } else if ("-list".equals(argv[0])) {
      listJobs = true;
    } else if("-kill-task".equals(argv[0])) {
      if(argv.length != 2)
        displayUsage();
      killTask = true;
      taskid = argv[1];
    } else if("-fail-task".equals(argv[0])) {
      if(argv.length != 2)
        displayUsage();
      failTask = true;
      taskid = argv[1];
    } else {
      displayUsage();
    }

    // initialize JobClient
    JobConf conf = null;
    if (submitJobFile != null) {
      conf = new JobConf(submitJobFile);
    } else {
      conf = new JobConf();
    }
    init(conf);
        
    // Submit the request
    int exitCode = -1;
    try {
      if (submitJobFile != null) {
        RunningJob job = submitJob(conf);
        System.out.println("Created job " + job.getJobID());
        exitCode = 0;
      } else if (getStatus) {
        RunningJob job = getJob(jobid);
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          System.out.println();
          System.out.println(job);
          exitCode = 0;
        }
      } else if (killJob) {
        RunningJob job = getJob(jobid);
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          job.killJob();
          System.out.println("Killed job " + jobid);
          exitCode = 0;
        }
      } else if (listEvents) {
        listEvents(jobid, fromEvent, nEvents);
        exitCode = 0;
      } else if (listJobs) {
        listJobs();
        exitCode = 0;
      } else if(killTask) {
        if(jobSubmitClient.killTask(taskid, false)) {
          System.out.println("Killed task " + taskid);
          exitCode = 0;
        } else {
          System.out.println("Could not kill task " + taskid);
          exitCode = -1;
        }
      } else if(failTask) {
        if(jobSubmitClient.killTask(taskid, true)) {
          System.out.println("Killed task " + taskid + " by failing it");
          exitCode = 0;
        } else {
          System.out.println("Could not fail task " + taskid);
          exitCode = -1;
        }
      }
    } finally {
      close();
    }
    return exitCode;
  }

  /**
   * List the events for the given job
   * @param jobId the job id for the job's events to list
   * @throws IOException
   */
  private void listEvents(String jobId, int fromEventId, int numEvents)
    throws IOException {
    TaskCompletionEvent[] events = 
      jobSubmitClient.getTaskCompletionEvents(jobId, fromEventId, numEvents);
    System.out.println("Task completion events for " + jobId);
    System.out.println("Number of events (from " + fromEventId + 
                       ") are: " + events.length);
    for(TaskCompletionEvent event: events) {
      System.out.println(event.getTaskStatus() + " " + event.getTaskId() + 
                         " " + event.getTaskTrackerHttp());
    }
  }

  /**
   * Dump a list of currently running jobs
   * @throws IOException
   */
  private void listJobs() throws IOException {
    JobStatus[] jobs = jobsToComplete();
    if (jobs == null)
      jobs = new JobStatus[0];

    System.out.printf("%d jobs currently running\n", jobs.length);
    System.out.printf("JobId\tState\tStartTime\tUserName\n");
    for (JobStatus job : jobs) {
      System.out.printf("%s\t%d\t%d\t%s\n", job.getJobId(), job.getRunState(),
          job.getStartTime(), job.getUsername());
    }
  }
    
  /**
   */
  public static void main(String argv[]) throws Exception {
    int res = ToolRunner.run(new JobClient(), argv);
    System.exit(res);
  }
}

