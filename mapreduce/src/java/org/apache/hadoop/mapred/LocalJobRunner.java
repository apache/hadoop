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
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job.RawSplit;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.server.jobtracker.State;
import org.apache.hadoop.util.ReflectionUtils;

/** Implements MapReduce locally, in-process, for debugging. */ 
public class LocalJobRunner implements ClientProtocol {
  public static final Log LOG =
    LogFactory.getLog(LocalJobRunner.class);

  private FileSystem fs;
  private HashMap<JobID, Job> jobs = new HashMap<JobID, Job>();
  private JobConf conf;
  private int map_tasks = 0;
  private int reduce_tasks = 0;

  private JobTrackerInstrumentation myMetrics = null;

  private static final String jobDir =  "localRunner/";
  
  public long getProtocolVersion(String protocol, long clientVersion) {
    return ClientProtocol.versionID;
  }
  
  @SuppressWarnings("unchecked")
  static RawSplit[] getRawSplits(JobContext jContext, JobConf job)
      throws Exception {
    JobConf jobConf = jContext.getJobConf();
    org.apache.hadoop.mapreduce.InputFormat<?,?> input =
      ReflectionUtils.newInstance(jContext.getInputFormatClass(), jobConf);

    List<org.apache.hadoop.mapreduce.InputSplit> splits = input.getSplits(jContext);
    RawSplit[] rawSplits = new RawSplit[splits.size()];
    DataOutputBuffer buffer = new DataOutputBuffer();
    SerializationFactory factory = new SerializationFactory(jobConf);
    Serializer serializer = 
      factory.getSerializer(splits.get(0).getClass());
    serializer.open(buffer);
    for (int i = 0; i < splits.size(); i++) {
      buffer.reset();
      serializer.serialize(splits.get(i));
      RawSplit rawSplit = new RawSplit();
      rawSplit.setClassName(splits.get(i).getClass().getName());
      rawSplit.setDataLength(splits.get(i).getLength());
      rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
      rawSplit.setLocations(splits.get(i).getLocations());
      rawSplits[i] = rawSplit;
    }
    return rawSplits;
  }

  private class Job extends Thread implements TaskUmbilicalProtocol {
    // The job directory on the system: JobClient places job configurations here.
    // This is analogous to JobTracker's system directory.
    private Path systemJobDir;
    private Path systemJobFile;
    
    // The job directory for the task.  Analagous to a task's job directory.
    private Path localJobDir;
    private Path localJobFile;

    private JobID id;
    private JobConf job;

    private JobStatus status;
    private ArrayList<TaskAttemptID> mapIds = new ArrayList<TaskAttemptID>();

    private JobProfile profile;
    private FileSystem localFs;
    boolean killed = false;
    
    private TrackerDistributedCacheManager trackerDistributerdCacheManager;
    private TaskDistributedCacheManager taskDistributedCacheManager;
    
    // Counters summed over all the map/reduce tasks which
    // have successfully completed
    private Counters completedTaskCounters = new Counters();
    
    // Current counters, including incomplete task(s)
    private Counters currentCounters = new Counters();

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    public Job(JobID jobid) throws IOException {
      this.systemJobDir = new Path(getSystemDir(), jobid.toString());
      this.systemJobFile = new Path(systemJobDir, "job.xml");
      this.id = jobid;
      JobConf conf = new JobConf(systemJobFile);
      this.localFs = FileSystem.getLocal(conf);
      this.localJobDir = localFs.makeQualified(conf.getLocalPath(jobDir));
      this.localJobFile = new Path(this.localJobDir, id + ".xml");

      // Manage the distributed cache.  If there are files to be copied,
      // this will trigger localFile to be re-written again.
      this.trackerDistributerdCacheManager =
          new TrackerDistributedCacheManager(conf);
      this.taskDistributedCacheManager = 
          trackerDistributerdCacheManager.newTaskDistributedCacheManager(conf);
      taskDistributedCacheManager.setup(
          new LocalDirAllocator(MRConfig.LOCAL_DIR), 
          new File(systemJobDir.toString()),
          "archive");
      
      if (DistributedCache.getSymlink(conf)) {
        // This is not supported largely because, 
        // for a Child subprocess, the cwd in LocalJobRunner
        // is not a fresh slate, but rather the user's working directory.
        // This is further complicated because the logic in
        // setupWorkDir only creates symlinks if there's a jarfile
        // in the configuration.
        LOG.warn("LocalJobRunner does not support " +
        		"symlinking into current working dir.");
      }
      // Setup the symlinks for the distributed cache.
      TaskRunner.setupWorkDir(conf, new File(localJobDir.toUri()).getAbsoluteFile());
      
      // Write out configuration file.  Instead of copying it from
      // systemJobFile, we re-write it, since setup(), above, may have
      // updated it.
      OutputStream out = localFs.create(localJobFile);
      try {
        conf.writeXml(out);
      } finally {
        out.close();
      }
      this.job = new JobConf(localJobFile);
      
      // Job (the current object) is a Thread, so we wrap its class loader.
      if (!taskDistributedCacheManager.getClassPaths().isEmpty()) {
        setContextClassLoader(taskDistributedCacheManager.makeClassLoader(
                getContextClassLoader()));
      }
      
      profile = new JobProfile(job.getUser(), id, systemJobFile.toString(), 
                               "http://localhost:8080/", job.getJobName());
      status = new JobStatus(id, 0.0f, 0.0f, JobStatus.RUNNING, 
          profile.getUser(), profile.getJobName(), profile.getJobFile(), 
          profile.getURL().toString());

      jobs.put(id, this);

      this.start();
    }

    JobProfile getProfile() {
      return profile;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      JobID jobId = profile.getJobID();
      JobContext jContext = new JobContextImpl(job, jobId);
      OutputCommitter outputCommitter = job.getOutputCommitter();
      try {
        // split input into minimum number of splits
        RawSplit[] rawSplits;
        if (job.getUseNewMapper()) {
          rawSplits = getRawSplits(jContext, job);
        } else {
          InputSplit[] splits = job.getInputFormat().getSplits(job, 1);
          rawSplits = new RawSplit[splits.length];
          DataOutputBuffer buffer = new DataOutputBuffer();
          for (int i = 0; i < splits.length; i++) {
            buffer.reset();
            splits[i].write(buffer);
            RawSplit rawSplit = new RawSplit();
            rawSplit.setClassName(splits[i].getClass().getName());
            rawSplit.setDataLength(splits[i].getLength());
            rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
            rawSplit.setLocations(splits[i].getLocations());
            rawSplits[i] = rawSplit;
          }
        }
        
        int numReduceTasks = job.getNumReduceTasks();
        if (numReduceTasks > 1 || numReduceTasks < 0) {
          // we only allow 0 or 1 reducer in local mode
          numReduceTasks = 1;
          job.setNumReduceTasks(1);
        }
        outputCommitter.setupJob(jContext);
        status.setSetupProgress(1.0f);

        Map<TaskAttemptID, MapOutputFile> mapOutputFiles =
            new HashMap<TaskAttemptID, MapOutputFile>();
        for (int i = 0; i < rawSplits.length; i++) {
          if (!this.isInterrupted()) {
            TaskAttemptID mapId = new TaskAttemptID(
                new TaskID(jobId, TaskType.MAP, i),0);  
            mapIds.add(mapId);
            MapTask map = new MapTask(systemJobFile.toString(),  
                                      mapId, i,
                                      rawSplits[i].getClassName(),
                                      rawSplits[i].getBytes(), 1);
            JobConf localConf = new JobConf(job);
            TaskRunner.setupChildMapredLocalDirs(map, localConf);

            MapOutputFile mapOutput = new MapOutputFile();
            mapOutput.setConf(localConf);
            mapOutputFiles.put(mapId, mapOutput);

            map.setJobFile(localJobFile.toString());
            map.localizeConfiguration(localConf);
            map.setConf(localConf);
            map_tasks += 1;
            myMetrics.launchMap(mapId);
            map.run(localConf, this);
            myMetrics.completeMap(mapId);
            map_tasks -= 1;
            updateCounters(map);
          } else {
            throw new InterruptedException();
          }
        }
        TaskAttemptID reduceId = 
          new TaskAttemptID(new TaskID(jobId, TaskType.REDUCE, 0), 0);
        try {
          if (numReduceTasks > 0) {
            ReduceTask reduce = new ReduceTask(systemJobFile.toString(), 
                reduceId, 0, mapIds.size(), 1);
            JobConf localConf = new JobConf(job);
            TaskRunner.setupChildMapredLocalDirs(reduce, localConf);
            // move map output to reduce input  
            for (int i = 0; i < mapIds.size(); i++) {
              if (!this.isInterrupted()) {
                TaskAttemptID mapId = mapIds.get(i);
                Path mapOut = mapOutputFiles.get(mapId).getOutputFile();
                MapOutputFile localOutputFile = new MapOutputFile();
                localOutputFile.setConf(localConf);
                Path reduceIn =
                  localOutputFile.getInputFileForWrite(mapId.getTaskID(),
                        localFs.getFileStatus(mapOut).getLen());
                if (!localFs.mkdirs(reduceIn.getParent())) {
                  throw new IOException("Mkdirs failed to create "
                      + reduceIn.getParent().toString());
                }
                if (!localFs.rename(mapOut, reduceIn))
                  throw new IOException("Couldn't rename " + mapOut);
              } else {
                throw new InterruptedException();
              }
            }
            if (!this.isInterrupted()) {
              reduce.setJobFile(localJobFile.toString());
              reduce.localizeConfiguration(localConf);
              reduce.setConf(localConf);
              reduce_tasks += 1;
              myMetrics.launchReduce(reduce.getTaskID());
              reduce.run(localConf, this);
              myMetrics.completeReduce(reduce.getTaskID());
              reduce_tasks -= 1;
              updateCounters(reduce);
            } else {
              throw new InterruptedException();
            }
          }
        } finally {
          for (MapOutputFile output : mapOutputFiles.values()) {
            output.removeAll();
          }
        }
        // delete the temporary directory in output directory
        outputCommitter.cleanupJob(jContext);
        status.setCleanupProgress(1.0f);

        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.SUCCEEDED);
        }

        JobEndNotifier.localRunnerNotification(job, status);

      } catch (Throwable t) {
        try {
          outputCommitter.cleanupJob(jContext);
        } catch (IOException ioe) {
          LOG.info("Error cleaning up job:" + id);
        }
        status.setCleanupProgress(1.0f);
        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.FAILED);
        }
        LOG.warn(id, t);

        JobEndNotifier.localRunnerNotification(job, status);

      } finally {
        try {
          fs.delete(systemJobFile.getParent(), true);  // delete submit dir
          localFs.delete(localJobFile, true);              // delete local copy
          // Cleanup distributed cache
          taskDistributedCacheManager.release();
          trackerDistributerdCacheManager.purgeCache();
        } catch (IOException e) {
          LOG.warn("Error cleaning up "+id+": "+e);
        }
      }
    }

    // TaskUmbilicalProtocol methods

    public JvmTask getTask(JvmContext context) { return null; }
    
    public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      LOG.info(taskStatus.getStateString());
      float taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        float numTasks = mapIds.size();
        status.setMapProgress(taskIndex/numTasks + taskStatus.getProgress()/numTasks);
      } else {
        status.setReduceProgress(taskStatus.getProgress());
      }
      currentCounters = Counters.sum(completedTaskCounters, taskStatus.getCounters());
      
      // ignore phase
      
      return true;
    }

    /**
     * Task is reporting that it is in commit_pending
     * and it is waiting for the commit Response
     */
    public void commitPending(TaskAttemptID taskid,
                              TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      statusUpdate(taskid, taskStatus);
    }

    /**
     * Updates counters corresponding to completed tasks.
     * @param task A map or reduce task which has just been 
     * successfully completed
     */ 
    private void updateCounters(Task task) {
      completedTaskCounters.incrAllCounters(task.getCounters());
    }

    public void reportDiagnosticInfo(TaskAttemptID taskid, String trace) {
      // Ignore for now
    }
    
    public void reportNextRecordRange(TaskAttemptID taskid, 
        SortedRanges.Range range) throws IOException {
      LOG.info("Task " + taskid + " reportedNextRecordRange " + range);
    }

    public boolean ping(TaskAttemptID taskid) throws IOException {
      return true;
    }
    
    public boolean canCommit(TaskAttemptID taskid) 
    throws IOException {
      return true;
    }
    
    public void done(TaskAttemptID taskId) throws IOException {
      int taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        status.setMapProgress(1.0f);
      } else {
        status.setReduceProgress(1.0f);
      }
    }

    public synchronized void fsError(TaskAttemptID taskId, String message) 
    throws IOException {
      LOG.fatal("FSError: "+ message + "from task: " + taskId);
    }

    public void shuffleError(TaskAttemptID taskId, String message) throws IOException {
      LOG.fatal("shuffleError: "+ message + "from task: " + taskId);
    }
    
    public synchronized void fatalError(TaskAttemptID taskId, String msg) 
    throws IOException {
      LOG.fatal("Fatal: "+ msg + "from task: " + taskId);
    }
    
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
        int fromEventId, int maxLocs, TaskAttemptID id) throws IOException {
      return new MapTaskCompletionEventsUpdate(
        org.apache.hadoop.mapred.TaskCompletionEvent.EMPTY_ARRAY, false);
    }
    
  }

  public LocalJobRunner(Configuration conf) throws IOException {
    this(new JobConf(conf));
  }

  @Deprecated
  public LocalJobRunner(JobConf conf) throws IOException {
    this.fs = FileSystem.getLocal(conf);
    this.conf = conf;
    myMetrics = new JobTrackerMetricsInst(null, new JobConf(conf));
  }

  // JobSubmissionProtocol methods

  private static int jobid = 0;
  public synchronized org.apache.hadoop.mapreduce.JobID getNewJobID() {
    return new org.apache.hadoop.mapreduce.JobID("local", ++jobid);
  }

  public org.apache.hadoop.mapreduce.JobStatus submitJob(
      org.apache.hadoop.mapreduce.JobID jobid) 
      throws IOException {
    return new Job(JobID.downgrade(jobid)).status;
  }

  public void killJob(org.apache.hadoop.mapreduce.JobID id) {
    jobs.get(id).killed = true;
    jobs.get(id).interrupt();
  }

  public void setJobPriority(org.apache.hadoop.mapreduce.JobID id,
      String jp) throws IOException {
    throw new UnsupportedOperationException("Changing job priority " +
                      "in LocalJobRunner is not supported.");
  }
  
  /** Throws {@link UnsupportedOperationException} */
  public boolean killTask(org.apache.hadoop.mapreduce.TaskAttemptID taskId,
      boolean shouldFail) throws IOException {
    throw new UnsupportedOperationException("Killing tasks in " +
    "LocalJobRunner is not supported");
  }

  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(
      org.apache.hadoop.mapreduce.JobID id, TaskType type) {
    return new org.apache.hadoop.mapreduce.TaskReport[0];
  }

  public org.apache.hadoop.mapreduce.JobStatus getJobStatus(
      org.apache.hadoop.mapreduce.JobID id) {
    Job job = jobs.get(JobID.downgrade(id));
    if(job != null)
      return job.status;
    else 
      return null;
  }
  
  public org.apache.hadoop.mapreduce.Counters getJobCounters(
      org.apache.hadoop.mapreduce.JobID id) {
    Job job = jobs.get(JobID.downgrade(id));
    return new org.apache.hadoop.mapreduce.Counters(job.currentCounters);
  }

  public String getFilesystemName() throws IOException {
    return fs.getUri().toString();
  }
  
  public ClusterMetrics getClusterMetrics() {
    return new ClusterMetrics(map_tasks, reduce_tasks, 1, 1, 1, 0, 0);
  }

  public State getJobTrackerState() throws IOException, InterruptedException {
    return State.RUNNING;
  }

  public long getTaskTrackerExpiryInterval() throws IOException, InterruptedException {
    return 0;
  }

  /** 
   * Get all active trackers in cluster. 
   * @return array of TaskTrackerInfo
   */
  public TaskTrackerInfo[] getActiveTrackers() 
      throws IOException, InterruptedException {
    return null;
  }

  /** 
   * Get all blacklisted trackers in cluster. 
   * @return array of TaskTrackerInfo
   */
  public TaskTrackerInfo[] getBlacklistedTrackers() 
      throws IOException, InterruptedException {
    return null;
  }

  public TaskCompletionEvent[] getTaskCompletionEvents(
      org.apache.hadoop.mapreduce.JobID jobid
      , int fromEventId, int maxEvents) throws IOException {
    return TaskCompletionEvent.EMPTY_ARRAY;
  }
  
  public org.apache.hadoop.mapreduce.JobStatus[] getAllJobs() {return null;}

  
  /**
   * Returns the diagnostic information for a particular task in the given job.
   * To be implemented
   */
  public String[] getTaskDiagnostics(
      org.apache.hadoop.mapreduce.TaskAttemptID taskid) throws IOException{
	  return new String [0];
  }

  /**
   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(
      conf.get(JTConfig.JT_SYSTEM_DIR, "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }

  public String getJobHistoryDir() {
    return null;
  }

  @Override
  public QueueInfo[] getChildQueues(String queueName) throws IOException {
    return null;
  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException {
    return null;
  }

  @Override
  public QueueInfo[] getQueues() throws IOException {
    return null;
  }


  @Override
  public QueueInfo getQueue(String queue) throws IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.mapreduce.QueueAclsInfo[] 
      getQueueAclsForCurrentUser() throws IOException{
    return null;
  }
}
