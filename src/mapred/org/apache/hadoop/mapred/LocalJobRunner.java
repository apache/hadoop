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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobTrackerMetricsInst;
import org.apache.hadoop.mapred.JvmTask;
import org.apache.hadoop.mapred.JobClient.RawSplit;
import org.apache.hadoop.util.ReflectionUtils;

/** Implements MapReduce locally, in-process, for debugging. */ 
class LocalJobRunner implements JobSubmissionProtocol {
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
    return JobSubmissionProtocol.versionID;
  }
  
  private class Job extends Thread
    implements TaskUmbilicalProtocol {
    private Path file;
    private JobID id;
    private JobConf job;

    private JobStatus status;
    private ArrayList<TaskAttemptID> mapIds = new ArrayList<TaskAttemptID>();
    private MapOutputFile mapoutputFile;
    private JobProfile profile;
    private Path localFile;
    private FileSystem localFs;
    boolean killed = false;
    
    // Counters summed over all the map/reduce tasks which
    // have successfully completed
    private Counters completedTaskCounters = new Counters();
    
    // Current counters, including incomplete task(s)
    private Counters currentCounters = new Counters();

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    public Job(JobID jobid, JobConf conf) throws IOException {
      this.file = new Path(getSystemDir(), jobid + "/job.xml");
      this.id = jobid;
      this.mapoutputFile = new MapOutputFile(jobid);
      this.mapoutputFile.setConf(conf);

      this.localFile = new JobConf(conf).getLocalPath(jobDir+id+".xml");
      this.localFs = FileSystem.getLocal(conf);

      fs.copyToLocalFile(file, localFile);
      this.job = new JobConf(localFile);
      profile = new JobProfile(job.getUser(), id, file.toString(), 
                               "http://localhost:8080/", job.getJobName());
      status = new JobStatus(id, 0.0f, 0.0f, JobStatus.RUNNING);

      jobs.put(id, this);

      this.start();
    }

    JobProfile getProfile() {
      return profile;
    }
    
    @Override
    public void run() {
      JobID jobId = profile.getJobID();
      JobContext jContext = new JobContext(conf, jobId);
      OutputCommitter outputCommitter = job.getOutputCommitter();
      try {
        // split input into minimum number of splits
        RawSplit[] rawSplits;
        if (job.getUseNewMapper()) {
          org.apache.hadoop.mapreduce.InputFormat<?,?> input =
              ReflectionUtils.newInstance(jContext.getInputFormatClass(), jContext.getJobConf());
                    
          List<org.apache.hadoop.mapreduce.InputSplit> splits = input.getSplits(jContext);
          rawSplits = new RawSplit[splits.size()];
          DataOutputBuffer buffer = new DataOutputBuffer();
          SerializationFactory factory = new SerializationFactory(conf);
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
        
        for (int i = 0; i < rawSplits.length; i++) {
          if (!this.isInterrupted()) {
            TaskAttemptID mapId = new TaskAttemptID(new TaskID(jobId, true, i),0);  
            mapIds.add(mapId);
            MapTask map = new MapTask(file.toString(),  
                                      mapId, i,
                                      rawSplits[i].getClassName(),
                                      rawSplits[i].getBytes());
            JobConf localConf = new JobConf(job);
            map.setJobFile(localFile.toString());
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
          new TaskAttemptID(new TaskID(jobId, false, 0), 0);
        try {
          if (numReduceTasks > 0) {
            // move map output to reduce input  
            for (int i = 0; i < mapIds.size(); i++) {
              if (!this.isInterrupted()) {
                TaskAttemptID mapId = mapIds.get(i);
                Path mapOut = this.mapoutputFile.getOutputFile(mapId);
                Path reduceIn = this.mapoutputFile.getInputFileForWrite(
                                  mapId.getTaskID(),reduceId,
                                  localFs.getLength(mapOut));
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
              ReduceTask reduce = new ReduceTask(file.toString(), 
                                                 reduceId, 0, mapIds.size());
              JobConf localConf = new JobConf(job);
              reduce.setJobFile(localFile.toString());
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
          for (TaskAttemptID mapId: mapIds) {
            this.mapoutputFile.removeAll(mapId);
          }
          if (numReduceTasks == 1) {
            this.mapoutputFile.removeAll(reduceId);
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
          fs.delete(file.getParent(), true);  // delete submit dir
          localFs.delete(localFile, true);              // delete local copy
        } catch (IOException e) {
          LOG.warn("Error cleaning up "+id+": "+e);
        }
      }
    }

    // TaskUmbilicalProtocol methods

    public JvmTask getTask(JVMId jvmId) { return null; }

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
      return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EMPTY_ARRAY,
                                               false);
    }
    
  }

  public LocalJobRunner(JobConf conf) throws IOException {
    this.fs = FileSystem.get(conf);
    this.conf = conf;
    myMetrics = new JobTrackerMetricsInst(null, new JobConf(conf));
  }

  // JobSubmissionProtocol methods

  private static int jobid = 0;
  public synchronized JobID getNewJobId() {
    return new JobID("local", ++jobid);
  }

  public JobStatus submitJob(JobID jobid) throws IOException {
    return new Job(jobid, this.conf).status;
  }

  public void killJob(JobID id) {
    jobs.get(id).killed = true;
    jobs.get(id).interrupt();
  }

  public void setJobPriority(JobID id, String jp) throws IOException {
    throw new UnsupportedOperationException("Changing job priority " +
                      "in LocalJobRunner is not supported.");
  }
  
  /** Throws {@link UnsupportedOperationException} */
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {
    throw new UnsupportedOperationException("Killing tasks in " +
    "LocalJobRunner is not supported");
  }

  public JobProfile getJobProfile(JobID id) {
    Job job = jobs.get(id);
    if(job != null)
      return job.getProfile();
    else 
      return null;
  }

  public TaskReport[] getMapTaskReports(JobID id) {
    return new TaskReport[0];
  }
  public TaskReport[] getReduceTaskReports(JobID id) {
    return new TaskReport[0];
  }
  public TaskReport[] getCleanupTaskReports(JobID id) {
    return new TaskReport[0];
  }
  public TaskReport[] getSetupTaskReports(JobID id) {
    return new TaskReport[0];
  }

  public JobStatus getJobStatus(JobID id) {
    Job job = jobs.get(id);
    if(job != null)
      return job.status;
    else 
      return null;
  }
  
  public Counters getJobCounters(JobID id) {
    Job job = jobs.get(id);
    return job.currentCounters;
  }

  public String getFilesystemName() throws IOException {
    return fs.getUri().toString();
  }
  
  public ClusterStatus getClusterStatus(boolean detailed) {
    return new ClusterStatus(1, 0, 0, map_tasks, reduce_tasks, 1, 1, 
                             JobTracker.State.RUNNING);
  }

  public JobStatus[] jobsToComplete() {return null;}

  public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid
      , int fromEventId, int maxEvents) throws IOException {
    return TaskCompletionEvent.EMPTY_ARRAY;
  }
  
  public JobStatus[] getAllJobs() {return null;}

  
  /**
   * Returns the diagnostic information for a particular task in the given job.
   * To be implemented
   */
  public String[] getTaskDiagnostics(TaskAttemptID taskid)
  		throws IOException{
	  return new String [0];
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }

  @Override
  public JobStatus[] getJobsFromQueue(String queue) throws IOException {
    return null;
  }

  @Override
  public JobQueueInfo[] getQueues() throws IOException {
    return null;
  }


  @Override
  public JobQueueInfo getQueueInfo(String queue) throws IOException {
    return null;
  }

}
