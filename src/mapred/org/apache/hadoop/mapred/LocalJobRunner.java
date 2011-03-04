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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobTrackerMetricsInst;
import org.apache.hadoop.mapred.JvmTask;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.security.UserGroupInformation;

/** Implements MapReduce locally, in-process, for debugging. */ 
class LocalJobRunner implements JobSubmissionProtocol {
  public static final Log LOG =
    LogFactory.getLog(LocalJobRunner.class);

  private FileSystem fs;
  private HashMap<JobID, Job> jobs = new HashMap<JobID, Job>();
  private JobConf conf;
  private int map_tasks = 0;
  private int reduce_tasks = 0;
  final Random rand = new Random();

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
    private Path systemJobDir;

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
    
    public Job(JobID jobid, String jobSubmitDir) throws IOException {
      this.systemJobDir = new Path(jobSubmitDir);
      this.file = new Path(systemJobDir, "job.xml");
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
        TaskSplitMetaInfo[] taskSplitMetaInfos =
          SplitMetaInfoReader.readSplitMetaInfo(jobId, localFs, conf, systemJobDir);        
        int numReduceTasks = job.getNumReduceTasks();
        if (numReduceTasks > 1 || numReduceTasks < 0) {
          // we only allow 0 or 1 reducer in local mode
          numReduceTasks = 1;
          job.setNumReduceTasks(1);
        }
        outputCommitter.setupJob(jContext);
        status.setSetupProgress(1.0f);
        
        for (int i = 0; i < taskSplitMetaInfos.length; i++) {
          if (!this.isInterrupted()) {
            TaskAttemptID mapId = new TaskAttemptID(new TaskID(jobId, true, i),0);  
            mapIds.add(mapId);
            MapTask map = new MapTask(file.toString(),  
                                      mapId, i,
                                      taskSplitMetaInfos[i].getSplitIndex(), 1);
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
                                                 reduceId, 0, mapIds.size(), 
                                                 1);
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
        outputCommitter.commitJob(jContext);
        status.setCleanupProgress(1.0f);

        if (killed) {
          this.status.setRunState(JobStatus.KILLED);
        } else {
          this.status.setRunState(JobStatus.SUCCEEDED);
        }

        JobEndNotifier.localRunnerNotification(job, status);

      } catch (Throwable t) {
        try {
          outputCommitter.abortJob(jContext, JobStatus.FAILED);
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
      return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EMPTY_ARRAY,
                                               false);
    }
    
  }

  public LocalJobRunner(JobConf conf) throws IOException {
    this.fs = FileSystem.getLocal(conf);
    this.conf = conf;
    myMetrics = new JobTrackerMetricsInst(null, new JobConf(conf));
  }

  // JobSubmissionProtocol methods

  private static int jobid = 0;
  public synchronized JobID getNewJobId() {
    return new JobID("local", ++jobid);
  }

  public JobStatus submitJob(JobID jobid, String jobSubmitDir) 
  throws IOException {
    return new Job(jobid, jobSubmitDir).status;
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
  
  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getStagingAreaDir()
   */
  public String getStagingAreaDir() throws IOException {
    Path stagingRootDir = 
      new Path(conf.get("mapreduce.jobtracker.staging.root.dir",
        "/tmp/hadoop/mapred/staging"));
    UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
    String user;
    if (ugi != null) {
      user = ugi.getUserName() + rand.nextInt();
    } else {
      user = "dummy" + rand.nextInt();
    }
    return fs.makeQualified(new Path(stagingRootDir, user+"/.staging")).toString();
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

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException{
    return null;
}
}
