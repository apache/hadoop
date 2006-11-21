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

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.JobTracker.JobTrackerMetrics;

/** Implements MapReduce locally, in-process, for debugging. */ 
class LocalJobRunner implements JobSubmissionProtocol {
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.LocalJobRunner");

  private FileSystem fs;
  private HashMap jobs = new HashMap();
  private Configuration conf;
  private int map_tasks = 0;
  private int reduce_tasks = 0;

  private JobTrackerMetrics myMetrics = null;

  public long getProtocolVersion(String protocol, long clientVersion) {
    return JobSubmissionProtocol.versionID;
  }
  
  private class Job extends Thread
    implements TaskUmbilicalProtocol {
    private String file;
    private String id;
    private JobConf job;
    private Random random = new Random();

    private JobStatus status;
    private ArrayList mapIds = new ArrayList();
    private MapOutputFile mapoutputFile;
    private JobProfile profile;
    private Path localFile;
    private FileSystem localFs;

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    public Job(String file, Configuration conf) throws IOException {
      this.file = file;
      this.id = "job_" + newId();
      this.mapoutputFile = new MapOutputFile();
      this.mapoutputFile.setConf(conf);

      this.localFile = new JobConf(conf).getLocalPath("localRunner/"+id+".xml");
      this.localFs = FileSystem.getNamed("local", conf);

      fs.copyToLocalFile(new Path(file), localFile);
      this.job = new JobConf(localFile);
      profile = new JobProfile(job.getUser(), id, file, 
                               "http://localhost:8080/", job.getJobName());
      status = new JobStatus(id, 0.0f, 0.0f, JobStatus.RUNNING);

      jobs.put(id, this);

      this.start();
    }

    JobProfile getProfile() {
      return profile;
    }
    
    public void run() {
      try {
        // split input into minimum number of splits
        FileSplit[] splits;
        splits = job.getInputFormat().getSplits(fs, job, 1);
        String jobId = profile.getJobId();
        
        // run a map task for each split
        job.setNumReduceTasks(1);                 // force a single reduce task
        for (int i = 0; i < splits.length; i++) {
          String mapId = "map_" + newId() ; 
          mapIds.add(mapId);
          MapTask map = new MapTask(jobId, file, "tip_m_" + mapId, 
                                    mapId, i,
                                    splits[i]);
          JobConf localConf = new JobConf(job);
          map.localizeConfiguration(localConf);
          map.setConf(localConf);
          map_tasks += 1;
          myMetrics.launchMap();
          map.run(localConf, this);
          myMetrics.completeMap();
          map_tasks -= 1;
        }

        // move map output to reduce input
        String reduceId = "reduce_" + newId();
        for (int i = 0; i < mapIds.size(); i++) {
          String mapId = (String)mapIds.get(i);
          Path mapOut = this.mapoutputFile.getOutputFile(mapId, 0);
          Path reduceIn = this.mapoutputFile.getInputFile(i, reduceId);
          if (!localFs.mkdirs(reduceIn.getParent())) {
            throw new IOException("Mkdirs failed to create " + 
                                  reduceIn.getParent().toString());
          }
          if (!localFs.rename(mapOut, reduceIn))
            throw new IOException("Couldn't rename " + mapOut);
          this.mapoutputFile.removeAll(mapId);
        }

        {
          ReduceTask reduce = new ReduceTask(jobId, file, 
                                             "tip_r_0001", reduceId, 0, mapIds.size());
          JobConf localConf = new JobConf(job);
          reduce.localizeConfiguration(localConf);
          reduce.setConf(localConf);
          reduce_tasks += 1;
          myMetrics.launchReduce();
          reduce.run(localConf, this);
          myMetrics.completeReduce();
          reduce_tasks -= 1;
        }
        this.mapoutputFile.removeAll(reduceId);
        
        this.status.setRunState(JobStatus.SUCCEEDED);

      } catch (Throwable t) {
        this.status.setRunState(JobStatus.FAILED);
        LOG.warn(id, t);

      } finally {
        try {
          fs.delete(new Path(file).getParent());  // delete submit dir
          localFs.delete(localFile);              // delete local copy
        } catch (IOException e) {
          LOG.warn("Error cleaning up "+id+": "+e);
        }
      }
    }

    private String newId() {
      return Integer.toString(Math.abs(random.nextInt()),36);
    }

    // TaskUmbilicalProtocol methods

    public Task getTask(String taskid) { return null; }

    public void progress(String taskId, float progress, String state, 
                         TaskStatus.Phase phase) {
      LOG.info(state);
      float taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        float numTasks = mapIds.size();
        status.setMapProgress(taskIndex/numTasks + progress/numTasks);
      } else {
        status.setReduceProgress(progress);
      }
      
      // ignore phase
    }

    public void reportDiagnosticInfo(String taskid, String trace) {
      // Ignore for now
    }

    public boolean ping(String taskid) throws IOException {
      return true;
    }

    public void done(String taskId) throws IOException {
      int taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        status.setMapProgress(1.0f);
      } else {
        status.setReduceProgress(1.0f);
      }
    }

    public synchronized void fsError(String message) throws IOException {
      LOG.fatal("FSError: "+ message);
    }

  }

  public LocalJobRunner(Configuration conf) throws IOException {
    this.fs = FileSystem.get(conf);
    this.conf = conf;
    myMetrics = new JobTrackerMetrics();
  }

  // JobSubmissionProtocol methods

  public JobStatus submitJob(String jobFile) throws IOException {
    return new Job(jobFile, this.conf).status;
  }

  public void killJob(String id) {
    ((Thread)jobs.get(id)).stop();
  }

  public JobProfile getJobProfile(String id) {
    Job job = (Job)jobs.get(id);
    return job.getProfile();
  }

  public TaskReport[] getMapTaskReports(String id) {
    return new TaskReport[0];
  }
  public TaskReport[] getReduceTaskReports(String id) {
    return new TaskReport[0];
  }

  public JobStatus getJobStatus(String id) {
    Job job = (Job)jobs.get(id);
    return job.status;
  }

  public String getFilesystemName() throws IOException {
    return fs.getName();
  }
  
  public ClusterStatus getClusterStatus() {
    return new ClusterStatus(1, map_tasks, reduce_tasks, 1);
  }

  public JobStatus[] jobsToComplete() {return null;}
}
