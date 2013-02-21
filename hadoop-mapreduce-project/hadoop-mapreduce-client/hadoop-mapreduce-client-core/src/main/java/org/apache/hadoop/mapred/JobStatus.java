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

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.authorize.AccessControlList;

/**************************************************
 * Describes the current status of a job.  This is
 * not intended to be a comprehensive piece of data.
 * For that, look at JobProfile.
 *************************************************
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobStatus extends org.apache.hadoop.mapreduce.JobStatus {

  public static final int RUNNING = 
    org.apache.hadoop.mapreduce.JobStatus.State.RUNNING.getValue();
  public static final int SUCCEEDED = 
    org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED.getValue();
  public static final int FAILED = 
    org.apache.hadoop.mapreduce.JobStatus.State.FAILED.getValue();
  public static final int PREP = 
    org.apache.hadoop.mapreduce.JobStatus.State.PREP.getValue();
  public static final int KILLED = 
    org.apache.hadoop.mapreduce.JobStatus.State.KILLED.getValue();

  private static final String UNKNOWN = "UNKNOWN";
  
  private static final String[] runStates =
    {UNKNOWN, "RUNNING", "SUCCEEDED", "FAILED", "PREP", "KILLED"};

  /**
   * Helper method to get human-readable state of the job.
   * @param state job state
   * @return human-readable state of the job
   */
  public static String getJobRunState(int state) {
    if (state < 1 || state >= runStates.length) {
      return UNKNOWN;
    }
    return runStates[state];
  }
  
  static org.apache.hadoop.mapreduce.JobStatus.State getEnum(int state) {
    switch (state) {
      case 1: return org.apache.hadoop.mapreduce.JobStatus.State.RUNNING;
      case 2: return org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED;
      case 3: return org.apache.hadoop.mapreduce.JobStatus.State.FAILED;
      case 4: return org.apache.hadoop.mapreduce.JobStatus.State.PREP;
      case 5: return org.apache.hadoop.mapreduce.JobStatus.State.KILLED;
    }
    return null;
  }
  
  /**
   */
  public JobStatus() {
  }
  
  @Deprecated
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
      float cleanupProgress, int runState) {
    this(jobid, mapProgress, reduceProgress, cleanupProgress, runState, null,
        null, null, null);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   */
  @Deprecated
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
      int runState) {
    this (jobid, mapProgress, reduceProgress, runState, null, null, null, null);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   * @param jp Priority of the job.
   */
  @Deprecated
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
      float cleanupProgress, int runState, JobPriority jp) {
    this(jobid, mapProgress, reduceProgress, cleanupProgress, runState, jp,
        null, null, null, null);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param setupProgress The progress made on the setup
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on the cleanup
   * @param runState The current state of the job
   * @param jp Priority of the job.
   */
  @Deprecated
  public JobStatus(JobID jobid, float setupProgress, float mapProgress,
      float reduceProgress, float cleanupProgress, 
      int runState, JobPriority jp) {
    this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
        runState, jp, null, null, null, null);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on cleanup
   * @param runState The current state of the job
   * @param user userid of the person who submitted the job.
   * @param jobName user-specified job name.
   * @param jobFile job configuration file. 
   * @param trackingUrl link to the web-ui for details of the job.
   */
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                   float cleanupProgress, int runState, 
                   String user, String jobName, 
                   String jobFile, String trackingUrl) {
    this(jobid, mapProgress, reduceProgress, cleanupProgress, runState,
        JobPriority.NORMAL, user, jobName, jobFile, trackingUrl);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   * @param user userid of the person who submitted the job.
   * @param jobName user-specified job name.
   * @param jobFile job configuration file. 
   * @param trackingUrl link to the web-ui for details of the job.
   */
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                   int runState, String user, String jobName, 
                   String jobFile, String trackingUrl) {
    this(jobid, mapProgress, reduceProgress, 0.0f, runState, user, jobName, 
        jobFile, trackingUrl);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   * @param jp Priority of the job.
   * @param user userid of the person who submitted the job.
   * @param jobName user-specified job name.
   * @param jobFile job configuration file. 
   * @param trackingUrl link to the web-ui for details of the job.
   */
   public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                      float cleanupProgress, int runState, JobPriority jp, 
                      String user, String jobName, String jobFile, 
                      String trackingUrl) {
     this(jobid, 0.0f, mapProgress, reduceProgress, 
          cleanupProgress, runState, jp, user, jobName, jobFile,
          trackingUrl);
   }
   
  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param setupProgress The progress made on the setup
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on the cleanup
   * @param runState The current state of the job
   * @param jp Priority of the job.
   * @param user userid of the person who submitted the job.
   * @param jobName user-specified job name.
   * @param jobFile job configuration file. 
   * @param trackingUrl link to the web-ui for details of the job.
   */
   public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                    float reduceProgress, float cleanupProgress, 
                    int runState, JobPriority jp, String user, String jobName, 
                    String jobFile, String trackingUrl) {
     this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
         runState, jp, user, jobName, "default", jobFile, trackingUrl);
   }

   /**
    * Create a job status object for a given jobid.
    * @param jobid The jobid of the job
    * @param setupProgress The progress made on the setup
    * @param mapProgress The progress made on the maps
    * @param reduceProgress The progress made on the reduces
    * @param cleanupProgress The progress made on the cleanup
    * @param runState The current state of the job
    * @param jp Priority of the job.
    * @param user userid of the person who submitted the job.
    * @param jobName user-specified job name.
    * @param jobFile job configuration file. 
    * @param trackingUrl link to the web-ui for details of the job.
    * @param isUber Whether job running in uber mode
    */
    public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                     float reduceProgress, float cleanupProgress, 
                     int runState, JobPriority jp, String user, String jobName, 
                     String jobFile, String trackingUrl, boolean isUber) {
      this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
          runState, jp, user, jobName, "default", jobFile, trackingUrl, isUber);
    }   
   
   /**
    * Create a job status object for a given jobid.
    * @param jobid The jobid of the job
    * @param setupProgress The progress made on the setup
    * @param mapProgress The progress made on the maps
    * @param reduceProgress The progress made on the reduces
    * @param cleanupProgress The progress made on the cleanup
    * @param runState The current state of the job
    * @param jp Priority of the job.
    * @param user userid of the person who submitted the job.
    * @param jobName user-specified job name.
    * @param queue job queue name.
    * @param jobFile job configuration file.
    * @param trackingUrl link to the web-ui for details of the job.
    */
   public JobStatus(JobID jobid, float setupProgress, float mapProgress,
       float reduceProgress, float cleanupProgress,
       int runState, JobPriority jp,
       String user, String jobName, String queue,
       String jobFile, String trackingUrl) {
     this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
         runState, jp,
         user, jobName, queue, jobFile, trackingUrl, false);
   }

   /**
    * Create a job status object for a given jobid.
    * @param jobid The jobid of the job
    * @param setupProgress The progress made on the setup
    * @param mapProgress The progress made on the maps
    * @param reduceProgress The progress made on the reduces
    * @param cleanupProgress The progress made on the cleanup
    * @param runState The current state of the job
    * @param jp Priority of the job.
    * @param user userid of the person who submitted the job.
    * @param jobName user-specified job name.
    * @param queue job queue name.
    * @param jobFile job configuration file. 
    * @param trackingUrl link to the web-ui for details of the job.
    * @param isUber Whether job running in uber mode
    */
   public JobStatus(JobID jobid, float setupProgress, float mapProgress,
       float reduceProgress, float cleanupProgress, 
       int runState, JobPriority jp, 
       String user, String jobName, String queue, 
       String jobFile, String trackingUrl, boolean isUber) {
     super(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
         getEnum(runState), org.apache.hadoop.mapreduce.JobPriority.valueOf(jp.name()),
         user, jobName, queue, jobFile, trackingUrl, isUber);
   }

  public static JobStatus downgrade(org.apache.hadoop.mapreduce.JobStatus stat){
    JobStatus old = new JobStatus(JobID.downgrade(stat.getJobID()),
      stat.getSetupProgress(), stat.getMapProgress(), stat.getReduceProgress(),
      stat.getCleanupProgress(), stat.getState().getValue(), 
      JobPriority.valueOf(stat.getPriority().name()),
      stat.getUsername(), stat.getJobName(), stat.getQueue(), stat.getJobFile(),
      stat.getTrackingUrl(), stat.isUber());
    old.setStartTime(stat.getStartTime());
    old.setFinishTime(stat.getFinishTime());
    old.setSchedulingInfo(stat.getSchedulingInfo());
    old.setHistoryFile(stat.getHistoryFile());
    return old;
  }
  /**
   * @deprecated use getJobID instead
   */
  @Deprecated
  public String getJobId() { return getJobID().toString(); }
  
  /**
   * @return The jobid of the Job
   */
  public JobID getJobID() { return JobID.downgrade(super.getJobID()); }
  
  /**
   * Return the priority of the job
   * @return job priority
   */
   public synchronized JobPriority getJobPriority() { 
     return JobPriority.valueOf(super.getPriority().name());
   }

   /**
    * Sets the map progress of this job
    * @param p The value of map progress to set to
    */
   protected synchronized void setMapProgress(float p) { 
     super.setMapProgress(p); 
   }

   /**
    * Sets the cleanup progress of this job
    * @param p The value of cleanup progress to set to
    */
   protected synchronized void setCleanupProgress(float p) { 
     super.setCleanupProgress(p); 
   }

   /**
    * Sets the setup progress of this job
    * @param p The value of setup progress to set to
    */
   protected synchronized void setSetupProgress(float p) { 
     super.setSetupProgress(p); 
   }

   /**
    * Sets the reduce progress of this Job
    * @param p The value of reduce progress to set to
    */
   protected synchronized void setReduceProgress(float p) { 
     super.setReduceProgress(p); 
   }
     
   /** 
    * Set the finish time of the job
    * @param finishTime The finishTime of the job
    */
   protected synchronized void setFinishTime(long finishTime) {
     super.setFinishTime(finishTime);
   }

   /**
    * Set the job history file url for a completed job
    */
   protected synchronized void setHistoryFile(String historyFile) {
     super.setHistoryFile(historyFile);
   }

   /**
    * Set the link to the web-ui for details of the job.
    */
   protected synchronized void setTrackingUrl(String trackingUrl) {
     super.setTrackingUrl(trackingUrl);
   }

   /**
    * Set the job retire flag to true.
    */
   protected synchronized void setRetired() {
     super.setRetired();
   }

   /**
    * Change the current run state of the job.
    */
   protected synchronized void setRunState(int state) {
     super.setState(getEnum(state));
   }

   /**
    * @return running state of the job
    */
   public synchronized int getRunState() { return super.getState().getValue(); }
     

   /** 
    * Set the start time of the job
    * @param startTime The startTime of the job
    */
   protected synchronized void setStartTime(long startTime) { 
     super.setStartTime(startTime);
   }
     
   /**
    * @param userName The username of the job
    */
   protected synchronized void setUsername(String userName) { 
     super.setUsername(userName);
   }

   /**
    * Used to set the scheduling information associated to a particular Job.
    * 
    * @param schedulingInfo Scheduling information of the job
    */
   protected synchronized void setSchedulingInfo(String schedulingInfo) {
     super.setSchedulingInfo(schedulingInfo);
   }

   protected synchronized void setJobACLs(Map<JobACL, AccessControlList> acls) {
     super.setJobACLs(acls);
   }

   public synchronized void setFailureInfo(String failureInfo) {
     super.setFailureInfo(failureInfo);
   }
   
  /**
   * Set the priority of the job, defaulting to NORMAL.
   * @param jp new job priority
   */
   public synchronized void setJobPriority(JobPriority jp) {
     super.setPriority(
       org.apache.hadoop.mapreduce.JobPriority.valueOf(jp.name()));
   }
  
   /**
    * @return Percentage of progress in maps 
    */
   public synchronized float mapProgress() { return super.getMapProgress(); }
     
   /**
    * @return Percentage of progress in cleanup 
    */
   public synchronized float cleanupProgress() { 
     return super.getCleanupProgress(); 
   }
     
   /**
    * @return Percentage of progress in setup 
    */
   public synchronized float setupProgress() { 
     return super.getSetupProgress(); 
   }
     
   /**
    * @return Percentage of progress in reduce 
    */
   public synchronized float reduceProgress() { 
     return super.getReduceProgress(); 
   }

   // A utility to convert new job runstates to the old ones.
   static int getOldNewJobRunState(
     org.apache.hadoop.mapreduce.JobStatus.State state) {
     return state.getValue();
   }
}