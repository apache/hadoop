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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.authorize.AccessControlList;

/**************************************************
 * Describes the current status of a job.
 **************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JobStatus implements Writable, Cloneable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (JobStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new JobStatus(); }
       });
  }

  /**
   * Current state of the job 
   */
  public static enum State {
    RUNNING(1),
    SUCCEEDED(2),
    FAILED(3),
    PREP(4),
    KILLED(5);
    
    int value;
    
    State(int value) {
      this.value = value;
    }
    
    public int getValue() {
      return value; 
    }
    
  };
  
  private JobID jobid;
  private float mapProgress;
  private float reduceProgress;
  private float cleanupProgress;
  private float setupProgress;
  private State runState;
  private long startTime;
  private String user;
  private JobPriority priority;
  private String schedulingInfo="NA";

  private Map<JobACL, AccessControlList> jobACLs =
      new HashMap<JobACL, AccessControlList>();

  private String jobName;
  private String jobFile;
  private long finishTime;
  private boolean isRetired;
  private String historyFile = "";
  private String trackingUrl ="";

    
  /**
   */
  public JobStatus() {
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
                    State runState, JobPriority jp, String user, String jobName, 
                    String jobFile, String trackingUrl) {
     this.jobid = jobid;
     this.setupProgress = setupProgress;
     this.mapProgress = mapProgress;
     this.reduceProgress = reduceProgress;
     this.cleanupProgress = cleanupProgress;
     this.runState = runState;
     this.user = user;
     if (jp == null) {
       throw new IllegalArgumentException("Job Priority cannot be null.");
     }
     priority = jp;
     this.jobName = jobName;
     this.jobFile = jobFile;
     this.trackingUrl = trackingUrl;
   }
   
  /**
   * Sets the map progress of this job
   * @param p The value of map progress to set to
   */
  protected synchronized void setMapProgress(float p) { 
    this.mapProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * Sets the cleanup progress of this job
   * @param p The value of cleanup progress to set to
   */
  protected synchronized void setCleanupProgress(float p) { 
    this.cleanupProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * Sets the setup progress of this job
   * @param p The value of setup progress to set to
   */
  protected synchronized void setSetupProgress(float p) { 
    this.setupProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * Sets the reduce progress of this Job
   * @param p The value of reduce progress to set to
   */
  protected synchronized void setReduceProgress(float p) { 
    this.reduceProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }
    
  /**
   * Set the priority of the job, defaulting to NORMAL.
   * @param jp new job priority
   */
  protected synchronized void setPriority(JobPriority jp) {
    if (jp == null) {
      throw new IllegalArgumentException("Job priority cannot be null.");
    }
    priority = jp;
  }
  
  /** 
   * Set the finish time of the job
   * @param finishTime The finishTime of the job
   */
  protected synchronized void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * Set the job history file url for a completed job
   */
  protected synchronized void setHistoryFile(String historyFile) {
    this.historyFile = historyFile;
  }

  /**
   * Set the link to the web-ui for details of the job.
   */
  protected synchronized void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  /**
   * Set the job retire flag to true.
   */
  protected synchronized void setRetired() {
    this.isRetired = true;
  }

  /**
   * Change the current run state of the job.
   */
  protected synchronized void setState(State state) {
    this.runState = state;
  }

  /** 
   * Set the start time of the job
   * @param startTime The startTime of the job
   */
  protected synchronized void setStartTime(long startTime) { 
    this.startTime = startTime;
  }
    
  /**
   * @param userName The username of the job
   */
  protected synchronized void setUsername(String userName) { 
    this.user = userName;
  }

  /**
   * Used to set the scheduling information associated to a particular Job.
   * 
   * @param schedulingInfo Scheduling information of the job
   */
  protected synchronized void setSchedulingInfo(String schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }

  protected synchronized void setJobACLs(Map<JobACL, AccessControlList> acls) {
    this.jobACLs = acls;
  }

  /**
   * @return Percentage of progress in maps 
   */
  public synchronized float getMapProgress() { return mapProgress; }
    
  /**
   * @return Percentage of progress in cleanup 
   */
  public synchronized float getCleanupProgress() { return cleanupProgress; }
    
  /**
   * @return Percentage of progress in setup 
   */
  public synchronized float getSetupProgress() { return setupProgress; }
    
  /**
   * @return Percentage of progress in reduce 
   */
  public synchronized float getReduceProgress() { return reduceProgress; }
    
  /**
   * @return running state of the job
   */
  public synchronized State getState() { return runState; }
    
  /**
   * @return start time of the job
   */
  synchronized public long getStartTime() { return startTime;}

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen since we do implement Clonable
      throw new InternalError(cnse.toString());
    }
  }
  
  /**
   * @return The jobid of the Job
   */
  public JobID getJobID() { return jobid; }
    
  /**
   * @return the username of the job
   */
  public synchronized String getUsername() { return this.user;}
  
  /**
   * Gets the Scheduling information associated to a particular Job.
   * @return the scheduling information of the job
   */
  public synchronized String getSchedulingInfo() {
   return schedulingInfo;
  }

  public synchronized Map<JobACL, AccessControlList> getJobACLs() {
    return jobACLs;
  }

  /**
   * Return the priority of the job
   * @return job priority
   */
   public synchronized JobPriority getPriority() { return priority; }
  
  /**
   * Returns true if the status is for a completed job.
   */
  public synchronized boolean isJobComplete() {
    return (runState == JobStatus.State.SUCCEEDED || 
            runState == JobStatus.State.FAILED || 
            runState == JobStatus.State.KILLED);
  }

  ///////////////////////////////////////
  // Writable
  ///////////////////////////////////////
  public synchronized void write(DataOutput out) throws IOException {
    jobid.write(out);
    out.writeFloat(setupProgress);
    out.writeFloat(mapProgress);
    out.writeFloat(reduceProgress);
    out.writeFloat(cleanupProgress);
    WritableUtils.writeEnum(out, runState);
    out.writeLong(startTime);
    Text.writeString(out, user);
    WritableUtils.writeEnum(out, priority);
    Text.writeString(out, schedulingInfo);
    out.writeLong(finishTime);
    out.writeBoolean(isRetired);
    Text.writeString(out, historyFile);
    Text.writeString(out, jobName);
    Text.writeString(out, trackingUrl);
    Text.writeString(out, jobFile);

    // Serialize the job's ACLs
    out.writeInt(jobACLs.size());
    for (Entry<JobACL, AccessControlList> entry : jobACLs.entrySet()) {
      WritableUtils.writeEnum(out, entry.getKey());
      Text.writeString(out, entry.getValue().toString());
    }
  }

  public synchronized void readFields(DataInput in) throws IOException {
    this.jobid = new JobID();
    this.jobid.readFields(in);
    this.setupProgress = in.readFloat();
    this.mapProgress = in.readFloat();
    this.reduceProgress = in.readFloat();
    this.cleanupProgress = in.readFloat();
    this.runState = WritableUtils.readEnum(in, State.class);
    this.startTime = in.readLong();
    this.user = Text.readString(in);
    this.priority = WritableUtils.readEnum(in, JobPriority.class);
    this.schedulingInfo = Text.readString(in);
    this.finishTime = in.readLong();
    this.isRetired = in.readBoolean();
    this.historyFile = Text.readString(in);
    this.jobName = Text.readString(in);
    this.trackingUrl = Text.readString(in);
    this.jobFile = Text.readString(in);

    // De-serialize the job's ACLs
    int numACLs = in.readInt();
    for (int i = 0; i < numACLs; i++) {
      JobACL aclType = WritableUtils.readEnum(in, JobACL.class);
      String acl = Text.readString(in);
      this.jobACLs.put(aclType, new AccessControlList(acl));
    }
  }

  /**
   * Get the user-specified job name.
   */
  public String getJobName() {
    return jobName;
  }

  /**
   * Get the configuration file for the job.
   */
  public String getJobFile() {
    return jobFile;
  }

  /**
   * Get the link to the web-ui for details of the job.
   */
  public synchronized String getTrackingUrl() {
    return trackingUrl;
  }

  /**
   * Get the finish time of the job.
   */
  public synchronized long getFinishTime() { 
    return finishTime;
  }

  /**
   * Check whether the job has retired.
   */
  public synchronized boolean isRetired() {
    return isRetired;
  }

  /**
   * @return the job history file name for a completed job. If job is not 
   * completed or history file not available then return null.
   */
  public synchronized String getHistoryFile() {
    return historyFile;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("job-id : " + jobid);
    buffer.append("map-progress : " + mapProgress);
    buffer.append("reduce-progress : " + reduceProgress);
    buffer.append("cleanup-progress : " + cleanupProgress);
    buffer.append("setup-progress : " + setupProgress);
    buffer.append("runstate : " + runState);
    buffer.append("start-time : " + startTime);
    buffer.append("user-name : " + user);
    buffer.append("priority : " + priority);
    buffer.append("scheduling-info : " + schedulingInfo);
    return buffer.toString();
  }
}
