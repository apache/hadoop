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

package org.apache.hadoop.mapred.jobcontrol;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Job extends ControlledJob {
  static final Log LOG = LogFactory.getLog(Job.class);

  final public static int SUCCESS = 0;
  final public static int WAITING = 1;
  final public static int RUNNING = 2;
  final public static int READY = 3;
  final public static int FAILED = 4;
  final public static int DEPENDENT_FAILED = 5;

  /** 
   * Construct a job.
   * @param jobConf a mapred job configuration representing a job to be executed.
   * @param dependingJobs an array of jobs the current job depends on
   */
  @SuppressWarnings("unchecked")
  public Job(JobConf jobConf, ArrayList<?> dependingJobs) throws IOException {
    super(org.apache.hadoop.mapreduce.Job.getInstance(jobConf),
          (List<ControlledJob>) dependingJobs);
  }

  public Job(JobConf conf) throws IOException {
    super(conf);
  }

  /**
   * @return the mapred ID of this job as assigned by the mapred framework.
   */
  public JobID getAssignedJobID() {
    org.apache.hadoop.mapreduce.JobID temp = super.getMapredJobId();
    if (temp == null) {
      return null;
    }
    return JobID.downgrade(temp);
  }

  /**
   * @deprecated setAssignedJobID should not be called.
   * JOBID is set by the framework.
   */
  @Deprecated
  public void setAssignedJobID(JobID mapredJobID) {
    // do nothing
  }

  /**
   * @return the mapred job conf of this job
   */
  public synchronized JobConf getJobConf() {
    return new JobConf(super.getJob().getConfiguration());
  }


  /**
   * Set the mapred job conf for this job.
   * @param jobConf the mapred job conf for this job.
   */
  public synchronized void setJobConf(JobConf jobConf) {
    try {
      super.setJob(org.apache.hadoop.mapreduce.Job.getInstance(jobConf));
    } catch (IOException ioe) { 
      LOG.info("Exception" + ioe);
    }
  }

  /**
   * @return the state of this job
   */
  public synchronized int getState() {
    State state = super.getJobState();
    if (state == State.SUCCESS) {
      return SUCCESS;
    } 
    if (state == State.WAITING) {
      return WAITING;
    }
    if (state == State.RUNNING) {
      return RUNNING;
    }
    if (state == State.READY) {
      return READY;
    }
    if (state == State.FAILED ) {
      return FAILED;
    }
    if (state == State.DEPENDENT_FAILED ) {
      return DEPENDENT_FAILED;
    }
    return -1;
  }
  
  /**
   * This is a no-op function, Its a behavior change from 1.x We no more can
   * change the state from job
   * 
   * @param state
   *          the new state for this job.
   */
  @Deprecated
  protected synchronized void setState(int state) {
    // No-Op, we dont want to change the sate
  }
  
  /**
   * Add a job to this jobs' dependency list. 
   * Dependent jobs can only be added while a Job 
   * is waiting to run, not during or afterwards.
   * 
   * @param dependingJob Job that this Job depends on.
   * @return <tt>true</tt> if the Job was added.
   */
  public synchronized boolean addDependingJob(Job dependingJob) {
    return super.addDependingJob(dependingJob);
  }
  
  /**
   * @return the job client of this job
   */
  public JobClient getJobClient() {
    try {
      return new JobClient(super.getJob().getConfiguration());
    } catch (IOException ioe) {
      return null;
    }
  }

  /**
   * @return the depending jobs of this job
   */
  public ArrayList<Job> getDependingJobs() {
    return JobControl.castToJobList(super.getDependentJobs());
  }

  /**
   * @return the mapred ID of this job as assigned by the mapred framework.
   */
  public synchronized String getMapredJobID() {
    if (super.getMapredJobId() != null) {
      return super.getMapredJobId().toString();
    }
    return null;
  }

  /**
   * This is no-op method for backward compatibility. It's a behavior change
   * from 1.x, we can not change job ids from job.
   * 
   * @param mapredJobID
   *          the mapred job ID for this job.
   */
  @Deprecated
  public synchronized void setMapredJobID(String mapredJobID) {
    setAssignedJobID(JobID.forName(mapredJobID));
  }
}
