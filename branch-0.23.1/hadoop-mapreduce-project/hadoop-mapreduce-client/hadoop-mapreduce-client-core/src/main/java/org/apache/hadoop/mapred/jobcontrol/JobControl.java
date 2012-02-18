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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobControl extends 
    org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl {

  /** 
   * Construct a job control for a group of jobs.
   * @param groupName a name identifying this group
   */
  public JobControl(String groupName) {
    super(groupName);
  }
  
  static ArrayList<Job> castToJobList(List<ControlledJob> cjobs) {
    ArrayList<Job> ret = new ArrayList<Job>();
    for (ControlledJob job : cjobs) {
      ret.add((Job)job);
    }
    return ret;
  }
  
  /**
   * @return the jobs in the waiting state
   */
  public ArrayList<Job> getWaitingJobs() {
    return castToJobList(super.getWaitingJobList());
  }
	
  /**
   * @return the jobs in the running state
   */
  public ArrayList<Job> getRunningJobs() {
    return castToJobList(super.getRunningJobList());
  }
	
  /**
   * @return the jobs in the ready state
   */
  public ArrayList<Job> getReadyJobs() {
    return castToJobList(super.getReadyJobsList());
  }
	
  /**
   * @return the jobs in the success state
   */
  public ArrayList<Job> getSuccessfulJobs() {
    return castToJobList(super.getSuccessfulJobList());
  }
	
  public ArrayList<Job> getFailedJobs() {
    return castToJobList(super.getFailedJobList());
  }

  /**
   * Add a collection of jobs
   * 
   * @param jobs
   */
  public void addJobs(Collection <Job> jobs) {
    for (Job job : jobs) {
      addJob(job);
    }
  }

  /**
   * @return the thread state
   */
  public int getState() {
    ThreadState state = super.getThreadState();
    if (state == ThreadState.RUNNING) {
      return 0;
    } 
    if (state == ThreadState.SUSPENDED) {
      return 1;
    }
    if (state == ThreadState.STOPPED) {
      return 2;
    }
    if (state == ThreadState.STOPPING) {
      return 3;
    }
    if (state == ThreadState.READY ) {
      return 4;
    }
    return -1;
  }

}
