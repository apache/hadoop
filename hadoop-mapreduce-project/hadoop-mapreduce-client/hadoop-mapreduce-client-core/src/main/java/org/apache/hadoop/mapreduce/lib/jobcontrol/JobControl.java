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

package org.apache.hadoop.mapreduce.lib.jobcontrol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob.State;
import org.apache.hadoop.util.StringUtils;

/** 
 *  This class encapsulates a set of MapReduce jobs and its dependency.
 *   
 *  It tracks the states of the jobs by placing them into different tables
 *  according to their states. 
 *  
 *  This class provides APIs for the client app to add a job to the group 
 *  and to get the jobs in the group in different states. When a job is 
 *  added, an ID unique to the group is assigned to the job. 
 *  
 *  This class has a thread that submits jobs when they become ready, 
 *  monitors the states of the running jobs, and updates the states of jobs
 *  based on the state changes of their depending jobs states. The class 
 *  provides APIs for suspending/resuming the thread, and 
 *  for stopping the thread.
 *  
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JobControl implements Runnable {
  private static final Log LOG = LogFactory.getLog(JobControl.class);

  // The thread can be in one of the following state
  public static enum ThreadState {RUNNING, SUSPENDED,STOPPED, STOPPING, READY};
	
  private ThreadState runnerState;			// the thread state
	
  private LinkedList<ControlledJob> jobsInProgress = new LinkedList<ControlledJob>();
  private LinkedList<ControlledJob> successfulJobs = new LinkedList<ControlledJob>();
  private LinkedList<ControlledJob> failedJobs = new LinkedList<ControlledJob>();
	
  private long nextJobID;
  private String groupName;
	
  /** 
   * Construct a job control for a group of jobs.
   * @param groupName a name identifying this group
   */
  public JobControl(String groupName) {
    this.nextJobID = -1;
    this.groupName = groupName;
    this.runnerState = ThreadState.READY;
  }
	
  private static List<ControlledJob> toList(
                   LinkedList<ControlledJob> jobs) {
    ArrayList<ControlledJob> retv = new ArrayList<ControlledJob>();
    for (ControlledJob job : jobs) {
      retv.add(job);
    }
    return retv;
  }
	
  synchronized private List<ControlledJob> getJobsIn(State state) {
    LinkedList<ControlledJob> l = new LinkedList<ControlledJob>();
    for(ControlledJob j: jobsInProgress) {
      if(j.getJobState() == state) {
        l.add(j);
      }
    }
    return l;
  }
  
  /**
   * @return the jobs in the waiting state
   */
  public List<ControlledJob> getWaitingJobList() {
    return getJobsIn(State.WAITING);
  }
	
  /**
   * @return the jobs in the running state
   */
  public List<ControlledJob> getRunningJobList() {
    return getJobsIn(State.RUNNING);
  }
	
  /**
   * @return the jobs in the ready state
   */
  public List<ControlledJob> getReadyJobsList() {
    return getJobsIn(State.READY);
  }
	
  /**
   * @return the jobs in the success state
   */
  synchronized public List<ControlledJob> getSuccessfulJobList() {
    return toList(this.successfulJobs);
  }
	
  synchronized public List<ControlledJob> getFailedJobList() {
    return toList(this.failedJobs);
  }
	
  private String getNextJobID() {
    nextJobID += 1;
    return this.groupName + this.nextJobID;
  }

  /**
   * Add a new controlled job.
   * @param aJob the new controlled job
   */
  synchronized public String addJob(ControlledJob aJob) {
    String id = this.getNextJobID();
    aJob.setJobID(id);
    aJob.setJobState(State.WAITING);
    jobsInProgress.add(aJob);
    return id;	
  }

  /**
   * Add a new job.
   * @param aJob the new job
   */
  synchronized public String addJob(Job aJob) {
    return addJob((ControlledJob) aJob);
  }

  /**
   * Add a collection of jobs
   * 
   * @param jobs
   */
  public void addJobCollection(Collection<ControlledJob> jobs) {
    for (ControlledJob job : jobs) {
      addJob(job);
    }
  }
	
  /**
   * @return the thread state
   */
  public ThreadState getThreadState() {
    return this.runnerState;
  }
	
  /**
   * set the thread state to STOPPING so that the 
   * thread will stop when it wakes up.
   */
  public void stop() {
    this.runnerState = ThreadState.STOPPING;
  }
	
  /**
   * suspend the running thread
   */
  public void suspend () {
    if (this.runnerState == ThreadState.RUNNING) {
      this.runnerState = ThreadState.SUSPENDED;
    }
  }
	
  /**
   * resume the suspended thread
   */
  public void resume () {
    if (this.runnerState == ThreadState.SUSPENDED) {
      this.runnerState = ThreadState.RUNNING;
    }
  }
	
  synchronized public boolean allFinished() {
    return jobsInProgress.isEmpty();
  }
	
  /**
   *  The main loop for the thread.
   *  The loop does the following:
   *  	Check the states of the running jobs
   *  	Update the states of waiting jobs
   *  	Submit the jobs in ready state
   */
  public void run() {
    try {
      this.runnerState = ThreadState.RUNNING;
      while (true) {
        while (this.runnerState == ThreadState.SUSPENDED) {
          try {
            Thread.sleep(5000);
          }
          catch (Exception e) {
            //TODO the thread was interrupted, do something!!!
          }
        }
        
        synchronized(this) {
          Iterator<ControlledJob> it = jobsInProgress.iterator();
          while(it.hasNext()) {
            ControlledJob j = it.next();
            LOG.debug("Checking state of job "+j);
            switch(j.checkState()) {
            case SUCCESS:
              successfulJobs.add(j);
              it.remove();
              break;
            case FAILED:
            case DEPENDENT_FAILED:
              failedJobs.add(j);
              it.remove();
              break;
            case READY:
              j.submit();
              break;
            case RUNNING:
            case WAITING:
              //Do Nothing
              break;
            }
          }
        }
        
        if (this.runnerState != ThreadState.RUNNING && 
            this.runnerState != ThreadState.SUSPENDED) {
          break;
        }
        try {
          Thread.sleep(5000);
        }
        catch (Exception e) {
          //TODO the thread was interrupted, do something!!!
        }
        if (this.runnerState != ThreadState.RUNNING && 
            this.runnerState != ThreadState.SUSPENDED) {
          break;
        }
      }
    }catch(Throwable t) {
      LOG.error("Error while trying to run jobs.",t);
      //Mark all jobs as failed because we got something bad.
      failAllJobs(t);
    }
    this.runnerState = ThreadState.STOPPED;
  }

  synchronized private void failAllJobs(Throwable t) {
    String message = "Unexpected System Error Occured: "+
    StringUtils.stringifyException(t);
    Iterator<ControlledJob> it = jobsInProgress.iterator();
    while(it.hasNext()) {
      ControlledJob j = it.next();
      try {
        j.failJob(message);
      } catch (IOException e) {
        LOG.error("Error while tyring to clean up "+j.getJobName(), e);
      } catch (InterruptedException e) {
        LOG.error("Error while tyring to clean up "+j.getJobName(), e);
      } finally {
        failedJobs.add(j);
        it.remove();
      }
    }
  }
}
