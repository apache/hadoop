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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

/**
 * Test whether the JobInProgressListeners are informed as expected.
 */
public class TestJobInProgressListener extends TestCase {
  private static final Log LOG = 
    LogFactory.getLog(TestJobInProgressListener.class);
  
  // A listener that inits the tasks one at a time and also listens to the 
  // events
  public static class MyListener extends JobInProgressListener {
    private List<JobInProgress> wjobs = new ArrayList<JobInProgress>();
    private List<JobInProgress> jobs = new ArrayList<JobInProgress>(); 
    
    public boolean contains (JobID id) {
      return contains(id, true) || contains(id, false);
    }
    
    public boolean contains (JobID id, boolean waiting) {
      List<JobInProgress> queue = waiting ? wjobs : jobs;
      for (JobInProgress job : queue) {
        if (job.getJobID().equals(id)) {
          return true;
        }
      }
      return false;
    }
    
    public void jobAdded(JobInProgress job) {
      LOG.info("Job " + job.getJobID().toString() + " added");
      wjobs.add(job);
    }
    
    public void jobRemoved(JobInProgress job) {
      LOG.info("Job " + job.getJobID().toString() + " removed");
    }
    
    public void jobUpdated(JobChangeEvent event) {
      LOG.info("Job " + event.getJobInProgress().getJobID().toString() + " updated");
      // remove the job is the event is for a completed job
      if (event instanceof JobStatusChangeEvent) {
        JobStatusChangeEvent statusEvent = (JobStatusChangeEvent)event;
        if (statusEvent.getEventType() == EventType.RUN_STATE_CHANGED) {
          // check if the state changes from 
          // RUNNING->COMPLETE(SUCCESS/KILLED/FAILED)
          JobInProgress jip = event.getJobInProgress();
          String jobId = jip.getJobID().toString();
          if (statusEvent.getJobInProgress().isComplete()) {
            LOG.info("Job " +  jobId + " deleted from the running queue");
            jobs.remove(jip);
          } else {
            // PREP->RUNNING
            LOG.info("Job " +  jobId + " deleted from the waiting queue");
            wjobs.remove(jip);
            jobs.add(jip);
          }
        }
      }
    }
  }
  
  public void testJobFailure() throws Exception {
    LOG.info("Testing job-success");
    
    MyListener myListener = new MyListener();
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1);
    
    JobConf job = mr.createJobConf();
    
    mr.getJobTrackerRunner().getJobTracker()
      .addJobInProgressListener(myListener);
    
    // submit and kill the job   
    JobID id = TestJobKillAndFail.runJobFail(job);

    // check if the job failure was notified
    assertFalse("Missing event notification on failing a running job", 
                myListener.contains(id));
    
  }
  
  public void testJobKill() throws Exception {
    LOG.info("Testing job-kill");
    
    MyListener myListener = new MyListener();
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1);
    
    JobConf job = mr.createJobConf();
    
    mr.getJobTrackerRunner().getJobTracker()
      .addJobInProgressListener(myListener);
    
    // submit and kill the job   
    JobID id = TestJobKillAndFail.runJobKill(job);

    // check if the job failure was notified
    assertFalse("Missing event notification on killing a running job", 
                myListener.contains(id));
    
  }
  
  public void testJobSuccess() throws Exception {
    LOG.info("Testing job-success");
    MyListener myListener = new MyListener();
    
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1);
    
    JobConf job = mr.createJobConf();
    
    mr.getJobTrackerRunner().getJobTracker()
      .addJobInProgressListener(myListener);
    
    // submit the job   
    RunningJob rJob = TestJobKillAndFail.runJob(job);
    
    // wait for the job to be running
    while (rJob.getJobState() != JobStatus.RUNNING) {
      TestJobTrackerRestart.waitFor(10);
    }
    
    LOG.info("Job " +  rJob.getID().toString() + " started running");
    
    // check if the listener was updated about this change
    assertFalse("Missing event notification for a running job", 
                myListener.contains(rJob.getID(), true));
    
    while (rJob.getJobState() != JobStatus.SUCCEEDED) {
      TestJobTrackerRestart.waitFor(10);
    }
    
    // check if the job success was notified
    assertFalse("Missing event notification for a successful job", 
                myListener.contains(rJob.getID(), false));
  }
}