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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants;

public class MockSimulatorEngine extends SimulatorEngine {
  HashSet<String> TRACKERS=null;
  HashMap<JobID, JobStory> jobs;
  HashSet<JobID> submittedJobs;
  HashSet<JobID> completedJobs;
  private int fixedJobs;
  private long startTime;

  public static final Log LOG = LogFactory.getLog(MockSimulatorEngine.class);
  
  public MockSimulatorEngine(int nJobs,
      @SuppressWarnings("unused") int nTrackers) {
    super();
    fixedJobs = nJobs;
    jobs = new HashMap<JobID, JobStory>();
    submittedJobs = new HashSet<JobID>();
    completedJobs = new HashSet<JobID>();
  }

  @Override
  public void run() throws IOException, InterruptedException {
    startTime = System.currentTimeMillis();
    init();
    validateInitialization();
    SimulatorEvent nextEvent;
    while ((nextEvent = queue.get()) != null && nextEvent.getTimeStamp() < terminateTime
        && !shutdown) {
      currentTime = nextEvent.getTimeStamp();
      SimulatorEventListener listener = nextEvent.getListener();
      if (nextEvent instanceof JobSubmissionEvent) {
        validateJobSubmission((JobSubmissionEvent)nextEvent);
      } else if (nextEvent instanceof JobCompleteEvent) {
        validateJobComplete((JobCompleteEvent)nextEvent);
      }
      List<SimulatorEvent> response = listener.accept(nextEvent);
      queue.addAll(response);
    }
    validateEnd();
    summary(System.out);
  }
  
  private void validateEnd() {
    Assert.assertEquals("Number of submitted jobs does not match trace",
        submittedJobs.size(), fixedJobs);
    Assert.assertEquals("Number of submitted jobs does not match trace",
        completedJobs.size(), fixedJobs);
  }

  private Pre21JobHistoryConstants.Values convertState (JobStatus status) {
    int runState = status.getRunState();
    if (runState == JobStatus.FAILED) {
      return Pre21JobHistoryConstants.Values.FAILED;
    } else if (runState == JobStatus.SUCCEEDED) {
      return Pre21JobHistoryConstants.Values.SUCCESS;
    } else {
      throw new IllegalArgumentException("unknown status " + status);
    }
  }
  
  private void validateJobComplete(JobCompleteEvent completeEvent) {
    JobID jobId = completeEvent.getJobStatus().getJobID();
    JobStatus finalStatus = completeEvent.getJobStatus();

    Assert.assertTrue("Job completed was not submitted:"+jobId, 
               submittedJobs.contains(jobId));
    Assert.assertFalse("Job completed more than once:" + jobId, 
                completedJobs.contains(jobId));
    completedJobs.add(jobId);
   
    Pre21JobHistoryConstants.Values finalValue = jobs.get(jobId).getOutcome();
    Pre21JobHistoryConstants.Values obtainedStatus = convertState(finalStatus);
    Assert.assertEquals("Job completion final status mismatch", obtainedStatus,
        finalValue);
  }

  private void validateJobSubmission(JobSubmissionEvent submissionEvent) {
    JobID jobId = submissionEvent.getJob().getJobID();
    LOG.info("Job being submitted: " + jobId);
    Assert.assertFalse("Job " + jobId + " is already submitted", submittedJobs
        .contains(jobId));
    LOG.info("Adding to submitted Jobs " + jobId);
    submittedJobs.add(jobId); 
    jobs.put(jobId, submissionEvent.getJob());
    Pre21JobHistoryConstants.Values finalValue = submissionEvent.getJob().getOutcome();
    Assert.assertTrue("Job has final state neither SUCCESS nor FAILED",
        finalValue == Pre21JobHistoryConstants.Values.FAILED
            || finalValue == Pre21JobHistoryConstants.Values.SUCCESS);
  }

  private void validateInitialization() {
    // The JobTracker has been initialized.
    Assert.assertTrue("SimulatorJobTracker is null", jt != null);
    Assert.assertTrue("Clock of simulator is behind startTime",
        SimulatorJobTracker.getClock().getTime() >= startTime);
    // The JobClient has been initialized
    Assert.assertTrue("SimulatorJobClient is null", jc != null);
  }
}
