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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;

/**
 * Class that simulates a job client. It's main functionality is to submit jobs
 * to the simulation engine, and shutdown the simulation engine if the job
 * producer runs out of jobs.
 * TODO: Change System.out.printXX to LOG.xxx.
 */
public class SimulatorJobClient implements SimulatorEventListener {
  private final ClientProtocol jobTracker;
  private final JobStoryProducer jobStoryProducer;
  private Set<JobID> runningJobs = new LinkedHashSet<JobID>();
  private boolean noMoreJobs = false;

  /**
   * Constructor.
   * 
   * @param jobTracker
   *          The job tracker where we submit job to. Note that the {@link
   *          SimulatorJobClient} interacts with the JobTracker through the
   *          {@link ClientProtocol}.
   * @param jobStoryProducer
   */
  public SimulatorJobClient(ClientProtocol jobTracker, JobStoryProducer jobStoryProducer) {
    this.jobTracker = jobTracker;
    this.jobStoryProducer = jobStoryProducer;
  }
  
  @Override
  public List<SimulatorEvent> init(long when) throws IOException {
    JobStory job = jobStoryProducer.getNextJob();
    if (job.getSubmissionTime() != when) {
      throw new IOException("Inconsistent submission time for the first job: "
          + when + " != " + job.getSubmissionTime()+".");
    }
    JobSubmissionEvent event = new JobSubmissionEvent(this, when, job);
    return Collections.<SimulatorEvent> singletonList(event);
  }
  
  @Override
  public List<SimulatorEvent> accept(SimulatorEvent event)
      throws IOException {
    if (event instanceof JobSubmissionEvent) {
      JobSubmissionEvent submitEvent = (JobSubmissionEvent)(event);
  
      // Submit job
      JobStatus status = null;
      try {
        status = submitJob(submitEvent.getJob());
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      runningJobs.add(status.getJobID());
      System.out.println("Job " + status.getJobID() + 
                         " is submitted at " + submitEvent.getTimeStamp());
      
      JobStory nextJob = jobStoryProducer.getNextJob();
      if (nextJob == null) {
        noMoreJobs = true;
        return SimulatorEngine.EMPTY_EVENTS;
      }
      
      return Collections.<SimulatorEvent>singletonList(
          new JobSubmissionEvent(this, nextJob.getSubmissionTime(), nextJob));
    } else if (event instanceof JobCompleteEvent) {
      JobCompleteEvent jobCompleteEvent = (JobCompleteEvent)event;
      JobStatus jobStatus = jobCompleteEvent.getJobStatus();
      System.out.println("Job " + jobStatus.getJobID() + 
                         " completed at " + jobCompleteEvent.getTimeStamp() + 
                         " with status: " + jobStatus.getState() +
                         " runtime: " + 
                         (jobCompleteEvent.getTimeStamp() - jobStatus.getStartTime()));
      runningJobs.remove(jobCompleteEvent.getJobStatus().getJobID());
      if (noMoreJobs && runningJobs.isEmpty()) {
        jobCompleteEvent.getEngine().shutdown();
      }
      return SimulatorEngine.EMPTY_EVENTS;
    } else {
      throw new IllegalArgumentException("unknown event type: " + event.getClass());
    }
  }

  @SuppressWarnings("deprecation")
  private JobStatus submitJob(JobStory job)
      throws IOException, InterruptedException {
    // honor the JobID from JobStory first.
    JobID jobId = job.getJobID();
    if (jobId == null) {
      // If not available, obtain JobID from JobTracker.
      jobId = jobTracker.getNewJobID();
    }
    
    SimulatorJobCache.put(org.apache.hadoop.mapred.JobID.downgrade(jobId), job);
    return jobTracker.submitJob(jobId);
  }
}
