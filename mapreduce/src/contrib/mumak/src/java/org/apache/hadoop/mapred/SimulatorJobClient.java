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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;

/**
 * Class that simulates a job client. It's main functionality is to submit jobs
 * to the simulation engine, and shutdown the simulation engine if the job
 * producer runs out of jobs.
 */
public class SimulatorJobClient implements SimulatorEventListener {
  protected static class JobSketchInfo {
    protected int numMaps;
    protected int numReduces;
    JobSketchInfo(int numMaps, int numReduces) {
      this.numMaps = numMaps;
      this.numReduces = numReduces;
    }
  }
  
  private final ClientProtocol jobTracker;
  private final JobStoryProducer jobStoryProducer;
  private final SimulatorJobSubmissionPolicy submissionPolicy;
  private static final int LOAD_PROB_INTERVAL_START = 1000;
  private static final int LOAD_PROB_INTERVAL_MAX = 320000;
  private int loadProbingInterval = LOAD_PROB_INTERVAL_START;
  
  /**
   * The minimum ratio between pending+running map tasks (aka. incomplete map
   * tasks) and cluster map slot capacity for us to consider the cluster is
   * overloaded. For running maps, we only count them partially. Namely, a 40%
   * completed map is counted as 0.6 map tasks in our calculation.
   */
  private static final float OVERLAOD_MAPTASK_MAPSLOT_RATIO=2.0f;
  /**
   * Keep track of the in-flight load-probing event.
   */
  private LoadProbingEvent inFlightLPE = null;
  /**
   * We do not have handle to the SimulatorEventQueue, and thus cannot cancel
   * events directly. Instead, we keep an identity-map (should have been an
   * identity-set except that JDK does not provide an identity-set) to skip
   * events that are cancelled.
   */
  private Map<LoadProbingEvent, Boolean> cancelledLPE = 
    new IdentityHashMap<LoadProbingEvent, Boolean>();
  
  private Map<JobID, JobSketchInfo> runningJobs = 
    new LinkedHashMap<JobID, JobSketchInfo>();
  private boolean noMoreJobs = false;
  private JobStory nextJob;
  
  /**
   * Constructor.
   * 
   * @param jobTracker
   *          The job tracker where we submit job to. Note that the {@link
   *          SimulatorJobClient} interacts with the JobTracker through the
   *          {@link ClientProtocol}.
   * @param jobStoryProducer
   * @param submissionPolicy How should we submit jobs to the JobTracker?
   */
  public SimulatorJobClient(ClientProtocol jobTracker, 
                            JobStoryProducer jobStoryProducer,
                            SimulatorJobSubmissionPolicy submissionPolicy) {
    this.jobTracker = jobTracker;
    this.jobStoryProducer = jobStoryProducer;
    this.submissionPolicy = submissionPolicy;
  }
  
  /**
   * Constructor.
   * 
   * @param jobTracker
   *          The job tracker where we submit job to. Note that the {@link
   *          SimulatorJobClient} interacts with the JobTracker through the
   *          {@link ClientProtocol}.
   * @param jobStoryProducer
   */
  public SimulatorJobClient(ClientProtocol jobTracker, 
                            JobStoryProducer jobStoryProducer) {
    this(jobTracker, jobStoryProducer,  SimulatorJobSubmissionPolicy.REPLAY);
  }

  @Override
  public List<SimulatorEvent> init(long when) throws IOException {
    JobStory job = jobStoryProducer.getNextJob();
    if (submissionPolicy == SimulatorJobSubmissionPolicy.REPLAY
        && job.getSubmissionTime() != when) {
      throw new IOException("Inconsistent submission time for the first job: "
          + when + " != " + job.getSubmissionTime()+".");
    }
    
    JobSubmissionEvent event = new JobSubmissionEvent(this, when, job);
    if (submissionPolicy != SimulatorJobSubmissionPolicy.STRESS) {
      return Collections.<SimulatorEvent> singletonList(event);
    } else {
      ArrayList<SimulatorEvent> ret = new ArrayList<SimulatorEvent>(2);
      ret.add(event);
      inFlightLPE = new LoadProbingEvent(this, when + loadProbingInterval);
      ret.add(inFlightLPE);
      return ret;
    }
  }

  /**
   * Doing exponential back-off probing because load probing could be pretty
   * expensive if we have many pending jobs.
   * 
   * @param overloaded Is the job tracker currently overloaded?
   */
  private void adjustLoadProbingInterval(boolean overloaded) {
    if (overloaded) {
      /**
       * We should only extend LPE interval when there is no in-flight LPE.
       */
      if (inFlightLPE == null) {
        loadProbingInterval = Math.min(loadProbingInterval * 2,
            LOAD_PROB_INTERVAL_MAX);
      }
    } else {
      loadProbingInterval = LOAD_PROB_INTERVAL_START;
    }
  }
  
  /**
   * We try to use some light-weight mechanism to determine cluster load.
   * @return Whether, from job client perspective, the cluster is overloaded.
   */
  private boolean isOverloaded(long now) throws IOException {
    try {
      ClusterMetrics clusterMetrics = jobTracker.getClusterMetrics();
      
      // If there are more jobs than number of task trackers, we assume the
      // cluster is overloaded. This is to bound the memory usage of the
      // simulator job tracker, in situations where we have jobs with small
      // number of map tasks and large number of reduce tasks.
      if (runningJobs.size() >= clusterMetrics.getTaskTrackerCount()) {
        System.out.printf("%d Overloaded is %s: " +
                "#runningJobs >= taskTrackerCount (%d >= %d)\n",
                now, Boolean.TRUE.toString(),
                runningJobs.size(), clusterMetrics.getTaskTrackerCount());
        return true;    
      }

      float incompleteMapTasks = 0; // include pending & running map tasks.
      for (Map.Entry<JobID, JobSketchInfo> entry : runningJobs.entrySet()) {
        org.apache.hadoop.mapreduce.JobStatus jobStatus = jobTracker
            .getJobStatus(entry.getKey());
        incompleteMapTasks += (1 - Math.min(jobStatus.getMapProgress(), 1.0))
            * entry.getValue().numMaps;
      }

      boolean overloaded = incompleteMapTasks >
          OVERLAOD_MAPTASK_MAPSLOT_RATIO * clusterMetrics.getMapSlotCapacity();
      String relOp = (overloaded) ? ">" : "<=";
      System.out.printf("%d Overloaded is %s: "
          + "incompleteMapTasks %s %.1f*mapSlotCapacity (%.1f %s %.1f*%d)\n",
          now, Boolean.toString(overloaded), relOp, OVERLAOD_MAPTASK_MAPSLOT_RATIO,
          incompleteMapTasks, relOp, OVERLAOD_MAPTASK_MAPSLOT_RATIO, 
          clusterMetrics.getMapSlotCapacity());
      return overloaded;
    } catch (InterruptedException e) {
      throw new IOException("InterruptedException", e);
    }
  }
  
  /**
   * Handles a simulation event that is either JobSubmissionEvent or 
   * JobCompletionEvent.
   *
   * @param event SimulatorEvent to respond to
   * @return list of events generated in response
   */
  @Override
  public List<SimulatorEvent> accept(SimulatorEvent event) throws IOException {
    if (event instanceof JobSubmissionEvent) {
      return processJobSubmissionEvent((JobSubmissionEvent) event);
    } else if (event instanceof JobCompleteEvent) {
      return processJobCompleteEvent((JobCompleteEvent) event);
    } else if (event instanceof LoadProbingEvent) {
      return processLoadProbingEvent((LoadProbingEvent) event);
    } else {
      throw new IllegalArgumentException("unknown event type: "
          + event.getClass());
    }
  }

  /**
   * Responds to a job submission event by submitting the job to the 
   * job tracker. If serializeJobSubmissions is true, it postpones the
   * submission until after the previous job finished instead.
   * 
   * @param submitEvent the submission event to respond to
   */
  private List<SimulatorEvent> processJobSubmissionEvent(
      JobSubmissionEvent submitEvent) throws IOException {
    // Submit job
    JobStatus status = null;
    JobStory story = submitEvent.getJob();
    try {
      status = submitJob(story);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    runningJobs.put(status.getJobID(), new JobSketchInfo(story.getNumberMaps(),
        story.getNumberReduces()));
    System.out.println("Job " + status.getJobID() + " is submitted at "
        + submitEvent.getTimeStamp());

    // Find the next job to submit
    nextJob = jobStoryProducer.getNextJob();
    if (nextJob == null) {
      noMoreJobs = true;
      return SimulatorEngine.EMPTY_EVENTS;
    } else if (submissionPolicy == SimulatorJobSubmissionPolicy.REPLAY) {
      // enqueue next submission event
      return Collections
          .<SimulatorEvent> singletonList(new JobSubmissionEvent(this, nextJob
              .getSubmissionTime(), nextJob));
    } else if (submissionPolicy == SimulatorJobSubmissionPolicy.STRESS) {
      return checkLoadAndSubmitJob(submitEvent.getTimeStamp());
    }
    
    return SimulatorEngine.EMPTY_EVENTS;
  }
  
  /**
   * Handles a job completion event. 
   * 
   * @param jobCompleteEvent the submission event to respond to
   * @throws IOException 
   */
  private List<SimulatorEvent> processJobCompleteEvent(
      JobCompleteEvent jobCompleteEvent) throws IOException {
    JobStatus jobStatus = jobCompleteEvent.getJobStatus();
    System.out.println("Job " + jobStatus.getJobID() + " completed at "
        + jobCompleteEvent.getTimeStamp() + " with status: "
        + jobStatus.getState() + " runtime: "
        + (jobCompleteEvent.getTimeStamp() - jobStatus.getStartTime()));
    runningJobs.remove(jobCompleteEvent.getJobStatus().getJobID());
    if (noMoreJobs && runningJobs.isEmpty()) {
      jobCompleteEvent.getEngine().shutdown();
    }

    if (!noMoreJobs) {
      if (submissionPolicy == SimulatorJobSubmissionPolicy.SERIAL) {
        long submissionTime = jobCompleteEvent.getTimeStamp() + 1;
        JobStory story = new SimulatorJobStory(nextJob, submissionTime);
        return Collections
            .<SimulatorEvent> singletonList(new JobSubmissionEvent(this,
                submissionTime, story));
      } else if (submissionPolicy == SimulatorJobSubmissionPolicy.STRESS) {
        return checkLoadAndSubmitJob(jobCompleteEvent.getTimeStamp());
      }
    }
    return SimulatorEngine.EMPTY_EVENTS;
  }
  
  /**
   * Check whether job tracker is overloaded. If not, submit the next job.
   * Pre-condition: noMoreJobs == false
   * @return A list of {@link SimulatorEvent}'s as the follow-up actions.
   */
  private List<SimulatorEvent> checkLoadAndSubmitJob(long now) throws IOException {
    List<SimulatorEvent> ret = new ArrayList<SimulatorEvent>(2);
    boolean overloaded = isOverloaded(now);
    adjustLoadProbingInterval(overloaded);
    
    if (inFlightLPE != null && (inFlightLPE.getTimeStamp()>now+loadProbingInterval)) {
      cancelledLPE.put(inFlightLPE, Boolean.TRUE);
      inFlightLPE = null;
    }
    
    if (inFlightLPE == null) {
      inFlightLPE = new LoadProbingEvent(this, now + loadProbingInterval);
      ret.add(inFlightLPE);
    }

    if (!overloaded) {
      long submissionTime = now + 1;
      JobStory story = new SimulatorJobStory(nextJob, submissionTime);
      ret.add(new JobSubmissionEvent(this, submissionTime, story));
    }
    
    return ret;
  }
  
  /**
   * Handles a load probing event. If cluster is not overloaded, submit a new job.
   * 
   * @param loadProbingEvent the load probing event
   */
  private List<SimulatorEvent> processLoadProbingEvent(
      LoadProbingEvent loadProbingEvent) throws IOException {
    if (cancelledLPE.containsKey(loadProbingEvent)) {
      cancelledLPE.remove(loadProbingEvent);
      return SimulatorEngine.EMPTY_EVENTS;
    }
    
    assert(loadProbingEvent == inFlightLPE);
    
    inFlightLPE = null;
    
    if (noMoreJobs) {
      return SimulatorEngine.EMPTY_EVENTS;
    }
    
    return checkLoadAndSubmitJob(loadProbingEvent.getTimeStamp());
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
    return jobTracker.submitJob(jobId, "dummy-path", null);
  }
}
