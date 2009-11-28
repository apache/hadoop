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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskInfo;

public class TestSimulatorJobClient {
  MockSimulatorJobTracker jobTracker = null;
  CheckedEventQueue eventQueue = null;
  SimulatorJobClient jobClient = null;

  static final Log LOG = LogFactory.getLog(TestSimulatorJobClient.class);
    
  long simulationStartTime = 100000;
  final int heartbeatInterval = 5000; // not used other than initializing SimJT
  final long[] jobSubmissionTimes = new long[] {
      1240335960685L,
      1240335962848L,
      1240336843916L,
      1240336853354L,
      1240336893801L,
      1240337079617L,
  };
  
  // assume reading from trace is correct
  @Test
  public final void testRelativeStartTime() throws IOException {
    long relativeStartTime = jobSubmissionTimes[0] - simulationStartTime;
    MockJobStoryProducer jobStoryProducer =
      new MockJobStoryProducer(jobSubmissionTimes, relativeStartTime);
    
    try {
      jobTracker = new MockSimulatorJobTracker(simulationStartTime,
                                               heartbeatInterval, true);
    } catch (Exception e) {
      Assert.fail("Couldn't set up the mock job tracker: " + e);
    }
    eventQueue = new CheckedEventQueue(simulationStartTime);
    jobClient = new SimulatorJobClient(jobTracker, jobStoryProducer);

    // add all expected events
    eventQueue.addExpected(simulationStartTime,
                           new JobSubmissionEvent(jobClient,
                                                  simulationStartTime,
                                                  jobStoryProducer.getJob(0)));
    for (int i = 1; i < jobSubmissionTimes.length; i++) {
      eventQueue.addExpected(jobSubmissionTimes[i-1] - relativeStartTime,
                             new JobSubmissionEvent(jobClient, 
                                                    jobSubmissionTimes[i] - relativeStartTime,
                                                    jobStoryProducer.getJob(i)));
    }

    long runUntil = eventQueue.getLastCheckTime();
    LOG.debug("Running until simulation time=" + runUntil);

    List<SimulatorEvent> events = jobClient.init(simulationStartTime);
    eventQueue.addAll(events);

    while (true) {
      // can't be empty as it must go past runUntil for verifiability
      // besides it is never empty because of HeartbeatEvent            
      SimulatorEvent currentEvent = eventQueue.get();
      // copy time, make sure TT does not modify it
      long now = currentEvent.getTimeStamp();
      LOG.debug("Number of events to deliver=" + (eventQueue.getSize()+1) +
                ", now=" + now);
      LOG.debug("Calling accept(), event=" + currentEvent + ", now=" + now);
      events = jobClient.accept(currentEvent);
      if (now > runUntil) {
        break;
      }                             
      LOG.debug("Accept() returned " + events.size() + " new event(s)");
      for (SimulatorEvent newEvent: events) {
        LOG.debug("New event " + newEvent);
      }
      eventQueue.addAll(events);
      LOG.debug("Done checking and enqueuing new events");
    }
    
    // make sure we have seen all expected events, even for the last 
    // time checked
    LOG.debug("going to check if all expected events have been processed");
    eventQueue.checkMissingExpected();
    // TODO: Mock JT should have consumed all entries from its job submission table
    //jobTracker.checkMissingJobSubmission();
  }

  static class MockJobStoryProducer implements JobStoryProducer {
    private long[] times;
    private int index = 0;
    private List<MockJobStory> jobs = new ArrayList<MockJobStory>();
    
    public MockJobStoryProducer(long[] times, long relativeStartTime) {
      super();
      Assert.assertTrue(times.length > 0);
      this.times = times;
      index = 0;
      
      for (long time: times) {
        jobs.add(new MockJobStory(time - relativeStartTime));
      }
    }
    
    @Override
    public JobStory getNextJob() {
      if (index >= times.length) {
        return null;
      }
      return jobs.get(index++);
    }
    
    public JobStory getJob(int i) {
      return jobs.get(i);
    }

    @Override
    public void close() throws IOException {
    }
  }
  
  static class MockJobStory implements JobStory {
    private long submissionTime;
    
    public MockJobStory(long submissionTime) {
      this.submissionTime = submissionTime;
    }
    
    @Override
    public InputSplit[] getInputSplits() {
      throw new UnsupportedOperationException();
    }

    @Override
    public JobConf getJobConf() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(int taskNumber,
        int taskAttemptNumber, int locality) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
      throw new UnsupportedOperationException();
    }
    
    @Override 
    public JobID getJobID() {
      return null;
    }

    @Override
    public int getNumberMaps() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberReduces() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSubmissionTime() {
      return submissionTime;
    }

    @Override
    public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType,
        int taskNumber, int taskAttemptNumber) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getUser() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Pre21JobHistoryConstants.Values getOutcome() {
      return Pre21JobHistoryConstants.Values.SUCCESS;
    }
  }
}
