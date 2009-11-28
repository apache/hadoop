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
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.apache.hadoop.tools.rumen.MapTaskAttemptInfo;
import org.apache.hadoop.tools.rumen.ReduceTaskAttemptInfo;
//
// Mock jobtracker class that check heartbeat() in parameters and 
// sends responses based on a prepopulated table
//  
public class MockSimulatorJobTracker implements InterTrackerProtocol,
                                                ClientProtocol {
  private final long simulationStartTime;
  private final int heartbeatInterval;
  
  // Helper table, used iff checkHeartbeats == true
  // Contains the expected task tracker status report at time t for all task 
  // trackers identified by their name and the heartbeat response to send
  private SortedMap<Long, TreeMap<String, HeartbeatHelper>> heartbeats = 
      new TreeMap<Long, TreeMap<String, HeartbeatHelper>>();
  private final boolean checkHeartbeats;
  private int jobId = 0;
  
  static final Log LOG = LogFactory.getLog(MockSimulatorJobTracker.class);

  public MockSimulatorJobTracker(long simulationStartTime,
                                 int heartbeatInterval,
                                 boolean checkHeartbeats) {
    this.simulationStartTime = simulationStartTime;
    this.heartbeatInterval = heartbeatInterval;
    this.checkHeartbeats = checkHeartbeats;
  }
  
  @Override
  public JobID getNewJobID() throws IOException {
    return new JobID("mockJT", jobId++);
  }

  @Override
  public JobStatus submitJob(JobID jobId) throws IOException {
    JobStatus status = new JobStatus(jobId, 0.0f, 0.0f, 0.0f, 0.0f,
        JobStatus.State.RUNNING, JobPriority.NORMAL, "", "", "", "");
    return status;
  }

  @Override
  public HeartbeatResponse heartbeat(TaskTrackerStatus status,
      boolean restarted, boolean initialContact, boolean acceptNewTasks,
      short responseId) throws IOException {
    if (!(status instanceof SimulatorTaskTrackerStatus)) {
      throw new IllegalArgumentException(
          "Expecting SimulatorTaskTrackerStatus, actual status type "
              + status.getClass());
    }
    SimulatorTaskTrackerStatus trackerStatus =
        (SimulatorTaskTrackerStatus)status;
    long now = trackerStatus.getCurrentSimulationTime();
    String trackerName = status.getTrackerName();
    
    LOG.debug("Received heartbeat() from trackerName=" + trackerName + 
              ", now=" + now);

    HeartbeatResponse response = new HeartbeatResponse();
    response.setHeartbeatInterval(heartbeatInterval);
    response.setActions(new TaskTrackerAction[0]);
    
    if (checkHeartbeats) {         
      Assert.assertFalse("No more heartbeats were expected ", heartbeats.isEmpty());
      long nextToCheck = heartbeats.firstKey();
      // Missing heartbeat check
      Assert.assertTrue(nextToCheck <= now);
      if (nextToCheck < now) {
        LOG.debug("Simulation time progressed, last checked heartbeat at=" + 
                   nextToCheck + ", now=" + now + ". Checking if no " +
                   "required heartbeats were missed in the past");
        SortedMap<String, HeartbeatHelper> previousHeartbeats = 
            heartbeats.get(nextToCheck);
        Assert.assertNotNull(previousHeartbeats);
        Assert.assertTrue(previousHeartbeats.isEmpty());
        heartbeats.remove(nextToCheck);
        nextToCheck = heartbeats.firstKey();
      }
      Assert.assertEquals("Heartbeat at the wrong time", nextToCheck, now);
      
      SortedMap<String, HeartbeatHelper> currentHeartbeats = 
            heartbeats.get(now);
      HeartbeatHelper currentHeartbeat = currentHeartbeats.get(trackerName);
      Assert.assertNotNull("Unknown task tracker name=" + trackerName,
                    currentHeartbeat);
      currentHeartbeats.remove(trackerName);
    
      currentHeartbeat.checkHeartbeatParameters(status, acceptNewTasks);
    
      response.setActions(currentHeartbeat.getTaskTrackerActions());
    }
    return response;
  }
  
  //
  // Populates the mock jobtracker's helper & checker table with expected
  // empty reports from the task trackers and empty task actions to perform
  // 
  public void expectEmptyHeartbeats(String taskTrackerName, 
                                    int numHeartbeats) {
    long simulationTime = simulationStartTime;
    for (int i=0; i<numHeartbeats; i++) {
      TreeMap<String, HeartbeatHelper> hb = heartbeats.get(simulationTime);
      if (hb == null) {
        hb = new TreeMap<String, HeartbeatHelper>();
        heartbeats.put(simulationTime, hb);
      }
      hb.put(taskTrackerName, new HeartbeatHelper());
      simulationTime += heartbeatInterval;
    }
  }

  // Fills in all the expected and return heartbeat parameters corresponding
  // to running a map task on a task tracker.
  // Use killTime < 0 if not killed
  public void runMapTask(String taskTrackerName, TaskAttemptID taskId,
                         long mapStart, long mapRuntime, long killHeartbeat) {
    long mapDone = mapStart + mapRuntime;
    long mapEndHeartbeat = nextHeartbeat(mapDone);
    final boolean isKilled = (killHeartbeat>=0);
    if (isKilled) {
      mapEndHeartbeat = nextHeartbeat(killHeartbeat + 1);
    }

    LOG.debug("mapStart=" + mapStart + ", mapDone=" + mapDone + 
              ", mapEndHeartbeat=" + mapEndHeartbeat + 
              ", killHeartbeat=" + killHeartbeat);
    
    final int numSlotsRequired = 1;
    org.apache.hadoop.mapred.TaskAttemptID taskIdOldApi = 
        org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId);        
    Task task = new MapTask("dummyjobfile", taskIdOldApi, 0, "dummysplitclass",
                            null, numSlotsRequired);
    // all byte counters are 0
    TaskInfo taskInfo = new TaskInfo(0, 0, 0, 0, 0); 
    MapTaskAttemptInfo taskAttemptInfo = 
        new MapTaskAttemptInfo(State.SUCCEEDED, taskInfo, mapRuntime);
    TaskTrackerAction action = 
        new SimulatorLaunchTaskAction(task, taskAttemptInfo);
    heartbeats.get(mapStart).get(taskTrackerName).addTaskTrackerAction(action);
    if (isKilled) {
      action = new KillTaskAction(taskIdOldApi);
      heartbeats.get(killHeartbeat).get(taskTrackerName).addTaskTrackerAction(
         action);
    }

    for(long simulationTime = mapStart + heartbeatInterval; 
        simulationTime <= mapEndHeartbeat;
        simulationTime += heartbeatInterval) {
      State state = simulationTime < mapEndHeartbeat ? 
          State.RUNNING : State.SUCCEEDED;
      if (simulationTime == mapEndHeartbeat && isKilled) {
        state = State.KILLED;
      }
      MapTaskStatus mapStatus = new MapTaskStatus(
          task.getTaskID(), 0.0f, 0, state, "", "", null, Phase.MAP, null);
      heartbeats.get(simulationTime).get(taskTrackerName).addTaskReport(
         mapStatus);
    }
  }

  // Fills in all the expected and return heartbeat parameters corresponding
  // to running a reduce task on a task tracker.
  // Use killTime<0 if not killed
  public void runReduceTask(String taskTrackerName, TaskAttemptID taskId,
                            long reduceStart, long mapDoneDelay, 
                            long reduceRuntime, long killHeartbeat) {
    long mapDone = nextHeartbeat(reduceStart + mapDoneDelay);
    long reduceDone = mapDone + reduceRuntime;
    long reduceEndHeartbeat = nextHeartbeat(reduceDone);
    final boolean isKilled = (killHeartbeat>=0);
    if (isKilled) {
      reduceEndHeartbeat = nextHeartbeat(killHeartbeat + 1);
    }

    LOG.debug("reduceStart=" + reduceStart + ", mapDone=" + mapDone + 
              ", reduceDone=" + reduceDone + 
              ", reduceEndHeartbeat=" + reduceEndHeartbeat +
              ", killHeartbeat=" + killHeartbeat);

    final int numSlotsRequired = 1;
    org.apache.hadoop.mapred.TaskAttemptID taskIdOldApi = 
        org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId);        
    Task task = new ReduceTask("dummyjobfile", taskIdOldApi, 0, 0,
                               numSlotsRequired);
    // all byte counters are 0
    TaskInfo taskInfo = new TaskInfo(0, 0, 0, 0, 0); 
    ReduceTaskAttemptInfo taskAttemptInfo = 
        new ReduceTaskAttemptInfo(State.SUCCEEDED, taskInfo, 0, 0, 
                                  reduceRuntime);
    TaskTrackerAction action = 
        new SimulatorLaunchTaskAction(task, taskAttemptInfo);    
    heartbeats.get(reduceStart).get(taskTrackerName).addTaskTrackerAction(
        action);
    if (!isKilled || mapDone < killHeartbeat) {
      action = new AllMapsCompletedTaskAction(task.getTaskID());
      heartbeats.get(mapDone).get(taskTrackerName).addTaskTrackerAction(
          action);
    }
    if (isKilled) {
      action = new KillTaskAction(taskIdOldApi);
      heartbeats.get(killHeartbeat).get(taskTrackerName).addTaskTrackerAction(
         action);
    }

    for(long simulationTime = reduceStart + heartbeatInterval; 
        simulationTime <= reduceEndHeartbeat;
        simulationTime += heartbeatInterval) {
      State state = simulationTime < reduceEndHeartbeat ? 
          State.RUNNING : State.SUCCEEDED;
      if (simulationTime == reduceEndHeartbeat && isKilled) {
        state = State.KILLED;
      }
      // mapDone is when the all maps done event delivered
      Phase phase = simulationTime <= mapDone ? Phase.SHUFFLE : Phase.REDUCE; 
      ReduceTaskStatus reduceStatus = new ReduceTaskStatus(
          task.getTaskID(), 0.0f, 0, state, "", "", null, phase, null);
      heartbeats.get(simulationTime).get(taskTrackerName).addTaskReport(
          reduceStatus);
    }
  }
  
  // Should be called at the end of the simulation: Mock JT should have 
  // consumed all entries from the heartbeats table by that time
  public void checkMissingHeartbeats() {
    Assert.assertEquals(1, heartbeats.size());
    long lastHeartbeat = heartbeats.firstKey();
    Assert.assertTrue("Missing heartbeats, last heartbeat=" + lastHeartbeat,
               heartbeats.get(lastHeartbeat).isEmpty());
  }
          
  // rounds up to the next heartbeat time
  public long nextHeartbeat(long time) {
    long numHeartbeats = (long)Math.ceil(
        (time - simulationStartTime)/(double)heartbeatInterval);
    return simulationStartTime + numHeartbeats * heartbeatInterval;
  }
  
  // Rest of InterTrackerProtocol follows, unused in simulation
  @Override
  public String getFilesystemName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reportTaskTrackerError(String taskTracker,
                                     String errorClass,
                                     String errorMessage) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid,
      int fromEventId, int maxEvents) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSystemDir() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public String getBuildVersion() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public long getProtocolVersion(String protocol, long clientVersion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(
      org.apache.hadoop.mapred.JobID jobid, int fromEventId, int maxEvents)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueueInfo[] getChildQueues(String queueName) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Counters getJobCounters(JobID jobid) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getJobHistoryDir() throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public JobStatus getJobStatus(JobID jobid) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.mapreduce.server.jobtracker.State getJobTrackerState()
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueueInfo getQueue(String queueName) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();

  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();

  }

  @Override
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    throw new UnsupportedOperationException();

  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    throw new UnsupportedOperationException();

  }

  @Override
  public String[] getTaskDiagnostics(TaskAttemptID taskId) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskReport[] getTaskReports(JobID jobid, TaskType type)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void killJob(JobID jobid) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setJobPriority(JobID jobid, String priority) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

}
