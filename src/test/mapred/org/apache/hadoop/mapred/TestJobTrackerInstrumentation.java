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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.mapred.TestTaskTrackerBlacklisting.FakeJobTracker;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

@SuppressWarnings("deprecation")
public class TestJobTrackerInstrumentation extends TestCase {

  static String trackers[] = new String[] { 
    "tracker_tracker1:1000",
    "tracker_tracker2:1000",
    "tracker_tracker3:1000" };

  static String hosts[] = new String[] { "tracker1", "tracker2", "tracker3" };
  // heartbeat responseId. increment this after sending a heartbeat
  private static short responseId = 1;

  private static FakeJobTracker jobTracker;
  private static FakeJobInProgress  fakeJob;

  private static int mapSlotsPerTracker = 4;
  private static int reduceSlotsPerTracker = 2;

  private static int numMapSlotsToReserve = 2;
  private static int numReduceSlotsToReserve = 2;

  private static MyJobTrackerMetricsInst mi;
  
  

  public static Test suite() {
    TestSetup setup = 
      new TestSetup(new TestSuite(TestJobTrackerInstrumentation.class)) {
      protected void setUp() throws Exception {
        JobConf conf = new JobConf();
        conf.set(JTConfig.JT_IPC_ADDRESS, "localhost:0");
        conf.set(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:0");
        conf.setInt(JTConfig.JT_MAX_TRACKER_BLACKLISTS, 1);
        conf.setClass(JTConfig.JT_TASK_SCHEDULER, 
            FakeTaskScheduler.class, TaskScheduler.class);

        conf.set(JTConfig.JT_INSTRUMENTATION, 
            MyJobTrackerMetricsInst.class.getName());
        jobTracker = new FakeJobTracker(conf, new FakeClock(), trackers);
        mi = (MyJobTrackerMetricsInst) jobTracker.getInstrumentation();
        for (String tracker : trackers) {
          FakeObjectUtilities.establishFirstContact(jobTracker, tracker);
        }
        
      }
      protected void tearDown() throws Exception {
      }
    };
    return setup;
  }

  private TaskTrackerStatus getTTStatus(String trackerName,
      List<TaskStatus> taskStatuses) {
    return new TaskTrackerStatus(trackerName, 
        JobInProgress.convertTrackerNameToHostName(trackerName), 0,
        taskStatuses, 0, mapSlotsPerTracker, reduceSlotsPerTracker);
  }

  public void testMetrics() throws Exception {
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[3];
    
    // create TaskTrackerStatus and send heartbeats
    TaskTrackerStatus[] status = new TaskTrackerStatus[trackers.length];
    status[0] = getTTStatus(trackers[0], new ArrayList<TaskStatus>());
    status[1] = getTTStatus(trackers[1], new ArrayList<TaskStatus>());
    status[2] = getTTStatus(trackers[2], new ArrayList<TaskStatus>());
    for (int i = 0; i< trackers.length; i++) {
      FakeObjectUtilities.sendHeartBeat(jobTracker, status[i], false,
          false, trackers[i], responseId);
    }
    responseId++;

    assertEquals("Mismatch in number of trackers",
        trackers.length, mi.numTrackers);
    
    int numMaps = 2;
    int numReds = 1;
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(false);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);
    conf.setMaxTaskFailuresPerTracker(1);
    conf.setBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);

    FakeJobInProgress job = new FakeJobInProgress(conf, jobTracker);
    assertTrue(mi.numJobsPreparing == 1);

    job.setClusterSize(trackers.length);
    job.initTasks();
    jobTracker.addJob(job.getJobID(), job);

    taskAttemptID[0] = job.findMapTask(trackers[0]);
    taskAttemptID[1] = job.findMapTask(trackers[1]);
    taskAttemptID[2] = job.findReduceTask(trackers[2]);
    
    job.finishTask(taskAttemptID[0]);
    job.finishTask(taskAttemptID[1]);
    job.finishTask(taskAttemptID[2]);
    jobTracker.finalizeJob(job);
 
    assertTrue("Mismatch in map tasks launched",
        mi.numMapTasksLaunched == numMaps);
    assertTrue("Mismatch in map tasks completed",
        mi.numMapTasksCompleted == numMaps);
    assertTrue("Mismatch in map tasks failed",
        mi.numMapTasksFailed == 0);

    assertTrue("Mismatch in reduce tasks launched",
        mi.numReduceTasksLaunched == numReds);
    assertTrue("Mismatch in reduce tasks completed",
        mi.numReduceTasksCompleted == numReds);
    assertTrue("Mismatch in reduce tasks failed",
        mi.numReduceTasksFailed == 0);

    assertTrue("Mismatch in num Jobs submitted",
        mi.numJobsSubmitted == 1);
    
    assertTrue("Mismatch in num map slots",
        mi.numMapSlots == (mapSlotsPerTracker * trackers.length));
    assertTrue("Mismatch in num reduce slots",
        mi.numReduceSlots == (reduceSlotsPerTracker * trackers.length));

  }
  
  public void testBlackListing() throws IOException {
    int numMaps, numReds;
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(false);
    conf.setMaxTaskFailuresPerTracker(1);
    conf.setBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[3];

    numMaps = 1;
    numReds = 1;
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);
    conf.setBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);

    FakeJobInProgress job1 = new FakeJobInProgress(conf, jobTracker);
    job1.setClusterSize(trackers.length);
    job1.initTasks();
    jobTracker.addJob(job1.getJobID(), job1);
    taskAttemptID[0] = job1.findMapTask(trackers[0]);
    job1.failTask(taskAttemptID[0]);
    taskAttemptID[1] = job1.findMapTask(trackers[1]);
    job1.finishTask(taskAttemptID[1]);
    taskAttemptID[2] = job1.findReduceTask(trackers[0]);
    job1.failTask(taskAttemptID[2]);
    taskAttemptID[2] = job1.findReduceTask(trackers[2]);
    job1.finishTask(taskAttemptID[2]);
    jobTracker.finalizeJob(job1);

    assertEquals("Mismatch in number of failed map tasks",
        1, mi.numMapTasksFailed);
    assertEquals("Mismatch in number of failed reduce tasks",
        1, mi.numReduceTasksFailed);
    
    assertEquals("Mismatch in number of blacklisted trackers",
        1, mi.numTrackersBlackListed);

    assertEquals("Mismatch in blacklisted map slots", 
        mi.numBlackListedMapSlots, 
        (mapSlotsPerTracker * mi.numTrackersBlackListed));

    assertEquals("Mismatch in blacklisted reduce slots", 
        mi.numBlackListedReduceSlots, 
        (reduceSlotsPerTracker * mi.numTrackersBlackListed));
  }

  public void testOccupiedSlotCounts() throws Exception {

    TaskTrackerStatus[] status = new TaskTrackerStatus[trackers.length];

    List<TaskStatus> list = new ArrayList<TaskStatus>();

    // create a map task status, which uses 2 slots. 
    int mapSlotsPerTask = 2;
    TaskStatus ts = TaskStatus.createTaskStatus(true, 
      new TaskAttemptID("jt", 1, TaskType.MAP, 0, 0), 0.0f, mapSlotsPerTask,
      TaskStatus.State.RUNNING, "", "", trackers[0], 
      TaskStatus.Phase.MAP, null);
    list.add(ts);
    int mapSlotsPerTask1 = 1;
    ts = TaskStatus.createTaskStatus(true, 
        new TaskAttemptID("jt", 1, TaskType.MAP, 0, 0), 0.0f, mapSlotsPerTask1,
        TaskStatus.State.RUNNING, "", "", trackers[0], 
        TaskStatus.Phase.MAP, null);
      list.add(ts);
    
    // create a reduce task status, which uses 3 slot.
    int reduceSlotsPerTask = 3;
    ts = TaskStatus.createTaskStatus(false, 
      new TaskAttemptID("jt", 1, TaskType.REDUCE, 0, 0), 0.0f,
      reduceSlotsPerTask,
      TaskStatus.State.RUNNING, "", "", trackers[0], 
      TaskStatus.Phase.REDUCE, null);
    list.add(ts);
    int reduceSlotsPerTask1 = 1;
    ts = TaskStatus.createTaskStatus(false, 
        new TaskAttemptID("jt", 1, TaskType.REDUCE, 0, 0), 0.0f,
        reduceSlotsPerTask1,
        TaskStatus.State.RUNNING, "", "", trackers[0], 
        TaskStatus.Phase.REDUCE, null);
    list.add(ts);
    
    // create TaskTrackerStatus and send heartbeats
    status = new TaskTrackerStatus[trackers.length];
    status[0] = getTTStatus(trackers[0], list);
    status[1] = getTTStatus(trackers[1], new ArrayList<TaskStatus>());
    status[2] = getTTStatus(trackers[2], new ArrayList<TaskStatus>());
    for (int i = 0; i< trackers.length; i++) {
      FakeObjectUtilities.sendHeartBeat(jobTracker, status[i], false,
        false, trackers[i], responseId);
    }
    responseId++;

    assertEquals("Mismatch in map slots occupied",
      mapSlotsPerTask+mapSlotsPerTask1, mi.numOccupiedMapSlots);
    assertEquals("Mismatch in reduce slots occupied",
      reduceSlotsPerTask+reduceSlotsPerTask1, mi.numOccupiedReduceSlots);
    assertEquals("Mismatch in num  running maps",
        2, mi.numRunningMaps);
      assertEquals("Mismatch in num running reduces",
        2, mi.numRunningReduces);
    
    //now send heartbeat with no running tasks
    status = new TaskTrackerStatus[1];
    status[0] = getTTStatus(trackers[0], new ArrayList<TaskStatus>());
    FakeObjectUtilities.sendHeartBeat(jobTracker, status[0], false,
        false, trackers[0], responseId);
    
    assertEquals("Mismatch in map slots occupied",
      0, mi.numOccupiedMapSlots);
    assertEquals("Mismatch in reduce slots occupied",
      0, mi.numOccupiedReduceSlots);
    assertEquals("Mismatch in num  running maps",
      0, mi.numRunningMaps);
    assertEquals("Mismatch in num running reduces",
      0, mi.numRunningReduces);
  }

  public void testReservedSlots() throws IOException {
      JobConf conf = new JobConf();
      conf.setNumMapTasks(1);
      conf.setNumReduceTasks(1);
      conf.setSpeculativeExecution(false);
      
      //Set task tracker objects for reservation.
      TaskTracker tt2 = jobTracker.getTaskTracker(trackers[1]);
      TaskTrackerStatus status2 = new TaskTrackerStatus(
          trackers[1],JobInProgress.convertTrackerNameToHostName(
              trackers[1]),0,new ArrayList<TaskStatus>(), 0, 2, 2);
      tt2.setStatus(status2);
      
      fakeJob = new FakeJobInProgress(conf, jobTracker);
      fakeJob.setClusterSize(3);
      fakeJob.initTasks();
      
      FakeObjectUtilities.sendHeartBeat(jobTracker, status2, false,
        true, trackers[1], responseId);
      responseId++; 
      
      assertEquals("Mismtach in reserved map slots", 
        numMapSlotsToReserve, mi.numReservedMapSlots); 
      assertEquals("Mismtach in reserved red slots", 
        numReduceSlotsToReserve, mi.numReservedReduceSlots); 
  }
  
  public void testDecomissionedTrackers() throws IOException {
    // create TaskTrackerStatus and send heartbeats
    TaskTrackerStatus[] status = new TaskTrackerStatus[trackers.length];
    status[0] = getTTStatus(trackers[0], new ArrayList<TaskStatus>());
    status[1] = getTTStatus(trackers[1], new ArrayList<TaskStatus>());
    status[2] = getTTStatus(trackers[2], new ArrayList<TaskStatus>());
    for (int i = 0; i< trackers.length; i++) {
      FakeObjectUtilities.sendHeartBeat(jobTracker, status[i], false,
          false, trackers[i], responseId);
    }
    
    assertEquals("Mismatch in number of trackers",
        trackers.length, mi.numTrackers);
    Set<String> dHosts = new HashSet<String>();
    dHosts.add(hosts[1]);
    assertEquals("Mismatch in number of decommissioned trackers",
        0, mi.numTrackersDecommissioned);
    jobTracker.decommissionNodes(dHosts);
    assertEquals("Mismatch in number of decommissioned trackers",
        1, mi.numTrackersDecommissioned);
    assertEquals("Mismatch in number of trackers",
        trackers.length - 1, mi.numTrackers);
  }
  
  public void testKillTasks() throws IOException {
    int numMaps, numReds;
    JobConf conf = new JobConf();
    conf.setSpeculativeExecution(false);
    conf.setMaxTaskFailuresPerTracker(1);
    conf.setBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);
    TaskAttemptID[] taskAttemptID = new TaskAttemptID[2];

    numMaps = 1;
    numReds = 1;
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);
    conf.setBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);

    assertEquals("Mismatch in number of killed map tasks",
        0, mi.numMapTasksKilled);
    assertEquals("Mismatch in number of killed reduce tasks",
        0, mi.numReduceTasksKilled);
    
    FakeJobInProgress job1 = new FakeJobInProgress(conf, jobTracker);
    job1.setClusterSize(trackers.length);
    job1.initTasks();
    jobTracker.addJob(job1.getJobID(), job1);
    taskAttemptID[0] = job1.findMapTask(trackers[0]);
    job1.killTask(taskAttemptID[0]);
    taskAttemptID[1] = job1.findReduceTask(trackers[0]);
    job1.killTask(taskAttemptID[1]);
    jobTracker.finalizeJob(job1);

    assertEquals("Mismatch in number of killed map tasks",
        1, mi.numMapTasksKilled);
    assertEquals("Mismatch in number of killed reduce tasks",
        1, mi.numReduceTasksKilled);
  }
  
  static class FakeTaskScheduler extends JobQueueTaskScheduler {
    public FakeTaskScheduler() {
      super();
    }
    public List<Task> assignTasks(TaskTracker tt) {
      tt.reserveSlots(TaskType.MAP, fakeJob, numMapSlotsToReserve);
      tt.reserveSlots(TaskType.REDUCE, fakeJob, numReduceSlotsToReserve);
      return new ArrayList<Task>();  
    }
  }
  
  static class FakeJobInProgress extends
  org.apache.hadoop.mapred.TestTaskTrackerBlacklisting.FakeJobInProgress {

    FakeJobInProgress(JobConf jobConf, JobTracker tracker) throws IOException {
      super(jobConf, tracker);
    }

    @Override 
    public synchronized void initTasks() throws IOException {
      super.initTasks();
      jobtracker.getInstrumentation().addWaitingMaps(getJobID(),
          numMapTasks);
      jobtracker.getInstrumentation().addWaitingReduces(getJobID(),
          numReduceTasks);
    }
  }

  static class MyJobTrackerMetricsInst extends JobTrackerInstrumentation  {
    public MyJobTrackerMetricsInst(JobTracker tracker, JobConf conf) {
      super(tracker, conf);
    }

    private int numMapTasksLaunched = 0;
    private int numMapTasksCompleted = 0;
    private int numMapTasksFailed = 0;
    private int numReduceTasksLaunched = 0;
    private int numReduceTasksCompleted = 0;
    private int numReduceTasksFailed = 0;
    private int numJobsSubmitted = 0;
    private int numJobsCompleted = 0;
    private int numWaitingMaps = 0;
    private int numWaitingReduces = 0;

    //Cluster status fields.
    private volatile int numMapSlots = 0;
    private volatile int numReduceSlots = 0;
    private int numBlackListedMapSlots = 0;
    private int numBlackListedReduceSlots = 0;

    private int numReservedMapSlots = 0;
    private int numReservedReduceSlots = 0;
    private int numOccupiedMapSlots = 0;
    private int numOccupiedReduceSlots = 0;
    
    private int numJobsFailed = 0;
    private int numJobsKilled = 0;
    
    private int numJobsPreparing = 0;
    private int numJobsRunning = 0;
    
    private int numRunningMaps = 0;
    private int numRunningReduces = 0;
    
    private int numMapTasksKilled = 0;
    private int numReduceTasksKilled = 0;

    private int numTrackers = 0;
    private int numTrackersBlackListed = 0;

    private int numTrackersDecommissioned = 0;

    @Override
    public synchronized void launchMap(TaskAttemptID taskAttemptID) {
      ++numMapTasksLaunched;
      decWaitingMaps(taskAttemptID.getJobID(), 1);
    }

    @Override
    public synchronized void completeMap(TaskAttemptID taskAttemptID) {
      ++numMapTasksCompleted;
    }

    @Override
    public synchronized void failedMap(TaskAttemptID taskAttemptID) {
      ++numMapTasksFailed;
      addWaitingMaps(taskAttemptID.getJobID(), 1);
    }

    @Override
    public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
      ++numReduceTasksLaunched;
      decWaitingReduces(taskAttemptID.getJobID(), 1);
    }

    @Override
    public synchronized void completeReduce(TaskAttemptID taskAttemptID) {
      ++numReduceTasksCompleted;
    }

    @Override
    public synchronized void failedReduce(TaskAttemptID taskAttemptID) {
      ++numReduceTasksFailed;
      addWaitingReduces(taskAttemptID.getJobID(), 1);
    }

    @Override
    public synchronized void submitJob(JobConf conf, JobID id) {
      ++numJobsSubmitted;
    }

    @Override
    public synchronized void completeJob(JobConf conf, JobID id) {
      ++numJobsCompleted;
    }

    @Override
    public synchronized void addWaitingMaps(JobID id, int task) {
      numWaitingMaps  += task;
    }

    @Override
    public synchronized void decWaitingMaps(JobID id, int task) {
      numWaitingMaps -= task;
    }

    @Override
    public synchronized void addWaitingReduces(JobID id, int task) {
      numWaitingReduces += task;
    }

    @Override
    public synchronized void decWaitingReduces(JobID id, int task){
      numWaitingReduces -= task;
    }

    @Override
    public void setMapSlots(int slots) {
      numMapSlots = slots;
    }

    @Override
    public void setReduceSlots(int slots) {
      numReduceSlots = slots;
    }

    @Override
    public synchronized void addBlackListedMapSlots(int slots){
      numBlackListedMapSlots += slots;
    }

    @Override
    public synchronized void decBlackListedMapSlots(int slots){
      numBlackListedMapSlots -= slots;
    }

    @Override
    public synchronized void addBlackListedReduceSlots(int slots){
      numBlackListedReduceSlots += slots;
    }

    @Override
    public synchronized void decBlackListedReduceSlots(int slots){
      numBlackListedReduceSlots -= slots;
    }
    
    @Override
    public synchronized void addReservedMapSlots(int slots)
    { 
      numReservedMapSlots += slots;
    }

    @Override
    public synchronized void decReservedMapSlots(int slots)
    {
      numReservedMapSlots -= slots;
    }

    @Override
    public synchronized void addReservedReduceSlots(int slots)
    {
      numReservedReduceSlots += slots;
    }

    @Override
    public synchronized void decReservedReduceSlots(int slots)
    {
      numReservedReduceSlots -= slots;
    }

    @Override
    public synchronized void addOccupiedMapSlots(int slots)
    {
      numOccupiedMapSlots += slots;
    }

    @Override
    public synchronized void decOccupiedMapSlots(int slots)
    {
      numOccupiedMapSlots -= slots;
    }

    @Override
    public synchronized void addOccupiedReduceSlots(int slots)
    {
      numOccupiedReduceSlots += slots;
    }

    @Override
    public synchronized void decOccupiedReduceSlots(int slots)
    {
      numOccupiedReduceSlots -= slots;
    }

    @Override
    public synchronized void failedJob(JobConf conf, JobID id) 
    {
      numJobsFailed++;
    }

    @Override
    public synchronized void killedJob(JobConf conf, JobID id) 
    {
      numJobsKilled++;
    }

    @Override
    public synchronized void addPrepJob(JobConf conf, JobID id) 
    {
      numJobsPreparing++;
    }

    @Override
    public synchronized void decPrepJob(JobConf conf, JobID id) 
    {
      numJobsPreparing--;
    }

    @Override
    public synchronized void addRunningJob(JobConf conf, JobID id) 
    {
      numJobsRunning++;
    }

    @Override
    public synchronized void decRunningJob(JobConf conf, JobID id) 
    {
      numJobsRunning--;
    }

    @Override
    public synchronized void addRunningMaps(int task)
    {
      numRunningMaps += task;
    }

    @Override
    public synchronized void decRunningMaps(int task) 
    {
      numRunningMaps -= task;
    }

    @Override
    public synchronized void addRunningReduces(int task)
    {
      numRunningReduces += task;
    }

    @Override
    public synchronized void decRunningReduces(int task)
    {
      numRunningReduces -= task;
    }

    @Override
    public synchronized void killedMap(TaskAttemptID taskAttemptID)
    {
      numMapTasksKilled++;
    }

    @Override
    public synchronized void killedReduce(TaskAttemptID taskAttemptID)
    {
      numReduceTasksKilled++;
    }

    @Override
    public synchronized void addTrackers(int trackers)
    {
      numTrackers += trackers;
    }

    @Override
    public synchronized void decTrackers(int trackers)
    {
      numTrackers -= trackers;
    }

    @Override
    public synchronized void addBlackListedTrackers(int trackers)
    {
      numTrackersBlackListed += trackers;
    }

    @Override
    public synchronized void decBlackListedTrackers(int trackers)
    {
      numTrackersBlackListed -= trackers;
    }

    @Override
    public synchronized void setDecommissionedTrackers(int trackers)
    {
      numTrackersDecommissioned = trackers;
    }
  }
}
