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
import java.util.Collection;
import java.util.Iterator;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;

/** 
 * Utilities used in unit test.
 *  
 */
public class FakeObjectUtilities {

  static final Log LOG = LogFactory.getLog(FakeObjectUtilities.class);

  private static String jtIdentifier = "test";
  private static int jobCounter;
  
  /**
   * A Fake JobTracker class for use in Unit Tests
   */
  static class FakeJobTracker extends JobTracker {
    
    int totalSlots;
    private String[] trackers;

    FakeJobTracker(JobConf conf, Clock clock, String[] tts) throws IOException, 
    InterruptedException, LoginException {
      super(conf, clock);
      this.trackers = tts;
      //initialize max{Map/Reduce} task capacities to twice the clustersize
      totalSlots = trackers.length * 4;
    }
    @Override
    public ClusterStatus getClusterStatus(boolean detailed) {
      return new ClusterStatus(
          taskTrackers().size() - getBlacklistedTrackerCount(),
          getBlacklistedTrackerCount(), 0, 0, 0, totalSlots/2, totalSlots/2, 
           JobTrackerStatus.RUNNING, 0);
    }

    public void setNumSlots(int totalSlots) {
      this.totalSlots = totalSlots;
    }
  }

  static class FakeJobInProgress extends JobInProgress {
    @SuppressWarnings("deprecation")
    FakeJobInProgress(JobConf jobConf, JobTracker tracker) throws IOException {
      super(new JobID(jtIdentifier, ++jobCounter), jobConf, tracker);
      Path jobFile = new Path("Dummy");
      this.profile = new JobProfile(jobConf.getUser(), getJobID(), 
          jobFile.toString(), null, jobConf.getJobName(),
          jobConf.getQueueName());
      this.jobHistory = new FakeJobHistory();
    }

    @Override
    public synchronized void initTasks() throws IOException {
     
      TaskSplitMetaInfo[] taskSplitMetaInfo = createSplits(jobId);
      numMapTasks = taskSplitMetaInfo.length;
      createMapTasks(null, taskSplitMetaInfo);
      nonRunningMapCache = createCache(taskSplitMetaInfo, maxLevel);
      createReduceTasks(null);
      tasksInited.set(true);
      this.status.setRunState(JobStatus.RUNNING);
    }
    
    @Override
    TaskSplitMetaInfo [] createSplits(org.apache.hadoop.mapreduce.JobID jobId){
      TaskSplitMetaInfo[] splits = 
        new TaskSplitMetaInfo[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        splits[i] = JobSplit.EMPTY_TASK_SPLIT;
      }
      return splits;
    }
    
    @Override
    protected void createMapTasks(String ignored, TaskSplitMetaInfo[] splits) {
      maps = new TaskInProgress[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        maps[i] = new TaskInProgress(getJobID(), "test", 
            splits[i], jobtracker, getJobConf(), this, i, 1);
      }
    }

    @Override
    protected void createReduceTasks(String ignored) {
      reduces = new TaskInProgress[numReduceTasks];
      for (int i = 0; i < numReduceTasks; i++) {
        reduces[i] = new TaskInProgress(getJobID(), "test", 
            numMapTasks, i, 
            jobtracker, getJobConf(), this, 1);
        nonRunningReduces.add(reduces[i]);
      }
    }

    private TaskAttemptID findTask(String trackerName, String trackerHost,
        Collection<TaskInProgress> nonRunningTasks, 
        Collection<TaskInProgress> runningTasks, TaskType taskType)
    throws IOException {
      TaskInProgress tip = null;
      Iterator<TaskInProgress> iter = nonRunningTasks.iterator();
      //look for a non-running task first
      while (iter.hasNext()) {
        TaskInProgress t = iter.next();
        if (t.isRunnable() && !t.isRunning()) {
          runningTasks.add(t);
          iter.remove();
          tip = t;
          break;
        }
      }
      if (tip == null) {
        if (getJobConf().getSpeculativeExecution()) {
          tip = findSpeculativeTask(runningTasks, trackerName, trackerHost,
              taskType);
        }
      }
      if (tip != null) {
        TaskAttemptID tId = tip.getTaskToRun(trackerName).getTaskID();
        if (tip.isMapTask()) {
          scheduleMap(tip);
        } else {
          scheduleReduce(tip);
        }
        //Set it to RUNNING
        makeRunning(tId, tip, trackerName);
        return tId;
      }
      return null;
    }

    public TaskAttemptID findMapTask(String trackerName)
    throws IOException {
      return findTask(trackerName, 
          JobInProgress.convertTrackerNameToHostName(trackerName),
          nonLocalMaps, nonLocalRunningMaps, TaskType.MAP);
    }

    public TaskAttemptID findReduceTask(String trackerName) 
    throws IOException {
      return findTask(trackerName, 
          JobInProgress.convertTrackerNameToHostName(trackerName),
          nonRunningReduces, runningReduces, TaskType.REDUCE);
    }

    public void finishTask(TaskAttemptID taskId) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          1.0f, 1, TaskStatus.State.SUCCEEDED, "", "", 
          tip.machineWhereTaskRan(taskId), 
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }
  
    private void makeRunning(TaskAttemptID taskId, TaskInProgress tip, 
        String taskTracker) {
      addRunningTaskToTIP(tip, taskId, new TaskTrackerStatus(taskTracker,
          JobInProgress.convertTrackerNameToHostName(taskTracker)), true);

      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          0.0f, 1, TaskStatus.State.RUNNING, "", "", taskTracker,
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }

    public void progressMade(TaskAttemptID taskId, float progress) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          progress, 1, TaskStatus.State.RUNNING, "", "", 
          tip.machineWhereTaskRan(taskId), 
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }
    
    public void failTask(TaskAttemptID taskId) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId,
          1.0f, 1, TaskStatus.State.FAILED, "", "", tip
              .machineWhereTaskRan(taskId), tip.isMapTask() ? Phase.MAP
              : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }

    public void killTask(TaskAttemptID taskId) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId,
          1.0f, 1, TaskStatus.State.KILLED, "", "", tip
              .machineWhereTaskRan(taskId), tip.isMapTask() ? Phase.MAP
              : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }

    public void cleanUpMetrics() {
    }
    
    public void setClusterSize(int clusterSize) {
      super.setClusterSize(clusterSize);
    }
  }
  
  static short sendHeartBeat(JobTracker jt, TaskTrackerStatus status, 
		  boolean initialContact, boolean acceptNewTasks,
                                             String tracker, short responseId) 
    throws IOException {
    if (status == null) {
      status = new TaskTrackerStatus(tracker, 
          JobInProgress.convertTrackerNameToHostName(tracker));

    }
      jt.heartbeat(status, false, initialContact, acceptNewTasks, responseId);
      return ++responseId ;
  }
  
  static void establishFirstContact(JobTracker jt, String tracker) 
    throws IOException {
    sendHeartBeat(jt, null, true, false, tracker, (short) 0);
  }

  static class FakeTaskInProgress extends TaskInProgress {

    public FakeTaskInProgress(JobID jobId, String jobFile, int numMaps,
        int partition, JobTracker jobTracker, JobConf conf, JobInProgress job,
        int numSlotsRequired) {
      super(jobId, jobFile, numMaps, partition, jobTracker, conf, job,
          numSlotsRequired);
    }

    public FakeTaskInProgress(JobID jobId, String jobFile, TaskSplitMetaInfo emptySplit,
        JobTracker jobTracker, JobConf jobConf,
        JobInProgress job, int partition, int numSlotsRequired) {
      super(jobId, jobFile, emptySplit, jobTracker, jobConf, job,
            partition, numSlotsRequired);
    }

    @Override
    synchronized boolean updateStatus(TaskStatus status) {
      TaskAttemptID taskid = status.getTaskID();
      taskStatuses.put(taskid, status);
      return false;
    }
  }
  
  static class FakeJobHistory extends JobHistory {
    @Override
    public void init(JobTracker jt, 
        JobConf conf,
        String hostname, 
        long jobTrackerStartTime) throws IOException { }
    
    @Override
    public void initDone(JobConf conf, FileSystem fs) throws IOException { }
    
    @Override
    public void markCompleted(org.apache.hadoop.mapreduce.JobID id)
    throws IOException { }
    
    @Override
    public void shutDown() { }

    @Override
    public void 
    logEvent(HistoryEvent event, org.apache.hadoop.mapreduce.JobID id) { }
    
    @Override
    public void closeWriter(org.apache.hadoop.mapreduce.JobID id) { }
  }

  static class FakeJobTrackerMetricsInst extends JobTrackerInstrumentation  {
    public FakeJobTrackerMetricsInst(JobTracker tracker, JobConf conf) {
      super(tracker, conf);
    }

    int numMapTasksLaunched = 0;
    int numMapTasksCompleted = 0;
    int numMapTasksFailed = 0;
    int numReduceTasksLaunched = 0;
    int numReduceTasksCompleted = 0;
    int numReduceTasksFailed = 0;
    int numJobsSubmitted = 0;
    int numJobsCompleted = 0;
    int numWaitingMaps = 0;
    int numWaitingReduces = 0;
    int numSpeculativeMaps = 0;
    int numSpeculativeReduces = 0;
    int numDataLocalMaps = 0;
    int numRackLocalMaps = 0;

    //Cluster status fields.
    volatile int numMapSlots = 0;
    volatile int numReduceSlots = 0;
    int numBlackListedMapSlots = 0;
    int numBlackListedReduceSlots = 0;

    int numReservedMapSlots = 0;
    int numReservedReduceSlots = 0;
    int numOccupiedMapSlots = 0;
    int numOccupiedReduceSlots = 0;

    int numJobsFailed = 0;
    int numJobsKilled = 0;

    int numJobsPreparing = 0;
    int numJobsRunning = 0;

    int numRunningMaps = 0;
    int numRunningReduces = 0;

    int numMapTasksKilled = 0;
    int numReduceTasksKilled = 0;

    int numTrackers = 0;
    int numTrackersBlackListed = 0;

    int numTrackersDecommissioned = 0;

    long numHeartbeats = 0;

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

    @Override
    public synchronized void heartbeat() {
      ++numHeartbeats;
    }

    @Override
    public synchronized void speculateReduce(TaskAttemptID taskAttemptID) {
      ++numSpeculativeReduces;
    }

    @Override
    public synchronized void speculateMap(TaskAttemptID taskAttemptID) {
      ++numSpeculativeMaps;
    }

    @Override
    public synchronized void launchDataLocalMap(TaskAttemptID taskAttemptID) {
      ++numDataLocalMaps;
    }

    @Override
    public synchronized void launchRackLocalMap(TaskAttemptID taskAttemptID) {
      ++numRackLocalMaps;
    }
  }
}
