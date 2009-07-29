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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapreduce.TaskType;

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
    InterruptedException {
      super(conf, clock);
      this.trackers = tts;
      //initialize max{Map/Reduce} task capacities to twice the clustersize
      totalSlots = trackers.length * 4;
    }
    @Override
    public ClusterStatus getClusterStatus(boolean detailed) {
      return new ClusterStatus(trackers.length,
          0, 0, 0, 0, totalSlots/2, totalSlots/2, JobTracker.State.RUNNING, 0);
    }

    public void setNumSlots(int totalSlots) {
      this.totalSlots = totalSlots;
    }
  }

  static class FakeJobInProgress extends JobInProgress {
    JobClient.RawSplit[] rawSplits;
    @SuppressWarnings("deprecation")
    FakeJobInProgress(JobConf jobConf, JobTracker tracker) throws IOException {
      super(new JobID(jtIdentifier, ++jobCounter), jobConf, tracker);
      Path jobFile = new Path("Dummy");
      this.profile = new JobProfile(jobConf.getUser(), getJobID(), 
          jobFile.toString(), null, jobConf.getJobName(),
          jobConf.getQueueName());
    }

    @Override
    public synchronized void initTasks() throws IOException {
     
      JobClient.RawSplit[] splits = createSplits();
      numMapTasks = splits.length;
      createMapTasks(null, splits);
      nonRunningMapCache = createCache(splits, maxLevel);
      createReduceTasks(null);
      tasksInited.set(true);
      this.status.setRunState(JobStatus.RUNNING);
    }
    
    @Override
    JobClient.RawSplit[] createSplits(){
      JobClient.RawSplit[] splits = new JobClient.RawSplit[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        splits[i] = new JobClient.RawSplit();
        splits[i].setLocations(new String[0]);
      }
      return splits;
    }
    
    @Override
    void createMapTasks(String ignored, JobClient.RawSplit[] splits) {
      maps = new TaskInProgress[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        maps[i] = new TaskInProgress(getJobID(), "test", 
            splits[i], jobtracker, getJobConf(), this, i, 1);
      }
    }

    @Override
    void createReduceTasks(String ignored) {
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

    public void cleanUpMetrics() {
    }
    
    public void setClusterSize(int clusterSize) {
      super.setClusterSize(clusterSize);
    }
  }
  
  static short sendHeartBeat(JobTracker jt, TaskTrackerStatus status, 
                                             boolean initialContact, 
                                             String tracker, short responseId) 
    throws IOException {
    if (status == null) {
      status = new TaskTrackerStatus(tracker, 
          JobInProgress.convertTrackerNameToHostName(tracker));

    }
      jt.heartbeat(status, false, initialContact, false, responseId);
      return ++responseId ;
  }
  
  static void establishFirstContact(JobTracker jt, String tracker) 
    throws IOException {
    sendHeartBeat(jt, null, true, tracker, (short) 0);
  }

}
