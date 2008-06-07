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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
/**************************************************
 * Describes the current status of a task.  This is
 * not intended to be a comprehensive piece of data.
 *
 **************************************************/
abstract class TaskStatus implements Writable, Cloneable {
  static final Log LOG =
    LogFactory.getLog(TaskStatus.class.getName());
  
  //enumeration for reporting current phase of a task. 
  public static enum Phase{STARTING, MAP, SHUFFLE, SORT, REDUCE}

  // what state is the task in?
  public static enum State {RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED, 
                            COMMIT_PENDING}
    
  private TaskAttemptID taskid;
  private float progress;
  private State runState;
  private String diagnosticInfo;
  private String stateString;
  private String taskTracker;
    
  private long startTime; 
  private long finishTime; 
    
  private Phase phase = Phase.STARTING; 
  private Counters counters;
  private boolean includeCounters;

  public TaskStatus() {}

  public TaskStatus(TaskAttemptID taskid, float progress,
                    State runState, String diagnosticInfo,
                    String stateString, String taskTracker,
                    Phase phase, Counters counters) {
    this.taskid = taskid;
    this.progress = progress;
    this.runState = runState;
    this.diagnosticInfo = diagnosticInfo;
    this.stateString = stateString;
    this.taskTracker = taskTracker;
    this.phase = phase;
    this.counters = counters;
    this.includeCounters = true;
  }
  
  public TaskAttemptID getTaskID() { return taskid; }
  public abstract boolean getIsMap();
  public float getProgress() { return progress; }
  public void setProgress(float progress) { this.progress = progress; } 
  public State getRunState() { return runState; }
  public String getTaskTracker() {return taskTracker;}
  public void setTaskTracker(String tracker) { this.taskTracker = tracker;}
  public void setRunState(State runState) { this.runState = runState; }
  public String getDiagnosticInfo() { return diagnosticInfo; }
  public void setDiagnosticInfo(String info) { 
    diagnosticInfo = 
      ((diagnosticInfo == null) ? info : diagnosticInfo.concat(info)); 
  }
  public String getStateString() { return stateString; }
  public void setStateString(String stateString) { this.stateString = stateString; }
  /**
   * Get task finish time. if shuffleFinishTime and sortFinishTime 
   * are not set before, these are set to finishTime. It takes care of 
   * the case when shuffle, sort and finish are completed with in the 
   * heartbeat interval and are not reported separately. if task state is 
   * TaskStatus.FAILED then finish time represents when the task failed.
   * @return finish time of the task. 
   */
  public long getFinishTime() {
    return finishTime;
  }

  /**
   * Sets finishTime. 
   * @param finishTime finish time of task.
   */
  void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }
  /**
   * Get shuffle finish time for the task. If shuffle finish time was 
   * not set due to shuffle/sort/finish phases ending within same
   * heartbeat interval, it is set to finish time of next phase i.e. sort 
   * or task finish when these are set.  
   * @return 0 if shuffleFinishTime, sortFinishTime and finish time are not set. else 
   * it returns approximate shuffle finish time.  
   */
  public long getShuffleFinishTime() {
    return 0;
  }

  /**
   * Set shuffle finish time. 
   * @param shuffleFinishTime 
   */
  void setShuffleFinishTime(long shuffleFinishTime) {}

  /**
   * Get sort finish time for the task,. If sort finish time was not set 
   * due to sort and reduce phase finishing in same heartebat interval, it is 
   * set to finish time, when finish time is set. 
   * @return 0 if sort finish time and finish time are not set, else returns sort
   * finish time if that is set, else it returns finish time. 
   */
  public long getSortFinishTime() {
    return 0;
  }

  /**
   * Sets sortFinishTime, if shuffleFinishTime is not set before 
   * then its set to sortFinishTime.  
   * @param sortFinishTime
   */
  void setSortFinishTime(long sortFinishTime) {}

  /**
   * Get start time of the task. 
   * @return 0 is start time is not set, else returns start time. 
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set startTime of the task.
   * @param startTime start time
   */
  void setStartTime(long startTime) {
    this.startTime = startTime;
  }
  /**
   * Get current phase of this task. Phase.Map in case of map tasks, 
   * for reduce one of Phase.SHUFFLE, Phase.SORT or Phase.REDUCE. 
   * @return . 
   */
  public Phase getPhase(){
    return this.phase; 
  }
  /**
   * Set current phase of this task.  
   * @param phase phase of this task
   */
  void setPhase(Phase phase){
    TaskStatus.Phase oldPhase = getPhase();
    if (oldPhase != phase){
      // sort phase started
      if (phase == TaskStatus.Phase.SORT){
        setShuffleFinishTime(System.currentTimeMillis());
      }else if (phase == TaskStatus.Phase.REDUCE){
        setSortFinishTime(System.currentTimeMillis());
      }
    }
    this.phase = phase; 
  }
  
  public boolean getIncludeCounters() {
    return includeCounters; 
  }
  
  public void setIncludeCounters(boolean send) {
    includeCounters = send;
  }
  
  /**
   * Get task's counters.
   */
  public Counters getCounters() {
    return counters;
  }
  /**
   * Set the task's counters.
   * @param counters
   */
  public void setCounters(Counters counters) {
    this.counters = counters;
  }
  
  /**
   * Get the list of maps from which output-fetches failed.
   * 
   * @return the list of maps from which output-fetches failed.
   */
  public List<TaskAttemptID> getFetchFailedMaps() {
    return null;
  }
  
  /**
   * Add to the list of maps from which output-fetches failed.
   *  
   * @param mapTaskId map from which fetch failed
   */
  synchronized void addFetchFailedMap(TaskAttemptID mapTaskId) {}

  /**
   * Update the status of the task.
   * 
   * @param progress
   * @param state
   * @param phase
   * @param counters
   */
  synchronized void statusUpdate(float progress, String state, 
                                 Counters counters) {
    setRunState(TaskStatus.State.RUNNING);
    setProgress(progress);
    setStateString(state);
    setCounters(counters);
  }
  
  /**
   * Update the status of the task.
   * 
   * @param status updated status
   */
  synchronized void statusUpdate(TaskStatus status) {
    this.progress = status.getProgress();
    this.runState = status.getRunState();
    this.stateString = status.getStateString();

    setDiagnosticInfo(status.getDiagnosticInfo());
    
    if (status.getStartTime() != 0) {
      this.startTime = status.getStartTime(); 
    }
    if (status.getFinishTime() != 0) {
      this.finishTime = status.getFinishTime(); 
    }
    
    this.phase = status.getPhase();
    this.counters = status.getCounters();
  }
  
  /**
   * Clear out transient information after sending out a status-update
   * from either the {@link Task} to the {@link TaskTracker} or from the
   * {@link TaskTracker} to the {@link JobTracker}. 
   */
  synchronized void clearStatus() {
    // Clear diagnosticInfo
    diagnosticInfo = "";
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen since we do implement Clonable
      throw new InternalError(cnse.toString());
    }
  }
  
  //////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    taskid.write(out);
    out.writeFloat(progress);
    WritableUtils.writeEnum(out, runState);
    Text.writeString(out, diagnosticInfo);
    Text.writeString(out, stateString);
    WritableUtils.writeEnum(out, phase);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    out.writeBoolean(includeCounters);
    if (includeCounters) {
      counters.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.taskid = TaskAttemptID.read(in);
    this.progress = in.readFloat();
    this.runState = WritableUtils.readEnum(in, State.class);
    this.diagnosticInfo = Text.readString(in);
    this.stateString = Text.readString(in);
    this.phase = WritableUtils.readEnum(in, Phase.class); 
    this.startTime = in.readLong(); 
    this.finishTime = in.readLong(); 
    counters = new Counters();
    this.includeCounters = in.readBoolean();
    if (includeCounters) {
      counters.readFields(in);
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Factory-like methods to create/read/write appropriate TaskStatus objects
  //////////////////////////////////////////////////////////////////////////////
  
  static TaskStatus createTaskStatus(DataInput in, TaskAttemptID taskId, float progress,
                                     State runState, String diagnosticInfo,
                                     String stateString, String taskTracker,
                                     Phase phase, Counters counters) 
  throws IOException {
    boolean isMap = in.readBoolean();
    return createTaskStatus(isMap, taskId, progress, runState, diagnosticInfo, 
                          stateString, taskTracker, phase, counters);
  }
  
  static TaskStatus createTaskStatus(boolean isMap, TaskAttemptID taskId, float progress,
                                   State runState, String diagnosticInfo,
                                   String stateString, String taskTracker,
                                   Phase phase, Counters counters) { 
    return (isMap) ? new MapTaskStatus(taskId, progress, runState, 
                                       diagnosticInfo, stateString, taskTracker, 
                                       phase, counters) :
                     new ReduceTaskStatus(taskId, progress, runState, 
                                          diagnosticInfo, stateString, 
                                          taskTracker, phase, counters);
  }
  
  static TaskStatus createTaskStatus(boolean isMap) {
    return (isMap) ? new MapTaskStatus() : new ReduceTaskStatus();
  }

  static TaskStatus readTaskStatus(DataInput in) throws IOException {
    boolean isMap = in.readBoolean();
    TaskStatus taskStatus = createTaskStatus(isMap);
    taskStatus.readFields(in);
    return taskStatus;
  }
  
  static void writeTaskStatus(DataOutput out, TaskStatus taskStatus) 
  throws IOException {
    out.writeBoolean(taskStatus.getIsMap());
    taskStatus.write(out);
  }
}

