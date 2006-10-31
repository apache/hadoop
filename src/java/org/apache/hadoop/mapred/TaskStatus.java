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

import org.apache.hadoop.io.*;

import java.io.*;
/**************************************************
 * Describes the current status of a task.  This is
 * not intended to be a comprehensive piece of data.
 *
 * @author Mike Cafarella
 **************************************************/
class TaskStatus implements Writable {
    //enumeration for reporting current phase of a task. 
    public static enum Phase{STARTING, MAP, SHUFFLE, SORT, REDUCE}

    // what state is the task in?
    public static enum State {RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED}
    
    private String taskid;
    private boolean isMap;
    private float progress;
    private State runState;
    private String diagnosticInfo;
    private String stateString;
    private String taskTracker;
    
    private long startTime ; 
    private long finishTime ; 
    
    // only for reduce tasks
    private long shuffleFinishTime ; 
    private long sortFinishTime ; 
    
    private Phase phase = Phase.STARTING; 

    public TaskStatus() {}

    public TaskStatus(String taskid, boolean isMap, float progress,
                      State runState, String diagnosticInfo,
                      String stateString, String taskTracker,
                      Phase phase) {
        this.taskid = taskid;
        this.isMap = isMap;
        this.progress = progress;
        this.runState = runState;
        this.diagnosticInfo = diagnosticInfo;
        this.stateString = stateString;
        this.taskTracker = taskTracker;
        this.phase = phase ;
    }
    
    public String getTaskId() { return taskid; }
    public boolean getIsMap() { return isMap; }
    public float getProgress() { return progress; }
    public void setProgress(float progress) { this.progress = progress; } 
    public State getRunState() { return runState; }
    public String getTaskTracker() {return taskTracker;}
    public void setTaskTracker(String tracker) { this.taskTracker = tracker;}
    public void setRunState(State runState) { this.runState = runState; }
    public String getDiagnosticInfo() { return diagnosticInfo; }
    public void setDiagnosticInfo(String info) { this.diagnosticInfo = info; }
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
      if( shuffleFinishTime == 0 ) {
        this.shuffleFinishTime = finishTime ; 
      }
      if( sortFinishTime == 0 ){
        this.sortFinishTime = finishTime ;
      }
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
      return shuffleFinishTime;
    }

    /**
     * Set shuffle finish time. 
     * @param shuffleFinishTime 
     */
    void setShuffleFinishTime(long shuffleFinishTime) {
      this.shuffleFinishTime = shuffleFinishTime;
    }

    /**
     * Get sort finish time for the task,. If sort finish time was not set 
     * due to sort and reduce phase finishing in same heartebat interval, it is 
     * set to finish time, when finish time is set. 
     * @return 0 if sort finish time and finish time are not set, else returns sort
     * finish time if that is set, else it returns finish time. 
     */
    public long getSortFinishTime() {
      return sortFinishTime;
    }

    /**
     * Sets sortFinishTime, if shuffleFinishTime is not set before 
     * then its set to sortFinishTime.  
     * @param sortFinishTime
     */
    void setSortFinishTime(long sortFinishTime) {
      this.sortFinishTime = sortFinishTime;
      if( 0 == this.shuffleFinishTime){
        this.shuffleFinishTime = sortFinishTime ;
      }
    }

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
     * @param p
     */
    void setPhase(Phase p){
      this.phase = p ; 
    }
    //////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, taskid);
        out.writeBoolean(isMap);
        out.writeFloat(progress);
        WritableUtils.writeEnum(out, runState);
        UTF8.writeString(out, diagnosticInfo);
        UTF8.writeString(out, stateString);
        WritableUtils.writeEnum(out, phase);
        out.writeLong(startTime);
        out.writeLong(finishTime);
        if(! isMap){
          out.writeLong(shuffleFinishTime);
          out.writeLong(sortFinishTime);
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.taskid = UTF8.readString(in);
        this.isMap = in.readBoolean();
        this.progress = in.readFloat();
        this.runState = WritableUtils.readEnum(in, State.class);
        this.diagnosticInfo = UTF8.readString(in);
        this.stateString = UTF8.readString(in);
        this.phase = WritableUtils.readEnum(in, Phase.class); 
        this.startTime = in.readLong(); 
        this.finishTime = in.readLong() ; 
        if( ! this.isMap ){
          shuffleFinishTime = in.readLong(); 
          sortFinishTime = in.readLong(); 
        }
     }
}

