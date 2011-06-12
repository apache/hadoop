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
import java.util.ArrayList;
import java.util.List;



class UberTaskStatus extends TaskStatus {

  private long mapFinishTime;
  // map version of sortFinishTime is ~ irrelevant

  private long shuffleFinishTime;
  private long sortFinishTime;
  private List<TaskAttemptID> failedFetchTasks = new ArrayList<TaskAttemptID>(1);

  public UberTaskStatus() {}

  public UberTaskStatus(TaskAttemptID taskid, float progress, int numSlots,
                        State runState, String diagnosticInfo,
                        String stateString, String taskTracker, Phase phase,
                        Counters counters) {
    super(taskid, progress, numSlots, runState, diagnosticInfo, stateString,
          taskTracker, phase, counters);
  }

  @Override
  public Object clone() {
    UberTaskStatus myClone = (UberTaskStatus)super.clone();
    myClone.failedFetchTasks = new ArrayList<TaskAttemptID>(failedFetchTasks);
    return myClone;
  }

  @Override
  public boolean getIsMap() {
    return false;
  }

  @Override
  public boolean getIsUber() {
    return true;
  }

  @Override
  void setFinishTime(long finishTime) {
    if (mapFinishTime == 0) {
      this.mapFinishTime = finishTime;
    }
    if (shuffleFinishTime == 0) {
      this.shuffleFinishTime = finishTime;
    }
    if (sortFinishTime == 0){
      this.sortFinishTime = finishTime;
    }
    super.setFinishTime(finishTime);
  }

  @Override
  public long getMapFinishTime() {
    return mapFinishTime;
  }

  @Override
  void setMapFinishTime(long mapFinishTime) {
    this.mapFinishTime = mapFinishTime;
  }

  @Override
  public long getShuffleFinishTime() {
    return shuffleFinishTime;
  }

  @Override
  void setShuffleFinishTime(long shuffleFinishTime) {
    if (mapFinishTime == 0) {
      this.mapFinishTime = shuffleFinishTime;
    }
    this.shuffleFinishTime = shuffleFinishTime;
  }

  @Override
  public long getSortFinishTime() {
    return sortFinishTime;
  }

  @Override
  void setSortFinishTime(long sortFinishTime) {
    if (mapFinishTime == 0) {
      this.mapFinishTime = sortFinishTime;
    }
    if (shuffleFinishTime == 0) {
      this.shuffleFinishTime = sortFinishTime;
    }
    this.sortFinishTime = sortFinishTime;
  }

  @Override
  public List<TaskAttemptID> getFetchFailedMaps() {
    return failedFetchTasks;
  }

  @Override
  public void addFetchFailedMap(TaskAttemptID mapTaskId) {
    failedFetchTasks.add(mapTaskId);
  }

  @Override
  synchronized void statusUpdate(TaskStatus status) {
    super.statusUpdate(status);

    if (status.getIsMap()) {  // status came from a sub-MapTask
      if (status.getMapFinishTime() != 0) {
        this.mapFinishTime = status.getMapFinishTime();
      }

    } else {                  // status came from a sub-ReduceTask
      if (status.getShuffleFinishTime() != 0) {
        this.shuffleFinishTime = status.getShuffleFinishTime();
      }

      if (status.getSortFinishTime() != 0) {
        sortFinishTime = status.getSortFinishTime();
      }

      List<TaskAttemptID> newFetchFailedMaps = status.getFetchFailedMaps();
      if (failedFetchTasks == null) {
        failedFetchTasks = newFetchFailedMaps;
      } else if (newFetchFailedMaps != null){
        failedFetchTasks.addAll(newFetchFailedMaps);
      }
    }
  }

  @Override
  synchronized void clearStatus() {
    super.clearStatus();
    failedFetchTasks.clear();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    mapFinishTime = in.readLong();
    shuffleFinishTime = in.readLong();
    sortFinishTime = in.readLong();
    int numFailedFetchTasks = in.readInt();
    failedFetchTasks = new ArrayList<TaskAttemptID>(numFailedFetchTasks);
    for (int i=0; i < numFailedFetchTasks; ++i) {
      TaskAttemptID taskId = new TaskAttemptID();
      taskId.readFields(in);
      failedFetchTasks.add(taskId);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(mapFinishTime);
    out.writeLong(shuffleFinishTime);
    out.writeLong(sortFinishTime);
    out.writeInt(failedFetchTasks.size());
    for (TaskAttemptID taskId : failedFetchTasks) {
      taskId.write(out);
    }
  }

}
