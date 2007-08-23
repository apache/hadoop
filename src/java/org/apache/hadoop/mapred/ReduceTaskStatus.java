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

import org.apache.hadoop.io.Text;


public class ReduceTaskStatus extends TaskStatus {

  private long shuffleFinishTime; 
  private long sortFinishTime; 
  private List<String> failedFetchTasks = new ArrayList<String>(1);
  
  public ReduceTaskStatus() {}

  public ReduceTaskStatus(String taskid, float progress, State runState,
          String diagnosticInfo, String stateString, String taskTracker,
          Phase phase, Counters counters) {
    super(taskid, progress, runState, diagnosticInfo, stateString, taskTracker,
            phase, counters);
  }

  public Object clone() {
    ReduceTaskStatus myClone = (ReduceTaskStatus)super.clone();
    myClone.failedFetchTasks = new ArrayList<String>(failedFetchTasks);
    return myClone;
  }

  public boolean getIsMap() {
    return false;
  }

  void setFinishTime(long finishTime) {
    if (shuffleFinishTime == 0) {
      this.shuffleFinishTime = finishTime; 
    }
    if (sortFinishTime == 0){
      this.sortFinishTime = finishTime;
    }
    super.setFinishTime(finishTime);
  }

  public long getShuffleFinishTime() {
    return shuffleFinishTime;
  }

  void setShuffleFinishTime(long shuffleFinishTime) {
    this.shuffleFinishTime = shuffleFinishTime;
  }

  public long getSortFinishTime() {
    return sortFinishTime;
  }

  void setSortFinishTime(long sortFinishTime) {
    this.sortFinishTime = sortFinishTime;
    if (0 == this.shuffleFinishTime){
      this.shuffleFinishTime = sortFinishTime;
    }
  }

  public List<String> getFetchFailedMaps() {
    return failedFetchTasks;
  }
  
  void addFetchFailedMap(String mapTaskId) {
    failedFetchTasks.add(mapTaskId);
  }
  
  synchronized void statusUpdate(TaskStatus status) {
    super.statusUpdate(status);
    
    if (status.getShuffleFinishTime() != 0) {
      this.shuffleFinishTime = status.getShuffleFinishTime();
    }
    
    if (status.getSortFinishTime() != 0) {
      sortFinishTime = status.getSortFinishTime();
    }
    
    List<String> newFetchFailedMaps = status.getFetchFailedMaps();
    if (failedFetchTasks == null) {
      failedFetchTasks = newFetchFailedMaps;
    } else if (newFetchFailedMaps != null){
      failedFetchTasks.addAll(newFetchFailedMaps);
    }
  }

  synchronized void clearStatus() {
    super.clearStatus();
    failedFetchTasks.clear();
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    shuffleFinishTime = in.readLong(); 
    sortFinishTime = in.readLong();
    int noFailedFetchTasks = in.readInt();
    failedFetchTasks = new ArrayList<String>(noFailedFetchTasks);
    for (int i=0; i < noFailedFetchTasks; ++i) {
      failedFetchTasks.add(Text.readString(in));
    }
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(shuffleFinishTime);
    out.writeLong(sortFinishTime);
    out.writeInt(failedFetchTasks.size());
    for (String taskId : failedFetchTasks) {
      Text.writeString(out, taskId);
    }
  }
  
}
