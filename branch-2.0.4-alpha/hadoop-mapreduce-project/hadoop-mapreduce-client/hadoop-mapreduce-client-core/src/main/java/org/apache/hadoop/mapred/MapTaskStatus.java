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


class MapTaskStatus extends TaskStatus {

  private long mapFinishTime = 0;
  
  public MapTaskStatus() {}

  public MapTaskStatus(TaskAttemptID taskid, float progress, int numSlots,
          State runState, String diagnosticInfo, String stateString,
          String taskTracker, Phase phase, Counters counters) {
    super(taskid, progress, numSlots, runState, diagnosticInfo, stateString,
          taskTracker, phase, counters);
  }

  @Override
  public boolean getIsMap() {
    return true;
  }

  /**
   * Sets finishTime. 
   * @param finishTime finish time of task.
   */
  @Override
  void setFinishTime(long finishTime) {
    super.setFinishTime(finishTime);
    // set mapFinishTime if it hasn't been set before
    if (getMapFinishTime() == 0) {
      setMapFinishTime(finishTime);
    }
  }
  
  @Override
  public long getShuffleFinishTime() {
    throw new UnsupportedOperationException("getShuffleFinishTime() not supported for MapTask");
  }

  @Override
  void setShuffleFinishTime(long shuffleFinishTime) {
    throw new UnsupportedOperationException("setShuffleFinishTime() not supported for MapTask");
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
  synchronized void statusUpdate(TaskStatus status) {
    super.statusUpdate(status);
    
    if (status.getMapFinishTime() != 0) {
      this.mapFinishTime = status.getMapFinishTime();
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    mapFinishTime = in.readLong();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(mapFinishTime);
  }

  @Override
  public void addFetchFailedMap(TaskAttemptID mapTaskId) {
    throw new UnsupportedOperationException
                ("addFetchFailedMap() not supported for MapTask");
  }

}
