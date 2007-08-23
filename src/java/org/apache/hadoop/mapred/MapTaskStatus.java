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

public class MapTaskStatus extends TaskStatus {

  public MapTaskStatus() {}

  public MapTaskStatus(String taskid, float progress,
          State runState, String diagnosticInfo, String stateString,
          String taskTracker, Phase phase, Counters counters) {
    super(taskid, progress, runState, diagnosticInfo, stateString,
          taskTracker, phase, counters);
  }

  public boolean getIsMap() {
    return true;
  }

  public long getShuffleFinishTime() {
    throw new UnsupportedOperationException("getShuffleFinishTime() not supported for MapTask");
  }

  void setShuffleFinishTime(long shuffleFinishTime) {
    throw new UnsupportedOperationException("setShuffleFinishTime() not supported for MapTask");
  }

  public long getSortFinishTime() {
    throw new UnsupportedOperationException("getSortFinishTime() not supported for MapTask");
  }

  void setSortFinishTime(long sortFinishTime) {
    throw new UnsupportedOperationException("setSortFinishTime() not supported for MapTask");
  }
}
