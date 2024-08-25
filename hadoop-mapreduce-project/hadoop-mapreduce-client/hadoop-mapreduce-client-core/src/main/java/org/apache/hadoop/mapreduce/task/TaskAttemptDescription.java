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

package org.apache.hadoop.mapreduce.task;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "TaskAttemptDescription")
@XmlAccessorType(XmlAccessType.FIELD)
public class TaskAttemptDescription {

  private String taskAttemptId;

  private String taskAttemptState;

  private long   startTime;

  private long   finishTime;

  private String phase;

  private float progress;

  private long shuffleFinishTime;

  private long sortFinishTime;

  public TaskAttemptDescription() {
  }

  public String getTaskAttemptId() {
    return taskAttemptId;
  }

  public void setTaskAttemptId(String taskAttemptId) {
    this.taskAttemptId = taskAttemptId;
  }

  public String getTaskAttemptState() {
    return taskAttemptState;
  }

  public void setTaskAttemptState(String taskAttemptState) {
    this.taskAttemptState = taskAttemptState;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public String getPhase() {
    return phase;
  }

  public void setPhase(String phase) {
    this.phase = phase;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public long getShuffleFinishTime() {
    return shuffleFinishTime;
  }

  public void setShuffleFinishTime(long shuffleFinishTime) {
    this.shuffleFinishTime = shuffleFinishTime;
  }

  public long getSortFinishTime() {
    return sortFinishTime;
  }

  public void setSortFinishTime(long sortFinishTime) {
    this.sortFinishTime = sortFinishTime;
  }
}
