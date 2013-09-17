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

package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.yarn.util.Times;

@XmlRootElement(name = "taskAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReduceTaskAttemptInfo extends TaskAttemptInfo {

  protected long shuffleFinishTime;
  protected long mergeFinishTime;
  protected long elapsedShuffleTime;
  protected long elapsedMergeTime;
  protected long elapsedReduceTime;

  public ReduceTaskAttemptInfo() {
  }

  public ReduceTaskAttemptInfo(TaskAttempt ta, TaskType type) {
    super(ta, type, false);

    this.shuffleFinishTime = ta.getShuffleFinishTime();
    this.mergeFinishTime = ta.getSortFinishTime();
    this.elapsedShuffleTime = Times.elapsed(this.startTime,
        this.shuffleFinishTime, false);
    if (this.elapsedShuffleTime == -1) {
      this.elapsedShuffleTime = 0;
    }
    this.elapsedMergeTime = Times.elapsed(this.shuffleFinishTime,
        this.mergeFinishTime, false);
    if (this.elapsedMergeTime == -1) {
      this.elapsedMergeTime = 0;
    }
    this.elapsedReduceTime = Times.elapsed(this.mergeFinishTime,
        this.finishTime, false);
    if (this.elapsedReduceTime == -1) {
      this.elapsedReduceTime = 0;
    }
  }

  public long getShuffleFinishTime() {
    return this.shuffleFinishTime;
  }

  public long getMergeFinishTime() {
    return this.mergeFinishTime;
  }

  public long getElapsedShuffleTime() {
    return this.elapsedShuffleTime;
  }

  public long getElapsedMergeTime() {
    return this.elapsedMergeTime;
  }

  public long getElapsedReduceTime() {
    return this.elapsedReduceTime;
  }
}
