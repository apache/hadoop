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

package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.util.StringUtils;

public class JobSummary {
  private JobId jobId;
  private long jobSubmitTime;
  private long jobLaunchTime;
  private long firstMapTaskLaunchTime; // MapAttempteStarted |
                                       // TaskAttemptStartEvent
  private long firstReduceTaskLaunchTime; // ReduceAttemptStarted |
                                          // TaskAttemptStartEvent
  private long jobFinishTime;
  private int numFinishedMaps;
  private int numFailedMaps;
  private int numFinishedReduces;
  private int numFailedReduces;
  // private int numSlotsPerMap; | Doesn't make sense with potentially different
  // resource models
  // private int numSlotsPerReduce; | Doesn't make sense with potentially
  // different resource models
  private String user;
  private String queue;
  private String jobStatus;
  private long mapSlotSeconds; // TODO Not generated yet in MRV2
  private long reduceSlotSeconds; // TODO Not generated yet MRV2
  // private int clusterSlotCapacity;

  JobSummary() {
  }

  public JobId getJobId() {
    return jobId;
  }

  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }

  public long getJobSubmitTime() {
    return jobSubmitTime;
  }

  public void setJobSubmitTime(long jobSubmitTime) {
    this.jobSubmitTime = jobSubmitTime;
  }

  public long getJobLaunchTime() {
    return jobLaunchTime;
  }

  public void setJobLaunchTime(long jobLaunchTime) {
    this.jobLaunchTime = jobLaunchTime;
  }

  public long getFirstMapTaskLaunchTime() {
    return firstMapTaskLaunchTime;
  }

  public void setFirstMapTaskLaunchTime(long firstMapTaskLaunchTime) {
    this.firstMapTaskLaunchTime = firstMapTaskLaunchTime;
  }

  public long getFirstReduceTaskLaunchTime() {
    return firstReduceTaskLaunchTime;
  }

  public void setFirstReduceTaskLaunchTime(long firstReduceTaskLaunchTime) {
    this.firstReduceTaskLaunchTime = firstReduceTaskLaunchTime;
  }

  public long getJobFinishTime() {
    return jobFinishTime;
  }

  public void setJobFinishTime(long jobFinishTime) {
    this.jobFinishTime = jobFinishTime;
  }

  public int getNumFinishedMaps() {
    return numFinishedMaps;
  }

  public void setNumFinishedMaps(int numFinishedMaps) {
    this.numFinishedMaps = numFinishedMaps;
  }

  public int getNumFailedMaps() {
    return numFailedMaps;
  }

  public void setNumFailedMaps(int numFailedMaps) {
    this.numFailedMaps = numFailedMaps;
  }

  // public int getNumSlotsPerMap() {
  // return numSlotsPerMap;
  // }
  //
  // public void setNumSlotsPerMap(int numSlotsPerMap) {
  // this.numSlotsPerMap = numSlotsPerMap;
  // }

  public int getNumFinishedReduces() {
    return numFinishedReduces;
  }

  public void setNumFinishedReduces(int numFinishedReduces) {
    this.numFinishedReduces = numFinishedReduces;
  }

  public int getNumFailedReduces() {
    return numFailedReduces;
  }

  public void setNumFailedReduces(int numFailedReduces) {
    this.numFailedReduces = numFailedReduces;
  }

  // public int getNumSlotsPerReduce() {
  // return numSlotsPerReduce;
  // }
  //
  // public void setNumSlotsPerReduce(int numSlotsPerReduce) {
  // this.numSlotsPerReduce = numSlotsPerReduce;
  // }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public String getJobStatus() {
    return jobStatus;
  }

  public void setJobStatus(String jobStatus) {
    this.jobStatus = jobStatus;
  }

  public long getMapSlotSeconds() {
    return mapSlotSeconds;
  }

  public void setMapSlotSeconds(long mapSlotSeconds) {
    this.mapSlotSeconds = mapSlotSeconds;
  }

  public long getReduceSlotSeconds() {
    return reduceSlotSeconds;
  }

  public void setReduceSlotSeconds(long reduceSlotSeconds) {
    this.reduceSlotSeconds = reduceSlotSeconds;
  }

  // public int getClusterSlotCapacity() {
  // return clusterSlotCapacity;
  // }
  //
  // public void setClusterSlotCapacity(int clusterSlotCapacity) {
  // this.clusterSlotCapacity = clusterSlotCapacity;
  // }

  public String getJobSummaryString() {
    SummaryBuilder summary = new SummaryBuilder()
      .add("jobId", jobId)
      .add("submitTime", jobSubmitTime)
      .add("launchTime", jobLaunchTime)
      .add("firstMapTaskLaunchTime", firstMapTaskLaunchTime)
      .add("firstReduceTaskLaunchTime", firstReduceTaskLaunchTime)
      .add("finishTime", jobFinishTime)
      .add("numMaps", numFinishedMaps + numFailedMaps)
      .add("numReduces", numFinishedReduces + numFailedReduces)
      .add("user", user)
      .add("queue", queue)
      .add("status", jobStatus)
      .add("mapSlotSeconds", mapSlotSeconds)
      .add("reduceSlotSeconds", reduceSlotSeconds);
    return summary.toString();
  }

  static final char EQUALS = '=';
  static final char[] charsToEscape = { StringUtils.COMMA, EQUALS,
      StringUtils.ESCAPE_CHAR };
  
  static class SummaryBuilder {
    final StringBuilder buffer = new StringBuilder();

    // A little optimization for a very common case
    SummaryBuilder add(String key, long value) {
      return _add(key, Long.toString(value));
    }

    <T> SummaryBuilder add(String key, T value) {
      return _add(key, StringUtils.escapeString(String.valueOf(value),
          StringUtils.ESCAPE_CHAR, charsToEscape));
    }

    SummaryBuilder add(SummaryBuilder summary) {
      if (buffer.length() > 0)
        buffer.append(StringUtils.COMMA);
      buffer.append(summary.buffer);
      return this;
    }

    SummaryBuilder _add(String key, String value) {
      if (buffer.length() > 0)
        buffer.append(StringUtils.COMMA);
      buffer.append(key).append(EQUALS).append(value);
      return this;
    }

    @Override
    public String toString() {
      return buffer.toString();
    }
  }
}