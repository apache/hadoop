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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskInfo;

/**
 * This class is a proxy class for JobStory/ZombieJob for a customized
 * submission time. Because in the simulation, submission time is totally
 * re-produced by the simulator, original submission time in job trace should be
 * ignored.
 */
public class SimulatorJobStory implements JobStory {
  private JobStory job;
  private long submissionTime;

  public SimulatorJobStory(JobStory job, long time) {
    this.job = job;
    this.submissionTime = time;
  }

  @Override
  public long getSubmissionTime() {
    return submissionTime;
  }

  @Override
  public InputSplit[] getInputSplits() {
    return job.getInputSplits();
  }

  @SuppressWarnings("deprecation")
  @Override
  public JobConf getJobConf() {
    return job.getJobConf();
  }

  @Override
  public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(int taskNumber,
      int taskAttemptNumber, int locality) {
    return job.getMapTaskAttemptInfoAdjusted(taskNumber, taskAttemptNumber,
        locality);
  }

  @Override
  public String getName() {
    return job.getName();
  }

  @Override
  public org.apache.hadoop.mapreduce.JobID getJobID() {
    return job.getJobID();
  }

  @Override
  public int getNumberMaps() {
    return job.getNumberMaps();
  }

  @Override
  public int getNumberReduces() {
    return job.getNumberReduces();
  }

  @Override
  public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType, int taskNumber,
      int taskAttemptNumber) {
    return job.getTaskAttemptInfo(taskType, taskNumber, taskAttemptNumber);
  }

  @Override
  public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {
    return job.getTaskInfo(taskType, taskNumber);
  }

  @Override
  public String getUser() {
    return job.getUser();
  }

  @Override
  public Pre21JobHistoryConstants.Values getOutcome() {
    return job.getOutcome();
  }
}
