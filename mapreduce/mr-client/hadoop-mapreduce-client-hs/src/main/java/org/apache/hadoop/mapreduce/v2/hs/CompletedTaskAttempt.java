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

package org.apache.hadoop.mapreduce.v2.hs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

public class CompletedTaskAttempt implements TaskAttempt {

  private final TaskAttemptInfo attemptInfo;
  private final TaskAttemptId attemptId;
  private final Counters counters;
  private final TaskAttemptState state;
  private final TaskAttemptReport report;
  private final List<String> diagnostics = new ArrayList<String>();

  CompletedTaskAttempt(TaskId taskId, TaskAttemptInfo attemptInfo) {
    this.attemptInfo = attemptInfo;
    this.attemptId = TypeConverter.toYarn(attemptInfo.getAttemptId());
    this.counters = TypeConverter.toYarn(
        new org.apache.hadoop.mapred.Counters(attemptInfo.getCounters()));
    this.state = TaskAttemptState.valueOf(attemptInfo.getState());
    
    if (attemptInfo.getError() != null) {
      diagnostics.add(attemptInfo.getError());
    }
    
    report = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskAttemptReport.class);
    report.setTaskAttemptId(attemptId);
    report.setTaskAttemptState(state);
    report.setProgress(getProgress());
    report.setStartTime(attemptInfo.getStartTime());
    
    report.setFinishTime(attemptInfo.getFinishTime());
    report.setDiagnosticInfo(attemptInfo.getError());
    //result.phase = attemptInfo.get;//TODO
    report.setStateString(state.toString());
    report.setCounters(getCounters());
  }

  @Override
  public ContainerId getAssignedContainerID() {
    //TODO ContainerId needs to be part of some historyEvent to be able to render the log directory.
    ContainerId containerId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ContainerId.class);
    containerId.setId(-1);
    containerId.setAppId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class));
    containerId.getAppId().setId(-1);
    containerId.getAppId().setClusterTimestamp(-1);
    return containerId;
  }

  @Override
  public String getNodeHttpAddress() {
    return attemptInfo.getHostname() + ":" + attemptInfo.getHttpPort();
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public TaskAttemptId getID() {
    return attemptId;
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }

  @Override
  public TaskAttemptReport getReport() {
    return report;
  }

  @Override
  public TaskAttemptState getState() {
    return state;
  }

  @Override
  public boolean isFinished() {
    return true;
  }

  @Override
  public List<String> getDiagnostics() {
    return diagnostics;
  }

  @Override
  public long getLaunchTime() {
    return report.getStartTime();
  }

  @Override
  public long getFinishTime() {
    return report.getFinishTime();
  }
}
