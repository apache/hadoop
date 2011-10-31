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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

public class CompletedTaskAttempt implements TaskAttempt {

  private final TaskAttemptInfo attemptInfo;
  private final TaskAttemptId attemptId;
  private Counters counters;
  private final TaskAttemptState state;
  private final TaskAttemptReport report;
  private final List<String> diagnostics = new ArrayList<String>();

  private String localDiagMessage;

  CompletedTaskAttempt(TaskId taskId, TaskAttemptInfo attemptInfo) {
    this.attemptInfo = attemptInfo;
    this.attemptId = TypeConverter.toYarn(attemptInfo.getAttemptId());
    if (attemptInfo.getCounters() != null)
      this.counters = TypeConverter.toYarn(attemptInfo.getCounters());
    if (attemptInfo.getTaskStatus() != null) {
      this.state = TaskAttemptState.valueOf(attemptInfo.getTaskStatus());
    } else {
      this.state = TaskAttemptState.KILLED;
      localDiagMessage = "Attmpt state missing from History : marked as KILLED";
      diagnostics.add(localDiagMessage);
    }
    
    if (attemptInfo.getError() != null) {
      diagnostics.add(attemptInfo.getError());
    }
    
    report = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskAttemptReport.class);
    report.setCounters(counters);
    
    report.setTaskAttemptId(attemptId);
    report.setTaskAttemptState(state);
    report.setProgress(getProgress());
    report.setStartTime(attemptInfo.getStartTime());
    
    report.setFinishTime(attemptInfo.getFinishTime());
    report.setShuffleFinishTime(attemptInfo.getShuffleFinishTime());
    report.setSortFinishTime(attemptInfo.getSortFinishTime());
    if (localDiagMessage != null) {
      report.setDiagnosticInfo(attemptInfo.getError() + ", " + localDiagMessage);
    } else {
    report.setDiagnosticInfo(attemptInfo.getError());
    }
//    report.setPhase(attemptInfo.get); //TODO
    report.setStateString(attemptInfo.getState());
    report.setCounters(getCounters());
    report.setContainerId(attemptInfo.getContainerId());
    String []hostSplits = attemptInfo.getHostname().split(":");
    if (hostSplits.length != 2) {
      report.setNodeManagerHost("UNKNOWN");
    } else {
      report.setNodeManagerHost(hostSplits[0]);
      report.setNodeManagerPort(Integer.parseInt(hostSplits[1]));
    }
    report.setNodeManagerHttpPort(attemptInfo.getHttpPort());
  }

  @Override
  public ContainerId getAssignedContainerID() {
    return attemptInfo.getContainerId();
  }

  @Override
  public String getAssignedContainerMgrAddress() {
    return attemptInfo.getHostname();
  }

  @Override
  public String getNodeHttpAddress() {
    return attemptInfo.getTrackerName() + ":" + attemptInfo.getHttpPort();
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
  
  @Override
  public long getShuffleFinishTime() {
    return report.getShuffleFinishTime();
  }

  @Override
  public long getSortFinishTime() {
    return report.getSortFinishTime();
  }

  @Override
  public int getShufflePort() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
