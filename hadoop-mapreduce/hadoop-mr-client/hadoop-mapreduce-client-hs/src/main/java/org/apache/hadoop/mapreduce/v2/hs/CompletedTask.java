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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

public class CompletedTask implements Task {


  private final TaskType type;
  private Counters counters;
  private final long startTime;
  private final long finishTime;
  private TaskState state;
  private final TaskId taskId;
  private final TaskReport report;
  private final Map<TaskAttemptId, TaskAttempt> attempts =
    new LinkedHashMap<TaskAttemptId, TaskAttempt>();
  
  private static final Log LOG = LogFactory.getLog(CompletedTask.class);

  CompletedTask(TaskId taskId, TaskInfo taskInfo) {
    //TODO JobHistoryParser.handleTaskFailedAttempt should use state from the event.
    LOG.debug("HandlingTaskId: [" + taskId + "]");
    this.taskId = taskId;
    this.startTime = taskInfo.getStartTime();
    this.finishTime = taskInfo.getFinishTime();
    this.type = TypeConverter.toYarn(taskInfo.getTaskType());
    if (taskInfo.getCounters() != null)
      this.counters = TypeConverter.toYarn(taskInfo.getCounters());
    if (taskInfo.getTaskStatus() != null) {
      this.state = TaskState.valueOf(taskInfo.getTaskStatus());
    } else {
      this.state = TaskState.KILLED;
    }
    report = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskReport.class);
    for (TaskAttemptInfo attemptHistory : taskInfo.getAllTaskAttempts()
        .values()) {
      CompletedTaskAttempt attempt = new CompletedTaskAttempt(taskId, 
          attemptHistory);
      report.addAllDiagnostics(attempt.getDiagnostics()); //TODO TMI?
      attempts.put(attempt.getID(), attempt);
      if (attemptHistory.getTaskStatus() != null
          && attemptHistory.getTaskStatus().equals(
              TaskState.SUCCEEDED.toString())
          && report.getSuccessfulAttempt() == null) {
        report.setSuccessfulAttempt(TypeConverter.toYarn(attemptHistory
            .getAttemptId()));
      }
    }
    report.setTaskId(taskId);
    report.setStartTime(startTime);
    report.setFinishTime(finishTime);
    report.setTaskState(state);
    report.setProgress(getProgress());
    report.setCounters(getCounters());
    report.addAllRunningAttempts(new ArrayList<TaskAttemptId>(attempts.keySet()));
  }

  @Override
  public boolean canCommit(TaskAttemptId taskAttemptID) {
    return false;
  }

  @Override
  public TaskAttempt getAttempt(TaskAttemptId attemptID) {
    return attempts.get(attemptID);
  }

  @Override
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    return attempts;
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public TaskId getID() {
    return taskId;
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }

  @Override
  public TaskReport getReport() {
    return report;
  }

  @Override
  public TaskType getType() {
    return type;
  }

  @Override
  public boolean isFinished() {
    return true;
  }

  @Override
  public TaskState getState() {
    return state;
  }

}
