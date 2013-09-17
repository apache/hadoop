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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.yarn.util.Records;

public class CompletedTask implements Task {

  private static final Counters EMPTY_COUNTERS = new Counters();

  private final TaskId taskId;
  private final TaskInfo taskInfo;
  private TaskReport report;
  private TaskAttemptId successfulAttempt;
  private List<String> reportDiagnostics = new LinkedList<String>();
  private Lock taskAttemptsLock = new ReentrantLock();
  private AtomicBoolean taskAttemptsLoaded = new AtomicBoolean(false);
  private final Map<TaskAttemptId, TaskAttempt> attempts =
    new LinkedHashMap<TaskAttemptId, TaskAttempt>();

  CompletedTask(TaskId taskId, TaskInfo taskInfo) {
    //TODO JobHistoryParser.handleTaskFailedAttempt should use state from the event.
    this.taskInfo = taskInfo;
    this.taskId = taskId;
  }

  @Override
  public boolean canCommit(TaskAttemptId taskAttemptID) {
    return false;
  }

  @Override
  public TaskAttempt getAttempt(TaskAttemptId attemptID) {
    loadAllTaskAttempts();
    return attempts.get(attemptID);
  }

  @Override
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    loadAllTaskAttempts();
    return attempts;
  }

  @Override
  public Counters getCounters() {
    return taskInfo.getCounters();
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
  public synchronized TaskReport getReport() {
    if (report == null) {
      constructTaskReport();
    }
    return report;
  }
  

  
  @Override
  public TaskType getType() {
    return TypeConverter.toYarn(taskInfo.getTaskType());
  }

  @Override
  public boolean isFinished() {
    return true;
  }

  @Override
  public TaskState getState() {
    return taskInfo.getTaskStatus() == null ? TaskState.KILLED : TaskState
        .valueOf(taskInfo.getTaskStatus());
  }

  private void constructTaskReport() {
    loadAllTaskAttempts();
    this.report = Records.newRecord(TaskReport.class);
    report.setTaskId(taskId);
    long minLaunchTime = Long.MAX_VALUE;
    for(TaskAttempt attempt: attempts.values()) {
      minLaunchTime = Math.min(minLaunchTime, attempt.getLaunchTime());
    }
    minLaunchTime = minLaunchTime == Long.MAX_VALUE ? -1 : minLaunchTime;
    report.setStartTime(minLaunchTime);
    report.setFinishTime(taskInfo.getFinishTime());
    report.setTaskState(getState());
    report.setProgress(getProgress());
    Counters counters = getCounters();
    if (counters == null) {
      counters = EMPTY_COUNTERS;
    }
    report.setCounters(TypeConverter.toYarn(counters));
    if (successfulAttempt != null) {
      report.setSuccessfulAttempt(successfulAttempt);
    }
    report.addAllDiagnostics(reportDiagnostics);
    report
        .addAllRunningAttempts(new ArrayList<TaskAttemptId>(attempts.keySet()));
  }

  private void loadAllTaskAttempts() {
    if (taskAttemptsLoaded.get()) {
      return;
    }
    taskAttemptsLock.lock();
    try {
      if (taskAttemptsLoaded.get()) {
        return;
      }

      for (TaskAttemptInfo attemptHistory : taskInfo.getAllTaskAttempts()
          .values()) {
        CompletedTaskAttempt attempt =
            new CompletedTaskAttempt(taskId, attemptHistory);
        reportDiagnostics.addAll(attempt.getDiagnostics());
        attempts.put(attempt.getID(), attempt);
        if (successfulAttempt == null
            && attemptHistory.getTaskStatus() != null
            && attemptHistory.getTaskStatus().equals(
                TaskState.SUCCEEDED.toString())) {
          successfulAttempt =
              TypeConverter.toYarn(attemptHistory.getAttemptId());
        }
      }
      taskAttemptsLoaded.set(true);
    } finally {
      taskAttemptsLock.unlock();
    }
  }
}
