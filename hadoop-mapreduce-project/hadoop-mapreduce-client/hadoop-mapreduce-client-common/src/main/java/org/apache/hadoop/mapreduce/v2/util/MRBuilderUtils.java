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

package org.apache.hadoop.mapreduce.v2.util;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.Records;

public class MRBuilderUtils {

  public static JobId newJobId(ApplicationId appId, int id) {
    JobId jobId = Records.newRecord(JobId.class);
    jobId.setAppId(appId);
    jobId.setId(id);
    return jobId;
  }

  public static JobId newJobId(long clusterTs, int appIdInt, int id) {
    ApplicationId appId = ApplicationId.newInstance(clusterTs, appIdInt);
    return MRBuilderUtils.newJobId(appId, id);
  }

  public static TaskId newTaskId(JobId jobId, int id, TaskType taskType) {
    TaskId taskId = Records.newRecord(TaskId.class);
    taskId.setJobId(jobId);
    taskId.setId(id);
    taskId.setTaskType(taskType);
    return taskId;
  }

  public static TaskAttemptId newTaskAttemptId(TaskId taskId, int attemptId) {
    TaskAttemptId taskAttemptId =
        Records.newRecord(TaskAttemptId.class);
    taskAttemptId.setTaskId(taskId);
    taskAttemptId.setId(attemptId);
    return taskAttemptId;
  }

  public static JobReport newJobReport(JobId jobId, String jobName,
      String userName, JobState state, long submitTime, long startTime,
      long finishTime, float setupProgress, float mapProgress,
      float reduceProgress, float cleanupProgress, String jobFile,
      List<AMInfo> amInfos, boolean isUber, String diagnostics) {
    return newJobReport(jobId, jobName, userName, state, submitTime, startTime,
        finishTime, setupProgress, mapProgress, reduceProgress,
        cleanupProgress, jobFile, amInfos, isUber, diagnostics,
        Priority.newInstance(0));
  }

  public static JobReport newJobReport(JobId jobId, String jobName,
      String userName, JobState state, long submitTime, long startTime, long finishTime,
      float setupProgress, float mapProgress, float reduceProgress,
      float cleanupProgress, String jobFile, List<AMInfo> amInfos,
      boolean isUber, String diagnostics, Priority priority) {
    JobReport report = Records.newRecord(JobReport.class);
    report.setJobId(jobId);
    report.setJobName(jobName);
    report.setUser(userName);
    report.setJobState(state);
    report.setSubmitTime(submitTime);
    report.setStartTime(startTime);
    report.setFinishTime(finishTime);
    report.setSetupProgress(setupProgress);
    report.setCleanupProgress(cleanupProgress);
    report.setMapProgress(mapProgress);
    report.setReduceProgress(reduceProgress);
    report.setJobFile(jobFile);
    report.setAMInfos(amInfos);
    report.setIsUber(isUber);
    report.setDiagnostics(diagnostics);
    report.setJobPriority(priority);
    return report;
  }

  public static AMInfo newAMInfo(ApplicationAttemptId appAttemptId,
      long startTime, ContainerId containerId, String nmHost, int nmPort,
      int nmHttpPort) {
    AMInfo amInfo = Records.newRecord(AMInfo.class);
    amInfo.setAppAttemptId(appAttemptId);
    amInfo.setStartTime(startTime);
    amInfo.setContainerId(containerId);
    amInfo.setNodeManagerHost(nmHost);
    amInfo.setNodeManagerPort(nmPort);
    amInfo.setNodeManagerHttpPort(nmHttpPort);
    return amInfo;
  }
}