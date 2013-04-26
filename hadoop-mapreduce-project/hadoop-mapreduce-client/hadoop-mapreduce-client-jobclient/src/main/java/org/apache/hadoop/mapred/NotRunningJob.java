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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class NotRunningJob implements MRClientProtocol {

  private RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

  private final JobState jobState;
  private final ApplicationReport applicationReport;


  private ApplicationReport getUnknownApplicationReport() {
    ApplicationId unknownAppId = recordFactory
        .newRecordInstance(ApplicationId.class);
    ApplicationAttemptId unknownAttemptId = recordFactory
        .newRecordInstance(ApplicationAttemptId.class);

    // Setting AppState to NEW and finalStatus to UNDEFINED as they are never
    // used for a non running job
    return BuilderUtils.newApplicationReport(unknownAppId, unknownAttemptId,
        "N/A", "N/A", "N/A", "N/A", 0, null, YarnApplicationState.NEW, "N/A",
        "N/A", 0, 0, FinalApplicationStatus.UNDEFINED, null, "N/A");
  }

  NotRunningJob(ApplicationReport applicationReport, JobState jobState) {
    this.applicationReport =
        (applicationReport ==  null) ?
            getUnknownApplicationReport() : applicationReport;
    this.jobState = jobState;
  }

  @Override
  public FailTaskAttemptResponse failTaskAttempt(
      FailTaskAttemptRequest request) throws YarnRemoteException {
    FailTaskAttemptResponse resp =
      recordFactory.newRecordInstance(FailTaskAttemptResponse.class);
    return resp;
  }

  @Override
  public GetCountersResponse getCounters(GetCountersRequest request)
      throws YarnRemoteException {
    GetCountersResponse resp =
      recordFactory.newRecordInstance(GetCountersResponse.class);
    Counters counters = recordFactory.newRecordInstance(Counters.class);
    counters.addAllCounterGroups(new HashMap<String, CounterGroup>());
    resp.setCounters(counters);
    return resp;
  }

  @Override
  public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
      throws YarnRemoteException {
    GetDiagnosticsResponse resp =
      recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
    resp.addDiagnostics("");
    return resp;
  }

  @Override
  public GetJobReportResponse getJobReport(GetJobReportRequest request)
      throws YarnRemoteException {
    JobReport jobReport =
      recordFactory.newRecordInstance(JobReport.class);
    jobReport.setJobId(request.getJobId());
    jobReport.setJobState(jobState);
    jobReport.setUser(applicationReport.getUser());
    jobReport.setStartTime(applicationReport.getStartTime());
    jobReport.setDiagnostics(applicationReport.getDiagnostics());
    jobReport.setJobName(applicationReport.getName());
    jobReport.setTrackingUrl(applicationReport.getTrackingUrl());
    jobReport.setFinishTime(applicationReport.getFinishTime());

    GetJobReportResponse resp =
        recordFactory.newRecordInstance(GetJobReportResponse.class);
    resp.setJobReport(jobReport);
    return resp;
  }

  @Override
  public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(
      GetTaskAttemptCompletionEventsRequest request)
      throws YarnRemoteException {
    GetTaskAttemptCompletionEventsResponse resp =
      recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
    resp.addAllCompletionEvents(new ArrayList<TaskAttemptCompletionEvent>());
    return resp;
  }

  @Override
  public GetTaskAttemptReportResponse getTaskAttemptReport(
      GetTaskAttemptReportRequest request) throws YarnRemoteException {
    //not invoked by anybody
    throw new NotImplementedException();
  }

  @Override
  public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
      throws YarnRemoteException {
    GetTaskReportResponse resp =
      recordFactory.newRecordInstance(GetTaskReportResponse.class);
    TaskReport report = recordFactory.newRecordInstance(TaskReport.class);
    report.setTaskId(request.getTaskId());
    report.setTaskState(TaskState.NEW);
    Counters counters = recordFactory.newRecordInstance(Counters.class);
    counters.addAllCounterGroups(new HashMap<String, CounterGroup>());
    report.setCounters(counters);
    report.addAllRunningAttempts(new ArrayList<TaskAttemptId>());
    return resp;
  }

  @Override
  public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
      throws YarnRemoteException {
    GetTaskReportsResponse resp =
      recordFactory.newRecordInstance(GetTaskReportsResponse.class);
    resp.addAllTaskReports(new ArrayList<TaskReport>());
    return resp;
  }

  @Override
  public KillJobResponse killJob(KillJobRequest request)
      throws YarnRemoteException {
    KillJobResponse resp =
      recordFactory.newRecordInstance(KillJobResponse.class);
    return resp;
  }

  @Override
  public KillTaskResponse killTask(KillTaskRequest request)
      throws YarnRemoteException {
    KillTaskResponse resp =
      recordFactory.newRecordInstance(KillTaskResponse.class);
    return resp;
  }

  @Override
  public KillTaskAttemptResponse killTaskAttempt(
      KillTaskAttemptRequest request) throws YarnRemoteException {
    KillTaskAttemptResponse resp =
      recordFactory.newRecordInstance(KillTaskAttemptResponse.class);
    return resp;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnRemoteException {
    /* Should not be invoked by anyone. */
    throw new NotImplementedException();
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnRemoteException {
    /* Should not be invoked by anyone. */
    throw new NotImplementedException();
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnRemoteException {
    /* Should not be invoked by anyone. */
    throw new NotImplementedException();
  }

  @Override
  public InetSocketAddress getConnectAddress() {
    /* Should not be invoked by anyone.  Normally used to set token service */
    throw new NotImplementedException();
  }
}
