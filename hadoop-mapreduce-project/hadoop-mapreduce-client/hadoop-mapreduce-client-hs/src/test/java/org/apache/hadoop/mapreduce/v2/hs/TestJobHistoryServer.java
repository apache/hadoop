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


import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
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
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.hs.TestJobHistoryEvents.MRAppWithHistory;
import org.apache.hadoop.mapreduce.v2.hs.TestJobHistoryParsing.MyResolver;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.RackResolver;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

/*
test JobHistoryServer protocols....
 */
public class TestJobHistoryServer {
  private static RecordFactory recordFactory = RecordFactoryProvider
          .getRecordFactory(null);

  JobHistoryServer historyServer=null;

  // simple test init/start/stop   JobHistoryServer. Status should change.
  @Test (timeout= 50000 )
  public void testStartStopServer() throws Exception {
    historyServer = new JobHistoryServer();
    Configuration config = new Configuration();
    historyServer.init(config);
    assertEquals(STATE.INITED, historyServer.getServiceState());
    assertEquals(3, historyServer.getServices().size());
    historyServer.start();
    assertEquals(STATE.STARTED, historyServer.getServiceState());
    historyServer.stop();
    assertEquals(STATE.STOPPED, historyServer.getServiceState());
    assertNotNull(historyServer.getClientService());
    HistoryClientService historyService = historyServer.getClientService();
    assertNotNull(historyService.getClientHandler().getConnectAddress());
  }

  /*
     Simple test PartialJob
  */
  @Test
  public void testPartialJob() throws Exception {
    JobId jobId = new JobIdPBImpl();
    jobId.setId(0);
    JobIndexInfo jii = new JobIndexInfo(0L, System.currentTimeMillis(), "user",
            "jobName", jobId, 3, 2, "JobStatus");
    PartialJob test = new PartialJob(jii, jobId);
    assertEquals(1.0f, test.getProgress(), 0.001);
    assertNull(test.getAllCounters());
    assertNull(test.getTasks());
    assertNull(test.getTasks(TaskType.MAP));
    assertNull(test.getTask(new TaskIdPBImpl()));

    assertNull(test.getTaskAttemptCompletionEvents(0, 100));
    assertNull(test.getMapAttemptCompletionEvents(0, 100));
    assertTrue(test.checkAccess(UserGroupInformation.getCurrentUser(), null));
    assertNull(test.getAMInfos());

  }

  //Test reports of  JobHistoryServer. History server should get log files from  MRApp and read them
  @Test (timeout= 50000 )
  public void testReports() throws Exception {
    Configuration config = new Configuration();
    config
            .setClass(
                    CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
                    MyResolver.class, DNSToSwitchMapping.class);

    RackResolver.init(config);
    MRApp app = new MRAppWithHistory(1, 1, true, this.getClass().getName(),
            true);
    app.submit(config);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    app.waitForState(job, JobState.SUCCEEDED);

    historyServer = new JobHistoryServer();

    historyServer.init(config);
    historyServer.start();
    
    // search JobHistory  service
    JobHistory jobHistory= null;
    for (Service service : historyServer.getServices() ) {
      if (service instanceof JobHistory) {
        jobHistory = (JobHistory) service;
      }
    };
    
    Map<JobId, Job> jobs= jobHistory.getAllJobs();
    
    assertEquals(1, jobs.size());
    assertEquals("job_0_0000",jobs.keySet().iterator().next().toString());
    
    Task task = job.getTasks().values().iterator().next();
    TaskAttempt attempt = task.getAttempts().values().iterator().next();

    HistoryClientService historyService = historyServer.getClientService();
    MRClientProtocol protocol = historyService.getClientHandler();

    GetTaskAttemptReportRequest gtarRequest = recordFactory
            .newRecordInstance(GetTaskAttemptReportRequest.class);
    // test getTaskAttemptReport
    TaskAttemptId taId = attempt.getID();
    taId.setTaskId(task.getID());
    taId.getTaskId().setJobId(job.getID());
    gtarRequest.setTaskAttemptId(taId);
    GetTaskAttemptReportResponse response = protocol
            .getTaskAttemptReport(gtarRequest);
    assertEquals("container_0_0000_01_000000", response.getTaskAttemptReport()
            .getContainerId().toString());
    assertTrue(response.getTaskAttemptReport().getDiagnosticInfo().isEmpty());
    // counters
    assertNotNull(response.getTaskAttemptReport().getCounters()
            .getCounter(TaskCounter.PHYSICAL_MEMORY_BYTES));
    assertEquals(taId.toString(), response.getTaskAttemptReport()
            .getTaskAttemptId().toString());
    // test getTaskReport
    GetTaskReportRequest request = recordFactory
            .newRecordInstance(GetTaskReportRequest.class);
    TaskId taskId = task.getID();
    taskId.setJobId(job.getID());
    request.setTaskId(taskId);
    GetTaskReportResponse reportResponse = protocol.getTaskReport(request);
    assertEquals("", reportResponse.getTaskReport().getDiagnosticsList()
            .iterator().next());
    // progress
    assertEquals(1.0f, reportResponse.getTaskReport().getProgress(), 0.01);
    // report has corrected taskId
    assertEquals(taskId.toString(), reportResponse.getTaskReport().getTaskId()
            .toString());
    // Task state should be SUCCEEDED
    assertEquals(TaskState.SUCCEEDED, reportResponse.getTaskReport()
            .getTaskState());
    // test getTaskAttemptCompletionEvents
    GetTaskAttemptCompletionEventsRequest taskAttemptRequest = recordFactory
            .newRecordInstance(GetTaskAttemptCompletionEventsRequest.class);
    taskAttemptRequest.setJobId(job.getID());
    GetTaskAttemptCompletionEventsResponse taskAttemptCompletionEventsResponse = protocol
            .getTaskAttemptCompletionEvents(taskAttemptRequest);
    assertEquals(0, taskAttemptCompletionEventsResponse.getCompletionEventCount());
    
    // test getDiagnostics
    GetDiagnosticsRequest diagnosticRequest = recordFactory
            .newRecordInstance(GetDiagnosticsRequest.class);
    diagnosticRequest.setTaskAttemptId(taId);
    GetDiagnosticsResponse diagnosticResponse = protocol
            .getDiagnostics(diagnosticRequest);
    // it is strange : why one empty string ?
    assertEquals(1, diagnosticResponse.getDiagnosticsCount());
    assertEquals("", diagnosticResponse.getDiagnostics(0));

    GetCountersRequest counterRequest = recordFactory
            .newRecordInstance(GetCountersRequest.class);
    counterRequest.setJobId(job.getID());
    GetCountersResponse counterResponse = protocol.getCounters(counterRequest);
    assertNotNull(counterResponse.getCounters().getCounterGroup("org.apache.hadoop.mapreduce.JobCounter"));
    // test getJobReport
    GetJobReportRequest reportRequest = recordFactory
            .newRecordInstance(GetJobReportRequest.class);
    reportRequest.setJobId(job.getID());
    GetJobReportResponse jobReport = protocol.getJobReport(reportRequest);
    assertEquals(1, jobReport.getJobReport().getAMInfos().size());
    assertNotNull(jobReport.getJobReport().getJobFile());
    assertEquals(job.getID().toString(), jobReport.getJobReport().getJobId().toString());
    assertNotNull(jobReport.getJobReport().getTrackingUrl());


    //getTaskReports
    GetTaskReportsRequest taskReportRequest = recordFactory
            .newRecordInstance(GetTaskReportsRequest.class);
    taskReportRequest.setJobId(job.getID());
    taskReportRequest.setTaskType(TaskType.MAP);
    GetTaskReportsResponse taskReportsResponse = protocol.getTaskReports(taskReportRequest);
    assertEquals(1, taskReportsResponse.getTaskReportList().size());
    assertEquals(1, taskReportsResponse.getTaskReportCount());
    assertEquals(task.getID(), taskReportsResponse.getTaskReport(0).getTaskId());
    assertEquals(TaskState.SUCCEEDED, taskReportsResponse.getTaskReport(0).getTaskState());

    //getDelegationToken
    GetDelegationTokenRequest delegationTokenRequest = recordFactory
            .newRecordInstance(GetDelegationTokenRequest.class);
    String s = UserGroupInformation.getCurrentUser().getShortUserName();
    delegationTokenRequest.setRenewer(s);
    GetDelegationTokenResponse delegationTokenResponse = protocol.getDelegationToken(delegationTokenRequest);
    assertEquals("MR_DELEGATION_TOKEN", delegationTokenResponse.getDelegationToken().getKind());
    assertNotNull(delegationTokenResponse.getDelegationToken().getIdentifier());

    //renewDelegationToken

    RenewDelegationTokenRequest renewDelegationRequest = recordFactory
            .newRecordInstance(RenewDelegationTokenRequest.class);
    renewDelegationRequest.setDelegationToken(delegationTokenResponse.getDelegationToken());
    RenewDelegationTokenResponse renewDelegationTokenResponse = protocol.renewDelegationToken(renewDelegationRequest);
    // should be about 1 day
    assertTrue(renewDelegationTokenResponse.getNextExpirationTime() > 0);


// cancelDelegationToken   

    CancelDelegationTokenRequest cancelDelegationTokenRequest = recordFactory
            .newRecordInstance(CancelDelegationTokenRequest.class);
    cancelDelegationTokenRequest.setDelegationToken(delegationTokenResponse.getDelegationToken());
    assertNotNull(protocol.cancelDelegationToken(cancelDelegationTokenRequest));

    historyServer.stop();
  }

  // test launch method
  @Test (timeout =60000)
  public void testLaunch() throws Exception {

    ExitUtil.disableSystemExit();
    try {
      historyServer = JobHistoryServer.launchJobHistoryServer(new String[0]);
    } catch (ExitUtil.ExitException e) {
      assertEquals(0,e.status);
      ExitUtil.resetFirstExitException();
      fail();
    }
  }
  
  @After
  public void stop(){
    if(historyServer !=null && !STATE.STOPPED.equals(historyServer.getServiceState())){
      historyServer.stop();
    }
  }
}
