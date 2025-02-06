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

package org.apache.hadoop.mapreduce.v2.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskRequest;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.junit.jupiter.api.Test;

public class TestMRClientService {

  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  @Test
  public void test() throws Exception {
    MRAppWithClientService app = new MRAppWithClientService(1, 0, false);
    Configuration conf = new Configuration();
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    assertEquals(1, job.getTasks().size(), "Num tasks not correct");
    Iterator<Task> it = job.getTasks().values().iterator();
    Task task = it.next();
    app.waitForState(task, TaskState.RUNNING);
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    app.waitForState(attempt, TaskAttemptState.RUNNING);

    // send the diagnostic
    String diagnostic1 = "Diagnostic1";
    String diagnostic2 = "Diagnostic2";
    app.getContext().getEventHandler().handle(
        new TaskAttemptDiagnosticsUpdateEvent(attempt.getID(), diagnostic1));

    // send the status update
    TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatus();
    taskAttemptStatus.id = attempt.getID();
    taskAttemptStatus.progress = 0.5f;
    taskAttemptStatus.stateString = "RUNNING";
    taskAttemptStatus.taskState = TaskAttemptState.RUNNING;
    taskAttemptStatus.phase = Phase.MAP;
    // send the status update
    app.getContext().getEventHandler().handle(
        new TaskAttemptStatusUpdateEvent(attempt.getID(),
            new AtomicReference<>(taskAttemptStatus)));

    
    //verify that all object are fully populated by invoking RPCs.
    YarnRPC rpc = YarnRPC.create(conf);
    MRClientProtocol proxy =
      (MRClientProtocol) rpc.getProxy(MRClientProtocol.class,
          app.clientService.getBindAddress(), conf);
    GetCountersRequest gcRequest =
        recordFactory.newRecordInstance(GetCountersRequest.class);    
    gcRequest.setJobId(job.getID());
    assertNotNull(proxy.getCounters(gcRequest).getCounters(), "Counters is null");

    GetJobReportRequest gjrRequest =
        recordFactory.newRecordInstance(GetJobReportRequest.class);
    gjrRequest.setJobId(job.getID());
    JobReport jr = proxy.getJobReport(gjrRequest).getJobReport();
    verifyJobReport(jr);
    

    GetTaskAttemptCompletionEventsRequest gtaceRequest =
        recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsRequest.class);
    gtaceRequest.setJobId(job.getID());
    gtaceRequest.setFromEventId(0);
    gtaceRequest.setMaxEvents(10);
    assertNotNull(proxy.getTaskAttemptCompletionEvents(gtaceRequest).getCompletionEventList(),
        "TaskCompletionEvents is null");

    GetDiagnosticsRequest gdRequest =
        recordFactory.newRecordInstance(GetDiagnosticsRequest.class);
    gdRequest.setTaskAttemptId(attempt.getID());
    assertNotNull(proxy.getDiagnostics(gdRequest).getDiagnosticsList(),
        "Diagnostics is null");

    GetTaskAttemptReportRequest gtarRequest =
        recordFactory.newRecordInstance(GetTaskAttemptReportRequest.class);
    gtarRequest.setTaskAttemptId(attempt.getID());
    TaskAttemptReport tar =
        proxy.getTaskAttemptReport(gtarRequest).getTaskAttemptReport();
    verifyTaskAttemptReport(tar);
    

    GetTaskReportRequest gtrRequest =
        recordFactory.newRecordInstance(GetTaskReportRequest.class);
    gtrRequest.setTaskId(task.getID());
    assertNotNull(proxy.getTaskReport(gtrRequest).getTaskReport(),
        "TaskReport is null");

    GetTaskReportsRequest gtreportsRequest =
        recordFactory.newRecordInstance(GetTaskReportsRequest.class);
    gtreportsRequest.setJobId(job.getID());
    gtreportsRequest.setTaskType(TaskType.MAP);
    assertNotNull(proxy.getTaskReports(gtreportsRequest).getTaskReportList(),
        "TaskReports for map is null");

    gtreportsRequest =
        recordFactory.newRecordInstance(GetTaskReportsRequest.class);
    gtreportsRequest.setJobId(job.getID());
    gtreportsRequest.setTaskType(TaskType.REDUCE);
    assertNotNull(proxy.getTaskReports(gtreportsRequest).getTaskReportList(),
        "TaskReports for reduce is null");

    List<String> diag = proxy.getDiagnostics(gdRequest).getDiagnosticsList();
    assertEquals(1, diag.size(), "Num diagnostics not correct");
    assertEquals(diagnostic1, diag.get(0), "Diag 1 not correct");

    TaskReport taskReport = proxy.getTaskReport(gtrRequest).getTaskReport();
    assertEquals(1, taskReport.getDiagnosticsCount(), "Num diagnostics not correct");

    //send the done signal to the task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));

    app.waitForState(job, JobState.SUCCEEDED);

    // For invalid jobid, throw IOException
    gtreportsRequest =
        recordFactory.newRecordInstance(GetTaskReportsRequest.class);
    gtreportsRequest.setJobId(TypeConverter.toYarn(JobID
        .forName("job_1415730144495_0001")));
    gtreportsRequest.setTaskType(TaskType.REDUCE);
    try {
      proxy.getTaskReports(gtreportsRequest);
      fail("IOException not thrown for invalid job id");
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testViewAclOnlyCannotModify() throws Exception {
    final MRAppWithClientService app = new MRAppWithClientService(1, 0, false);
    final Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf.set(MRJobConfig.JOB_ACL_VIEW_JOB, "viewonlyuser");
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    assertEquals(1, job.getTasks().size(), "Num tasks not correct");
    Iterator<Task> it = job.getTasks().values().iterator();
    Task task = it.next();
    app.waitForState(task, TaskState.RUNNING);
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    app.waitForState(attempt, TaskAttemptState.RUNNING);

    UserGroupInformation viewOnlyUser =
        UserGroupInformation.createUserForTesting(
            "viewonlyuser", new String[] {});
    assertTrue(job.checkAccess(viewOnlyUser, JobACL.VIEW_JOB), "viewonlyuser cannot view job");
    assertFalse(job.checkAccess(viewOnlyUser, JobACL.MODIFY_JOB), "viewonlyuser can modify job");
    MRClientProtocol client = viewOnlyUser.doAs(
        (PrivilegedExceptionAction<MRClientProtocol>) () -> {
          YarnRPC rpc = YarnRPC.create(conf);
          return (MRClientProtocol) rpc.getProxy(MRClientProtocol.class,
              app.clientService.getBindAddress(), conf);
        });

    KillJobRequest killJobRequest = recordFactory.newRecordInstance(
        KillJobRequest.class);
    killJobRequest.setJobId(app.getJobId());
    try {
      client.killJob(killJobRequest);
      fail("viewonlyuser killed job");
    } catch (AccessControlException e) {
      // pass
    }

    KillTaskRequest killTaskRequest = recordFactory.newRecordInstance(
        KillTaskRequest.class);
    killTaskRequest.setTaskId(task.getID());
    try {
      client.killTask(killTaskRequest);
      fail("viewonlyuser killed task");
    } catch (AccessControlException e) {
      // pass
    }

    KillTaskAttemptRequest killTaskAttemptRequest =
        recordFactory.newRecordInstance(KillTaskAttemptRequest.class);
    killTaskAttemptRequest.setTaskAttemptId(attempt.getID());
    try {
      client.killTaskAttempt(killTaskAttemptRequest);
      fail("viewonlyuser killed task attempt");
    } catch (AccessControlException e) {
      // pass
    }

    FailTaskAttemptRequest failTaskAttemptRequest =
        recordFactory.newRecordInstance(FailTaskAttemptRequest.class);
    failTaskAttemptRequest.setTaskAttemptId(attempt.getID());
    try {
      client.failTaskAttempt(failTaskAttemptRequest);
      fail("viewonlyuser killed task attempt");
    } catch (AccessControlException e) {
      // pass
    }
  }

  private void verifyJobReport(JobReport jr) {
    assertNotNull(jr, "JobReport is null");
    List<AMInfo> amInfos = jr.getAMInfos();
    assertEquals(1, amInfos.size());
    assertEquals(JobState.RUNNING, jr.getJobState());
    AMInfo amInfo = amInfos.get(0);
    assertEquals(MRApp.NM_HOST, amInfo.getNodeManagerHost());
    assertEquals(MRApp.NM_PORT, amInfo.getNodeManagerPort());
    assertEquals(MRApp.NM_HTTP_PORT, amInfo.getNodeManagerHttpPort());
    assertEquals(1, amInfo.getAppAttemptId().getAttemptId());
    assertEquals(1, amInfo.getContainerId().getApplicationAttemptId().getAttemptId());
    assertTrue(amInfo.getStartTime() > 0);
    assertFalse(jr.isUber());
  }
  
  private void verifyTaskAttemptReport(TaskAttemptReport tar) {
    assertEquals(TaskAttemptState.RUNNING, tar.getTaskAttemptState());
    assertNotNull(tar, "TaskAttemptReport is null");
    assertEquals(MRApp.NM_HOST, tar.getNodeManagerHost());
    assertEquals(MRApp.NM_PORT, tar.getNodeManagerPort());
    assertEquals(MRApp.NM_HTTP_PORT, tar.getNodeManagerHttpPort());
    assertEquals(1, tar.getContainerId().getApplicationAttemptId().getAttemptId());
  }
  
  class MRAppWithClientService extends MRApp {
    MRClientService clientService = null;
    MRAppWithClientService(int maps, int reduces, boolean autoComplete) {
      super(maps, reduces, autoComplete, "MRAppWithClientService", true);
    }
    @Override
    protected ClientService createClientService(AppContext context) {
      clientService = new MRClientService(context);
      return clientService;
    }
  }
  
  public static void main(String[] args) throws Exception {
    TestMRClientService t = new TestMRClientService();
    t.test();
  }
}
