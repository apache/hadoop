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

package org.apache.hadoop.mapreduce.v2.app.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
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
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.MRAMPolicyProvider;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMWebApp;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This module is responsible for talking to the 
 * jobclient (user facing).
 *
 */
public class MRClientService extends AbstractService implements ClientService {

  static final Logger LOG = LoggerFactory.getLogger(MRClientService.class);
  
  private MRClientProtocol protocolHandler;
  private Server server;
  private WebApp webApp;
  private InetSocketAddress bindAddress;
  private AppContext appContext;

  public MRClientService(AppContext appContext) {
    super(MRClientService.class.getName());
    this.appContext = appContext;
    this.protocolHandler = new MRClientProtocolHandler();
  }

  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress address = new InetSocketAddress(0);

    server =
        rpc.getServer(MRClientProtocol.class, protocolHandler, address,
            conf, appContext.getClientToAMTokenSecretManager(),
            conf.getInt(MRJobConfig.MR_AM_JOB_CLIENT_THREAD_COUNT, 
                MRJobConfig.DEFAULT_MR_AM_JOB_CLIENT_THREAD_COUNT),
                MRJobConfig.MR_AM_JOB_CLIENT_PORT_RANGE);
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new MRAMPolicyProvider());
    }

    server.start();
    this.bindAddress = NetUtils.createSocketAddrForHost(appContext.getNMHostname(),
        server.getListenerAddress().getPort());
    LOG.info("Instantiated MRClientService at " + this.bindAddress);
    try {
      HttpConfig.Policy httpPolicy = conf.getBoolean(
          MRJobConfig.MR_AM_WEBAPP_HTTPS_ENABLED,
          MRJobConfig.DEFAULT_MR_AM_WEBAPP_HTTPS_ENABLED)
          ? Policy.HTTPS_ONLY : Policy.HTTP_ONLY;
      boolean needsClientAuth = conf.getBoolean(
          MRJobConfig.MR_AM_WEBAPP_HTTPS_CLIENT_AUTH,
          MRJobConfig.DEFAULT_MR_AM_WEBAPP_HTTPS_CLIENT_AUTH);
      webApp =
          WebApps.$for("mapreduce", AppContext.class, appContext, "ws")
            .withHttpPolicy(conf, httpPolicy)
            .withPortRange(conf, MRJobConfig.MR_AM_WEBAPP_PORT_RANGE)
            .needsClientAuth(needsClientAuth)
            .start(new AMWebApp());
    } catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
    }
    super.serviceStart();
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    if (webApp != null) {
      webApp.stop();
    }
    super.serviceStop();
  }

  @Override
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @Override
  public int getHttpPort() {
    return webApp.port();
  }

  class MRClientProtocolHandler implements MRClientProtocol {

    private RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

    @Override
    public InetSocketAddress getConnectAddress() {
      return getBindAddress();
    }
    
    private Job verifyAndGetJob(JobId jobID, JobACL accessType,
        boolean exceptionThrow) throws IOException {
      Job job = appContext.getJob(jobID);
      if (job == null && exceptionThrow) {
        throw new IOException("Unknown Job " + jobID);
      }
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      if (job != null && !job.checkAccess(ugi, accessType)) {
        throw new AccessControlException("User " + ugi.getShortUserName()
            + " cannot perform operation " + accessType.name() + " on "
            + jobID);
      }
      return job;
    }
 
    private Task verifyAndGetTask(TaskId taskID, 
        JobACL accessType) throws IOException {
      Task task =
          verifyAndGetJob(taskID.getJobId(), accessType, true).getTask(taskID);
      if (task == null) {
        throw new IOException("Unknown Task " + taskID);
      }
      return task;
    }

    private TaskAttempt verifyAndGetAttempt(TaskAttemptId attemptID, 
        JobACL accessType) throws IOException {
      TaskAttempt attempt = verifyAndGetTask(attemptID.getTaskId(), 
          accessType).getAttempt(attemptID);
      if (attempt == null) {
        throw new IOException("Unknown TaskAttempt " + attemptID);
      }
      return attempt;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request) 
      throws IOException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId, JobACL.VIEW_JOB, true);
      GetCountersResponse response =
        recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(TypeConverter.toYarn(job.getAllCounters()));
      return response;
    }
    
    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request) 
      throws IOException {
      JobId jobId = request.getJobId();
      // false is for retain compatibility
      Job job = verifyAndGetJob(jobId, JobACL.VIEW_JOB, false);
      GetJobReportResponse response = 
        recordFactory.newRecordInstance(GetJobReportResponse.class);
      if (job != null) {
        response.setJobReport(job.getReport());
      }
      else {
        response.setJobReport(null);
      }
      return response;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
        GetTaskAttemptReportRequest request) throws IOException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      GetTaskAttemptReportResponse response =
        recordFactory.newRecordInstance(GetTaskAttemptReportResponse.class);
      response.setTaskAttemptReport(
          verifyAndGetAttempt(taskAttemptId, JobACL.VIEW_JOB).getReport());
      return response;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request) 
      throws IOException {
      TaskId taskId = request.getTaskId();
      GetTaskReportResponse response = 
        recordFactory.newRecordInstance(GetTaskReportResponse.class);
      response.setTaskReport(
          verifyAndGetTask(taskId, JobACL.VIEW_JOB).getReport());
      return response;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(
        GetTaskAttemptCompletionEventsRequest request) 
        throws IOException {
      JobId jobId = request.getJobId();
      int fromEventId = request.getFromEventId();
      int maxEvents = request.getMaxEvents();
      Job job = verifyAndGetJob(jobId, JobACL.VIEW_JOB, true);
      
      GetTaskAttemptCompletionEventsResponse response = 
        recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
      response.addAllCompletionEvents(Arrays.asList(
          job.getTaskAttemptCompletionEvents(fromEventId, maxEvents)));
      return response;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public KillJobResponse killJob(KillJobRequest request) 
      throws IOException {
      JobId jobId = request.getJobId();
      UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
      String message = "Kill job " + jobId + " received from " + callerUGI
          + " at " + Server.getRemoteAddress();
      LOG.info(message);
      verifyAndGetJob(jobId, JobACL.MODIFY_JOB, false);
      appContext.getEventHandler().handle(
          new JobDiagnosticsUpdateEvent(jobId, message));
      appContext.getEventHandler().handle(
          new JobEvent(jobId, JobEventType.JOB_KILL));
      KillJobResponse response = 
        recordFactory.newRecordInstance(KillJobResponse.class);
      return response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KillTaskResponse killTask(KillTaskRequest request) 
      throws IOException {
      TaskId taskId = request.getTaskId();
      UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
      String message = "Kill task " + taskId + " received from " + callerUGI
          + " at " + Server.getRemoteAddress();
      LOG.info(message);
      verifyAndGetTask(taskId, JobACL.MODIFY_JOB);
      appContext.getEventHandler().handle(
          new TaskEvent(taskId, TaskEventType.T_KILL));
      KillTaskResponse response = 
        recordFactory.newRecordInstance(KillTaskResponse.class);
      return response;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public KillTaskAttemptResponse killTaskAttempt(
        KillTaskAttemptRequest request) throws IOException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
      String message = "Kill task attempt " + taskAttemptId
          + " received from " + callerUGI + " at "
          + Server.getRemoteAddress();
      LOG.info(message);
      verifyAndGetAttempt(taskAttemptId, JobACL.MODIFY_JOB);
      appContext.getEventHandler().handle(
          new TaskAttemptDiagnosticsUpdateEvent(taskAttemptId, message));
      appContext.getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptId, 
              TaskAttemptEventType.TA_KILL));
      KillTaskAttemptResponse response = 
        recordFactory.newRecordInstance(KillTaskAttemptResponse.class);
      return response;
    }

    @Override
    public GetDiagnosticsResponse getDiagnostics(
        GetDiagnosticsRequest request) throws IOException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      
      GetDiagnosticsResponse response = 
        recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
      response.addAllDiagnostics(verifyAndGetAttempt(taskAttemptId,
          JobACL.VIEW_JOB).getDiagnostics());
      return response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public FailTaskAttemptResponse failTaskAttempt(
        FailTaskAttemptRequest request) throws IOException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
      String message = "Fail task attempt " + taskAttemptId
          + " received from " + callerUGI + " at "
          + Server.getRemoteAddress();
      LOG.info(message);
      verifyAndGetAttempt(taskAttemptId, JobACL.MODIFY_JOB);
      appContext.getEventHandler().handle(
          new TaskAttemptDiagnosticsUpdateEvent(taskAttemptId, message));
      appContext.getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptId, 
              TaskAttemptEventType.TA_FAILMSG_BY_CLIENT));
      FailTaskAttemptResponse response = recordFactory.
        newRecordInstance(FailTaskAttemptResponse.class);
      return response;
    }

    private final Object getTaskReportsLock = new Object();

    @Override
    public GetTaskReportsResponse getTaskReports(
        GetTaskReportsRequest request) throws IOException {
      JobId jobId = request.getJobId();
      TaskType taskType = request.getTaskType();
      
      GetTaskReportsResponse response = 
        recordFactory.newRecordInstance(GetTaskReportsResponse.class);
      
      Job job = verifyAndGetJob(jobId, JobACL.VIEW_JOB, true);
      Collection<Task> tasks = job.getTasks(taskType).values();
      LOG.info("Getting task report for " + taskType + "   " + jobId
          + ". Report-size will be " + tasks.size());

      // Take lock to allow only one call, otherwise heap will blow up because
      // of counters in the report when there are multiple callers.
      synchronized (getTaskReportsLock) {
        for (Task task : tasks) {
          response.addTaskReport(task.getReport());
        }
      }

      return response;
    }

    @Override
    public GetDelegationTokenResponse getDelegationToken(
        GetDelegationTokenRequest request) throws IOException {
      throw new IOException("MR AM not authorized to issue delegation" +
      		" token");
    }

    @Override
    public RenewDelegationTokenResponse renewDelegationToken(
        RenewDelegationTokenRequest request) throws IOException {
      throw new IOException("MR AM not authorized to renew delegation" +
          " token");
    }

    @Override
    public CancelDelegationTokenResponse cancelDelegationToken(
        CancelDelegationTokenRequest request) throws IOException {
      throw new IOException("MR AM not authorized to cancel delegation" +
          " token");
    }
  }

  public KillTaskAttemptResponse forceKillTaskAttempt(
      KillTaskAttemptRequest request) throws YarnException, IOException {
    return protocolHandler.killTaskAttempt(request);
  }

  public WebApp getWebApp() {
    return webApp;
  }
}
