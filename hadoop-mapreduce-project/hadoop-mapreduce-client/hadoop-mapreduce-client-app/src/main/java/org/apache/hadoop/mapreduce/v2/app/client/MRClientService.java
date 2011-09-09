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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.ipc.Server;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
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
import org.apache.hadoop.mapreduce.v2.app.webapp.AMWebApp;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

/**
 * This module is responsible for talking to the 
 * jobclient (user facing).
 *
 */
public class MRClientService extends AbstractService 
    implements ClientService {

  static final Log LOG = LogFactory.getLog(MRClientService.class);
  
  private MRClientProtocol protocolHandler;
  private Server server;
  private WebApp webApp;
  private InetSocketAddress bindAddress;
  private AppContext appContext;

  public MRClientService(AppContext appContext) {
    super("MRClientService");
    this.appContext = appContext;
    this.protocolHandler = new MRClientProtocolHandler();
  }

  public void start() {
    Configuration conf = new Configuration(getConfig()); // Just for not messing up sec-info class config
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress address = NetUtils.createSocketAddr("0.0.0.0:0");
    InetAddress hostNameResolved = null;
    try {
      hostNameResolved = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new YarnException(e);
    }

    ClientToAMSecretManager secretManager = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      secretManager = new ClientToAMSecretManager();
      String secretKeyStr =
          System
              .getenv(ApplicationConstants.APPLICATION_CLIENT_SECRET_ENV_NAME);
      byte[] bytes = Base64.decodeBase64(secretKeyStr);
      ApplicationTokenIdentifier identifier =
          new ApplicationTokenIdentifier(this.appContext.getApplicationID());
      secretManager.setMasterKey(identifier, bytes);
      conf.setClass(
          YarnConfiguration.YARN_SECURITY_INFO,
          SchedulerSecurityInfo.class, SecurityInfo.class); // Same for now.
    }
    server =
        rpc.getServer(MRClientProtocol.class, protocolHandler, address,
            conf, secretManager,
            conf.getInt(MRJobConfig.MR_AM_JOB_CLIENT_THREAD_COUNT, 
                MRJobConfig.DEFAULT_MR_AM_JOB_CLIENT_THREAD_COUNT));
    server.start();
    this.bindAddress =
        NetUtils.createSocketAddr(hostNameResolved.getHostAddress()
            + ":" + server.getPort());
    LOG.info("Instantiated MRClientService at " + this.bindAddress);
    try {
      webApp = WebApps.$for("yarn", AppContext.class, appContext).with(conf).
          start(new AMWebApp());
    } catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
    }
    super.start();
  }

  public void stop() {
    server.close();
    if (webApp != null) {
      webApp.stop();
    }
    super.stop();
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

    private Job verifyAndGetJob(JobId jobID, 
        boolean modifyAccess) throws YarnRemoteException {
      Job job = appContext.getJob(jobID);
      if (job == null) {
        throw RPCUtil.getRemoteException("Unknown job " + jobID);
      }
      //TODO fix job acls.
      //JobACL operation = JobACL.VIEW_JOB;
      //if (modifyAccess) {
      //  operation = JobACL.MODIFY_JOB;
      //}
      //TO disable check access ofr now.
      //checkAccess(job, operation);
      return job;
    }
 
    private Task verifyAndGetTask(TaskId taskID, 
        boolean modifyAccess) throws YarnRemoteException {
      Task task = verifyAndGetJob(taskID.getJobId(), 
          modifyAccess).getTask(taskID);
      if (task == null) {
        throw RPCUtil.getRemoteException("Unknown Task " + taskID);
      }
      return task;
    }

    private TaskAttempt verifyAndGetAttempt(TaskAttemptId attemptID, 
        boolean modifyAccess) throws YarnRemoteException {
      TaskAttempt attempt = verifyAndGetTask(attemptID.getTaskId(), 
          modifyAccess).getAttempt(attemptID);
      if (attempt == null) {
        throw RPCUtil.getRemoteException("Unknown TaskAttempt " + attemptID);
      }
      return attempt;
    }

    private void checkAccess(Job job, JobACL jobOperation) 
      throws YarnRemoteException {
      if (!UserGroupInformation.isSecurityEnabled()) {
        return;
      }
      UserGroupInformation callerUGI;
      try {
        callerUGI = UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }
      if(!job.checkAccess(callerUGI, jobOperation)) {
        throw RPCUtil.getRemoteException(new AccessControlException("User "
            + callerUGI.getShortUserName() + " cannot perform operation "
            + jobOperation.name() + " on " + job.getID()));
      }
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request) 
      throws YarnRemoteException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId, false);
      GetCountersResponse response =
        recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(job.getCounters());
      return response;
    }
    
    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request) 
      throws YarnRemoteException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId, false);
      GetJobReportResponse response = 
        recordFactory.newRecordInstance(GetJobReportResponse.class);
      response.setJobReport(job.getReport());
      return response;
    }
    
    
    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
        GetTaskAttemptReportRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      GetTaskAttemptReportResponse response =
        recordFactory.newRecordInstance(GetTaskAttemptReportResponse.class);
      response.setTaskAttemptReport(
          verifyAndGetAttempt(taskAttemptId, false).getReport());
      return response;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request) 
      throws YarnRemoteException {
      TaskId taskId = request.getTaskId();
      GetTaskReportResponse response = 
        recordFactory.newRecordInstance(GetTaskReportResponse.class);
      response.setTaskReport(verifyAndGetTask(taskId, false).getReport());
      return response;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(
        GetTaskAttemptCompletionEventsRequest request) 
        throws YarnRemoteException {
      JobId jobId = request.getJobId();
      int fromEventId = request.getFromEventId();
      int maxEvents = request.getMaxEvents();
      Job job = verifyAndGetJob(jobId, false);
      
      GetTaskAttemptCompletionEventsResponse response = 
        recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
      response.addAllCompletionEvents(Arrays.asList(
          job.getTaskAttemptCompletionEvents(fromEventId, maxEvents)));
      return response;
    }
    
    @Override
    public KillJobResponse killJob(KillJobRequest request) 
      throws YarnRemoteException {
      JobId jobId = request.getJobId();
      String message = "Kill Job received from client " + jobId;
      LOG.info(message);
  	  verifyAndGetJob(jobId, true);
      appContext.getEventHandler().handle(
          new JobDiagnosticsUpdateEvent(jobId, message));
      appContext.getEventHandler().handle(
          new JobEvent(jobId, JobEventType.JOB_KILL));
      KillJobResponse response = 
        recordFactory.newRecordInstance(KillJobResponse.class);
      return response;
    }

    @Override
    public KillTaskResponse killTask(KillTaskRequest request) 
      throws YarnRemoteException {
      TaskId taskId = request.getTaskId();
      String message = "Kill task received from client " + taskId;
      LOG.info(message);
      verifyAndGetTask(taskId, true);
      appContext.getEventHandler().handle(
          new TaskEvent(taskId, TaskEventType.T_KILL));
      KillTaskResponse response = 
        recordFactory.newRecordInstance(KillTaskResponse.class);
      return response;
    }
    
    @Override
    public KillTaskAttemptResponse killTaskAttempt(
        KillTaskAttemptRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      String message = "Kill task attempt received from client " + taskAttemptId;
      LOG.info(message);
      verifyAndGetAttempt(taskAttemptId, true);
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
        GetDiagnosticsRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      
      GetDiagnosticsResponse response = 
        recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
      response.addAllDiagnostics(
          verifyAndGetAttempt(taskAttemptId, false).getDiagnostics());
      return response;
    }

    @Override
    public FailTaskAttemptResponse failTaskAttempt(
        FailTaskAttemptRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      String message = "Fail task attempt received from client " + taskAttemptId;
      LOG.info(message);
      verifyAndGetAttempt(taskAttemptId, true);
      appContext.getEventHandler().handle(
          new TaskAttemptDiagnosticsUpdateEvent(taskAttemptId, message));
      appContext.getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptId, 
              TaskAttemptEventType.TA_FAILMSG));
      FailTaskAttemptResponse response = recordFactory.
        newRecordInstance(FailTaskAttemptResponse.class);
      return response;
    }

    @Override
    public GetTaskReportsResponse getTaskReports(
        GetTaskReportsRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      TaskType taskType = request.getTaskType();
      
      GetTaskReportsResponse response = 
        recordFactory.newRecordInstance(GetTaskReportsResponse.class);
      
      Job job = verifyAndGetJob(jobId, false);
      LOG.info("Getting task report for " + taskType + "   " + jobId);
      Collection<Task> tasks = job.getTasks(taskType).values();
      LOG.info("Getting task report size " + tasks.size());
      for (Task task : tasks) {
        response.addTaskReport(task.getReport());
	  }
      return response;
    }
  }
}
