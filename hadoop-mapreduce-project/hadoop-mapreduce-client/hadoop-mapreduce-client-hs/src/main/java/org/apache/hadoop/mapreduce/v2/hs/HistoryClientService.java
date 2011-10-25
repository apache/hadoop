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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.JobACL;
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
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.MRAMPolicyProvider;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HsWebApp;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.security.client.ClientHSSecurityInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

/**
 * This module is responsible for talking to the 
 * JobClient (user facing).
 *
 */
public class HistoryClientService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(HistoryClientService.class);
  
  private MRClientProtocol protocolHandler;
  private Server server;
  private WebApp webApp;
  private InetSocketAddress bindAddress;
  private HistoryContext history;

  public HistoryClientService(HistoryContext history) {
    super("HistoryClientService");
    this.history = history;
    this.protocolHandler = new MRClientProtocolHandler();
  }

  public void start() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    initializeWebApp(conf);
    String serviceAddr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS);
    InetSocketAddress address = NetUtils.createSocketAddr(serviceAddr,
      JHAdminConfig.DEFAULT_MR_HISTORY_PORT,
      JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS);
    InetAddress hostNameResolved = null;
    try {
      hostNameResolved = InetAddress.getLocalHost(); //address.getAddress().getLocalHost();
    } catch (UnknownHostException e) {
      throw new YarnException(e);
    }

    server =
        rpc.getServer(MRClientProtocol.class, protocolHandler, address,
            conf, null,
            conf.getInt(JHAdminConfig.MR_HISTORY_CLIENT_THREAD_COUNT, 
                JHAdminConfig.DEFAULT_MR_HISTORY_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      server.refreshServiceAcl(conf, new MRAMPolicyProvider());
    }
    
    server.start();
    this.bindAddress =
        NetUtils.createSocketAddr(hostNameResolved.getHostAddress()
            + ":" + server.getPort());
    LOG.info("Instantiated MRClientService at " + this.bindAddress);
    
    super.start();
  }

  private void initializeWebApp(Configuration conf) {
    webApp = new HsWebApp(history);
    String bindAddress = conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS);
    WebApps.$for("jobhistory", this).at(bindAddress).start(webApp); 
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop();
    }
    if (webApp != null) {
      webApp.stop();
    }
    super.stop();
  }

  private class MRClientProtocolHandler implements MRClientProtocol {

    private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    
    private Job verifyAndGetJob(final JobId jobID) throws YarnRemoteException {
      UserGroupInformation loginUgi = null;
      Job job = null;
      try {
        loginUgi = UserGroupInformation.getLoginUser();
        job = loginUgi.doAs(new PrivilegedExceptionAction<Job>() {

          @Override
          public Job run() throws Exception {
            Job job = history.getJob(jobID);
            return job;
          }
        });
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      } catch (InterruptedException e) {
        throw RPCUtil.getRemoteException(e);
      }
      if (job == null) {
        throw RPCUtil.getRemoteException("Unknown job " + jobID);
      }
      JobACL operation = JobACL.VIEW_JOB;
      //TODO disable check access for now.
      checkAccess(job, operation);
      return job;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId);
      GetCountersResponse response = recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(job.getCounters());
      return response;
    }
    
    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId);
      GetJobReportResponse response = recordFactory.newRecordInstance(GetJobReportResponse.class);
      response.setJobReport(job.getReport());
      return response;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(GetTaskAttemptReportRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      Job job = verifyAndGetJob(taskAttemptId.getTaskId().getJobId());
      GetTaskAttemptReportResponse response = recordFactory.newRecordInstance(GetTaskAttemptReportResponse.class);
      response.setTaskAttemptReport(job.getTask(taskAttemptId.getTaskId()).getAttempt(taskAttemptId).getReport());
      return response;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request) throws YarnRemoteException {
      TaskId taskId = request.getTaskId();
      Job job = verifyAndGetJob(taskId.getJobId());
      GetTaskReportResponse response = recordFactory.newRecordInstance(GetTaskReportResponse.class);
      response.setTaskReport(job.getTask(taskId).getReport());
      return response;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(GetTaskAttemptCompletionEventsRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      int fromEventId = request.getFromEventId();
      int maxEvents = request.getMaxEvents();
      
      Job job = verifyAndGetJob(jobId);
      GetTaskAttemptCompletionEventsResponse response = recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
      response.addAllCompletionEvents(Arrays.asList(job.getTaskAttemptCompletionEvents(fromEventId, maxEvents)));
      return response;
    }
      
    @Override
    public KillJobResponse killJob(KillJobRequest request) throws YarnRemoteException {
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }
    
    @Override
    public KillTaskResponse killTask(KillTaskRequest request) throws YarnRemoteException {
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }
    
    @Override
    public KillTaskAttemptResponse killTaskAttempt(KillTaskAttemptRequest request) throws YarnRemoteException {
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }

    @Override
    public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
    
      Job job = verifyAndGetJob(taskAttemptId.getTaskId().getJobId());
      
      GetDiagnosticsResponse response = recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
      response.addAllDiagnostics(job.getTask(taskAttemptId.getTaskId()).getAttempt(taskAttemptId).getDiagnostics());
      return response;
    }

    @Override 
    public FailTaskAttemptResponse failTaskAttempt(FailTaskAttemptRequest request) throws YarnRemoteException {
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }

    @Override
    public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      TaskType taskType = request.getTaskType();
      
      GetTaskReportsResponse response = recordFactory.newRecordInstance(GetTaskReportsResponse.class);
      Job job = verifyAndGetJob(jobId);
      Collection<Task> tasks = job.getTasks(taskType).values();
      for (Task task : tasks) {
        response.addTaskReport(task.getReport());
      }
      return response;
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
      if (!job.checkAccess(callerUGI, jobOperation)) {
        throw RPCUtil.getRemoteException(new AccessControlException("User "
            + callerUGI.getShortUserName() + " cannot perform operation "
            + jobOperation.name() + " on " + job.getID()));
      }
    }
  }
}
