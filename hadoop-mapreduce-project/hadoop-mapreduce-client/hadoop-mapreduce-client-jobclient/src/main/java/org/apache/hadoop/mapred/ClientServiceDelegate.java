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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;

public class ClientServiceDelegate {
  private static final Log LOG = LogFactory.getLog(ClientServiceDelegate.class);

  // Caches for per-user NotRunningJobs
  private static HashMap<JobState, HashMap<String, NotRunningJob>> notRunningJobs =
      new HashMap<JobState, HashMap<String, NotRunningJob>>();

  private final Configuration conf;
  private final JobID jobId;
  private final ApplicationId appId;
  private final ResourceMgrDelegate rm;
  private final MRClientProtocol historyServerProxy;
  private boolean forceRefresh;
  private MRClientProtocol realProxy = null;
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private static String UNKNOWN_USER = "Unknown User";
  private String trackingUrl;

  public ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm,
      JobID jobId, MRClientProtocol historyServerProxy) {
    this.conf = new Configuration(conf); // Cloning for modifying.
    // For faster redirects from AM to HS.
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 3);
    this.rm = rm;
    this.jobId = jobId;
    this.historyServerProxy = historyServerProxy;
    this.appId = TypeConverter.toYarn(jobId).getAppId();
  }

  // Get the instance of the NotRunningJob corresponding to the specified
  // user and state
  private NotRunningJob getNotRunningJob(ApplicationReport applicationReport,
      JobState state) {
    synchronized (notRunningJobs) {
      HashMap<String, NotRunningJob> map = notRunningJobs.get(state);
      if (map == null) {
        map = new HashMap<String, NotRunningJob>();
        notRunningJobs.put(state, map);
      }
      String user =
          (applicationReport == null) ?
              UNKNOWN_USER : applicationReport.getUser();
      NotRunningJob notRunningJob = map.get(user);
      if (notRunningJob == null) {
        notRunningJob = new NotRunningJob(applicationReport, state);
        map.put(user, notRunningJob);
      }
      return notRunningJob;
    }
  }

  private MRClientProtocol getProxy() throws YarnRemoteException {
    if (!forceRefresh && realProxy != null) {
      return realProxy;
    }
    
    // Possibly allow nulls through the PB tunnel, otherwise deal with an exception
    // and redirect to the history server.
    ApplicationReport application = rm.getApplicationReport(appId);
    if (application != null) {
      trackingUrl = application.getTrackingUrl();
    }
    String serviceAddr = null;
    while (application == null || YarnApplicationState.RUNNING == application.getYarnApplicationState()) {
      if (application == null) {
        LOG.info("Could not get Job info from RM for job " + jobId
            + ". Redirecting to job history server.");
        return checkAndGetHSProxy(null, JobState.NEW);
      }
      try {
        if (application.getHost() == null || "".equals(application.getHost())) {
          LOG.debug("AM not assigned to Job. Waiting to get the AM ...");
          Thread.sleep(2000);

          LOG.debug("Application state is " + application.getYarnApplicationState());
          application = rm.getApplicationReport(appId);
          continue;
        }
        serviceAddr = application.getHost() + ":" + application.getRpcPort();
        if (UserGroupInformation.isSecurityEnabled()) {
          String clientTokenEncoded = application.getClientToken();
          Token<ApplicationTokenIdentifier> clientToken =
            new Token<ApplicationTokenIdentifier>();
          clientToken.decodeFromUrlString(clientTokenEncoded);
          // RPC layer client expects ip:port as service for tokens
          InetSocketAddress addr = NetUtils.createSocketAddr(application
              .getHost(), application.getRpcPort());
          clientToken.setService(new Text(addr.getAddress().getHostAddress()
              + ":" + addr.getPort()));
          UserGroupInformation.getCurrentUser().addToken(clientToken);
        }
        LOG.info("Tracking Url of JOB is " + application.getTrackingUrl());
        LOG.info("Connecting to " + serviceAddr);
        instantiateAMProxy(serviceAddr);
        return realProxy;
      } catch (IOException e) {
        //possibly the AM has crashed
        //there may be some time before AM is restarted
        //keep retrying by getting the address from RM
        LOG.info("Could not connect to " + serviceAddr +
        ". Waiting for getting the latest AM address...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e1) {
          LOG.warn("getProxy() call interruped", e1);
          throw new YarnException(e1);
        }
        application = rm.getApplicationReport(appId);
        if (application == null) {
          LOG.info("Could not get Job info from RM for job " + jobId
              + ". Redirecting to job history server.");
          return checkAndGetHSProxy(null, JobState.RUNNING);
        }
      } catch (InterruptedException e) {
        LOG.warn("getProxy() call interruped", e);
        throw new YarnException(e);
      }
    }

    /** we just want to return if its allocating, so that we don't
     * block on it. This is to be able to return job status
     * on an allocating Application.
     */
    String user = application.getUser();
    if (user == null) {
      throw RPCUtil.getRemoteException("User is not set in the application report");
    }
    if (application.getYarnApplicationState() == YarnApplicationState.NEW ||
        application.getYarnApplicationState() == YarnApplicationState.SUBMITTED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.NEW);
    }

    if (application.getYarnApplicationState() == YarnApplicationState.FAILED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.FAILED);
    }

    if (application.getYarnApplicationState() == YarnApplicationState.KILLED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.KILLED);
    }

    //History server can serve a job only if application
    //succeeded.
    if (application.getYarnApplicationState() == YarnApplicationState.FINISHED) {
      LOG.info("Application state is completed. FinalApplicationStatus="
          + application.getFinalApplicationStatus().toString()
          + ". Redirecting to job history server");
      realProxy = checkAndGetHSProxy(application, JobState.SUCCEEDED);
    }
    return realProxy;
  }

  private MRClientProtocol checkAndGetHSProxy(
      ApplicationReport applicationReport, JobState state) {
    if (null == historyServerProxy) {
      LOG.warn("Job History Server is not configured.");
      return getNotRunningJob(applicationReport, state);
    }
    return historyServerProxy;
  }

  private void instantiateAMProxy(final String serviceAddr) throws IOException {
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    LOG.trace("Connecting to ApplicationMaster at: " + serviceAddr);
    realProxy = currentUser.doAs(new PrivilegedAction<MRClientProtocol>() {
      @Override
      public MRClientProtocol run() {
        YarnRPC rpc = YarnRPC.create(conf);
        return (MRClientProtocol) rpc.getProxy(MRClientProtocol.class,
            NetUtils.createSocketAddr(serviceAddr), conf);
      }
    });
    LOG.trace("Connected to ApplicationMaster at: " + serviceAddr);
  }

  private synchronized Object invoke(String method, Class argClass,
      Object args) throws YarnRemoteException {
    Method methodOb = null;
    try {
      methodOb = MRClientProtocol.class.getMethod(method, argClass);
    } catch (SecurityException e) {
      throw new YarnException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnException("Method name mismatch", e);
    }
    while (true) {
      try {
        return methodOb.invoke(getProxy(), args);
      } catch (YarnRemoteException yre) {
        LOG.warn("Exception thrown by remote end.", yre);
        throw yre;
      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof YarnRemoteException) {
          LOG.warn("Error from remote end: " + e
              .getTargetException().getLocalizedMessage());
          LOG.debug("Tracing remote error ", e.getTargetException());
          throw (YarnRemoteException) e.getTargetException();
        }
        LOG.info("Failed to contact AM/History for job " + jobId + 
            " retrying..");
        LOG.debug("Failed exception on AM/History contact", 
            e.getTargetException());
        forceRefresh = true;
      } catch (Exception e) {
        LOG.info("Failed to contact AM/History for job " + jobId
            + "  Will retry..");
        LOG.debug("Failing to contact application master", e);
        forceRefresh = true;
      }
    }
  }

  public org.apache.hadoop.mapreduce.Counters getJobCounters(JobID arg0) throws IOException,
  InterruptedException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobID = TypeConverter.toYarn(arg0);
      GetCountersRequest request = recordFactory.newRecordInstance(GetCountersRequest.class);
      request.setJobId(jobID);
      Counters cnt = ((GetCountersResponse)
          invoke("getCounters", GetCountersRequest.class, request)).getCounters();
      return TypeConverter.fromYarn(cnt);

  }

  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1, int arg2)
      throws IOException, InterruptedException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobID = TypeConverter
        .toYarn(arg0);
    GetTaskAttemptCompletionEventsRequest request = recordFactory
        .newRecordInstance(GetTaskAttemptCompletionEventsRequest.class);
    request.setJobId(jobID);
    request.setFromEventId(arg1);
    request.setMaxEvents(arg2);
    List<org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent> list =
      ((GetTaskAttemptCompletionEventsResponse) invoke(
        "getTaskAttemptCompletionEvents", GetTaskAttemptCompletionEventsRequest.class, request)).
        getCompletionEventList();
    return TypeConverter
        .fromYarn(list
            .toArray(new org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent[0]));
  }

  public String[] getTaskDiagnostics(org.apache.hadoop.mapreduce.TaskAttemptID arg0)
      throws IOException, InterruptedException {

    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID = TypeConverter
        .toYarn(arg0);
    GetDiagnosticsRequest request = recordFactory
        .newRecordInstance(GetDiagnosticsRequest.class);
    request.setTaskAttemptId(attemptID);
    List<String> list = ((GetDiagnosticsResponse) invoke("getDiagnostics",
        GetDiagnosticsRequest.class, request)).getDiagnosticsList();
    String[] result = new String[list.size()];
    int i = 0;
    for (String c : list) {
      result[i++] = c.toString();
    }
    return result;
  }
  
  public JobStatus getJobStatus(JobID oldJobID) throws YarnRemoteException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId =
      TypeConverter.toYarn(oldJobID);
    GetJobReportRequest request =
        recordFactory.newRecordInstance(GetJobReportRequest.class);
    request.setJobId(jobId);
    JobReport report = ((GetJobReportResponse) invoke("getJobReport",
        GetJobReportRequest.class, request)).getJobReport();
    if (StringUtils.isEmpty(report.getJobFile())) {
      String jobFile = MRApps.getJobFile(conf, report.getUser(), oldJobID);
      report.setJobFile(jobFile);
    }
    String historyTrackingUrl = report.getTrackingUrl();
    return TypeConverter.fromYarn(report, "http://"
        + (StringUtils.isNotEmpty(historyTrackingUrl) ? historyTrackingUrl
            : trackingUrl));
  }

  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(JobID oldJobID, TaskType taskType)
       throws YarnRemoteException, YarnRemoteException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId =
      TypeConverter.toYarn(oldJobID);
    GetTaskReportsRequest request =
        recordFactory.newRecordInstance(GetTaskReportsRequest.class);
    request.setJobId(jobId);
    request.setTaskType(TypeConverter.toYarn(taskType));

    List<org.apache.hadoop.mapreduce.v2.api.records.TaskReport> taskReports =
      ((GetTaskReportsResponse) invoke("getTaskReports", GetTaskReportsRequest.class,
          request)).getTaskReportList();

    return TypeConverter.fromYarn
    (taskReports).toArray(new org.apache.hadoop.mapreduce.TaskReport[0]);
  }

  public boolean killTask(TaskAttemptID taskAttemptID, boolean fail)
       throws YarnRemoteException {
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID
      = TypeConverter.toYarn(taskAttemptID);
    if (fail) {
      FailTaskAttemptRequest failRequest = recordFactory.newRecordInstance(FailTaskAttemptRequest.class);
      failRequest.setTaskAttemptId(attemptID);
      invoke("failTaskAttempt", FailTaskAttemptRequest.class, failRequest);
    } else {
      KillTaskAttemptRequest killRequest = recordFactory.newRecordInstance(KillTaskAttemptRequest.class);
      killRequest.setTaskAttemptId(attemptID);
      invoke("killTaskAttempt", KillTaskAttemptRequest.class, killRequest);
    }
    return true;
  }

  public boolean killJob(JobID oldJobID)
       throws YarnRemoteException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId
    = TypeConverter.toYarn(oldJobID);
    KillJobRequest killRequest = recordFactory.newRecordInstance(KillJobRequest.class);
    killRequest.setJobId(jobId);
    invoke("killJob", KillJobRequest.class, killRequest);
    return true;
  }


}
