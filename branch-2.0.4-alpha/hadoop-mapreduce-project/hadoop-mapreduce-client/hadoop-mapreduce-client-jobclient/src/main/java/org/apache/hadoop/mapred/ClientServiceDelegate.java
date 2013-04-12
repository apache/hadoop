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
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.LogParams;
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
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ClientToken;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientTokenIdentifier;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class ClientServiceDelegate {
  private static final Log LOG = LogFactory.getLog(ClientServiceDelegate.class);
  private static final String UNAVAILABLE = "N/A";

  // Caches for per-user NotRunningJobs
  private HashMap<JobState, HashMap<String, NotRunningJob>> notRunningJobs;

  private final Configuration conf;
  private final JobID jobId;
  private final ApplicationId appId;
  private final ResourceMgrDelegate rm;
  private final MRClientProtocol historyServerProxy;
  private MRClientProtocol realProxy = null;
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private static String UNKNOWN_USER = "Unknown User";
  private String trackingUrl;

  private boolean amAclDisabledStatusLogged = false;

  public ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm,
      JobID jobId, MRClientProtocol historyServerProxy) {
    this.conf = new Configuration(conf); // Cloning for modifying.
    // For faster redirects from AM to HS.
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        this.conf.getInt(MRJobConfig.MR_CLIENT_TO_AM_IPC_MAX_RETRIES,
            MRJobConfig.DEFAULT_MR_CLIENT_TO_AM_IPC_MAX_RETRIES));
    this.rm = rm;
    this.jobId = jobId;
    this.historyServerProxy = historyServerProxy;
    this.appId = TypeConverter.toYarn(jobId).getAppId();
    notRunningJobs = new HashMap<JobState, HashMap<String, NotRunningJob>>();
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
    if (realProxy != null) {
      return realProxy;
    }
    
    // Possibly allow nulls through the PB tunnel, otherwise deal with an exception
    // and redirect to the history server.
    ApplicationReport application = rm.getApplicationReport(appId);
    if (application != null) {
      trackingUrl = application.getTrackingUrl();
    }
    InetSocketAddress serviceAddr = null;
    while (application == null
        || YarnApplicationState.RUNNING == application
            .getYarnApplicationState()) {
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
        } else if (UNAVAILABLE.equals(application.getHost())) {
          if (!amAclDisabledStatusLogged) {
            LOG.info("Job " + jobId + " is running, but the host is unknown."
                + " Verify user has VIEW_JOB access.");
            amAclDisabledStatusLogged = true;
          }
          return getNotRunningJob(application, JobState.RUNNING);
        }
        if(!conf.getBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED, false)) {
          UserGroupInformation newUgi = UserGroupInformation.createRemoteUser(
              UserGroupInformation.getCurrentUser().getUserName());
          serviceAddr = NetUtils.createSocketAddrForHost(
              application.getHost(), application.getRpcPort());
          if (UserGroupInformation.isSecurityEnabled()) {
            ClientToken clientToken = application.getClientToken();
            Token<ClientTokenIdentifier> token =
                ProtoUtils.convertFromProtoFormat(clientToken, serviceAddr);
            newUgi.addToken(token);
          }
          LOG.debug("Connecting to " + serviceAddr);
          final InetSocketAddress finalServiceAddr = serviceAddr;
          realProxy = newUgi.doAs(new PrivilegedExceptionAction<MRClientProtocol>() {
            @Override
            public MRClientProtocol run() throws IOException {
              return instantiateAMProxy(finalServiceAddr);
            }
          });
        } else {
          if (!amAclDisabledStatusLogged) {
            LOG.info("Network ACL closed to AM for job " + jobId
                + ". Not going to try to reach the AM.");
            amAclDisabledStatusLogged = true;
          }
          return getNotRunningJob(null, JobState.RUNNING);
        }
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
    if (application.getYarnApplicationState() == YarnApplicationState.NEW
        || application.getYarnApplicationState() == YarnApplicationState.SUBMITTED
        || application.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
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

  MRClientProtocol instantiateAMProxy(final InetSocketAddress serviceAddr)
      throws IOException {
    LOG.trace("Connecting to ApplicationMaster at: " + serviceAddr);
    YarnRPC rpc = YarnRPC.create(conf);
    MRClientProtocol proxy = 
         (MRClientProtocol) rpc.getProxy(MRClientProtocol.class,
            serviceAddr, conf);
    LOG.trace("Connected to ApplicationMaster at: " + serviceAddr);
    return proxy;
  }

  private synchronized Object invoke(String method, Class argClass,
      Object args) throws IOException {
    Method methodOb = null;
    try {
      methodOb = MRClientProtocol.class.getMethod(method, argClass);
    } catch (SecurityException e) {
      throw new YarnException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnException("Method name mismatch", e);
    }
    int maxRetries = this.conf.getInt(
        MRJobConfig.MR_CLIENT_MAX_RETRIES,
        MRJobConfig.DEFAULT_MR_CLIENT_MAX_RETRIES);
    IOException lastException = null;
    while (maxRetries > 0) {
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
        LOG.debug("Failed to contact AM/History for job " + jobId + 
            " retrying..", e.getTargetException());
        // Force reconnection by setting the proxy to null.
        realProxy = null;
        // HS/AMS shut down
        maxRetries--;
        lastException = new IOException(e.getMessage());
        
      } catch (Exception e) {
        LOG.debug("Failed to contact AM/History for job " + jobId
            + "  Will retry..", e);
        // Force reconnection by setting the proxy to null.
        realProxy = null;
        // RM shutdown
        maxRetries--;
        lastException = new IOException(e.getMessage());     
      }
    }
    throw lastException;
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
  
  public JobStatus getJobStatus(JobID oldJobID) throws IOException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId =
      TypeConverter.toYarn(oldJobID);
    GetJobReportRequest request =
        recordFactory.newRecordInstance(GetJobReportRequest.class);
    request.setJobId(jobId);
    JobReport report = ((GetJobReportResponse) invoke("getJobReport",
        GetJobReportRequest.class, request)).getJobReport();
    JobStatus jobStatus = null;
    if (report != null) {
      if (StringUtils.isEmpty(report.getJobFile())) {
        String jobFile = MRApps.getJobFile(conf, report.getUser(), oldJobID);
        report.setJobFile(jobFile);
      }
      String historyTrackingUrl = report.getTrackingUrl();
      String url = StringUtils.isNotEmpty(historyTrackingUrl)
          ? historyTrackingUrl : trackingUrl;
      if (!UNAVAILABLE.equals(url)) {
        url = HttpConfig.getSchemePrefix() + url;
      }
      jobStatus = TypeConverter.fromYarn(report, url);
    }
    return jobStatus;
  }

  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(JobID oldJobID, TaskType taskType)
       throws IOException{
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
       throws IOException {
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
       throws IOException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId
    = TypeConverter.toYarn(oldJobID);
    KillJobRequest killRequest = recordFactory.newRecordInstance(KillJobRequest.class);
    killRequest.setJobId(jobId);
    invoke("killJob", KillJobRequest.class, killRequest);
    return true;
  }

  public LogParams getLogFilePath(JobID oldJobID, TaskAttemptID oldTaskAttemptID)
      throws YarnRemoteException, IOException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId =
        TypeConverter.toYarn(oldJobID);
    GetJobReportRequest request =
        recordFactory.newRecordInstance(GetJobReportRequest.class);
    request.setJobId(jobId);

    JobReport report =
        ((GetJobReportResponse) invoke("getJobReport",
            GetJobReportRequest.class, request)).getJobReport();
    if (EnumSet.of(JobState.SUCCEEDED, JobState.FAILED, JobState.KILLED,
        JobState.ERROR).contains(report.getJobState())) {
      if (oldTaskAttemptID != null) {
        GetTaskAttemptReportRequest taRequest =
            recordFactory.newRecordInstance(GetTaskAttemptReportRequest.class);
        taRequest.setTaskAttemptId(TypeConverter.toYarn(oldTaskAttemptID));
        TaskAttemptReport taReport =
            ((GetTaskAttemptReportResponse) invoke("getTaskAttemptReport",
                GetTaskAttemptReportRequest.class, taRequest))
                .getTaskAttemptReport();
        if (taReport.getContainerId() == null
            || taReport.getNodeManagerHost() == null) {
          throw new IOException("Unable to get log information for task: "
              + oldTaskAttemptID);
        }
        return new LogParams(
            taReport.getContainerId().toString(),
            taReport.getContainerId().getApplicationAttemptId()
                .getApplicationId().toString(),
            BuilderUtils.newNodeId(taReport.getNodeManagerHost(),
                taReport.getNodeManagerPort()).toString(), report.getUser());
      } else {
        if (report.getAMInfos() == null || report.getAMInfos().size() == 0) {
          throw new IOException("Unable to get log information for job: "
              + oldJobID);
        }
        AMInfo amInfo = report.getAMInfos().get(report.getAMInfos().size() - 1);
        return new LogParams(
            amInfo.getContainerId().toString(),
            amInfo.getAppAttemptId().getApplicationId().toString(),
            BuilderUtils.newNodeId(amInfo.getNodeManagerHost(),
                amInfo.getNodeManagerPort()).toString(), report.getUser());
      }
    } else {
      throw new IOException("Cannot get log path for a in-progress job");
    }
  }
}
