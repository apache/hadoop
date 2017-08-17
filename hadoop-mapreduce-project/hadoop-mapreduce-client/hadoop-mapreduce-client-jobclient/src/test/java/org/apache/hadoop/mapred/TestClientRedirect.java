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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
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
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.junit.Assert;
import org.junit.Test;

public class TestClientRedirect {

  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  private static final Log LOG = LogFactory.getLog(TestClientRedirect.class);
  private static final String RMADDRESS = "0.0.0.0:8054";
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private static final String AMHOSTADDRESS = "0.0.0.0:10020";
  private static final String HSHOSTADDRESS = "0.0.0.0:10021";
  private volatile boolean amContact = false;
  private volatile boolean hsContact = false;
  private volatile boolean amRunning = false;
  private volatile boolean amRestarting = false;

  @Test
  public void testRedirect() throws Exception {

    Configuration conf = new YarnConfiguration();
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    conf.set(YarnConfiguration.RM_ADDRESS, RMADDRESS);
    conf.set(JHAdminConfig.MR_HISTORY_ADDRESS, HSHOSTADDRESS);

    // Start the RM.
    RMService rmService = new RMService("test");
    rmService.init(conf);
    rmService.start();

    // Start the AM.
    AMService amService = new AMService();
    amService.init(conf);
    amService.start(conf);

    // Start the HS.
    HistoryService historyService = new HistoryService();
    historyService.init(conf);
    historyService.start(conf);

    LOG.info("services started");

    Cluster cluster = new Cluster(conf);
    org.apache.hadoop.mapreduce.JobID jobID =
      new org.apache.hadoop.mapred.JobID("201103121733", 1);
    org.apache.hadoop.mapreduce.Counters counters =
        cluster.getJob(jobID).getCounters();
    validateCounters(counters);
    Assert.assertTrue(amContact);

    LOG.info("Sleeping for 5 seconds before stop for" +
    " the client socket to not get EOF immediately..");
    Thread.sleep(5000);

    //bring down the AM service
    amService.stop();

    LOG.info("Sleeping for 5 seconds after stop for" +
    		" the server to exit cleanly..");
    Thread.sleep(5000);

    amRestarting = true;

    // Same client
    //results are returned from fake (not started job)
    counters = cluster.getJob(jobID).getCounters();
    Assert.assertEquals(0, counters.countCounters());
    Job job = cluster.getJob(jobID);
    org.apache.hadoop.mapreduce.TaskID taskId =
      new org.apache.hadoop.mapreduce.TaskID(jobID, TaskType.MAP, 0);
    TaskAttemptID tId = new TaskAttemptID(taskId, 0);

    //invoke all methods to check that no exception is thrown
    job.killJob();
    job.killTask(tId);
    job.failTask(tId);
    job.getTaskCompletionEvents(0, 100);
    job.getStatus();
    job.getTaskDiagnostics(tId);
    job.getTaskReports(TaskType.MAP);
    job.getTrackingURL();

    amRestarting = false;
    amService = new AMService();
    amService.init(conf);
    amService.start(conf);
    amContact = false; //reset

    counters = cluster.getJob(jobID).getCounters();
    validateCounters(counters);
    Assert.assertTrue(amContact);

    // Stop the AM. It is not even restarting. So it should be treated as
    // completed.
    amService.stop();

    // Same client
    counters = cluster.getJob(jobID).getCounters();
    validateCounters(counters);
    Assert.assertTrue(hsContact);

    rmService.stop();
    historyService.stop();
  }

  private void validateCounters(org.apache.hadoop.mapreduce.Counters counters) {
    Iterator<org.apache.hadoop.mapreduce.CounterGroup> it = counters.iterator();
    while (it.hasNext()) {
      org.apache.hadoop.mapreduce.CounterGroup group = it.next();
      LOG.info("Group " + group.getDisplayName());
      Iterator<org.apache.hadoop.mapreduce.Counter> itc = group.iterator();
      while (itc.hasNext()) {
        LOG.info("Counter is " + itc.next().getDisplayName());
      }
    }
    Assert.assertEquals(1, counters.countCounters());
  }

  class RMService extends AbstractService implements ApplicationClientProtocol {
    private String clientServiceBindAddress;
    InetSocketAddress clientBindAddress;
    private Server server;

    public RMService(String name) {
      super(name);
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      clientServiceBindAddress = RMADDRESS;
      /*
      clientServiceBindAddress = conf.get(
          YarnConfiguration.APPSMANAGER_ADDRESS,
          YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS);
          */
      clientBindAddress = NetUtils.createSocketAddr(clientServiceBindAddress);
      super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
      // All the clients to appsManager are supposed to be authenticated via
      // Kerberos if security is enabled, so no secretManager.
      YarnRPC rpc = YarnRPC.create(getConfig());
      Configuration clientServerConf = new Configuration(getConfig());
      this.server = rpc.getServer(ApplicationClientProtocol.class, this,
          clientBindAddress, clientServerConf, null, 1);
      this.server.start();
      super.serviceStart();
    }

    @Override
    public GetNewApplicationResponse getNewApplication(
        GetNewApplicationRequest request) throws IOException {
      return null;
    }

    @Override
    public GetApplicationReportResponse getApplicationReport(
        GetApplicationReportRequest request) throws IOException {
      ApplicationId applicationId = request.getApplicationId();
      ApplicationReport application = recordFactory
          .newRecordInstance(ApplicationReport.class);
      application.setApplicationId(applicationId);
      application.setFinalApplicationStatus(FinalApplicationStatus.UNDEFINED);
      if (amRunning) {
        application.setYarnApplicationState(YarnApplicationState.RUNNING);
      } else if (amRestarting) {
        application.setYarnApplicationState(YarnApplicationState.SUBMITTED);
      } else {
        application.setYarnApplicationState(YarnApplicationState.FINISHED);
        application.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
      }
      String[] split = AMHOSTADDRESS.split(":");
      application.setHost(split[0]);
      application.setRpcPort(Integer.parseInt(split[1]));
      application.setUser("TestClientRedirect-user");
      application.setName("N/A");
      application.setQueue("N/A");
      application.setStartTime(0);
      application.setFinishTime(0);
      application.setTrackingUrl("N/A");
      application.setDiagnostics("N/A");

      GetApplicationReportResponse response = recordFactory
          .newRecordInstance(GetApplicationReportResponse.class);
      response.setApplicationReport(application);
      return response;
    }

    @Override
    public SubmitApplicationResponse submitApplication(
        SubmitApplicationRequest request) throws IOException {
      throw new IOException("Test");
    }

    @Override
    public FailApplicationAttemptResponse failApplicationAttempt(
        FailApplicationAttemptRequest request) throws IOException {
      return recordFactory.newRecordInstance(FailApplicationAttemptResponse.class);
    }

    @Override
    public KillApplicationResponse forceKillApplication(
        KillApplicationRequest request) throws IOException {
      return KillApplicationResponse.newInstance(true);
    }

    @Override
    public GetClusterMetricsResponse getClusterMetrics(
        GetClusterMetricsRequest request) throws IOException {
      return null;
    }

    @Override
    public GetApplicationsResponse getApplications(
        GetApplicationsRequest request) throws IOException {
      return null;
    }

    @Override
    public GetClusterNodesResponse getClusterNodes(
        GetClusterNodesRequest request) throws IOException {
      return null;
    }

    @Override
    public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
        throws IOException {
      return null;
    }

    @Override
    public GetQueueUserAclsInfoResponse getQueueUserAcls(
        GetQueueUserAclsInfoRequest request) throws IOException {
      return null;
    }

    @Override
    public GetDelegationTokenResponse getDelegationToken(
        GetDelegationTokenRequest request) throws IOException {
      return null;
    }

    @Override
    public RenewDelegationTokenResponse renewDelegationToken(
        RenewDelegationTokenRequest request) throws IOException {
      return null;
    }

    @Override
    public CancelDelegationTokenResponse cancelDelegationToken(
        CancelDelegationTokenRequest request) throws IOException {
      return null;
    }

    @Override
    public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
        MoveApplicationAcrossQueuesRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public GetApplicationAttemptReportResponse getApplicationAttemptReport(
        GetApplicationAttemptReportRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public GetApplicationAttemptsResponse getApplicationAttempts(
        GetApplicationAttemptsRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public GetContainerReportResponse getContainerReport(
        GetContainerReportRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public GetContainersResponse getContainers(GetContainersRequest request)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public GetNewReservationResponse getNewReservation(
        GetNewReservationRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ReservationSubmissionResponse submitReservation(
        ReservationSubmissionRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ReservationUpdateResponse updateReservation(
        ReservationUpdateRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ReservationDeleteResponse deleteReservation(
        ReservationDeleteRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ReservationListResponse listReservations(
            ReservationListRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public GetNodesToLabelsResponse getNodeToLabels(
        GetNodesToLabelsRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public GetClusterNodeLabelsResponse getClusterNodeLabels(
        GetClusterNodeLabelsRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public GetLabelsToNodesResponse getLabelsToNodes(
        GetLabelsToNodesRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public UpdateApplicationPriorityResponse updateApplicationPriority(
        UpdateApplicationPriorityRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public SignalContainerResponse signalToContainer(
        SignalContainerRequest request) throws IOException {
      return null;
    }

    @Override
    public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
        UpdateApplicationTimeoutsRequest request)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public GetAllResourceTypeInfoResponse getResourceTypeInfo(
        GetAllResourceTypeInfoRequest request)
        throws YarnException, IOException {
      return null;
    }
  }

  class HistoryService extends AMService implements HSClientProtocol {
    public HistoryService() {
      super(HSHOSTADDRESS);
      this.protocol = HSClientProtocol.class;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request)
        throws IOException {
      hsContact = true;
      Counters counters = getMyCounters();
      GetCountersResponse response = recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(counters);
      return response;
   }
  }

  class AMService extends AbstractService
      implements MRClientProtocol {
    protected Class<?> protocol;
    private InetSocketAddress bindAddress;
    private Server server;
    private final String hostAddress;

    public AMService() {
      this(AMHOSTADDRESS);
    }

    @Override
    public InetSocketAddress getConnectAddress() {
      return bindAddress;
    }
    
    public AMService(String hostAddress) {
      super("AMService");
      this.protocol = MRClientProtocol.class;
      this.hostAddress = hostAddress;
    }

    public void start(Configuration conf) {
      YarnRPC rpc = YarnRPC.create(conf);
      //TODO : use fixed port ??
      InetSocketAddress address = NetUtils.createSocketAddr(hostAddress);
      InetAddress hostNameResolved = null;
      try {
        address.getAddress();
        hostNameResolved = InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        throw new YarnRuntimeException(e);
      }

      server =
          rpc.getServer(protocol, this, address,
              conf, null, 1);
      server.start();
      this.bindAddress = NetUtils.getConnectAddress(server);
       super.start();
       amRunning = true;
    }

    @Override
    protected void serviceStop() throws Exception {
      if (server != null) {
        server.stop();
      }
      super.serviceStop();
      amRunning = false;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request)
        throws IOException {
      JobId jobID = request.getJobId();

      amContact = true;

      Counters counters = getMyCounters();
      GetCountersResponse response = recordFactory
          .newRecordInstance(GetCountersResponse.class);
      response.setCounters(counters);
      return response;
    }

    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request)
        throws IOException {

      amContact = true;

      JobReport jobReport = recordFactory.newRecordInstance(JobReport.class);
      jobReport.setJobId(request.getJobId());
      jobReport.setJobState(JobState.RUNNING);
      jobReport.setJobName("TestClientRedirect-jobname");
      jobReport.setUser("TestClientRedirect-user");
      jobReport.setStartTime(0L);
      jobReport.setFinishTime(1L);

      GetJobReportResponse response = recordFactory
          .newRecordInstance(GetJobReportResponse.class);
      response.setJobReport(jobReport);
      return response;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
        throws IOException {
      return null;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
        GetTaskAttemptReportRequest request) throws IOException {
      return null;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse
        getTaskAttemptCompletionEvents(
            GetTaskAttemptCompletionEventsRequest request)
            throws IOException {
      return null;
    }

    @Override
    public GetTaskReportsResponse
        getTaskReports(GetTaskReportsRequest request)
            throws IOException {
      return null;
    }

    @Override
    public GetDiagnosticsResponse
        getDiagnostics(GetDiagnosticsRequest request)
            throws IOException {
      return null;
    }

    @Override
    public KillJobResponse killJob(KillJobRequest request)
        throws IOException {
      return recordFactory.newRecordInstance(KillJobResponse.class);
    }

    @Override
    public KillTaskResponse killTask(KillTaskRequest request)
        throws IOException {
      return null;
    }

    @Override
    public KillTaskAttemptResponse killTaskAttempt(
        KillTaskAttemptRequest request) throws IOException {
      return null;
    }

    @Override
    public FailTaskAttemptResponse failTaskAttempt(
        FailTaskAttemptRequest request) throws IOException {
      return null;
    }

    @Override
    public org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse getDelegationToken(
        org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest request)
        throws IOException {
      return null;
    }

    @Override
    public org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse renewDelegationToken(
        org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest request)
        throws IOException {
      return null;
    }

    @Override
    public org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenResponse cancelDelegationToken(
        org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest request)
        throws IOException {
      return null;
    }
  }

  static Counters getMyCounters() {
    Counter counter = recordFactory.newRecordInstance(Counter.class);
    counter.setName("Mycounter");
    counter.setDisplayName("My counter display name");
    counter.setValue(12345);

    CounterGroup group = recordFactory
        .newRecordInstance(CounterGroup.class);
    group.setName("MyGroup");
    group.setDisplayName("My groupd display name");
    group.setCounter("myCounter", counter);

    Counters counters = recordFactory.newRecordInstance(Counters.class);
    counters.setCounterGroup("myGroupd", group);
    return counters;
  }
}
