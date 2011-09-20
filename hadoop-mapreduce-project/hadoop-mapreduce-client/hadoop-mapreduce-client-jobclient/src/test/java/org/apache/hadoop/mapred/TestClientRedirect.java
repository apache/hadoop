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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
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
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.factory.providers.YarnRemoteExceptionFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;
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
    RMService rmService = new RMService("test");
    rmService.init(conf);
    rmService.start();
  
    AMService amService = new AMService();
    amService.init(conf);
    amService.start(conf);
    amRunning = true;

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
    amRunning = false;

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
    amRunning = true;
    amContact = false; //reset
    
    counters = cluster.getJob(jobID).getCounters();
    validateCounters(counters);
    Assert.assertTrue(amContact);
    
    amRunning = false;

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

  class RMService extends AbstractService implements ClientRMProtocol {
    private String clientServiceBindAddress;
    InetSocketAddress clientBindAddress;
    private Server server;

    public RMService(String name) {
      super(name);
    }

    @Override
    public void init(Configuration conf) {
      clientServiceBindAddress = RMADDRESS;
      /*
      clientServiceBindAddress = conf.get(
          YarnConfiguration.APPSMANAGER_ADDRESS,
          YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS);
          */
      clientBindAddress = NetUtils.createSocketAddr(clientServiceBindAddress);
      super.init(conf);
    }

    @Override
    public void start() {
      // All the clients to appsManager are supposed to be authenticated via
      // Kerberos if security is enabled, so no secretManager.
      YarnRPC rpc = YarnRPC.create(getConfig());
      Configuration clientServerConf = new Configuration(getConfig());
      this.server = rpc.getServer(ClientRMProtocol.class, this,
          clientBindAddress, clientServerConf, null, 1);
      this.server.start();
      super.start();
    }

    @Override
    public GetNewApplicationIdResponse getNewApplicationId(GetNewApplicationIdRequest request) throws YarnRemoteException {
      return null;
    }
    
    @Override
    public GetApplicationReportResponse getApplicationReport(
        GetApplicationReportRequest request) throws YarnRemoteException {
      ApplicationId applicationId = request.getApplicationId();
      ApplicationReport application = recordFactory
          .newRecordInstance(ApplicationReport.class);
      application.setApplicationId(applicationId);
      if (amRunning) {
        application.setState(ApplicationState.RUNNING);
      } else if (amRestarting) {
        application.setState(ApplicationState.SUBMITTED);
      } else {
        application.setState(ApplicationState.SUCCEEDED);
      }
      String[] split = AMHOSTADDRESS.split(":");
      application.setHost(split[0]);
      application.setRpcPort(Integer.parseInt(split[1]));
      application.setUser("TestClientRedirect-user");
      GetApplicationReportResponse response = recordFactory
          .newRecordInstance(GetApplicationReportResponse.class);
      response.setApplicationReport(application);
      return response;
    }

    @Override
    public SubmitApplicationResponse submitApplication(
        SubmitApplicationRequest request) throws YarnRemoteException {
      throw YarnRemoteExceptionFactoryProvider.getYarnRemoteExceptionFactory(
          null).createYarnRemoteException("Test");
    }

    @Override
    public FinishApplicationResponse finishApplication(
        FinishApplicationRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public GetClusterMetricsResponse getClusterMetrics(
        GetClusterMetricsRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public GetAllApplicationsResponse getAllApplications(
        GetAllApplicationsRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public GetClusterNodesResponse getClusterNodes(
        GetClusterNodesRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
        throws YarnRemoteException {
      return null;
    }

    @Override
    public GetQueueUserAclsInfoResponse getQueueUserAcls(
        GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
      return null;
    }
  }

  class HistoryService extends AMService {
    public HistoryService() {
      super(HSHOSTADDRESS);
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request) throws YarnRemoteException {
      hsContact = true;
      Counters counters = getMyCounters();
      GetCountersResponse response = recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(counters);
      return response;
   }
  }

  class AMService extends AbstractService 
      implements MRClientProtocol {
    private InetSocketAddress bindAddress;
    private Server server;
    private final String hostAddress;
    public AMService() {
      this(AMHOSTADDRESS);
    }
    
    public AMService(String hostAddress) {
      super("AMService");
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
        throw new YarnException(e);
      }

      server =
          rpc.getServer(MRClientProtocol.class, this, address,
              conf, null, 1);
      server.start();
      this.bindAddress =
        NetUtils.createSocketAddr(hostNameResolved.getHostAddress()
            + ":" + server.getPort());
       super.start();
    }

    public void stop() {
      server.close();
      super.stop();
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request)
        throws YarnRemoteException {
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
        throws YarnRemoteException {

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
        throws YarnRemoteException {
      return null;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
        GetTaskAttemptReportRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse
        getTaskAttemptCompletionEvents(
            GetTaskAttemptCompletionEventsRequest request)
            throws YarnRemoteException {
      return null;
    }

    @Override
    public GetTaskReportsResponse
        getTaskReports(GetTaskReportsRequest request)
            throws YarnRemoteException {
      return null;
    }

    @Override
    public GetDiagnosticsResponse
        getDiagnostics(GetDiagnosticsRequest request)
            throws YarnRemoteException {
      return null;
    }

    @Override
    public KillJobResponse killJob(KillJobRequest request)
        throws YarnRemoteException {
      return null;
    }

    @Override
    public KillTaskResponse killTask(KillTaskRequest request)
        throws YarnRemoteException {
      return null;
    }

    @Override
    public KillTaskAttemptResponse killTaskAttempt(
        KillTaskAttemptRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public FailTaskAttemptResponse failTaskAttempt(
        FailTaskAttemptRequest request) throws YarnRemoteException {
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
