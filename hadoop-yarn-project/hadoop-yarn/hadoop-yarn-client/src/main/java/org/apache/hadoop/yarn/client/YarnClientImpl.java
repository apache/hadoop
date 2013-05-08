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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class YarnClientImpl extends AbstractService implements YarnClient {

  private static final Log LOG = LogFactory.getLog(YarnClientImpl.class);

  protected ClientRMProtocol rmClient;
  protected InetSocketAddress rmAddress;
  protected long statePollIntervalMillis;

  private static final String ROOT = "root";

  public YarnClientImpl() {
    this(null);
  }
  
  public YarnClientImpl(InetSocketAddress rmAddress) {
    super(YarnClientImpl.class.getName());
    this.rmAddress = rmAddress;
  }

  private static InetSocketAddress getRmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
  }

  @Override
  public synchronized void init(Configuration conf) {
    if (this.rmAddress == null) {
      this.rmAddress = getRmAddress(conf);
    }
    statePollIntervalMillis = conf.getLong(
        YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS,
        YarnConfiguration.DEFAULT_YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS);
    super.init(conf);
  }

  @Override
  public synchronized void start() {
    YarnRPC rpc = YarnRPC.create(getConfig());

    this.rmClient = (ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, getConfig());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to ResourceManager at " + rmAddress);
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    if (this.rmClient != null) {
      RPC.stopProxy(this.rmClient);
    }
    super.stop();
  }

  @Override
  public GetNewApplicationResponse getNewApplication()
      throws YarnRemoteException, IOException {
    GetNewApplicationRequest request =
        Records.newRecord(GetNewApplicationRequest.class);
    return rmClient.getNewApplication(request);
  }

  @Override
  public ApplicationId
      submitApplication(ApplicationSubmissionContext appContext)
          throws YarnRemoteException, IOException {
    ApplicationId applicationId = appContext.getApplicationId();
    appContext.setApplicationId(applicationId);
    SubmitApplicationRequest request =
        Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(appContext);
    rmClient.submitApplication(request);

    int pollCount = 0;
    while (true) {
      YarnApplicationState state =
          getApplicationReport(applicationId).getYarnApplicationState();
      if (!state.equals(YarnApplicationState.NEW) &&
          !state.equals(YarnApplicationState.NEW_SAVING)) {
        break;
      }
      // Notify the client through the log every 10 poll, in case the client
      // is blocked here too long.
      if (++pollCount % 10 == 0) {
        LOG.info("Application submission is not finished, " +
            "submitted application " + applicationId +
            " is still in " + state);
      }
      try {
        Thread.sleep(statePollIntervalMillis);
      } catch (InterruptedException ie) {
      }
    }


    LOG.info("Submitted application " + applicationId + " to ResourceManager"
        + " at " + rmAddress);
    return applicationId;
  }

  @Override
  public void killApplication(ApplicationId applicationId)
      throws YarnRemoteException, IOException {
    LOG.info("Killing application " + applicationId);
    KillApplicationRequest request =
        Records.newRecord(KillApplicationRequest.class);
    request.setApplicationId(applicationId);
    rmClient.forceKillApplication(request);
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnRemoteException, IOException {
    GetApplicationReportRequest request =
        Records.newRecord(GetApplicationReportRequest.class);
    request.setApplicationId(appId);
    GetApplicationReportResponse response =
        rmClient.getApplicationReport(request);
    return response.getApplicationReport();
  }

  @Override
  public List<ApplicationReport> getApplicationList()
      throws YarnRemoteException, IOException {
    GetAllApplicationsRequest request =
        Records.newRecord(GetAllApplicationsRequest.class);
    GetAllApplicationsResponse response = rmClient.getAllApplications(request);
    return response.getApplicationList();
  }

  @Override
  public YarnClusterMetrics getYarnClusterMetrics() throws YarnRemoteException,
      IOException {
    GetClusterMetricsRequest request =
        Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse response = rmClient.getClusterMetrics(request);
    return response.getClusterMetrics();
  }

  @Override
  public List<NodeReport> getNodeReports() throws YarnRemoteException,
      IOException {
    GetClusterNodesRequest request =
        Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse response = rmClient.getClusterNodes(request);
    return response.getNodeReports();
  }

  @Override
  public DelegationToken getRMDelegationToken(Text renewer)
      throws YarnRemoteException, IOException {
    /* get the token from RM */
    GetDelegationTokenRequest rmDTRequest =
        Records.newRecord(GetDelegationTokenRequest.class);
    rmDTRequest.setRenewer(renewer.toString());
    GetDelegationTokenResponse response =
        rmClient.getDelegationToken(rmDTRequest);
    return response.getRMDelegationToken();
  }


  private GetQueueInfoRequest
      getQueueInfoRequest(String queueName, boolean includeApplications,
          boolean includeChildQueues, boolean recursive) {
    GetQueueInfoRequest request = Records.newRecord(GetQueueInfoRequest.class);
    request.setQueueName(queueName);
    request.setIncludeApplications(includeApplications);
    request.setIncludeChildQueues(includeChildQueues);
    request.setRecursive(recursive);
    return request;
  }

  @Override
  public QueueInfo getQueueInfo(String queueName) throws YarnRemoteException,
      IOException {
    GetQueueInfoRequest request =
        getQueueInfoRequest(queueName, true, false, false);
    Records.newRecord(GetQueueInfoRequest.class);
    return rmClient.getQueueInfo(request).getQueueInfo();
  }

  @Override
  public List<QueueUserACLInfo> getQueueAclsInfo() throws YarnRemoteException,
      IOException {
    GetQueueUserAclsInfoRequest request =
        Records.newRecord(GetQueueUserAclsInfoRequest.class);
    return rmClient.getQueueUserAcls(request).getUserAclsInfoList();
  }

  @Override
  public List<QueueInfo> getAllQueues() throws YarnRemoteException,
      IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo rootQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(ROOT, false, true, true))
          .getQueueInfo();
    getChildQueues(rootQueue, queues, true);
    return queues;
  }

  @Override
  public List<QueueInfo> getRootQueueInfos() throws YarnRemoteException,
      IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo rootQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(ROOT, false, true, true))
          .getQueueInfo();
    getChildQueues(rootQueue, queues, false);
    return queues;
  }

  @Override
  public List<QueueInfo> getChildQueueInfos(String parent)
      throws YarnRemoteException, IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo parentQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(parent, false, true, false))
          .getQueueInfo();
    getChildQueues(parentQueue, queues, true);
    return queues;
  }

  private void getChildQueues(QueueInfo parent, List<QueueInfo> queues,
      boolean recursive) {
    List<QueueInfo> childQueues = parent.getChildQueues();

    for (QueueInfo child : childQueues) {
      queues.add(child);
      if (recursive) {
        getChildQueues(child, queues, recursive);
      }
    }
  }
}
