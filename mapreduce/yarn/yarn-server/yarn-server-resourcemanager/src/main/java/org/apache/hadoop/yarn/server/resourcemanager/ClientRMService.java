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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
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
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeManagerInfo;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationImpl;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.application.ApplicationACL;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.application.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.ClusterTracker;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.service.AbstractService;


/**
 * The client interface to the Resource Manager. This module handles all the rpc
 * interfaces to the resource manager from the client.
 */
public class ClientRMService extends AbstractService implements
    ClientRMProtocol {
  private static final Log LOG = LogFactory.getLog(ClientRMService.class);

  final private AtomicInteger applicationCounter = new AtomicInteger(0);

  final private ClusterTracker clusterInfo;
  final private YarnScheduler scheduler;
  final private RMContext rmContext;
  private final ClientToAMSecretManager clientToAMSecretManager;
  private final AMLivelinessMonitor amLivelinessMonitor;

  private String clientServiceBindAddress;
  private Server server;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  InetSocketAddress clientBindAddress;

  private  ApplicationACLsManager aclsManager;
  private Map<ApplicationACL, AccessControlList> applicationACLs;
  
  public ClientRMService(RMContext rmContext,
      AMLivelinessMonitor amLivelinessMonitor,
      ClientToAMSecretManager clientToAMSecretManager,
      ClusterTracker clusterInfo, YarnScheduler scheduler) {
    super(ClientRMService.class.getName());
    this.clusterInfo = clusterInfo;
    this.scheduler = scheduler;
    this.rmContext = rmContext;
    this.amLivelinessMonitor = amLivelinessMonitor;
    this.clientToAMSecretManager = clientToAMSecretManager;
  }
  
  @Override
  public void init(Configuration conf) {
    clientServiceBindAddress =
      conf.get(YarnConfiguration.APPSMANAGER_ADDRESS,
          YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS);
    clientBindAddress =
      NetUtils.createSocketAddr(clientServiceBindAddress);

    this.aclsManager = new ApplicationACLsManager(conf);
    this.applicationACLs = aclsManager.constructApplicationACLs(conf);

    super.init(conf);
  }
  
  @Override
  public void start() {
    // All the clients to appsManager are supposed to be authenticated via
    // Kerberos if security is enabled, so no secretManager.
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration clientServerConf = new Configuration(getConfig());
    clientServerConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        ClientRMSecurityInfo.class, SecurityInfo.class);
    this.server =   
      rpc.getServer(ClientRMProtocol.class, this,
            clientBindAddress,
            clientServerConf, null,
            clientServerConf.getInt(RMConfig.RM_CLIENT_THREADS, 
                RMConfig.DEFAULT_RM_CLIENT_THREADS));
    this.server.start();
    super.start();
  }

  private ApplicationReport createApplicationReport(Application application,
      String user, String queue, String name, Container masterContainer) {
    ApplicationMaster am = application.getMaster();
    ApplicationReport applicationReport = 
      recordFactory.newRecordInstance(ApplicationReport.class);
    applicationReport.setApplicationId(am.getApplicationId());
    applicationReport.setMasterContainer(masterContainer);
    applicationReport.setHost(am.getHost());
    applicationReport.setRpcPort(am.getRpcPort());
    applicationReport.setClientToken(am.getClientToken());
    applicationReport.setTrackingUrl(am.getTrackingUrl());
    applicationReport.setDiagnostics(am.getDiagnostics());
    applicationReport.setName(name);
    applicationReport.setQueue(queue);
    applicationReport.setState(am.getState());
    applicationReport.setStatus(am.getStatus());
    applicationReport.setUser(user);
    return applicationReport;
  }

  /**
   * check if the calling user has the access to application information.
   * @param applicationId
   * @param callerUGI
   * @param owner
   * @param appACL
   * @return
   */
  private boolean checkAccess(UserGroupInformation callerUGI, String owner, ApplicationACL appACL) {
      if (!UserGroupInformation.isSecurityEnabled()) {
        return true;
      }
      AccessControlList applicationACL = applicationACLs.get(appACL);
      return aclsManager.checkAccess(callerUGI, appACL, owner, applicationACL);
  }

  public ApplicationId getNewApplicationId() {
    ApplicationId applicationId = org.apache.hadoop.yarn.util.BuilderUtils
        .newApplicationId(recordFactory, ResourceManager.clusterTimeStamp,
            applicationCounter.incrementAndGet());
    LOG.info("Allocated new applicationId: " + applicationId.getId());
    return applicationId;
  }

  @Override
  public GetNewApplicationIdResponse getNewApplicationId(
      GetNewApplicationIdRequest request) throws YarnRemoteException {
    GetNewApplicationIdResponse response = recordFactory
        .newRecordInstance(GetNewApplicationIdResponse.class);
    response.setApplicationId(getNewApplicationId());
    return response;
  }
  
  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnRemoteException {
    ApplicationId applicationId = request.getApplicationId();
    Application application = rmContext.getApplications().get(applicationId);
    ApplicationReport report = (application == null) ? null
        : createApplicationReport(application, application.getUser(),
            application.getQueue(), application.getName(), application
                .getMasterContainer());

    GetApplicationReportResponse response = recordFactory
        .newRecordInstance(GetApplicationReportResponse.class);
    response.setApplicationReport(report);
    return response;
  }

  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnRemoteException {
    ApplicationSubmissionContext submissionContext = request
        .getApplicationSubmissionContext();
    try {

      ApplicationId applicationId = submissionContext.getApplicationId();
      String clientTokenStr = null;
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      if (UserGroupInformation.isSecurityEnabled()) {
        Token<ApplicationTokenIdentifier> clientToken = new Token<ApplicationTokenIdentifier>(
            new ApplicationTokenIdentifier(applicationId),
            this.clientToAMSecretManager);
        clientTokenStr = clientToken.encodeToUrlString();
        LOG.debug("Sending client token as " + clientTokenStr);
      }

      submissionContext.setQueue(submissionContext.getQueue() == null
          ? "default" : submissionContext.getQueue());
      submissionContext.setApplicationName(submissionContext
          .getApplicationName() == null ? "N/A" : submissionContext
          .getApplicationName());

      ApplicationStore appStore = rmContext.getApplicationsStore()
          .createApplicationStore(submissionContext.getApplicationId(),
              submissionContext);
      Application application = new ApplicationImpl(rmContext, getConfig(),
          user, submissionContext, clientTokenStr, appStore,
          this.amLivelinessMonitor);
      if (rmContext.getApplications().putIfAbsent(
          application.getApplicationID(), application) != null) {
        throw new IOException("Application with id "
            + application.getApplicationID()
            + " is already present! Cannot add a duplicate!");
      }

      /**
       * this can throw so we need to call it synchronously to let the client
       * know as soon as it submits. For backwards compatibility we cannot make
       * it asynchronous
       */
      try {
        scheduler.addApplication(applicationId, application.getMaster(),
            user, application.getQueue(), submissionContext.getPriority(),
            application.getStore());
      } catch (IOException io) {
        LOG.info("Failed to submit application " + applicationId, io);
        rmContext.getDispatcher().getSyncHandler().handle(
            new ApplicationEvent(ApplicationEventType.FAILED, applicationId));
        throw io;
      }

      rmContext.getDispatcher().getSyncHandler().handle(
          new ApplicationEvent(ApplicationEventType.ALLOCATE, applicationId));

      // TODO this should happen via dispatcher. should move it out to scheudler
      // negotiator.
      LOG.info("Application with id " + applicationId.getId()
          + " submitted by user " + user + " with " + submissionContext);
    } catch (IOException ie) {
      LOG.info("Exception in submitting application", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    SubmitApplicationResponse response = recordFactory
        .newRecordInstance(SubmitApplicationResponse.class);
    return response;
  }

  @Override
  public FinishApplicationResponse finishApplication(
      FinishApplicationRequest request) throws YarnRemoteException {

    ApplicationId applicationId = request.getApplicationId();

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    Application application = rmContext.getApplications().get(applicationId);
    // TODO: What if null
    if (!checkAccess(callerUGI, application.getUser(),
        ApplicationACL.MODIFY_APP)) {
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + ApplicationACL.MODIFY_APP.name() + " on " + applicationId));
    }

    rmContext.getDispatcher().getEventHandler().handle(
        new ApplicationEvent(ApplicationEventType.KILL, applicationId));

    FinishApplicationResponse response = recordFactory
        .newRecordInstance(FinishApplicationResponse.class);
    return response;
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(GetClusterMetricsRequest request) throws YarnRemoteException {
    GetClusterMetricsResponse response = recordFactory.newRecordInstance(GetClusterMetricsResponse.class);
    response.setClusterMetrics(clusterInfo.getClusterMetrics());
    return response;
  }
  
  @Override
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) throws YarnRemoteException {

    List<ApplicationReport> reports = new ArrayList<ApplicationReport>();
    for (Application application : rmContext.getApplications().values()) {
      reports.add(createApplicationReport(application, application.getUser(),
          application.getQueue(), application.getName(), application
              .getMasterContainer()));
    }

    GetAllApplicationsResponse response = 
      recordFactory.newRecordInstance(GetAllApplicationsResponse.class);
    response.setApplicationList(reports);
    return response;
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnRemoteException {
    GetClusterNodesResponse response = 
      recordFactory.newRecordInstance(GetClusterNodesResponse.class);
    List<NodeInfo> nodeInfos = clusterInfo.getAllNodeInfo();
    List<NodeManagerInfo> nodes = 
      new ArrayList<NodeManagerInfo>(nodeInfos.size());
    for (NodeInfo nodeInfo : nodeInfos) {
      nodes.add(createNodeManagerInfo(nodeInfo));
    }
    response.setNodeManagerList(nodes);
    return response;
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnRemoteException {
    GetQueueInfoResponse response =
      recordFactory.newRecordInstance(GetQueueInfoResponse.class);
    try {
      QueueInfo queueInfo = 
        scheduler.getQueueInfo(request.getQueueName(), 
            request.getIncludeApplications(), 
            request.getIncludeChildQueues(), 
            request.getRecursive());
      response.setQueueInfo(queueInfo);
    } catch (IOException ioe) {
      LOG.info("Failed to getQueueInfo for " + request.getQueueName(), ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
    
    return response;
  }

  private NodeManagerInfo createNodeManagerInfo(NodeInfo nodeInfo) {
    NodeManagerInfo node = 
      recordFactory.newRecordInstance(NodeManagerInfo.class);
    node.setNodeAddress(nodeInfo.getNodeAddress());
    node.setRackName(nodeInfo.getRackName());
    node.setCapability(nodeInfo.getTotalCapability());
    node.setUsed(nodeInfo.getUsedResource());
    node.setNumContainers(nodeInfo.getNumContainers());
    return node;
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
    GetQueueUserAclsInfoResponse response = 
      recordFactory.newRecordInstance(GetQueueUserAclsInfoResponse.class);
    response.setUserAclsInfoList(scheduler.getQueueUserAclInfo());
    return response;
  }

  @Override
  public void stop() {
    if (this.server != null) {
        this.server.close();
    }
    super.stop();
  }
}
