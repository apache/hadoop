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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
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
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.service.AbstractService;


/**
 * The client interface to the Resource Manager. This module handles all the rpc
 * interfaces to the resource manager from the client.
 */
public class ClientRMService extends AbstractService implements
    ClientRMProtocol {
  private static final ArrayList<ApplicationReport> EMPTY_APPS_REPORT = new ArrayList<ApplicationReport>();

  private static final Log LOG = LogFactory.getLog(ClientRMService.class);

  final private AtomicInteger applicationCounter = new AtomicInteger(0);
  final private YarnScheduler scheduler;
  final private RMContext rmContext;
  private final RMAppManager rmAppManager;

  private String clientServiceBindAddress;
  private Server server;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  InetSocketAddress clientBindAddress;

  private  ApplicationACLsManager aclsManager;
  private Map<ApplicationACL, AccessControlList> applicationACLs;
  
  public ClientRMService(RMContext rmContext, YarnScheduler scheduler,
      RMAppManager rmAppManager) {
    super(ClientRMService.class.getName());
    this.scheduler = scheduler;
    this.rmContext = rmContext;
    this.rmAppManager = rmAppManager;
  }
  
  @Override
  public void init(Configuration conf) {
    clientServiceBindAddress =
      conf.get(YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS);
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
        YarnConfiguration.YARN_SECURITY_INFO,
        ClientRMSecurityInfo.class, SecurityInfo.class);
    this.server =   
      rpc.getServer(ClientRMProtocol.class, this,
            clientBindAddress,
            clientServerConf, null,
            clientServerConf.getInt(YarnConfiguration.RM_CLIENT_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_RM_CLIENT_THREAD_COUNT));
    this.server.start();
    super.start();
  }

  /**
   * check if the calling user has the access to application information.
   * @param appAttemptId
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
    RMApp application = rmContext.getRMApps().get(applicationId);
    ApplicationReport report = (application == null) ? null : application
        .createAndGetApplicationReport();

    GetApplicationReportResponse response = recordFactory
        .newRecordInstance(GetApplicationReportResponse.class);
    response.setApplicationReport(report);
    return response;
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnRemoteException {
    ApplicationSubmissionContext submissionContext = request
        .getApplicationSubmissionContext();
    ApplicationId applicationId = submissionContext.getApplicationId();
    String user = submissionContext.getUser();
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
      if (rmContext.getRMApps().get(applicationId) != null) {
        throw new IOException("Application with id " + applicationId
            + " is already present! Cannot add a duplicate!");
      }
      
      // Safety 
      submissionContext.setUser(user);
      
      // This needs to be synchronous as the client can query 
      // immediately following the submission to get the application status.
      // So call handle directly and do not send an event.
      rmAppManager.handle(new RMAppManagerSubmitEvent(submissionContext));

      LOG.info("Application with id " + applicationId.getId() + 
          " submitted by user " + user + " with " + submissionContext);
      RMAuditLogger.logSuccess(user, AuditConstants.SUBMIT_APP_REQUEST,
          "ClientRMService", applicationId);
    } catch (IOException ie) {
      LOG.info("Exception in submitting application", ie);
      RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_APP_REQUEST, 
          ie.getMessage(), "ClientRMService",
          "Exception in submitting application", applicationId);
      throw RPCUtil.getRemoteException(ie);
    }

    SubmitApplicationResponse response = recordFactory
        .newRecordInstance(SubmitApplicationResponse.class);
    return response;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnRemoteException {

    ApplicationId applicationId = request.getApplicationId();

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      RMAuditLogger.logFailure("UNKNOWN", AuditConstants.KILL_APP_REQUEST,
          "UNKNOWN", "ClientRMService" , "Error getting UGI",
          applicationId);
      throw RPCUtil.getRemoteException(ie);
    }

    RMApp application = this.rmContext.getRMApps().get(applicationId);
    // TODO: What if null
    if (!checkAccess(callerUGI, application.getUser(),
        ApplicationACL.MODIFY_APP)) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(), 
          AuditConstants.KILL_APP_REQUEST, 
          "User doesn't have MODIFY_APP permissions", "ClientRMService",
          AuditConstants.UNAUTHORIZED_USER, applicationId);
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + ApplicationACL.MODIFY_APP.name() + " on " + applicationId));
    }

    this.rmContext.getDispatcher().getEventHandler().handle(
        new RMAppEvent(applicationId, RMAppEventType.KILL));

    RMAuditLogger.logSuccess(callerUGI.getShortUserName(), 
        AuditConstants.KILL_APP_REQUEST, "ClientRMService" , applicationId);
    KillApplicationResponse response = recordFactory
        .newRecordInstance(KillApplicationResponse.class);
    return response;
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnRemoteException {
    GetClusterMetricsResponse response = recordFactory
        .newRecordInstance(GetClusterMetricsResponse.class);
    YarnClusterMetrics ymetrics = recordFactory
        .newRecordInstance(YarnClusterMetrics.class);
    ymetrics.setNumNodeManagers(this.rmContext.getRMNodes().size());
    response.setClusterMetrics(ymetrics);
    return response;
  }
  
  @Override
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) throws YarnRemoteException {

    List<ApplicationReport> reports = new ArrayList<ApplicationReport>();
    for (RMApp application : this.rmContext.getRMApps().values()) {
      reports.add(application.createAndGetApplicationReport());
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
    Collection<RMNode> nodes = this.rmContext.getRMNodes().values();
    List<NodeReport> nodeReports = new ArrayList<NodeReport>(nodes.size());
    for (RMNode nodeInfo : nodes) {
      nodeReports.add(createNodeReports(nodeInfo));
    }
    response.setNodeReports(nodeReports);
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
            request.getIncludeChildQueues(), 
            request.getRecursive());
      List<ApplicationReport> appReports = EMPTY_APPS_REPORT;
      if (request.getIncludeApplications()) {
        Collection<RMApp> apps = this.rmContext.getRMApps().values();
        appReports = new ArrayList<ApplicationReport>(
            apps.size());
        for (RMApp app : apps) {
          appReports.add(app.createAndGetApplicationReport());
        }
      }
      queueInfo.setApplications(appReports);
      response.setQueueInfo(queueInfo);
    } catch (IOException ioe) {
      LOG.info("Failed to getQueueInfo for " + request.getQueueName(), ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
    
    return response;
  }

  private NodeReport createNodeReports(RMNode rmNode) {
    NodeReport report = recordFactory.newRecordInstance(NodeReport.class);
    report.setNodeId(rmNode.getNodeID());
    report.setRackName(rmNode.getRackName());
    report.setCapability(rmNode.getTotalCapability());
    report.setNodeHealthStatus(rmNode.getNodeHealthStatus());
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport schedulerNodeReport = scheduler
        .getNodeReport(rmNode.getNodeID());
    report.setUsed(schedulerNodeReport.getUsedResources());
    report.setNumContainers(schedulerNodeReport.getNumContainers());
    return report;
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
