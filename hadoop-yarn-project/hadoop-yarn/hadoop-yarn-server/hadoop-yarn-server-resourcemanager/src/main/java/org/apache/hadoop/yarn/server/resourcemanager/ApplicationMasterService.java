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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;

@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService implements
    AMRMProtocol {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);
  private final AMLivelinessMonitor amLivelinessMonitor;
  private YarnScheduler rScheduler;
  private InetSocketAddress bindAddress;
  private Server server;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private final ConcurrentMap<ApplicationAttemptId, AMResponse> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AMResponse>();
  private final AMResponse reboot = recordFactory.newRecordInstance(AMResponse.class);
  private final RMContext rmContext;

  public ApplicationMasterService(RMContext rmContext, YarnScheduler scheduler) {
    super(ApplicationMasterService.class.getName());
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.reboot.setReboot(true);
//    this.reboot.containers = new ArrayList<Container>();
    this.rmContext = rmContext;
  }

  @Override
  public void start() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    InetSocketAddress masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    this.server =
      rpc.getServer(AMRMProtocol.class, this, masterServiceAddress,
          conf, this.rmContext.getApplicationTokenSecretManager(),
          conf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT, 
              YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new RMPolicyProvider());
    }
    
    this.server.start();
    this.bindAddress =
        conf.updateConnectAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                               server.getListenerAddress());
    super.start();
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  private void authorizeRequest(ApplicationAttemptId appAttemptID)
      throws YarnRemoteException {

    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    String appAttemptIDStr = appAttemptID.toString();

    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg = "Cannot obtain the user-name for ApplicationAttemptID: "
          + appAttemptIDStr + ". Got exception: "
          + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }

    if (!remoteUgi.getUserName().equals(appAttemptIDStr)) {
      String msg = "Unauthorized request from ApplicationMaster. "
          + "Expected ApplicationAttemptID: " + remoteUgi.getUserName()
          + " Found: " + appAttemptIDStr;
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnRemoteException {

    ApplicationAttemptId applicationAttemptId = request
        .getApplicationAttemptId();
    authorizeRequest(applicationAttemptId);

    ApplicationId appID = applicationAttemptId.getApplicationId();
    AMResponse lastResponse = responseMap.get(applicationAttemptId);
    if (lastResponse == null) {
      String message = "Application doesn't exist in cache "
          + applicationAttemptId;
      LOG.error(message);
      RMAuditLogger.logFailure(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM, message, "ApplicationMasterService",
          "Error in registering application master", appID,
          applicationAttemptId);
      throw RPCUtil.getRemoteException(message);
    }

    // Allow only one thread in AM to do registerApp at a time.
    synchronized (lastResponse) {

      LOG.info("AM registration " + applicationAttemptId);
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptRegistrationEvent(applicationAttemptId, request
              .getHost(), request.getRpcPort(), request.getTrackingUrl()));

      RMApp app = this.rmContext.getRMApps().get(appID);
      RMAuditLogger.logSuccess(app.getUser(),
          AuditConstants.REGISTER_AM, "ApplicationMasterService", appID,
          applicationAttemptId);

      // Pick up min/max resource from scheduler...
      RegisterApplicationMasterResponse response = recordFactory
          .newRecordInstance(RegisterApplicationMasterResponse.class);
      response.setMinimumResourceCapability(rScheduler
          .getMinimumResourceCapability());
      response.setMaximumResourceCapability(rScheduler
          .getMaximumResourceCapability());
      response.setApplicationACLs(app.getRMAppAttempt(applicationAttemptId)
          .getSubmissionContext().getAMContainerSpec().getApplicationACLs());
      return response;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnRemoteException {

    ApplicationAttemptId applicationAttemptId = request
        .getApplicationAttemptId();
    authorizeRequest(applicationAttemptId);

    AMResponse lastResponse = responseMap.get(applicationAttemptId);
    if (lastResponse == null) {
      String message = "Application doesn't exist in cache "
          + applicationAttemptId;
      LOG.error(message);
      throw RPCUtil.getRemoteException(message);
    }

    // Allow only one thread in AM to do finishApp at a time.
    synchronized (lastResponse) {

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptUnregistrationEvent(applicationAttemptId, request
              .getTrackingUrl(), request.getFinalApplicationStatus(), request
              .getDiagnostics()));

      FinishApplicationMasterResponse response = recordFactory
          .newRecordInstance(FinishApplicationMasterResponse.class);
      return response;
    }
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnRemoteException {

    ApplicationAttemptId appAttemptId = request.getApplicationAttemptId();
    authorizeRequest(appAttemptId);

    this.amLivelinessMonitor.receivedPing(appAttemptId);

    /* check if its in cache */
    AllocateResponse allocateResponse = recordFactory
        .newRecordInstance(AllocateResponse.class);
    AMResponse lastResponse = responseMap.get(appAttemptId);
    if (lastResponse == null) {
      LOG.error("AppAttemptId doesnt exist in cache " + appAttemptId);
      allocateResponse.setAMResponse(reboot);
      return allocateResponse;
    }
    if ((request.getResponseId() + 1) == lastResponse.getResponseId()) {
      /* old heartbeat */
      allocateResponse.setAMResponse(lastResponse);
      return allocateResponse;
    } else if (request.getResponseId() + 1 < lastResponse.getResponseId()) {
      LOG.error("Invalid responseid from appAttemptId " + appAttemptId);
      // Oh damn! Sending reboot isn't enough. RM state is corrupted. TODO:
      // Reboot is not useful since after AM reboots, it will send register and 
      // get an exception. Might as well throw an exception here.
      allocateResponse.setAMResponse(reboot);
      return allocateResponse;
    } 
    
    // Allow only one thread in AM to do heartbeat at a time.
    synchronized (lastResponse) {

      // Send the status update to the appAttempt.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptStatusupdateEvent(appAttemptId, request
              .getProgress()));

      List<ResourceRequest> ask = request.getAskList();
      List<ContainerId> release = request.getReleaseList();

      // Send new requests to appAttempt.
      Allocation allocation =
          this.rScheduler.allocate(appAttemptId, ask, release);

      RMApp app = this.rmContext.getRMApps().get(
          appAttemptId.getApplicationId());
      RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);
      
      AMResponse response = recordFactory.newRecordInstance(AMResponse.class);

      // update the response with the deltas of node status changes
      List<RMNode> updatedNodes = new ArrayList<RMNode>();
      if(app.pullRMNodeUpdates(updatedNodes) > 0) {
        List<NodeReport> updatedNodeReports = new ArrayList<NodeReport>();
        for(RMNode rmNode: updatedNodes) {
          SchedulerNodeReport schedulerNodeReport =  
              rScheduler.getNodeReport(rmNode.getNodeID());
          Resource used = BuilderUtils.newResource(0, 0);
          int numContainers = 0;
          if (schedulerNodeReport != null) {
            used = schedulerNodeReport.getUsedResource();
            numContainers = schedulerNodeReport.getNumContainers();
          }
          NodeReport report = BuilderUtils.newNodeReport(rmNode.getNodeID(),
              rmNode.getState(),
              rmNode.getHttpAddress(), rmNode.getRackName(), used,
              rmNode.getTotalCapability(), numContainers,
              rmNode.getNodeHealthStatus());
          
          updatedNodeReports.add(report);
        }
        response.setUpdatedNodes(updatedNodeReports);
      }

      response.setAllocatedContainers(allocation.getContainers());
      response.setCompletedContainersStatuses(appAttempt
          .pullJustFinishedContainers());
      response.setResponseId(lastResponse.getResponseId() + 1);
      response.setAvailableResources(allocation.getResourceLimit());
      
      AMResponse oldResponse = responseMap.put(appAttemptId, response);
      if (oldResponse == null) {
        // appAttempt got unregistered, remove it back out
        responseMap.remove(appAttemptId);
        String message = "App Attempt removed from the cache during allocate"
            + appAttemptId;
        LOG.error(message);
        allocateResponse.setAMResponse(reboot);
        return allocateResponse;
      }
      
      allocateResponse.setAMResponse(response);
      allocateResponse.setNumClusterNodes(this.rScheduler.getNumClusterNodes());
      return allocateResponse;
    }
  }

  public void registerAppAttempt(ApplicationAttemptId attemptId) {
    AMResponse response = recordFactory.newRecordInstance(AMResponse.class);
    response.setResponseId(0);
    LOG.info("Registering " + attemptId);
    responseMap.put(attemptId, response);
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId) {
    responseMap.remove(attemptId);
  }

  public void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }
  
  @Override
  public void stop() {
    if (this.server != null) {
      this.server.stop();
    }
    super.stop();
  }
}
