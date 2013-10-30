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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService implements
    ApplicationMasterProtocol {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);
  private final AMLivelinessMonitor amLivelinessMonitor;
  private YarnScheduler rScheduler;
  private InetSocketAddress bindAddress;
  private Server server;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final ConcurrentMap<ApplicationAttemptId, AllocateResponse> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AllocateResponse>();
  private final AllocateResponse resync =
      recordFactory.newRecordInstance(AllocateResponse.class);
  private final RMContext rmContext;

  public ApplicationMasterService(RMContext rmContext, YarnScheduler scheduler) {
    super(ApplicationMasterService.class.getName());
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.resync.setAMCommand(AMCommand.AM_RESYNC);
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    InetSocketAddress masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    Configuration serverConf = conf;
    // If the auth is not-simple, enforce it to be token-based.
    serverConf = new Configuration(conf);
    serverConf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
    this.server =
      rpc.getServer(ApplicationMasterProtocol.class, this, masterServiceAddress,
          serverConf, this.rmContext.getAMRMTokenSecretManager(),
          serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT, 
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
    super.serviceStart();
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  // Obtain the needed AMRMTokenIdentifier from the remote-UGI. RPC layer
  // currently sets only the required id, but iterate through anyways just to be
  // sure.
  private AMRMTokenIdentifier selectAMRMTokenIdentifier(
      UserGroupInformation remoteUgi) throws IOException {
    AMRMTokenIdentifier result = null;
    Set<TokenIdentifier> tokenIds = remoteUgi.getTokenIdentifiers();
    for (TokenIdentifier tokenId : tokenIds) {
      if (tokenId instanceof AMRMTokenIdentifier) {
        result = (AMRMTokenIdentifier) tokenId;
        break;
      }
    }

    return result;
  }

  private ApplicationAttemptId authorizeRequest()
      throws YarnException {

    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg =
          "Cannot obtain the user-name for authorizing ApplicationMaster. "
              + "Got exception: " + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }

    boolean tokenFound = false;
    String message = "";
    AMRMTokenIdentifier appTokenIdentifier = null;
    try {
      appTokenIdentifier = selectAMRMTokenIdentifier(remoteUgi);
      if (appTokenIdentifier == null) {
        tokenFound = false;
        message = "No AMRMToken found for user " + remoteUgi.getUserName();
      } else {
        tokenFound = true;
      }
    } catch (IOException e) {
      tokenFound = false;
      message =
          "Got exception while looking for AMRMToken for user "
              + remoteUgi.getUserName();
    }

    if (!tokenFound) {
      LOG.warn(message);
      throw RPCUtil.getRemoteException(message);
    }

    return appTokenIdentifier.getApplicationAttemptId();
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {

    ApplicationAttemptId applicationAttemptId = authorizeRequest();

    ApplicationId appID = applicationAttemptId.getApplicationId();
    AllocateResponse lastResponse = responseMap.get(applicationAttemptId);
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

      if (hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is already registered : "
                + applicationAttemptId.getApplicationId();
        LOG.warn(message);
        RMAuditLogger.logFailure(
          this.rmContext.getRMApps()
            .get(applicationAttemptId.getApplicationId()).getUser(),
          AuditConstants.REGISTER_AM, "", "ApplicationMasterService", message,
          applicationAttemptId.getApplicationId(), applicationAttemptId);
        throw new InvalidApplicationMasterRequestException(message);
      }
      
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);
      RMApp app = this.rmContext.getRMApps().get(appID);
      
      // Setting the response id to 0 to identify if the
      // application master is register for the respective attemptid
      lastResponse.setResponseId(0);
      responseMap.put(applicationAttemptId, lastResponse);
      LOG.info("AM registration " + applicationAttemptId);
      this.rmContext
        .getDispatcher()
        .getEventHandler()
        .handle(
          new RMAppAttemptRegistrationEvent(applicationAttemptId, request
            .getHost(), request.getRpcPort(), request.getTrackingUrl()));
      RMAuditLogger.logSuccess(app.getUser(), AuditConstants.REGISTER_AM,
        "ApplicationMasterService", appID, applicationAttemptId);

      // Pick up min/max resource from scheduler...
      RegisterApplicationMasterResponse response = recordFactory
          .newRecordInstance(RegisterApplicationMasterResponse.class);
      response.setMaximumResourceCapability(rScheduler
          .getMaximumResourceCapability());
      response.setApplicationACLs(app.getRMAppAttempt(applicationAttemptId)
          .getSubmissionContext().getAMContainerSpec().getApplicationACLs());
      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Setting client token master key");
        response.setClientToAMTokenMasterKey(java.nio.ByteBuffer.wrap(rmContext
            .getClientToAMTokenSecretManager()
            .getMasterKey(applicationAttemptId).getEncoded()));        
      }
      return response;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {

    ApplicationAttemptId applicationAttemptId = authorizeRequest();

    AllocateResponse lastResponse = responseMap.get(applicationAttemptId);
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

      if (rmContext.getRMApps().get(applicationAttemptId.getApplicationId())
          .isAppSafeToUnregister()) {
        return FinishApplicationMasterResponse.newInstance(true);
      } else {
        return FinishApplicationMasterResponse.newInstance(false);
      }
    }
  }

  /**
   * @param appAttemptId
   * @return true if application is registered for the respective attemptid
   */
  public boolean hasApplicationMasterRegistered(
      ApplicationAttemptId appAttemptId) {
    boolean hasApplicationMasterRegistered = false;
    AllocateResponse lastResponse = responseMap.get(appAttemptId);
    if (lastResponse != null) {
      synchronized (lastResponse) {
        if (lastResponse.getResponseId() >= 0) {
          hasApplicationMasterRegistered = true;
        }
      }
    }
    return hasApplicationMasterRegistered;
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {

    ApplicationAttemptId appAttemptId = authorizeRequest();

    this.amLivelinessMonitor.receivedPing(appAttemptId);

    /* check if its in cache */
    AllocateResponse lastResponse = responseMap.get(appAttemptId);
    if (lastResponse == null) {
      LOG.error("AppAttemptId doesnt exist in cache " + appAttemptId);
      return resync;
    }
    
    if (!hasApplicationMasterRegistered(appAttemptId)) {
      String message =
          "Application Master is trying to allocate before registering for: "
              + appAttemptId.getApplicationId();
      LOG.error(message);
      RMAuditLogger.logFailure(
        this.rmContext.getRMApps().get(appAttemptId.getApplicationId())
          .getUser(), AuditConstants.REGISTER_AM, "",
        "ApplicationMasterService", message, appAttemptId.getApplicationId(),
        appAttemptId);
      throw new InvalidApplicationMasterRequestException(message);
    }

    if ((request.getResponseId() + 1) == lastResponse.getResponseId()) {
      /* old heartbeat */
      return lastResponse;
    } else if (request.getResponseId() + 1 < lastResponse.getResponseId()) {
      LOG.error("Invalid responseid from appAttemptId " + appAttemptId);
      // Oh damn! Sending reboot isn't enough. RM state is corrupted. TODO:
      // Reboot is not useful since after AM reboots, it will send register and 
      // get an exception. Might as well throw an exception here.
      return resync;
    } 
    
    // Allow only one thread in AM to do heartbeat at a time.
    synchronized (lastResponse) {

      // Send the status update to the appAttempt.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptStatusupdateEvent(appAttemptId, request
              .getProgress()));

      List<ResourceRequest> ask = request.getAskList();
      List<ContainerId> release = request.getReleaseList();
      
      ResourceBlacklistRequest blacklistRequest = request.getResourceBlacklistRequest();
      List<String> blacklistAdditions = 
          (blacklistRequest != null) ? 
              blacklistRequest.getBlacklistAdditions() : null;
      List<String> blacklistRemovals = 
          (blacklistRequest != null) ? 
              blacklistRequest.getBlacklistRemovals() : null;
      
      // sanity check
      try {
        RMServerUtils.validateResourceRequests(ask,
            rScheduler.getMaximumResourceCapability());
      } catch (InvalidResourceRequestException e) {
        LOG.warn("Invalid resource ask by application " + appAttemptId, e);
        throw e;
      }
      
      try {
        RMServerUtils.validateBlacklistRequest(blacklistRequest);
      }  catch (InvalidResourceBlacklistRequestException e) {
        LOG.warn("Invalid blacklist request by application " + appAttemptId, e);
        throw e;
      }
      
      try {
        RMServerUtils.validateContainerReleaseRequest(release, appAttemptId);
      } catch (InvalidContainerReleaseException e) {
        LOG.warn("Invalid container release by application " + appAttemptId, e);
        throw e;
      }
      
      // Send new requests to appAttempt.
      Allocation allocation =
          this.rScheduler.allocate(appAttemptId, ask, release, 
              blacklistAdditions, blacklistRemovals);

      RMApp app = this.rmContext.getRMApps().get(
          appAttemptId.getApplicationId());
      RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);
      
      AllocateResponse allocateResponse =
          recordFactory.newRecordInstance(AllocateResponse.class);

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
              rmNode.getHealthReport(),
              rmNode.getLastHealthReportTime());
          
          updatedNodeReports.add(report);
        }
        allocateResponse.setUpdatedNodes(updatedNodeReports);
      }

      allocateResponse.setAllocatedContainers(allocation.getContainers());
      allocateResponse.setCompletedContainersStatuses(appAttempt
          .pullJustFinishedContainers());
      allocateResponse.setResponseId(lastResponse.getResponseId() + 1);
      allocateResponse.setAvailableResources(allocation.getResourceLimit());
      
      allocateResponse.setNumClusterNodes(this.rScheduler.getNumClusterNodes());
   
      // add preemption to the allocateResponse message (if any)
      allocateResponse.setPreemptionMessage(generatePreemptionMessage(allocation));

      // Adding NMTokens for allocated containers.
      if (!allocation.getContainers().isEmpty()) {
        allocateResponse.setNMTokens(rmContext.getNMTokenSecretManager()
            .createAndGetNMTokens(app.getUser(), appAttemptId,
                allocation.getContainers()));
      }

      // before returning response, verify in sync
      AllocateResponse oldResponse =
          responseMap.put(appAttemptId, allocateResponse);
      if (oldResponse == null) {
        // appAttempt got unregistered, remove it back out
        responseMap.remove(appAttemptId);
        String message = "App Attempt removed from the cache during allocate"
            + appAttemptId;
        LOG.error(message);
        return resync;
      }

      return allocateResponse;
    }
  }
  
  private PreemptionMessage generatePreemptionMessage(Allocation allocation){
    PreemptionMessage pMsg = null;
    // assemble strict preemption request
    if (allocation.getStrictContainerPreemptions() != null) {
       pMsg =
        recordFactory.newRecordInstance(PreemptionMessage.class);
      StrictPreemptionContract pStrict =
          recordFactory.newRecordInstance(StrictPreemptionContract.class);
      Set<PreemptionContainer> pCont = new HashSet<PreemptionContainer>();
      for (ContainerId cId : allocation.getStrictContainerPreemptions()) {
        PreemptionContainer pc =
            recordFactory.newRecordInstance(PreemptionContainer.class);
        pc.setId(cId);
        pCont.add(pc);
      }
      pStrict.setContainers(pCont);
      pMsg.setStrictContract(pStrict);
    }

    // assemble negotiable preemption request
    if (allocation.getResourcePreemptions() != null &&
        allocation.getResourcePreemptions().size() > 0 &&
        allocation.getContainerPreemptions() != null &&
        allocation.getContainerPreemptions().size() > 0) {
      if (pMsg == null) {
        pMsg =
            recordFactory.newRecordInstance(PreemptionMessage.class);
      }
      PreemptionContract contract =
          recordFactory.newRecordInstance(PreemptionContract.class);
      Set<PreemptionContainer> pCont = new HashSet<PreemptionContainer>();
      for (ContainerId cId : allocation.getContainerPreemptions()) {
        PreemptionContainer pc =
            recordFactory.newRecordInstance(PreemptionContainer.class);
        pc.setId(cId);
        pCont.add(pc);
      }
      List<PreemptionResourceRequest> pRes = new ArrayList<PreemptionResourceRequest>();
      for (ResourceRequest crr : allocation.getResourcePreemptions()) {
        PreemptionResourceRequest prr =
            recordFactory.newRecordInstance(PreemptionResourceRequest.class);
        prr.setResourceRequest(crr);
        pRes.add(prr);
      }
      contract.setContainers(pCont);
      contract.setResourceRequest(pRes);
      pMsg.setContract(contract);
    }
    
    return pMsg;
  }

  public void registerAppAttempt(ApplicationAttemptId attemptId) {
    AllocateResponse response =
        recordFactory.newRecordInstance(AllocateResponse.class);
    // set response id to -1 before application master for the following
    // attemptID get registered
    response.setResponseId(-1);
    LOG.info("Registering app attempt : " + attemptId);
    responseMap.put(attemptId, response);
    rmContext.getNMTokenSecretManager().registerApplicationAttempt(attemptId);
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId) {
    LOG.info("Unregistering app attempt : " + attemptId);
    responseMap.remove(attemptId);
    rmContext.getNMTokenSecretManager().unregisterApplicationAttempt(attemptId);
  }

  public void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    super.serviceStop();
  }
}
