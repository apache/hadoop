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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;


import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security
    .AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService implements
    ApplicationMasterProtocol {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);
  private final AMLivelinessMonitor amLivelinessMonitor;
  private YarnScheduler rScheduler;
  protected InetSocketAddress masterServiceAddress;
  protected Server server;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final ConcurrentMap<ApplicationAttemptId, AllocateResponseLock> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AllocateResponseLock>();
  protected final RMContext rmContext;

  public ApplicationMasterService(String name, RMContext rmContext,
      YarnScheduler scheduler) {
    super(name);
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.rmContext = rmContext;
  }

  public ApplicationMasterService(RMContext rmContext,
      YarnScheduler scheduler) {
    this(ApplicationMasterService.class.getName(), rmContext, scheduler);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    Configuration serverConf = conf;
    // If the auth is not-simple, enforce it to be token-based.
    serverConf = new Configuration(conf);
    serverConf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
    this.server = getServer(rpc, serverConf, masterServiceAddress,
        this.rmContext.getAMRMTokenSecretManager());
    // TODO more exceptions could be added later.
    this.server.addTerseExceptions(
        ApplicationMasterNotRegisteredException.class);

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();
    this.masterServiceAddress =
        conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                               YarnConfiguration.RM_SCHEDULER_ADDRESS,
                               YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                               server.getListenerAddress());
    super.serviceStart();
  }

  protected Server getServer(YarnRPC rpc, Configuration serverConf,
      InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
    return rpc.getServer(ApplicationMasterProtocol.class, this, addr,
        serverConf, secretManager,
        serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.masterServiceAddress;
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {

    AMRMTokenIdentifier amrmTokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();
    ApplicationAttemptId applicationAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();

    ApplicationId appID = applicationAttemptId.getApplicationId();
    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      RMAuditLogger.logFailure(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM, "Application doesn't exist in cache "
              + applicationAttemptId, "ApplicationMasterService",
          "Error in registering application master", appID,
          applicationAttemptId);
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do registerApp at a time.
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is already registered : "
                + appID;
        LOG.warn(message);
        RMAuditLogger.logFailure(
          this.rmContext.getRMApps()
            .get(appID).getUser(),
          AuditConstants.REGISTER_AM, "", "ApplicationMasterService", message,
          appID, applicationAttemptId);
        throw new InvalidApplicationMasterRequestException(message);
      }
      
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);
      RMApp app = this.rmContext.getRMApps().get(appID);
      
      // Setting the response id to 0 to identify if the
      // application master is register for the respective attemptid
      lastResponse.setResponseId(0);
      lock.setAllocateResponse(lastResponse);
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
          .getMaximumResourceCapability(app.getQueue()));
      response.setApplicationACLs(app.getRMAppAttempt(applicationAttemptId)
          .getSubmissionContext().getAMContainerSpec().getApplicationACLs());
      response.setQueue(app.getQueue());
      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Setting client token master key");
        response.setClientToAMTokenMasterKey(java.nio.ByteBuffer.wrap(rmContext
            .getClientToAMTokenSecretManager()
            .getMasterKey(applicationAttemptId).getEncoded()));        
      }

      // For work-preserving AM restart, retrieve previous attempts' containers
      // and corresponding NM tokens.
      if (app.getApplicationSubmissionContext()
          .getKeepContainersAcrossApplicationAttempts()) {
        List<Container> transferredContainers = rScheduler
            .getTransferredContainers(applicationAttemptId);
        if (!transferredContainers.isEmpty()) {
          response.setContainersFromPreviousAttempts(transferredContainers);
          List<NMToken> nmTokens = new ArrayList<NMToken>();
          for (Container container : transferredContainers) {
            try {
              NMToken token = rmContext.getNMTokenSecretManager()
                  .createAndGetNMToken(app.getUser(), applicationAttemptId,
                      container);
              if (null != token) {
                nmTokens.add(token);
              }
            } catch (IllegalArgumentException e) {
              // if it's a DNS issue, throw UnknowHostException directly and
              // that
              // will be automatically retried by RMProxy in RPC layer.
              if (e.getCause() instanceof UnknownHostException) {
                throw (UnknownHostException) e.getCause();
              }
            }
          }
          response.setNMTokensFromPreviousAttempts(nmTokens);
          LOG.info("Application " + appID + " retrieved "
              + transferredContainers.size() + " containers from previous"
              + " attempts and " + nmTokens.size() + " NM tokens.");
        }
      }

      response.setSchedulerResourceTypes(rScheduler
        .getSchedulingResourceTypes());

      return response;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {

    ApplicationAttemptId applicationAttemptId =
        YarnServerSecurityUtils.authorizeRequest().getApplicationAttemptId();
    ApplicationId appId = applicationAttemptId.getApplicationId();

    RMApp rmApp =
        rmContext.getRMApps().get(applicationAttemptId.getApplicationId());

    // Remove collector address when app get finished.
    if (YarnConfiguration.timelineServiceV2Enabled(getConfig())) {
      rmApp.removeCollectorAddr();
    }
    // checking whether the app exits in RMStateStore at first not to throw
    // ApplicationDoesNotExistInCacheException before and after
    // RM work-preserving restart.
    if (rmApp.isAppFinalStateStored()) {
      LOG.info(rmApp.getApplicationId() + " unregistered successfully. ");
      return FinishApplicationMasterResponse.newInstance(true);
    }

    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do finishApp at a time.
    synchronized (lock) {
      if (!hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is trying to unregister before registering for: "
                + appId;
        LOG.error(message);
        RMAuditLogger.logFailure(
            this.rmContext.getRMApps()
                .get(appId).getUser(),
            AuditConstants.UNREGISTER_AM, "", "ApplicationMasterService",
            message, appId,
            applicationAttemptId);
        throw new ApplicationMasterNotRegisteredException(message);
      }

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptUnregistrationEvent(applicationAttemptId, request
              .getTrackingUrl(), request.getFinalApplicationStatus(), request
              .getDiagnostics()));

      // For UnmanagedAMs, return true so they don't retry
      return FinishApplicationMasterResponse.newInstance(
          rmApp.getApplicationSubmissionContext().getUnmanagedAM());
    }
  }

  private void throwApplicationDoesNotExistInCacheException(
      ApplicationAttemptId appAttemptId)
      throws InvalidApplicationMasterRequestException {
    String message = "Application doesn't exist in cache "
        + appAttemptId;
    LOG.error(message);
    throw new InvalidApplicationMasterRequestException(message);
  }
  
  /**
   * @param appAttemptId
   * @return true if application is registered for the respective attemptid
   */
  public boolean hasApplicationMasterRegistered(
      ApplicationAttemptId appAttemptId) {
    boolean hasApplicationMasterRegistered = false;
    AllocateResponseLock lastResponse = responseMap.get(appAttemptId);
    if (lastResponse != null) {
      synchronized (lastResponse) {
        if (lastResponse.getAllocateResponse() != null
            && lastResponse.getAllocateResponse().getResponseId() >= 0) {
          hasApplicationMasterRegistered = true;
        }
      }
    }
    return hasApplicationMasterRegistered;
  }

  protected final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
      EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {

    AMRMTokenIdentifier amrmTokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();

    ApplicationAttemptId appAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();
    ApplicationId applicationId = appAttemptId.getApplicationId();

    this.amLivelinessMonitor.receivedPing(appAttemptId);

    /* check if its in cache */
    AllocateResponseLock lock = responseMap.get(appAttemptId);
    if (lock == null) {
      String message =
          "Application attempt " + appAttemptId
              + " doesn't exist in ApplicationMasterService cache.";
      LOG.error(message);
      throw new ApplicationAttemptNotFoundException(message);
    }
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (!hasApplicationMasterRegistered(appAttemptId)) {
        String message =
            "AM is not registered for known application attempt: " + appAttemptId
                + " or RM had restarted after AM registered . AM should re-register.";
        throw new ApplicationMasterNotRegisteredException(message);
      }

      if ((request.getResponseId() + 1) == lastResponse.getResponseId()) {
        /* old heartbeat */
        return lastResponse;
      } else if (request.getResponseId() + 1 < lastResponse.getResponseId()) {
        String message =
            "Invalid responseId in AllocateRequest from application attempt: "
                + appAttemptId + ", expect responseId to be "
                + (lastResponse.getResponseId() + 1);
        throw new InvalidApplicationMasterRequestException(message);
      }

      //filter illegal progress values
      float filteredProgress = request.getProgress();
      if (Float.isNaN(filteredProgress) || filteredProgress == Float.NEGATIVE_INFINITY
        || filteredProgress < 0) {
         request.setProgress(0);
      } else if (filteredProgress > 1 || filteredProgress == Float.POSITIVE_INFINITY) {
        request.setProgress(1);
      }

      // Send the status update to the appAttempt.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptStatusupdateEvent(appAttemptId, request
              .getProgress()));

      List<ResourceRequest> ask = request.getAskList();
      List<ContainerId> release = request.getReleaseList();

      ResourceBlacklistRequest blacklistRequest =
          request.getResourceBlacklistRequest();
      List<String> blacklistAdditions =
          (blacklistRequest != null) ?
              blacklistRequest.getBlacklistAdditions() : Collections.EMPTY_LIST;
      List<String> blacklistRemovals =
          (blacklistRequest != null) ?
              blacklistRequest.getBlacklistRemovals() : Collections.EMPTY_LIST;
      RMApp app =
          this.rmContext.getRMApps().get(applicationId);
      
      // set label expression for Resource Requests if resourceName=ANY 
      ApplicationSubmissionContext asc = app.getApplicationSubmissionContext();
      for (ResourceRequest req : ask) {
        if (null == req.getNodeLabelExpression()
            && ResourceRequest.ANY.equals(req.getResourceName())) {
          req.setNodeLabelExpression(asc.getNodeLabelExpression());
        }
      }
      
      Resource maximumCapacity = rScheduler.getMaximumResourceCapability();
              
      // sanity check
      try {
        RMServerUtils.normalizeAndValidateRequests(ask,
            maximumCapacity, app.getQueue(),
            rScheduler, rmContext);
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

      // In the case of work-preserving AM restart, it's possible for the
      // AM to release containers from the earlier attempt.
      if (!app.getApplicationSubmissionContext()
        .getKeepContainersAcrossApplicationAttempts()) {
        try {
          RMServerUtils.validateContainerReleaseRequest(release, appAttemptId);
        } catch (InvalidContainerReleaseException e) {
          LOG.warn("Invalid container release by application " + appAttemptId,
              e);
          throw e;
        }
      }

      // Split Update Resource Requests into increase and decrease.
      // No Exceptions are thrown here. All update errors are aggregated
      // and returned to the AM.
      List<UpdateContainerRequest> increaseResourceReqs = new ArrayList<>();
      List<UpdateContainerRequest> decreaseResourceReqs = new ArrayList<>();
      List<UpdateContainerError> updateContainerErrors =
          RMServerUtils.validateAndSplitUpdateResourceRequests(rmContext,
              request, maximumCapacity, increaseResourceReqs,
              decreaseResourceReqs);

      // Send new requests to appAttempt.
      Allocation allocation;
      RMAppAttemptState state =
          app.getRMAppAttempt(appAttemptId).getAppAttemptState();
      if (state.equals(RMAppAttemptState.FINAL_SAVING) ||
          state.equals(RMAppAttemptState.FINISHING) ||
          app.isAppFinalStateStored()) {
        LOG.warn(appAttemptId + " is in " + state +
                 " state, ignore container allocate request.");
        allocation = EMPTY_ALLOCATION;
      } else {
        allocation =
            this.rScheduler.allocate(appAttemptId, ask, release,
                blacklistAdditions, blacklistRemovals,
                increaseResourceReqs, decreaseResourceReqs);
      }

      if (!blacklistAdditions.isEmpty() || !blacklistRemovals.isEmpty()) {
        LOG.info("blacklist are updated in Scheduler." +
            "blacklistAdditions: " + blacklistAdditions + ", " +
            "blacklistRemovals: " + blacklistRemovals);
      }
      RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);
      AllocateResponse allocateResponse =
          recordFactory.newRecordInstance(AllocateResponse.class);
      if (!allocation.getContainers().isEmpty()) {
        allocateResponse.setNMTokens(allocation.getNMTokens());
      }

      // Notify the AM of container update errors
      if (!updateContainerErrors.isEmpty()) {
        allocateResponse.setUpdateErrors(updateContainerErrors);
      }
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
          NodeId nodeId = rmNode.getNodeID();
          NodeReport report =
              BuilderUtils.newNodeReport(nodeId, rmNode.getState(),
                  rmNode.getHttpAddress(), rmNode.getRackName(), used,
                  rmNode.getTotalCapability(), numContainers,
                  rmNode.getHealthReport(), rmNode.getLastHealthReportTime(),
                  rmNode.getNodeLabels());

          updatedNodeReports.add(report);
        }
        allocateResponse.setUpdatedNodes(updatedNodeReports);
      }

      allocateResponse.setAllocatedContainers(allocation.getContainers());
      allocateResponse.setCompletedContainersStatuses(appAttempt
          .pullJustFinishedContainers());
      allocateResponse.setResponseId(lastResponse.getResponseId() + 1);
      allocateResponse.setAvailableResources(allocation.getResourceLimit());
      
      // Handling increased/decreased containers
      List<UpdatedContainer> updatedContainers = new ArrayList<>();
      if (allocation.getIncreasedContainers() != null) {
        for (Container c : allocation.getIncreasedContainers()) {
          updatedContainers.add(
              UpdatedContainer.newInstance(
                  ContainerUpdateType.INCREASE_RESOURCE, c));
        }
      }
      if (allocation.getDecreasedContainers() != null) {
        for (Container c : allocation.getDecreasedContainers()) {
          updatedContainers.add(
              UpdatedContainer.newInstance(
                  ContainerUpdateType.DECREASE_RESOURCE, c));
        }
      }

      allocateResponse.setUpdatedContainers(updatedContainers);

      allocateResponse.setNumClusterNodes(this.rScheduler.getNumClusterNodes());

      // add collector address for this application
      if (YarnConfiguration.timelineServiceV2Enabled(getConfig())) {
        allocateResponse.setCollectorAddr(
            this.rmContext.getRMApps().get(applicationId).getCollectorAddr());
      }

      // add preemption to the allocateResponse message (if any)
      allocateResponse
          .setPreemptionMessage(generatePreemptionMessage(allocation));

      // Set application priority
      allocateResponse.setApplicationPriority(app
          .getApplicationSubmissionContext().getPriority());

      // update AMRMToken if the token is rolled-up
      MasterKeyData nextMasterKey =
          this.rmContext.getAMRMTokenSecretManager().getNextMasterKeyData();

      if (nextMasterKey != null
          && nextMasterKey.getMasterKey().getKeyId() != amrmTokenIdentifier
            .getKeyId()) {
        RMAppAttemptImpl appAttemptImpl = (RMAppAttemptImpl)appAttempt;
        Token<AMRMTokenIdentifier> amrmToken = appAttempt.getAMRMToken();
        if (nextMasterKey.getMasterKey().getKeyId() !=
            appAttemptImpl.getAMRMTokenKeyId()) {
          LOG.info("The AMRMToken has been rolled-over. Send new AMRMToken back"
              + " to application: " + applicationId);
          amrmToken = rmContext.getAMRMTokenSecretManager()
              .createAndGetAMRMToken(appAttemptId);
          appAttemptImpl.setAMRMToken(amrmToken);
        }
        allocateResponse.setAMRMToken(org.apache.hadoop.yarn.api.records.Token
          .newInstance(amrmToken.getIdentifier(), amrmToken.getKind()
            .toString(), amrmToken.getPassword(), amrmToken.getService()
            .toString()));
      }

      /*
       * As we are updating the response inside the lock object so we don't
       * need to worry about unregister call occurring in between (which
       * removes the lock object).
       */
      lock.setAllocateResponse(allocateResponse);
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
    responseMap.put(attemptId, new AllocateResponseLock(response));
    rmContext.getNMTokenSecretManager().registerApplicationAttempt(attemptId);
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId) {
    LOG.info("Unregistering app attempt : " + attemptId);
    responseMap.remove(attemptId);
    rmContext.getNMTokenSecretManager().unregisterApplicationAttempt(attemptId);
  }

  public void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    super.serviceStop();
  }
  
  public static class AllocateResponseLock {
    private AllocateResponse response;
    
    public AllocateResponseLock(AllocateResponse response) {
      this.response = response;
    }
    
    public synchronized AllocateResponse getAllocateResponse() {
      return response;
    }
    
    public synchronized void setAllocateResponse(AllocateResponse response) {
      this.response = response;
    }
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }
}
