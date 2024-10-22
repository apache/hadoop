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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
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
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.AbstractPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.DisabledPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.PlacementConstraintProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.SchedulerPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService implements
    ApplicationMasterProtocol {
  private static final Logger LOG = LoggerFactory.
      getLogger(ApplicationMasterService.class);

  private final AMLivelinessMonitor amLivelinessMonitor;
  private YarnScheduler rScheduler;
  protected InetSocketAddress masterServiceAddress;
  protected Server server;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final ConcurrentMap<ApplicationAttemptId, AllocateResponseLock> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AllocateResponseLock>();
  private final ConcurrentHashMap<ApplicationAttemptId, Boolean>
      finishedAttemptCache = new ConcurrentHashMap<>();
  protected final RMContext rmContext;
  private final AMSProcessingChain amsProcessingChain;
  private boolean timelineServiceV2Enabled;

  public ApplicationMasterService(RMContext rmContext,
      YarnScheduler scheduler) {
    this(ApplicationMasterService.class.getName(), rmContext, scheduler);
  }

  public ApplicationMasterService(String name, RMContext rmContext,
      YarnScheduler scheduler) {
    super(name);
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.rmContext = rmContext;
    this.amsProcessingChain = new AMSProcessingChain(new DefaultAMSProcessor());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    initializeProcessingChain(conf);
  }

  private void addPlacementConstraintHandler(Configuration conf) {
    String placementConstraintsHandler =
        conf.get(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
            YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    if (placementConstraintsHandler
        .equals(YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used, all scheduling requests will "
          + "be rejected.");
      amsProcessingChain.addProcessor(new DisabledPlacementProcessor());
    } else if (placementConstraintsHandler
        .equals(YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used. Scheduling requests will be "
          + "handled by the placement constraint processor");
      amsProcessingChain.addProcessor(new PlacementConstraintProcessor());
    } else if (placementConstraintsHandler
        .equals(YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used. Scheduling requests will be "
          + "handled by the main scheduler.");
      amsProcessingChain.addProcessor(new SchedulerPlacementProcessor());
    }
  }

  private void initializeProcessingChain(Configuration conf) {
    amsProcessingChain.init(rmContext, null);
    addPlacementConstraintHandler(conf);

    List<ApplicationMasterServiceProcessor> processors = getProcessorList(conf);
    if (processors != null) {
      Collections.reverse(processors);
      for (ApplicationMasterServiceProcessor p : processors) {
        // Ensure only single instance of PlacementProcessor is included
        if (p instanceof AbstractPlacementProcessor) {
          LOG.warn("Found PlacementProcessor=" + p.getClass().getCanonicalName()
              + " defined in "
              + YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS
              + ", however PlacementProcessor handler should be configured "
              + "by using " + YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER
              + ", this processor will be ignored.");
          continue;
        }
        this.amsProcessingChain.addProcessor(p);
      }
    }
  }

  protected List<ApplicationMasterServiceProcessor> getProcessorList(
      Configuration conf) {
    return conf.getInstances(
        YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS,
        ApplicationMasterServiceProcessor.class);
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
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(conf);

    super.serviceStart();
  }

  protected Server getServer(YarnRPC rpc, Configuration serverConf,
      InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
    return rpc.getServer(ApplicationMasterProtocol.class, this, addr,
        serverConf, secretManager,
        serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
  }

  protected AMSProcessingChain getProcessingChain() {
    return this.amsProcessingChain;
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
        // allow UAM re-register if work preservation is enabled
        ApplicationSubmissionContext appContext =
            rmContext.getRMApps().get(appID).getApplicationSubmissionContext();
        if (!(appContext.getUnmanagedAM()
            && appContext.getKeepContainersAcrossApplicationAttempts())) {
          String message =
              AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE + appID;
          LOG.warn(message);
          RMAuditLogger.logFailure(
              this.rmContext.getRMApps().get(appID).getUser(),
              AuditConstants.REGISTER_AM, "", "ApplicationMasterService",
              message, appID, applicationAttemptId);
          throw new InvalidApplicationMasterRequestException(message);
        }
      }

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      // Setting the response id to 0 to identify if the
      // application master is register for the respective attemptid
      lastResponse.setResponseId(0);
      lock.setAllocateResponse(lastResponse);

      RegisterApplicationMasterResponse response =
          recordFactory.newRecordInstance(
              RegisterApplicationMasterResponse.class);
      this.amsProcessingChain.registerApplicationMaster(
          amrmTokenIdentifier.getApplicationAttemptId(), request, response);
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
    if (timelineServiceV2Enabled) {
      ((RMAppImpl) rmApp).removeCollectorData();
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

      FinishApplicationMasterResponse response =
          FinishApplicationMasterResponse.newInstance(false);
      if (finishedAttemptCache.putIfAbsent(applicationAttemptId, true)
          == null) {
        this.amsProcessingChain
            .finishApplicationMaster(applicationAttemptId, request, response);
      }
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);
      return response;
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

  private final static List<Container> EMPTY_CONTAINER_LIST =
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
    RMAppAttemptMetrics rmMetrics = getAppAttemptMetrics(appAttemptId);
    // we do this here to prevent the internal lock in allocate()
    if (rmMetrics != null) {
      rmMetrics.setAllocateLatenciesTimestamps(request.getAskList());
    }
    AllocateResponse response = allocate(request, amrmTokenIdentifier);
    if (rmMetrics != null) {
      rmMetrics.updateAllocateLatencies(response.getAllocatedContainers());
    }
    return response;
  }

  protected AllocateResponse allocate(AllocateRequest request,
      AMRMTokenIdentifier amrmTokenIdentifier)
      throws YarnException, IOException {
    ApplicationAttemptId appAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();
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
            "AM is not registered for known application attempt: "
                + appAttemptId
                + " or RM had restarted after AM registered. "
                + " AM should re-register.";
        throw new ApplicationMasterNotRegisteredException(message);
      }

      // Normally request.getResponseId() == lastResponse.getResponseId()
      if (AMRMClientUtils.getNextResponseId(
          request.getResponseId()) == lastResponse.getResponseId()) {
        // heartbeat one step old, simply return lastReponse
        return lastResponse;
      } else if (request.getResponseId() != lastResponse.getResponseId()) {
        throw new InvalidApplicationMasterRequestException(AMRMClientUtils
            .assembleInvalidResponseIdExceptionMessage(appAttemptId,
                lastResponse.getResponseId(), request.getResponseId()));
      }

      AllocateResponse response =
          recordFactory.newRecordInstance(AllocateResponse.class);
      this.amsProcessingChain.allocate(
          amrmTokenIdentifier.getApplicationAttemptId(), request, response);

      // update AMRMToken if the token is rolled-up
      MasterKeyData nextMasterKey =
          this.rmContext.getAMRMTokenSecretManager().getNextMasterKeyData();

      if (nextMasterKey != null
          && nextMasterKey.getMasterKey().getKeyId() != amrmTokenIdentifier
          .getKeyId()) {
        RMApp app =
            this.rmContext.getRMApps().get(appAttemptId.getApplicationId());
        RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);
        RMAppAttemptImpl appAttemptImpl = (RMAppAttemptImpl)appAttempt;
        Token<AMRMTokenIdentifier> amrmToken = appAttempt.getAMRMToken();
        if (nextMasterKey.getMasterKey().getKeyId() !=
            appAttemptImpl.getAMRMTokenKeyId()) {
          LOG.info("The AMRMToken has been rolled-over. Send new AMRMToken back"
              + " to application: " + appAttemptId.getApplicationId());
          amrmToken = rmContext.getAMRMTokenSecretManager()
              .createAndGetAMRMToken(appAttemptId);
          appAttemptImpl.setAMRMToken(amrmToken);
        }
        response.setAMRMToken(org.apache.hadoop.yarn.api.records.Token
            .newInstance(amrmToken.getIdentifier(), amrmToken.getKind()
                .toString(), amrmToken.getPassword(), amrmToken.getService()
                .toString()));
      }

      /*
       * As we are updating the response inside the lock object so we don't
       * need to worry about unregister call occurring in between (which
       * removes the lock object).
       */
      response.setResponseId(
          AMRMClientUtils.getNextResponseId(lastResponse.getResponseId()));
      lock.setAllocateResponse(response);
      return response;
    }
  }

  protected RMAppAttemptMetrics getAppAttemptMetrics(
      ApplicationAttemptId appAttemptId) {
    if (appAttemptId == null) {
      return null;
    }
    ConcurrentMap<ApplicationId, RMApp>  apps = this.rmContext.getRMApps();
    ApplicationId appId = appAttemptId.getApplicationId();
    if (appId == null) {
      return null;
    }
    RMApp app = apps.get(appId);
    if (app == null) {
      return null;
    }

    Map<ApplicationAttemptId, RMAppAttempt> attempts = app.getAppAttempts();
    if (attempts == null) {
      return null;
    } else {
      RMAppAttempt attempt = attempts.get(appAttemptId);
      if (attempt == null) {
        return null;
      }
      return attempt.getRMAppAttemptMetrics();
    }
  }

  public void registerAppAttempt(ApplicationAttemptId attemptId) {
    AllocateResponse response =
        recordFactory.newRecordInstance(AllocateResponse.class);
    // set response id to -1 before application master for the following
    // attemptID get registered
    response.setResponseId(AMRMClientUtils.PRE_REGISTER_RESPONSE_ID);
    LOG.info("Registering app attempt : " + attemptId);
    responseMap.put(attemptId, new AllocateResponseLock(response));
    rmContext.getNMTokenSecretManager().registerApplicationAttempt(attemptId);
  }

  @VisibleForTesting
  protected boolean setAttemptLastResponseId(ApplicationAttemptId attemptId,
      int lastResponseId) {
    AllocateResponseLock lock = responseMap.get(attemptId);
    if (lock == null || lock.getAllocateResponse() == null) {
      return false;
    }
    lock.getAllocateResponse().setResponseId(lastResponseId);
    return true;
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId) {
    LOG.info("Unregistering app attempt : " + attemptId);
    responseMap.remove(attemptId);
    finishedAttemptCache.remove(attemptId);
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
    responseMap.clear();
    finishedAttemptCache.clear();
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
