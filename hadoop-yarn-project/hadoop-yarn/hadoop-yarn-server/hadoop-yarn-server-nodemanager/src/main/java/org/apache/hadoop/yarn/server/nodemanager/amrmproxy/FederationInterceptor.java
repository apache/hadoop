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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProto;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.AMHeartbeatRequestHandler;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationRegistryClient;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.uam.UnmanagedAMPoolManager;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Extends the AbstractRequestInterceptor and provides an implementation for
 * federation of YARN RM and scaling an application across multiple YARN
 * sub-clusters. All the federation specific implementation is encapsulated in
 * this class. This is always the last intercepter in the chain.
 */
public class FederationInterceptor extends AbstractRequestInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationInterceptor.class);

  public static final String NMSS_CLASS_PREFIX = "FederationInterceptor/";

  public static final String NMSS_REG_REQUEST_KEY =
      NMSS_CLASS_PREFIX + "registerRequest";
  public static final String NMSS_REG_RESPONSE_KEY =
      NMSS_CLASS_PREFIX + "registerResponse";

  /**
   * When AMRMProxy HA is enabled, secondary AMRMTokens will be stored in Yarn
   * Registry. Otherwise if NM recovery is enabled, the UAM token are stored in
   * local NMSS instead under this directory name.
   */
  public static final String NMSS_SECONDARY_SC_PREFIX =
      NMSS_CLASS_PREFIX + "secondarySC/";
  public static final String STRING_TO_BYTE_FORMAT = "UTF-8";

  private static final RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  /**
   * From AM's perspective, FederationInterceptor behaves exactly the same as
   * YarnRM (ApplicationMasterService). This is to remember the last heart beat
   * response, used to handle duplicate heart beat and responseId from AM.
   */
  private AllocateResponse lastAllocateResponse;
  private final Object lastAllocateResponseLock = new Object();

  private ApplicationAttemptId attemptId;

  /**
   * The home sub-cluster is the sub-cluster where the AM container is running
   * in.
   */
  private AMRMClientRelayer homeRMRelayer;
  private SubClusterId homeSubClusterId;
  private AMHeartbeatRequestHandler homeHeartbeartHandler;

  /**
   * UAM pool for secondary sub-clusters (ones other than home sub-cluster),
   * using subClusterId as uamId. One UAM is created per sub-cluster RM except
   * the home RM.
   *
   * Creation and register of UAM in secondary sub-clusters happen on-demand,
   * when AMRMProxy policy routes resource request to these sub-clusters for the
   * first time. AM heart beats to them are also handled asynchronously for
   * performance reasons.
   */
  private UnmanagedAMPoolManager uamPool;

  /**
   * The rmProxy relayers for secondary sub-clusters that keep track of all
   * pending requests.
   */
  private Map<String, AMRMClientRelayer> secondaryRelayers;

  /**
   * Stores the AllocateResponses that are received asynchronously from all the
   * sub-cluster resource managers, including home RM, but not merged and
   * returned back to AM yet.
   */
  private Map<SubClusterId, List<AllocateResponse>> asyncResponseSink;

  /**
   * Remembers the last allocate response from all known sub-clusters. This is
   * used together with sub-cluster timeout to assemble entries about
   * cluster-wide info (e.g. AvailableResource, NumClusterNodes) in the allocate
   * response back to AM.
   */
  private Map<SubClusterId, AllocateResponse> lastSCResponse;

  /**
   * The async UAM registration result that is not consumed yet.
   */
  private Map<SubClusterId, RegisterApplicationMasterResponse> uamRegistrations;

  // For unit test synchronization
  private Map<SubClusterId, Future<?>> uamRegisterFutures;

  /** Thread pool used for asynchronous operations. */
  private ExecutorService threadpool;

  /**
   * A flag for work preserving NM restart. If we just recovered, we need to
   * generate an {@link ApplicationMasterNotRegisteredException} exception back
   * to AM (similar to what RM will do after its restart/fail-over) in its next
   * allocate to trigger AM re-register (which we will shield from RM and just
   * return our saved register response) and a full pending requests re-send, so
   * that all the {@link AMRMClientRelayer} will be re-populated with all
   * pending requests.
   *
   * TODO: When split-merge is not idempotent, this can lead to some
   * over-allocation without a full cancel to RM.
   */
  private volatile boolean justRecovered;

  /** if true, allocate will be no-op, skipping actual processing. */
  private volatile boolean finishAMCalled;

  /**
   * Used to keep track of the container Id and the sub cluster RM that created
   * the container, so that we know which sub-cluster to forward later requests
   * about existing containers to.
   */
  private Map<ContainerId, SubClusterId> containerIdToSubClusterIdMap;

  /**
   * The original registration request that was sent by the AM. This instance is
   * reused to register/re-register with all the sub-cluster RMs.
   */
  private RegisterApplicationMasterRequest amRegistrationRequest;

  /**
   * The original registration response returned to AM. This instance is reused
   * for duplicate register request from AM, triggered by timeout between AM and
   * AMRMProxy.
   */
  private RegisterApplicationMasterResponse amRegistrationResponse;

  private FederationStateStoreFacade federationFacade;

  private SubClusterResolver subClusterResolver;

  /**
   * Records the last time a successful heartbeat response received from a known
   * sub-cluster. lastHeartbeatTimeStamp.keySet() should be in sync with
   * uamPool.getAllUAMIds().
   */
  private Map<SubClusterId, Long> lastSCResponseTime;
  private long subClusterTimeOut;

  private long lastAMHeartbeatTime;

  /** The policy used to split requests among sub-clusters. */
  private FederationAMRMProxyPolicy policyInterpreter;

  private FederationRegistryClient registryClient;

  // the maximum wait time for the first async heart beat response
  private long heartbeatMaxWaitTimeMs;

  private MonotonicClock clock = new MonotonicClock();

  /**
   * Creates an instance of the FederationInterceptor class.
   */
  public FederationInterceptor() {
    this.containerIdToSubClusterIdMap = new ConcurrentHashMap<>();
    this.asyncResponseSink = new ConcurrentHashMap<>();
    this.lastSCResponse = new ConcurrentHashMap<>();
    this.uamRegistrations = new ConcurrentHashMap<>();
    this.uamRegisterFutures = new ConcurrentHashMap<>();
    this.threadpool = Executors.newCachedThreadPool();
    this.uamPool = createUnmanagedAMPoolManager(this.threadpool);
    this.secondaryRelayers = new ConcurrentHashMap<>();
    this.amRegistrationRequest = null;
    this.amRegistrationResponse = null;
    this.justRecovered = false;
    this.finishAMCalled = false;
    this.lastSCResponseTime = new ConcurrentHashMap<>();
    this.lastAMHeartbeatTime = this.clock.getTime();
  }

  /**
   * Initializes the instance using specified context.
   */
  @Override
  public void init(AMRMProxyApplicationContext appContext) {
    super.init(appContext);
    LOG.info("Initializing Federation Interceptor");

    // Update the conf if available
    Configuration conf = appContext.getConf();
    if (conf == null) {
      conf = getConf();
    } else {
      setConf(conf);
    }

    // The proxy ugi used to talk to home RM as well as Yarn Registry, loaded
    // with the up-to-date AMRMToken issued by home RM.
    UserGroupInformation appOwner;
    try {
      appOwner = UserGroupInformation.createProxyUser(appContext.getUser(),
          UserGroupInformation.getCurrentUser());
    } catch (Exception ex) {
      throw new YarnRuntimeException(ex);
    }

    if (appContext.getRegistryClient() != null) {
      this.registryClient = new FederationRegistryClient(conf,
          appContext.getRegistryClient(), appOwner);
      // Add all app tokens for Yarn Registry access
      if (appContext.getCredentials() != null) {
        appOwner.addCredentials(appContext.getCredentials());
      }
    }

    this.attemptId = appContext.getApplicationAttemptId();
    ApplicationId appId = this.attemptId.getApplicationId();
    this.homeSubClusterId =
        SubClusterId.newInstance(YarnConfiguration.getClusterId(conf));
    this.homeRMRelayer = new AMRMClientRelayer(createHomeRMProxy(appContext,
        ApplicationMasterProtocol.class, appOwner), appId,
        this.homeSubClusterId.toString());

    this.homeHeartbeartHandler =
        createHomeHeartbeartHandler(conf, appId, this.homeRMRelayer);
    this.homeHeartbeartHandler.setUGI(appOwner);
    this.homeHeartbeartHandler.setDaemon(true);
    this.homeHeartbeartHandler.start();

    // set lastResponseId to -1 before application master registers
    this.lastAllocateResponse =
        RECORD_FACTORY.newRecordInstance(AllocateResponse.class);
    this.lastAllocateResponse
        .setResponseId(AMRMClientUtils.PRE_REGISTER_RESPONSE_ID);

    this.federationFacade = FederationStateStoreFacade.getInstance();
    this.subClusterResolver = this.federationFacade.getSubClusterResolver();

    // AMRMProxyPolicy will be initialized in registerApplicationMaster
    this.policyInterpreter = null;

    this.uamPool.init(conf);
    this.uamPool.start();

    this.heartbeatMaxWaitTimeMs =
        conf.getLong(YarnConfiguration.FEDERATION_AMRMPROXY_HB_MAX_WAIT_MS,
            YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_HB_MAX_WAIT_MS);

    this.subClusterTimeOut =
        conf.getLong(YarnConfiguration.FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT,
            YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT);
    if (this.subClusterTimeOut <= 0) {
      LOG.info(
          "{} configured to be {}, should be positive. Using default of {}.",
          YarnConfiguration.FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT,
          this.subClusterTimeOut,
          YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT);
      this.subClusterTimeOut =
          YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT;
    }
  }

  @Override
  public void recover(Map<String, byte[]> recoveredDataMap) {
    super.recover(recoveredDataMap);
    LOG.info("Recovering data for FederationInterceptor for {}",
        this.attemptId);
    this.justRecovered = true;

    if (recoveredDataMap == null) {
      return;
    }
    try {
      if (recoveredDataMap.containsKey(NMSS_REG_REQUEST_KEY)) {
        RegisterApplicationMasterRequestProto pb =
            RegisterApplicationMasterRequestProto
                .parseFrom(recoveredDataMap.get(NMSS_REG_REQUEST_KEY));
        this.amRegistrationRequest =
            new RegisterApplicationMasterRequestPBImpl(pb);
        LOG.info("amRegistrationRequest recovered for {}", this.attemptId);

        // Give the register request to homeRMRelayer for future re-registration
        this.homeRMRelayer.setAMRegistrationRequest(this.amRegistrationRequest);
      }
      if (recoveredDataMap.containsKey(NMSS_REG_RESPONSE_KEY)) {
        RegisterApplicationMasterResponseProto pb =
            RegisterApplicationMasterResponseProto
                .parseFrom(recoveredDataMap.get(NMSS_REG_RESPONSE_KEY));
        this.amRegistrationResponse =
            new RegisterApplicationMasterResponsePBImpl(pb);
        LOG.info("amRegistrationResponse recovered for {}", this.attemptId);
      }

      // Recover UAM amrmTokens from registry or NMSS
      Map<String, Token<AMRMTokenIdentifier>> uamMap;
      if (this.registryClient != null) {
        uamMap = this.registryClient
            .loadStateFromRegistry(this.attemptId.getApplicationId());
        LOG.info("Found {} existing UAMs for application {} in Yarn Registry",
            uamMap.size(), this.attemptId.getApplicationId());
      } else {
        uamMap = new HashMap<>();
        for (Entry<String, byte[]> entry : recoveredDataMap.entrySet()) {
          if (entry.getKey().startsWith(NMSS_SECONDARY_SC_PREFIX)) {
            // entry for subClusterId -> UAM amrmToken
            String scId =
                entry.getKey().substring(NMSS_SECONDARY_SC_PREFIX.length());
            Token<AMRMTokenIdentifier> amrmToken = new Token<>();
            amrmToken.decodeFromUrlString(
                new String(entry.getValue(), STRING_TO_BYTE_FORMAT));
            uamMap.put(scId, amrmToken);
            LOG.debug("Recovered UAM in {} from NMSS", scId);
          }
        }
        LOG.info("Found {} existing UAMs for application {} in NMStateStore",
            uamMap.size(), this.attemptId.getApplicationId());
      }

      // Re-attach the UAMs
      int containers = 0;
      for (Map.Entry<String, Token<AMRMTokenIdentifier>> entry : uamMap
          .entrySet()) {
        SubClusterId subClusterId = SubClusterId.newInstance(entry.getKey());

        // Create a config loaded with federation on and subclusterId
        // for each UAM
        YarnConfiguration config = new YarnConfiguration(getConf());
        FederationProxyProviderUtil.updateConfForFederation(config,
            subClusterId.getId());

        try {
          this.uamPool.reAttachUAM(subClusterId.getId(), config,
              this.attemptId.getApplicationId(),
              this.amRegistrationResponse.getQueue(),
              getApplicationContext().getUser(), this.homeSubClusterId.getId(),
              entry.getValue(), subClusterId.toString());

          this.secondaryRelayers.put(subClusterId.getId(),
              this.uamPool.getAMRMClientRelayer(subClusterId.getId()));

          RegisterApplicationMasterResponse response =
              this.uamPool.registerApplicationMaster(subClusterId.getId(),
                  this.amRegistrationRequest);

          // Set sub-cluster to be timed out initially
          lastSCResponseTime.put(subClusterId,
              clock.getTime() - subClusterTimeOut);

          // Running containers from secondary RMs
          for (Container container : response
              .getContainersFromPreviousAttempts()) {
            containerIdToSubClusterIdMap.put(container.getId(), subClusterId);
            containers++;
            LOG.debug("  From subcluster {} running container {}",
                subClusterId, container.getId());
          }
          LOG.info("Recovered {} running containers from UAM in {}",
              response.getContainersFromPreviousAttempts().size(),
              subClusterId);

        } catch (Exception e) {
          LOG.error("Error reattaching UAM to " + subClusterId + " for "
              + this.attemptId, e);
        }
      }

      // Get the running containers from home RM, note that we will also get the
      // AM container itself from here. We don't need it, but no harm to put the
      // map as well.
      UserGroupInformation appSubmitter = UserGroupInformation
          .createRemoteUser(getApplicationContext().getUser());
      ApplicationClientProtocol rmClient =
          createHomeRMProxy(getApplicationContext(),
              ApplicationClientProtocol.class, appSubmitter);

      GetContainersResponse response = rmClient
          .getContainers(GetContainersRequest.newInstance(this.attemptId));
      for (ContainerReport container : response.getContainerList()) {
        containerIdToSubClusterIdMap.put(container.getContainerId(),
            this.homeSubClusterId);
        containers++;
        LOG.debug("  From home RM {} running container {}",
            this.homeSubClusterId, container.getContainerId());
      }
      LOG.info("{} running containers including AM recovered from home RM {}",
          response.getContainerList().size(), this.homeSubClusterId);

      LOG.info(
          "In all {} UAMs {} running containers including AM recovered for {}",
          uamMap.size(), containers, this.attemptId);

      if (this.amRegistrationResponse != null) {
        // Initialize the AMRMProxyPolicy
        String queue = this.amRegistrationResponse.getQueue();
        this.policyInterpreter =
            FederationPolicyUtils.loadAMRMPolicy(queue, this.policyInterpreter,
                getConf(), this.federationFacade, this.homeSubClusterId);
      }
    } catch (IOException | YarnException e) {
      throw new YarnRuntimeException(e);
    }

  }

  /**
   * Sends the application master's registration request to the home RM.
   *
   * Between AM and AMRMProxy, FederationInterceptor modifies the RM behavior,
   * so that when AM registers more than once, it returns the same register
   * success response instead of throwing
   * {@link InvalidApplicationMasterRequestException}. Furthermore, we present
   * to AM as if we are the RM that never fails over (except when AMRMProxy
   * restarts). When actual RM fails over, we always re-register automatically.
   *
   * We did this because FederationInterceptor can receive concurrent register
   * requests from AM because of timeout between AM and AMRMProxy, which is
   * shorter than the timeout + failOver between FederationInterceptor
   * (AMRMProxy) and RM.
   *
   * For the same reason, this method needs to be synchronized.
   */
  @Override
  public synchronized RegisterApplicationMasterResponse
      registerApplicationMaster(RegisterApplicationMasterRequest request)
          throws YarnException, IOException {

    // Reset the heartbeat responseId to zero upon register
    synchronized (this.lastAllocateResponseLock) {
      this.lastAllocateResponse.setResponseId(0);
    }
    this.justRecovered = false;

    // If AM is calling with a different request, complain
    if (this.amRegistrationRequest != null) {
      if (!this.amRegistrationRequest.equals(request)) {
        throw new YarnException("AM should not call "
            + "registerApplicationMaster with a different request body");
      }
    } else {
      // Save the registration request. This will be used for registering with
      // secondary sub-clusters using UAMs, as well as re-register later
      this.amRegistrationRequest = request;
      if (getNMStateStore() != null) {
        try {
          RegisterApplicationMasterRequestPBImpl pb =
              (RegisterApplicationMasterRequestPBImpl)
                  this.amRegistrationRequest;
          getNMStateStore().storeAMRMProxyAppContextEntry(this.attemptId,
              NMSS_REG_REQUEST_KEY, pb.getProto().toByteArray());
        } catch (Exception e) {
          LOG.error("Error storing AMRMProxy application context entry for "
              + this.attemptId, e);
        }
      }
    }

    /*
     * Present to AM as if we are the RM that never fails over. When actual RM
     * fails over, we always re-register automatically.
     *
     * We did this because it is possible for AM to send duplicate register
     * request because of timeout. When it happens, it is fine to simply return
     * the success message. Out of all outstanding register threads, only the
     * last one will still have an unbroken RPC connection and successfully
     * return the response.
     */
    if (this.amRegistrationResponse != null) {
      return this.amRegistrationResponse;
    }

    /*
     * Send a registration request to the home resource manager. Note that here
     * we don't register with other sub-cluster resource managers because that
     * will prevent us from using new sub-clusters that get added while the AM
     * is running and will breaks the elasticity feature. The registration with
     * the other sub-cluster RM will be done lazily as needed later.
     */
    this.amRegistrationResponse =
        this.homeRMRelayer.registerApplicationMaster(request);
    if (this.amRegistrationResponse
        .getContainersFromPreviousAttempts() != null) {
      cacheAllocatedContainers(
          this.amRegistrationResponse.getContainersFromPreviousAttempts(),
          this.homeSubClusterId);
    }

    ApplicationId appId = this.attemptId.getApplicationId();
    reAttachUAMAndMergeRegisterResponse(this.amRegistrationResponse, appId);

    if (getNMStateStore() != null) {
      try {
        RegisterApplicationMasterResponsePBImpl pb =
            (RegisterApplicationMasterResponsePBImpl)
                this.amRegistrationResponse;
        getNMStateStore().storeAMRMProxyAppContextEntry(this.attemptId,
            NMSS_REG_RESPONSE_KEY, pb.getProto().toByteArray());
      } catch (Exception e) {
        LOG.error("Error storing AMRMProxy application context entry for "
            + this.attemptId, e);
      }
    }

    // the queue this application belongs will be used for getting
    // AMRMProxy policy from state store.
    String queue = this.amRegistrationResponse.getQueue();
    if (queue == null) {
      LOG.warn("Received null queue for application " + appId
          + " from home subcluster. Will use default queue name "
          + YarnConfiguration.DEFAULT_QUEUE_NAME
          + " for getting AMRMProxyPolicy");
    } else {
      LOG.info("Application " + appId + " belongs to queue " + queue);
    }

    // Initialize the AMRMProxyPolicy
    try {
      this.policyInterpreter =
          FederationPolicyUtils.loadAMRMPolicy(queue, this.policyInterpreter,
              getConf(), this.federationFacade, this.homeSubClusterId);
    } catch (FederationPolicyInitializationException e) {
      throw new YarnRuntimeException(e);
    }
    return this.amRegistrationResponse;
  }

  /**
   * Sends the heart beats to the home RM and the secondary sub-cluster RMs that
   * are being used by the application.
   */
  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
    Preconditions.checkArgument(this.policyInterpreter != null,
        "Allocate should be called after registerApplicationMaster");
    this.lastAMHeartbeatTime = this.clock.getTime();

    if (this.justRecovered) {
      throw new ApplicationMasterNotRegisteredException(
          "AMRMProxy just restarted and recovered for " + this.attemptId
              + ". AM should re-register and full re-send pending requests.");
    }

    if (this.finishAMCalled) {
      LOG.warn("FinishApplicationMaster already called by {}, skip heartbeat "
          + "processing and return dummy response" + this.attemptId);
      return RECORD_FACTORY.newRecordInstance(AllocateResponse.class);
    }

    // Check responseId and handle duplicate heartbeat exactly same as RM
    synchronized (this.lastAllocateResponseLock) {
      LOG.info("Heartbeat from " + this.attemptId + " with responseId "
          + request.getResponseId() + " when we are expecting "
          + this.lastAllocateResponse.getResponseId());
      // Normally request.getResponseId() == lastResponse.getResponseId()
      if (AMRMClientUtils.getNextResponseId(
          request.getResponseId()) == this.lastAllocateResponse
              .getResponseId()) {
        // heartbeat one step old, simply return lastReponse
        return this.lastAllocateResponse;
      } else if (request.getResponseId() != this.lastAllocateResponse
          .getResponseId()) {
        throw new InvalidApplicationMasterRequestException(
            AMRMClientUtils.assembleInvalidResponseIdExceptionMessage(attemptId,
                this.lastAllocateResponse.getResponseId(),
                request.getResponseId()));
      }
    }

    try {
      // Split the heart beat request into multiple requests, one for each
      // sub-cluster RM that is used by this application.
      Map<SubClusterId, AllocateRequest> requests =
          splitAllocateRequest(request);

      /**
       * Send the requests to the all sub-cluster resource managers. All
       * requests are synchronously triggered but sent asynchronously. Later the
       * responses will be collected and merged.
       */
      sendRequestsToResourceManagers(requests);

      // Wait for the first async response to arrive
      long startTime = this.clock.getTime();
      synchronized (this.asyncResponseSink) {
        try {
          this.asyncResponseSink.wait(this.heartbeatMaxWaitTimeMs);
        } catch (InterruptedException e) {
        }
      }
      long firstResponseTime = this.clock.getTime() - startTime;

      // An extra brief wait for other async heart beats, so that most of their
      // responses can make it back to AM in the same heart beat round trip.
      try {
        Thread.sleep(firstResponseTime);
      } catch (InterruptedException e) {
      }

      // Prepare the response to AM
      AllocateResponse response = generateBaseAllocationResponse();

      // Merge all responses from response sink
      mergeAllocateResponses(response);

      // Merge the containers and NMTokens from the new registrations into
      // the response

      if (!isNullOrEmpty(this.uamRegistrations)) {
        Map<SubClusterId, RegisterApplicationMasterResponse> newRegistrations;
        synchronized (this.uamRegistrations) {
          newRegistrations = new HashMap<>(this.uamRegistrations);
          this.uamRegistrations.clear();
        }
        mergeRegistrationResponses(response, newRegistrations);
      }

      // update the responseId and return the final response to AM
      synchronized (this.lastAllocateResponseLock) {
        response.setResponseId(AMRMClientUtils
            .getNextResponseId(this.lastAllocateResponse.getResponseId()));
        this.lastAllocateResponse = response;
      }
      return response;
    } catch (Throwable ex) {
      LOG.error("Exception encountered while processing heart beat for "
          + this.attemptId, ex);
      throw new YarnException(ex);
    }
  }

  /**
   * Sends the finish application master request to all the resource managers
   * used by the application.
   */
  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {

    this.finishAMCalled = true;

    // TODO: consider adding batchFinishApplicationMaster in UAMPoolManager
    boolean failedToUnRegister = false;
    ExecutorCompletionService<FinishApplicationMasterResponseInfo> compSvc =
        null;

    // Application master is completing operation. Send the finish
    // application master request to all the registered sub-cluster resource
    // managers in parallel, wait for the responses and aggregate the results.
    Set<String> subClusterIds = this.uamPool.getAllUAMIds();
    if (subClusterIds.size() > 0) {
      final FinishApplicationMasterRequest finishRequest = request;
      compSvc =
          new ExecutorCompletionService<FinishApplicationMasterResponseInfo>(
              this.threadpool);

      LOG.info("Sending finish application request to {} sub-cluster RMs",
          subClusterIds.size());
      for (final String subClusterId : subClusterIds) {
        compSvc.submit(new Callable<FinishApplicationMasterResponseInfo>() {
          @Override
          public FinishApplicationMasterResponseInfo call() throws Exception {
            LOG.info("Sending finish application request to RM {}",
                subClusterId);
            FinishApplicationMasterResponse uamResponse = null;
            try {
              uamResponse =
                  uamPool.finishApplicationMaster(subClusterId, finishRequest);

              if (uamResponse.getIsUnregistered()) {
                secondaryRelayers.remove(subClusterId);
                if (getNMStateStore() != null) {
                  getNMStateStore().removeAMRMProxyAppContextEntry(attemptId,
                      NMSS_SECONDARY_SC_PREFIX + subClusterId);
                }
              }
            } catch (Throwable e) {
              LOG.warn("Failed to finish unmanaged application master: "
                  + "RM address: " + subClusterId + " ApplicationId: "
                  + attemptId, e);
            }
            return new FinishApplicationMasterResponseInfo(uamResponse,
                subClusterId);
          }
        });
      }
    }

    // While the finish application request is being processed
    // asynchronously by other sub-cluster resource managers, send the same
    // request to the home resource manager on this thread.
    FinishApplicationMasterResponse homeResponse =
        this.homeRMRelayer.finishApplicationMaster(request);

    // Stop the home heartbeat thread
    this.homeHeartbeartHandler.shutdown();

    if (subClusterIds.size() > 0) {
      // Wait for other sub-cluster resource managers to return the
      // response and merge it with the home response
      LOG.info(
          "Waiting for finish application response from {} sub-cluster RMs",
          subClusterIds.size());
      for (int i = 0; i < subClusterIds.size(); ++i) {
        try {
          Future<FinishApplicationMasterResponseInfo> future = compSvc.take();
          FinishApplicationMasterResponseInfo uamResponse = future.get();
          LOG.debug("Received finish application response from RM: {}",
              uamResponse.getSubClusterId());
          if (uamResponse.getResponse() == null
              || !uamResponse.getResponse().getIsUnregistered()) {
            failedToUnRegister = true;
          }
        } catch (Throwable e) {
          failedToUnRegister = true;
          LOG.warn("Failed to finish unmanaged application master: "
              + " ApplicationId: " + this.attemptId, e);
        }
      }
    }

    if (failedToUnRegister) {
      homeResponse.setIsUnregistered(false);
    } else {
      // Clean up UAMs only when the app finishes successfully, so that no more
      // attempt will be launched.
      this.uamPool.stop();
      if (this.registryClient != null) {
        this.registryClient
            .removeAppFromRegistry(this.attemptId.getApplicationId());
      }
    }
    return homeResponse;
  }

  @Override
  public void setNextInterceptor(RequestInterceptor next) {
    throw new YarnRuntimeException(
        "setNextInterceptor is being called on FederationInterceptor. "
            + "It should always be used as the last interceptor in the chain");
  }

  /**
   * This is called when the application pipeline is being destroyed. We will
   * release all the resources that we are holding in this call.
   */
  @Override
  public void shutdown() {
    LOG.info("Shutting down FederationInterceptor for {}", this.attemptId);

    // Do not stop uamPool service and kill UAMs here because of possible second
    // app attempt
    try {
      this.uamPool.shutDownConnections();
    } catch (YarnException e) {
      LOG.error("Error shutting down all UAM clients without killing them", e);
    }

    if (this.threadpool != null) {
      try {
        this.threadpool.shutdown();
      } catch (Throwable ex) {
      }
      this.threadpool = null;
    }

    // Stop the home heartbeat thread
    this.homeHeartbeartHandler.shutdown();
    this.homeRMRelayer.shutdown();

    super.shutdown();
  }

  /**
   * Only for unit test cleanup.
   */
  @VisibleForTesting
  protected void cleanupRegistry() {
    if (this.registryClient != null) {
      this.registryClient.cleanAllApplications();
    }
  }

  @VisibleForTesting
  protected FederationRegistryClient getRegistryClient() {
    return this.registryClient;
  }

  @VisibleForTesting
  protected ApplicationAttemptId getAttemptId() {
    return this.attemptId;
  }

  @VisibleForTesting
  protected AMHeartbeatRequestHandler getHomeHeartbeartHandler() {
    return this.homeHeartbeartHandler;
  }

  /**
   * Create the UAM pool manager for secondary sub-clsuters. For unit test to
   * override.
   *
   * @param threadPool the thread pool to use
   * @return the UAM pool manager instance
   */
  @VisibleForTesting
  protected UnmanagedAMPoolManager createUnmanagedAMPoolManager(
      ExecutorService threadPool) {
    return new UnmanagedAMPoolManager(threadPool);
  }

  @VisibleForTesting
  protected AMHeartbeatRequestHandler createHomeHeartbeartHandler(
      Configuration conf, ApplicationId appId,
      AMRMClientRelayer rmProxyRelayer) {
    return new AMHeartbeatRequestHandler(conf, appId, rmProxyRelayer);
  }

  /**
   * Create a proxy instance that is used to connect to the Home resource
   * manager.
   *
   * @param appContext AMRMProxyApplicationContext
   * @param protocol the protocol class for the proxy
   * @param user the ugi for the proxy
   * @param <T> the type of the proxy
   * @return the proxy created
   */
  protected <T> T createHomeRMProxy(AMRMProxyApplicationContext appContext,
      Class<T> protocol, UserGroupInformation user) {
    try {
      return FederationProxyProviderUtil.createRMProxy(appContext.getConf(),
          protocol, this.homeSubClusterId, user, appContext.getAMRMToken());
    } catch (Exception ex) {
      throw new YarnRuntimeException(ex);
    }
  }

  private void mergeRegisterResponse(
      RegisterApplicationMasterResponse homeResponse,
      RegisterApplicationMasterResponse otherResponse) {

    if (!isNullOrEmpty(otherResponse.getContainersFromPreviousAttempts())) {
      if (!isNullOrEmpty(homeResponse.getContainersFromPreviousAttempts())) {
        homeResponse.getContainersFromPreviousAttempts()
            .addAll(otherResponse.getContainersFromPreviousAttempts());
      } else {
        homeResponse.setContainersFromPreviousAttempts(
            otherResponse.getContainersFromPreviousAttempts());
      }
    }

    if (!isNullOrEmpty(otherResponse.getNMTokensFromPreviousAttempts())) {
      if (!isNullOrEmpty(homeResponse.getNMTokensFromPreviousAttempts())) {
        homeResponse.getNMTokensFromPreviousAttempts()
            .addAll(otherResponse.getNMTokensFromPreviousAttempts());
      } else {
        homeResponse.setNMTokensFromPreviousAttempts(
            otherResponse.getNMTokensFromPreviousAttempts());
      }
    }
  }

  /**
   * Try re-attach to all existing and running UAMs in secondary sub-clusters
   * launched by previous application attempts if any. All running containers in
   * the UAMs will be combined into the registerResponse. For the first attempt,
   * the registry will be empty for this application and thus no-op here.
   */
  protected void reAttachUAMAndMergeRegisterResponse(
      RegisterApplicationMasterResponse homeResponse,
      final ApplicationId appId) {

    if (this.registryClient == null) {
      // Both AMRMProxy HA and NM work preserving restart is not enabled
      LOG.warn("registryClient is null, skip attaching existing UAM if any");
      return;
    }

    // Load existing running UAMs from the previous attempts from
    // registry, if any
    Map<String, Token<AMRMTokenIdentifier>> uamMap =
        this.registryClient.loadStateFromRegistry(appId);
    if (uamMap.size() == 0) {
      LOG.info("No existing UAM for application {} found in Yarn Registry",
          appId);
      return;
    }
    LOG.info("Found {} existing UAMs for application {} in Yarn Registry. "
        + "Reattaching in parallel", uamMap.size(), appId);

    ExecutorCompletionService<RegisterApplicationMasterResponse>
        completionService = new ExecutorCompletionService<>(this.threadpool);

    for (Entry<String, Token<AMRMTokenIdentifier>> entry : uamMap.entrySet()) {
      final SubClusterId subClusterId =
          SubClusterId.newInstance(entry.getKey());
      final Token<AMRMTokenIdentifier> amrmToken = entry.getValue();

      completionService
          .submit(new Callable<RegisterApplicationMasterResponse>() {
            @Override
            public RegisterApplicationMasterResponse call() throws Exception {
              RegisterApplicationMasterResponse response = null;
              try {
                // Create a config loaded with federation on and subclusterId
                // for each UAM
                YarnConfiguration config = new YarnConfiguration(getConf());
                FederationProxyProviderUtil.updateConfForFederation(config,
                    subClusterId.getId());

                uamPool.reAttachUAM(subClusterId.getId(), config, appId,
                    amRegistrationResponse.getQueue(),
                    getApplicationContext().getUser(), homeSubClusterId.getId(),
                    amrmToken, subClusterId.toString());

                secondaryRelayers.put(subClusterId.getId(),
                    uamPool.getAMRMClientRelayer(subClusterId.getId()));

                response = uamPool.registerApplicationMaster(
                    subClusterId.getId(), amRegistrationRequest);

                // Set sub-cluster to be timed out initially
                lastSCResponseTime.put(subClusterId,
                    clock.getTime() - subClusterTimeOut);

                if (response != null
                    && response.getContainersFromPreviousAttempts() != null) {
                  cacheAllocatedContainers(
                      response.getContainersFromPreviousAttempts(),
                      subClusterId);
                }
                LOG.info("UAM {} reattached for {}", subClusterId, appId);
              } catch (Throwable e) {
                LOG.error(
                    "Reattaching UAM " + subClusterId + " failed for " + appId,
                    e);
              }
              return response;
            }
          });
    }

    // Wait for the re-attach responses
    for (int i = 0; i < uamMap.size(); i++) {
      try {
        Future<RegisterApplicationMasterResponse> future =
            completionService.take();
        RegisterApplicationMasterResponse registerResponse = future.get();
        if (registerResponse != null) {
          LOG.info("Merging register response for {}", appId);
          mergeRegisterResponse(homeResponse, registerResponse);
        }
      } catch (Exception e) {
        LOG.warn("Reattaching UAM failed for ApplicationId: " + appId, e);
      }
    }
  }

  private SubClusterId getSubClusterForNode(String nodeName) {
    SubClusterId subClusterId = null;
    try {
      subClusterId = this.subClusterResolver.getSubClusterForNode(nodeName);
    } catch (YarnException e) {
      LOG.error("Failed to resolve sub-cluster for node " + nodeName
          + ", skipping this node", e);
      return null;
    }
    if (subClusterId == null) {
      LOG.error("Failed to resolve sub-cluster for node {}, skipping this node",
          nodeName);
      return null;
    }
    return subClusterId;
  }

  /**
   * In federation, the heart beat request needs to be sent to all the sub
   * clusters from which the AM has requested containers. This method splits the
   * specified AllocateRequest from the AM and creates a new request for each
   * sub-cluster RM.
   */
  private Map<SubClusterId, AllocateRequest> splitAllocateRequest(
      AllocateRequest request) throws YarnException {
    Map<SubClusterId, AllocateRequest> requestMap =
        new HashMap<SubClusterId, AllocateRequest>();

    // Create heart beat request for home sub-cluster resource manager
    findOrCreateAllocateRequestForSubCluster(this.homeSubClusterId, request,
        requestMap);

    // Create heart beat request instances for all other already registered
    // sub-cluster resource managers
    Set<String> subClusterIds = this.uamPool.getAllUAMIds();
    for (String subClusterId : subClusterIds) {
      findOrCreateAllocateRequestForSubCluster(
          SubClusterId.newInstance(subClusterId), request, requestMap);
    }

    if (!isNullOrEmpty(request.getAskList())) {
      // Ask the federation policy interpreter to split the ask list for
      // sending it to all the sub-cluster resource managers.
      Map<SubClusterId, List<ResourceRequest>> asks =
          splitResourceRequests(request.getAskList());

      // Add the askLists to the corresponding sub-cluster requests.
      for (Entry<SubClusterId, List<ResourceRequest>> entry : asks.entrySet()) {
        AllocateRequest newRequest = findOrCreateAllocateRequestForSubCluster(
            entry.getKey(), request, requestMap);
        newRequest.getAskList().addAll(entry.getValue());
      }
    }

    if (request.getResourceBlacklistRequest() != null) {
      if (!isNullOrEmpty(
          request.getResourceBlacklistRequest().getBlacklistAdditions())) {
        for (String resourceName : request.getResourceBlacklistRequest()
            .getBlacklistAdditions()) {
          SubClusterId subClusterId = getSubClusterForNode(resourceName);
          if (subClusterId != null) {
            AllocateRequest newRequest =
                findOrCreateAllocateRequestForSubCluster(subClusterId, request,
                    requestMap);
            newRequest.getResourceBlacklistRequest().getBlacklistAdditions()
                .add(resourceName);
          }
        }
      }
      if (!isNullOrEmpty(
          request.getResourceBlacklistRequest().getBlacklistRemovals())) {
        for (String resourceName : request.getResourceBlacklistRequest()
            .getBlacklistRemovals()) {
          SubClusterId subClusterId = getSubClusterForNode(resourceName);
          if (subClusterId != null) {
            AllocateRequest newRequest =
                findOrCreateAllocateRequestForSubCluster(subClusterId, request,
                    requestMap);
            newRequest.getResourceBlacklistRequest().getBlacklistRemovals()
                .add(resourceName);
          }
        }
      }
    }

    if (!isNullOrEmpty(request.getReleaseList())) {
      for (ContainerId cid : request.getReleaseList()) {
        if (warnIfNotExists(cid, "release")) {
          SubClusterId subClusterId =
              this.containerIdToSubClusterIdMap.get(cid);
          AllocateRequest newRequest = requestMap.get(subClusterId);
          newRequest.getReleaseList().add(cid);
        }
      }
    }

    if (!isNullOrEmpty(request.getUpdateRequests())) {
      for (UpdateContainerRequest ucr : request.getUpdateRequests()) {
        if (warnIfNotExists(ucr.getContainerId(), "update")) {
          SubClusterId subClusterId =
              this.containerIdToSubClusterIdMap.get(ucr.getContainerId());
          AllocateRequest newRequest = requestMap.get(subClusterId);
          newRequest.getUpdateRequests().add(ucr);
        }
      }
    }

    return requestMap;
  }

  /**
   * This methods sends the specified AllocateRequests to the appropriate
   * sub-cluster resource managers asynchronously.
   *
   * @param requests contains the heart beat requests to send to the resource
   *          manager keyed by the sub-cluster id
   * @throws YarnException
   * @throws IOException
   */
  private void sendRequestsToResourceManagers(
      Map<SubClusterId, AllocateRequest> requests)
      throws YarnException, IOException {

    // Create new UAM instances for the sub-cluster that we haven't seen before
    List<SubClusterId> newSubClusters =
        registerAndAllocateWithNewSubClusters(requests);

    // Now that all the registrations are done, send the allocation request
    // to the sub-cluster RMs asynchronously and don't wait for the response.
    // The responses will arrive asynchronously and will be added to the
    // response sink, then merged and sent to the application master.
    for (Entry<SubClusterId, AllocateRequest> entry : requests.entrySet()) {
      SubClusterId subClusterId = entry.getKey();
      if (newSubClusters.contains(subClusterId)) {
        // For new sub-clusters, we have already sent the request right after
        // register in the async thread
        continue;
      }

      if (subClusterId.equals(this.homeSubClusterId)) {
        // Request for the home sub-cluster resource manager
        this.homeHeartbeartHandler.allocateAsync(entry.getValue(),
            new HeartbeatCallBack(this.homeSubClusterId, false));
      } else {
        if (!this.uamPool.hasUAMId(subClusterId.getId())) {
          throw new YarnException("UAM not found for " + this.attemptId
              + " in sub-cluster " + subClusterId);
        }
        this.uamPool.allocateAsync(subClusterId.getId(), entry.getValue(),
            new HeartbeatCallBack(subClusterId, true));
      }
    }
  }

  /**
   * This method ensures that Unmanaged AMs are created for newly specified
   * sub-clusters, registers with the corresponding resource managers and send
   * the first allocate request async.
   */
  private List<SubClusterId> registerAndAllocateWithNewSubClusters(
      final Map<SubClusterId, AllocateRequest> requests) throws IOException {

    // Check to see if there are any new sub-clusters in this request
    // list and create and register Unmanaged AM instance for the new ones
    List<SubClusterId> newSubClusters = new ArrayList<>();
    for (SubClusterId subClusterId : requests.keySet()) {
      if (!subClusterId.equals(this.homeSubClusterId)
          && !this.uamPool.hasUAMId(subClusterId.getId())) {
        newSubClusters.add(subClusterId);

        // Set sub-cluster to be timed out initially
        lastSCResponseTime.put(subClusterId,
            clock.getTime() - subClusterTimeOut);
      }
    }

    this.uamRegisterFutures.clear();
    for (final SubClusterId scId : newSubClusters) {
      Future<?> future = this.threadpool.submit(new Runnable() {
        @Override
        public void run() {
          String subClusterId = scId.getId();

          // Create a config loaded with federation on and subclusterId
          // for each UAM
          YarnConfiguration config = new YarnConfiguration(getConf());
          FederationProxyProviderUtil.updateConfForFederation(config,
              subClusterId);

          RegisterApplicationMasterResponse uamResponse = null;
          Token<AMRMTokenIdentifier> token = null;
          try {
            // For appNameSuffix, use subClusterId of the home sub-cluster
            token = uamPool.launchUAM(subClusterId, config,
                attemptId.getApplicationId(), amRegistrationResponse.getQueue(),
                getApplicationContext().getUser(), homeSubClusterId.toString(),
                true, subClusterId);

            secondaryRelayers.put(subClusterId,
                uamPool.getAMRMClientRelayer(subClusterId));

            uamResponse = uamPool.registerApplicationMaster(subClusterId,
                amRegistrationRequest);
          } catch (Throwable e) {
            LOG.error("Failed to register application master: " + subClusterId
                + " Application: " + attemptId, e);
            // TODO: UAM registration for this sub-cluster RM
            // failed. For now, we ignore the resource requests and continue
            // but we need to fix this and handle this situation. One way would
            // be to send the request to another RM by consulting the policy.
            return;
          }
          uamRegistrations.put(scId, uamResponse);
          LOG.info("Successfully registered unmanaged application master: "
              + subClusterId + " ApplicationId: " + attemptId);

          try {
            uamPool.allocateAsync(subClusterId, requests.get(scId),
                new HeartbeatCallBack(scId, true));
          } catch (Throwable e) {
            LOG.error("Failed to allocate async to " + subClusterId
                + " Application: " + attemptId, e);
          }

          // Save the UAM token in registry or NMSS
          try {
            if (registryClient != null) {
              registryClient.writeAMRMTokenForUAM(attemptId.getApplicationId(),
                  subClusterId, token);
            } else if (getNMStateStore() != null) {
              getNMStateStore().storeAMRMProxyAppContextEntry(attemptId,
                  NMSS_SECONDARY_SC_PREFIX + subClusterId,
                  token.encodeToUrlString().getBytes(STRING_TO_BYTE_FORMAT));
            }
          } catch (Throwable e) {
            LOG.error("Failed to persist UAM token from " + subClusterId
                + " Application: " + attemptId, e);
          }
        }
      });
      this.uamRegisterFutures.put(scId, future);
    }
    return newSubClusters;
  }

  /**
   * Prepare the base allocation response. Use lastSCResponse and
   * lastHeartbeatTimeStamp to assemble entries about cluster-wide info, e.g.
   * AvailableResource, NumClusterNodes.
   */
  protected AllocateResponse generateBaseAllocationResponse() {
    AllocateResponse baseResponse =
        RECORD_FACTORY.newRecordInstance(AllocateResponse.class);

    baseResponse.setAvailableResources(Resource.newInstance(0, 0));
    baseResponse.setNumClusterNodes(0);

    Set<SubClusterId> expiredSC = getTimedOutSCs(false);
    for (Entry<SubClusterId, AllocateResponse> entry : lastSCResponse
        .entrySet()) {
      if (expiredSC.contains(entry.getKey())) {
        // Skip expired sub-clusters
        continue;
      }
      AllocateResponse response = entry.getValue();

      if (response.getAvailableResources() != null) {
        baseResponse.setAvailableResources(
            Resources.add(baseResponse.getAvailableResources(),
                response.getAvailableResources()));
      }
      baseResponse.setNumClusterNodes(
          baseResponse.getNumClusterNodes() + response.getNumClusterNodes());
    }
    return baseResponse;
  }

  /**
   * Merge the responses from all sub-clusters that we received asynchronously
   * and keeps track of the containers received from each sub-cluster resource
   * managers.
   */
  private void mergeAllocateResponses(AllocateResponse mergedResponse) {
    synchronized (this.asyncResponseSink) {
      for (Entry<SubClusterId, List<AllocateResponse>> entry :
          this.asyncResponseSink.entrySet()) {
        SubClusterId subClusterId = entry.getKey();
        List<AllocateResponse> responses = entry.getValue();
        if (responses.size() > 0) {
          for (AllocateResponse response : responses) {
            removeFinishedContainersFromCache(
                response.getCompletedContainersStatuses());
            cacheAllocatedContainers(response.getAllocatedContainers(),
                subClusterId);
            mergeAllocateResponse(mergedResponse, response, subClusterId);
          }
          responses.clear();
        }
      }
    }
  }

  /**
   * Removes the finished containers from the local cache.
   */
  private void removeFinishedContainersFromCache(
      List<ContainerStatus> finishedContainers) {
    for (ContainerStatus container : finishedContainers) {
      LOG.debug("Completed container {}", container);
      if (containerIdToSubClusterIdMap
          .containsKey(container.getContainerId())) {
        containerIdToSubClusterIdMap.remove(container.getContainerId());
      }
    }
  }

  /**
   * Helper method for merging the registration responses from the secondary sub
   * cluster RMs into the allocate response to return to the AM.
   */
  private void mergeRegistrationResponses(AllocateResponse homeResponse,
      Map<SubClusterId, RegisterApplicationMasterResponse> registrations) {

    for (Entry<SubClusterId, RegisterApplicationMasterResponse> entry :
        registrations.entrySet()) {
      RegisterApplicationMasterResponse registration = entry.getValue();

      if (!isNullOrEmpty(registration.getContainersFromPreviousAttempts())) {
        List<Container> tempContainers = homeResponse.getAllocatedContainers();
        if (!isNullOrEmpty(tempContainers)) {
          tempContainers
              .addAll(registration.getContainersFromPreviousAttempts());
          homeResponse.setAllocatedContainers(tempContainers);
        } else {
          homeResponse.setAllocatedContainers(
              registration.getContainersFromPreviousAttempts());
        }
        cacheAllocatedContainers(
            registration.getContainersFromPreviousAttempts(), entry.getKey());
      }

      if (!isNullOrEmpty(registration.getNMTokensFromPreviousAttempts())) {
        List<NMToken> tempTokens = homeResponse.getNMTokens();
        if (!isNullOrEmpty(tempTokens)) {
          tempTokens.addAll(registration.getNMTokensFromPreviousAttempts());
          homeResponse.setNMTokens(tempTokens);
        } else {
          homeResponse
              .setNMTokens(registration.getNMTokensFromPreviousAttempts());
        }
      }
    }
  }

  @VisibleForTesting
  protected void mergeAllocateResponse(AllocateResponse homeResponse,
      AllocateResponse otherResponse, SubClusterId otherRMAddress) {

    if (otherResponse.getAMRMToken() != null) {
      // Propagate only the new amrmToken from home sub-cluster back to
      // AMRMProxyService
      if (otherRMAddress.equals(this.homeSubClusterId)) {
        homeResponse.setAMRMToken(otherResponse.getAMRMToken());
      } else {
        throw new YarnRuntimeException(
            "amrmToken from UAM " + otherRMAddress + " should be null here");
      }
    }

    if (!isNullOrEmpty(otherResponse.getAllocatedContainers())) {
      if (!isNullOrEmpty(homeResponse.getAllocatedContainers())) {
        homeResponse.getAllocatedContainers()
            .addAll(otherResponse.getAllocatedContainers());
      } else {
        homeResponse
            .setAllocatedContainers(otherResponse.getAllocatedContainers());
      }
    }

    if (!isNullOrEmpty(otherResponse.getCompletedContainersStatuses())) {
      if (!isNullOrEmpty(homeResponse.getCompletedContainersStatuses())) {
        homeResponse.getCompletedContainersStatuses()
            .addAll(otherResponse.getCompletedContainersStatuses());
      } else {
        homeResponse.setCompletedContainersStatuses(
            otherResponse.getCompletedContainersStatuses());
      }
    }

    if (!isNullOrEmpty(otherResponse.getUpdatedNodes())) {
      if (!isNullOrEmpty(homeResponse.getUpdatedNodes())) {
        homeResponse.getUpdatedNodes().addAll(otherResponse.getUpdatedNodes());
      } else {
        homeResponse.setUpdatedNodes(otherResponse.getUpdatedNodes());
      }
    }

    if (otherResponse.getApplicationPriority() != null) {
      homeResponse.setApplicationPriority(
          otherResponse.getApplicationPriority());
    }

    homeResponse.setNumClusterNodes(
        homeResponse.getNumClusterNodes() + otherResponse.getNumClusterNodes());

    PreemptionMessage homePreempMessage = homeResponse.getPreemptionMessage();
    PreemptionMessage otherPreempMessage = otherResponse.getPreemptionMessage();

    if (homePreempMessage == null && otherPreempMessage != null) {
      homeResponse.setPreemptionMessage(otherPreempMessage);
    }

    if (homePreempMessage != null && otherPreempMessage != null) {
      PreemptionContract par1 = homePreempMessage.getContract();
      PreemptionContract par2 = otherPreempMessage.getContract();

      if (par1 == null && par2 != null) {
        homePreempMessage.setContract(par2);
      }

      if (par1 != null && par2 != null) {
        par1.getResourceRequest().addAll(par2.getResourceRequest());
        par1.getContainers().addAll(par2.getContainers());
      }

      StrictPreemptionContract spar1 = homePreempMessage.getStrictContract();
      StrictPreemptionContract spar2 = otherPreempMessage.getStrictContract();

      if (spar1 == null && spar2 != null) {
        homePreempMessage.setStrictContract(spar2);
      }

      if (spar1 != null && spar2 != null) {
        spar1.getContainers().addAll(spar2.getContainers());
      }
    }

    if (!isNullOrEmpty(otherResponse.getNMTokens())) {
      if (!isNullOrEmpty(homeResponse.getNMTokens())) {
        homeResponse.getNMTokens().addAll(otherResponse.getNMTokens());
      } else {
        homeResponse.setNMTokens(otherResponse.getNMTokens());
      }
    }

    if (!isNullOrEmpty(otherResponse.getUpdatedContainers())) {
      if (!isNullOrEmpty(homeResponse.getUpdatedContainers())) {
        homeResponse.getUpdatedContainers()
            .addAll(otherResponse.getUpdatedContainers());
      } else {
        homeResponse.setUpdatedContainers(otherResponse.getUpdatedContainers());
      }
    }

    if (!isNullOrEmpty(otherResponse.getUpdateErrors())) {
      if (!isNullOrEmpty(homeResponse.getUpdateErrors())) {
        homeResponse.getUpdateErrors().addAll(otherResponse.getUpdateErrors());
      } else {
        homeResponse.setUpdateErrors(otherResponse.getUpdateErrors());
      }
    }
  }

  /**
   * Add allocated containers to cache mapping.
   */
  private void cacheAllocatedContainers(List<Container> containers,
      SubClusterId subClusterId) {
    for (Container container : containers) {
      LOG.debug("Adding container {}", container);

      if (this.containerIdToSubClusterIdMap.containsKey(container.getId())) {
        SubClusterId existingSubClusterId =
            this.containerIdToSubClusterIdMap.get(container.getId());
        if (existingSubClusterId.equals(subClusterId)) {
          /*
           * When RM fails over, the new RM master might send out the same
           * container allocation more than once.
           *
           * It is also possible because of a recent NM restart with NM recovery
           * enabled. We recover running containers from RM. But RM might not
           * notified AM of some of these containers yet. When RM dose notify,
           * we will already have these containers in the map.
           *
           * Either case, just warn and move on.
           */
          LOG.warn(
              "Duplicate containerID: {} found in the allocated containers"
                  + " from same sub-cluster: {}, so ignoring.",
              container.getId(), subClusterId);
        } else {
          // The same container allocation from different sub-clusters,
          // something is wrong.
          // TODO: YARN-6667 if some subcluster RM is configured wrong, we
          // should not fail the entire heartbeat.
          throw new YarnRuntimeException(
              "Duplicate containerID found in the allocated containers. This"
                  + " can happen if the RM epoch is not configured properly."
                  + " ContainerId: " + container.getId().toString()
                  + " ApplicationId: " + this.attemptId + " From RM: "
                  + subClusterId
                  + " . Previous container was from sub-cluster: "
                  + existingSubClusterId);
        }
      }

      this.containerIdToSubClusterIdMap.put(container.getId(), subClusterId);
    }
  }

  /**
   * Check to see if an AllocateRequest exists in the Map for the specified sub
   * cluster. If not found, create a new one, copy the value of responseId and
   * progress from the orignialAMRequest, save it in the specified Map and
   * return the new instance. If found, just return the old instance.
   */
  private static AllocateRequest findOrCreateAllocateRequestForSubCluster(
      SubClusterId subClusterId, AllocateRequest originalAMRequest,
      Map<SubClusterId, AllocateRequest> requestMap) {
    AllocateRequest newRequest = null;
    if (requestMap.containsKey(subClusterId)) {
      newRequest = requestMap.get(subClusterId);
    } else {
      newRequest = createAllocateRequest();
      newRequest.setResponseId(originalAMRequest.getResponseId());
      newRequest.setProgress(originalAMRequest.getProgress());
      requestMap.put(subClusterId, newRequest);
    }
    return newRequest;
  }

  /**
   * Create an empty AllocateRequest instance.
   */
  private static AllocateRequest createAllocateRequest() {
    AllocateRequest request =
        RECORD_FACTORY.newRecordInstance(AllocateRequest.class);
    request.setAskList(new ArrayList<ResourceRequest>());
    request.setReleaseList(new ArrayList<ContainerId>());
    ResourceBlacklistRequest blackList =
        ResourceBlacklistRequest.newInstance(null, null);
    blackList.setBlacklistAdditions(new ArrayList<String>());
    blackList.setBlacklistRemovals(new ArrayList<String>());
    request.setResourceBlacklistRequest(blackList);
    request.setUpdateRequests(new ArrayList<UpdateContainerRequest>());
    return request;
  }

  protected Set<SubClusterId> getTimedOutSCs(boolean verbose) {
    Set<SubClusterId> timedOutSCs = new HashSet<>();
    for (Map.Entry<SubClusterId, Long> entry : this.lastSCResponseTime
        .entrySet()) {
      if (entry.getValue() > this.lastAMHeartbeatTime) {
        // AM haven't heartbeat to us (and thus we to all SCs) for a long time,
        // should not consider the SC as timed out
        continue;
      }
      long duration = this.clock.getTime() - entry.getValue();
      if (duration > this.subClusterTimeOut) {
        if (verbose) {
          LOG.warn(
              "Subcluster {} doesn't have a successful heartbeat"
                  + " for {} seconds for {}",
              entry.getKey(), (double) duration / 1000, this.attemptId);
        }
        timedOutSCs.add(entry.getKey());
      }
    }
    return timedOutSCs;
  }

  /**
   * Check to see if the specified containerId exists in the cache and log an
   * error if not found.
   *
   * @param containerId the container id
   * @param actionName the name of the action
   * @return true if the container exists in the map, false otherwise
   */
  private boolean warnIfNotExists(ContainerId containerId, String actionName) {
    if (!this.containerIdToSubClusterIdMap.containsKey(containerId)) {
      LOG.error(
          "AM is trying to {} a container {} that does not exist. Might happen "
              + "shortly after NM restart when NM recovery is enabled",
          actionName, containerId.toString());
      return false;
    }
    return true;
  }

  /**
   * Splits the specified request to send it to different sub clusters. The
   * splitting algorithm is very simple. If the request does not have a node
   * preference, the policy decides the sub cluster. If the request has a node
   * preference and if locality is required, then it is sent to the sub cluster
   * that contains the requested node. If node preference is specified and
   * locality is not required, then the policy decides the sub cluster.
   *
   * @param askList the ask list to split
   * @return the split asks
   * @throws YarnException if split fails
   */
  protected Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> askList) throws YarnException {
    return policyInterpreter.splitResourceRequests(askList,
        getTimedOutSCs(true));
  }

  @VisibleForTesting
  protected int getUnmanagedAMPoolSize() {
    return this.uamPool.getAllUAMIds().size();
  }

  @VisibleForTesting
  protected UnmanagedAMPoolManager getUnmanagedAMPool() {
    return this.uamPool;
  }

  @VisibleForTesting
  protected Map<SubClusterId, Future<?>> getUamRegisterFutures() {
    return this.uamRegisterFutures;
  }

  @VisibleForTesting
  public Map<SubClusterId, List<AllocateResponse>> getAsyncResponseSink() {
    return this.asyncResponseSink;
  }

  /**
   * Async callback handler for heart beat response from all sub-clusters.
   */
  private class HeartbeatCallBack implements AsyncCallback<AllocateResponse> {
    private SubClusterId subClusterId;
    private boolean isUAM;

    HeartbeatCallBack(SubClusterId subClusterId, boolean isUAM) {
      this.subClusterId = subClusterId;
      this.isUAM = isUAM;
    }

    @Override
    public void callback(AllocateResponse response) {
      synchronized (asyncResponseSink) {
        List<AllocateResponse> responses = null;
        if (asyncResponseSink.containsKey(subClusterId)) {
          responses = asyncResponseSink.get(subClusterId);
        } else {
          responses = new ArrayList<>();
          asyncResponseSink.put(subClusterId, responses);
        }
        responses.add(response);
        // Notify main thread about the response arrival
        asyncResponseSink.notifyAll();
      }
      lastSCResponse.put(subClusterId, response);
      lastSCResponseTime.put(subClusterId, clock.getTime());

      // Notify policy of allocate response
      try {
        policyInterpreter.notifyOfResponse(subClusterId, response);
      } catch (YarnException e) {
        LOG.warn("notifyOfResponse for policy failed for sub-cluster "
            + subClusterId, e);
      }

      // Save the new AMRMToken for the UAM if present
      // Do this last because it can be slow...
      if (this.isUAM && response.getAMRMToken() != null) {
        Token<AMRMTokenIdentifier> newToken = ConverterUtils
            .convertFromYarn(response.getAMRMToken(), (Text) null);
        // Do not further propagate the new amrmToken for UAM
        response.setAMRMToken(null);

        // Update the token in registry or NMSS
        if (registryClient != null) {
          if (registryClient.writeAMRMTokenForUAM(attemptId.getApplicationId(),
              subClusterId.getId(), newToken)) {
            try {
              AMRMTokenIdentifier identifier = new AMRMTokenIdentifier();
              identifier.readFields(new DataInputStream(
                  new ByteArrayInputStream(newToken.getIdentifier())));
              LOG.info(
                  "Received new UAM amrmToken with keyId {} and "
                      + "service {} from {} for {}, written to Registry",
                  identifier.getKeyId(), newToken.getService(), subClusterId,
                  attemptId);
            } catch (IOException e) {
            }
          }
        } else if (getNMStateStore() != null) {
          try {
            getNMStateStore().storeAMRMProxyAppContextEntry(attemptId,
                NMSS_SECONDARY_SC_PREFIX + subClusterId.getId(),
                newToken.encodeToUrlString().getBytes(STRING_TO_BYTE_FORMAT));
          } catch (IOException e) {
            LOG.error("Error storing UAM token as AMRMProxy "
                + "context entry in NMSS for " + attemptId, e);
          }
        }
      }
    }
  }

  /**
   * Private structure for encapsulating SubClusterId and
   * FinishApplicationMasterResponse instances.
   */
  private static class FinishApplicationMasterResponseInfo {
    private FinishApplicationMasterResponse response;
    private String subClusterId;

    FinishApplicationMasterResponseInfo(
        FinishApplicationMasterResponse response, String subClusterId) {
      this.response = response;
      this.subClusterId = subClusterId;
    }

    public FinishApplicationMasterResponse getResponse() {
      return response;
    }

    public String getSubClusterId() {
      return subClusterId;
    }
  }

  /**
   * Utility method to check if the specified Collection is null or empty.
   *
   * @param c the collection object
   * @param <T> element type of the collection
   * @return whether is it is null or empty
   */
  public static <T> boolean isNullOrEmpty(Collection<T> c) {
    return (c == null || c.size() == 0);
  }

  /**
   * Utility method to check if the specified Collection is null or empty.
   *
   * @param c the map object
   * @param <T1> key type of the map
   * @param <T2> value type of the map
   * @return whether is it is null or empty
   */
  public static <T1, T2> boolean isNullOrEmpty(Map<T1, T2> c) {
    return (c == null || c.size() == 0);
  }
}
