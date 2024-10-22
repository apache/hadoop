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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredAMRMProxyState;
import org.apache.hadoop.yarn.server.nodemanager.scheduler.DistributedScheduler;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize
    .NMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Preconditions;

/**
 * AMRMProxyService is a service that runs on each node manager that can be used
 * to intercept and inspect messages from application master to the cluster
 * resource manager. It listens to messages from the application master and
 * creates a request intercepting pipeline instance for each application. The
 * pipeline is a chain of interceptor instances that can inspect and modify the
 * request/response as needed.
 */
public class AMRMProxyService extends CompositeService implements
    ApplicationMasterProtocol {
  private static final Logger LOG = LoggerFactory
      .getLogger(AMRMProxyService.class);

  private static final String NMSS_USER_KEY = "user";
  private static final String NMSS_AMRMTOKEN_KEY = "amrmtoken";

  private final Clock clock = new MonotonicClock();
  private Server server;
  private final Context nmContext;
  private final AsyncDispatcher dispatcher;
  private InetSocketAddress listenerEndpoint;
  private AMRMProxyTokenSecretManager secretManager;
  private Map<ApplicationId, RequestInterceptorChainWrapper> applPipelineMap;
  private RegistryOperations registry;
  private AMRMProxyMetrics metrics;
  private FederationStateStoreFacade federationFacade;
  private boolean federationEnabled = false;

  /**
   * Creates an instance of the service.
   *
   * @param nmContext NM context
   * @param dispatcher NM dispatcher
   */
  public AMRMProxyService(Context nmContext, AsyncDispatcher dispatcher) {
    super(AMRMProxyService.class.getName());
    Preconditions.checkArgument(nmContext != null, "nmContext is null");
    Preconditions.checkArgument(dispatcher != null, "dispatcher is null");
    this.nmContext = nmContext;
    this.dispatcher = dispatcher;
    this.applPipelineMap = new ConcurrentHashMap<>();

    this.dispatcher.register(ApplicationEventType.class, new ApplicationEventHandler());
    metrics = AMRMProxyMetrics.getMetrics();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.secretManager =
        new AMRMProxyTokenSecretManager(this.nmContext.getNMStateStore());
    this.secretManager.init(conf);

    if (conf.getBoolean(YarnConfiguration.AMRM_PROXY_HA_ENABLED,
        YarnConfiguration.DEFAULT_AMRM_PROXY_HA_ENABLED)) {
      this.registry = FederationStateStoreFacade.createInstance(conf,
          YarnConfiguration.YARN_REGISTRY_CLASS,
          YarnConfiguration.DEFAULT_YARN_REGISTRY_CLASS,
          RegistryOperations.class);
      addService(this.registry);
    }
    this.federationFacade = FederationStateStoreFacade.getInstance(conf);
    this.federationEnabled =
        conf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
            YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting AMRMProxyService.");
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation.setConfiguration(conf);

    this.listenerEndpoint =
        conf.getSocketAddr(YarnConfiguration.AMRM_PROXY_ADDRESS,
            YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS,
            YarnConfiguration.DEFAULT_AMRM_PROXY_PORT);

    Configuration serverConf = new Configuration(conf);
    serverConf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());

    int numWorkerThreads =
        serverConf.getInt(
            YarnConfiguration.AMRM_PROXY_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_AMRM_PROXY_CLIENT_THREAD_COUNT);

    this.secretManager.start();

    this.server =
        rpc.getServer(ApplicationMasterProtocol.class, this,
            listenerEndpoint, serverConf, this.secretManager,
            numWorkerThreads);

    if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      this.server.refreshServiceAcl(conf, NMPolicyProvider.getInstance());
    }

    this.server.start();
    LOG.info("AMRMProxyService listening on address: {}.", this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping AMRMProxyService.");
    if (this.server != null) {
      this.server.stop();
    }
    this.secretManager.stop();
    super.serviceStop();
  }

  /**
   * Recover from NM state store. Called after serviceInit before serviceStart.
   *
   * @throws IOException if recover fails
   */
  public void recover() throws IOException {
    LOG.info("Recovering AMRMProxyService.");

    RecoveredAMRMProxyState state =
        this.nmContext.getNMStateStore().loadAMRMProxyState();

    this.secretManager.recover(state);

    LOG.info("Recovering {} running applications for AMRMProxy.",
        state.getAppContexts().size());

    for (Map.Entry<ApplicationAttemptId, Map<String, byte[]>> entry : state
        .getAppContexts().entrySet()) {
      ApplicationAttemptId attemptId = entry.getKey();
      LOG.info("Recovering app attempt {}.", attemptId);
      long startTime = clock.getTime();

      // Try recover for the running application attempt
      try {
        String user = null;
        Token<AMRMTokenIdentifier> amrmToken = null;
        for (Map.Entry<String, byte[]> contextEntry : entry.getValue()
            .entrySet()) {
          if (contextEntry.getKey().equals(NMSS_USER_KEY)) {
            user = new String(contextEntry.getValue(), StandardCharsets.UTF_8);
          } else if (contextEntry.getKey().equals(NMSS_AMRMTOKEN_KEY)) {
            amrmToken = new Token<>();
            amrmToken.decodeFromUrlString(
                new String(contextEntry.getValue(), StandardCharsets.UTF_8));
            // Clear the service field, as if RM just issued the token
            amrmToken.setService(new Text());
          }
        }

        if (amrmToken == null) {
          throw new IOException("No amrmToken found for app attempt " + attemptId);
        }
        if (user == null) {
          throw new IOException("No user found for app attempt " + attemptId);
        }

        // Regenerate the local AMRMToken for the AM
        Token<AMRMTokenIdentifier> localToken =
            this.secretManager.createAndGetAMRMToken(attemptId);

        // Retrieve the AM container credentials from NM context
        Credentials amCred = null;
        for (Container container : this.nmContext.getContainers().values()) {
          LOG.debug("From NM Context container {}.", container.getContainerId());
          if (container.getContainerId().getApplicationAttemptId().equals(
              attemptId) && container.getContainerTokenIdentifier() != null) {
            LOG.debug("Container type {}.",
                container.getContainerTokenIdentifier().getContainerType());
            if (container.getContainerTokenIdentifier()
                .getContainerType() == ContainerType.APPLICATION_MASTER) {
              LOG.info("AM container {} found in context, has credentials: {}.",
                  container.getContainerId(),
                  (container.getCredentials() != null));
              amCred = container.getCredentials();
            }
          }
        }
        if (amCred == null) {
          LOG.error("No credentials found for AM container of {}. "
              + "Yarn registry access might not work.", attemptId);
        }

        // Create the interceptor pipeline for the AM
        initializePipeline(attemptId, user, amrmToken, localToken,
            entry.getValue(), true, amCred);
        long endTime = clock.getTime();
        this.metrics.succeededRecoverRequests(endTime - startTime);
      } catch (Throwable e) {
        LOG.error("Exception when recovering {}, removing it from NMStateStore and move on.",
            attemptId, e);
        this.metrics.incrFailedAppRecoveryCount();
        this.nmContext.getNMStateStore().removeAMRMProxyAppContext(attemptId);
      }
    }
  }

  /**
   * This is called by the AMs started on this node to register with the RM.
   * This method does the initial authorization and then forwards the request to
   * the application instance specific interceptor chain.
   */
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    this.metrics.incrRequestCount();
    long startTime = clock.getTime();
    try {
      RequestInterceptorChainWrapper pipeline =
          authorizeAndGetInterceptorChain();

      LOG.info("RegisteringAM Host: {}, Port: {}, Tracking Url: {} for application {}. ",
          request.getHost(), request.getRpcPort(), request.getTrackingUrl(),
          pipeline.getApplicationAttemptId());

      RegisterApplicationMasterResponse response =
          pipeline.getRootInterceptor().registerApplicationMaster(request);

      long endTime = clock.getTime();
      this.metrics.succeededRegisterAMRequests(endTime - startTime);
      LOG.info("RegisterAM processing finished in {} ms for application {}.",
          endTime - startTime, pipeline.getApplicationAttemptId());
      return response;
    } catch (Throwable t) {
      this.metrics.incrFailedRegisterAMRequests();
      throw t;
    }
  }

  /**
   * This is called by the AMs started on this node to unregister from the RM.
   * This method does the initial authorization and then forwards the request to
   * the application instance specific interceptor chain.
   */
  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    this.metrics.incrRequestCount();
    long startTime = clock.getTime();
    try {
      RequestInterceptorChainWrapper pipeline =
          authorizeAndGetInterceptorChain();
      LOG.info("Finishing application master for {}. Tracking Url: {}.",
          pipeline.getApplicationAttemptId(), request.getTrackingUrl());
      FinishApplicationMasterResponse response =
          pipeline.getRootInterceptor().finishApplicationMaster(request);

      long endTime = clock.getTime();
      this.metrics.succeededFinishAMRequests(endTime - startTime);
      LOG.info("FinishAM finished with isUnregistered = {} in {} ms for {}.",
          response.getIsUnregistered(), endTime - startTime,
          pipeline.getApplicationAttemptId());
      return response;
    } catch (Throwable t) {
      this.metrics.incrFailedFinishAMRequests();
      throw t;
    }
  }

  /**
   * This is called by the AMs started on this node to send heart beat to RM.
   * This method does the initial authorization and then forwards the request to
   * the application instance specific pipeline, which is a chain of request
   * interceptor objects. One application request processing pipeline is created
   * per AM instance.
   */
  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
    this.metrics.incrAllocateCount();
    long startTime = clock.getTime();
    try {
      AMRMTokenIdentifier amrmTokenIdentifier =
          YarnServerSecurityUtils.authorizeRequest();
      RequestInterceptorChainWrapper pipeline =
          getInterceptorChain(amrmTokenIdentifier);
      AllocateResponse allocateResponse =
          pipeline.getRootInterceptor().allocate(request);

      updateAMRMTokens(amrmTokenIdentifier, pipeline, allocateResponse);

      long endTime = clock.getTime();
      this.metrics.succeededAllocateRequests(endTime - startTime);
      LOG.info("Allocate processing finished in {} ms for application {}.",
          endTime - startTime, pipeline.getApplicationAttemptId());
      return allocateResponse;
    } catch (Throwable t) {
      this.metrics.incrFailedAllocateRequests();
      throw t;
    }
  }

  /**
   * Callback from the ContainerManager implementation for initializing the
   * application request processing pipeline.
   *
   * @param request - encapsulates information for starting an AM
   * @throws IOException if fails
   * @throws YarnException if fails
   */
  public void processApplicationStartRequest(StartContainerRequest request)
      throws IOException, YarnException {
    this.metrics.incrRequestCount();
    long startTime = clock.getTime();
    try {
      ContainerTokenIdentifier containerTokenIdentifierForKey =
          BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());
      ApplicationAttemptId appAttemptId =
          containerTokenIdentifierForKey.getContainerID()
              .getApplicationAttemptId();
      ApplicationId applicationID = appAttemptId.getApplicationId();
      // Checking if application is there in federation state store only
      // if federation is enabled. If
      // application is submitted to router then it adds it in statestore.
      // if application is not found in statestore that means its
      // submitted to RM
      if (!checkIfAppExistsInStateStore(applicationID)) {
        return;
      }
      LOG.info("Callback received for initializing request processing pipeline for an AM.");
      Credentials credentials = YarnServerSecurityUtils
          .parseCredentials(request.getContainerLaunchContext());

      Token<AMRMTokenIdentifier> amrmToken =
          getFirstAMRMToken(credentials.getAllTokens());
      if (amrmToken == null) {
        throw new YarnRuntimeException(
            "AMRMToken not found in the start container request for application:" + appAttemptId);
      }

      // Substitute the existing AMRM Token with a local one. Keep the rest of
      // the tokens in the credentials intact.
      Token<AMRMTokenIdentifier> localToken =
          this.secretManager.createAndGetAMRMToken(appAttemptId);
      credentials.addToken(localToken.getService(), localToken);

      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      request.getContainerLaunchContext()
          .setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));

      initializePipeline(appAttemptId,
          containerTokenIdentifierForKey.getApplicationSubmitter(), amrmToken,
          localToken, null, false, credentials);

      long endTime = clock.getTime();
      this.metrics.succeededAppStartRequests(endTime - startTime);
    } catch (Throwable t) {
      this.metrics.incrFailedAppStartRequests();
      throw t;
    }
  }

  /**
   * Initializes the request interceptor pipeline for the specified application.
   *
   * @param applicationAttemptId attempt id
   * @param user user name
   * @param amrmToken amrmToken issued by RM
   * @param localToken amrmToken issued by AMRMProxy
   * @param recoveredDataMap the recovered states for AMRMProxy from NMSS
   * @param isRecovery whether this is to recover a previously existing pipeline
   */
  protected void initializePipeline(ApplicationAttemptId applicationAttemptId,
      String user, Token<AMRMTokenIdentifier> amrmToken,
      Token<AMRMTokenIdentifier> localToken,
      Map<String, byte[]> recoveredDataMap, boolean isRecovery,
      Credentials credentials) {
    RequestInterceptorChainWrapper chainWrapper = null;
    synchronized (applPipelineMap) {
      if (applPipelineMap
          .containsKey(applicationAttemptId.getApplicationId())) {
        LOG.warn("Request to start an already existing appId was received. "
            + " This can happen if an application failed and a new attempt "
            + "was created on this machine.  ApplicationId: {}.", applicationAttemptId);

        RequestInterceptorChainWrapper chainWrapperBackup =
            this.applPipelineMap.get(applicationAttemptId.getApplicationId());
        if (chainWrapperBackup != null
            && chainWrapperBackup.getApplicationAttemptId() != null
            && !chainWrapperBackup.getApplicationAttemptId()
                .equals(applicationAttemptId)) {
          // TODO: revisit in AMRMProxy HA in YARN-6128
          // Remove the existing pipeline
          LOG.info("Remove the previous pipeline for ApplicationId: {}.", applicationAttemptId);
          RequestInterceptorChainWrapper pipeline =
              applPipelineMap.remove(applicationAttemptId.getApplicationId());

          if (!isRecovery && this.nmContext.getNMStateStore() != null) {
            try {
              this.nmContext.getNMStateStore()
                  .removeAMRMProxyAppContext(applicationAttemptId);
            } catch (IOException ioe) {
              LOG.error("Error removing AMRMProxy application context for {}.",
                  applicationAttemptId, ioe);
            }
          }

          try {
            pipeline.getRootInterceptor().shutdown();
          } catch (Throwable ex) {
            LOG.warn("Failed to shutdown the request processing pipeline for app: {}.",
                applicationAttemptId.getApplicationId(), ex);
          }
        } else {
          return;
        }
      }

      chainWrapper = new RequestInterceptorChainWrapper();
      this.applPipelineMap.put(applicationAttemptId.getApplicationId(),
          chainWrapper);
    }

    // We register the pipeline instance in the map first and then initialize it
    // later because chain initialization can be expensive, and we would like to
    // release the lock as soon as possible to prevent other applications from
    // blocking when one application's chain is initializing
    LOG.info("Initializing request processing pipeline for application. "
        + " ApplicationId: {} for the user: {}.", applicationAttemptId, user);

    try {
      RequestInterceptor interceptorChain =
          this.createRequestInterceptorChain();
      interceptorChain.init(
          createApplicationMasterContext(this.nmContext, applicationAttemptId,
              user, amrmToken, localToken, credentials, this.registry));
      if (isRecovery) {
        if (recoveredDataMap == null) {
          throw new YarnRuntimeException("null recoveredDataMap received for recover");
        }
        interceptorChain.recover(recoveredDataMap);
      }
      chainWrapper.init(interceptorChain, applicationAttemptId);

      if (!isRecovery && this.nmContext.getNMStateStore() != null) {
        try {
          this.nmContext.getNMStateStore().storeAMRMProxyAppContextEntry(
              applicationAttemptId, NMSS_USER_KEY, user.getBytes(StandardCharsets.UTF_8));
          this.nmContext.getNMStateStore().storeAMRMProxyAppContextEntry(
              applicationAttemptId, NMSS_AMRMTOKEN_KEY,
              amrmToken.encodeToUrlString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
          LOG.error("Error storing AMRMProxy application context entry for {}.",
              applicationAttemptId, e);
        }
      }
    } catch (Exception e) {
      this.applPipelineMap.remove(applicationAttemptId.getApplicationId());
      throw e;
    }
  }

  /**
   * Shuts down the request processing pipeline for the specified application
   * attempt id.
   *
   * @param applicationId application id
   */
  protected void stopApplication(ApplicationId applicationId) {
    this.metrics.incrRequestCount();
    Preconditions.checkArgument(applicationId != null, "applicationId is null");
    RequestInterceptorChainWrapper pipeline =
        this.applPipelineMap.remove(applicationId);
    boolean isStopSuccess = true;
    long startTime = clock.getTime();

    if (pipeline == null) {
      LOG.info("No interceptor pipeline for application {},"
          + " likely because its AM is not run in this node.", applicationId);
      isStopSuccess = false;
    } else {
      // Remove the appAttempt in AMRMTokenSecretManager
      this.secretManager.applicationMasterFinished(pipeline.getApplicationAttemptId());
      LOG.info("Stopping the request processing pipeline for application: {}.", applicationId);
      try {
        RequestInterceptor interceptor = pipeline.getRootInterceptor();
        stopUnmanagedApplicaiton(interceptor);
        interceptor.shutdown();
      } catch (Throwable ex) {
        LOG.warn("Failed to shutdown the request processing pipeline for app: {}.",
            applicationId, ex);
        isStopSuccess = false;
      }

      // Remove the app context from NMSS after the interceptors are shutdown
      if (this.nmContext.getNMStateStore() != null) {
        try {
          this.nmContext.getNMStateStore()
              .removeAMRMProxyAppContext(pipeline.getApplicationAttemptId());
        } catch (IOException e) {
          LOG.error("Error removing AMRMProxy application context for {}.",
              applicationId, e);
          isStopSuccess = false;
        }
      }
    }

    if (isStopSuccess) {
      long endTime = clock.getTime();
      this.metrics.succeededAppStopRequests(endTime - startTime);
    } else {
      this.metrics.incrFailedAppStopRequests();
    }
  }

  private void stopUnmanagedApplicaiton(RequestInterceptor interceptor) {
    if (interceptor == null) {
      return;
    }
    if (interceptor instanceof FederationInterceptor) {
      ((FederationInterceptor) interceptor).getUnmanagedAMPool().stop();
    }
    stopUnmanagedApplicaiton(interceptor.getNextInterceptor());
  }

  private void updateAMRMTokens(AMRMTokenIdentifier amrmTokenIdentifier,
      RequestInterceptorChainWrapper pipeline,
      AllocateResponse allocateResponse) {

    AMRMProxyApplicationContextImpl context =
        (AMRMProxyApplicationContextImpl) pipeline.getRootInterceptor().getApplicationContext();

    try {
      long startTime = clock.getTime();

      // check to see if the RM has issued a new AMRMToken & accordingly update
      // the real ARMRMToken in the current context
      if (allocateResponse.getAMRMToken() != null) {
        LOG.info("RM rolled master-key for amrm-tokens.");

        org.apache.hadoop.yarn.api.records.Token token = allocateResponse.getAMRMToken();

        // Do not propagate this info back to AM
        allocateResponse.setAMRMToken(null);

        org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> newToken =
                ConverterUtils.convertFromYarn(token, (Text) null);

        // Update the AMRMToken in context map, and in NM state store if it is
        // different
        if (context.setAMRMToken(newToken) && this.nmContext.getNMStateStore() != null) {
          this.nmContext.getNMStateStore().storeAMRMProxyAppContextEntry(
              context.getApplicationAttemptId(), NMSS_AMRMTOKEN_KEY,
              newToken.encodeToUrlString().getBytes(StandardCharsets.UTF_8));
        }
      }

      // Check if the local AMRMToken is rolled up and update the context and
      // response accordingly
      MasterKeyData nextMasterKey = this.secretManager.getNextMasterKeyData();

      if (nextMasterKey != null) {
        MasterKey masterKey = nextMasterKey.getMasterKey();
        if (masterKey.getKeyId() != amrmTokenIdentifier.getKeyId()) {
          Token<AMRMTokenIdentifier> localToken = context.getLocalAMRMToken();
          if (masterKey.getKeyId() != context.getLocalAMRMTokenKeyId()) {
            LOG.info("The local AMRMToken has been rolled-over."
                + " Send new local AMRMToken back to application: {}",
                pipeline.getApplicationId());
            localToken = this.secretManager.createAndGetAMRMToken(
                pipeline.getApplicationAttemptId());
            context.setLocalAMRMToken(localToken);
          }

          allocateResponse
              .setAMRMToken(org.apache.hadoop.yarn.api.records.Token
                  .newInstance(localToken.getIdentifier(), localToken
                      .getKind().toString(), localToken.getPassword(),
                      localToken.getService().toString()));
        }
      }
      long endTime = clock.getTime();
      this.metrics.succeededUpdateTokenRequests(endTime - startTime);
    } catch (IOException e) {
      LOG.error("Error storing AMRMProxy application context entry for {}.",
          context.getApplicationAttemptId(), e);
      this.metrics.incrFailedUpdateAMRMTokenRequests();
    }
  }

  private AMRMProxyApplicationContext createApplicationMasterContext(
      Context context, ApplicationAttemptId applicationAttemptId, String user,
      Token<AMRMTokenIdentifier> amrmToken,
      Token<AMRMTokenIdentifier> localToken, Credentials credentials,
      RegistryOperations registryImpl) {
    AMRMProxyApplicationContextImpl appContext =
        new AMRMProxyApplicationContextImpl(context, getConfig(),
            applicationAttemptId, user, amrmToken, localToken, credentials,
            registryImpl);
    return appContext;
  }

  /**
   * Gets the Request interceptor chains for all the applications.
   *
   * @return the request interceptor chains.
   */
  protected Map<ApplicationId, RequestInterceptorChainWrapper> getPipelines() {
    return this.applPipelineMap;
  }

  /**
   * This method creates and returns reference of the first interceptor in the
   * chain of request interceptor instances.
   *
   * @return the reference of the first interceptor in the chain
   */
  protected RequestInterceptor createRequestInterceptorChain() {
    Configuration conf = getConfig();

    List<String> interceptorClassNames = getInterceptorClassNames(conf);

    RequestInterceptor pipeline = null;
    RequestInterceptor current = null;
    for (String interceptorClassName : interceptorClassNames) {
      try {
        Class<?> interceptorClass =
            conf.getClassByName(interceptorClassName);
        if (RequestInterceptor.class.isAssignableFrom(interceptorClass)) {
          RequestInterceptor interceptorInstance =
              (RequestInterceptor) ReflectionUtils.newInstance(
                  interceptorClass, conf);
          if (pipeline == null) {
            pipeline = interceptorInstance;
            current = interceptorInstance;
            continue;
          } else {
            current.setNextInterceptor(interceptorInstance);
            current = interceptorInstance;
          }
        } else {
          throw new YarnRuntimeException("Class: " + interceptorClassName
              + " not instance of "
              + RequestInterceptor.class.getCanonicalName());
        }
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException(
            "Could not instantiate ApplicationMasterRequestInterceptor: "
            + interceptorClassName, e);
      }
    }

    if (pipeline == null) {
      throw new YarnRuntimeException(
          "RequestInterceptor pipeline is not configured in the system");
    }
    return pipeline;
  }

  /**
   * Returns the comma separated interceptor class names from the configuration.
   *
   * @param conf configuration
   * @return the interceptor class names as an instance of ArrayList
   */
  private List<String> getInterceptorClassNames(Configuration conf) {
    String configuredInterceptorClassNames =
        conf.get(
            YarnConfiguration.AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE,
            YarnConfiguration.DEFAULT_AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE);

    List<String> interceptorClassNames = new ArrayList<>();
    Collection<String> tempList =
        StringUtils.getStringCollection(configuredInterceptorClassNames);
    for (String item : tempList) {
      interceptorClassNames.add(item.trim());
    }

    // Make sure DistributedScheduler is present at the beginning of the chain.
    if (this.nmContext.isDistributedSchedulingEnabled()) {
      interceptorClassNames.add(0, DistributedScheduler.class.getName());
    }

    return interceptorClassNames;
  }

  /**
   * Authorizes the request and returns the application specific request
   * processing pipeline.
   *
   * @return the interceptor wrapper instance
   * @throws YarnException if fails
   */
  private RequestInterceptorChainWrapper authorizeAndGetInterceptorChain()
      throws YarnException {
    AMRMTokenIdentifier tokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();
    return getInterceptorChain(tokenIdentifier);
  }

  private RequestInterceptorChainWrapper getInterceptorChain(
      AMRMTokenIdentifier tokenIdentifier) throws YarnException {
    ApplicationAttemptId appAttemptId =
        tokenIdentifier.getApplicationAttemptId();

    synchronized (this.applPipelineMap) {
      if (!this.applPipelineMap.containsKey(appAttemptId.getApplicationId())) {
        throw new YarnException(
            "The AM request processing pipeline is not initialized for app: "
            + appAttemptId.getApplicationId());
      }

      return this.applPipelineMap.get(appAttemptId.getApplicationId());
    }
  }

  boolean checkIfAppExistsInStateStore(ApplicationId applicationID) {
    if (!federationEnabled) {
      return true;
    }

    try {
      // Check if app is there in state store. If app is not there then it
      // throws Exception
      this.federationFacade.getApplicationHomeSubCluster(applicationID);
    } catch (YarnException ex) {
      return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private Token<AMRMTokenIdentifier> getFirstAMRMToken(
      Collection<Token<? extends TokenIdentifier>> allTokens) {
    Iterator<Token<? extends TokenIdentifier>> iter = allTokens.iterator();
    while (iter.hasNext()) {
      Token<? extends TokenIdentifier> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        return (Token<AMRMTokenIdentifier>) token;
      }
    }

    return null;
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.listenerEndpoint;
  }

  @Private
  public AMRMProxyTokenSecretManager getSecretManager() {
    return this.secretManager;
  }

  /**
   * Private class for handling application stop events.
   */
  class ApplicationEventHandler implements EventHandler<ApplicationEvent> {

    @Override
    public void handle(ApplicationEvent event) {
      Application app =
          AMRMProxyService.this.nmContext.getApplications().get(event.getApplicationID());
      if (app != null) {
        switch (event.getType()) {
        case APPLICATION_RESOURCES_CLEANEDUP:
          LOG.info("Application stop event received for stopping AppId: {}.",
              event.getApplicationID().toString());
          AMRMProxyService.this.stopApplication(event.getApplicationID());
          break;
        default:
          LOG.debug("AMRMProxy is ignoring event: {}.", event.getType());
          break;
        }
      } else {
        LOG.warn("Event {} sent to absent application {}.", event, event.getApplicationID());
      }
    }
  }

  /**
   * Private structure for encapsulating RequestInterceptor and
   * ApplicationAttemptId instances.
   *
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private RequestInterceptor rootInterceptor;
    private ApplicationAttemptId applicationAttemptId;

    /**
     * Initializes the wrapper with the specified parameters.
     *
     * @param interceptor the root request interceptor
     * @param appAttemptId attempt id
     */
    public synchronized void init(RequestInterceptor interceptor,
        ApplicationAttemptId appAttemptId) {
      rootInterceptor = interceptor;
      applicationAttemptId = appAttemptId;
    }

    /**
     * Gets the root request interceptor.
     *
     * @return the root request interceptor
     */
    public synchronized RequestInterceptor getRootInterceptor() {
      return rootInterceptor;
    }

    /**
     * Gets the application attempt identifier.
     *
     * @return the application attempt identifier
     */
    public synchronized ApplicationAttemptId getApplicationAttemptId() {
      return applicationAttemptId;
    }

    /**
     * Gets the application identifier.
     *
     * @return the application identifier
     */
    public synchronized ApplicationId getApplicationId() {
      return applicationAttemptId.getApplicationId();
    }
  }
}
