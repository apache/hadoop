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
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;

import org.apache.hadoop.yarn.server.nodemanager.scheduler.DistributedScheduler;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * AMRMProxyService is a service that runs on each node manager that can be used
 * to intercept and inspect messages from application master to the cluster
 * resource manager. It listens to messages from the application master and
 * creates a request intercepting pipeline instance for each application. The
 * pipeline is a chain of intercepter instances that can inspect and modify the
 * request/response as needed.
 */
public class AMRMProxyService extends AbstractService implements
    ApplicationMasterProtocol {
  private static final Logger LOG = LoggerFactory
      .getLogger(AMRMProxyService.class);
  private Server server;
  private final Context nmContext;
  private final AsyncDispatcher dispatcher;
  private InetSocketAddress listenerEndpoint;
  private AMRMProxyTokenSecretManager secretManager;
  private Map<ApplicationId, RequestInterceptorChainWrapper> applPipelineMap;

  /**
   * Creates an instance of the service.
   * 
   * @param nmContext
   * @param dispatcher
   */
  public AMRMProxyService(Context nmContext, AsyncDispatcher dispatcher) {
    super(AMRMProxyService.class.getName());
    Preconditions.checkArgument(nmContext != null, "nmContext is null");
    Preconditions.checkArgument(dispatcher != null, "dispatcher is null");
    this.nmContext = nmContext;
    this.dispatcher = dispatcher;
    this.applPipelineMap =
        new ConcurrentHashMap<ApplicationId, RequestInterceptorChainWrapper>();

    this.dispatcher.register(ApplicationEventType.class,
        new ApplicationEventHandler());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting AMRMProxyService");
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

    this.secretManager = new AMRMProxyTokenSecretManager(serverConf);
    this.secretManager.start();

    this.server =
        rpc.getServer(ApplicationMasterProtocol.class, this,
            listenerEndpoint, serverConf, this.secretManager,
            numWorkerThreads);

    this.server.start();
    LOG.info("AMRMProxyService listening on address: "
        + this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping AMRMProxyService");
    if (this.server != null) {
      this.server.stop();
    }

    this.secretManager.stop();

    super.serviceStop();
  }

  /**
   * This is called by the AMs started on this node to register with the RM.
   * This method does the initial authorization and then forwards the request to
   * the application instance specific intercepter chain.
   */
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    LOG.info("Registering application master." + " Host:"
        + request.getHost() + " Port:" + request.getRpcPort()
        + " Tracking Url:" + request.getTrackingUrl());
    RequestInterceptorChainWrapper pipeline =
        authorizeAndGetInterceptorChain();
    return pipeline.getRootInterceptor()
        .registerApplicationMaster(request);
  }

  /**
   * This is called by the AMs started on this node to unregister from the RM.
   * This method does the initial authorization and then forwards the request to
   * the application instance specific intercepter chain.
   */
  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    LOG.info("Finishing application master. Tracking Url:"
        + request.getTrackingUrl());
    RequestInterceptorChainWrapper pipeline =
        authorizeAndGetInterceptorChain();
    return pipeline.getRootInterceptor().finishApplicationMaster(request);
  }

  /**
   * This is called by the AMs started on this node to send heart beat to RM.
   * This method does the initial authorization and then forwards the request to
   * the application instance specific pipeline, which is a chain of request
   * intercepter objects. One application request processing pipeline is created
   * per AM instance.
   */
  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
    AMRMTokenIdentifier amrmTokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();
    RequestInterceptorChainWrapper pipeline =
        getInterceptorChain(amrmTokenIdentifier);
    AllocateResponse allocateResponse =
        pipeline.getRootInterceptor().allocate(request);

    updateAMRMTokens(amrmTokenIdentifier, pipeline, allocateResponse);

    return allocateResponse;
  }

  /**
   * Callback from the ContainerManager implementation for initializing the
   * application request processing pipeline.
   *
   * @param request - encapsulates information for starting an AM
   * @throws IOException
   * @throws YarnException
   */
  public void processApplicationStartRequest(StartContainerRequest request)
      throws IOException, YarnException {
    LOG.info("Callback received for initializing request "
        + "processing pipeline for an AM");
    ContainerTokenIdentifier containerTokenIdentifierForKey =
        BuilderUtils.newContainerTokenIdentifier(request
            .getContainerToken());
    ApplicationAttemptId appAttemptId =
        containerTokenIdentifierForKey.getContainerID()
            .getApplicationAttemptId();
    Credentials credentials =
        YarnServerSecurityUtils.parseCredentials(request
            .getContainerLaunchContext());

    Token<AMRMTokenIdentifier> amrmToken =
        getFirstAMRMToken(credentials.getAllTokens());
    if (amrmToken == null) {
      throw new YarnRuntimeException(
          "AMRMToken not found in the start container request for application:"
              + appAttemptId.toString());
    }

    // Substitute the existing AMRM Token with a local one. Keep the rest of the
    // tokens in the credentials intact.
    Token<AMRMTokenIdentifier> localToken =
        this.secretManager.createAndGetAMRMToken(appAttemptId);
    credentials.addToken(localToken.getService(), localToken);

    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    request.getContainerLaunchContext().setTokens(
        ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));

    initializePipeline(containerTokenIdentifierForKey.getContainerID()
        .getApplicationAttemptId(),
        containerTokenIdentifierForKey.getApplicationSubmitter(),
        amrmToken, localToken);
  }

  /**
   * Initializes the request intercepter pipeline for the specified application.
   * 
   * @param applicationAttemptId
   * @param user
   * @param amrmToken
   */
  protected void initializePipeline(
      ApplicationAttemptId applicationAttemptId, String user,
      Token<AMRMTokenIdentifier> amrmToken,
      Token<AMRMTokenIdentifier> localToken) {
    RequestInterceptorChainWrapper chainWrapper = null;
    synchronized (applPipelineMap) {
      if (applPipelineMap.containsKey(applicationAttemptId.getApplicationId())) {
        LOG.warn("Request to start an already existing appId was received. "
            + " This can happen if an application failed and a new attempt "
            + "was created on this machine.  ApplicationId: "
            + applicationAttemptId.toString());
        return;
      }

      chainWrapper = new RequestInterceptorChainWrapper();
      this.applPipelineMap.put(applicationAttemptId.getApplicationId(),
          chainWrapper);
    }

    // We register the pipeline instance in the map first and then initialize it
    // later because chain initialization can be expensive and we would like to
    // release the lock as soon as possible to prevent other applications from
    // blocking when one application's chain is initializing
    LOG.info("Initializing request processing pipeline for application. "
        + " ApplicationId:" + applicationAttemptId + " for the user: "
        + user);

    RequestInterceptor interceptorChain =
        this.createRequestInterceptorChain();
    interceptorChain.init(createApplicationMasterContext(
        applicationAttemptId, user, amrmToken, localToken));
    chainWrapper.init(interceptorChain, applicationAttemptId);
  }

  /**
   * Shuts down the request processing pipeline for the specified application
   * attempt id.
   *
   * @param applicationId
   */
  protected void stopApplication(ApplicationId applicationId) {
    Preconditions.checkArgument(applicationId != null,
        "applicationId is null");
    RequestInterceptorChainWrapper pipeline =
        this.applPipelineMap.remove(applicationId);

    if (pipeline == null) {
      LOG.info("Request to stop an application that does not exist. Id:"
          + applicationId);
    } else {
      LOG.info("Stopping the request processing pipeline for application: "
          + applicationId);
      try {
        pipeline.getRootInterceptor().shutdown();
      } catch (Throwable ex) {
        LOG.warn(
            "Failed to shutdown the request processing pipeline for app:"
                + applicationId, ex);
      }
    }
  }

  private void updateAMRMTokens(AMRMTokenIdentifier amrmTokenIdentifier,
      RequestInterceptorChainWrapper pipeline,
      AllocateResponse allocateResponse) {
    AMRMProxyApplicationContextImpl context =
        (AMRMProxyApplicationContextImpl) pipeline.getRootInterceptor()
            .getApplicationContext();

    // check to see if the RM has issued a new AMRMToken & accordingly update
    // the real ARMRMToken in the current context
    if (allocateResponse.getAMRMToken() != null) {
      org.apache.hadoop.yarn.api.records.Token token =
          allocateResponse.getAMRMToken();

      org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> newTokenId =
          new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(
              token.getIdentifier().array(), token.getPassword().array(),
              new Text(token.getKind()), new Text(token.getService()));

      context.setAMRMToken(newTokenId);
    }

    // Check if the local AMRMToken is rolled up and update the context and
    // response accordingly
    MasterKeyData nextMasterKey =
        this.secretManager.getNextMasterKeyData();

    if (nextMasterKey != null
        && nextMasterKey.getMasterKey().getKeyId() != amrmTokenIdentifier
            .getKeyId()) {
      Token<AMRMTokenIdentifier> localToken = context.getLocalAMRMToken();
      if (nextMasterKey.getMasterKey().getKeyId() != context
          .getLocalAMRMTokenKeyId()) {
        LOG.info("The local AMRMToken has been rolled-over."
            + " Send new local AMRMToken back to application: "
            + pipeline.getApplicationId());
        localToken =
            this.secretManager.createAndGetAMRMToken(pipeline
                .getApplicationAttemptId());
        context.setLocalAMRMToken(localToken);
      }

      allocateResponse
          .setAMRMToken(org.apache.hadoop.yarn.api.records.Token
              .newInstance(localToken.getIdentifier(), localToken
                  .getKind().toString(), localToken.getPassword(),
                  localToken.getService().toString()));
    }
  }

  private AMRMProxyApplicationContext createApplicationMasterContext(
      ApplicationAttemptId applicationAttemptId, String user,
      Token<AMRMTokenIdentifier> amrmToken,
      Token<AMRMTokenIdentifier> localToken) {
    AMRMProxyApplicationContextImpl appContext =
        new AMRMProxyApplicationContextImpl(this.nmContext, getConfig(),
            applicationAttemptId, user, amrmToken, localToken);
    return appContext;
  }

  /**
   * Gets the Request intercepter chains for all the applications.
   * 
   * @return the request intercepter chains.
   */
  protected Map<ApplicationId, RequestInterceptorChainWrapper> getPipelines() {
    return this.applPipelineMap;
  }

  /**
   * This method creates and returns reference of the first intercepter in the
   * chain of request intercepter instances.
   *
   * @return the reference of the first intercepter in the chain
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
   * Returns the comma separated intercepter class names from the configuration.
   *
   * @param conf
   * @return the intercepter class names as an instance of ArrayList
   */
  private List<String> getInterceptorClassNames(Configuration conf) {
    String configuredInterceptorClassNames =
        conf.get(
            YarnConfiguration.AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE,
            YarnConfiguration.DEFAULT_AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE);

    List<String> interceptorClassNames = new ArrayList<String>();
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
   * @return the the intercepter wrapper instance
   * @throws YarnException
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
      if (!this.applPipelineMap.containsKey(appAttemptId
          .getApplicationId())) {
        throw new YarnException(
            "The AM request processing pipeline is not initialized for app: "
                + appAttemptId.getApplicationId().toString());
      }

      return this.applPipelineMap.get(appAttemptId.getApplicationId());
    }
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
   *
   */
  class ApplicationEventHandler implements EventHandler<ApplicationEvent> {

    @Override
    public void handle(ApplicationEvent event) {
      Application app =
          AMRMProxyService.this.nmContext.getApplications().get(
              event.getApplicationID());
      if (app != null) {
        switch (event.getType()) {
        case FINISH_APPLICATION:
          LOG.info("Application stop event received for stopping AppId:"
              + event.getApplicationID().toString());
          AMRMProxyService.this.stopApplication(event.getApplicationID());
          break;
        default:
          if (LOG.isDebugEnabled()) {
            LOG.debug("AMRMProxy is ignoring event: " + event.getType());
          }
          break;
        }
      } else {
        LOG.warn("Event " + event + " sent to absent application "
            + event.getApplicationID());
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
     * @param rootInterceptor
     * @param applicationAttemptId
     */
    public synchronized void init(RequestInterceptor rootInterceptor,
        ApplicationAttemptId applicationAttemptId) {
      this.rootInterceptor = rootInterceptor;
      this.applicationAttemptId = applicationAttemptId;
    }

    /**
     * Gets the root request intercepter.
     * 
     * @return the root request intercepter
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
