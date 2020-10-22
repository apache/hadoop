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

package org.apache.hadoop.yarn.server.uam;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.AMHeartbeatRequestHandler;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * UnmanagedApplicationManager is used to register unmanaged application and
 * negotiate for resources from resource managers. An unmanagedAM is an AM that
 * is not launched and managed by the RM. Allocate calls are handled
 * asynchronously using {@link AsyncCallback}.
 */
@Public
@Unstable
public class UnmanagedApplicationManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(UnmanagedApplicationManager.class);
  private static final long AM_STATE_WAIT_TIMEOUT_MS = 10000;
  public static final String APP_NAME = "UnmanagedAM";
  private static final String DEFAULT_QUEUE_CONFIG = "uam.default.queue.name";

  private AMHeartbeatRequestHandler heartbeatHandler;
  private AMRMClientRelayer rmProxyRelayer;
  private ApplicationId applicationId;
  private String submitter;
  private String appNameSuffix;
  private Configuration conf;
  private String queueName;
  private UserGroupInformation userUgi;
  private RegisterApplicationMasterRequest registerRequest;
  private ApplicationClientProtocol rmClient;
  private long asyncApiPollIntervalMillis;
  private RecordFactory recordFactory;
  private boolean keepContainersAcrossApplicationAttempts;

  /*
   * This flag is used as an indication that this method launchUAM/reAttachUAM
   * is called (and perhaps blocked in initializeUnmanagedAM below due to RM
   * connection/failover issue and not finished yet). Set the flag before
   * calling the blocking call to RM.
   */
  private boolean connectionInitiated;

  /**
   * Constructor.
   *
   * @param conf configuration
   * @param appId application Id to use for this UAM
   * @param queueName the queue of the UAM
   * @param submitter user name of the app
   * @param appNameSuffix the app name suffix to use
   * @param rmName name of the YarnRM
   * @param keepContainersAcrossApplicationAttempts keep container flag for UAM
   *          recovery. See {@link ApplicationSubmissionContext
   *          #setKeepContainersAcrossApplicationAttempts(boolean)}
   */
  public UnmanagedApplicationManager(Configuration conf, ApplicationId appId,
      String queueName, String submitter, String appNameSuffix,
      boolean keepContainersAcrossApplicationAttempts, String rmName) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    Preconditions.checkNotNull(appId, "ApplicationId cannot be null");
    Preconditions.checkNotNull(submitter, "App submitter cannot be null");

    this.conf = conf;
    this.applicationId = appId;
    this.queueName = queueName;
    this.submitter = submitter;
    this.appNameSuffix = appNameSuffix;
    this.userUgi = null;
    // Relayer's rmClient will be set after the RM connection is created
    this.rmProxyRelayer =
        new AMRMClientRelayer(null, this.applicationId, rmName);
    this.heartbeatHandler = createAMHeartbeatRequestHandler(this.conf,
        this.applicationId, this.rmProxyRelayer);

    this.connectionInitiated = false;
    this.registerRequest = null;
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    this.asyncApiPollIntervalMillis = conf.getLong(
        YarnConfiguration.
            YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS,
        YarnConfiguration.
            DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
    this.keepContainersAcrossApplicationAttempts =
        keepContainersAcrossApplicationAttempts;
  }

  @VisibleForTesting
  protected AMHeartbeatRequestHandler createAMHeartbeatRequestHandler(
      Configuration config, ApplicationId appId,
      AMRMClientRelayer relayer) {
    return new AMHeartbeatRequestHandler(config, appId, relayer);
  }

  /**
   * Launch a new UAM in the resource manager.
   *
   * @return identifier uam identifier
   * @throws YarnException if fails
   * @throws IOException if fails
   */
  public Token<AMRMTokenIdentifier> launchUAM()
      throws YarnException, IOException {
    this.connectionInitiated = true;

    // Blocking call to RM
    Token<AMRMTokenIdentifier> amrmToken =
        initializeUnmanagedAM(this.applicationId);

    // Creates the UAM connection
    createUAMProxy(amrmToken);
    return amrmToken;
  }

  /**
   * Re-attach to an existing UAM in the resource manager.
   *
   * @param amrmToken the UAM token
   * @throws IOException if re-attach fails
   * @throws YarnException if re-attach fails
   */
  public void reAttachUAM(Token<AMRMTokenIdentifier> amrmToken)
      throws IOException, YarnException {
    this.connectionInitiated = true;

    // Creates the UAM connection
    createUAMProxy(amrmToken);
  }

  protected void createUAMProxy(Token<AMRMTokenIdentifier> amrmToken)
      throws IOException {
    this.userUgi = UserGroupInformation.createProxyUser(
        this.applicationId.toString(), UserGroupInformation.getCurrentUser());
    this.rmProxyRelayer.setRMClient(createRMProxy(
        ApplicationMasterProtocol.class, this.conf, this.userUgi, amrmToken));
    this.heartbeatHandler.setUGI(this.userUgi);
  }

  /**
   * Registers this {@link UnmanagedApplicationManager} with the resource
   * manager.
   *
   * @param request RegisterApplicationMasterRequest
   * @return register response
   * @throws YarnException if register fails
   * @throws IOException if register fails
   */
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    // Save the register request for re-register later
    this.registerRequest = request;

    LOG.info("Registering the Unmanaged application master {}",
        this.applicationId);
    RegisterApplicationMasterResponse response =
        this.rmProxyRelayer.registerApplicationMaster(this.registerRequest);
    this.heartbeatHandler.resetLastResponseId();

    for (Container container : response.getContainersFromPreviousAttempts()) {
      LOG.debug("RegisterUAM returned existing running container {}",
          container.getId());
    }
    for (NMToken nmToken : response.getNMTokensFromPreviousAttempts()) {
      LOG.debug("RegisterUAM returned existing NM token for node {}",
          nmToken.getNodeId());
    }
    LOG.info(
        "RegisterUAM returned {} existing running container and {} NM tokens",
        response.getContainersFromPreviousAttempts().size(),
        response.getNMTokensFromPreviousAttempts().size());

    // Only when register succeed that we start the heartbeat thread
    this.heartbeatHandler.setDaemon(true);
    this.heartbeatHandler.start();

    return response;
  }

  /**
   * Unregisters from the resource manager and stops the request handler thread.
   *
   * @param request the finishApplicationMaster request
   * @return the response
   * @throws YarnException if finishAM call fails
   * @throws IOException if finishAM call fails
   */
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    if (this.userUgi == null) {
      if (this.connectionInitiated) {
        // This is possible if the async launchUAM is still
        // blocked and retrying. Return a dummy response in this case.
        LOG.warn("Unmanaged AM still not successfully launched/registered yet."
            + " Stopping the UAM heartbeat thread anyways.");
        return FinishApplicationMasterResponse.newInstance(false);
      } else {
        throw new YarnException("finishApplicationMaster should not "
            + "be called before createAndRegister");
      }
    }
    FinishApplicationMasterResponse response =
        this.rmProxyRelayer.finishApplicationMaster(request);
    if (response.getIsUnregistered()) {
      shutDownConnections();
    }
    return response;
  }

  /**
   * Force kill the UAM.
   *
   * @return kill response
   * @throws IOException if fails to create rmProxy
   * @throws YarnException if force kill fails
   */
  public KillApplicationResponse forceKillApplication()
      throws IOException, YarnException {
    shutDownConnections();

    KillApplicationRequest request =
        KillApplicationRequest.newInstance(this.applicationId);
    if (this.rmClient == null) {
      this.rmClient = createRMProxy(ApplicationClientProtocol.class, this.conf,
          UserGroupInformation.createRemoteUser(this.submitter), null);
    }
    return this.rmClient.forceKillApplication(request);
  }

  /**
   * Sends the specified heart beat request to the resource manager and invokes
   * the callback asynchronously with the response.
   *
   * @param request the allocate request
   * @param callback the callback method for the request
   * @throws YarnException if registerAM is not called yet
   */
  public void allocateAsync(AllocateRequest request,
      AsyncCallback<AllocateResponse> callback) throws YarnException {
    this.heartbeatHandler.allocateAsync(request, callback);

    // Two possible cases why the UAM is not successfully registered yet:
    // 1. launchUAM is not called at all. Should throw here.
    // 2. launchUAM is called but hasn't successfully returned.
    //
    // In case 2, we have already save the allocate request above, so if the
    // registration succeed later, no request is lost.
    if (this.userUgi == null) {
      if (this.connectionInitiated) {
        LOG.info("Unmanaged AM still not successfully launched/registered yet."
            + " Saving the allocate request and send later.");
      } else {
        throw new YarnException(
            "AllocateAsync should not be called before launchUAM");
      }
    }
  }

  /**
   * Shutdown this UAM client, without killing the UAM in the YarnRM side.
   */
  public void shutDownConnections() {
    this.heartbeatHandler.shutdown();
    this.rmProxyRelayer.shutdown();
  }

  /**
   * Returns the application id of the UAM.
   *
   * @return application id of the UAM
   */
  public ApplicationId getAppId() {
    return this.applicationId;
  }

  /**
   * Returns the rmProxy relayer of this UAM.
   *
   * @return rmProxy relayer of the UAM
   */
  public AMRMClientRelayer getAMRMClientRelayer() {
    return this.rmProxyRelayer;
  }

  /**
   * Returns RM proxy for the specified protocol type. Unit test cases can
   * override this method and return mock proxy instances.
   *
   * @param protocol protocal of the proxy
   * @param config configuration
   * @param user ugi for the proxy connection
   * @param token token for the connection
   * @param <T> type of the proxy
   * @return the proxy instance
   * @throws IOException if fails to create the proxy
   */
  protected <T> T createRMProxy(Class<T> protocol, Configuration config,
      UserGroupInformation user, Token<AMRMTokenIdentifier> token)
      throws IOException {
    return AMRMClientUtils.createRMProxy(config, protocol, user, token);
  }

  /**
   * Launch and initialize an unmanaged AM. First, it creates a new application
   * on the RM and negotiates a new attempt id. Then it waits for the RM
   * application attempt state to reach YarnApplicationAttemptState.LAUNCHED
   * after which it returns the AM-RM token.
   *
   * @param appId application id
   * @return the UAM token
   * @throws IOException if initialize fails
   * @throws YarnException if initialize fails
   */
  protected Token<AMRMTokenIdentifier> initializeUnmanagedAM(
      ApplicationId appId) throws IOException, YarnException {
    try {
      UserGroupInformation appSubmitter =
          UserGroupInformation.createRemoteUser(this.submitter);
      this.rmClient = createRMProxy(ApplicationClientProtocol.class, this.conf,
          appSubmitter, null);

      // Submit the application
      submitUnmanagedApp(appId);

      // Monitor the application attempt to wait for launch state
      monitorCurrentAppAttempt(appId,
          EnumSet.of(YarnApplicationState.ACCEPTED,
              YarnApplicationState.RUNNING, YarnApplicationState.KILLED,
              YarnApplicationState.FAILED, YarnApplicationState.FINISHED),
          YarnApplicationAttemptState.LAUNCHED);
      return getUAMToken();
    } finally {
      this.rmClient = null;
    }
  }

  private void submitUnmanagedApp(ApplicationId appId)
      throws YarnException, IOException {
    SubmitApplicationRequest submitRequest =
        this.recordFactory.newRecordInstance(SubmitApplicationRequest.class);

    ApplicationSubmissionContext context = this.recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);

    context.setApplicationId(appId);
    context.setApplicationName(APP_NAME + "-" + appNameSuffix);
    if (StringUtils.isBlank(this.queueName)) {
      context.setQueue(this.conf.get(DEFAULT_QUEUE_CONFIG,
          YarnConfiguration.DEFAULT_QUEUE_NAME));
    } else {
      context.setQueue(this.queueName);
    }

    ContainerLaunchContext amContainer =
        this.recordFactory.newRecordInstance(ContainerLaunchContext.class);
    Resource resource = BuilderUtils.newResource(1024, 1);
    context.setResource(resource);
    context.setAMContainerSpec(amContainer);
    submitRequest.setApplicationSubmissionContext(context);

    context.setUnmanagedAM(true);
    context.setKeepContainersAcrossApplicationAttempts(
        this.keepContainersAcrossApplicationAttempts);

    LOG.info("Submitting unmanaged application {}", appId);
    this.rmClient.submitApplication(submitRequest);
  }

  /**
   * Monitor the submitted application and attempt until it reaches certain
   * states.
   *
   * @param appId Application Id of application to be monitored
   * @param appStates acceptable application state
   * @param attemptState acceptable application attempt state
   * @return the application report
   * @throws YarnException if getApplicationReport fails
   * @throws IOException if getApplicationReport fails
   */
  private ApplicationAttemptReport monitorCurrentAppAttempt(ApplicationId appId,
      Set<YarnApplicationState> appStates,
      YarnApplicationAttemptState attemptState)
      throws YarnException, IOException {

    long startTime = System.currentTimeMillis();
    ApplicationAttemptId appAttemptId = null;
    while (true) {
      if (appAttemptId == null) {
        // Get application report for the appId we are interested in
        ApplicationReport report = getApplicationReport(appId);
        YarnApplicationState state = report.getYarnApplicationState();
        if (appStates.contains(state)) {
          if (state != YarnApplicationState.ACCEPTED) {
            throw new YarnRuntimeException(
                "Received non-accepted application state: " + state + " for "
                    + appId + ". This is likely because this is not the first "
                    + "app attempt in home sub-cluster, and AMRMProxy HA "
                    + "(yarn.nodemanager.amrmproxy.ha.enable) is not enabled.");
          }
          appAttemptId =
              getApplicationReport(appId).getCurrentApplicationAttemptId();
        } else {
          LOG.info("Current application state of {} is {}, will retry later.",
              appId, state);
        }
      }

      if (appAttemptId != null) {
        GetApplicationAttemptReportRequest req = this.recordFactory
            .newRecordInstance(GetApplicationAttemptReportRequest.class);
        req.setApplicationAttemptId(appAttemptId);
        ApplicationAttemptReport attemptReport = this.rmClient
            .getApplicationAttemptReport(req).getApplicationAttemptReport();
        if (attemptState
            .equals(attemptReport.getYarnApplicationAttemptState())) {
          return attemptReport;
        }
        LOG.info("Current attempt state of " + appAttemptId + " is "
            + attemptReport.getYarnApplicationAttemptState()
            + ", waiting for current attempt to reach " + attemptState);
      }

      try {
        Thread.sleep(this.asyncApiPollIntervalMillis);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for current attempt of " + appId
            + " to reach " + attemptState);
      }

      if (System.currentTimeMillis() - startTime > AM_STATE_WAIT_TIMEOUT_MS) {
        throw new RuntimeException("Timeout for waiting current attempt of "
            + appId + " to reach " + attemptState);
      }
    }
  }

  /**
   * Gets the amrmToken of the unmanaged AM.
   *
   * @return the amrmToken of the unmanaged AM.
   * @throws IOException if getApplicationReport fails
   * @throws YarnException if getApplicationReport fails
   */
  protected Token<AMRMTokenIdentifier> getUAMToken()
      throws IOException, YarnException {
    Token<AMRMTokenIdentifier> token = null;
    org.apache.hadoop.yarn.api.records.Token amrmToken =
        getApplicationReport(this.applicationId).getAMRMToken();
    if (amrmToken != null) {
      token = ConverterUtils.convertFromYarn(amrmToken, (Text) null);
    } else {
      LOG.warn(
          "AMRMToken not found in the application report for application: {}",
          this.applicationId);
    }
    return token;
  }

  private ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    GetApplicationReportRequest request =
        this.recordFactory.newRecordInstance(GetApplicationReportRequest.class);
    request.setApplicationId(appId);
    return this.rmClient.getApplicationReport(request).getApplicationReport();
  }

  @VisibleForTesting
  public int getRequestQueueSize() {
    return this.heartbeatHandler.getRequestQueueSize();
  }

  @VisibleForTesting
  protected void drainHeartbeatThread() {
    this.heartbeatHandler.drainHeartbeatThread();
  }

  @VisibleForTesting
  protected boolean isHeartbeatThreadAlive() {
    return this.heartbeatHandler.isAlive();
  }
}