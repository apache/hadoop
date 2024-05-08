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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * A service that manages a pool of UAM managers in
 * {@link UnmanagedApplicationManager}.
 */
@Public
@Unstable
public class UnmanagedAMPoolManager extends AbstractService {
  public static final Logger LOG =
      LoggerFactory.getLogger(UnmanagedAMPoolManager.class);

  // Map from uamId to UAM instances
  private Map<String, UnmanagedApplicationManager> unmanagedAppMasterMap;

  private Map<String, ApplicationId> appIdMap;

  private ExecutorService threadpool;

  private final String dispatcherThreadName = "UnmanagedAMPoolManager-Finish-Thread";

  private Thread finishApplicationThread;

  public UnmanagedAMPoolManager(ExecutorService threadpool) {
    super(UnmanagedAMPoolManager.class.getName());
    this.threadpool = threadpool;
  }

  @Override
  protected void serviceStart() throws Exception {
    if (this.threadpool == null) {
      this.threadpool = Executors.newCachedThreadPool();
    }
    this.unmanagedAppMasterMap = new ConcurrentHashMap<>();
    this.appIdMap = new ConcurrentHashMap<>();
    super.serviceStart();
  }

  /**
   * Normally we should finish all applications before stop. If there are still
   * UAMs running, force kill all of them. Do parallel kill because of
   * performance reasons.
   *
   */
  @Override
  protected void serviceStop() throws Exception {

    if (!this.unmanagedAppMasterMap.isEmpty()) {
      finishApplicationThread = new Thread(createForceFinishApplicationThread());
      finishApplicationThread.setName(dispatcherThreadName);
      finishApplicationThread.start();
    }

    super.serviceStop();
  }

  /**
   * Create a new UAM and register the application, without specifying uamId and
   * appId. We will ask for an appId from RM and use it as the uamId.
   *
   * @param registerRequest RegisterApplicationMasterRequest
   * @param conf configuration for this UAM
   * @param queueName queue of the application
   * @param submitter submitter name of the UAM
   * @param appNameSuffix application name suffix for the UAM
   * @param keepContainersAcrossApplicationAttempts keep container flag for UAM
   *          recovery.
   * @param rmName name of the YarnRM
   * @param originalAppSubmissionContext ApplicationSubmissionContext
   * @see ApplicationSubmissionContext
   *          #setKeepContainersAcrossApplicationAttempts(boolean)
   * @return uamId for the UAM
   * @throws YarnException if registerApplicationMaster fails
   * @throws IOException if registerApplicationMaster fails
   */
  public String createAndRegisterNewUAM(
      RegisterApplicationMasterRequest registerRequest, Configuration conf,
      String queueName, String submitter, String appNameSuffix,
      boolean keepContainersAcrossApplicationAttempts, String rmName,
      ApplicationSubmissionContext originalAppSubmissionContext)
      throws YarnException, IOException {
    ApplicationId appId;
    ApplicationClientProtocol rmClient;
    try {
      UserGroupInformation appSubmitter =
          UserGroupInformation.createRemoteUser(submitter);
      rmClient = AMRMClientUtils.createRMProxy(conf,
          ApplicationClientProtocol.class, appSubmitter, null);

      // Get a new appId from RM
      GetNewApplicationResponse response =
          rmClient.getNewApplication(GetNewApplicationRequest.newInstance());
      if (response == null) {
        throw new YarnException("getNewApplication got null response");
      }
      appId = response.getApplicationId();
      LOG.info("Received new application ID {} from RM", appId);
    } finally {
      rmClient = null;
    }

    // Launch the UAM in RM
    launchUAM(appId.toString(), conf, appId, queueName, submitter,
        appNameSuffix, keepContainersAcrossApplicationAttempts, rmName,
        originalAppSubmissionContext);

    // Register the UAM application
    registerApplicationMaster(appId.toString(), registerRequest);

    // Returns the appId as uamId
    return appId.toString();
  }

  /**
   * Launch a new UAM, using the provided uamId and appId.
   *
   * @param uamId uam Id
   * @param conf configuration for this UAM
   * @param appId application id for the UAM
   * @param queueName queue of the application
   * @param submitter submitter name of the UAM
   * @param appNameSuffix application name suffix for the UAM
   * @param keepContainersAcrossApplicationAttempts keep container flag for UAM
   *          recovery.
   * @param rmName name of the YarnRM
   * @param originalAppSubmissionContext AppSubmissionContext
   * @see ApplicationSubmissionContext
   *          #setKeepContainersAcrossApplicationAttempts(boolean)
   * @return UAM token
   * @throws YarnException if fails
   * @throws IOException if fails
   */
  public Token<AMRMTokenIdentifier> launchUAM(String uamId, Configuration conf,
      ApplicationId appId, String queueName, String submitter,
      String appNameSuffix, boolean keepContainersAcrossApplicationAttempts,
      String rmName, ApplicationSubmissionContext originalAppSubmissionContext)
      throws YarnException, IOException {

    if (this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " already exists");
    }

    UnmanagedApplicationManager uam = createUAM(conf, appId, queueName,
        submitter, appNameSuffix, keepContainersAcrossApplicationAttempts,
        rmName, originalAppSubmissionContext);

    // Put the UAM into map first before initializing it to avoid additional UAM
    // for the same uamId being created concurrently
    this.unmanagedAppMasterMap.put(uamId, uam);

    Token<AMRMTokenIdentifier> amrmToken;
    try {
      LOG.info("Launching UAM id {} for application {}", uamId, appId);
      amrmToken = uam.launchUAM();
    } catch (Exception e) {
      // Add the map earlier and remove here if register failed because we want
      // to make sure there is only one uam instance per uamId at any given time
      this.unmanagedAppMasterMap.remove(uamId);
      throw e;
    }

    this.appIdMap.put(uamId, uam.getAppId());
    return amrmToken;
  }

  /**
   * Re-attach to an existing UAM, using the provided uamIdentifier.
   *
   * @param uamId uam Id
   * @param conf configuration for this UAM
   * @param appId application id for the UAM
   * @param queueName queue of the application
   * @param submitter submitter name of the UAM
   * @param appNameSuffix application name suffix for the UAM
   * @param uamToken UAM token
   * @param rmName name of the YarnRM
   * @param originalAppSubmissionContext AppSubmissionContext
   * @throws YarnException if fails
   * @throws IOException if fails
   */
  public void reAttachUAM(String uamId, Configuration conf, ApplicationId appId,
      String queueName, String submitter, String appNameSuffix,
      Token<AMRMTokenIdentifier> uamToken, String rmName,
      ApplicationSubmissionContext originalAppSubmissionContext)
      throws YarnException, IOException {

    if (this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " already exists");
    }
    UnmanagedApplicationManager uam = createUAM(conf, appId, queueName,
        submitter, appNameSuffix, true, rmName, originalAppSubmissionContext);
    // Put the UAM into map first before initializing it to avoid additional UAM
    // for the same uamId being created concurrently
    this.unmanagedAppMasterMap.put(uamId, uam);

    try {
      LOG.info("Reattaching UAM id {} for application {}", uamId, appId);
      uam.reAttachUAM(uamToken);
    } catch (Exception e) {
      // Add the map earlier and remove here if register failed because we want
      // to make sure there is only one uam instance per uamId at any given time
      this.unmanagedAppMasterMap.remove(uamId);
      throw e;
    }

    this.appIdMap.put(uamId, uam.getAppId());
  }

  /**
   * Creates the UAM instance. Pull out to make unit test easy.
   *
   * @param conf Configuration
   * @param appId application id
   * @param queueName queue of the application
   * @param submitter submitter name of the application
   * @param appNameSuffix application name suffix
   * @param keepContainersAcrossApplicationAttempts keep container flag for UAM
   * @param rmName name of the YarnRM
   * @param originalAppSubmissionContext ApplicationSubmissionContext
   * @return the UAM instance
   */
  @VisibleForTesting
  protected UnmanagedApplicationManager createUAM(Configuration conf,
      ApplicationId appId, String queueName, String submitter,
      String appNameSuffix, boolean keepContainersAcrossApplicationAttempts,
      String rmName, ApplicationSubmissionContext originalAppSubmissionContext) {
    return new UnmanagedApplicationManager(conf, appId, queueName, submitter,
        appNameSuffix, keepContainersAcrossApplicationAttempts, rmName,
        originalAppSubmissionContext);
  }

  /**
   * Register application master for the UAM.
   *
   * @param uamId uam Id
   * @param registerRequest RegisterApplicationMasterRequest
   * @return register response
   * @throws YarnException if register fails
   * @throws IOException if register fails
   */
  public RegisterApplicationMasterResponse registerApplicationMaster(
      String uamId, RegisterApplicationMasterRequest registerRequest)
      throws YarnException, IOException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    LOG.info("Registering UAM id {} for application {}", uamId,
        this.appIdMap.get(uamId));
    return this.unmanagedAppMasterMap.get(uamId)
        .registerApplicationMaster(registerRequest);
  }

  /**
   * AllocateAsync to an UAM.
   *
   * @param uamId uam Id
   * @param request AllocateRequest
   * @param callback callback for response
   * @throws YarnException if allocate fails
   * @throws IOException if allocate fails
   */
  public void allocateAsync(String uamId, AllocateRequest request,
      AsyncCallback<AllocateResponse> callback)
      throws YarnException, IOException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    this.unmanagedAppMasterMap.get(uamId).allocateAsync(request, callback);
  }

  /**
   * Finish an UAM/application.
   *
   * @param uamId uam Id
   * @param request FinishApplicationMasterRequest
   * @return FinishApplicationMasterResponse
   * @throws YarnException if finishApplicationMaster call fails
   * @throws IOException if finishApplicationMaster call fails
   */
  public FinishApplicationMasterResponse finishApplicationMaster(String uamId,
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    LOG.info("Finishing UAM id {} for application {}", uamId,
        this.appIdMap.get(uamId));
    FinishApplicationMasterResponse response =
        this.unmanagedAppMasterMap.get(uamId).finishApplicationMaster(request);

    if (response.getIsUnregistered()) {
      // Only remove the UAM when the unregister finished
      this.unmanagedAppMasterMap.remove(uamId);
      this.appIdMap.remove(uamId);
      LOG.info("UAM id {} is unregistered", uamId);
    }
    return response;
  }

  /**
   * Shutdown an UAM client without killing it in YarnRM.
   *
   * @param uamId uam Id
   * @throws YarnException if fails
   */
  public void shutDownConnections(String uamId)
      throws YarnException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    LOG.info(
        "Shutting down UAM id {} for application {} without killing the UAM",
        uamId, this.appIdMap.get(uamId));
    this.unmanagedAppMasterMap.remove(uamId).shutDownConnections();
  }

  /**
   * Shutdown all UAM clients without killing them in YarnRM.
   *
   * @throws YarnException if fails
   */
  public void shutDownConnections() throws YarnException {
    for (String uamId : this.unmanagedAppMasterMap.keySet()) {
      shutDownConnections(uamId);
    }
  }

  /**
   * Get the id of all running UAMs.
   *
   * @return uamId set
   */
  public Set<String> getAllUAMIds() {
    // Return a clone of the current id set for concurrency reasons, so that the
    // returned map won't change with the actual map
    return new HashSet<>(this.unmanagedAppMasterMap.keySet());
  }

  /**
   * Return whether an UAM exists.
   *
   * @param uamId uam Id
   * @return UAM exists or not
   */
  public boolean hasUAMId(String uamId) {
    return this.unmanagedAppMasterMap.containsKey(uamId);
  }

  /**
   * Return the rmProxy relayer of an UAM.
   *
   * @param uamId uam Id
   * @return the rmProxy relayer
   * @throws YarnException if fails
   */
  public AMRMClientRelayer getAMRMClientRelayer(String uamId)
      throws YarnException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    return this.unmanagedAppMasterMap.get(uamId).getAMRMClientRelayer();
  }

  @VisibleForTesting
  public int getRequestQueueSize(String uamId) throws YarnException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    return this.unmanagedAppMasterMap.get(uamId).getRequestQueueSize();
  }

  @VisibleForTesting
  public void drainUAMHeartbeats() {
    for (UnmanagedApplicationManager uam : this.unmanagedAppMasterMap
        .values()) {
      uam.drainHeartbeatThread();
    }
  }

  /**
   * Complete FinishApplicationMaster interface calls in batches.
   *
   * @param request FinishApplicationMasterRequest
   * @param appId application Id
   * @return Returns the Map,
   *         the key is subClusterId, the value is FinishApplicationMasterResponse
   */
  public Map<String, FinishApplicationMasterResponse> batchFinishApplicationMaster(
      FinishApplicationMasterRequest request, String appId) {

    Map<String, FinishApplicationMasterResponse> responseMap = new HashMap<>();
    Set<String> subClusterIds = this.unmanagedAppMasterMap.keySet();

    if (subClusterIds != null && !subClusterIds.isEmpty()) {
      ExecutorCompletionService<Map<String, FinishApplicationMasterResponse>> finishAppService =
          new ExecutorCompletionService<>(this.threadpool);
      LOG.info("Sending finish application request to {} sub-cluster RMs", subClusterIds.size());

      for (final String subClusterId : subClusterIds) {
        finishAppService.submit(() -> {
          LOG.info("Sending finish application request to RM {}", subClusterId);
          try {
            FinishApplicationMasterResponse uamResponse =
                finishApplicationMaster(subClusterId, request);
            return Collections.singletonMap(subClusterId, uamResponse);
          } catch (Throwable e) {
            LOG.warn("Failed to finish unmanaged application master: " +
                " RM address: {} ApplicationId: {}", subClusterId, appId, e);
            return Collections.singletonMap(subClusterId, null);
          }
        });
      }

      for (int i = 0; i < subClusterIds.size(); ++i) {
        try {
          Future<Map<String, FinishApplicationMasterResponse>> future = finishAppService.take();
          Map<String, FinishApplicationMasterResponse> uamResponse = future.get();
          LOG.debug("Received finish application response from RM: {}", uamResponse.keySet());
          responseMap.putAll(uamResponse);
        } catch (Throwable e) {
          LOG.warn("Failed to finish unmanaged application master: ApplicationId: {}", appId, e);
        }
      }
    }

    return responseMap;
  }

  Runnable createForceFinishApplicationThread() {
    return () -> {

      ExecutorCompletionService<Pair<String, KillApplicationResponse>> completionService =
          new ExecutorCompletionService<>(threadpool);

      // Save a local copy of the key set so that it won't change with the map
      Set<String> addressList = new HashSet<>(unmanagedAppMasterMap.keySet());

      LOG.warn("Abnormal shutdown of UAMPoolManager, still {} UAMs in map", addressList.size());

      for (final String uamId : addressList) {
        completionService.submit(() -> {
          try {
            ApplicationId appId = appIdMap.get(uamId);
            LOG.info("Force-killing UAM id {} for application {}", uamId, appId);
            UnmanagedApplicationManager applicationManager = unmanagedAppMasterMap.remove(uamId);
            KillApplicationResponse response = applicationManager.forceKillApplication();
            return Pair.of(uamId, response);
          } catch (Exception e) {
            LOG.error("Failed to kill unmanaged application master", e);
            return Pair.of(uamId, null);
          }
        });
      }

      for (int i = 0; i < addressList.size(); ++i) {
        try {
          Future<Pair<String, KillApplicationResponse>> future = completionService.take();
          Pair<String, KillApplicationResponse> pairs = future.get();
          String uamId = pairs.getLeft();
          ApplicationId appId = appIdMap.get(uamId);
          KillApplicationResponse response = pairs.getRight();
          if (response == null) {
            throw new YarnException(
                "Failed Force-killing UAM id " + uamId + " for application " + appId);
          }
          LOG.info("Force-killing UAM id = {} for application {} KillCompleted {}.",
              uamId, appId, response.getIsKillCompleted());
        } catch (Exception e) {
          LOG.error("Failed to kill unmanaged application master", e);
        }
      }

      appIdMap.clear();
    };
  }

  public void unAttachUAM(String uamId) {
    if (this.unmanagedAppMasterMap.containsKey(uamId)) {
      UnmanagedApplicationManager appManager = this.unmanagedAppMasterMap.get(uamId);
      appManager.shutDownConnections();
    }
    this.unmanagedAppMasterMap.remove(uamId);
    this.appIdMap.remove(uamId);
  }

  @VisibleForTesting
  protected Map<String, UnmanagedApplicationManager> getUnmanagedAppMasterMap() {
    return unmanagedAppMasterMap;
  }

  @VisibleForTesting
  protected Thread getFinishApplicationThread() {
    return finishApplicationThread;
  }
}
