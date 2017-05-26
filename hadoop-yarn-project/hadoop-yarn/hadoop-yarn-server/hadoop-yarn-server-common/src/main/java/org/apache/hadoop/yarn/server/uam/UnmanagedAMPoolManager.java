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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.AMRMClientUtils;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

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

  private Map<String, ApplicationAttemptId> attemptIdMap;

  private ExecutorService threadpool;

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
    this.attemptIdMap = new ConcurrentHashMap<>();
    super.serviceStart();
  }

  /**
   * Normally we should finish all applications before stop. If there are still
   * UAMs running, force kill all of them. Do parallel kill because of
   * performance reasons.
   *
   * TODO: move waiting for the kill to finish into a separate thread, without
   * blocking the serviceStop.
   */
  @Override
  protected void serviceStop() throws Exception {
    ExecutorCompletionService<KillApplicationResponse> completionService =
        new ExecutorCompletionService<>(this.threadpool);
    if (this.unmanagedAppMasterMap.isEmpty()) {
      return;
    }

    // Save a local copy of the key set so that it won't change with the map
    Set<String> addressList =
        new HashSet<>(this.unmanagedAppMasterMap.keySet());
    LOG.warn("Abnormal shutdown of UAMPoolManager, still {} UAMs in map",
        addressList.size());

    for (final String uamId : addressList) {
      completionService.submit(new Callable<KillApplicationResponse>() {
        @Override
        public KillApplicationResponse call() throws Exception {
          try {
            LOG.info("Force-killing UAM id " + uamId + " for application "
                + attemptIdMap.get(uamId));
            return unmanagedAppMasterMap.remove(uamId).forceKillApplication();
          } catch (Exception e) {
            LOG.error("Failed to kill unmanaged application master", e);
            return null;
          }
        }
      });
    }

    for (int i = 0; i < addressList.size(); ++i) {
      try {
        Future<KillApplicationResponse> future = completionService.take();
        future.get();
      } catch (Exception e) {
        LOG.error("Failed to kill unmanaged application master", e);
      }
    }
    this.attemptIdMap.clear();
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
   * @return uamId for the UAM
   * @throws YarnException if registerApplicationMaster fails
   * @throws IOException if registerApplicationMaster fails
   */
  public String createAndRegisterNewUAM(
      RegisterApplicationMasterRequest registerRequest, Configuration conf,
      String queueName, String submitter, String appNameSuffix)
      throws YarnException, IOException {
    ApplicationId appId = null;
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

    createAndRegisterNewUAM(appId.toString(), registerRequest, conf, appId,
        queueName, submitter, appNameSuffix);
    return appId.toString();
  }

  /**
   * Create a new UAM and register the application, using the provided uamId and
   * appId.
   *
   * @param uamId identifier for the UAM
   * @param registerRequest RegisterApplicationMasterRequest
   * @param conf configuration for this UAM
   * @param appId application id for the UAM
   * @param queueName queue of the application
   * @param submitter submitter name of the UAM
   * @param appNameSuffix application name suffix for the UAM
   * @return RegisterApplicationMasterResponse
   * @throws YarnException if registerApplicationMaster fails
   * @throws IOException if registerApplicationMaster fails
   */
  public RegisterApplicationMasterResponse createAndRegisterNewUAM(String uamId,
      RegisterApplicationMasterRequest registerRequest, Configuration conf,
      ApplicationId appId, String queueName, String submitter,
      String appNameSuffix) throws YarnException, IOException {

    if (this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " already exists");
    }
    UnmanagedApplicationManager uam =
        createUAM(conf, appId, queueName, submitter, appNameSuffix);
    // Put the UAM into map first before initializing it to avoid additional UAM
    // for the same uamId being created concurrently
    this.unmanagedAppMasterMap.put(uamId, uam);

    RegisterApplicationMasterResponse response = null;
    try {
      LOG.info("Creating and registering UAM id {} for application {}", uamId,
          appId);
      response = uam.createAndRegisterApplicationMaster(registerRequest);
    } catch (Exception e) {
      // Add the map earlier and remove here if register failed because we want
      // to make sure there is only one uam instance per uamId at any given time
      this.unmanagedAppMasterMap.remove(uamId);
      throw e;
    }

    this.attemptIdMap.put(uamId, uam.getAttemptId());
    return response;
  }

  /**
   * Creates the UAM instance. Pull out to make unit test easy.
   *
   * @param conf Configuration
   * @param appId application id
   * @param queueName queue of the application
   * @param submitter submitter name of the application
   * @param appNameSuffix application name suffix
   * @return the UAM instance
   */
  @VisibleForTesting
  protected UnmanagedApplicationManager createUAM(Configuration conf,
      ApplicationId appId, String queueName, String submitter,
      String appNameSuffix) {
    return new UnmanagedApplicationManager(conf, appId, queueName, submitter,
        appNameSuffix);
  }

  /**
   * AllocateAsync to an UAM.
   *
   * @param uamId identifier for the UAM
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
   * @param uamId identifier for the UAM
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
    LOG.info("Finishing application for UAM id {} ", uamId);
    FinishApplicationMasterResponse response =
        this.unmanagedAppMasterMap.get(uamId).finishApplicationMaster(request);

    if (response.getIsUnregistered()) {
      // Only remove the UAM when the unregister finished
      this.unmanagedAppMasterMap.remove(uamId);
      this.attemptIdMap.remove(uamId);
      LOG.info("UAM id {} is unregistered", uamId);
    }
    return response;
  }

  /**
   * Get the id of all running UAMs.
   *
   * @return uamId set
   */
  public Set<String> getAllUAMIds() {
    // Return a clone of the current id set for concurrency reasons, so that the
    // returned map won't change with the actual map
    return new HashSet<String>(this.unmanagedAppMasterMap.keySet());
  }

  /**
   * Return whether an UAM exists.
   *
   * @param uamId identifier for the UAM
   * @return UAM exists or not
   */
  public boolean hasUAMId(String uamId) {
    return this.unmanagedAppMasterMap.containsKey(uamId);
  }

}
