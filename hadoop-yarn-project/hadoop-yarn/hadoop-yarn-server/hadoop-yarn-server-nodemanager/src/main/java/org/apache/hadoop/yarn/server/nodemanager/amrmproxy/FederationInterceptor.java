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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.uam.UnmanagedAMPoolManager;
import org.apache.hadoop.yarn.server.utils.AMRMClientUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Extends the AbstractRequestInterceptor and provides an implementation for
 * federation of YARN RM and scaling an application across multiple YARN
 * sub-clusters. All the federation specific implementation is encapsulated in
 * this class. This is always the last intercepter in the chain.
 */
public class FederationInterceptor extends AbstractRequestInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationInterceptor.class);

  /**
   * The home sub-cluster is the sub-cluster where the AM container is running
   * in.
   */
  private ApplicationMasterProtocol homeRM;
  private SubClusterId homeSubClusterId;

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

  /** Thread pool used for asynchronous operations. */
  private ExecutorService threadpool;

  /**
   * Stores the AllocateResponses that are received asynchronously from all the
   * sub-cluster resource managers except the home RM.
   */
  private Map<SubClusterId, List<AllocateResponse>> asyncResponseSink;

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
   * The original registration response from home RM. This instance is reused
   * for duplicate register request from AM, triggered by timeout between AM and
   * AMRMProxy.
   */
  private RegisterApplicationMasterResponse amRegistrationResponse;

  private FederationStateStoreFacade federationFacade;

  private SubClusterResolver subClusterResolver;

  /** The policy used to split requests among sub-clusters. */
  private FederationAMRMProxyPolicy policyInterpreter;

  /**
   * The proxy ugi used to talk to home RM, loaded with the up-to-date AMRMToken
   * issued by home RM.
   */
  private UserGroupInformation appOwner;

  /**
   * Creates an instance of the FederationInterceptor class.
   */
  public FederationInterceptor() {
    this.containerIdToSubClusterIdMap = new ConcurrentHashMap<>();
    this.asyncResponseSink = new ConcurrentHashMap<>();
    this.threadpool = Executors.newCachedThreadPool();
    this.uamPool = createUnmanagedAMPoolManager(this.threadpool);
    this.amRegistrationRequest = null;
    this.amRegistrationResponse = null;
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

    try {
      this.appOwner = UserGroupInformation.createProxyUser(appContext.getUser(),
          UserGroupInformation.getCurrentUser());
    } catch (Exception ex) {
      throw new YarnRuntimeException(ex);
    }

    this.homeSubClusterId =
        SubClusterId.newInstance(YarnConfiguration.getClusterId(conf));
    this.homeRM = createHomeRMProxy(appContext);

    this.federationFacade = FederationStateStoreFacade.getInstance();
    this.subClusterResolver = this.federationFacade.getSubClusterResolver();

    // AMRMProxyPolicy will be initialized in registerApplicationMaster
    this.policyInterpreter = null;

    this.uamPool.init(conf);
    this.uamPool.start();
  }

  /**
   * Sends the application master's registration request to the home RM.
   *
   * Between AM and AMRMProxy, FederationInterceptor modifies the RM behavior,
   * so that when AM registers more than once, it returns the same register
   * success response instead of throwing
   * {@link InvalidApplicationMasterRequestException}. Furthermore, we present
   * to AM as if we are the RM that never fails over. When actual RM fails over,
   * we always re-register automatically.
   *
   * We did this because FederationInterceptor can receive concurrent register
   * requests from AM because of timeout between AM and AMRMProxy, which is
   * shorter than the timeout + failOver between FederationInterceptor
   * (AMRMProxy) and RM.
   */
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    // If AM is calling with a different request, complain
    if (this.amRegistrationRequest != null
        && !this.amRegistrationRequest.equals(request)) {
      throw new YarnException("A different request body recieved. AM should"
          + " not call registerApplicationMaster with different request body");
    }

    // Save the registration request. This will be used for registering with
    // secondary sub-clusters using UAMs, as well as re-register later
    this.amRegistrationRequest = request;

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
    try {
      this.amRegistrationResponse =
          this.homeRM.registerApplicationMaster(request);
    } catch (InvalidApplicationMasterRequestException e) {
      if (e.getMessage()
          .contains(AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE)) {
        // Some other register thread might have succeeded in the meantime
        if (this.amRegistrationResponse != null) {
          LOG.info("Other concurrent thread registered successfully, "
              + "simply return the same success register response");
          return this.amRegistrationResponse;
        }
      }
      // This is a real issue, throw back to AM
      throw e;
    }

    // the queue this application belongs will be used for getting
    // AMRMProxy policy from state store.
    String queue = this.amRegistrationResponse.getQueue();
    if (queue == null) {
      LOG.warn("Received null queue for application "
          + getApplicationContext().getApplicationAttemptId().getApplicationId()
          + " from home sub-cluster. Will use default queue name "
          + YarnConfiguration.DEFAULT_QUEUE_NAME
          + " for getting AMRMProxyPolicy");
    } else {
      LOG.info("Application "
          + getApplicationContext().getApplicationAttemptId().getApplicationId()
          + " belongs to queue " + queue);
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
      throws YarnException {
    Preconditions.checkArgument(this.policyInterpreter != null,
        "Allocate should be called after registerApplicationMaster");

    try {
      // Split the heart beat request into multiple requests, one for each
      // sub-cluster RM that is used by this application.
      Map<SubClusterId, AllocateRequest> requests =
          splitAllocateRequest(request);

      // Send the requests to the secondary sub-cluster resource managers.
      // These secondary requests are send asynchronously and the responses will
      // be collected and merged with the home response. In addition, it also
      // return the newly registered Unmanaged AMs.
      Registrations newRegistrations =
          sendRequestsToSecondaryResourceManagers(requests);

      // Send the request to the home RM and get the response
      AllocateResponse homeResponse = AMRMClientUtils.allocateWithReRegister(
          requests.get(this.homeSubClusterId), this.homeRM,
          this.amRegistrationRequest,
          getApplicationContext().getApplicationAttemptId());

      // Notify policy of home response
      try {
        this.policyInterpreter.notifyOfResponse(this.homeSubClusterId,
            homeResponse);
      } catch (YarnException e) {
        LOG.warn("notifyOfResponse for policy failed for home sub-cluster "
            + this.homeSubClusterId, e);
      }

      // If the resource manager sent us a new token, add to the current user
      if (homeResponse.getAMRMToken() != null) {
        LOG.debug("Received new AMRMToken");
        YarnServerSecurityUtils.updateAMRMToken(homeResponse.getAMRMToken(),
            this.appOwner, getConf());
      }

      // Merge the responses from home and secondary sub-cluster RMs
      homeResponse = mergeAllocateResponses(homeResponse);

      // Merge the containers and NMTokens from the new registrations into
      // the homeResponse.
      if (!isNullOrEmpty(newRegistrations.getSuccessfulRegistrations())) {
        homeResponse = mergeRegistrationResponses(homeResponse,
            newRegistrations.getSuccessfulRegistrations());
      }

      // return the final response to the application master.
      return homeResponse;
    } catch (IOException ex) {
      LOG.error("Exception encountered while processing heart beat", ex);
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
            } catch (Throwable e) {
              LOG.warn("Failed to finish unmanaged application master: "
                  + "RM address: " + subClusterId + " ApplicationId: "
                  + getApplicationContext().getApplicationAttemptId(), e);
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
        AMRMClientUtils.finishAMWithReRegister(request, this.homeRM,
            this.amRegistrationRequest,
            getApplicationContext().getApplicationAttemptId());

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
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received finish application response from RM: "
                + uamResponse.getSubClusterId());
          }
          if (uamResponse.getResponse() == null
              || !uamResponse.getResponse().getIsUnregistered()) {
            failedToUnRegister = true;
          }
        } catch (Throwable e) {
          failedToUnRegister = true;
          LOG.warn("Failed to finish unmanaged application master: "
              + " ApplicationId: "
              + getApplicationContext().getApplicationAttemptId(), e);
        }
      }
    }

    if (failedToUnRegister) {
      homeResponse.setIsUnregistered(false);
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
    if (this.uamPool != null) {
      this.uamPool.stop();
    }
    if (threadpool != null) {
      try {
        threadpool.shutdown();
      } catch (Throwable ex) {
      }
      threadpool = null;
    }
    super.shutdown();
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

  /**
   * Returns instance of the ApplicationMasterProtocol proxy class that is used
   * to connect to the Home resource manager.
   *
   * @param appContext AMRMProxyApplicationContext
   * @return the proxy created
   */
  protected ApplicationMasterProtocol createHomeRMProxy(
      AMRMProxyApplicationContext appContext) {
    try {
      return FederationProxyProviderUtil.createRMProxy(appContext.getConf(),
          ApplicationMasterProtocol.class, this.homeSubClusterId, this.appOwner,
          appContext.getAMRMToken());
    } catch (Exception ex) {
      throw new YarnRuntimeException(ex);
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

    if (request.getResourceBlacklistRequest() != null && !isNullOrEmpty(
        request.getResourceBlacklistRequest().getBlacklistAdditions())) {
      for (String resourceName : request.getResourceBlacklistRequest()
          .getBlacklistAdditions()) {
        SubClusterId subClusterId = getSubClusterForNode(resourceName);
        if (subClusterId != null) {
          AllocateRequest newRequest = findOrCreateAllocateRequestForSubCluster(
              subClusterId, request, requestMap);
          newRequest.getResourceBlacklistRequest().getBlacklistAdditions()
              .add(resourceName);
        }
      }
    }

    if (request.getResourceBlacklistRequest() != null && !isNullOrEmpty(
        request.getResourceBlacklistRequest().getBlacklistRemovals())) {
      for (String resourceName : request.getResourceBlacklistRequest()
          .getBlacklistRemovals()) {
        SubClusterId subClusterId = getSubClusterForNode(resourceName);
        if (subClusterId != null) {
          AllocateRequest newRequest = findOrCreateAllocateRequestForSubCluster(
              subClusterId, request, requestMap);
          newRequest.getResourceBlacklistRequest().getBlacklistRemovals()
              .add(resourceName);
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
   * sub-cluster resource managers.
   *
   * @param requests contains the heart beat requests to send to the resource
   *          manager keyed by the resource manager address
   * @return the registration responses from the newly added sub-cluster
   *         resource managers
   * @throws YarnException
   * @throws IOException
   */
  private Registrations sendRequestsToSecondaryResourceManagers(
      Map<SubClusterId, AllocateRequest> requests)
      throws YarnException, IOException {

    // Create new UAM instances for the sub-cluster that we have not seen
    // before
    Registrations registrations = registerWithNewSubClusters(requests.keySet());

    // Now that all the registrations are done, send the allocation request
    // to the sub-cluster RMs using the Unmanaged application masters
    // asynchronously and don't wait for the response. The responses will
    // arrive asynchronously and will be added to the response sink. These
    // responses will be sent to the application master in some future heart
    // beat response.
    for (Entry<SubClusterId, AllocateRequest> entry : requests.entrySet()) {
      final SubClusterId subClusterId = entry.getKey();

      if (subClusterId.equals(this.homeSubClusterId)) {
        // Skip the request for the home sub-cluster resource manager.
        // It will be handled separately in the allocate() method
        continue;
      }

      if (!this.uamPool.hasUAMId(subClusterId.getId())) {
        // TODO: This means that the registration for this sub-cluster RM
        // failed. For now, we ignore the resource requests and continue
        // but we need to fix this and handle this situation. One way would
        // be to send the request to another RM by consulting the policy.
        LOG.warn("Unmanaged AM registration not found for sub-cluster {}",
            subClusterId);
        continue;
      }

      this.uamPool.allocateAsync(subClusterId.getId(), entry.getValue(),
          new AsyncCallback<AllocateResponse>() {
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
              }

              // Notify policy of secondary sub-cluster responses
              try {
                policyInterpreter.notifyOfResponse(subClusterId, response);
              } catch (YarnException e) {
                LOG.warn(
                    "notifyOfResponse for policy failed for home sub-cluster "
                        + subClusterId,
                    e);
              }
            }
          });
    }

    return registrations;
  }

  /**
   * This method ensures that Unmanaged AMs are created for each of the
   * specified sub-cluster specified in the input and registers with the
   * corresponding resource managers.
   */
  private Registrations registerWithNewSubClusters(
      Set<SubClusterId> subClusterSet) throws IOException {

    List<SubClusterId> failedRegistrations = new ArrayList<>();
    Map<SubClusterId, RegisterApplicationMasterResponse>
        successfulRegistrations = new HashMap<>();

    // Check to see if there are any new sub-clusters in this request
    // list and create and register Unmanaged AM instance for the new ones
    List<String> newSubClusters = new ArrayList<>();
    for (SubClusterId subClusterId : subClusterSet) {
      if (!subClusterId.equals(this.homeSubClusterId)
          && !this.uamPool.hasUAMId(subClusterId.getId())) {
        newSubClusters.add(subClusterId.getId());
      }
    }

    if (newSubClusters.size() > 0) {
      final RegisterApplicationMasterRequest registerRequest =
          this.amRegistrationRequest;
      final AMRMProxyApplicationContext appContext = getApplicationContext();
      ExecutorCompletionService<RegisterApplicationMasterResponseInfo>
          completionService = new ExecutorCompletionService<>(threadpool);

      for (final String subClusterId : newSubClusters) {
        completionService
            .submit(new Callable<RegisterApplicationMasterResponseInfo>() {
              @Override
              public RegisterApplicationMasterResponseInfo call()
                  throws Exception {

                // Create a config loaded with federation on and subclusterId
                // for each UAM
                YarnConfiguration config = new YarnConfiguration(getConf());
                FederationProxyProviderUtil.updateConfForFederation(config,
                    subClusterId);

                RegisterApplicationMasterResponse uamResponse = null;
                try {
                  // For appNameSuffix, use subClusterId of the home sub-cluster
                  uamResponse = uamPool.createAndRegisterNewUAM(subClusterId,
                      registerRequest, config,
                      appContext.getApplicationAttemptId().getApplicationId(),
                      amRegistrationResponse.getQueue(), appContext.getUser(),
                      homeSubClusterId.toString());
                } catch (Throwable e) {
                  LOG.error("Failed to register application master: "
                      + subClusterId + " Application: "
                      + appContext.getApplicationAttemptId(), e);
                }
                return new RegisterApplicationMasterResponseInfo(uamResponse,
                    SubClusterId.newInstance(subClusterId));
              }
            });
      }

      // Wait for other sub-cluster resource managers to return the
      // response and add it to the Map for returning to the caller
      for (int i = 0; i < newSubClusters.size(); ++i) {
        try {
          Future<RegisterApplicationMasterResponseInfo> future =
              completionService.take();
          RegisterApplicationMasterResponseInfo uamResponse = future.get();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received register application response from RM: "
                + uamResponse.getSubClusterId());
          }

          if (uamResponse.getResponse() == null) {
            failedRegistrations.add(uamResponse.getSubClusterId());
          } else {
            LOG.info("Successfully registered unmanaged application master: "
                + uamResponse.getSubClusterId() + " ApplicationId: "
                + getApplicationContext().getApplicationAttemptId());
            successfulRegistrations.put(uamResponse.getSubClusterId(),
                uamResponse.getResponse());
          }
        } catch (Exception e) {
          LOG.warn("Failed to register unmanaged application master: "
              + " ApplicationId: "
              + getApplicationContext().getApplicationAttemptId(), e);
        }
      }
    }

    return new Registrations(successfulRegistrations, failedRegistrations);
  }

  /**
   * Merges the responses from other sub-clusters that we received
   * asynchronously with the specified home cluster response and keeps track of
   * the containers received from each sub-cluster resource managers.
   */
  private AllocateResponse mergeAllocateResponses(
      AllocateResponse homeResponse) {
    // Timing issue, we need to remove the completed and then save the new ones.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Remove containers: "
          + homeResponse.getCompletedContainersStatuses());
      LOG.debug("Adding containers: " + homeResponse.getAllocatedContainers());
    }
    removeFinishedContainersFromCache(
        homeResponse.getCompletedContainersStatuses());
    cacheAllocatedContainers(homeResponse.getAllocatedContainers(),
        this.homeSubClusterId);

    synchronized (this.asyncResponseSink) {
      for (Entry<SubClusterId, List<AllocateResponse>> entry : asyncResponseSink
          .entrySet()) {
        SubClusterId subClusterId = entry.getKey();
        List<AllocateResponse> responses = entry.getValue();
        if (responses.size() > 0) {
          for (AllocateResponse response : responses) {
            removeFinishedContainersFromCache(
                response.getCompletedContainersStatuses());
            cacheAllocatedContainers(response.getAllocatedContainers(),
                subClusterId);
            mergeAllocateResponse(homeResponse, response, subClusterId);
          }
          responses.clear();
        }
      }
    }

    return homeResponse;
  }

  /**
   * Removes the finished containers from the local cache.
   */
  private void removeFinishedContainersFromCache(
      List<ContainerStatus> finishedContainers) {
    for (ContainerStatus container : finishedContainers) {
      if (containerIdToSubClusterIdMap
          .containsKey(container.getContainerId())) {
        containerIdToSubClusterIdMap.remove(container.getContainerId());
      }
    }
  }

  /**
   * Helper method for merging the responses from the secondary sub cluster RMs
   * with the home response to return to the AM.
   */
  private AllocateResponse mergeRegistrationResponses(
      AllocateResponse homeResponse,
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

    return homeResponse;
  }

  private void mergeAllocateResponse(AllocateResponse homeResponse,
      AllocateResponse otherResponse, SubClusterId otherRMAddress) {

    if (!isNullOrEmpty(otherResponse.getAllocatedContainers())) {
      if (!isNullOrEmpty(homeResponse.getAllocatedContainers())) {
        homeResponse.getAllocatedContainers()
            .addAll(otherResponse.getAllocatedContainers());
      } else {
        homeResponse
            .setAllocatedContainers(otherResponse.getAllocatedContainers());
      }
    }

    if (otherResponse.getAvailableResources() != null) {
      if (homeResponse.getAvailableResources() != null) {
        homeResponse.setAvailableResources(
            Resources.add(homeResponse.getAvailableResources(),
                otherResponse.getAvailableResources()));
      } else {
        homeResponse
            .setAvailableResources(otherResponse.getAvailableResources());
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

    if (!isNullOrEmpty(otherResponse.getNMTokens())) {
      if (!isNullOrEmpty(homeResponse.getNMTokens())) {
        homeResponse.getNMTokens().addAll(otherResponse.getNMTokens());
      } else {
        homeResponse.setNMTokens(otherResponse.getNMTokens());
      }
    }

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
        par2.getContainers().addAll(par2.getContainers());
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
  }

  /**
   * Add allocated containers to cache mapping.
   */
  private void cacheAllocatedContainers(List<Container> containers,
      SubClusterId subClusterId) {
    for (Container container : containers) {
      if (containerIdToSubClusterIdMap.containsKey(container.getId())) {
        SubClusterId existingSubClusterId =
            containerIdToSubClusterIdMap.get(container.getId());
        if (existingSubClusterId.equals(subClusterId)) {
          // When RM fails over, the new RM master might send out the same
          // container allocation more than once. Just move on in this case.
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
                  + " ApplicationId: "
                  + getApplicationContext().getApplicationAttemptId()
                  + " From RM: " + subClusterId
                  + " . Previous container was from sub-cluster: "
                  + existingSubClusterId);
        }
      }

      containerIdToSubClusterIdMap.put(container.getId(), subClusterId);
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
        AllocateRequest.newInstance(0, 0, null, null, null);
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
      LOG.error("AM is trying to {} a container {} that does not exist. ",
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
    return this.policyInterpreter.splitResourceRequests(askList);
  }

  @VisibleForTesting
  public int getUnmanagedAMPoolSize() {
    return this.uamPool.getAllUAMIds().size();
  }

  /**
   * Private structure for encapsulating SubClusterId and
   * RegisterApplicationMasterResponse instances.
   */
  private static class RegisterApplicationMasterResponseInfo {
    private RegisterApplicationMasterResponse response;
    private SubClusterId subClusterId;

    RegisterApplicationMasterResponseInfo(
        RegisterApplicationMasterResponse response, SubClusterId subClusterId) {
      this.response = response;
      this.subClusterId = subClusterId;
    }

    public RegisterApplicationMasterResponse getResponse() {
      return response;
    }

    public SubClusterId getSubClusterId() {
      return subClusterId;
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
   * Private structure for encapsulating successful and failed application
   * master registration responses.
   */
  private static class Registrations {
    private Map<SubClusterId, RegisterApplicationMasterResponse>
        successfulRegistrations;
    private List<SubClusterId> failedRegistrations;

    Registrations(
        Map<SubClusterId, RegisterApplicationMasterResponse>
            successfulRegistrations,
        List<SubClusterId> failedRegistrations) {
      this.successfulRegistrations = successfulRegistrations;
      this.failedRegistrations = failedRegistrations;
    }

    public Map<SubClusterId, RegisterApplicationMasterResponse>
        getSuccessfulRegistrations() {
      return this.successfulRegistrations;
    }

    public List<SubClusterId> getFailedRegistrations() {
      return this.failedRegistrations;
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
