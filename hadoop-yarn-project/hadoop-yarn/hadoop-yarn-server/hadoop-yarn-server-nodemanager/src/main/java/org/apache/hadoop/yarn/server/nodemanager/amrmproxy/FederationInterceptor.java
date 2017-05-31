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
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.utils.AMRMClientUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /** The proxy ugi used to talk to home RM. */
  private UserGroupInformation appOwner;

  /**
   * Creates an instance of the FederationInterceptor class.
   */
  public FederationInterceptor() {
    this.containerIdToSubClusterIdMap = new ConcurrentHashMap<>();
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
          + " from home subcluster. Will use default queue name "
          + YarnConfiguration.DEFAULT_QUEUE_NAME
          + " for getting AMRMProxyPolicy");
    } else {
      LOG.info("Application "
          + getApplicationContext().getApplicationAttemptId().getApplicationId()
          + " belongs to queue " + queue);
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

    try {
      // Split the heart beat request into multiple requests, one for each
      // sub-cluster RM that is used by this application.
      Map<SubClusterId, AllocateRequest> requests =
          splitAllocateRequest(request);

      // Send the request to the home RM and get the response
      AllocateResponse homeResponse = AMRMClientUtils.allocateWithReRegister(
          requests.get(this.homeSubClusterId), this.homeRM,
          this.amRegistrationRequest,
          getApplicationContext().getApplicationAttemptId());

      // If the resource manager sent us a new token, add to the current user
      if (homeResponse.getAMRMToken() != null) {
        LOG.debug("Received new AMRMToken");
        YarnServerSecurityUtils.updateAMRMToken(homeResponse.getAMRMToken(),
            this.appOwner, getConf());
      }

      // Merge the responses from home and secondary sub-cluster RMs
      homeResponse = mergeAllocateResponses(homeResponse);

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

    FinishApplicationMasterResponse homeResponse =
        AMRMClientUtils.finishAMWithReRegister(request, this.homeRM,
            this.amRegistrationRequest,
            getApplicationContext().getApplicationAttemptId());
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
    super.shutdown();
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

    if (!isNullOrEmpty(request.getAskList())) {
      AllocateRequest newRequest = findOrCreateAllocateRequestForSubCluster(
          this.homeSubClusterId, request, requestMap);
      newRequest.getAskList().addAll(request.getAskList());
    }

    if (request.getResourceBlacklistRequest() != null && !isNullOrEmpty(
        request.getResourceBlacklistRequest().getBlacklistAdditions())) {
      for (String resourceName : request.getResourceBlacklistRequest()
          .getBlacklistAdditions()) {
        AllocateRequest newRequest = findOrCreateAllocateRequestForSubCluster(
            this.homeSubClusterId, request, requestMap);
        newRequest.getResourceBlacklistRequest().getBlacklistAdditions()
            .add(resourceName);
      }
    }

    if (request.getResourceBlacklistRequest() != null && !isNullOrEmpty(
        request.getResourceBlacklistRequest().getBlacklistRemovals())) {
      for (String resourceName : request.getResourceBlacklistRequest()
          .getBlacklistRemovals()) {
        AllocateRequest newRequest = findOrCreateAllocateRequestForSubCluster(
            this.homeSubClusterId, request, requestMap);
        newRequest.getResourceBlacklistRequest().getBlacklistRemovals()
            .add(resourceName);
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
                  + " from same subcluster: {}, so ignoring.",
              container.getId(), subClusterId);
        } else {
          // The same container allocation from different subclusters,
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
                  + " . Previous container was from subcluster: "
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
   * Utility method to check if the specified Collection is null or empty
   *
   * @param c the collection object
   * @param <T> element type of the collection
   * @return whether is it is null or empty
   */
  public static <T> boolean isNullOrEmpty(Collection<T> c) {
    return (c == null || c.size() == 0);
  }
}
