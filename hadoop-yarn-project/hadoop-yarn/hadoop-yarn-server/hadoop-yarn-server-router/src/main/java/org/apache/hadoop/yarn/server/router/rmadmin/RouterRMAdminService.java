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

package org.apache.hadoop.yarn.server.router.rmadmin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.security.authorize.RouterPolicyProvider;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * RouterRMAdminService is a service that runs on each router that can be used
 * to intercept and inspect {@code ResourceManagerAdministrationProtocol}
 * messages from client to the cluster resource manager. It listens
 * {@code ResourceManagerAdministrationProtocol} messages from the client and
 * creates a request intercepting pipeline instance for each client. The
 * pipeline is a chain of interceptor instances that can inspect and modify the
 * request/response as needed. The main difference with AMRMProxyService is the
 * protocol they implement.
 */
public class RouterRMAdminService extends AbstractService
    implements ResourceManagerAdministrationProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRMAdminService.class);

  private Server server;
  private InetSocketAddress listenerEndpoint;

  // For each user we store an interceptors' pipeline.
  // For performance issue we use LRU cache to keep in memory the newest ones
  // and remove the oldest used ones.
  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  public RouterRMAdminService() {
    super(RouterRMAdminService.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting Router RMAdmin Service.");
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation.setConfiguration(conf);

    this.listenerEndpoint =
        conf.getSocketAddr(YarnConfiguration.ROUTER_BIND_HOST,
            YarnConfiguration.ROUTER_RMADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_RMADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_RMADMIN_PORT);

    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    this.userPipelineMap = Collections.synchronizedMap(new LRUCacheHashMap<>(maxCacheSize, true));

    Configuration serverConf = new Configuration(conf);

    int numWorkerThreads =
        serverConf.getInt(YarnConfiguration.RM_ADMIN_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_ADMIN_CLIENT_THREAD_COUNT);

    this.server = rpc.getServer(ResourceManagerAdministrationProtocol.class,
        this, listenerEndpoint, serverConf, null, numWorkerThreads);

    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      refreshServiceAcls(conf, RouterPolicyProvider.getInstance());
    }

    this.server.start();
    LOG.info("Router RMAdminService listening on address: {}.", this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping Router RMAdminService.");
    if (this.server != null) {
      this.server.stop();
    }
    userPipelineMap.clear();
    super.serviceStop();
  }

  void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }

  @VisibleForTesting
  public RequestInterceptorChainWrapper getInterceptorChain()
      throws IOException {
    String user = UserGroupInformation.getCurrentUser().getUserName();
    RequestInterceptorChainWrapper chain = userPipelineMap.get(user);
    if (chain != null && chain.getRootInterceptor() != null) {
      return chain;
    }
    return initializePipeline(user);
  }

  /**
   * Gets the Request interceptor chains for all the users.
   *
   * @return the request interceptor chains.
   */
  @VisibleForTesting
  protected Map<String, RequestInterceptorChainWrapper> getPipelines() {
    return this.userPipelineMap;
  }

  /**
   * This method creates and returns reference of the first interceptor in the
   * chain of request interceptor instances.
   *
   * @return the reference of the first interceptor in the chain
   */
  @VisibleForTesting
  protected RMAdminRequestInterceptor createRequestInterceptorChain() {
    Configuration conf = getConfig();
    return RouterServerUtil.createRequestInterceptorChain(conf,
        YarnConfiguration.ROUTER_RMADMIN_INTERCEPTOR_CLASS_PIPELINE,
        YarnConfiguration.DEFAULT_ROUTER_RMADMIN_INTERCEPTOR_CLASS,
        RMAdminRequestInterceptor.class);
  }

  /**
   * Initializes the request interceptor pipeline for the specified user.
   *
   * @param user
   */
  private RequestInterceptorChainWrapper initializePipeline(String user) {
    synchronized (this.userPipelineMap) {
      if (this.userPipelineMap.containsKey(user)) {
        LOG.info("Request to start an already existing user: {}"
            + " was received, so ignoring.", user);
        return userPipelineMap.get(user);
      }

      RequestInterceptorChainWrapper chainWrapper =
          new RequestInterceptorChainWrapper();
      try {
        // We should init the pipeline instance after it is created and then
        // add to the map, to ensure thread safe.
        LOG.info("Initializing request processing pipeline for user: {}.", user);

        RMAdminRequestInterceptor interceptorChain =
            this.createRequestInterceptorChain();
        interceptorChain.init(user);
        chainWrapper.init(interceptorChain);
      } catch (Exception e) {
        LOG.error("Init RMAdminRequestInterceptor error for user: {}.", user, e);
        throw e;
      }

      this.userPipelineMap.put(user, chainWrapper);
      return chainWrapper;
    }
  }

  /**
   * Private structure for encapsulating RequestInterceptor and user instances.
   *
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private RMAdminRequestInterceptor rootInterceptor;

    /**
     * Initializes the wrapper with the specified parameters.
     *
     * @param interceptor the first interceptor in the pipeline
     */
    public synchronized void init(RMAdminRequestInterceptor interceptor) {
      this.rootInterceptor = interceptor;
    }

    /**
     * Gets the root request interceptor.
     *
     * @return the root request interceptor
     */
    public synchronized RMAdminRequestInterceptor getRootInterceptor() {
      return rootInterceptor;
    }

    /**
     * Shutdown the chain of interceptors when the object is destroyed.
     */
    @Override
    protected void finalize() {
      rootInterceptor.shutdown();
    }
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getGroupsForUser(user);
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshQueues(request);

  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshNodes(request);

  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor()
        .refreshSuperUserGroupsConfiguration(request);

  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshUserToGroupsMappings(request);

  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshAdminAcls(request);

  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshServiceAcls(request);

  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().updateNodeResource(request);

  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(
      RefreshNodesResourcesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshNodesResources(request);

  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().addToClusterNodeLabels(request);

  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().removeFromClusterNodeLabels(request);

  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().replaceLabelsOnNode(request);

  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor()
        .checkForDecommissioningNodes(checkForDecommissioningNodesRequest);
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshClusterMaxPriority(request);
  }

  @Override
  public NodesToAttributesMappingResponse mapAttributesToNodes(
      NodesToAttributesMappingRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().mapAttributesToNodes(request);
  }

  @Override
  public DeregisterSubClusterResponse deregisterSubCluster(
      DeregisterSubClusterRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().deregisterSubCluster(request);
  }

  @Override
  public SaveFederationQueuePolicyResponse saveFederationQueuePolicy(
      SaveFederationQueuePolicyRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().saveFederationQueuePolicy(request);
  }

  @Override
  public BatchSaveFederationQueuePoliciesResponse batchSaveFederationQueuePolicies(
      BatchSaveFederationQueuePoliciesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().batchSaveFederationQueuePolicies(request);
  }
}
