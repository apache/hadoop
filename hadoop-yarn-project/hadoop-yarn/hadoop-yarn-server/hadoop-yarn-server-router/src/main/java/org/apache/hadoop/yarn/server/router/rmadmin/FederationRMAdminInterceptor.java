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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingResponse;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

public class FederationRMAdminInterceptor extends AbstractRMAdminRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRMAdminInterceptor.class);

  private Map<SubClusterId, ResourceManagerAdministrationProtocol> adminRMProxies;
  private FederationStateStoreFacade federationFacade;
  private final Clock clock = new MonotonicClock();
  private RouterMetrics routerMetrics;
  private ThreadPoolExecutor executorService;
  private Configuration conf;

  @Override
  public void init(String userName) {
    super.init(userName);

    int numThreads = getConf().getInt(
        YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREADS_SIZE);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router RMAdminClient-" + userName + "-%d ").build();

    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    this.executorService = new ThreadPoolExecutor(numThreads, numThreads,
        0L, TimeUnit.MILLISECONDS, workQueue, threadFactory);

    federationFacade = FederationStateStoreFacade.getInstance();
    this.conf = this.getConf();
    this.adminRMProxies = new ConcurrentHashMap<>();
    routerMetrics = RouterMetrics.getMetrics();
  }

  @VisibleForTesting
  protected ResourceManagerAdministrationProtocol getAdminRMProxyForSubCluster(
      SubClusterId subClusterId) throws YarnException {

    if (adminRMProxies.containsKey(subClusterId)) {
      return adminRMProxies.get(subClusterId);
    }

    ResourceManagerAdministrationProtocol adminRMProxy = null;
    try {
      boolean serviceAuthEnabled = this.conf.getBoolean(
          CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
      UserGroupInformation realUser = user;
      if (serviceAuthEnabled) {
        realUser = UserGroupInformation.createProxyUser(
            user.getShortUserName(), UserGroupInformation.getLoginUser());
      }
      adminRMProxy = FederationProxyProviderUtil.createRMProxy(getConf(),
          ResourceManagerAdministrationProtocol.class, subClusterId, realUser);
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException(e,
          "Unable to create the interface to reach the SubCluster %s", subClusterId);
    }
    adminRMProxies.put(subClusterId, adminRMProxy);
    return adminRMProxy;
  }

  @Override
  public void setNextInterceptor(RMAdminRequestInterceptor next) {
    throw new YarnRuntimeException("setNextInterceptor is being called on "
       + "FederationRMAdminRequestInterceptor, which should be the last one "
       + "in the chain. Check if the interceptor pipeline configuration "
       + "is correct");
  }

  /**
   * Refresh queue requests.
   *
   * The Router supports refreshing all SubCluster queues at once,
   * and also supports refreshing queues by SubCluster.
   *
   * @param request RefreshQueuesRequest, If subClusterId is not empty,
   * it means that we want to refresh the queue of the specified subClusterId.
   * If subClusterId is empty, it means we want to refresh all queues.
   *
   * @return RefreshQueuesResponse, There is no specific information in the response,
   * as long as it is not empty, it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions from yarn servers.
   * @throws IOException io error occurs.
   */
  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {

    // parameter verification.
    if (request == null) {
      routerMetrics.incrRefreshQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing RefreshQueues request.", null);
    }

    // call refreshQueues of activeSubClusters.
    try {
      long startTime = clock.getTime();
      RMAdminProtocolMethod remoteMethod = new RMAdminProtocolMethod(
           new Class[] {RefreshQueuesRequest.class}, new Object[] {request});

      String subClusterId = request.getSubClusterId();
      Collection<RefreshQueuesResponse> refreshQueueResps =
          remoteMethod.invokeConcurrent(this, RefreshQueuesResponse.class, subClusterId);

      // If we get the return result from refreshQueueResps,
      // it means that the call has been successful,
      // and the RefreshQueuesResponse method can be reconstructed and returned.
      if (CollectionUtils.isNotEmpty(refreshQueueResps)) {
        long stopTime = clock.getTime();
        routerMetrics.succeededRefreshQueuesRetrieved(stopTime - startTime);
        return RefreshQueuesResponse.newInstance();
      }
    } catch (YarnException e) {
      routerMetrics.incrRefreshQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException(e, "Unable to refreshQueue due to exception.");
    }

    routerMetrics.incrRefreshQueuesFailedRetrieved();
    throw new YarnException("Unable to refreshQueue.");
  }

  /**
   * Refresh node requests.
   *
   * The Router supports refreshing all SubCluster nodes at once,
   * and also supports refreshing node by SubCluster.
   *
   * @param request RefreshNodesRequest, If subClusterId is not empty,
   * it means that we want to refresh the node of the specified subClusterId.
   * If subClusterId is empty, it means we want to refresh all nodes.
   *
   * @return RefreshNodesResponse, There is no specific information in the response,
   * as long as it is not empty, it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions from yarn servers.
   * @throws IOException io error occurs.
   */
  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {

    // parameter verification.
    // We will not check whether the DecommissionType is empty,
    // because this parameter has a default value at the proto level.
    if (request == null) {
      routerMetrics.incrRefreshNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing RefreshNodes request.", null);
    }

    // call refreshNodes of activeSubClusters.
    try {
      long startTime = clock.getTime();
      RMAdminProtocolMethod remoteMethod = new RMAdminProtocolMethod(
          new Class[] {RefreshNodesRequest.class}, new Object[] {request});

      String subClusterId = request.getSubClusterId();
      Collection<RefreshNodesResponse> refreshNodesResps =
          remoteMethod.invokeConcurrent(this, RefreshNodesResponse.class, subClusterId);

      if (CollectionUtils.isNotEmpty(refreshNodesResps)) {
        long stopTime = clock.getTime();
        routerMetrics.succeededRefreshNodesRetrieved(stopTime - startTime);
        return RefreshNodesResponse.newInstance();
      }
    } catch (YarnException e) {
      routerMetrics.incrRefreshNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException(e, "Unable to refreshNodes due to exception.");
    }

    routerMetrics.incrRefreshNodesFailedRetrieved();
    throw new YarnException("Unable to refreshNodes.");
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws StandbyException, YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws StandbyException, YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(RefreshAdminAclsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(RefreshServiceAclsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(UpdateNodeResourceRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(RefreshNodesResourcesRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(ReplaceLabelsOnNodeRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public NodesToAttributesMappingResponse mapAttributesToNodes(
      NodesToAttributesMappingRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    return new String[0];
  }

  @VisibleForTesting
  public FederationStateStoreFacade getFederationFacade() {
    return federationFacade;
  }

  @VisibleForTesting
  public ThreadPoolExecutor getExecutorService() {
    return executorService;
  }
}
