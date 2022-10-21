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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
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
import java.util.concurrent.ConcurrentHashMap;

public class FederationRMAdminInterceptor extends AbstractRMAdminRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRMAdminInterceptor.class);

  private Map<SubClusterId, ResourceManagerAdministrationProtocol> adminRMProxies;
  private FederationStateStoreFacade federationFacade;
  private final Clock clock = new MonotonicClock();
  private RouterMetrics routerMetrics;
  private Configuration conf;

  @Override
  public void init(String userName) {
    super.init(userName);
    federationFacade = FederationStateStoreFacade.getInstance();
    this.conf = this.getConf();
    this.adminRMProxies = new ConcurrentHashMap<>();
    routerMetrics = RouterMetrics.getMetrics();
  }

  @VisibleForTesting
  protected ResourceManagerAdministrationProtocol getClientRMProxyForSubCluster(
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
      RouterServerUtil.logAndThrowException(
          "Unable to create the interface to reach the SubCluster " + subClusterId, e);
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

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {
    throw new NotImplementedException();
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
}
