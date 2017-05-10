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
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Extends the {@link AbstractRMAdminRequestInterceptor} class and provides an
 * implementation that simply forwards the client requests to the cluster
 * resource manager.
 *
 */
public class DefaultRMAdminRequestInterceptor
    extends AbstractRMAdminRequestInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultRMAdminRequestInterceptor.class);
  private ResourceManagerAdministrationProtocol rmAdminProxy;
  private UserGroupInformation user = null;

  @Override
  public void init(String userName) {
    super.init(userName);
    try {
      // Do not create a proxy user if user name matches the user name on
      // current UGI
      if (userName.equalsIgnoreCase(
          UserGroupInformation.getCurrentUser().getUserName())) {
        user = UserGroupInformation.getCurrentUser();
      } else {
        user = UserGroupInformation.createProxyUser(userName,
            UserGroupInformation.getCurrentUser());
      }

      final Configuration conf = this.getConf();

      rmAdminProxy = user.doAs(
          new PrivilegedExceptionAction<ResourceManagerAdministrationProtocol>() {
            @Override
            public ResourceManagerAdministrationProtocol run()
                throws Exception {
              return ClientRMProxy.createRMProxy(conf,
                  ResourceManagerAdministrationProtocol.class);
            }
          });
    } catch (IOException e) {
      String message = "Error while creating Router RMAdmin Service for user:";
      if (user != null) {
        message += ", user: " + user;
      }

      LOG.info(message);
      throw new YarnRuntimeException(message, e);
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public void setNextInterceptor(RMAdminRequestInterceptor next) {
    throw new YarnRuntimeException("setNextInterceptor is being called on "
        + "DefaultRMAdminRequestInterceptor, which should be the last one "
        + "in the chain. Check if the interceptor pipeline configuration "
        + "is correct");
  }

  @VisibleForTesting
  public void setRMAdmin(ResourceManagerAdministrationProtocol rmAdmin) {
    this.rmAdminProxy = rmAdmin;

  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {
    return rmAdminProxy.refreshQueues(request);
  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {
    return rmAdminProxy.refreshNodes(request);
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws StandbyException, YarnException, IOException {
    return rmAdminProxy.refreshSuperUserGroupsConfiguration(request);
  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws StandbyException, YarnException, IOException {
    return rmAdminProxy.refreshUserToGroupsMappings(request);
  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnException, IOException {
    return rmAdminProxy.refreshAdminAcls(request);
  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnException, IOException {
    return rmAdminProxy.refreshServiceAcls(request);
  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) throws YarnException, IOException {
    return rmAdminProxy.updateNodeResource(request);
  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(
      RefreshNodesResourcesRequest request) throws YarnException, IOException {
    return rmAdminProxy.refreshNodesResources(request);
  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request) throws YarnException, IOException {
    return rmAdminProxy.addToClusterNodeLabels(request);
  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    return rmAdminProxy.removeFromClusterNodeLabels(request);
  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {
    return rmAdminProxy.replaceLabelsOnNode(request);
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {
    return rmAdminProxy
        .checkForDecommissioningNodes(checkForDecommissioningNodesRequest);
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request)
      throws YarnException, IOException {
    return rmAdminProxy.refreshClusterMaxPriority(request);
  }

  @Override
  public String[] getGroupsForUser(String userName) throws IOException {
    return rmAdminProxy.getGroupsForUser(userName);
  }
}
