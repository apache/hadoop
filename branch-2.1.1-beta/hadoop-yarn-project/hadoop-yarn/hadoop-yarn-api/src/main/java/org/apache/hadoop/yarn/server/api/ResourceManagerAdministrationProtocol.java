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

package org.apache.hadoop.yarn.server.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;

@Private
@Stable
public interface ResourceManagerAdministrationProtocol extends GetUserMappingsProtocol {

  @Public
  @Stable
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request) 
  throws YarnException, IOException;

  @Public
  @Stable
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
  throws YarnException, IOException;

  @Public
  @Stable
  public RefreshSuperUserGroupsConfigurationResponse 
  refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
  throws YarnException, IOException;

  @Public
  @Stable
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
  throws YarnException, IOException;

  @Public
  @Stable
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request)
  throws YarnException, IOException;

  @Public
  @Stable
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request)
  throws YarnException, IOException;
}
