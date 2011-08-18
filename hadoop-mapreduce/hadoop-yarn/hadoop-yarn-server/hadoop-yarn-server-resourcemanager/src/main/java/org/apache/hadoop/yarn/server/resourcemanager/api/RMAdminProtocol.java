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

package org.apache.hadoop.yarn.server.resourcemanager.api;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshUserToGroupsMappingsResponse;

public interface RMAdminProtocol {
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request) 
  throws YarnRemoteException;
  
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
  throws YarnRemoteException;
  
  public RefreshSuperUserGroupsConfigurationResponse 
  refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
  throws YarnRemoteException;

  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
  throws YarnRemoteException;
  
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request)
  throws YarnRemoteException;
}
