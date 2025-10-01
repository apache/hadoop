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
package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterRefreshUserMappingsProtocolServerSideTranslatorPB
    extends RefreshUserMappingsProtocolServerSideTranslatorPB {

  private final RouterRpcServer server;
  private final boolean isAsyncRpc;

  private final static RefreshUserToGroupsMappingsResponseProto
      VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE =
      RefreshUserToGroupsMappingsResponseProto.newBuilder().build();

  private final static RefreshSuperUserGroupsConfigurationResponseProto
      VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE =
      RefreshSuperUserGroupsConfigurationResponseProto.newBuilder()
          .build();

  public RouterRefreshUserMappingsProtocolServerSideTranslatorPB(
      RefreshUserMappingsProtocol impl) {
    super(impl);
    this.server = (RouterRpcServer) impl;
    this.isAsyncRpc = server.isAsync();
  }

  @Override
  public RefreshUserToGroupsMappingsResponseProto refreshUserToGroupsMappings(
      RpcController controller, RefreshUserToGroupsMappingsRequestProto request)
      throws ServiceException {
    if (!isAsyncRpc) {
      return super.refreshUserToGroupsMappings(controller, request);
    }
    asyncRouterServer(() -> {
      server.refreshUserToGroupsMappings();
      return null;
    }, result ->
        VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE);
    return null;
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponseProto refreshSuperUserGroupsConfiguration(
      RpcController controller,
      RefreshSuperUserGroupsConfigurationRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.refreshSuperUserGroupsConfiguration(controller, request);
    }
    asyncRouterServer(() -> {
      server.refreshSuperUserGroupsConfiguration();
      return null;
    }, result ->
        VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE);
    return null;
  }
}
