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

package org.apache.hadoop.security.protocolPB;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.thirdparty.protobuf.RpcController;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

public class RefreshUserMappingsProtocolClientSideTranslatorPB implements
    ProtocolMetaInterface, RefreshUserMappingsProtocol, Closeable {

  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final RefreshUserMappingsProtocolPB rpcProxy;
  
  private final static RefreshUserToGroupsMappingsRequestProto 
  VOID_REFRESH_USER_TO_GROUPS_MAPPING_REQUEST = 
      RefreshUserToGroupsMappingsRequestProto.newBuilder().build();

  private final static RefreshSuperUserGroupsConfigurationRequestProto
  VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_REQUEST = 
      RefreshSuperUserGroupsConfigurationRequestProto.newBuilder().build();

  public RefreshUserMappingsProtocolClientSideTranslatorPB(
      RefreshUserMappingsProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    ipc(() -> rpcProxy.refreshUserToGroupsMappings(NULL_CONTROLLER,
        VOID_REFRESH_USER_TO_GROUPS_MAPPING_REQUEST));
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() throws IOException {
    ipc(() -> rpcProxy.refreshSuperUserGroupsConfiguration(NULL_CONTROLLER,
        VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_REQUEST));
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil
        .isMethodSupported(rpcProxy, RefreshUserMappingsProtocolPB.class,
            RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            RPC.getProtocolVersion(RefreshUserMappingsProtocolPB.class),
            methodName);
  }
}
