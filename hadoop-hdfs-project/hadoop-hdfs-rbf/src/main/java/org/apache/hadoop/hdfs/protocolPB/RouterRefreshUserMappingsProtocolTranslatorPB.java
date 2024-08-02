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

import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import java.io.IOException;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncIpcClient;

public class RouterRefreshUserMappingsProtocolTranslatorPB
    extends RefreshUserMappingsProtocolClientSideTranslatorPB {
  private final RefreshUserMappingsProtocolPB rpcProxy;
  public RouterRefreshUserMappingsProtocolTranslatorPB(RefreshUserMappingsProtocolPB rpcProxy) {
    super(rpcProxy);
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.refreshUserToGroupsMappings();
      return;
    }

    asyncIpcClient(() -> rpcProxy.refreshUserToGroupsMappings(null,
            RefreshUserMappingsProtocolProtos
                .RefreshUserToGroupsMappingsRequestProto
                .newBuilder().build()),
        res -> null, Void.class);
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.refreshSuperUserGroupsConfiguration();
      return;
    }

    asyncIpcClient(() -> rpcProxy.refreshSuperUserGroupsConfiguration(null,
            RefreshUserMappingsProtocolProtos
                .RefreshSuperUserGroupsConfigurationRequestProto
                .newBuilder().build()),
        res -> null, Void.class);
  }
}
