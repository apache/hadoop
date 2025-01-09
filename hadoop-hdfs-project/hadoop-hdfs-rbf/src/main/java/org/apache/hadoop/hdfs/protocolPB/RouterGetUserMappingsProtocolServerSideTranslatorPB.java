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
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterGetUserMappingsProtocolServerSideTranslatorPB
    extends GetUserMappingsProtocolServerSideTranslatorPB {
  private final RouterRpcServer server;
  private final boolean isAsyncRpc;

  public RouterGetUserMappingsProtocolServerSideTranslatorPB(GetUserMappingsProtocol impl) {
    super(impl);
    this.server = (RouterRpcServer) impl;
    this.isAsyncRpc = server.isAsync();
  }

  @Override
  public GetGroupsForUserResponseProto getGroupsForUser(
      RpcController controller,
      GetGroupsForUserRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.getGroupsForUser(controller, request);
    }
    asyncRouterServer(() -> server.getGroupsForUser(request.getUser()), groups -> {
      GetGroupsForUserResponseProto.Builder builder =
          GetGroupsForUserResponseProto
          .newBuilder();
      for (String g : groups) {
        builder.addGroups(g);
      }
      return builder.build();
    });
    return null;
  }
}
