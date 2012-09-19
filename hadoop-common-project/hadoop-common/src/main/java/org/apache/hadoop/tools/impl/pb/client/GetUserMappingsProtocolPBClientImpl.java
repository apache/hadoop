/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocol.GetGroupsForUserRequestProto;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocol.GetGroupsForUserResponseProto;

import com.google.protobuf.ServiceException;

public class GetUserMappingsProtocolPBClientImpl implements
    ProtocolMetaInterface, GetUserMappingsProtocol, Closeable {

  private GetUserMappingsProtocolPB proxy;
  
  public GetUserMappingsProtocolPBClientImpl(
      long clientVersion, InetSocketAddress addr, Configuration conf)
      throws IOException {
    RPC.setProtocolEngine(conf, GetUserMappingsProtocolPB.class,
        ProtobufRpcEngine.class);
    proxy = (GetUserMappingsProtocolPB) RPC.getProxy(
        GetUserMappingsProtocolPB.class, clientVersion, addr, conf);
  }
  
  public GetUserMappingsProtocolPBClientImpl(
      GetUserMappingsProtocolPB proxy) {
    this.proxy = proxy;
  }
  
  @Override
  public void close() throws IOException {
    RPC.stopProxy(proxy);
  }
  
  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    GetGroupsForUserRequestProto requestProto = 
        GetGroupsForUserRequestProto.newBuilder().setUser(user).build();
    try {
      GetGroupsForUserResponseProto responseProto =
          proxy.getGroupsForUser(null, requestProto);
      return (String[]) responseProto.getGroupsList().toArray(
          new String[responseProto.getGroupsCount()]);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(proxy,
        GetUserMappingsProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(GetUserMappingsProtocolPB.class), methodName);
  }
}
