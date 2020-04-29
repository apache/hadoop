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
package org.apache.hadoop.ipc;

import org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolVersionProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * This class serves the requests for protocol versions and signatures by
 * looking them up in the server registry.
 */
public class ProtocolMetaInfoServerSideTranslatorPB implements
    ProtocolMetaInfoPB {

  RPC.Server server;
  
  public ProtocolMetaInfoServerSideTranslatorPB(RPC.Server server) {
    this.server = server;
  }
  
  @Override
  public GetProtocolVersionsResponseProto getProtocolVersions(
      RpcController controller, GetProtocolVersionsRequestProto request)
      throws ServiceException {
    String protocol = request.getProtocol();
    GetProtocolVersionsResponseProto.Builder builder = 
        GetProtocolVersionsResponseProto.newBuilder();
    for (RPC.RpcKind r : RPC.RpcKind.values()) {
      long[] versions;
      try {
        versions = getProtocolVersionForRpcKind(r, protocol);
      } catch (ClassNotFoundException e) {
        throw new ServiceException(e);
      }
      ProtocolVersionProto.Builder b = ProtocolVersionProto.newBuilder();
      if (versions != null) {
        b.setRpcKind(r.toString());
        for (long v : versions) {
          b.addVersions(v);
        }
      }
      builder.addProtocolVersions(b.build());
    }
    return builder.build();
  }

  @Override
  public GetProtocolSignatureResponseProto getProtocolSignature(
      RpcController controller, GetProtocolSignatureRequestProto request)
      throws ServiceException {
    GetProtocolSignatureResponseProto.Builder builder = GetProtocolSignatureResponseProto
        .newBuilder();
    String protocol = request.getProtocol();
    String rpcKind = request.getRpcKind();
    long[] versions;
    try {
      versions = getProtocolVersionForRpcKind(RPC.RpcKind.valueOf(rpcKind),
          protocol);
    } catch (ClassNotFoundException e1) {
      throw new ServiceException(e1);
    }
    if (versions == null) {
      return builder.build();
    }
    for (long v : versions) {
      ProtocolSignatureProto.Builder sigBuilder = ProtocolSignatureProto
          .newBuilder();
      sigBuilder.setVersion(v);
      try {
        ProtocolSignature signature = ProtocolSignature.getProtocolSignature(
            protocol, v);
        for (int m : signature.getMethods()) {
          sigBuilder.addMethods(m);
        }
      } catch (ClassNotFoundException e) {
        throw new ServiceException(e);
      }
      builder.addProtocolSignature(sigBuilder.build());
    }
    return builder.build();
  }
  
  private long[] getProtocolVersionForRpcKind(RPC.RpcKind rpcKind,
      String protocol) throws ClassNotFoundException {
    Class<?> protocolClass = Class.forName(protocol);
    String protocolName = RPC.getProtocolName(protocolClass);
    VerProtocolImpl[] vers = server.getSupportedProtocolVersions(rpcKind,
        protocolName);
    if (vers == null) {
      return null;
    }
    long [] versions = new long[vers.length];
    for (int i=0; i<versions.length; i++) {
      versions[i] = vers[i].version;
    }
    return versions;
  }
}
