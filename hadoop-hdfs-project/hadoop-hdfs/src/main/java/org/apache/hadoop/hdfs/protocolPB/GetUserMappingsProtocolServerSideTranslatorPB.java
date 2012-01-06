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

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GetUserMappingsProtocolServerSideTranslatorPB implements
    GetUserMappingsProtocolPB {

  private final GetUserMappingsProtocol impl;

  public GetUserMappingsProtocolServerSideTranslatorPB(
      GetUserMappingsProtocol impl) {
    this.impl = impl;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(GetUserMappingsProtocolPB.class);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link GetUserMappingsProtocol}
     */
    if (!protocol.equals(RPC
        .getProtocolName(GetUserMappingsProtocolPB.class))) {
      throw new IOException("Namenode Serverside implements "
          + RPC.getProtocolName(GetUserMappingsProtocolPB.class)
          + ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(GetUserMappingsProtocolPB.class),
        GetUserMappingsProtocolPB.class);
  }

  @Override
  public ProtocolSignatureWritable getProtocolSignature2(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link GetUserMappingsProtocolPB}
     */
    return ProtocolSignatureWritable.convert(this.getProtocolSignature(
        protocol, clientVersion, clientMethodsHash));
  }

  @Override
  public GetGroupsForUserResponseProto getGroupsForUser(
      RpcController controller, GetGroupsForUserRequestProto request)
      throws ServiceException {
    String[] groups;
    try {
      groups = impl.getGroupsForUser(request.getUser());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    GetGroupsForUserResponseProto.Builder builder = GetGroupsForUserResponseProto
        .newBuilder();
    for (String g : groups) {
      builder.addGroups(g);
    }
    return builder.build();
  }
}
