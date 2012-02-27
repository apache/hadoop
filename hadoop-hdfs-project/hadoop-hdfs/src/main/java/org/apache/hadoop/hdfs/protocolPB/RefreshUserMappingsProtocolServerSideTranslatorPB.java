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

import org.apache.hadoop.hdfs.protocol.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RefreshUserMappingsProtocolServerSideTranslatorPB implements RefreshUserMappingsProtocolPB {

  private final RefreshUserMappingsProtocol impl;
  
  public RefreshUserMappingsProtocolServerSideTranslatorPB(RefreshUserMappingsProtocol impl) {
    this.impl = impl;
  }
  
  @Override
  public RefreshUserToGroupsMappingsResponseProto 
      refreshUserToGroupsMappings(RpcController controller, 
      RefreshUserToGroupsMappingsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshUserToGroupsMappings();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RefreshUserToGroupsMappingsResponseProto.newBuilder().build();
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponseProto 
      refreshSuperUserGroupsConfiguration(RpcController controller,
      RefreshSuperUserGroupsConfigurationRequestProto request)
      throws ServiceException {
    try {
      impl.refreshSuperUserGroupsConfiguration();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RefreshSuperUserGroupsConfigurationResponseProto.newBuilder()
        .build();
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(RefreshUserMappingsProtocolPB.class);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link RefreshUserMappingsProtocol}
     */
    if (!protocol.equals(RPC
        .getProtocolName(RefreshUserMappingsProtocolPB.class))) {
      throw new IOException("Namenode Serverside implements "
          + RPC.getProtocolName(RefreshUserMappingsProtocolPB.class)
          + ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(RefreshUserMappingsProtocolPB.class),
        RefreshUserMappingsProtocolPB.class);
  }

  @Override
  public ProtocolSignatureWritable getProtocolSignature2(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link RefreshUserMappingsProtocolPB}
     */
    return ProtocolSignatureWritable.convert(this.getProtocolSignature(
        protocol, clientVersion, clientMethodsHash));
  }
}
