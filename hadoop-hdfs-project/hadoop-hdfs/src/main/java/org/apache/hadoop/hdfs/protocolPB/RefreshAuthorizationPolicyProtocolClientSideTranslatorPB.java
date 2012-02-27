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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.ipc.RpcPayloadHeader.RpcKind;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RefreshAuthorizationPolicyProtocolClientSideTranslatorPB implements
    ProtocolMetaInterface, RefreshAuthorizationPolicyProtocol, Closeable {

  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final RefreshAuthorizationPolicyProtocolPB rpcProxy;

  public RefreshAuthorizationPolicyProtocolClientSideTranslatorPB(
      InetSocketAddress nameNodeAddr, UserGroupInformation ugi,
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, RefreshAuthorizationPolicyProtocolPB.class,
        ProtobufRpcEngine.class);
    rpcProxy = RPC.getProxy(RefreshAuthorizationPolicyProtocolPB.class,
        RPC.getProtocolVersion(RefreshAuthorizationPolicyProtocolPB.class),
        NameNode.getAddress(conf), ugi, conf,
        NetUtils.getSocketFactory(conf, RefreshAuthorizationPolicyProtocol.class));
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return rpcProxy.getProtocolVersion(protocol, clientVersion);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignatureWritable.convert(rpcProxy.getProtocolSignature2(
        protocol, clientVersion, clientMethodsHash));
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public void refreshServiceAcl() throws IOException {
    RefreshServiceAclRequestProto request = RefreshServiceAclRequestProto
        .newBuilder().build();
    try {
      rpcProxy.refreshServiceAcl(NULL_CONTROLLER, request);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        RefreshAuthorizationPolicyProtocolPB.class,
        RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(RefreshAuthorizationPolicyProtocolPB.class),
        methodName);
  }
}
