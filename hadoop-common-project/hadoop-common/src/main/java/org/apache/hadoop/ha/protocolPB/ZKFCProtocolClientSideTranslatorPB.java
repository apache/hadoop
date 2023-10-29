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
package org.apache.hadoop.ha.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFCProtocol;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveRequestProto;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.GracefulFailoverRequestProto;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.protobuf.RpcController;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;


public class ZKFCProtocolClientSideTranslatorPB implements
  ZKFCProtocol, Closeable, ProtocolTranslator {

  private final static RpcController NULL_CONTROLLER = null;
  private final ZKFCProtocolPB rpcProxy;

  public ZKFCProtocolClientSideTranslatorPB(
      InetSocketAddress addr, Configuration conf,
      SocketFactory socketFactory, int timeout) throws IOException {
    RPC.setProtocolEngine(conf, ZKFCProtocolPB.class,
        ProtobufRpcEngine2.class);
    rpcProxy = RPC.getProxy(ZKFCProtocolPB.class,
        RPC.getProtocolVersion(ZKFCProtocolPB.class), addr,
        UserGroupInformation.getCurrentUser(), conf, socketFactory, timeout);
  }

  @Override
  public void cedeActive(int millisToCede) throws IOException,
      AccessControlException {
    CedeActiveRequestProto req = CedeActiveRequestProto.newBuilder()
        .setMillisToCede(millisToCede)
        .build();
    ipc(() -> rpcProxy.cedeActive(NULL_CONTROLLER, req));
  }

  @Override
  public void gracefulFailover() throws IOException, AccessControlException {
    ipc(() -> rpcProxy.gracefulFailover(NULL_CONTROLLER,
        GracefulFailoverRequestProto.getDefaultInstance()));
  }


  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
}
