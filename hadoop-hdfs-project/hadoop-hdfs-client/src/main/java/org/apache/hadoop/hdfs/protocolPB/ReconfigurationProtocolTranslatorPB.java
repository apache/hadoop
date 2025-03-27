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
import java.util.List;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.hdfs.protocol.ReconfigurationProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.StartReconfigurationRequestProto;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.protobuf.RpcController;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * This class is the client side translator to translate the requests made on
 * {@link ReconfigurationProtocol} interfaces to the RPC server implementing
 * {@link ReconfigurationProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ReconfigurationProtocolTranslatorPB implements
    ProtocolMetaInterface, ReconfigurationProtocol, ProtocolTranslator,
    Closeable {
  public static final Logger LOG = LoggerFactory
      .getLogger(ReconfigurationProtocolTranslatorPB.class);

  private static final RpcController NULL_CONTROLLER = null;
  private static final StartReconfigurationRequestProto VOID_START_RECONFIG =
      StartReconfigurationRequestProto.newBuilder().build();

  private static final ListReconfigurablePropertiesRequestProto
      VOID_LIST_RECONFIGURABLE_PROPERTIES =
      ListReconfigurablePropertiesRequestProto.newBuilder().build();

  private static final GetReconfigurationStatusRequestProto
      VOID_GET_RECONFIG_STATUS =
      GetReconfigurationStatusRequestProto.newBuilder().build();

  private final ReconfigurationProtocolPB rpcProxy;

  public ReconfigurationProtocolTranslatorPB(InetSocketAddress addr,
      UserGroupInformation ticket, Configuration conf, SocketFactory factory)
      throws IOException {
    rpcProxy = createReconfigurationProtocolProxy(addr, ticket, conf, factory,
        0);
  }

  static ReconfigurationProtocolPB createReconfigurationProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int socketTimeout) throws IOException {
    RPC.setProtocolEngine(conf, ReconfigurationProtocolPB.class,
        ProtobufRpcEngine2.class);
    return RPC.getProxy(ReconfigurationProtocolPB.class,
        RPC.getProtocolVersion(ReconfigurationProtocolPB.class),
        addr, ticket, conf, factory, socketTimeout);
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public void startReconfiguration() throws IOException {
    ipc(() -> rpcProxy.startReconfiguration(NULL_CONTROLLER, VOID_START_RECONFIG));
  }

  @Override
  public ReconfigurationTaskStatus getReconfigurationStatus()
      throws IOException {
    return ReconfigurationProtocolUtils.getReconfigurationStatus(
        ipc(() -> rpcProxy
            .getReconfigurationStatus(
                NULL_CONTROLLER,
                VOID_GET_RECONFIG_STATUS)));
  }

  @Override
  public List<String> listReconfigurableProperties() throws IOException {
    ListReconfigurablePropertiesResponseProto response;
    response = ipc(() -> rpcProxy.listReconfigurableProperties(NULL_CONTROLLER,
        VOID_LIST_RECONFIGURABLE_PROPERTIES));
    return response.getNameList();
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        ReconfigurationProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(ReconfigurationProtocolPB.class),
        methodName);
  }
}
