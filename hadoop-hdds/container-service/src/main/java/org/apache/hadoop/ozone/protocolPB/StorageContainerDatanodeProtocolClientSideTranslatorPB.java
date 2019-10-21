/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMDatanodeRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMDatanodeRequest.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMDatanodeResponse;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisterRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.Type;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * This class is the client-side translator to translate the requests made on
 * the {@link StorageContainerDatanodeProtocol} interface to the RPC server
 * implementing {@link StorageContainerDatanodeProtocolPB}.
 */
public class StorageContainerDatanodeProtocolClientSideTranslatorPB
    implements StorageContainerDatanodeProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private final StorageContainerDatanodeProtocolPB rpcProxy;

  /**
   * Constructs a Client side interface that calls into SCM datanode protocol.
   *
   * @param rpcProxy - Proxy for RPC.
   */
  public StorageContainerDatanodeProtocolClientSideTranslatorPB(
      StorageContainerDatanodeProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the close may
   * fail require careful attention. It is strongly advised to relinquish the
   * underlying resources and to internally <em>mark</em> the {@code Closeable}
   * as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  /**
   * Return the proxy object underlying this protocol translator.
   *
   * @return the proxy object underlying this protocol translator.
   */
  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  /**
   * Helper method to wrap the request and send the message.
   */
  private SCMDatanodeResponse submitRequest(Type type,
      Consumer<SCMDatanodeRequest.Builder> builderConsumer) throws IOException {
    final SCMDatanodeResponse response;
    try {
      Builder builder = SCMDatanodeRequest.newBuilder()
          .setCmdType(type);
      builderConsumer.accept(builder);
      SCMDatanodeRequest wrapper = builder.build();

      response = rpcProxy.submitRequest(NULL_RPC_CONTROLLER, wrapper);
    } catch (ServiceException ex) {
      throw ProtobufHelper.getRemoteException(ex);
    }
    return response;
  }

  /**
   * Returns SCM version.
   *
   * @param unused - set to null and unused.
   * @return Version info.
   */
  @Override
  public SCMVersionResponseProto getVersion(SCMVersionRequestProto
      request) throws IOException {
    return submitRequest(Type.GetVersion,
        (builder) -> builder
            .setGetVersionRequest(SCMVersionRequestProto.newBuilder().build()))
        .getGetVersionResponse();
  }

  /**
   * Send by datanode to SCM.
   *
   * @param heartbeat node heartbeat
   * @throws IOException
   */

  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(
      SCMHeartbeatRequestProto heartbeat) throws IOException {
    return submitRequest(Type.SendHeartbeat,
        (builder) -> builder.setSendHeartbeatRequest(heartbeat))
        .getSendHeartbeatResponse();
  }

  /**
   * Register Datanode.
   *
   * @param datanodeDetailsProto - Datanode Details
   * @param nodeReport - Node Report.
   * @param containerReportsRequestProto - Container Reports.
   * @return SCM Command.
   */
  @Override
  public SCMRegisteredResponseProto register(
      DatanodeDetailsProto datanodeDetailsProto, NodeReportProto nodeReport,
      ContainerReportsProto containerReportsRequestProto,
      PipelineReportsProto pipelineReportsProto)
      throws IOException {
    SCMRegisterRequestProto.Builder req =
        SCMRegisterRequestProto.newBuilder();
    req.setDatanodeDetails(datanodeDetailsProto);
    req.setContainerReport(containerReportsRequestProto);
    req.setPipelineReports(pipelineReportsProto);
    req.setNodeReport(nodeReport);
    return submitRequest(Type.Register,
        (builder) -> builder.setRegisterRequest(req))
        .getRegisterResponse();
  }
}
