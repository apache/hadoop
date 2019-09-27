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

import java.io.IOException;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMDatanodeRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMDatanodeResponse;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisterRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.Type;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerDatanodeProtocolPB} to the {@link
 * StorageContainerDatanodeProtocol} server implementation.
 */
public class StorageContainerDatanodeProtocolServerSideTranslatorPB
    implements StorageContainerDatanodeProtocolPB {

  private static final Logger LOG = LoggerFactory
      .getLogger(StorageContainerDatanodeProtocolServerSideTranslatorPB.class);

  private final StorageContainerDatanodeProtocol impl;
  private final OzoneProtocolMessageDispatcher<SCMDatanodeRequest,
      SCMDatanodeResponse> dispatcher;

  public StorageContainerDatanodeProtocolServerSideTranslatorPB(
      StorageContainerDatanodeProtocol impl,
      ProtocolMessageMetrics protocolMessageMetrics) {
    this.impl = impl;
    dispatcher =
        new OzoneProtocolMessageDispatcher<>("SCMDatanodeProtocol",
            protocolMessageMetrics,
            LOG);
  }

  public SCMRegisteredResponseProto register(
      SCMRegisterRequestProto request) throws IOException {
    ContainerReportsProto containerRequestProto = request
        .getContainerReport();
    NodeReportProto dnNodeReport = request.getNodeReport();
    PipelineReportsProto pipelineReport = request.getPipelineReports();
    return impl.register(request.getDatanodeDetails(), dnNodeReport,
        containerRequestProto, pipelineReport);

  }

  @Override
  public SCMDatanodeResponse submitRequest(RpcController controller,
      SCMDatanodeRequest request) throws ServiceException {
    return dispatcher.processRequest(request, this::processMessage,
        request.getCmdType(), request.getTraceID());
  }

  public SCMDatanodeResponse processMessage(SCMDatanodeRequest request)
      throws ServiceException {
    try {
      Type cmdType = request.getCmdType();
      switch (cmdType) {
      case GetVersion:
        return SCMDatanodeResponse.newBuilder()
            .setCmdType(cmdType)
            .setStatus(Status.OK)
            .setGetVersionResponse(
                impl.getVersion(request.getGetVersionRequest()))
            .build();
      case SendHeartbeat:
        return SCMDatanodeResponse.newBuilder()
            .setCmdType(cmdType)
            .setStatus(Status.OK)
            .setSendHeartbeatResponse(
                impl.sendHeartbeat(request.getSendHeartbeatRequest()))
            .build();
      case Register:
        return SCMDatanodeResponse.newBuilder()
            .setCmdType(cmdType)
            .setStatus(Status.OK)
            .setRegisterResponse(register(request.getRegisterRequest()))
            .build();
      default:
        throw new ServiceException("Unknown command type: " + cmdType);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}