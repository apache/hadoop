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
package org.apache.hadoop.hdds.scm.protocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetDataNodeCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto.ResponseCode;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetOMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Status;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.ozone.protocolPB.ProtocolMessageMetrics;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link SCMSecurityProtocolPB} to the {@link
 * SCMSecurityProtocol} server implementation.
 */
public class SCMSecurityProtocolServerSideTranslatorPB
    implements SCMSecurityProtocolPB {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMSecurityProtocolServerSideTranslatorPB.class);

  private final SCMSecurityProtocol impl;

  private OzoneProtocolMessageDispatcher<SCMSecurityRequest,
      SCMSecurityResponse>
      dispatcher;

  public SCMSecurityProtocolServerSideTranslatorPB(SCMSecurityProtocol impl,
      ProtocolMessageMetrics messageMetrics) {
    this.impl = impl;
    this.dispatcher =
        new OzoneProtocolMessageDispatcher<>("ScmSecurityProtocol",
            messageMetrics, LOG);
  }

  @Override
  public SCMSecurityResponse submitRequest(RpcController controller,
      SCMSecurityRequest request) throws ServiceException {
    return dispatcher.processRequest(request, this::processRequest,
        request.getCmdType(), request.getTraceID());
  }

  public SCMSecurityResponse processRequest(SCMSecurityRequest request)
      throws ServiceException {
    try {
      switch (request.getCmdType()) {
      case GetCertificate:
        return SCMSecurityResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetCertResponseProto(
                getCertificate(request.getGetCertificateRequest()))
            .build();
      case GetCACertificate:
        return SCMSecurityResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetCertResponseProto(
                getCACertificate(request.getGetCACertificateRequest()))
            .build();
      case GetOMCertificate:
        return SCMSecurityResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetCertResponseProto(
                getOMCertificate(request.getGetOMCertRequest()))
            .build();
      case GetDataNodeCertificate:
        return SCMSecurityResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetCertResponseProto(
                getDataNodeCertificate(request.getGetDataNodeCertRequest()))
            .build();
      default:
        throw new IllegalArgumentException(
            "Unknown request type: " + request.getCmdType());
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param request
   * @return SCMGetDataNodeCertResponseProto.
   */

  public SCMGetCertResponseProto getDataNodeCertificate(
      SCMGetDataNodeCertRequestProto request)
      throws IOException {

    String certificate = impl
        .getDataNodeCertificate(request.getDatanodeDetails(),
            request.getCSR());
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate)
            .setX509CACertificate(impl.getCACertificate());

    return builder.build();

  }

  /**
   * Get SCM signed certificate for OzoneManager.
   *
   * @param request
   * @return SCMGetCertResponseProto.
   */
  public SCMGetCertResponseProto getOMCertificate(
      SCMGetOMCertRequestProto request) throws IOException {
    String certificate = impl
        .getOMCertificate(request.getOmDetails(),
            request.getCSR());
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate)
            .setX509CACertificate(impl.getCACertificate());
    return builder.build();

  }

  public SCMGetCertResponseProto getCertificate(
      SCMGetCertificateRequestProto request) throws IOException {

    String certificate = impl.getCertificate(request.getCertSerialId());
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate);
    return builder.build();

  }

  public SCMGetCertResponseProto getCACertificate(
      SCMSecurityProtocolProtos.SCMGetCACertificateRequestProto request)
      throws IOException {

    String certificate = impl.getCACertificate();
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate);
    return builder.build();

  }

}