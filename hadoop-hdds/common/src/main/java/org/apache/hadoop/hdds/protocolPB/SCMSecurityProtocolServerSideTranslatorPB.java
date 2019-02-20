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
package org.apache.hadoop.hdds.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;

import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetDataNodeCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto.ResponseCode;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetOMCertRequestProto;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link SCMSecurityProtocolPB} to the {@link
 * SCMSecurityProtocol} server implementation.
 */
public class SCMSecurityProtocolServerSideTranslatorPB implements
    SCMSecurityProtocolPB {

  private final SCMSecurityProtocol impl;

  public SCMSecurityProtocolServerSideTranslatorPB(SCMSecurityProtocol impl) {
    this.impl = impl;
  }

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param controller
   * @param request
   * @return SCMGetDataNodeCertResponseProto.
   */
  @Override
  public SCMGetCertResponseProto getDataNodeCertificate(
      RpcController controller, SCMGetDataNodeCertRequestProto request)
      throws ServiceException {
    try {
      String certificate = impl
          .getDataNodeCertificate(request.getDatanodeDetails(),
              request.getCSR());
      SCMGetCertResponseProto.Builder builder =
          SCMGetCertResponseProto
              .newBuilder()
              .setResponseCode(ResponseCode.success)
              .setX509Certificate(certificate);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Get SCM signed certificate for OzoneManager.
   *
   * @param controller
   * @param request
   * @return SCMGetCertResponseProto.
   */
  @Override
  public SCMGetCertResponseProto getOMCertificate(
      RpcController controller, SCMGetOMCertRequestProto request)
      throws ServiceException {
    try {
      String certificate = impl
          .getOMCertificate(request.getOmDetails(),
              request.getCSR());
      SCMGetCertResponseProto.Builder builder =
          SCMGetCertResponseProto
              .newBuilder()
              .setResponseCode(ResponseCode.success)
              .setX509Certificate(certificate);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SCMGetCertResponseProto getCertificate(RpcController controller,
      SCMGetCertificateRequestProto request) throws ServiceException {
    try {
      String certificate = impl.getCertificate(request.getCertSerialId());
      SCMGetCertResponseProto.Builder builder =
          SCMGetCertResponseProto
              .newBuilder()
              .setResponseCode(ResponseCode.success)
              .setX509Certificate(certificate);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SCMGetCertResponseProto getCACertificate(RpcController controller,
      SCMSecurityProtocolProtos.SCMGetCACertificateRequestProto request)
      throws ServiceException {
    try {
      String certificate = impl.getCACertificate();
      SCMGetCertResponseProto.Builder builder =
          SCMGetCertResponseProto
              .newBuilder()
              .setResponseCode(ResponseCode.success)
              .setX509Certificate(certificate);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}