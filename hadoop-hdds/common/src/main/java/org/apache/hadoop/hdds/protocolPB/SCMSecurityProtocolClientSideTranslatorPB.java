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
import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCACertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetDataNodeCertRequestProto;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;

import static org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetOMCertRequestProto;

/**
 * This class is the client-side translator that forwards requests for
 * {@link SCMSecurityProtocol} to the {@link SCMSecurityProtocolPB} proxy.
 */
public class SCMSecurityProtocolClientSideTranslatorPB implements
    SCMSecurityProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private final SCMSecurityProtocolPB rpcProxy;

  public SCMSecurityProtocolClientSideTranslatorPB(
      SCMSecurityProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param dataNodeDetails - DataNode Details.
   * @param certSignReq     - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  @Override
  public String getDataNodeCertificate(DatanodeDetailsProto dataNodeDetails,
      String certSignReq) throws IOException {
    SCMGetDataNodeCertRequestProto.Builder builder =
        SCMGetDataNodeCertRequestProto
            .newBuilder()
            .setCSR(certSignReq)
            .setDatanodeDetails(dataNodeDetails);
    try {
      return rpcProxy
          .getDataNodeCertificate(NULL_RPC_CONTROLLER, builder.build())
          .getX509Certificate();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails       - OzoneManager Details.
   * @param certSignReq     - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  @Override
  public String getOMCertificate(OzoneManagerDetailsProto omDetails,
      String certSignReq) throws IOException {
    SCMGetOMCertRequestProto.Builder builder = SCMGetOMCertRequestProto
        .newBuilder()
        .setCSR(certSignReq)
        .setOmDetails(omDetails);
    try {
      return rpcProxy.getOMCertificate(NULL_RPC_CONTROLLER, builder.build())
          .getX509Certificate();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Get SCM signed certificate with given serial id. Throws exception if
   * certificate is not found.
   *
   * @param certSerialId    - Certificate serial id.
   * @return string         - pem encoded certificate.
   */
  @Override
  public String getCertificate(String certSerialId) throws IOException {
    Builder builder = SCMGetCertificateRequestProto
        .newBuilder()
        .setCertSerialId(certSerialId);
    try {
      return rpcProxy.getCertificate(NULL_RPC_CONTROLLER, builder.build())
          .getX509Certificate();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Get CA certificate.
   *
   * @return serial   - Root certificate.
   */
  @Override
  public String getCACertificate() throws IOException {
    SCMGetCACertificateRequestProto protoIns = SCMGetCACertificateRequestProto
        .getDefaultInstance();
    try {
      return rpcProxy.getCACertificate(NULL_RPC_CONTROLLER, protoIns)
          .getX509Certificate();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
}