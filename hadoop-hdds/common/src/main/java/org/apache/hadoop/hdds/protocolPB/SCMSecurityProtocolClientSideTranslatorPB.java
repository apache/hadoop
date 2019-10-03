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

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCACertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetDataNodeCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest.Builder;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Type;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
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
   * Helper method to wrap the request and send the message.
   */
  private SCMSecurityResponse submitRequest(
      SCMSecurityProtocolProtos.Type type,
      Consumer<Builder> builderConsumer) throws IOException {
    final SCMSecurityResponse response;
    try {

      Builder builder = SCMSecurityRequest.newBuilder()
          .setCmdType(type)
          .setTraceID(TracingUtil.exportCurrentSpan());
      builderConsumer.accept(builder);
      SCMSecurityRequest wrapper = builder.build();

      response = rpcProxy.submitRequest(NULL_RPC_CONTROLLER, wrapper);
    } catch (ServiceException ex) {
      throw ProtobufHelper.getRemoteException(ex);
    }
    return response;
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
    return getDataNodeCertificateChain(dataNodeDetails, certSignReq)
        .getX509Certificate();
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails   - OzoneManager Details.
   * @param certSignReq - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  @Override
  public String getOMCertificate(OzoneManagerDetailsProto omDetails,
      String certSignReq) throws IOException {
    return getOMCertChain(omDetails, certSignReq).getX509Certificate();
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails   - OzoneManager Details.
   * @param certSignReq - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  public SCMGetCertResponseProto getOMCertChain(
      OzoneManagerDetailsProto omDetails, String certSignReq)
      throws IOException {
    SCMGetOMCertRequestProto request = SCMGetOMCertRequestProto
        .newBuilder()
        .setCSR(certSignReq)
        .setOmDetails(omDetails)
        .build();
    return submitRequest(Type.GetOMCertificate,
        builder -> builder.setGetOMCertRequest(request))
        .getGetCertResponseProto();
  }

  /**
   * Get SCM signed certificate with given serial id. Throws exception if
   * certificate is not found.
   *
   * @param certSerialId - Certificate serial id.
   * @return string         - pem encoded certificate.
   */
  @Override
  public String getCertificate(String certSerialId) throws IOException {
    SCMGetCertificateRequestProto request = SCMGetCertificateRequestProto
        .newBuilder()
        .setCertSerialId(certSerialId)
        .build();
    return submitRequest(Type.GetCertificate,
        builder -> builder.setGetCertificateRequest(request))
        .getGetCertResponseProto()
        .getX509Certificate();
  }

  /**
   * Get SCM signed certificate for Datanode.
   *
   * @param dnDetails   - Datanode Details.
   * @param certSignReq - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  public SCMGetCertResponseProto getDataNodeCertificateChain(
      DatanodeDetailsProto dnDetails, String certSignReq)
      throws IOException {

    SCMGetDataNodeCertRequestProto request =
        SCMGetDataNodeCertRequestProto.newBuilder()
            .setCSR(certSignReq)
            .setDatanodeDetails(dnDetails)
            .build();
    return submitRequest(Type.GetDataNodeCertificate,
        builder -> builder.setGetDataNodeCertRequest(request))
        .getGetCertResponseProto();
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
    return submitRequest(Type.GetCACertificate,
        builder -> builder.setGetCACertificateRequest(protoIns))
        .getGetCertResponseProto().getX509Certificate();

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