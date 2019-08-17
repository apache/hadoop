/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.server;

import com.google.protobuf.BlockingService;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.KerberosInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;

/**
 * The protocol used to perform security related operations with SCM.
 */
@KerberosInfo(
    serverPrincipal = ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public class SCMSecurityProtocolServer implements SCMSecurityProtocol {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SCMClientProtocolServer.class);
  private final SecurityConfig config;
  private final CertificateServer certificateServer;
  private final RPC.Server rpcServer;
  private final InetSocketAddress rpcAddress;

  SCMSecurityProtocolServer(OzoneConfiguration conf,
      CertificateServer certificateServer) throws IOException {
    this.config = new SecurityConfig(conf);
    this.certificateServer = certificateServer;

    final int handlerCount =
        conf.getInt(ScmConfigKeys.OZONE_SCM_SECURITY_HANDLER_COUNT_KEY,
            ScmConfigKeys.OZONE_SCM_SECURITY_HANDLER_COUNT_DEFAULT);
    rpcAddress = HddsServerUtil
        .getScmSecurityInetAddress(conf);
    // SCM security service RPC service.
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    BlockingService secureProtoPbService =
        SCMSecurityProtocolProtos.SCMSecurityProtocolService
            .newReflectiveBlockingService(
                new SCMSecurityProtocolServerSideTranslatorPB(this));
    this.rpcServer =
        StorageContainerManager.startRpcServer(
            conf,
            rpcAddress,
            SCMSecurityProtocolPB.class,
            secureProtoPbService,
            handlerCount);
    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      rpcServer.refreshServiceAcl(conf, SCMPolicyProvider.getInstance());
    }
  }

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param dnDetails       - DataNode Details.
   * @param certSignReq     - Certificate signing request.
   * @return String         - SCM signed pem encoded certificate.
   */
  @Override
  public String getDataNodeCertificate(
      DatanodeDetailsProto dnDetails,
      String certSignReq) throws IOException {
    LOGGER.info("Processing CSR for dn {}, UUID: {}", dnDetails.getHostName(),
        dnDetails.getUuid());
    Objects.requireNonNull(dnDetails);
    Future<X509CertificateHolder> future =
        certificateServer.requestCertificate(certSignReq,
            KERBEROS_TRUSTED);

    try {
      return CertificateCodec.getPEMEncodedString(future.get());
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("getDataNodeCertificate operation failed. ", e);
      throw new IOException("getDataNodeCertificate operation failed. ", e);
    }
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails       - OzoneManager Details.
   * @param certSignReq     - Certificate signing request.
   * @return String         - SCM signed pem encoded certificate.
   */
  @Override
  public String getOMCertificate(OzoneManagerDetailsProto omDetails,
      String certSignReq) throws IOException {
    LOGGER.info("Processing CSR for om {}, UUID: {}", omDetails.getHostName(),
        omDetails.getUuid());
    Objects.requireNonNull(omDetails);
    Future<X509CertificateHolder> future =
        certificateServer.requestCertificate(certSignReq,
            KERBEROS_TRUSTED);

    try {
      return CertificateCodec.getPEMEncodedString(future.get());
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("getOMCertificate operation failed. ", e);
      throw new IOException("getOMCertificate operation failed. ", e);
    }
  }

  /**
   * Get SCM signed certificate with given serial id.
   *
   * @param certSerialId    - Certificate serial id.
   * @return string         - pem encoded SCM signed certificate.
   */
  @Override
  public String getCertificate(String certSerialId) throws IOException {
    LOGGER.debug("Getting certificate with certificate serial id",
        certSerialId);
    try {
      X509Certificate certificate =
          certificateServer.getCertificate(certSerialId);
      if (certificate != null) {
        return CertificateCodec.getPEMEncodedString(certificate);
      }
    } catch (CertificateException e) {
      LOGGER.error("getCertificate operation failed. ", e);
      throw new IOException("getCertificate operation failed. ", e);
    }
    LOGGER.debug("Certificate with serial id {} not found.", certSerialId);
    throw new IOException("Certificate not found");
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @return string         - Root certificate.
   */
  @Override
  public String getCACertificate() throws IOException {
    LOGGER.debug("Getting CA certificate.");
    try {
      return CertificateCodec.getPEMEncodedString(
          certificateServer.getCACertificate());
    } catch (CertificateException e) {
      LOGGER.error("getRootCertificate operation failed. ", e);
      throw new IOException("getRootCertificate operation failed. ", e);
    }
  }

  public RPC.Server getRpcServer() {
    return rpcServer;
  }

  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  public void start() {
    LOGGER.info(StorageContainerManager.buildRpcServerStartMessage("Starting"
        + " RPC server for SCMSecurityProtocolServer.", getRpcAddress()));
    getRpcServer().start();
  }

  public void stop() {
    try {
      LOGGER.info("Stopping the SCMSecurityProtocolServer.");
      getRpcServer().stop();
    } catch (Exception ex) {
      LOGGER.error("SCMSecurityProtocolServer stop failed.", ex);
    }
  }

  public void join() throws InterruptedException {
    LOGGER.trace("Join RPC server for SCMSecurityProtocolServer.");
    getRpcServer().join();
  }

}
