/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.transport.server;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.token.BlockTokenVerifier;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.MISSING_BLOCK_TOKEN;

/**
 * A server endpoint that acts as the communication layer for Ozone containers.
 */
public abstract class XceiverServer implements XceiverServerSpi {

  private final SecurityConfig secConfig;
  private final TokenVerifier tokenVerifier;
  private final CertificateClient caClient;

  public XceiverServer(Configuration conf, CertificateClient client) {
    Preconditions.checkNotNull(conf);
    this.secConfig = new SecurityConfig(conf);
    this.caClient = client;
    tokenVerifier = new BlockTokenVerifier(secConfig, getCaClient());
  }

  /**
   * Default implementation which just validates security token if security is
   * enabled.
   *
   * @param request ContainerCommandRequest
   */
  @Override
  public void submitRequest(ContainerCommandRequestProto request,
      HddsProtos.PipelineID pipelineID) throws IOException {
    if (secConfig.isSecurityEnabled()) {
      String encodedToken = request.getEncodedToken();
      if (encodedToken == null) {
        throw new SCMSecurityException("Security is enabled but client " +
            "request is missing block token.", MISSING_BLOCK_TOKEN);
      }
      tokenVerifier.verify(encodedToken, encodedToken);
    }
  }

  @VisibleForTesting
  protected CertificateClient getCaClient() {
    return caClient;
  }

  protected SecurityConfig getSecurityConfig() {
    return secConfig;
  }

  protected TokenVerifier getBlockTokenVerifier() {
    return tokenVerifier;
  }

  public SecurityConfig getSecConfig() {
    return secConfig;
  }

}
