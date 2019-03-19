/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.security.x509.SecurityConfig;
/**
 * Certificate client for DataNodes.
 */
public class DNCertificateClient extends DefaultCertificateClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(DNCertificateClient.class);
  public DNCertificateClient(SecurityConfig securityConfig,
      String certSerialId) {
    super(securityConfig, LOG, certSerialId);
  }

  public DNCertificateClient(SecurityConfig securityConfig) {
    super(securityConfig, LOG, null);
  }

  /**
   * Returns a CSR builder that can be used to creates a Certificate signing
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder getCSRBuilder()
      throws CertificateException {
    return super.getCSRBuilder()
        .setDigitalEncryption(false)
        .setDigitalSignature(false);
  }

  public Logger getLogger() {
    return LOG;
  }
}
