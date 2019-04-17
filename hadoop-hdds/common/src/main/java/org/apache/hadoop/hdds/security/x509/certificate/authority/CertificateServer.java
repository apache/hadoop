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

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.Future;

/**
 * Interface for Certificate Authority. This can be extended to talk to
 * external CAs later or HSMs later.
 */
public interface CertificateServer {
  /**
   * Initialize the Certificate Authority.
   *
   * @param securityConfig - Security Configuration.
   * @param type - The Type of CertificateServer we are creating, we make this
   * explicit so that when we read code it is visible to the users.
   * @throws SCMSecurityException - Throws if the init fails.
   */
  void init(SecurityConfig securityConfig, CAType type)
      throws SCMSecurityException;

  /**
   * Returns the CA Certificate for this CA.
   *
   * @return X509CertificateHolder - Certificate for this CA.
   * @throws CertificateException - usually thrown if this CA is not
   *                              initialized.
   * @throws IOException          - on Error.
   */
  X509CertificateHolder getCACertificate()
      throws CertificateException, IOException;

  /**
   * Returns the Certificate corresponding to given certificate serial id if
   * exist. Return null if it doesn't exist.
   *
   * @return certSerialId         - Certificate serial id.
   * @throws CertificateException - usually thrown if this CA is not
   *                              initialized.
   * @throws IOException          - on Error.
   */
  X509Certificate getCertificate(String certSerialId)
      throws CertificateException, IOException;

  /**
   * Request a Certificate based on Certificate Signing Request.
   *
   * @param csr  - Certificate Signing Request.
   * @param type - An Enum which says what kind of approval process to follow.
   * @return A future that will have this certificate when this request is
   * approved.
   * @throws SCMSecurityException - on Error.
   */
  Future<X509CertificateHolder> requestCertificate(
      PKCS10CertificationRequest csr,
      CertificateApprover.ApprovalType type)
      throws SCMSecurityException;


  /**
   * Request a Certificate based on Certificate Signing Request.
   *
   * @param csr - Certificate Signing Request as a PEM encoded String.
   * @param type - An Enum which says what kind of approval process to follow.
   * @return A future that will have this certificate when this request is
   * approved.
   * @throws SCMSecurityException - on Error.
   */
  Future<X509CertificateHolder> requestCertificate(String csr,
      ApprovalType type) throws IOException;

  /**
   * Revokes a Certificate issued by this CertificateServer.
   *
   * @param certificate - Certificate to revoke
   * @param approver - Approval process to follow.
   * @return Future that tells us what happened.
   * @throws SCMSecurityException - on Error.
   */
  Future<Boolean> revokeCertificate(X509Certificate certificate,
      ApprovalType approver) throws SCMSecurityException;

  /**
   * TODO : CRL, OCSP etc. Later. This is the start of a CertificateServer
   * framework.
   */


  /**
   * Make it explicit what type of CertificateServer we are creating here.
   */
  enum CAType {
    SELF_SIGNED_CA,
    INTERMEDIARY_CA
  }
}
