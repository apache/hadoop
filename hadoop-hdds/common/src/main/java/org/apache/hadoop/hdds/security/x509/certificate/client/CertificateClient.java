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

import org.apache.hadoop.hdds.security.x509.certificates.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;

import java.io.InputStream;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertStore;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Certificate client provides and interface to certificate operations that
 * needs to be performed by all clients in the Ozone eco-system.
 */
public interface CertificateClient {

  /**
   * Returns the private key of the specified component if it exists on the
   * local system.
   *
   * @param component - String name like DN, OM, SCM etc.
   * @return private key or Null if there is no data.
   */
  PrivateKey getPrivateKey(String component);

  /**
   * Returns the public key of the specified component if it exists on the local
   * system.
   *
   * @param component - String name like DN, OM, SCM etc.
   * @return public key or Null if there is no data.
   */
  PublicKey getPublicKey(String component);

  /**
   * Returns the certificate  of the specified component if it exists on the
   * local system.
   *
   * @param component - String name like DN, OM, SCM etc.
   * @return certificate or Null if there is no data.
   */
  X509Certificate getCertificate(String component);

  /**
   * Verifies if this certificate is part of a trusted chain.
   *
   * @return true if it trusted, false otherwise.
   */
  boolean verifyCertificate(X509Certificate certificate);

  /**
   * Creates digital signature over the data stream using the components private
   * key.
   *
   * @param stream - Data stream to sign.
   * @return byte array - containing the signature.
   */
  byte[] signDataStream(InputStream stream, String component)
      throws CertificateException;

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   * @param stream - Data Stream.
   * @param signature - Byte Array containing the signature.
   * @param cert - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  boolean verifySignature(InputStream stream, byte[] signature,
      X509Certificate cert);

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   * @param data - Data in byte array.
   * @param signature - Byte Array containing the signature.
   * @param cert - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  boolean verifySignature(byte[] data, byte[] signature,
      X509Certificate cert);

  /**
   * Returns a CSR builder that can be used to creates a Certificate sigining
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  CertificateSignRequest.Builder getCSRBuilder();

  /**
   * Get the certificate of well-known entity from SCM.
   *
   * @param query - String Query, please see the implementation for the
   * discussion on the query formats.
   * @return X509Certificate or null if not found.
   */
  X509Certificate queryCertificate(String query);

  /**
   * Stores the private key of a specified component.
   *
   * @param key - private key
   * @param component - name of the component.
   * @throws CertificateException
   */
  void storePrivateKey(PrivateKey key, String component)
      throws CertificateException;

  /**
   * Stores the public key of a specified component.
   *
   * @param key - public key
   * @throws CertificateException
   */
  void storePublicKey(PublicKey key, String component)
      throws CertificateException;

  /**
   * Stores the Certificate of a specific component.
   *
   * @param certificate - X509 Certificate
   * @param component - Name of the component.
   * @throws CertificateException
   */
  void storeCertificate(X509Certificate certificate, String component)
      throws CertificateException;

  /**
   * Stores the trusted chain of certificates for a specific component.
   *
   * @param certStore - Cert Store.
   * @param component - Trust Chain.
   * @throws CertificateException
   */
  void storeTrustChain(CertStore certStore,
      String component) throws CertificateException;

  /**
   * Stores the trusted chain of certificates for a specific component.
   *
   * @param certificates - List of Certificates.
   * @param component - String component.
   * @throws CertificateException
   */
  void storeTrustChain(List<X509Certificate> certificates,
      String component) throws CertificateException;

}
