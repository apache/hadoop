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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.certificates.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;

import java.io.InputStream;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertStore;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Test implementation for CertificateClient. To be used only for test
 * purposes.
 */

public class CertificateClientTestImpl implements CertificateClient {

  private final SecurityConfig securityConfig;
  private final KeyPair keyPair;
  private final Configuration config;
  private final X509Certificate x509Certificate;

  public CertificateClientTestImpl(OzoneConfiguration conf) throws Exception {
    securityConfig = new SecurityConfig(conf);
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    keyPair = keyGen.generateKey();
    config = conf;
    SelfSignedCertificate.Builder builder =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(LocalDate.now())
            .setEndDate(LocalDate.now().plus(365, ChronoUnit.DAYS))
            .setClusterID("cluster1")
            .setKey(keyPair)
            .setSubject("TestCertSub")
            .setConfiguration(config)
            .setScmID("TestScmId1")
            .makeCA();
    X509CertificateHolder certificateHolder = null;
    certificateHolder = builder.build();
    x509Certificate = new JcaX509CertificateConverter().getCertificate(
        certificateHolder);
  }

  @Override
  public PrivateKey getPrivateKey() {
    return keyPair.getPrivate();
  }

  @Override
  public PublicKey getPublicKey() {
    return keyPair.getPublic();
  }

  /**
   * Returns the certificate  of the specified component if it exists on the
   * local system.
   *
   * @return certificate or Null if there is no data.
   */
  @Override
  public X509Certificate getCertificate(String certSerialId)
      throws CertificateException {
    return x509Certificate;
  }

  @Override
  public X509Certificate getCertificate() {
    return x509Certificate;
  }

  @Override
  public boolean verifyCertificate(X509Certificate certificate) {
    return true;
  }

  @Override
  public byte[] signDataStream(InputStream stream)
      throws CertificateException {
    return new byte[0];
  }

  @Override
  public byte[] signData(byte[] data) throws CertificateException {
    return new byte[0];
  }

  @Override
  public boolean verifySignature(InputStream stream, byte[] signature,
      X509Certificate cert) throws CertificateException {
    return true;
  }

  @Override
  public boolean verifySignature(byte[] data, byte[] signature,
      X509Certificate cert) throws CertificateException {
    return true;
  }

  @Override
  public CertificateSignRequest.Builder getCSRBuilder() {
    return new CertificateSignRequest.Builder();
  }

  @Override
  public X509Certificate queryCertificate(String query) {
    return null;
  }

  @Override
  public void storeCertificate(String cert, boolean force)
      throws CertificateException {

  }

  /**
   * Stores the trusted chain of certificates for a specific component.
   *
   * @param keyStore - Cert Store.
   * @throws CertificateException - on Error.
   */
  @Override
  public void storeTrustChain(CertStore keyStore) throws CertificateException {

  }

  @Override
  public void storeTrustChain(List<X509Certificate> certificates)
      throws CertificateException {

  }

  @Override
  public InitResponse init() throws CertificateException {
    return null;
  }
}
