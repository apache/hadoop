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

import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.bouncycastle.cert.X509CertificateHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;


import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec.getPEMEncodedString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link DefaultCertificateClient}.
 */
public class TestDefaultCertificateClient {

  private String certSerialId;
  private X509Certificate x509Certificate;
  private OMCertificateClient omCertClient;
  private DNCertificateClient dnCertClient;
  private HDDSKeyGenerator keyGenerator;
  private Path omMetaDirPath;
  private Path dnMetaDirPath;
  private SecurityConfig omSecurityConfig;
  private SecurityConfig dnSecurityConfig;
  private final static String UTF = "UTF-8";
  private KeyCodec omKeyCodec;
  private KeyCodec dnKeyCodec;

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.setStrings(OZONE_SCM_NAMES, "localhost");
    config.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 2);
    final String omPath = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());
    final String dnPath = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());

    omMetaDirPath = Paths.get(omPath, "test");
    dnMetaDirPath = Paths.get(dnPath, "test");

    config.set(HDDS_METADATA_DIR_NAME, omMetaDirPath.toString());
    omSecurityConfig = new SecurityConfig(config);
    config.set(HDDS_METADATA_DIR_NAME, dnMetaDirPath.toString());
    dnSecurityConfig = new SecurityConfig(config);


    keyGenerator = new HDDSKeyGenerator(omSecurityConfig);
    omKeyCodec = new KeyCodec(omSecurityConfig);
    dnKeyCodec = new KeyCodec(dnSecurityConfig);

    Files.createDirectories(omSecurityConfig.getKeyLocation());
    Files.createDirectories(dnSecurityConfig.getKeyLocation());
    x509Certificate = generateX509Cert(null);
    certSerialId = x509Certificate.getSerialNumber().toString();
    getCertClient();
  }

  private void getCertClient() {
    omCertClient = new OMCertificateClient(omSecurityConfig, certSerialId);
    dnCertClient = new DNCertificateClient(dnSecurityConfig, certSerialId);
  }

  @After
  public void tearDown() {
    omCertClient = null;
    dnCertClient = null;
    FileUtils.deleteQuietly(omMetaDirPath.toFile());
    FileUtils.deleteQuietly(dnMetaDirPath.toFile());
  }

  /**
   * Tests: 1. getPrivateKey 2. getPublicKey 3. storePrivateKey 4.
   * storePublicKey
   */
  @Test
  public void testKeyOperations() throws Exception {
    cleanupOldKeyPair();
    PrivateKey pvtKey = omCertClient.getPrivateKey();
    PublicKey publicKey = omCertClient.getPublicKey();
    assertNull(publicKey);
    assertNull(pvtKey);

    KeyPair keyPair = generateKeyPairFiles();
    pvtKey = omCertClient.getPrivateKey();
    assertNotNull(pvtKey);
    assertEquals(pvtKey, keyPair.getPrivate());

    publicKey = dnCertClient.getPublicKey();
    assertNotNull(publicKey);
    assertEquals(publicKey, keyPair.getPublic());
  }

  private KeyPair generateKeyPairFiles() throws Exception {
    cleanupOldKeyPair();
    KeyPair keyPair = keyGenerator.generateKey();
    omKeyCodec.writePrivateKey(keyPair.getPrivate());
    omKeyCodec.writePublicKey(keyPair.getPublic());

    dnKeyCodec.writePrivateKey(keyPair.getPrivate());
    dnKeyCodec.writePublicKey(keyPair.getPublic());
    return keyPair;
  }

  private void cleanupOldKeyPair() {
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getPublicKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(dnSecurityConfig.getKeyLocation()
        .toString(), dnSecurityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(dnSecurityConfig.getKeyLocation()
        .toString(), dnSecurityConfig.getPublicKeyFileName()).toFile());
  }

  /**
   * Tests: 1. storeCertificate 2. getCertificate 3. verifyCertificate
   */
  @Test
  public void testCertificateOps() throws Exception {
    X509Certificate cert = omCertClient.getCertificate();
    assertNull(cert);
    omCertClient.storeCertificate(getPEMEncodedString(x509Certificate),
        true);

    cert = omCertClient.getCertificate(
        x509Certificate.getSerialNumber().toString());
    assertNotNull(cert);
    assertTrue(cert.getEncoded().length > 0);
    assertEquals(cert, x509Certificate);

    // TODO: test verifyCertificate once implemented.
  }

  private X509Certificate generateX509Cert(KeyPair keyPair) throws Exception {
    if (keyPair == null) {
      keyPair = generateKeyPairFiles();
    }
    return KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
        omSecurityConfig.getSignatureAlgo());
  }

  @Test
  public void testSignDataStream() throws Exception {
    String data = RandomStringUtils.random(100, UTF);
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getPublicKeyFileName()).toFile());

    // Expect error when there is no private key to sign.
    LambdaTestUtils.intercept(IOException.class, "Error while " +
            "signing the stream",
        () -> omCertClient.signDataStream(IOUtils.toInputStream(data,
            UTF)));

    generateKeyPairFiles();
    byte[] sign = omCertClient.signDataStream(IOUtils.toInputStream(data,
        UTF));
    validateHash(sign, data.getBytes());
  }

  /**
   * Validate hash using public key of KeyPair.
   */
  private void validateHash(byte[] hash, byte[] data)
      throws Exception {
    Signature rsaSignature =
        Signature.getInstance(omSecurityConfig.getSignatureAlgo(),
            omSecurityConfig.getProvider());
    rsaSignature.initVerify(omCertClient.getPublicKey());
    rsaSignature.update(data);
    Assert.assertTrue(rsaSignature.verify(hash));
  }

  /**
   * Tests: 1. verifySignature
   */
  @Test
  public void verifySignatureStream() throws Exception {
    String data = RandomStringUtils.random(500, UTF);
    byte[] sign = omCertClient.signDataStream(IOUtils.toInputStream(data,
        UTF));

    // Positive tests.
    assertTrue(omCertClient.verifySignature(data.getBytes(), sign,
        x509Certificate));
    assertTrue(omCertClient.verifySignature(IOUtils.toInputStream(data, UTF),
        sign, x509Certificate));

    // Negative tests.
    assertFalse(omCertClient.verifySignature(data.getBytes(),
        "abc".getBytes(), x509Certificate));
    assertFalse(omCertClient.verifySignature(IOUtils.toInputStream(data,
        UTF), "abc".getBytes(), x509Certificate));

  }

  /**
   * Tests: 1. verifySignature
   */
  @Test
  public void verifySignatureDataArray() throws Exception {
    String data = RandomStringUtils.random(500, UTF);
    byte[] sign = omCertClient.signData(data.getBytes());

    // Positive tests.
    assertTrue(omCertClient.verifySignature(data.getBytes(), sign,
        x509Certificate));
    assertTrue(omCertClient.verifySignature(IOUtils.toInputStream(data, UTF),
        sign, x509Certificate));

    // Negative tests.
    assertFalse(omCertClient.verifySignature(data.getBytes(),
        "abc".getBytes(), x509Certificate));
    assertFalse(omCertClient.verifySignature(IOUtils.toInputStream(data,
        UTF), "abc".getBytes(), x509Certificate));

  }

  @Test
  public void queryCertificate() throws Exception {
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        "Operation not supported",
        () -> omCertClient.queryCertificate(""));
  }

  @Test
  public void testCertificateLoadingOnInit() throws Exception {
    KeyPair keyPair = keyGenerator.generateKey();
    X509Certificate cert1 = generateX509Cert(keyPair);
    X509Certificate cert2 = generateX509Cert(keyPair);
    X509Certificate cert3 = generateX509Cert(keyPair);

    Path certPath = dnSecurityConfig.getCertificateLocation();
    CertificateCodec codec = new CertificateCodec(dnSecurityConfig);

    // Certificate not found.
    LambdaTestUtils.intercept(CertificateException.class, "Error while" +
            " getting certificate",
        () -> dnCertClient.getCertificate(cert1.getSerialNumber()
            .toString()));
    LambdaTestUtils.intercept(CertificateException.class, "Error while" +
            " getting certificate",
        () -> dnCertClient.getCertificate(cert2.getSerialNumber()
            .toString()));
    LambdaTestUtils.intercept(CertificateException.class, "Error while" +
            " getting certificate",
        () -> dnCertClient.getCertificate(cert3.getSerialNumber()
            .toString()));
    codec.writeCertificate(certPath, "1.crt",
        getPEMEncodedString(cert1), true);
    codec.writeCertificate(certPath, "2.crt",
        getPEMEncodedString(cert2), true);
    codec.writeCertificate(certPath, "3.crt",
        getPEMEncodedString(cert3), true);

    // Re instentiate DN client which will load certificates from filesystem.
    dnCertClient = new DNCertificateClient(dnSecurityConfig, certSerialId);

    assertNotNull(dnCertClient.getCertificate(cert1.getSerialNumber()
        .toString()));
    assertNotNull(dnCertClient.getCertificate(cert2.getSerialNumber()
        .toString()));
    assertNotNull(dnCertClient.getCertificate(cert3.getSerialNumber()
        .toString()));

  }

  @Test
  public void testStoreCertificate() throws Exception {
    KeyPair keyPair = keyGenerator.generateKey();
    X509Certificate cert1 = generateX509Cert(keyPair);
    X509Certificate cert2 = generateX509Cert(keyPair);
    X509Certificate cert3 = generateX509Cert(keyPair);

    dnCertClient.storeCertificate(getPEMEncodedString(cert1), true);
    dnCertClient.storeCertificate(getPEMEncodedString(cert2), true);
    dnCertClient.storeCertificate(getPEMEncodedString(cert3), true);

    assertNotNull(dnCertClient.getCertificate(cert1.getSerialNumber()
        .toString()));
    assertNotNull(dnCertClient.getCertificate(cert2.getSerialNumber()
        .toString()));
    assertNotNull(dnCertClient.getCertificate(cert3.getSerialNumber()
        .toString()));
  }

  @Test
  public void testInitCertAndKeypairValidationFailures() throws Exception {

    GenericTestUtils.LogCapturer dnClientLog = GenericTestUtils.LogCapturer
        .captureLogs(dnCertClient.getLogger());
    GenericTestUtils.LogCapturer omClientLog = GenericTestUtils.LogCapturer
        .captureLogs(omCertClient.getLogger());
    KeyPair keyPair = keyGenerator.generateKey();
    KeyPair keyPair2 = keyGenerator.generateKey();
    dnClientLog.clearOutput();
    omClientLog.clearOutput();

    // Case 1. Expect failure when keypair validation fails.
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getPublicKeyFileName()).toFile());


    FileUtils.deleteQuietly(Paths.get(dnSecurityConfig.getKeyLocation()
        .toString(), dnSecurityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(dnSecurityConfig.getKeyLocation()
        .toString(), dnSecurityConfig.getPublicKeyFileName()).toFile());

    omKeyCodec.writePrivateKey(keyPair.getPrivate());
    omKeyCodec.writePublicKey(keyPair2.getPublic());

    dnKeyCodec.writePrivateKey(keyPair.getPrivate());
    dnKeyCodec.writePublicKey(keyPair2.getPublic());


    // Check for DN.
    assertEquals(dnCertClient.init(), FAILURE);
    assertTrue(dnClientLog.getOutput().contains("Keypair validation " +
        "failed"));
    dnClientLog.clearOutput();
    omClientLog.clearOutput();

    // Check for OM.
    assertEquals(omCertClient.init(), FAILURE);
    assertTrue(omClientLog.getOutput().contains("Keypair validation " +
        "failed"));
    dnClientLog.clearOutput();
    omClientLog.clearOutput();

    // Case 2. Expect failure when certificate is generated from different
    // private key and keypair validation fails.
    getCertClient();
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getCertificateFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(dnSecurityConfig.getKeyLocation()
        .toString(), dnSecurityConfig.getCertificateFileName()).toFile());

    CertificateCodec omCertCodec = new CertificateCodec(omSecurityConfig);
    omCertCodec.writeCertificate(new X509CertificateHolder(
        x509Certificate.getEncoded()));

    CertificateCodec dnCertCodec = new CertificateCodec(dnSecurityConfig);
    dnCertCodec.writeCertificate(new X509CertificateHolder(
        x509Certificate.getEncoded()));
    // Check for DN.
    assertEquals(dnCertClient.init(), FAILURE);
    assertTrue(dnClientLog.getOutput().contains("Keypair validation " +
        "failed"));
    dnClientLog.clearOutput();
    omClientLog.clearOutput();

    // Check for OM.
    assertEquals(omCertClient.init(), FAILURE);
    assertTrue(omClientLog.getOutput().contains("Keypair validation failed"));
    dnClientLog.clearOutput();
    omClientLog.clearOutput();

    // Case 3. Expect failure when certificate is generated from different
    // private key and certificate validation fails.

    // Re write the correct public key.
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getPublicKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(dnSecurityConfig.getKeyLocation()
        .toString(), dnSecurityConfig.getPublicKeyFileName()).toFile());
    getCertClient();
    omKeyCodec.writePublicKey(keyPair.getPublic());
    dnKeyCodec.writePublicKey(keyPair.getPublic());

    // Check for DN.
    assertEquals(dnCertClient.init(), FAILURE);
    assertTrue(dnClientLog.getOutput().contains("Stored certificate is " +
        "generated with different"));
    dnClientLog.clearOutput();
    omClientLog.clearOutput();

    //Check for OM.
    assertEquals(omCertClient.init(), FAILURE);
    assertTrue(omClientLog.getOutput().contains("Stored certificate is " +
        "generated with different"));
    dnClientLog.clearOutput();
    omClientLog.clearOutput();

    // Case 4. Failure when public key recovery fails.
    getCertClient();
    FileUtils.deleteQuietly(Paths.get(omSecurityConfig.getKeyLocation()
        .toString(), omSecurityConfig.getPublicKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(dnSecurityConfig.getKeyLocation()
        .toString(), dnSecurityConfig.getPublicKeyFileName()).toFile());

    // Check for DN.
    assertEquals(dnCertClient.init(), FAILURE);
    assertTrue(dnClientLog.getOutput().contains("Can't recover public key"));

    // Check for OM.
    assertEquals(omCertClient.init(), FAILURE);
    assertTrue(omClientLog.getOutput().contains("Can't recover public key"));
    dnClientLog.clearOutput();
    omClientLog.clearOutput();
  }

}