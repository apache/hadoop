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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link DefaultCertificateClient}.
 */
public class TestDefaultCertificateClient {

  private OMCertificateClient omCertClient;
  private DNCertificateClient dnCertClient;
  private HDDSKeyGenerator keyGenerator;
  private Path metaDirPath;
  private SecurityConfig securityConfig;
  private final static String UTF = "UTF-8";
  private KeyCodec keyCodec;

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    final String path = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());
    metaDirPath = Paths.get(path, "test");
    config.set(HDDS_METADATA_DIR_NAME, metaDirPath.toString());
    securityConfig = new SecurityConfig(config);
    getCertClient();
    keyGenerator = new HDDSKeyGenerator(securityConfig);
    keyCodec = new KeyCodec(securityConfig);
    Files.createDirectories(securityConfig.getKeyLocation());
  }

  private void getCertClient() {
    omCertClient = new OMCertificateClient(securityConfig);
    dnCertClient = new DNCertificateClient(securityConfig);
  }

  @After
  public void tearDown() {
    omCertClient = null;
    FileUtils.deleteQuietly(metaDirPath.toFile());
  }

  /**
   * Tests: 1. getPrivateKey 2. getPublicKey 3. storePrivateKey 4.
   * storePublicKey
   */
  @Test
  public void testKeyOperations() throws Exception {
    PrivateKey pvtKey = omCertClient.getPrivateKey();
    PublicKey publicKey = omCertClient.getPublicKey();
    assertNull(publicKey);
    assertNull(pvtKey);

    KeyPair keyPair = generateKeyPairFiles();
    pvtKey = omCertClient.getPrivateKey();
    assertNotNull(pvtKey);
    assertEquals(pvtKey, keyPair.getPrivate());

    publicKey = omCertClient.getPublicKey();
    assertNotNull(publicKey);
    assertEquals(publicKey, keyPair.getPublic());
  }

  private KeyPair generateKeyPairFiles() throws Exception {
    KeyPair keyPair = keyGenerator.generateKey();
    keyCodec.writePrivateKey(keyPair.getPrivate());
    keyCodec.writePublicKey(keyPair.getPublic());
    return keyPair;
  }

  /**
   * Tests: 1. storeCertificate 2. getCertificate 3. verifyCertificate
   */
  @Test
  public void testCertificateOps() throws Exception {
    X509Certificate cert = omCertClient.getCertificate();
    assertNull(cert);

    X509Certificate x509Certificate = generateX509Cert(null);
    omCertClient.storeCertificate(x509Certificate);

    cert = omCertClient.getCertificate();
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
        securityConfig.getSignatureAlgo());
  }

  @Test
  public void testSignDataStream() throws Exception {
    String data = RandomStringUtils.random(100);
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
        Signature.getInstance(securityConfig.getSignatureAlgo(),
            securityConfig.getProvider());
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

    X509Certificate x509Certificate = generateX509Cert(null);
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
    X509Certificate x509Certificate = generateX509Cert(null);
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
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
        .toString(), securityConfig.getPrivateKeyFileName()).toFile());
    keyCodec.writePrivateKey(keyPair.getPrivate());

    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
        .toString(), securityConfig.getPublicKeyFileName()).toFile());
    keyCodec.writePublicKey(keyPair2.getPublic());

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
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
        .toString(), securityConfig.getCertificateFileName()).toFile());
    X509Certificate x509Certificate = KeyStoreTestUtil.generateCertificate(
        "CN=Test", keyGenerator.generateKey(), 10,
        securityConfig.getSignatureAlgo());
    CertificateCodec codec = new CertificateCodec(securityConfig);
    codec.writeCertificate(new X509CertificateHolder(
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
    getCertClient();
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
        .toString(), securityConfig.getPublicKeyFileName()).toFile());
    keyCodec.writePublicKey(keyPair.getPublic());

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
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
        .toString(), securityConfig.getPublicKeyFileName()).toFile());

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