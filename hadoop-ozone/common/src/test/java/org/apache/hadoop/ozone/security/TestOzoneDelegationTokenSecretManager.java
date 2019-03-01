/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.security;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.OMCertificateClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.X509Certificate;

/**
 * Test class for {@link OzoneDelegationTokenSecretManager}.
 */
public class TestOzoneDelegationTokenSecretManager {

  private OzoneDelegationTokenSecretManager secretManager;
  private SecurityConfig securityConfig;
  private CertificateClient certificateClient;
  private long expiryTime;
  private Text serviceRpcAdd;
  private OzoneConfiguration conf;
  private static final String BASEDIR = GenericTestUtils.getTempPath(
      TestOzoneDelegationTokenSecretManager.class.getSimpleName());
  private final static Text TEST_USER = new Text("testUser");
  private long tokenMaxLifetime = 1000 * 20;
  private long tokenRemoverScanInterval = 1000 * 20;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, BASEDIR);
    securityConfig = new SecurityConfig(conf);
    certificateClient = setupCertificateClient();
    certificateClient.init();
    expiryTime = Time.monotonicNow() + 60 * 60 * 24;
    serviceRpcAdd = new Text("localhost");
  }

  /**
   * Helper function to create certificate client.
   * */
  private CertificateClient setupCertificateClient() throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate cert = KeyStoreTestUtil
        .generateCertificate("CN=OzoneMaster", keyPair, 30, "SHA256withRSA");

    return new OMCertificateClient(securityConfig) {
      @Override
      public X509Certificate getCertificate() {
        return cert;
      }

      @Override
      public PrivateKey getPrivateKey() {
        return keyPair.getPrivate();
      }

      @Override
      public PublicKey getPublicKey() {
        return keyPair.getPublic();
      }
    };
  }

  @After
  public void tearDown() throws IOException {
    secretManager.stop();
    FileUtils.deleteQuietly(new File(BASEDIR));
  }

  @Test
  public void testCreateToken() throws Exception {
    secretManager = createSecretManager(conf, tokenMaxLifetime,
        expiryTime, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);
    OzoneTokenIdentifier identifier =
        OzoneTokenIdentifier.readProtoBuf(token.getIdentifier());
    // Check basic details.
    Assert.assertTrue(identifier.getRealUser().equals(TEST_USER));
    Assert.assertTrue(identifier.getRenewer().equals(TEST_USER));
    Assert.assertTrue(identifier.getOwner().equals(TEST_USER));

    validateHash(token.getPassword(), token.getIdentifier());
  }

  @Test
  public void testRenewTokenSuccess() throws Exception {
    secretManager = createSecretManager(conf, tokenMaxLifetime,
        expiryTime, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);
    Thread.sleep(10 * 5);
    long renewalTime = secretManager.renewToken(token, TEST_USER.toString());
    Assert.assertTrue(renewalTime > 0);
  }

  /**
   * Tests failure for mismatch in renewer.
   */
  @Test
  public void testRenewTokenFailure() throws Exception {
    secretManager = createSecretManager(conf, tokenMaxLifetime,
        expiryTime, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER, TEST_USER);
    LambdaTestUtils.intercept(AccessControlException.class,
        "rougeUser tries to renew a token", () -> {
          secretManager.renewToken(token, "rougeUser");
        });
  }

  /**
   * Tests token renew failure due to max time.
   */
  @Test
  public void testRenewTokenFailureMaxTime() throws Exception {
    secretManager = createSecretManager(conf, 100,
        100, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);
    Thread.sleep(101);
    LambdaTestUtils.intercept(IOException.class,
        "testUser tried to renew an expired token", () -> {
          secretManager.renewToken(token, TEST_USER.toString());
        });
  }

  /**
   * Tests token renew failure due to renewal time.
   */
  @Test
  public void testRenewTokenFailureRenewalTime() throws Exception {
    secretManager = createSecretManager(conf, 1000 * 10,
        10, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);
    Thread.sleep(15);
    LambdaTestUtils.intercept(IOException.class, "is expired", () -> {
      secretManager.renewToken(token, TEST_USER.toString());
    });
  }

  @Test
  public void testCreateIdentifier() throws Exception {
    secretManager = createSecretManager(conf, tokenMaxLifetime,
        expiryTime, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    OzoneTokenIdentifier identifier = secretManager.createIdentifier();
    // Check basic details.
    Assert.assertTrue(identifier.getOwner().equals(new Text("")));
    Assert.assertTrue(identifier.getRealUser().equals(new Text("")));
    Assert.assertTrue(identifier.getRenewer().equals(new Text("")));
  }

  @Test
  public void testCancelTokenSuccess() throws Exception {
    secretManager = createSecretManager(conf, tokenMaxLifetime,
        expiryTime, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER, TEST_USER);
    secretManager.cancelToken(token, TEST_USER.toString());
  }

  @Test
  public void testCancelTokenFailure() throws Exception {
    secretManager = createSecretManager(conf, tokenMaxLifetime,
        expiryTime, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    Token<OzoneTokenIdentifier> token = secretManager.createToken(TEST_USER,
        TEST_USER,
        TEST_USER);
    LambdaTestUtils.intercept(AccessControlException.class,
        "rougeUser is not authorized to cancel the token", () -> {
          secretManager.cancelToken(token, "rougeUser");
        });
  }

  @Test
  public void testVerifySignatureSuccess() throws Exception {
    secretManager = createSecretManager(conf, tokenMaxLifetime,
        expiryTime, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    OzoneTokenIdentifier id = new OzoneTokenIdentifier();
    id.setOmCertSerialId(certificateClient.getCertificate()
        .getSerialNumber().toString());
    id.setMaxDate(Time.now() + 60 * 60 * 24);
    id.setOwner(new Text("test"));
    Assert.assertTrue(secretManager.verifySignature(id,
        certificateClient.signData(id.getBytes())));
  }

  @Test
  public void testVerifySignatureFailure() throws Exception {
    secretManager = createSecretManager(conf, tokenMaxLifetime,
        expiryTime, tokenRemoverScanInterval);
    secretManager.start(certificateClient);
    OzoneTokenIdentifier id = new OzoneTokenIdentifier();
    // set invalid om cert serial id
    id.setOmCertSerialId("1927393");
    id.setMaxDate(Time.now() + 60*60*24);
    id.setOwner(new Text("test"));
    Assert.assertFalse(secretManager.verifySignature(id,
        certificateClient.signData(id.getBytes())));
  }

  /**
   * Validate hash using public key of KeyPair.
   */
  private void validateHash(byte[] hash, byte[] identifier) throws Exception {
    Signature rsaSignature =
        Signature.getInstance(securityConfig.getSignatureAlgo(),
            securityConfig.getProvider());
    rsaSignature.initVerify(certificateClient.getPublicKey());
    rsaSignature.update(identifier);
    Assert.assertTrue(rsaSignature.verify(hash));
  }

  /**
   * Create instance of {@link OzoneDelegationTokenSecretManager}.
   */
  private OzoneDelegationTokenSecretManager
      createSecretManager(OzoneConfiguration config, long tokenMaxLife,
      long expiry, long tokenRemoverScanTime) throws IOException {
    return new OzoneDelegationTokenSecretManager(config, tokenMaxLife,
        expiry, tokenRemoverScanTime, serviceRpcAdd);
  }
}