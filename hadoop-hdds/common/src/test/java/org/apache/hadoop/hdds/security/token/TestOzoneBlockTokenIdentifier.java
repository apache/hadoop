/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.token;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for OzoneManagerDelegationToken.
 */
public class TestOzoneBlockTokenIdentifier {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestOzoneBlockTokenIdentifier.class);
  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestOzoneBlockTokenIdentifier.class.getSimpleName());
  private static final String KEYSTORES_DIR =
      new File(BASEDIR).getAbsolutePath();
  private static long expiryTime;
  private static KeyPair keyPair;
  private static X509Certificate cert;

  @BeforeClass
  public static void setUp() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    expiryTime = Time.monotonicNow() + 60 * 60 * 24;

    // Create Ozone Master key pair.
    keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    // Create Ozone Master certificate (SCM CA issued cert) and key store.
    cert = KeyStoreTestUtil
        .generateCertificate("CN=OzoneMaster", keyPair, 30, "SHA256withRSA");
  }

  @After
  public void cleanUp() throws Exception {
    // KeyStoreTestUtil.cleanupSSLConfig(KEYSTORES_DIR, sslConfsDir);
  }

  @Test
  public void testSignToken() throws GeneralSecurityException, IOException {
    String keystore = new File(KEYSTORES_DIR, "keystore.jks")
        .getAbsolutePath();
    String truststore = new File(KEYSTORES_DIR, "truststore.jks")
        .getAbsolutePath();
    String trustPassword = "trustPass";
    String keyStorePassword = "keyStorePass";
    String keyPassword = "keyPass";


    KeyStoreTestUtil.createKeyStore(keystore, keyStorePassword, keyPassword,
        "OzoneMaster", keyPair.getPrivate(), cert);

    // Create trust store and put the certificate in the trust store
    Map<String, X509Certificate> certs = Collections.singletonMap("server",
        cert);
    KeyStoreTestUtil.createTrustStore(truststore, trustPassword, certs);

    // Sign the OzoneMaster Token with Ozone Master private key
    PrivateKey privateKey = keyPair.getPrivate();
    OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier(
        "testUser", "84940",
        EnumSet.allOf(HddsProtos.BlockTokenSecretProto.AccessModeProto.class),
        expiryTime, cert.getSerialNumber().toString(), 128L);
    byte[] signedToken = signTokenAsymmetric(tokenId, privateKey);

    // Verify a valid signed OzoneMaster Token with Ozone Master
    // public key(certificate)
    boolean isValidToken = verifyTokenAsymmetric(tokenId, signedToken, cert);
    LOG.info("{} is {}", tokenId, isValidToken ? "valid." : "invalid.");

    // Verify an invalid signed OzoneMaster Token with Ozone Master
    // public key(certificate)
    tokenId = new OzoneBlockTokenIdentifier("", "",
        EnumSet.allOf(HddsProtos.BlockTokenSecretProto.AccessModeProto.class),
        expiryTime, cert.getSerialNumber().toString(), 128L);
    LOG.info("Unsigned token {} is {}", tokenId,
        verifyTokenAsymmetric(tokenId, RandomUtils.nextBytes(128), cert));

  }

  @Test
  public void testTokenSerialization() throws GeneralSecurityException,
      IOException {
    String keystore = new File(KEYSTORES_DIR, "keystore.jks")
        .getAbsolutePath();
    String truststore = new File(KEYSTORES_DIR, "truststore.jks")
        .getAbsolutePath();
    String trustPassword = "trustPass";
    String keyStorePassword = "keyStorePass";
    String keyPassword = "keyPass";
    long maxLength = 128L;

    KeyStoreTestUtil.createKeyStore(keystore, keyStorePassword, keyPassword,
        "OzoneMaster", keyPair.getPrivate(), cert);

    // Create trust store and put the certificate in the trust store
    Map<String, X509Certificate> certs = Collections.singletonMap("server",
        cert);
    KeyStoreTestUtil.createTrustStore(truststore, trustPassword, certs);

    // Sign the OzoneMaster Token with Ozone Master private key
    PrivateKey privateKey = keyPair.getPrivate();
    OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier(
        "testUser", "84940",
        EnumSet.allOf(HddsProtos.BlockTokenSecretProto.AccessModeProto.class),
        expiryTime, cert.getSerialNumber().toString(), maxLength);
    byte[] signedToken = signTokenAsymmetric(tokenId, privateKey);


    Token<BlockTokenIdentifier> token = new Token(tokenId.getBytes(),
        signedToken, tokenId.getKind(), new Text("host:port"));

    String encodeToUrlString = token.encodeToUrlString();

    Token<BlockTokenIdentifier>decodedToken = new Token();
    decodedToken.decodeFromUrlString(encodeToUrlString);

    OzoneBlockTokenIdentifier decodedTokenId = new OzoneBlockTokenIdentifier();
    decodedTokenId.readFields(new DataInputStream(
        new ByteArrayInputStream(decodedToken.getIdentifier())));

    Assert.assertEquals(decodedTokenId, tokenId);
    Assert.assertEquals(decodedTokenId.getMaxLength(), maxLength);

    // Verify a decoded signed Token with public key(certificate)
    boolean isValidToken = verifyTokenAsymmetric(decodedTokenId, decodedToken
        .getPassword(), cert);
    LOG.info("{} is {}", tokenId, isValidToken ? "valid." : "invalid.");
  }


  public byte[] signTokenAsymmetric(OzoneBlockTokenIdentifier tokenId,
      PrivateKey privateKey) throws NoSuchAlgorithmException,
      InvalidKeyException, SignatureException {
    Signature rsaSignature = Signature.getInstance("SHA256withRSA");
    rsaSignature.initSign(privateKey);
    rsaSignature.update(tokenId.getBytes());
    byte[] signature = rsaSignature.sign();
    return signature;
  }

  public boolean verifyTokenAsymmetric(OzoneBlockTokenIdentifier tokenId,
      byte[] signature, Certificate certificate) throws InvalidKeyException,
      NoSuchAlgorithmException, SignatureException {
    Signature rsaSignature = Signature.getInstance("SHA256withRSA");
    rsaSignature.initVerify(certificate);
    rsaSignature.update(tokenId.getBytes());
    boolean isValid = rsaSignature.verify(signature);
    return isValid;
  }

  private byte[] signTokenSymmetric(OzoneBlockTokenIdentifier identifier,
      Mac mac, SecretKey key) {
    try {
      mac.init(key);
    } catch (InvalidKeyException ike) {
      throw new IllegalArgumentException("Invalid key to HMAC computation",
          ike);
    }
    return mac.doFinal(identifier.getBytes());
  }

  OzoneBlockTokenIdentifier generateTestToken() {
    return new OzoneBlockTokenIdentifier(RandomStringUtils.randomAlphabetic(6),
        RandomStringUtils.randomAlphabetic(5),
        EnumSet.allOf(HddsProtos.BlockTokenSecretProto.AccessModeProto.class),
        expiryTime, cert.getSerialNumber().toString(), 1024768L);
  }

  @Test
  public void testAsymmetricTokenPerf() throws NoSuchAlgorithmException,
      CertificateEncodingException, NoSuchProviderException,
      InvalidKeyException, SignatureException {
    final int testTokenCount = 1000;
    List<OzoneBlockTokenIdentifier> tokenIds = new ArrayList<>();
    List<byte[]> tokenPasswordAsym = new ArrayList<>();
    for (int i = 0; i < testTokenCount; i++) {
      tokenIds.add(generateTestToken());
    }

    KeyPair kp = KeyStoreTestUtil.generateKeyPair("RSA");

    // Create Ozone Master certificate (SCM CA issued cert) and key store
    X509Certificate certificate;
    certificate = KeyStoreTestUtil.generateCertificate("CN=OzoneMaster",
        kp, 30, "SHA256withRSA");

    long startTime = Time.monotonicNowNanos();
    for (int i = 0; i < testTokenCount; i++) {
      tokenPasswordAsym.add(
          signTokenAsymmetric(tokenIds.get(i), kp.getPrivate()));
    }
    long duration = Time.monotonicNowNanos() - startTime;
    LOG.info("Average token sign time with HmacSha256(RSA/1024 key) is {} ns",
        duration / testTokenCount);

    startTime = Time.monotonicNowNanos();
    for (int i = 0; i < testTokenCount; i++) {
      verifyTokenAsymmetric(tokenIds.get(i), tokenPasswordAsym.get(i),
          certificate);
    }
    duration = Time.monotonicNowNanos() - startTime;
    LOG.info("Average token verify time with HmacSha256(RSA/1024 key) "
        + "is {} ns", duration / testTokenCount);
  }

  @Test
  public void testSymmetricTokenPerf() {
    String hmacSHA1 = "HmacSHA1";
    String hmacSHA256 = "HmacSHA256";

    testSymmetricTokenPerfHelper(hmacSHA1, 64);
    testSymmetricTokenPerfHelper(hmacSHA256, 1024);
  }

  public void testSymmetricTokenPerfHelper(String hmacAlgorithm, int keyLen) {
    final int testTokenCount = 1000;
    List<OzoneBlockTokenIdentifier> tokenIds = new ArrayList<>();
    List<byte[]> tokenPasswordSym = new ArrayList<>();
    for (int i = 0; i < testTokenCount; i++) {
      tokenIds.add(generateTestToken());
    }

    KeyGenerator keyGen;
    try {
      keyGen = KeyGenerator.getInstance(hmacAlgorithm);
      keyGen.init(keyLen);
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find " + hmacAlgorithm +
          " algorithm.");
    }

    Mac mac;
    try {
      mac = Mac.getInstance(hmacAlgorithm);
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find " + hmacAlgorithm +
          " algorithm.");
    }

    SecretKey secretKey = keyGen.generateKey();

    long startTime = Time.monotonicNowNanos();
    for (int i = 0; i < testTokenCount; i++) {
      tokenPasswordSym.add(
          signTokenSymmetric(tokenIds.get(i), mac, secretKey));
    }
    long duration = Time.monotonicNowNanos() - startTime;
    LOG.info("Average token sign time with {}({} symmetric key) is {} ns",
        hmacAlgorithm, keyLen, duration / testTokenCount);
  }

  // TODO: verify certificate with a trust store
  public boolean verifyCert(Certificate certificate) {
    return true;
  }
}