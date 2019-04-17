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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.bouncycastle.cert.X509CertificateHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.GETCERT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.RECOVER;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.SUCCESS;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link DefaultCertificateClient}.
 */
@RunWith(Parameterized.class)
@SuppressWarnings("visibilitymodifier")
public class TestCertificateClientInit {

  private KeyPair keyPair;
  private String certSerialId = "3284792342234";
  private CertificateClient dnCertificateClient;
  private CertificateClient omCertificateClient;
  private HDDSKeyGenerator keyGenerator;
  private Path metaDirPath;
  private SecurityConfig securityConfig;
  private KeyCodec keyCodec;
  private X509Certificate x509Certificate;

  @Parameter
  public boolean pvtKeyPresent;
  @Parameter(1)
  public boolean pubKeyPresent;
  @Parameter(2)
  public boolean certPresent;
  @Parameter(3)
  public InitResponse expectedResult;

  @Parameterized.Parameters
  public static Collection<Object[]> initData() {
    return Arrays.asList(new Object[][]{
        {false, false, false, GETCERT},
        {false, false, true, FAILURE},
        {false, true, false, FAILURE},
        {true, false, false, FAILURE},
        {false, true, true, FAILURE},
        {true, true, false, GETCERT},
        {true, false, true, SUCCESS},
        {true, true, true, SUCCESS}});
  }

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    final String path = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());
    metaDirPath = Paths.get(path, "test");
    config.set(HDDS_METADATA_DIR_NAME, metaDirPath.toString());
    securityConfig = new SecurityConfig(config);
    keyGenerator = new HDDSKeyGenerator(securityConfig);
    keyPair = keyGenerator.generateKey();
    x509Certificate = getX509Certificate();
    certSerialId = x509Certificate.getSerialNumber().toString();
    dnCertificateClient = new DNCertificateClient(securityConfig,
        certSerialId);
    omCertificateClient = new OMCertificateClient(securityConfig,
        certSerialId);
    keyCodec = new KeyCodec(securityConfig);

    Files.createDirectories(securityConfig.getKeyLocation());
  }

  @After
  public void tearDown() {
    dnCertificateClient = null;
    omCertificateClient = null;
    FileUtils.deleteQuietly(metaDirPath.toFile());
  }


  @Test
  public void testInitDatanode() throws Exception {
    if (pvtKeyPresent) {
      keyCodec.writePrivateKey(keyPair.getPrivate());
    } else {
      FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
          .toString(), securityConfig.getPrivateKeyFileName()).toFile());
    }

    if (pubKeyPresent) {
      if (dnCertificateClient.getPublicKey() == null) {
        keyCodec.writePublicKey(keyPair.getPublic());
      }
    } else {
      FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
          .toString(), securityConfig.getPublicKeyFileName()).toFile());
    }

    if (certPresent) {
      CertificateCodec codec = new CertificateCodec(securityConfig);
      codec.writeCertificate(new X509CertificateHolder(
          x509Certificate.getEncoded()));
    } else {
      FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
          .toString(), securityConfig.getCertificateFileName()).toFile());
    }
    InitResponse response = dnCertificateClient.init();

    assertTrue(response.equals(expectedResult));

    if (!response.equals(FAILURE)) {
      assertTrue(OzoneSecurityUtil.checkIfFileExist(
          securityConfig.getKeyLocation(),
          securityConfig.getPrivateKeyFileName()));
      assertTrue(OzoneSecurityUtil.checkIfFileExist(
          securityConfig.getKeyLocation(),
          securityConfig.getPublicKeyFileName()));
    }
  }

  @Test
  public void testInitOzoneManager() throws Exception {
    if (pvtKeyPresent) {
      keyCodec.writePrivateKey(keyPair.getPrivate());
    } else {
      FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
          .toString(), securityConfig.getPrivateKeyFileName()).toFile());
    }

    if (pubKeyPresent) {
      if (omCertificateClient.getPublicKey() == null) {
        keyCodec.writePublicKey(keyPair.getPublic());
      }
    } else {
      FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
          .toString(), securityConfig.getPublicKeyFileName()).toFile());
    }

    if (certPresent) {
      CertificateCodec codec = new CertificateCodec(securityConfig);
      codec.writeCertificate(new X509CertificateHolder(
          x509Certificate.getEncoded()));
    } else {
      FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
          .toString(), securityConfig.getCertificateFileName()).toFile());
    }
    InitResponse response = omCertificateClient.init();

    if (pvtKeyPresent && pubKeyPresent & !certPresent) {
      assertTrue(response.equals(RECOVER));
    } else {
      assertTrue(response.equals(expectedResult));
    }

    if (!response.equals(FAILURE)) {
      assertTrue(OzoneSecurityUtil.checkIfFileExist(
          securityConfig.getKeyLocation(),
          securityConfig.getPrivateKeyFileName()));
      assertTrue(OzoneSecurityUtil.checkIfFileExist(
          securityConfig.getKeyLocation(),
          securityConfig.getPublicKeyFileName()));
    }
  }

  private X509Certificate getX509Certificate() throws Exception {
    return KeyStoreTestUtil.generateCertificate(
        "CN=Test", keyPair, 10, securityConfig.getSignatureAlgo());
  }
}