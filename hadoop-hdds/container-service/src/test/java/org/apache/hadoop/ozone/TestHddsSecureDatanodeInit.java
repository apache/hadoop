/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.HddsDatanodeService.getLogger;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

/**
 * Test class for {@link HddsDatanodeService}.
 */
public class TestHddsSecureDatanodeInit {

  private static File testDir;
  private static OzoneConfiguration conf;
  private static HddsDatanodeService service;
  private static String[] args = new String[]{};
  private static PrivateKey privateKey;
  private static PublicKey publicKey;
  private static GenericTestUtils.LogCapturer dnLogs;
  private static CertificateClient client;
  private static SecurityConfig securityConfig;
  private static KeyCodec keyCodec;
  private static CertificateCodec certCodec;
  private static X509CertificateHolder certHolder;

  @BeforeClass
  public static void setUp() throws Exception {
    testDir = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    //conf.set(ScmConfigKeys.OZONE_SCM_NAMES, "localhost");
    String volumeDir = testDir + "/disk1";
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, volumeDir);

    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setClass(OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY,
        TestHddsDatanodeService.MockService.class,
        ServicePlugin.class);
    securityConfig = new SecurityConfig(conf);

    service = HddsDatanodeService.createHddsDatanodeService(args, conf);
    dnLogs = GenericTestUtils.LogCapturer.captureLogs(getLogger());
    callQuietly(() -> {
      service.start(null);
      return null;
    });
    callQuietly(() -> {
      service.initializeCertificateClient(conf);
      return null;
    });
    certCodec = new CertificateCodec(securityConfig);
    keyCodec = new KeyCodec(securityConfig);
    dnLogs.clearOutput();
    privateKey = service.getCertificateClient().getPrivateKey();
    publicKey = service.getCertificateClient().getPublicKey();
    X509Certificate x509Certificate = null;

    x509Certificate = KeyStoreTestUtil.generateCertificate(
        "CN=Test", new KeyPair(publicKey, privateKey), 10,
        securityConfig.getSignatureAlgo());
    certHolder = new X509CertificateHolder(x509Certificate.getEncoded());

  }

  @AfterClass
  public static void tearDown() {
    FileUtil.fullyDelete(testDir);
  }

  @Before
  public void setUpDNCertClient(){

    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
        .toString(), securityConfig.getPrivateKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation()
        .toString(), securityConfig.getPublicKeyFileName()).toFile());
    FileUtils.deleteQuietly(Paths.get(securityConfig
        .getCertificateLocation().toString(),
        securityConfig.getCertificateFileName()).toFile());
    dnLogs.clearOutput();
    client = new DNCertificateClient(securityConfig,
        certHolder.getSerialNumber().toString());
    service.setCertificateClient(client);
  }

  @Test
  public void testSecureDnStartupCase0() throws Exception {

    // Case 0: When keypair as well as certificate is missing. Initial keypair
    // boot-up. Get certificate will fail as no SCM is not running.
    LambdaTestUtils.intercept(Exception.class, "",
        () -> service.initializeCertificateClient(conf));

    Assert.assertNotNull(client.getPrivateKey());
    Assert.assertNotNull(client.getPublicKey());
    Assert.assertNull(client.getCertificate());
    Assert.assertTrue(dnLogs.getOutput().contains("Init response: GETCERT"));
  }

  @Test
  public void testSecureDnStartupCase1() throws Exception {
    // Case 1: When only certificate is present.

    certCodec.writeCertificate(certHolder);
    LambdaTestUtils.intercept(RuntimeException.class, "DN security" +
            " initialization failed",
        () -> service.initializeCertificateClient(conf));
    Assert.assertNull(client.getPrivateKey());
    Assert.assertNull(client.getPublicKey());
    Assert.assertNotNull(client.getCertificate());
    Assert.assertTrue(dnLogs.getOutput().contains("Init response: FAILURE"));
  }

  @Test
  public void testSecureDnStartupCase2() throws Exception {
    // Case 2: When private key and certificate is missing.
    keyCodec.writePublicKey(publicKey);
    LambdaTestUtils.intercept(RuntimeException.class, "DN security" +
            " initialization failed",
        () -> service.initializeCertificateClient(conf));
    Assert.assertNull(client.getPrivateKey());
    Assert.assertNotNull(client.getPublicKey());
    Assert.assertNull(client.getCertificate());
    Assert.assertTrue(dnLogs.getOutput().contains("Init response: FAILURE"));
  }

  @Test
  public void testSecureDnStartupCase3() throws Exception {
    // Case 3: When only public key and certificate is present.
    keyCodec.writePublicKey(publicKey);
    certCodec.writeCertificate(certHolder);
    LambdaTestUtils.intercept(RuntimeException.class, "DN security" +
            " initialization failed",
        () -> service.initializeCertificateClient(conf));
    Assert.assertNull(client.getPrivateKey());
    Assert.assertNotNull(client.getPublicKey());
    Assert.assertNotNull(client.getCertificate());
    Assert.assertTrue(dnLogs.getOutput().contains("Init response: FAILURE"));
  }

  @Test
  public void testSecureDnStartupCase4() throws Exception {
    // Case 4: When public key as well as certificate is missing.
    keyCodec.writePrivateKey(privateKey);
    LambdaTestUtils.intercept(RuntimeException.class, " DN security" +
            " initialization failed",
        () -> service.initializeCertificateClient(conf));
    Assert.assertNotNull(client.getPrivateKey());
    Assert.assertNull(client.getPublicKey());
    Assert.assertNull(client.getCertificate());
    Assert.assertTrue(dnLogs.getOutput().contains("Init response: FAILURE"));
    dnLogs.clearOutput();
  }

  @Test
  public void testSecureDnStartupCase5() throws Exception {
    // Case 5: If private key and certificate is present.
    certCodec.writeCertificate(certHolder);
    keyCodec.writePrivateKey(privateKey);
    service.initializeCertificateClient(conf);
    Assert.assertNotNull(client.getPrivateKey());
    Assert.assertNotNull(client.getPublicKey());
    Assert.assertNotNull(client.getCertificate());
    Assert.assertTrue(dnLogs.getOutput().contains("Init response: SUCCESS"));
  }

  @Test
  public void testSecureDnStartupCase6() throws Exception {
    // Case 6: If key pair already exist than response should be GETCERT.
    keyCodec.writePublicKey(publicKey);
    keyCodec.writePrivateKey(privateKey);
    LambdaTestUtils.intercept(Exception.class, "",
        () -> service.initializeCertificateClient(conf));
    Assert.assertNotNull(client.getPrivateKey());
    Assert.assertNotNull(client.getPublicKey());
    Assert.assertNull(client.getCertificate());
    Assert.assertTrue(dnLogs.getOutput().contains("Init response: GETCERT"));
  }

  @Test
  public void testSecureDnStartupCase7() throws Exception {
    // Case 7 When keypair and certificate is present.
    keyCodec.writePublicKey(publicKey);
    keyCodec.writePrivateKey(privateKey);
    certCodec.writeCertificate(certHolder);

    service.initializeCertificateClient(conf);
    Assert.assertNotNull(client.getPrivateKey());
    Assert.assertNotNull(client.getPublicKey());
    Assert.assertNotNull(client.getCertificate());
    Assert.assertTrue(dnLogs.getOutput().contains("Init response: SUCCESS"));
  }

  /**
   * Invoke a callable; Ignore all exception.
   * @param closure closure to execute
   * @return
   */
  public static void callQuietly(Callable closure) {
    try {
      closure.call();
    } catch (Throwable e) {
      // Ignore all Throwable,
    }
  }

  @Test
  public void testGetCSR() throws Exception {
    keyCodec.writePublicKey(publicKey);
    keyCodec.writePrivateKey(privateKey);
    service.setCertificateClient(client);
    PKCS10CertificationRequest csr =
        service.getCSR(conf);
    Assert.assertNotNull(csr);

    csr = service.getCSR(conf);
    Assert.assertNotNull(csr);

    csr = service.getCSR(conf);
    Assert.assertNotNull(csr);

    csr = service.getCSR(conf);
    Assert.assertNotNull(csr);
  }

}
