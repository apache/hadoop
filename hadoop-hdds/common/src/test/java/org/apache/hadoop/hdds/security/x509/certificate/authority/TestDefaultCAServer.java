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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.test.LambdaTestUtils;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.Assert.*;

/**
 * Tests the Default CA Server.
 */
public class TestDefaultCAServer {
  private static OzoneConfiguration conf = new OzoneConfiguration();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private MockCAStore caStore;

  @Before
  public void init() throws IOException {
    conf.set(OZONE_METADATA_DIRS, temporaryFolder.newFolder().toString());
    caStore = new MockCAStore();
  }

  @Test
  public void testInit() throws SCMSecurityException, CertificateException,
      IOException {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore);
    testCA.init(securityConfig, CertificateServer.CAType.SELF_SIGNED_CA);
    X509CertificateHolder first = testCA.getCACertificate();
    assertNotNull(first);
    //Init is idempotent.
    testCA.init(securityConfig, CertificateServer.CAType.SELF_SIGNED_CA);
    X509CertificateHolder second = testCA.getCACertificate();
    assertEquals(first, second);

    // we only support Self Signed CA for now.
    try {
      testCA.init(securityConfig, CertificateServer.CAType.INTERMEDIARY_CA);
      fail("code should not reach here, exception should have been thrown.");
    } catch (IllegalStateException e) {
      // This is a run time exception, hence it is not caught by the junit
      // expected Exception.
      assertTrue(e.toString().contains("Not implemented"));
    }
  }

  @Test
  public void testMissingCertificate() {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore);
    Consumer<SecurityConfig> caInitializer =
        ((DefaultCAServer) testCA).processVerificationStatus(
        DefaultCAServer.VerificationStatus.MISSING_CERTIFICATE);
    try {

      caInitializer.accept(securityConfig);
      fail("code should not reach here, exception should have been thrown.");
    } catch (IllegalStateException e) {
      // This also is a runtime exception. Hence not caught by junit expected
      // exception.
      assertTrue(e.toString().contains("Missing Root Certs"));
    }
  }

  @Test
  public void testMissingKey() {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore);
    Consumer<SecurityConfig> caInitializer =
        ((DefaultCAServer) testCA).processVerificationStatus(
            DefaultCAServer.VerificationStatus.MISSING_KEYS);
    try {

      caInitializer.accept(securityConfig);
      fail("code should not reach here, exception should have been thrown.");
    } catch (IllegalStateException e) {
      // This also is a runtime exception. Hence not caught by junit expected
      // exception.
      assertTrue(e.toString().contains("Missing Keys"));
    }
  }

  /**
   * The most important test of this test suite. This tests that we are able
   * to create a Test CA, creates it own self-Signed CA and then issue a
   * certificate based on a CSR.
   * @throws SCMSecurityException - on ERROR.
   * @throws ExecutionException - on ERROR.
   * @throws InterruptedException - on ERROR.
   * @throws NoSuchProviderException - on ERROR.
   * @throws NoSuchAlgorithmException - on ERROR.
   */
  @Test
  public void testRequestCertificate() throws IOException,
      ExecutionException, InterruptedException,
      NoSuchProviderException, NoSuchAlgorithmException {
    String scmId =  RandomStringUtils.randomAlphabetic(4);
    String clusterId =  RandomStringUtils.randomAlphabetic(4);
    KeyPair keyPair =
        new HDDSKeyGenerator(conf).generateKey();
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setClusterID(clusterId)
        .setScmID(scmId)
        .setSubject("Ozone Cluster")
        .setConfiguration(conf)
        .setKey(keyPair)
        .build();

    // Let us convert this to a string to mimic the common use case.
    String csrString = CertificateSignRequest.getEncodedString(csr);

    CertificateServer testCA = new DefaultCAServer("testCA",
        clusterId, scmId, caStore);
    testCA.init(new SecurityConfig(conf),
        CertificateServer.CAType.SELF_SIGNED_CA);

    Future<X509CertificateHolder> holder = testCA.requestCertificate(csrString,
        CertificateApprover.ApprovalType.TESTING_AUTOMATIC);
    // Right now our calls are synchronous. Eventually this will have to wait.
    assertTrue(holder.isDone());
    assertNotNull(holder.get());
  }

  /**
   * Tests that we are able
   * to create a Test CA, creates it own self-Signed CA and then issue a
   * certificate based on a CSR when scmId and clusterId are not set in
   * csr subject.
   * @throws SCMSecurityException - on ERROR.
   * @throws ExecutionException - on ERROR.
   * @throws InterruptedException - on ERROR.
   * @throws NoSuchProviderException - on ERROR.
   * @throws NoSuchAlgorithmException - on ERROR.
   */
  @Test
  public void testRequestCertificateWithInvalidSubject() throws IOException,
      ExecutionException, InterruptedException,
      NoSuchProviderException, NoSuchAlgorithmException {
    KeyPair keyPair =
        new HDDSKeyGenerator(conf).generateKey();
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setSubject("Ozone Cluster")
        .setConfiguration(conf)
        .setKey(keyPair)
        .build();

    // Let us convert this to a string to mimic the common use case.
    String csrString = CertificateSignRequest.getEncodedString(csr);

    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore);
    testCA.init(new SecurityConfig(conf),
        CertificateServer.CAType.SELF_SIGNED_CA);

    Future<X509CertificateHolder> holder = testCA.requestCertificate(csrString,
        CertificateApprover.ApprovalType.TESTING_AUTOMATIC);
    // Right now our calls are synchronous. Eventually this will have to wait.
    assertTrue(holder.isDone());
    assertNotNull(holder.get());
  }

  @Test
  public void testRequestCertificateWithInvalidSubjectFailure()
      throws Exception {
    KeyPair keyPair =
        new HDDSKeyGenerator(conf).generateKey();
    PKCS10CertificationRequest csr = new CertificateSignRequest.Builder()
        .addDnsName("hadoop.apache.org")
        .addIpAddress("8.8.8.8")
        .setCA(false)
        .setScmID("wrong one")
        .setClusterID("223432rf")
        .setSubject("Ozone Cluster")
        .setConfiguration(conf)
        .setKey(keyPair)
        .build();

    // Let us convert this to a string to mimic the common use case.
    String csrString = CertificateSignRequest.getEncodedString(csr);

    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4), caStore);
    testCA.init(new SecurityConfig(conf),
        CertificateServer.CAType.SELF_SIGNED_CA);

    LambdaTestUtils.intercept(ExecutionException.class, "ScmId and " +
            "ClusterId in CSR subject are incorrect",
        () -> {
          Future<X509CertificateHolder> holder =
              testCA.requestCertificate(csrString,
                  CertificateApprover.ApprovalType.TESTING_AUTOMATIC);
          holder.isDone();
          holder.get();
        });
  }

}
