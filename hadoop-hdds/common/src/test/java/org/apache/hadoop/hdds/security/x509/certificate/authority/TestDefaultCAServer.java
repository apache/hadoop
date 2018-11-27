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
import org.bouncycastle.cert.X509CertificateHolder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.security.cert.CertificateException;
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

  @Before
  public void init() throws IOException {
    conf.set(OZONE_METADATA_DIRS, temporaryFolder.newFolder().toString());
  }

  @Test
  public void testInit() throws SCMSecurityException, CertificateException,
      IOException {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateServer testCA = new DefaultCAServer("testCA",
        RandomStringUtils.randomAlphabetic(4),
        RandomStringUtils.randomAlphabetic(4));
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
        RandomStringUtils.randomAlphabetic(4));
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
        RandomStringUtils.randomAlphabetic(4));
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
}