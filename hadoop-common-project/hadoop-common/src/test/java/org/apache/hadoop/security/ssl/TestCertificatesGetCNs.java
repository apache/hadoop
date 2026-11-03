/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.security.ssl;

import java.security.KeyPair;
import java.security.cert.X509Certificate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SSLHostnameVerifier.Certificates#getCNs(X509Certificate)},
 * checking that CN values are read from the subject's CN relative
 * distinguished names and nothing else.
 */
public class TestCertificatesGetCNs {

  private static final int DAYS = 30;

  private static final String ALG = "SHA256withRSA";

  /**
   * Get the Common Names of an X509 Distinguished Name as round tripped
   * through the {@link SSLHostnameVerifier}.
   * @param dn Distinguished Name.
   * @return extracted CNs.
   */
  private String[] commonNames(String dn) throws Exception {
    KeyPair pair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate cert =
        KeyStoreTestUtil.generateCertificate(dn, pair, DAYS, ALG);
    return SSLHostnameVerifier.Certificates.getCNs(cert);
  }

  @Test
  public void testSingleCN() throws Exception {
    assertHasCommonName("CN=good.example.com, O=Example, C=US", "good.example.com");
  }

  /**
   * Assert Common Name is as expected.
   * @param dn Distinguished Name.
   * @param expected expected common name
   */
  private void assertHasCommonName(final String dn,
      final String expected) throws Exception {
    assertThat(commonNames(dn))
        .describedAs("Common Name of %s", dn)
        .containsExactly(expected);
  }

  /**
   * Assert Common Name array is null.
   * @param dn Distinguished Name.
   */
  private void assertNullCommonNames(final String dn) throws Exception {
    assertThat(commonNames(dn))
        .describedAs("Common Name of %s", dn)
        .isNull();
  }

  @Test
  public void testMostSignificantFirst() throws Exception {
    assertThat(commonNames("CN=first.example.com, OU=unit, CN=second.example.com"))
        .containsExactly("first.example.com", "second.example.com");
  }

  @Test
  public void testNoCN() throws Exception {
    assertNullCommonNames("OU=unit, O=Example, C=US");
  }

  @Test
  public void testCnTextInOtherAttributeIsIgnored() throws Exception {
    assertHasCommonName("CN=real.example.com, OU=CN=other.example.com\\,",
        "real.example.com");
  }

  @Test
  public void testCnTextInLeadingAttributeIsIgnored() throws Exception {
    assertNullCommonNames("OU=CN=other.example.com\\,, O=Example");
  }
}