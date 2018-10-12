/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_KIND;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit test for {@link KMSClientProvider} class.
 */
public class TestKMSClientProvider {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestKMSClientProvider.class);

  private final Token token = new Token();
  private final Token oldToken = new Token();
  private final String urlString = "https://host:16000/kms";
  private final String providerUriString = "kms://https@host:16000/kms";
  private final String oldTokenService = "host:16000";

  @Rule
  public Timeout globalTimeout = new Timeout(60000);

  {
    GenericTestUtils.setLogLevel(KMSClientProvider.LOG, Level.TRACE);
  }

  @Before
  public void setup() {
    SecurityUtil.setTokenServiceUseIp(false);
    token.setKind(TOKEN_KIND);
    token.setService(new Text(providerUriString));
    oldToken.setKind(TOKEN_KIND);
    oldToken.setService(new Text(oldTokenService));
  }

  @Test
  public void testSelectDelegationToken() throws Exception {
    final Credentials creds = new Credentials();
    creds.addToken(new Text(providerUriString), token);
    assertNull(KMSClientProvider.selectDelegationToken(creds, null));
    assertNull(KMSClientProvider
        .selectDelegationToken(creds, new Text(oldTokenService)));
    assertEquals(token, KMSClientProvider
        .selectDelegationToken(creds, new Text(providerUriString)));
  }

  @Test
  public void testSelectTokenOldService() throws Exception {
    final Configuration conf = new Configuration();
    final URI uri = new URI(providerUriString);
    final KMSClientProvider kp = new KMSClientProvider(uri, conf);
    try {
      final Credentials creds = new Credentials();
      creds.addToken(new Text(oldTokenService), oldToken);
      final Token t = kp.selectDelegationToken(creds);
      assertEquals(oldToken, t);
    } finally {
      kp.close();
    }
  }

  @Test
  public void testSelectTokenWhenBothExist() throws Exception {
    final Credentials creds = new Credentials();
    final Configuration conf = new Configuration();
    final URI uri = new URI(providerUriString);
    final KMSClientProvider kp = new KMSClientProvider(uri, conf);
    try {
      creds.addToken(token.getService(), token);
      creds.addToken(oldToken.getService(), oldToken);
      final Token t = kp.selectDelegationToken(creds);
      assertEquals("new token should be selected when both exist", token, t);
    } finally {
      kp.close();
    }
  }

  @Test
  public void testURLSelectTokenUriFormat() throws Exception {
    testURLSelectToken(token);
  }

  @Test
  public void testURLSelectTokenIpPort() throws Exception {
    testURLSelectToken(oldToken);
  }

  private void testURLSelectToken(final Token tok)
      throws URISyntaxException, IOException {
    final Configuration conf = new Configuration();
    final URI uri = new URI(providerUriString);
    final KMSClientProvider kp = new KMSClientProvider(uri, conf);
    final DelegationTokenAuthenticatedURL url = kp.createAuthenticatedURL();
    final Credentials creds = new Credentials();
    creds.addToken(tok.getService(), tok);
    final Token chosen = url.selectDelegationToken(new URL(urlString), creds);
    assertEquals(tok, chosen);
  }
}