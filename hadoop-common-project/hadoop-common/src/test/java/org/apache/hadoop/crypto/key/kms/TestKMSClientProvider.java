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
import org.slf4j.event.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URL;

import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_KIND;
import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_LEGACY_KIND;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link KMSClientProvider} class.
 */
public class TestKMSClientProvider {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestKMSClientProvider.class);

  private final Token token = new Token();
  private final Token legacyToken = new Token();
  private final String uriString = "kms://https@host:16000/kms";
  private final String legacyTokenService = "host:16000";

  @Rule
  public Timeout globalTimeout = new Timeout(30000);

  {
    GenericTestUtils.setLogLevel(KMSClientProvider.LOG, Level.TRACE);
  }

  @Before
  public void setup() {
    SecurityUtil.setTokenServiceUseIp(false);
    token.setKind(TOKEN_KIND);
    token.setService(new Text(uriString));
    legacyToken.setKind(TOKEN_LEGACY_KIND);
    legacyToken.setService(new Text(legacyTokenService));
  }

  @Test
  public void testNotCopyFromLegacyToken() throws Exception {
    final DelegationTokenAuthenticatedURL url =
        mock(DelegationTokenAuthenticatedURL.class);
    final Configuration conf = new Configuration();
    final URI uri = new URI(uriString);
    final KMSClientProvider kp = new KMSClientProvider(uri, conf, uri);
    try {
      final KMSClientProvider spyKp = spy(kp);
      when(spyKp.createKMSAuthenticatedURL()).thenReturn(url);
      when(url.getDelegationToken(any(URL.class),
          any(DelegationTokenAuthenticatedURL.Token.class), any(String.class),
          any(String.class))).thenReturn(legacyToken);

      final Credentials creds = new Credentials();
      final Token<?>[] tokens = spyKp.addDelegationTokens("yarn", creds);
      LOG.info("Got tokens: {}", tokens);
      assertEquals(1, tokens.length);
      LOG.info("uri:" + uriString);
      // if KMS server returned a legacy token, new client should leave the
      // service being legacy and not set uri string
      assertEquals(legacyTokenService, tokens[0].getService().toString());
    } finally {
      kp.close();
    }
  }

  @Test
  public void testCopyFromToken() throws Exception {
    final DelegationTokenAuthenticatedURL url =
        mock(DelegationTokenAuthenticatedURL.class);
    final Configuration conf = new Configuration();
    final URI uri = new URI(uriString);
    final KMSClientProvider kp = new KMSClientProvider(uri, conf, uri);
    try {
      final KMSClientProvider spyKp = spy(kp);
      when(spyKp.createKMSAuthenticatedURL()).thenReturn(url);
      when(url.getDelegationToken(any(URL.class),
          any(DelegationTokenAuthenticatedURL.Token.class), any(String.class),
          any(String.class))).thenReturn(token);

      final Credentials creds = new Credentials();
      final Token<?>[] tokens = spyKp.addDelegationTokens("yarn", creds);
      LOG.info("Got tokens: {}", tokens);
      assertEquals(2, tokens.length);
      assertTrue(creds.getAllTokens().contains(token));
      assertNotNull(creds.getToken(legacyToken.getService()));
    } finally {
      kp.close();
    }
  }

  @Test
  public void testSelectTokenWhenBothExist() throws Exception {
    final Credentials creds = new Credentials();
    final Configuration conf = new Configuration();
    final URI uri = new URI(uriString);
    final KMSClientProvider kp = new KMSClientProvider(uri, conf, uri);
    try {
      creds.addToken(token.getService(), token);
      creds.addToken(legacyToken.getService(), legacyToken);
      Token t = kp.selectKMSDelegationToken(creds);
      assertEquals(token, t);
    } finally {
      kp.close();
    }
  }

  @Test
  public void testSelectTokenLegacyService() throws Exception {
    final Configuration conf = new Configuration();
    final URI uri = new URI(uriString);
    final KMSClientProvider kp = new KMSClientProvider(uri, conf, uri);
    try {
      Text legacyService = new Text(legacyTokenService);
      token.setService(legacyService);
      final Credentials creds = new Credentials();
      creds.addToken(legacyService, token);
      Token t = kp.selectKMSDelegationToken(creds);
      assertEquals(token, t);
    } finally {
      kp.close();
    }
  }
}
