/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.token.delegation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.NetUtilsTestResolver;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reproduces a delegation-token service mismatch when
 * {@code hadoop.security.token.service.use_ip} is false and the DNS search
 * domain keeps the trailing dot that dnsjava {@code Name.toString()} emits.
 *
 * <p>A short NameNode host such as {@code SERVER} is expanded to
 * {@code server.example.com.} (note the final dot), so the client looks up
 * a token for {@code server.example.com.:8020}. The token held for the FQDN
 * has service {@code server.example.com:8020}; token selection requires an
 * exact match, so the client fails with
 * "Client cannot authenticate via:[TOKEN, KERBEROS]".
 */
public class TestTrailingDotSearchDomainDelegationToken {

  private static final String SEARCH_DOMAIN = "example.com";
  private static final String DOTTED_SEARCH_DOMAIN = SEARCH_DOMAIN + ".";
  /** Root domain, as dnsjava reports {@code search .} from resolv.conf. */
  private static final String ROOT_SEARCH_DOMAIN = ".";
  private static final String SHORT_HOST = "SERVER";
  private static final String FQDN = "server.example.com";
  private static final String IP = "10.20.30.40";
  /** Default HDFS NameNode RPC port; kept here to avoid hdfs-client test dep. */
  private static final int NN_RPC_PORT = 8020;

  private static final URI SHORT_URI =
      URI.create("hdfs://" + SHORT_HOST + ":" + NN_RPC_PORT);
  private static final URI FQDN_URI =
      URI.create("hdfs://" + FQDN + ":" + NN_RPC_PORT);
  private static final URI UPPER_FQDN_URI =
      URI.create("hdfs://" + SHORT_HOST + "." + SEARCH_DOMAIN + ":" + NN_RPC_PORT);

  private static final Text HDFS_DELEGATION_KIND =
      new Text("HDFS_DELEGATION_TOKEN");

  @BeforeEach
  public void useHostnamesForTokenService() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, false);
    SecurityUtil.setConfiguration(conf);
  }

  @AfterEach
  public void restoreTokenServiceResolver() {
    SecurityUtil.setTokenServiceUseIp(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT);
  }

  @Test
  public void testSearchDomainWithoutTrailingDotMatchesFqdnToken() {
    assertSameTokenService(SEARCH_DOMAIN, SHORT_URI, FQDN_URI);
  }

  @Test
  public void testSearchDomainWithTrailingDotMatchesFqdnToken() {
    // dnsjava Name.toString() yields "example.com." rather than "example.com".
    assertSameTokenService(DOTTED_SEARCH_DOMAIN, SHORT_URI, FQDN_URI);
  }

  @Test
  public void testQualifiedHostnameIgnoresTrailingDotSearchDomain() {
    assertSameTokenService(DOTTED_SEARCH_DOMAIN, UPPER_FQDN_URI, FQDN_URI);
  }

  /**
   * The root search domain stands for the short host itself, so the token
   * service must be the unrooted short name, ahead of later search domains.
   */
  @Test
  public void testRootSearchDomainMatchesShortHostToken() {
    NetUtilsTestResolver resolver =
        installResolver(ROOT_SEARCH_DOMAIN, SEARCH_DOMAIN);
    assertClientSelectsToken(resolver, SHORT_URI,
        new Text("server:" + NN_RPC_PORT));
  }

  private static void assertSameTokenService(String searchDomain,
      URI clientUri, URI tokenUri) {
    NetUtilsTestResolver resolver = installResolver(searchDomain);
    assertClientSelectsToken(resolver, clientUri, buildTokenService(tokenUri));
  }

  private static void assertClientSelectsToken(NetUtilsTestResolver resolver,
      URI clientUri, Text heldService) {
    Text clientService = buildTokenService(clientUri);
    assertEquals(heldService.toString(), clientService.toString(),
        "token service for " + clientUri
            + " must match the held token " + heldService
            + "; lookups " + Arrays.toString(resolver.getHostSearches()));

    Token<TokenIdentifier> held = new Token<>(
        new byte[] {1}, new byte[] {2}, HDFS_DELEGATION_KIND, heldService);
    List<Token<? extends TokenIdentifier>> tokens = new ArrayList<>();
    tokens.add(held);
    Token<?> selected = selectToken(clientService, tokens);
    assertNotNull(selected, "client has no delegation token for " + clientService
        + "; held token is " + heldService);
    assertEquals(heldService, selected.getService());
  }

  /**
   * Same matching rule as {@link AbstractDelegationTokenSelector#selectToken}.
   */
  private static Token<?> selectToken(Text service,
      List<Token<? extends TokenIdentifier>> tokens) {
    if (service == null) {
      return null;
    }
    for (Token<? extends TokenIdentifier> token : tokens) {
      if (HDFS_DELEGATION_KIND.equals(token.getKind())
          && service.equals(token.getService())) {
        return token;
      }
    }
    return null;
  }

  private static Text buildTokenService(URI uri) {
    return SecurityUtil.buildTokenService(
        NetUtils.createSocketAddr(uri.getAuthority(), NN_RPC_PORT));
  }

  /**
   * Install a resolver whose search list is exactly {@code searchDomains}.
   * The registered names are the rooted lookups of the short host under
   * each domain, with the caller's host case left intact.
   */
  private static NetUtilsTestResolver installResolver(
      String... searchDomains) {
    NetUtilsTestResolver resolver = NetUtilsTestResolver.install();
    resolver.setSearchDomains(searchDomains);
    for (String domain : searchDomains) {
      if (ROOT_SEARCH_DOMAIN.equals(domain)) {
        resolver.addResolvedHost(SHORT_HOST + ".", IP);
      } else {
        String relative = domain.endsWith(".")
            ? domain.substring(0, domain.length() - 1)
            : domain;
        resolver.addResolvedHost(SHORT_HOST + "." + relative + ".", IP);
      }
    }
    resolver.addResolvedHost(FQDN + ".", IP);
    resolver.reset();
    return resolver;
  }
}
