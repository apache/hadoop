/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.hdfs.server.federation.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.SWebHdfs;
import org.apache.hadoop.fs.contract.router.SecurityConfUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Test Delegation Tokens from the Router HTTP interface.
 */
public class TestRouterHttpDelegationToken {

  private Router router;
  private WebHdfsFileSystem fs;

  /**
   * Custom filter to be able to test auth methods and let the other ones go.
   */
  public static final class NoAuthFilter extends AuthenticationFilter {
    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {
      Properties props = new Properties();
      Enumeration<?> names = filterConfig.getInitParameterNames();
      while (names.hasMoreElements()) {
        String name = (String) names.nextElement();
        if (name.startsWith(configPrefix)) {
          String value = filterConfig.getInitParameter(name);
          props.put(name.substring(configPrefix.length()), value);
        }
      }
      props.put(AuthenticationFilter.AUTH_TYPE, "simple");
      props.put(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
      return props;
    }
  }

  @Before
  public void setup() throws Exception {
    Configuration conf = SecurityConfUtil.initSecurity();
    conf.set(RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(RBFConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_KEY,
        NoAuthFilter.class.getName());

    // Start routers with an RPC and HTTP service only
    Configuration routerConf = new RouterConfigBuilder()
        .rpc()
        .http()
        .build();

    conf.addResource(routerConf);
    router = new Router();
    router.init(conf);
    router.start();

    InetSocketAddress webAddress = router.getHttpServerAddress();
    URI webURI = new URI(SWebHdfs.SCHEME, null,
        webAddress.getHostName(), webAddress.getPort(), null, null, null);
    fs = (WebHdfsFileSystem)FileSystem.get(webURI, conf);
  }

  @After
  public void cleanup() throws Exception {
    if (router != null) {
      router.stop();
      router.close();
    }
  }

  @Test
  public void testGetDelegationToken() throws Exception {
    final String renewer = "renewer0";
    Token<?> token = fs.getDelegationToken(renewer);
    assertNotNull(token);

    DelegationTokenIdentifier tokenId =
        getTokenIdentifier(token.getIdentifier());
    assertEquals("router", tokenId.getOwner().toString());
    assertEquals(renewer, tokenId.getRenewer().toString());
    assertEquals("", tokenId.getRealUser().toString());
    assertEquals("SWEBHDFS delegation", token.getKind().toString());
    assertNotNull(token.getPassword());

    InetSocketAddress webAddress = router.getHttpServerAddress();
    assertEquals(webAddress.getHostName() + ":" + webAddress.getPort(),
        token.getService().toString());
  }

  @Test
  public void testRenewDelegationToken() throws Exception {
    Token<?> token = fs.getDelegationToken("router");
    DelegationTokenIdentifier tokenId =
        getTokenIdentifier(token.getIdentifier());

    long t = fs.renewDelegationToken(token);
    assertTrue(t + " should not be larger than " + tokenId.getMaxDate(),
        t <= tokenId.getMaxDate());
  }

  @Test
  public void testCancelDelegationToken() throws Exception {
    Token<?> token = fs.getDelegationToken("router");
    fs.cancelDelegationToken(token);
    LambdaTestUtils.intercept(InvalidToken.class,
        "Renewal request for unknown token",
        () -> fs.renewDelegationToken(token));
  }

  private DelegationTokenIdentifier getTokenIdentifier(byte[] id)
      throws IOException {
    DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
    ByteArrayInputStream bais = new ByteArrayInputStream(id);
    DataInputStream dais = new DataInputStream(bais);
    identifier.readFields(dais);
    return identifier;
  }
}
