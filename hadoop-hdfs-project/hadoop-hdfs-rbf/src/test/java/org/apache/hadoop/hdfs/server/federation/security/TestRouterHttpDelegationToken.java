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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_HTTP_AUTHENTICATION_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.SWebHdfs;
import org.apache.hadoop.fs.contract.router.SecurityConfUtil;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.RenewerParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Test Delegation Tokens from the Router HTTP interface.
 */
public class TestRouterHttpDelegationToken {

  public static final String FILTER_INITIALIZER_PROPERTY =
      "hadoop.http.filter.initializers";
  private Router router;
  private WebHdfsFileSystem fs;

  /**
   * The initializer of custom filter.
   */
  public static final class NoAuthFilterInitializer
      extends AuthenticationFilterInitializer {
    static final String PREFIX = "hadoop.http.authentication.";

    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
      Map<String, String> filterConfig = getFilterConfigMap(conf, PREFIX);
      container.addFilter("authentication", NoAuthFilter.class.getName(),
          filterConfig);
    }
  }

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
    conf.set(FILTER_INITIALIZER_PROPERTY,
        NoAuthFilterInitializer.class.getName());
    conf.set(HADOOP_HTTP_AUTHENTICATION_TYPE, "simple");

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
    SecurityConfUtil.destroy();
  }

  @Test
  public void testGetDelegationToken() throws Exception {
    final String renewer = "renewer0";
    Token<?> token = getDelegationToken(fs, renewer);
    assertNotNull(token);

    DelegationTokenIdentifier tokenId =
        getTokenIdentifier(token.getIdentifier());
    assertEquals("router", tokenId.getOwner().toString());
    assertEquals(renewer, tokenId.getRenewer().toString());
    assertEquals("", tokenId.getRealUser().toString());
    assertEquals("SWEBHDFS delegation", token.getKind().toString());
    assertNotNull(token.getPassword());
  }

  @Test
  public void testRenewDelegationToken() throws Exception {
    Token<?> token = getDelegationToken(fs, "router");
    DelegationTokenIdentifier tokenId =
        getTokenIdentifier(token.getIdentifier());

    long t = renewDelegationToken(fs, token);
    assertTrue(t + " should not be larger than " + tokenId.getMaxDate(),
        t <= tokenId.getMaxDate());
  }

  @Test
  public void testCancelDelegationToken() throws Exception {
    Token<?> token = getDelegationToken(fs, "router");
    cancelDelegationToken(fs, token);
    LambdaTestUtils.intercept(IOException.class,
        "Server returned HTTP response code: 403 ",
        () -> renewDelegationToken(fs, token));
  }

  private Token<DelegationTokenIdentifier> getDelegationToken(
      WebHdfsFileSystem webHdfs, String renewer) throws IOException {
    Map<?, ?> json = sendHttpRequest(webHdfs, GetOpParam.Op.GETDELEGATIONTOKEN,
        new RenewerParam(renewer));
    return WebHdfsTestUtil.convertJsonToDelegationToken(json);
  }

  private long renewDelegationToken(WebHdfsFileSystem webHdfs, Token<?> token)
      throws IOException {
    Map<?, ?> json =
        sendHttpRequest(webHdfs, PutOpParam.Op.RENEWDELEGATIONTOKEN,
            new TokenArgumentParam(token.encodeToUrlString()));
    return ((Number) json.get("long")).longValue();
  }

  private void cancelDelegationToken(WebHdfsFileSystem webHdfs, Token<?> token)
      throws IOException {
    sendHttpRequest(webHdfs, PutOpParam.Op.CANCELDELEGATIONTOKEN,
        new TokenArgumentParam(token.encodeToUrlString()));
  }

  private Map<?, ?> sendHttpRequest(WebHdfsFileSystem webHdfs,
      final HttpOpParam.Op op, final Param<?, ?>... parameters)
      throws IOException {
    String user = SecurityConfUtil.getRouterUserName();
    // process parameters, add user.name
    List<Param<?, ?>> pList = new ArrayList<>();
    pList.add(new UserParam(user));
    pList.addAll(Arrays.asList(parameters));

    // build request url
    final URL url = WebHdfsTestUtil.toUrl(webHdfs, op, null,
        pList.toArray(new Param<?, ?>[pList.size()]));

    // open connection and send request
    HttpURLConnection conn =
        WebHdfsTestUtil.openConnection(url, webHdfs.getConf());
    conn.setRequestMethod(op.getType().toString());
    WebHdfsTestUtil.sendRequest(conn);
    final Map<?, ?> json = WebHdfsTestUtil.getAndParseResponse(conn);
    conn.disconnect();
    return json;
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
