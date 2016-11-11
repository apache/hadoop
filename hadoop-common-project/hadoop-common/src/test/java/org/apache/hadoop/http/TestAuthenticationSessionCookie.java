/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.http;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Test;
import org.eclipse.jetty.util.log.Log;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.HttpCookie;
import java.util.HashMap;
import java.util.List;

public class TestAuthenticationSessionCookie {
  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestHttpCookieFlag.class.getSimpleName());
  private static boolean isCookiePersistent;
  private static final long TOKEN_VALIDITY_SEC = 1000;
  private static long expires;
  private static String keystoresDir;
  private static String sslConfDir;
  private static HttpServer2 server;

  public static class DummyAuthenticationFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
      isCookiePersistent = false;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain chain) throws IOException,
                                                   ServletException {
      HttpServletResponse resp = (HttpServletResponse) response;
      AuthenticationFilter.createAuthCookie(resp, "token", null, null, expires,
              isCookiePersistent, true);
      chain.doFilter(request, resp);
    }

    @Override
    public void destroy() {
    }
  }

  public static class DummyFilterInitializer extends FilterInitializer {
    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
      container.addFilter("DummyAuth", DummyAuthenticationFilter.class
              .getName(), new HashMap<>());
    }
  }

  public static class Dummy2AuthenticationFilter
  extends DummyAuthenticationFilter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
      isCookiePersistent = true;
      expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    }

    @Override
    public void destroy() {
    }
  }

  public static class Dummy2FilterInitializer extends FilterInitializer {
    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
      container.addFilter("Dummy2Auth", Dummy2AuthenticationFilter.class
              .getName(), new HashMap<>());
    }
  }

  public void startServer(boolean isTestSessionCookie) throws Exception {
    Configuration conf = new Configuration();
    if (isTestSessionCookie) {
      conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
            DummyFilterInitializer.class.getName());
    } else {
      conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
            Dummy2FilterInitializer.class.getName());
    }

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    Configuration sslConf = KeyStoreTestUtil.getSslConfig();

    server = new HttpServer2.Builder()
            .setName("test")
            .addEndpoint(new URI("http://localhost"))
            .addEndpoint(new URI("https://localhost"))
            .setConf(conf)
            .keyPassword(sslConf.get("ssl.server.keystore.keypassword"))
            .keyStore(sslConf.get("ssl.server.keystore.location"),
                    sslConf.get("ssl.server.keystore.password"),
                    sslConf.get("ssl.server.keystore.type", "jks"))
            .trustStore(sslConf.get("ssl.server.truststore.location"),
                    sslConf.get("ssl.server.truststore.password"),
                    sslConf.get("ssl.server.truststore.type", "jks")).build();
    server.addServlet("echo", "/echo", TestHttpServer.EchoServlet.class);
    server.start();
  }

  @Test
  public void testSessionCookie() throws IOException {
    try {
        startServer(true);
    } catch (Exception e) {
        // Auto-generated catch block
        e.printStackTrace();
    }

    URL base = new URL("http://" + NetUtils.getHostPortString(server
            .getConnectorAddress(0)));
    HttpURLConnection conn = (HttpURLConnection) new URL(base,
            "/echo").openConnection();

    String header = conn.getHeaderField("Set-Cookie");
    List<HttpCookie> cookies = HttpCookie.parse(header);
    Assert.assertTrue(!cookies.isEmpty());
    Log.getLog().info(header);
    Assert.assertFalse(header.contains("; Expires="));
    Assert.assertTrue("token".equals(cookies.get(0).getValue()));
  }
  
  @Test
  public void testPersistentCookie() throws IOException {
    try {
        startServer(false);
    } catch (Exception e) {
        // Auto-generated catch block
        e.printStackTrace();
    }

    URL base = new URL("http://" + NetUtils.getHostPortString(server
            .getConnectorAddress(0)));
    HttpURLConnection conn = (HttpURLConnection) new URL(base,
            "/echo").openConnection();

    String header = conn.getHeaderField("Set-Cookie");
    List<HttpCookie> cookies = HttpCookie.parse(header);
    Assert.assertTrue(!cookies.isEmpty());
    Log.getLog().info(header);
    Assert.assertTrue(header.contains("; Expires="));
    Assert.assertTrue("token".equals(cookies.get(0).getValue()));
  }

  @After
  public void cleanup() throws Exception {
    server.stop();
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }
}
