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
package org.apache.hadoop.security.authentication.client;

import org.apache.catalina.deploy.FilterDef;
import org.apache.catalina.deploy.FilterMap;
import org.apache.catalina.startup.Tomcat;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.auth.SPNegoScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.DispatcherType;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.security.Principal;
import java.util.EnumSet;
import java.util.Properties;

import org.junit.Assert;

public class AuthenticatorTestCase {
  private Server server;
  private String host = null;
  private int port = -1;
  private boolean useTomcat = false;
  private Tomcat tomcat = null;
  ServletContextHandler context;

  private static Properties authenticatorConfig;

  public AuthenticatorTestCase() {}

  public AuthenticatorTestCase(boolean useTomcat) {
    this.useTomcat = useTomcat;
  }

  protected static void setAuthenticationHandlerConfig(Properties config) {
    authenticatorConfig = config;
  }

  public static class TestFilter extends AuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
      return authenticatorConfig;
    }
  }

  @SuppressWarnings("serial")
  public static class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      InputStream is = req.getInputStream();
      OutputStream os = resp.getOutputStream();
      int c = is.read();
      while (c > -1) {
        os.write(c);
        c = is.read();
      }
      is.close();
      os.close();
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  protected int getLocalPort() throws Exception {
    ServerSocket ss = new ServerSocket(0);
    int ret = ss.getLocalPort();
    ss.close();
    return ret;
  }

  protected void start() throws Exception {
    if (useTomcat) startTomcat();
    else startJetty();
  }

  protected void startJetty() throws Exception {
    server = new Server();
    context = new ServletContextHandler();
    context.setContextPath("/foo");
    server.setHandler(context);
    context.addFilter(new FilterHolder(TestFilter.class), "/*",
        EnumSet.of(DispatcherType.REQUEST));
    context.addServlet(new ServletHolder(TestServlet.class), "/bar");
    host = "localhost";
    port = getLocalPort();
    ServerConnector connector = new ServerConnector(server);
    connector.setHost(host);
    connector.setPort(port);
    server.setConnectors(new Connector[] {connector});
    server.start();
    System.out.println("Running embedded servlet container at: http://" + host + ":" + port);
  }

  protected void startTomcat() throws Exception {
    tomcat = new Tomcat();
    File base = new File(System.getProperty("java.io.tmpdir"));
    org.apache.catalina.Context ctx =
      tomcat.addContext("/foo",base.getAbsolutePath());
    FilterDef fd = new FilterDef();
    fd.setFilterClass(TestFilter.class.getName());
    fd.setFilterName("TestFilter");
    FilterMap fm = new FilterMap();
    fm.setFilterName("TestFilter");
    fm.addURLPattern("/*");
    fm.addServletName("/bar");
    ctx.addFilterDef(fd);
    ctx.addFilterMap(fm);
    tomcat.addServlet(ctx, "/bar", TestServlet.class.getName());
    ctx.addServletMapping("/bar", "/bar");
    host = "localhost";
    port = getLocalPort();
    tomcat.setHostname(host);
    tomcat.setPort(port);
    tomcat.start();
  }

  protected void stop() throws Exception {
    if (useTomcat) stopTomcat();
    else stopJetty();
  }

  protected void stopJetty() throws Exception {
    try {
      server.stop();
    } catch (Exception e) {
    }

    try {
      server.destroy();
    } catch (Exception e) {
    }
  }

  protected void stopTomcat() throws Exception {
    try {
      tomcat.stop();
    } catch (Exception e) {
    }

    try {
      tomcat.destroy();
    } catch (Exception e) {
    }
  }

  protected String getBaseURL() {
    return "http://" + host + ":" + port + "/foo/bar";
  }

  private static class TestConnectionConfigurator
      implements ConnectionConfigurator {
    boolean invoked;

    @Override
    public HttpURLConnection configure(HttpURLConnection conn)
        throws IOException {
      invoked = true;
      return conn;
    }
  }

  private String POST = "test";

  protected void _testAuthentication(Authenticator authenticator, boolean doPost) throws Exception {
    start();
    try {
      URL url = new URL(getBaseURL());
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      Assert.assertFalse(token.isSet());
      TestConnectionConfigurator connConf = new TestConnectionConfigurator();
      AuthenticatedURL aUrl = new AuthenticatedURL(authenticator, connConf);
      HttpURLConnection conn = aUrl.openConnection(url, token);
      Assert.assertTrue(connConf.invoked);
      String tokenStr = token.toString();
      if (doPost) {
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
      }
      conn.connect();
      if (doPost) {
        Writer writer = new OutputStreamWriter(conn.getOutputStream());
        writer.write(POST);
        writer.close();
      }
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      if (doPost) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String echo = reader.readLine();
        Assert.assertEquals(POST, echo);
        Assert.assertNull(reader.readLine());
      }
      aUrl = new AuthenticatedURL();
      conn = aUrl.openConnection(url, token);
      conn.connect();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertEquals(tokenStr, token.toString());
    } finally {
      stop();
    }
  }

  private HttpClient getHttpClient() {
    HttpClientBuilder builder = HttpClientBuilder.create();
    // Register auth schema
    builder.setDefaultAuthSchemeRegistry(
        s-> httpContext -> new SPNegoScheme(true, true)
    );

    Credentials useJaasCreds = new Credentials() {
      public String getPassword() {
        return null;
      }
      public Principal getUserPrincipal() {
        return null;
      }
    };

    CredentialsProvider jaasCredentialProvider
        = new BasicCredentialsProvider();
    jaasCredentialProvider.setCredentials(AuthScope.ANY, useJaasCreds);
    // Set credential provider
    builder.setDefaultCredentialsProvider(jaasCredentialProvider);

    return builder.build();
  }

  private void doHttpClientRequest(HttpClient httpClient, HttpUriRequest request) throws Exception {
    HttpResponse response = null;
    try {
      response = httpClient.execute(request);
      final int httpStatus = response.getStatusLine().getStatusCode();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, httpStatus);
    } finally {
      if (response != null) EntityUtils.consumeQuietly(response.getEntity());
    }
  }

  protected void _testAuthenticationHttpClient(Authenticator authenticator, boolean doPost) throws Exception {
    start();
    try {
      HttpClient httpClient = getHttpClient();
      doHttpClientRequest(httpClient, new HttpGet(getBaseURL()));

      // Always do a GET before POST to trigger the SPNego negotiation
      if (doPost) {
        HttpPost post = new HttpPost(getBaseURL());
        byte [] postBytes = POST.getBytes();
        ByteArrayInputStream bis = new ByteArrayInputStream(postBytes);
        InputStreamEntity entity = new InputStreamEntity(bis, postBytes.length);

        // Important that the entity is not repeatable -- this means if
        // we have to renegotiate (e.g. b/c the cookie wasn't handled properly)
        // the test will fail.
        Assert.assertFalse(entity.isRepeatable());
        post.setEntity(entity);
        doHttpClientRequest(httpClient, post);
      }
    } finally {
      stop();
    }
  }
}
