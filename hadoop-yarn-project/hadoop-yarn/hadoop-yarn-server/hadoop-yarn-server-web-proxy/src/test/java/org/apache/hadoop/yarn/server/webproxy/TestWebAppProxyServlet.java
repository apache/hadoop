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

package org.apache.hadoop.yarn.server.webproxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the WebAppProxyServlet and WebAppProxy. For back end use simple web
 * server.
 */
public class TestWebAppProxyServlet {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestWebAppProxyServlet.class);

  private static Server server;
  private static int originalPort = 0;
  private static int numberOfHeaders = 0;
  private static final String UNKNOWN_HEADER = "Unknown-Header";
  private static boolean hasUnknownHeader = false;
  Configuration configuration = new Configuration();


  /**
   * Simple http server. Server should send answer with status 200
   */
  @BeforeAll
  public static void start() throws Exception {
    server = new Server(0);
    ((QueuedThreadPool)server.getThreadPool()).setMaxThreads(20);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/foo");
    server.setHandler(context);
    context.addServlet(new ServletHolder(TestServlet.class), "/bar");
    context.addServlet(new ServletHolder(TimeOutTestServlet.class), "/timeout");
    ((ServerConnector)server.getConnectors()[0]).setHost("localhost");
    server.start();
    originalPort = ((ServerConnector)server.getConnectors()[0]).getLocalPort();
    LOG.info("Running embedded servlet container at: http://localhost:{}", originalPort);
    // This property needs to be set otherwise CORS Headers will be dropped
    // by HttpUrlConnection
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
  }

  @SuppressWarnings("serial")
  public static class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      int numHeaders = 0;
      hasUnknownHeader = false;
      @SuppressWarnings("unchecked")
      Enumeration<String> names = req.getHeaderNames();
      while(names.hasMoreElements()) {
        String headerName = names.nextElement();
        if (headerName.equals(UNKNOWN_HEADER)) {
          hasUnknownHeader = true;
        }
        ++numHeaders;
      }
      numberOfHeaders = numHeaders;
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
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

  @SuppressWarnings("serial")
  public static class TimeOutTestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        Thread.sleep(10 * 1000);
      } catch (InterruptedException e) {
        LOG.warn("doGet() interrupted", e);
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  @Test
  @Timeout(5000)
  void testWebAppProxyServlet() throws Exception {
    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9090");
    // overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS
    configuration.setInt("hadoop.http.max.threads", 10);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(configuration);
    proxy.start();

    int proxyPort = proxy.proxy.proxyServer.getConnectorAddress(0).getPort();
    AppReportFetcherForTest appReportFetcher = proxy.proxy.appReportFetcher;

    // wrong url
    try {
      // wrong url without app ID
      URL emptyUrl = new URL("http://localhost:" + proxyPort + "/proxy");
      HttpURLConnection emptyProxyConn = (HttpURLConnection) emptyUrl
          .openConnection();
      emptyProxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND, emptyProxyConn.getResponseCode());

      // wrong url. Set wrong app ID
      URL wrongUrl = new URL("http://localhost:" + proxyPort + "/proxy/app");
      HttpURLConnection proxyConn = (HttpURLConnection) wrongUrl
          .openConnection();

      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR,
          proxyConn.getResponseCode());

      // set true Application ID in url
      URL url = new URL("http://localhost:" + proxyPort + "/proxy/application_00_0");
      proxyConn = (HttpURLConnection) url.openConnection();
      // set cookie
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      assertTrue(isResponseCookiePresent(
          proxyConn, "checked_application_0_0000", "true"));

      // test that redirection is squashed correctly
      URL redirectUrl = new URL("http://localhost:" + proxyPort
          + "/proxy/redirect/application_00_0");
      proxyConn = (HttpURLConnection) redirectUrl.openConnection();
      proxyConn.setInstanceFollowRedirects(false);
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, proxyConn.getResponseCode(),
          "The proxy returned an unexpected status code rather than"
              + "redirecting the connection (302)");

      String expected =
          WebAppUtils.getResolvedRMWebAppURLWithScheme(configuration)
              + "/cluster/failure/application_00_0";
      String redirect = proxyConn.getHeaderField(ProxyUtils.LOCATION);

      assertEquals(expected, redirect, "The proxy did not redirect the connection to the failure "
          + "page of the RM");

      // cannot found application 1: null
      appReportFetcher.answer = 1;
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          proxyConn.getResponseCode());
      assertFalse(isResponseCookiePresent(
          proxyConn, "checked_application_0_0000", "true"));

      // cannot found application 2: ApplicationNotFoundException
      appReportFetcher.answer = 4;
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          proxyConn.getResponseCode());
      assertFalse(isResponseCookiePresent(
          proxyConn, "checked_application_0_0000", "true"));

      // wrong user
      appReportFetcher.answer = 2;
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      String s = readInputStream(proxyConn.getInputStream());
      assertTrue(s
          .contains("to continue to an Application Master web interface owned by"));
      assertTrue(s.contains("WARNING: The following page may not be safe!"));

      //case if task has a not running status
      appReportFetcher.answer = 3;
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());

      // test user-provided path and query parameter can be appended to the
      // original tracking url
      appReportFetcher.answer = 5;
      URL clientUrl = new URL("http://localhost:" + proxyPort
          + "/proxy/application_00_0/test/tez?x=y&h=p");
      proxyConn = (HttpURLConnection) clientUrl.openConnection();
      proxyConn.connect();
      LOG.info("" + proxyConn.getURL());
      LOG.info("ProxyConn.getHeaderField(): " +  proxyConn.getHeaderField(ProxyUtils.LOCATION));
      assertEquals("http://localhost:" + originalPort
          + "/foo/bar/test/tez?a=b&x=y&h=p#main", proxyConn.getURL().toString());
    } finally {
      proxy.close();
    }
  }

  @Test
  void testWebAppProxyConnectionTimeout()
      throws IOException, ServletException {
    assertThrows(SocketTimeoutException.class, () -> {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getMethod()).thenReturn("GET");
      when(request.getRemoteUser()).thenReturn("dr.who");
      when(request.getPathInfo()).thenReturn("/application_00_0");
      when(request.getHeaderNames()).thenReturn(Collections.emptyEnumeration());

      HttpServletResponse response = mock(HttpServletResponse.class);
      when(response.getOutputStream()).thenReturn(null);

      WebAppProxyServlet servlet = new WebAppProxyServlet();
      YarnConfiguration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.RM_PROXY_TIMEOUT_ENABLED,
          true);
      conf.setInt(YarnConfiguration.RM_PROXY_CONNECTION_TIMEOUT,
          1000);

      servlet.setConf(conf);

      ServletConfig config = mock(ServletConfig.class);
      ServletContext context = mock(ServletContext.class);
      when(config.getServletContext()).thenReturn(context);

      AppReportFetcherForTest appReportFetcher =
          new AppReportFetcherForTest(new YarnConfiguration());

      when(config.getServletContext()
          .getAttribute(WebAppProxy.FETCHER_ATTRIBUTE))
          .thenReturn(appReportFetcher);

      appReportFetcher.answer = 7;

      servlet.init(config);
      servlet.doGet(request, response);

    });

  }

  @Test
  @Timeout(5000)
  void testAppReportForEmptyTrackingUrl() throws Exception {
    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9090");
    // overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS
    configuration.setInt("hadoop.http.max.threads", 10);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(configuration);
    proxy.start();

    int proxyPort = proxy.proxy.proxyServer.getConnectorAddress(0).getPort();
    AppReportFetcherForTest appReportFetcher = proxy.proxy.appReportFetcher;

    try {
      //set AHS_ENABLED = false to simulate getting the app report from RM
      configuration.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
          false);
      ApplicationId app = ApplicationId.newInstance(0, 0);
      appReportFetcher.answer = 6;
      URL url = new URL("http://localhost:" + proxyPort +
          "/proxy/" + app.toString());
      HttpURLConnection proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.connect();
      try {
        proxyConn.getResponseCode();
      } catch (ConnectException e) {
        // Connection Exception is expected as we have set
        // appReportFetcher.answer = 6, which does not set anything for
        // original tracking url field in the app report.
      }
      String appAddressInRm =
          WebAppUtils.getResolvedRMWebAppURLWithScheme(configuration) +
              "/cluster" + "/app/" + app.toString();
      assertEquals(proxyConn.getURL().toString(), appAddressInRm,
          "Webapp proxy servlet should have redirected to RM");

      //set AHS_ENABLED = true to simulate getting the app report from AHS
      configuration.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
          true);
      proxy.proxy.appReportFetcher.setAhsAppPageUrlBase(configuration);
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.connect();
      try {
        proxyConn.getResponseCode();
      } catch (ConnectException e) {
        // Connection Exception is expected as we have set
        // appReportFetcher.answer = 6, which does not set anything for
        // original tracking url field in the app report.
      }
      String appAddressInAhs =
          WebAppUtils.getHttpSchemePrefix(configuration) + WebAppUtils.getAHSWebAppURLWithoutScheme(
              configuration) + "/applicationhistory" + "/app/" + app.toString();
      assertEquals(proxyConn.getURL().toString(), appAddressInAhs,
          "Webapp proxy servlet should have redirected to AHS");
    } finally {
      proxy.close();
    }
  }

  @Test
  @Timeout(5000)
  void testWebAppProxyPassThroughHeaders() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9091");
    configuration.setInt("hadoop.http.max.threads", 10);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(configuration);
    proxy.start();

    int proxyPort = proxy.proxy.proxyServer.getConnectorAddress(0).getPort();

    try {
      URL url = new URL("http://localhost:" + proxyPort + "/proxy/application_00_1");
      HttpURLConnection proxyConn = (HttpURLConnection) url.openConnection();
      // set headers
      proxyConn.addRequestProperty("Origin", "http://www.someurl.com");
      proxyConn.addRequestProperty("Access-Control-Request-Method", "GET");
      proxyConn.addRequestProperty(
          "Access-Control-Request-Headers", "Authorization");
      proxyConn.addRequestProperty(UNKNOWN_HEADER, "unknown");
      // Verify if four headers mentioned above have been added
      assertThat(proxyConn.getRequestProperties()).hasSize(4);
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      // Verify if number of headers received by end server is 9.
      // This should match WebAppProxyServlet#PASS_THROUGH_HEADERS.
      // Nine headers include Accept, Host, Connection, User-Agent, Cookie,
      // Origin, Access-Control-Request-Method, Accept-Encoding, and
      // Access-Control-Request-Headers. Pls note that Unknown-Header is dropped
      // by proxy as it is not in the list of allowed headers.
      assertEquals(numberOfHeaders, 9);
      assertFalse(hasUnknownHeader);
    } finally {
      proxy.close();
    }
  }


  /**
   * Test main method of WebAppProxyServer
   */
  @Test
  @Timeout(5000)
  void testWebAppProxyServerMainMethod() throws Exception {
    WebAppProxyServer mainServer = null;
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9099");
    try {
      mainServer  = WebAppProxyServer.startServer(conf);
      int counter = 20;

      URL wrongUrl = new URL("http://localhost:9099/proxy/app");
      HttpURLConnection proxyConn = null;
      while (counter > 0) {
        counter--;
        try {
          proxyConn = (HttpURLConnection) wrongUrl.openConnection();
          proxyConn.connect();
          proxyConn.getResponseCode();
          // server started ok
          counter = 0;
        } catch (Exception e) {
          Thread.sleep(100);
        }
      }
      assertNotNull(proxyConn);
      // wrong application Id
      assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR,
          proxyConn.getResponseCode());
    } finally {
      if (mainServer != null) {
        mainServer.stop();
      }
    }
  }

  @Test
  @Timeout(5000)
  void testCheckHttpsStrictAndNotProvided() throws Exception {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    StringWriter sw = new StringWriter();
    when(resp.getWriter()).thenReturn(new PrintWriter(sw));
    YarnConfiguration conf = new YarnConfiguration();
    final URI httpLink = new URI("http://foo.com");
    final URI httpsLink = new URI("https://foo.com");

    // NONE policy
    conf.set(YarnConfiguration.RM_APPLICATION_HTTPS_POLICY, "NONE");
    assertFalse(WebAppProxyServlet.checkHttpsStrictAndNotProvided(
        resp, httpsLink, conf));
    assertEquals("", sw.toString());
    Mockito.verify(resp, Mockito.times(0)).setContentType(Mockito.any());
    assertFalse(WebAppProxyServlet.checkHttpsStrictAndNotProvided(
        resp, httpLink, conf));
    assertEquals("", sw.toString());
    Mockito.verify(resp, Mockito.times(0)).setContentType(Mockito.any());

    // LENIENT policy
    conf.set(YarnConfiguration.RM_APPLICATION_HTTPS_POLICY, "LENIENT");
    assertFalse(WebAppProxyServlet.checkHttpsStrictAndNotProvided(
        resp, httpsLink, conf));
    assertEquals("", sw.toString());
    Mockito.verify(resp, Mockito.times(0)).setContentType(Mockito.any());
    assertFalse(WebAppProxyServlet.checkHttpsStrictAndNotProvided(
        resp, httpLink, conf));
    assertEquals("", sw.toString());
    Mockito.verify(resp, Mockito.times(0)).setContentType(Mockito.any());

    // STRICT policy
    conf.set(YarnConfiguration.RM_APPLICATION_HTTPS_POLICY, "STRICT");
    assertFalse(WebAppProxyServlet.checkHttpsStrictAndNotProvided(
        resp, httpsLink, conf));
    assertEquals("", sw.toString());
    Mockito.verify(resp, Mockito.times(0)).setContentType(Mockito.any());
    assertTrue(WebAppProxyServlet.checkHttpsStrictAndNotProvided(
        resp, httpLink, conf));
    String s = sw.toString();
    assertTrue(s.contains("HTTPS must be used"),
        "Was expecting an HTML page explaining that an HTTPS tracking"
            + " url must be used but found " + s);
    Mockito.verify(resp, Mockito.times(1)).setContentType(MimeType.HTML);
  }

  private String readInputStream(InputStream input) throws Exception {
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    byte[] buffer = new byte[512];
    int read;
    while ((read = input.read(buffer)) >= 0) {
      data.write(buffer, 0, read);
    }
    return new String(data.toByteArray(), "UTF-8");
  }

  private boolean isResponseCookiePresent(HttpURLConnection proxyConn,
      String expectedName, String expectedValue) {
    Map<String, List<String>> headerFields = proxyConn.getHeaderFields();
    List<String> cookiesHeader = headerFields.get("Set-Cookie");
    if (cookiesHeader != null) {
      for (String cookie : cookiesHeader) {
        HttpCookie c = HttpCookie.parse(cookie).get(0);
        if (c.getName().equals(expectedName)
            && c.getValue().equals(expectedValue)) {
          return true;
        }
      }
    }
    return false;
  }

  @AfterAll
  public static void stop() throws Exception {
    try {
      server.stop();
    } catch (Exception e) {
    }

    try {
      server.destroy();
    } catch (Exception e) {
    }
  }

  private class WebAppProxyServerForTest extends CompositeService {

    private WebAppProxyForTest proxy = null;

    public WebAppProxyServerForTest() {
      super(WebAppProxyServer.class.getName());
    }

    @Override
    public synchronized void serviceInit(Configuration conf) throws Exception {
      proxy = new WebAppProxyForTest();
      addService(proxy);
      super.serviceInit(conf);
    }

  }

  private class WebAppProxyForTest extends WebAppProxy {

    HttpServer2 proxyServer;
    AppReportFetcherForTest appReportFetcher;

    @Override
    protected void serviceStart() throws Exception {
      Configuration conf = getConfig();
      String bindAddress = conf.get(YarnConfiguration.PROXY_ADDRESS);
      bindAddress = StringUtils.split(bindAddress, ':')[0];
      AccessControlList acl = new AccessControlList(
          conf.get(YarnConfiguration.YARN_ADMIN_ACL,
          YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
      proxyServer = new HttpServer2.Builder()
          .setName("proxy")
          .addEndpoint(
              URI.create(WebAppUtils.getHttpSchemePrefix(conf) + bindAddress
                  + ":0")).setFindPort(true)
          .setConf(conf)
          .setACL(acl)
          .build();
      proxyServer.addServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);

      appReportFetcher = new AppReportFetcherForTest(conf);
      proxyServer.setAttribute(FETCHER_ATTRIBUTE, appReportFetcher);
      proxyServer.setAttribute(IS_SECURITY_ENABLED_ATTRIBUTE, Boolean.TRUE);

      String proxy = WebAppUtils.getProxyHostAndPort(conf);
      String[] proxyParts = proxy.split(":");
      String proxyHost = proxyParts[0];

      proxyServer.setAttribute(PROXY_HOST_ATTRIBUTE, proxyHost);
      proxyServer.start();
      LOG.info("Proxy server is started at port {}",
          proxyServer.getConnectorAddress(0).getPort());
    }

  }

  private class AppReportFetcherForTest extends DefaultAppReportFetcher {
    int answer = 0;
    private String ahsAppPageUrlBase = null;

    public AppReportFetcherForTest(Configuration conf) {
      super(conf);
    }

    public FetchedAppReport getApplicationReport(ApplicationId appId)
        throws YarnException {
      if (answer == 0) {
        return getDefaultApplicationReport(appId);
      } else if (answer == 1) {
        return null;
      } else if (answer == 2) {
        FetchedAppReport result = getDefaultApplicationReport(appId);
        result.getApplicationReport().setUser("user");
        return result;
      } else if (answer == 3) {
        FetchedAppReport result =  getDefaultApplicationReport(appId);
        result.getApplicationReport().
            setYarnApplicationState(YarnApplicationState.KILLED);
        return result;
      } else if (answer == 4) {
        throw new ApplicationNotFoundException("Application is not found");
      } else if (answer == 5) {
        // test user-provided path and query parameter can be appended to the
        // original tracking url
        FetchedAppReport result = getDefaultApplicationReport(appId);
        result.getApplicationReport().setOriginalTrackingUrl("localhost:"
            + originalPort + "/foo/bar?a=b#main");
        result.getApplicationReport().
            setYarnApplicationState(YarnApplicationState.FINISHED);
        return result;
      } else if (answer == 6) {
        return getDefaultApplicationReport(appId, false);
      } else if (answer == 7) {
        // test connection timeout
        FetchedAppReport result = getDefaultApplicationReport(appId);
        result.getApplicationReport().setOriginalTrackingUrl("localhost:"
            + originalPort + "/foo/timeout?a=b#main");
        return result;
      }
      return null;
    }

    /*
     * If this method is called with isTrackingUrl=false, no tracking url
     * will set in the app report. Hence, there will be a connection exception
     * when the proxyCon tries to connect.
     */
    private FetchedAppReport getDefaultApplicationReport(ApplicationId appId,
        boolean isTrackingUrl) {
      FetchedAppReport fetchedReport;
      ApplicationReport result = new ApplicationReportPBImpl();
      result.setApplicationId(appId);
      result.setYarnApplicationState(YarnApplicationState.RUNNING);
      result.setUser(CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER);
      if (isTrackingUrl) {
        result.setOriginalTrackingUrl("localhost:" + originalPort + "/foo/bar");
      }
      if(configuration.getBoolean(YarnConfiguration.
          APPLICATION_HISTORY_ENABLED, false)) {
        fetchedReport = new FetchedAppReport(result, AppReportSource.AHS);
      } else {
        fetchedReport = new FetchedAppReport(result, AppReportSource.RM);
      }
      return fetchedReport;
    }

    private FetchedAppReport getDefaultApplicationReport(ApplicationId appId) {
      return getDefaultApplicationReport(appId, true);
    }

    @VisibleForTesting
    public String getAhsAppPageUrlBase() {
      return ahsAppPageUrlBase != null ? ahsAppPageUrlBase : super.getAhsAppPageUrlBase();
    }

    @VisibleForTesting
    public void setAhsAppPageUrlBase(Configuration conf) {
      this.ahsAppPageUrlBase = StringHelper.pjoin(
          WebAppUtils.getHttpSchemePrefix(conf) + WebAppUtils.getAHSWebAppURLWithoutScheme(conf),
          "applicationhistory", "app");
    }
  }
}
