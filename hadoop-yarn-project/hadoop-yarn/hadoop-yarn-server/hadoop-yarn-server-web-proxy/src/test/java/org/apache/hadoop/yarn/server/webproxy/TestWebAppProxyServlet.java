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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  @BeforeClass
  public static void start() throws Exception {
    server = new Server(0);
    ((QueuedThreadPool)server.getThreadPool()).setMaxThreads(10);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/foo");
    server.setHandler(context);
    context.addServlet(new ServletHolder(TestServlet.class), "/bar");
    ((ServerConnector)server.getConnectors()[0]).setHost("localhost");
    server.start();
    originalPort = ((ServerConnector)server.getConnectors()[0]).getLocalPort();
    LOG.info("Running embedded servlet container at: http://localhost:"
        + originalPort);
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

  @Test(timeout=5000)
  public void testWebAppProxyServlet() throws Exception {
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
      assertEquals("The proxy returned an unexpected status code rather than"
          + "redirecting the connection (302)",
          HttpURLConnection.HTTP_MOVED_TEMP, proxyConn.getResponseCode());

      String expected =
          WebAppUtils.getResolvedRMWebAppURLWithScheme(configuration)
            + "/cluster/failure/application_00_0";
      String redirect = proxyConn.getHeaderField(ProxyUtils.LOCATION);

      assertEquals("The proxy did not redirect the connection to the failure "
          + "page of the RM", expected, redirect);

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

  @Test(timeout=5000)
  public void testAppReportForEmptyTrackingUrl() throws Exception {
    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9090");
    // overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS
    configuration.setInt("hadoop.http.max.threads", 10);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(configuration);
    proxy.start();

    int proxyPort = proxy.proxy.proxyServer.getConnectorAddress(0).getPort();
    AppReportFetcherForTest appReportFetcher = proxy.proxy.appReportFetcher;

    try {
    //set AHS_ENBALED = false to simulate getting the app report from RM
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
    assertTrue("Webapp proxy servlet should have redirected to RM",
        proxyConn.getURL().toString().equals(appAddressInRm));

    //set AHS_ENBALED = true to simulate getting the app report from AHS
    configuration.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        true);
    proxyConn = (HttpURLConnection) url.openConnection();
    proxyConn.connect();
    try {
      proxyConn.getResponseCode();
    } catch (ConnectException e) {
      // Connection Exception is expected as we have set
      // appReportFetcher.answer = 6, which does not set anything for
      // original tracking url field in the app report.
    }
    String appAddressInAhs = WebAppUtils.getHttpSchemePrefix(configuration) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(configuration) +
        "/applicationhistory" + "/app/" + app.toString();
    assertTrue("Webapp proxy servlet should have redirected to AHS",
        proxyConn.getURL().toString().equals(appAddressInAhs));
    }
    finally {
      proxy.close();
    }
  }

  @Test(timeout=5000)
  public void testWebAppProxyPassThroughHeaders() throws Exception {
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
      assertEquals(proxyConn.getRequestProperties().size(), 4);
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      // Verify if number of headers received by end server is 8.
      // Eight headers include Accept, Host, Connection, User-Agent, Cookie,
      // Origin, Access-Control-Request-Method and
      // Access-Control-Request-Headers. Pls note that Unknown-Header is dropped
      // by proxy as it is not in the list of allowed headers.
      assertEquals(numberOfHeaders, 8);
      assertFalse(hasUnknownHeader);
    } finally {
      proxy.close();
    }
  }


  /**
   * Test main method of WebAppProxyServer
   */
  @Test(timeout=5000)
  public void testWebAppProxyServerMainMethod() throws Exception {
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

  @AfterClass
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
      proxyServer.setAttribute(FETCHER_ATTRIBUTE,
          appReportFetcher );
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

  private class AppReportFetcherForTest extends AppReportFetcher {
    int answer = 0;

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
      }
      return null;
    }

    /*
     * If this method is called with isTrackingUrl=false, no tracking url
     * will set in the app report. Hence, there will be a connection exception
     * when the prxyCon tries to connect.
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
  }
}
