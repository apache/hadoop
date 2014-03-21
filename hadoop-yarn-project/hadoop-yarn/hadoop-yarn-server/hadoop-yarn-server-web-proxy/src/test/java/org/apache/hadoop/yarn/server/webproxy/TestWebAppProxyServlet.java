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
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/**
 * Test the WebAppProxyServlet and WebAppProxy. For back end use simple web
 * server.
 */
public class TestWebAppProxyServlet {

  private static final Log LOG = LogFactory
      .getLog(TestWebAppProxyServlet.class);

  private static Server server;
  private static int originalPort = 0;

  /**
   * Simple http server. Server should send answer with status 200
   */
  @BeforeClass
  public static void start() throws Exception {
    server = new Server(0);
    Context context = new Context();
    context.setContextPath("/foo");
    server.setHandler(context);
    context.addServlet(new ServletHolder(TestServlet.class), "/bar/");
    server.getConnectors()[0].setHost("localhost");
    server.start();
    originalPort = server.getConnectors()[0].getLocalPort();
    LOG.info("Running embedded servlet container at: http://localhost:"
        + originalPort);
  }

  @SuppressWarnings("serial")
  public static class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
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

    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9090");
    // overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS 
    configuration.setInt("hadoop.http.max.threads", 5);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(configuration);
    proxy.start();
    
    int proxyPort = proxy.proxy.proxyServer.getConnectorAddress(0).getPort();
    AppReportFetcherForTest appReportFetcher = proxy.proxy.appReportFetcher;

    // wrong url
    try {
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
    public synchronized void init(Configuration conf) {
      Configuration config = new YarnConfiguration(conf);
      proxy = new WebAppProxyForTest();
      addService(proxy);
      super.init(config);
    }

  }

  private class WebAppProxyForTest extends WebAppProxy {
    
    HttpServer2 proxyServer;
    AppReportFetcherForTest appReportFetcher;
    
    @Override
    public void start() {
      try {
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
        System.out.println("Proxy server is started at port " + 
            proxyServer.getConnectorAddress(0).getPort());
      } catch (Exception e) {
        LOG.fatal("Could not start proxy web server", e);
        throw new YarnRuntimeException("Could not start proxy web server", e);
      }
    }

  }

  private class AppReportFetcherForTest extends AppReportFetcher {
    
    int answer = 0;
    
    public AppReportFetcherForTest(Configuration conf) {
      super(conf);
    }

    public ApplicationReport getApplicationReport(ApplicationId appId)
        throws YarnException {
      if (answer == 0) {
        return getDefaultApplicationReport(appId);
      } else if (answer == 1) {
        return null;
      } else if (answer == 2) {
        ApplicationReport result = getDefaultApplicationReport(appId);
        result.setUser("user");
        return result;
      } else if (answer == 3) {
        ApplicationReport result =  getDefaultApplicationReport(appId);
        result.setYarnApplicationState(YarnApplicationState.KILLED);
        return result;
      } else if (answer == 4) {
        throw new ApplicationNotFoundException("Application is not found");
      }
      return null;
    }

    private ApplicationReport getDefaultApplicationReport(ApplicationId appId) {
      ApplicationReport result = new ApplicationReportPBImpl();
      result.setApplicationId(appId);
      result.setOriginalTrackingUrl("localhost:" + originalPort + "/foo/bar");
      result.setYarnApplicationState(YarnApplicationState.RUNNING);
      result.setUser(CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER);
      return result;
    }
    
  }
}
