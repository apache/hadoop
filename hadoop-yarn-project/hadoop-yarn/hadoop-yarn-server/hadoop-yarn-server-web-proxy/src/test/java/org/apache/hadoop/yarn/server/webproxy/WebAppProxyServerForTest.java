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

import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Map;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebAppProxyServerForTest extends CompositeService {

  private static final Logger LOG = LoggerFactory.getLogger(WebAppProxyServerForTest.class);

  private WebAppProxyForTest proxy = null;
  private final String trackingUrlProtocol;
  private final int originalPort;

  public WebAppProxyServerForTest(int originalPort, String trackingUrlProtocol) {
    super(WebAppProxyServer.class.getName());
    this.originalPort = originalPort;
    if (trackingUrlProtocol == null || trackingUrlProtocol.isEmpty()) {
      this.trackingUrlProtocol = "";
    } else {
      this.trackingUrlProtocol = trackingUrlProtocol + "://";
    }
  }

  public WebAppProxyServerForTest(int originalPort) {
    this(originalPort, "");
  }

  public static boolean isResponseCookiePresent(HttpURLConnection proxyConn,
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

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    proxy = new WebAppProxyForTest();
    addService(proxy);
    super.serviceInit(conf);
  }

  public int getProxyPort() {
    return proxy.proxyServer.getConnectorAddress(0).getPort();
  }

  public AppReportFetcherForTest getAppReportFetcher() {
    return proxy.appReportFetcher;
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

  public class AppReportFetcherForTest extends AppReportFetcher {

    private int answer = 0;

    public AppReportFetcherForTest(Configuration conf) {
      super(conf);
    }

    public void setAnswer(int answer) {
      this.answer = answer;
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
        result.setOriginalTrackingUrl(trackingUrlProtocol + "localhost:" + originalPort + "/foo/bar");
      }
      if(getConfig().getBoolean(YarnConfiguration.
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
