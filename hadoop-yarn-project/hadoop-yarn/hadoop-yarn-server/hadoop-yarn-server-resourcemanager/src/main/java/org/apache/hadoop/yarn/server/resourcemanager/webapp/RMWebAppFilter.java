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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.pjoin;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.http.IsActiveServlet;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

@Singleton
public class RMWebAppFilter extends GuiceContainer {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMWebAppFilter.class);

  private Injector injector;
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  // define a set of URIs which do not need to do redirection
  private static final Set<String> NON_REDIRECTED_URIS = Sets.newHashSet(
      "/conf", "/stacks", "/logLevel", "/logs", IsActiveServlet.PATH_SPEC);
  private String path;
  private boolean ahsEnabled;
  private String ahsPageURLPrefix;
  private static final int BASIC_SLEEP_TIME = 5;
  private static final int MAX_SLEEP_TIME = 5 * 60;
  private static final Random randnum = new Random();

  @Inject
  public RMWebAppFilter(Injector injector, Configuration conf) {
    super(injector);
    this.injector=injector;
    InetSocketAddress sock = YarnConfiguration.useHttps(conf)
        ? conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT)
        : conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);

    path = sock.getHostName() + ":" + Integer.toString(sock.getPort());
    path = YarnConfiguration.useHttps(conf)
        ? "https://" + path
        : "http://" + path;
    ahsEnabled = conf.getBoolean(
        YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED);
    ahsPageURLPrefix = pjoin(
        WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(
            conf), "applicationhistory");
  }

  @Override
  public void doFilter(HttpServletRequest request,
      HttpServletResponse response, FilterChain chain) throws IOException,
      ServletException {
    response.setCharacterEncoding("UTF-8");
    String htmlEscapedUri = HtmlQuoting.quoteHtmlChars(request.getRequestURI());

    if (htmlEscapedUri == null) {
      htmlEscapedUri = "/";
    }

    String uriWithQueryString =
        WebAppUtils.appendQueryParams(request, htmlEscapedUri);
    String htmlEscapedUriWithQueryString =
        WebAppUtils.getHtmlEscapedURIWithQueryString(request);

    RMWebApp rmWebApp = injector.getInstance(RMWebApp.class);
    rmWebApp.checkIfStandbyRM();
    if (rmWebApp.isStandby()
        && shouldRedirect(rmWebApp, htmlEscapedUri)) {

      String redirectPath = rmWebApp.getRedirectPath();

      if (redirectPath != null && !redirectPath.isEmpty()) {
        redirectPath += uriWithQueryString;
        String redirectMsg = "This is standby RM. The redirect url is: "
            + htmlEscapedUriWithQueryString;
        PrintWriter out = response.getWriter();
        out.println(redirectMsg);
        response.setHeader("Location", redirectPath);
        response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        return;
      } else {
        boolean doRetry = true;
        String retryIntervalStr =
            request.getParameter(YarnWebParams.NEXT_REFRESH_INTERVAL);
        int retryInterval = 0;
        if (retryIntervalStr != null) {
          try {
            retryInterval = Integer.parseInt(retryIntervalStr.trim());
          } catch (NumberFormatException ex) {
            doRetry = false;
          }
        }
        int next = calculateExponentialTime(retryInterval);

        String redirectUrl =
            appendOrReplaceParamter(path + uriWithQueryString,
              YarnWebParams.NEXT_REFRESH_INTERVAL + "=" + (retryInterval + 1));
        if (redirectUrl == null || next > MAX_SLEEP_TIME) {
          doRetry = false;
        }
        String redirectMsg =
            doRetry ? "Can not find any active RM. Will retry in next " + next
                + " seconds." : "There is no active RM right now.";
        redirectMsg += "\nHA Zookeeper Connection State: "
            + rmWebApp.getHAZookeeperConnectionState();
        PrintWriter out = response.getWriter();
        out.println(redirectMsg);
        if (doRetry) {
          response.setHeader("Refresh", next + ";url=" + redirectUrl);
          response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        }
      }
      return;
    } else if (ahsEnabled) {
      String ahsRedirectUrl = ahsRedirectPath(uriWithQueryString, rmWebApp);
      if(ahsRedirectUrl != null) {
        response.setHeader("Location", ahsRedirectUrl);
        response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        return;
      }
    }

    super.doFilter(request, response, chain);
  }

  private String ahsRedirectPath(String uri, RMWebApp rmWebApp) {
    // TODO: Commonize URL parsing code. Will be done in YARN-4642.
    String redirectPath = null;
    if(uri.contains("/cluster/")) {
      String[] parts = uri.split("/");
      if(parts.length > 3) {
        RMContext context = rmWebApp.getRMContext();
        String type = parts[2];
        ApplicationId appId = null;
        ApplicationAttemptId appAttemptId = null;
        ContainerId containerId = null;
        switch(type){
        case "app":
          try {
            appId = Apps.toAppID(parts[3]);
          } catch (YarnRuntimeException | NumberFormatException e) {
            LOG.debug("Error parsing {} as an ApplicationId",
                parts[3], e);
            return redirectPath;
          }
          if(!context.getRMApps().containsKey(appId)) {
            redirectPath = pjoin(ahsPageURLPrefix, "app", appId);
          }
          break;
        case "appattempt":
          try{
            appAttemptId = ApplicationAttemptId.fromString(parts[3]);
          } catch (IllegalArgumentException e) {
            LOG.debug("Error parsing {} as an ApplicationAttemptId",
                parts[3], e);
            return redirectPath;
          }
          if(!context.getRMApps().containsKey(
              appAttemptId.getApplicationId())) {
            redirectPath = pjoin(ahsPageURLPrefix,
                "appattempt", appAttemptId);
          }
          break;
        case "container":
          try {
            containerId = ContainerId.fromString(parts[3]);
          } catch (IllegalArgumentException e) {
            LOG.debug("Error parsing {} as an ContainerId",
                parts[3], e);
            return redirectPath;
          }
          if(!context.getRMApps().containsKey(
              containerId.getApplicationAttemptId().getApplicationId())) {
            redirectPath = pjoin(ahsPageURLPrefix,
                "container", containerId);
          }
          break;
        default:
          break;
        }
      }
    }
    return redirectPath;
  }

  private boolean shouldRedirect(RMWebApp rmWebApp, String uri) {
    return !uri.equals("/" + rmWebApp.wsName() + "/v1/cluster/info")
        && !uri.equals("/" + rmWebApp.name() + "/cluster")
        && !uri.startsWith(ProxyUriUtils.PROXY_BASE)
        && !NON_REDIRECTED_URIS.contains(uri);
  }

  private String appendOrReplaceParamter(String uri, String newQuery) {
    if (uri.contains(YarnWebParams.NEXT_REFRESH_INTERVAL + "=")) {
      return uri.replaceAll(YarnWebParams.NEXT_REFRESH_INTERVAL + "=[^&]+",
        newQuery);
    }
    try {
      URI oldUri = new URI(uri);
      String appendQuery = oldUri.getQuery();
      if (appendQuery == null) {
        appendQuery = newQuery;
      } else {
        appendQuery += "&" + newQuery;
      }

      URI newUri =
          new URI(oldUri.getScheme(), oldUri.getAuthority(), oldUri.getPath(),
            appendQuery, oldUri.getFragment());

      return newUri.toString();
    } catch (URISyntaxException e) {
      return null;
    }
  }

  private static int calculateExponentialTime(int retries) {
    long baseTime = BASIC_SLEEP_TIME * (1L << retries);
    return (int) (baseTime * (randnum.nextDouble() + 0.5));
  }
}