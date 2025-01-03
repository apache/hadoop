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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import com.google.inject.Injector;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.http.NameValuePair;

@Singleton
public class NMWebAppFilter implements Filter {

  private Injector injector;
  private Context nmContext;

  private static final long serialVersionUID = 1L;

  @Inject
  public NMWebAppFilter(Injector injector, Context nmContext) {
    this.injector = injector;
    this.nmContext = nmContext;
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
      FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;
    String redirectPath = containerLogPageRedirectPath(request);
    if (redirectPath != null) {
      String redirectMsg = "Redirecting to log server" + " : " + redirectPath;
      PrintWriter out = response.getWriter();
      out.println(redirectMsg);
      response.setHeader("Location", redirectPath);
      response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
      return;
    }
    filterChain.doFilter(request, response);
  }

  private String containerLogPageRedirectPath(HttpServletRequest request) {
    String uri = HtmlQuoting.quoteHtmlChars(request.getRequestURI());
    String redirectPath = null;
    if (!uri.contains("/ws/v1/node") && uri.contains("/containerlogs")) {
      String[] parts = uri.split("/");
      String containerIdStr = parts[3];
      String appOwner = parts[4];
      String logType = null;
      if (parts.length > 5) {
        logType = parts[5];
      }
      if (containerIdStr != null && !containerIdStr.isEmpty()) {
        ContainerId containerId;
        try {
          containerId = ContainerId.fromString(containerIdStr);
        } catch (IllegalArgumentException ex) {
          return redirectPath;
        }
        ApplicationId appId =
            containerId.getApplicationAttemptId().getApplicationId();
        Application app = nmContext.getApplications().get(appId);

        boolean fetchAggregatedLog = false;
        List<NameValuePair> params = WebAppUtils.getURLEncodedQueryParam(request);
        if (params != null) {
          for (NameValuePair param : params) {
            if (param.getName().equals(ContainerLogsPage
                .LOG_AGGREGATION_TYPE)) {
              if (param.getValue().equals(ContainerLogsPage
                  .LOG_AGGREGATION_REMOTE_TYPE)) {
                fetchAggregatedLog = true;
              }
            }
          }
        }

        Configuration nmConf = nmContext.getLocalDirsHandler().getConfig();
        if ((app == null || fetchAggregatedLog)
            && nmConf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
              YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
          String logServerUrl =
              nmConf.get(YarnConfiguration.YARN_LOG_SERVER_URL);
          if (logServerUrl != null && !logServerUrl.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(logServerUrl);
            sb.append("/");
            sb.append(nmContext.getNodeId().toString());
            sb.append("/");
            sb.append(containerIdStr);
            sb.append("/");
            sb.append(containerIdStr);
            sb.append("/");
            sb.append(appOwner);
            if (logType != null && !logType.isEmpty()) {
              sb.append("/");
              sb.append(logType);
            }
            redirectPath =
                WebAppUtils.appendQueryParams(request, sb.toString());
          } else {
            injector.getInstance(RequestContext.class).set(
              ContainerLogsPage.REDIRECT_URL, "false");
          }
        }
      }
    }
    return redirectPath;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void destroy() {
  }
}
