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

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import com.google.inject.Injector;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

@Singleton
public class NMWebAppFilter extends GuiceContainer{

  private Injector injector;
  private Context nmContext;

  private static final long serialVersionUID = 1L;

  @Inject
  public NMWebAppFilter(Injector injector, Context nmContext) {
    super(injector);
    this.injector = injector;
    this.nmContext = nmContext;
  }

  @Override
  public void doFilter(HttpServletRequest request,
      HttpServletResponse response, FilterChain chain) throws IOException,
      ServletException {
    String redirectPath = containerLogPageRedirectPath(request);
    if (redirectPath != null) {
      String redirectMsg =
          "Redirecting to log server" + " : " + redirectPath;
      PrintWriter out = response.getWriter();
      out.println(redirectMsg);
      response.setHeader("Location", redirectPath);
      response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
      return;
    }
    super.doFilter(request, response, chain);
  }

  private String containerLogPageRedirectPath(HttpServletRequest request) {
    String uri = HtmlQuoting.quoteHtmlChars(request.getRequestURI());
    String redirectPath = null;
    if (!uri.contains("/ws/v1/node") && uri.contains("/containerlogs")) {
      String[] parts = uri.split("/");
      String containerIdStr = parts[3];
      String appOwner = parts[4];
      if (containerIdStr != null && !containerIdStr.isEmpty()) {
        ContainerId containerId = null;
        try {
          containerId = ContainerId.fromString(containerIdStr);
        } catch (IllegalArgumentException ex) {
          return redirectPath;
        }
        ApplicationId appId =
            containerId.getApplicationAttemptId().getApplicationId();
        Application app = nmContext.getApplications().get(appId);
        Configuration nmConf = nmContext.getLocalDirsHandler().getConfig();
        if (app == null
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
}
