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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.http.HtmlQuoting;

import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

@Singleton
public class RMWebAppFilter extends GuiceContainer {

  private Injector injector;
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  // define a set of URIs which do not need to do redirection
  private static final Set<String> NON_REDIRECTED_URIS = Sets.newHashSet(
      "/conf", "/stacks", "/logLevel", "/metrics", "/jmx", "/logs");

  @Inject
  public RMWebAppFilter(Injector injector) {
    super(injector);
    this.injector=injector;
  }

  @Override
  public void doFilter(HttpServletRequest request,
      HttpServletResponse response, FilterChain chain) throws IOException,
      ServletException {
    response.setCharacterEncoding("UTF-8");
    String uri = HtmlQuoting.quoteHtmlChars(request.getRequestURI());

    if (uri == null) {
      uri = "/";
    }
    RMWebApp rmWebApp = injector.getInstance(RMWebApp.class);
    rmWebApp.checkIfStandbyRM();
    if (rmWebApp.isStandby()
        && shouldRedirect(rmWebApp, uri)) {
      String redirectPath = rmWebApp.getRedirectPath() + uri;

      if (redirectPath != null && !redirectPath.isEmpty()) {
        String redirectMsg =
            "This is standby RM. Redirecting to the current active RM: "
                + redirectPath;
        response.addHeader("Refresh", "3; url=" + redirectPath);
        PrintWriter out = response.getWriter();
        out.println(redirectMsg);
        return;
      }
    }

    super.doFilter(request, response, chain);

  }

  private boolean shouldRedirect(RMWebApp rmWebApp, String uri) {
    return !uri.equals("/" + rmWebApp.wsName() + "/v1/cluster/info")
        && !uri.equals("/" + rmWebApp.name() + "/cluster")
        && !NON_REDIRECTED_URIS.contains(uri);
  }
}
