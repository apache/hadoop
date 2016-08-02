/*
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

package org.apache.slider.server.appmaster.web.rest;

import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter;
import org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpPrincipal;
import org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpServletRequestWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * This is a filter which is used to forward insecure operations
 * There's some metrics to track all operations too
 */
public class InsecureAmFilter extends AmIpFilter {
  public static final String WS_CONTEXT_ROOT = "slider.rest.context.root";
  protected static final Logger log =
      LoggerFactory.getLogger(InsecureAmFilter.class);

  private String wsContextRoot;


  @Override
  public void init(FilterConfig conf) throws ServletException {
    super.init(conf);
    wsContextRoot = conf.getInitParameter(WS_CONTEXT_ROOT);
    if (wsContextRoot == null) {
      throw new ServletException("No value set for " + WS_CONTEXT_ROOT);
    }
  }

  private void rejectNonHttpRequests(ServletRequest req) throws
      ServletException {
    if (!(req instanceof HttpServletRequest)) {
      throw new ServletException("This filter only works for HTTP/HTTPS");
    }
  }  

  @Override
  public void doFilter(ServletRequest req,
      ServletResponse resp,
      FilterChain chain) throws IOException, ServletException {
    rejectNonHttpRequests(req);
    HttpServletRequest httpReq = (HttpServletRequest) req;
    HttpServletResponse httpResp = (HttpServletResponse) resp;


    String requestURI = httpReq.getRequestURI();
    if (requestURI == null || !requestURI.startsWith(wsContextRoot)) {
      // hand off to the AM filter if it is not the context root
      super.doFilter(req, resp, chain);
      return;
    }

    String user = null;

    if (httpReq.getCookies() != null) {
      for (Cookie c : httpReq.getCookies()) {
        if (WebAppProxyServlet.PROXY_USER_COOKIE_NAME.equals(c.getName())) {
          user = c.getValue();
          break;
        }
      }
    }
    
    if (user == null) {
      log.debug("Could not find " + WebAppProxyServlet.PROXY_USER_COOKIE_NAME
               + " cookie, so user will not be set");
      chain.doFilter(req, resp);
    } else {
      final AmIpPrincipal principal = new AmIpPrincipal(user);
      ServletRequest requestWrapper = new AmIpServletRequestWrapper(httpReq,
          principal);
      chain.doFilter(requestWrapper, resp);
    }

  }
}
