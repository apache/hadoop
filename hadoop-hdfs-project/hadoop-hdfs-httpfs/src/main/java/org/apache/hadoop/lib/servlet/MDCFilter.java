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

package org.apache.hadoop.lib.servlet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.MDC;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Principal;

/**
 * Filter that sets request contextual information for the slf4j MDC.
 * <p/>
 * It sets the following values:
 * <ul>
 * <li>hostname: if the {@link HostnameFilter} is present and configured
 * before this filter</li>
 * <li>user: the <code>HttpServletRequest.getUserPrincipal().getName()</code></li>
 * <li>method: the HTTP method fo the request (GET, POST, ...)</li>
 * <li>path: the path of the request URL</li>
 * </ul>
 */
@InterfaceAudience.Private
public class MDCFilter implements Filter {

  /**
   * Initializes the filter.
   * <p/>
   * This implementation is a NOP.
   *
   * @param config filter configuration.
   *
   * @throws ServletException thrown if the filter could not be initialized.
   */
  @Override
  public void init(FilterConfig config) throws ServletException {
  }

  /**
   * Sets the slf4j <code>MDC</code> and delegates the request to the chain.
   *
   * @param request servlet request.
   * @param response servlet response.
   * @param chain filter chain.
   *
   * @throws IOException thrown if an IO error occurrs.
   * @throws ServletException thrown if a servet error occurrs.
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
    throws IOException, ServletException {
    try {
      MDC.clear();
      String hostname = HostnameFilter.get();
      if (hostname != null) {
        MDC.put("hostname", HostnameFilter.get());
      }
      Principal principal = ((HttpServletRequest) request).getUserPrincipal();
      String user = (principal != null) ? principal.getName() : null;
      if (user != null) {
        MDC.put("user", user);
      }
      MDC.put("method", ((HttpServletRequest) request).getMethod());
      MDC.put("path", ((HttpServletRequest) request).getPathInfo());
      chain.doFilter(request, response);
    } finally {
      MDC.clear();
    }
  }

  /**
   * Destroys the filter.
   * <p/>
   * This implementation is a NOP.
   */
  @Override
  public void destroy() {
  }
}

