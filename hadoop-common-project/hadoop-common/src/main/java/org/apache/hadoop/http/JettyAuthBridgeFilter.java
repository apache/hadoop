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
package org.apache.hadoop.http;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

/**
 * Reads {@link HttpServletRequest#getRemoteUser()} on the request flowing
 * through the filter chain and forwards the user to
 * {@link HadoopJettyAuthentication#attach}. This pushes the user onto the
 * base Jetty Request so that Jetty observers running outside the filter
 * chain — {@code RequestLogHandler}, {@code StatisticsHandler}, async
 * dispatch, JMX — can resolve it via the standard servlet / Jetty API
 * instead of seeing {@code NOT_CHECKED}.
 *
 * <p>The bridge does not need to know about Hadoop authentication
 * mechanisms. Both modes route the user identifier into
 * {@code getRemoteUser()} via a request wrap before this filter runs:
 * <ul>
 *   <li>Secure mode: {@code AuthenticationFilter} validates the Kerberos
 *       ticket / delegation token and wraps with the authenticated
 *       principal.</li>
 *   <li>Insecure mode: {@code PseudoAuthenticationHandler} reads the
 *       {@code user.name} query parameter, and {@code AuthenticationFilter}
 *       wraps with that user.</li>
 * </ul>
 *
 * <p>When the request also carries a {@code doAs} query parameter
 * different from the resolved user, the attached label uses the
 * Kerberos-principal-style format {@code "<effective>/<real>"} — for
 * example, {@code "alice/oozie"}. The {@code /} separator keeps the
 * resulting {@code %u} token a single field for NCSA-style log parsers.
 *
 * <p>{@code doAs} authorization itself is performed later by the relevant
 * servlet code; unauthorized attempts are still logged with the
 * slash-label alongside the resulting 403, which is useful for audit.
 *
 * <p>{@link HttpServer2} installs this filter automatically <em>after</em>
 * the user-configured authentication filters, so the wrap from upstream
 * auth filters is visible by the time this one runs.
 */
public class JettyAuthBridgeFilter implements Filter {

  /** Canonical Hadoop impersonation parameter; matched case-insensitively. */
  private static final String DO_AS_PARAM = "doAs";

  @Override
  public void init(FilterConfig filterConfig) {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    if (request instanceof HttpServletRequest) {
      HttpServletRequest http = (HttpServletRequest) request;
      String realUser = http.getRemoteUser();
      if (realUser != null) {
        String doAs = readDoAs(http);
        String label = (doAs != null && !doAs.equals(realUser))
            ? doAs + "/" + realUser
            : realUser;
        HadoopJettyAuthentication.attach(request, label);
      }
    }
    chain.doFilter(request, response);
  }

  /** Case-insensitive {@code doAs} parameter lookup. */
  private static String readDoAs(HttpServletRequest request) {
    Enumeration<String> names = request.getParameterNames();
    while (names.hasMoreElements()) {
      String name = names.nextElement();
      if (DO_AS_PARAM.equalsIgnoreCase(name)) {
        String value = request.getParameter(name);
        if (value != null && !value.isEmpty()) {
          return value;
        }
      }
    }
    return null;
  }

  @Override
  public void destroy() {
  }
}
