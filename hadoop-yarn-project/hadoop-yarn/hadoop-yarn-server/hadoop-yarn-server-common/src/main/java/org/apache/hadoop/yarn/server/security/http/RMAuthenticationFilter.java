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

package org.apache.hadoop.yarn.server.security.http;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;

@Private
@Unstable
public class RMAuthenticationFilter extends
    DelegationTokenAuthenticationFilter {

  static private AbstractDelegationTokenSecretManager<?> manager;
  private static final String OLD_HEADER = "Hadoop-YARN-Auth-Delegation-Token";

  public RMAuthenticationFilter() {
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    filterConfig.getServletContext().setAttribute(
      DelegationTokenAuthenticationFilter.DELEGATION_TOKEN_SECRET_MANAGER_ATTR,
      manager);
    super.init(filterConfig);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    String newHeader =
        req.getHeader(DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER);
    if (newHeader == null || newHeader.isEmpty()) {
      // For backward compatibility, allow use of the old header field
      // only when the new header doesn't exist
      final String oldHeader = req.getHeader(OLD_HEADER);
      if (oldHeader != null && !oldHeader.isEmpty()) {
        request = new HttpServletRequestWrapper(req) {
          @Override
          public String getHeader(String name) {
            if (name
                .equals(DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER)) {
              return oldHeader;
            }
            return super.getHeader(name);
          }
        };
      }
    }
    super.doFilter(request, response, filterChain);
  }

  public static void setDelegationTokenSecretManager(
      AbstractDelegationTokenSecretManager<?> manager) {
    RMAuthenticationFilter.manager = manager;
  }
}
