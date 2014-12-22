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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that resolves the requester hostname.
 */
@InterfaceAudience.Private
public class HostnameFilter implements Filter {
  static final ThreadLocal<String> HOSTNAME_TL = new ThreadLocal<String>();
  private static final Logger log = LoggerFactory.getLogger(HostnameFilter.class);

  /**
   * Initializes the filter.
   * <p>
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
   * Resolves the requester hostname and delegates the request to the chain.
   * <p>
   * The requester hostname is available via the {@link #get} method.
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
      String hostname;
      try {
        String address = request.getRemoteAddr();
        if (address != null) {
          hostname = InetAddress.getByName(address).getCanonicalHostName();
        } else {
          log.warn("Request remote address is NULL");
          hostname = "???";
        }
      } catch (UnknownHostException ex) {
        log.warn("Request remote address could not be resolved, {0}", ex.toString(), ex);
        hostname = "???";
      }
      HOSTNAME_TL.set(hostname);
      chain.doFilter(request, response);
    } finally {
      HOSTNAME_TL.remove();
    }
  }

  /**
   * Returns the requester hostname.
   *
   * @return the requester hostname.
   */
  public static String get() {
    return HOSTNAME_TL.get();
  }

  /**
   * Destroys the filter.
   * <p>
   * This implementation is a NOP.
   */
  @Override
  public void destroy() {
  }
}
