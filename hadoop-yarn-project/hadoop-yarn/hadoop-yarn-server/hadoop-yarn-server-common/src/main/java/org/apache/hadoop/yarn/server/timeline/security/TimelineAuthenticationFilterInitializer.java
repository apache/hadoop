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

package org.apache.hadoop.yarn.server.timeline.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticationHandler;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Initializes {@link TimelineAuthenticationFilter} which provides support for
 * Kerberos HTTP SPNEGO authentication.
 * <p>
 * It enables Kerberos HTTP SPNEGO plus delegation token authentication for the
 * timeline server.
 * <p>
 * Refer to the {@code core-default.xml} file, after the comment 'HTTP
 * Authentication' for details on the configuration options. All related
 * configuration properties have {@code hadoop.http.authentication.} as prefix.
 */
public class TimelineAuthenticationFilterInitializer extends FilterInitializer {

  /**
   * The configuration prefix of timeline HTTP authentication.
   */
  public static final String PREFIX =
      "yarn.timeline-service.http-authentication.";

  @VisibleForTesting
  Map<String, String> filterConfig;

  protected void setAuthFilterConfig(Configuration conf) {
    filterConfig = new HashMap<String, String>();

    // setting the cookie path to root '/' so it is used for all resources.
    filterConfig.put(AuthenticationFilter.COOKIE_PATH, "/");

    for (Map.Entry<String, String> entry : conf) {
      String name = entry.getKey();
      if (name.startsWith(ProxyUsers.CONF_HADOOP_PROXYUSER)) {
        String value = conf.get(name);
        name = name.substring("hadoop.".length());
        filterConfig.put(name, value);
      }
    }
    for (Map.Entry<String, String> entry : conf) {
      String name = entry.getKey();
      if (name.startsWith(PREFIX)) {
        // yarn.timeline-service.http-authentication.proxyuser will override
        // hadoop.proxyuser
        String value = conf.get(name);
        name = name.substring(PREFIX.length());
        filterConfig.put(name, value);
      }
    }

    // Resolve _HOST into bind address
    String bindAddress = conf.get(HttpServer2.BIND_ADDRESS);
    String principal =
        filterConfig.get(KerberosAuthenticationHandler.PRINCIPAL);
    if (principal != null) {
      try {
        principal = SecurityUtil.getServerPrincipal(principal, bindAddress);
      } catch (IOException ex) {
        throw new RuntimeException("Could not resolve Kerberos principal " +
            "name: " + ex.toString(), ex);
      }
      filterConfig.put(KerberosAuthenticationHandler.PRINCIPAL,
          principal);
    }
  }

  protected Map<String, String> getFilterConfig() {
    return filterConfig;
  }

  /**
   * Initializes {@link TimelineAuthenticationFilter}.
   * <p>
   * Propagates to {@link TimelineAuthenticationFilter} configuration all YARN
   * configuration properties prefixed with {@value #PREFIX}.
   *
   * @param container
   *          The filter container.
   * @param conf
   *          Configuration for run-time parameters.
   */
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    setAuthFilterConfig(conf);

    String authType = filterConfig.get(AuthenticationFilter.AUTH_TYPE);
    if (authType.equals(PseudoAuthenticationHandler.TYPE)) {
      filterConfig.put(AuthenticationFilter.AUTH_TYPE,
          PseudoDelegationTokenAuthenticationHandler.class.getName());
    } else if (authType.equals(KerberosAuthenticationHandler.TYPE)) {
      filterConfig.put(AuthenticationFilter.AUTH_TYPE,
          KerberosDelegationTokenAuthenticationHandler.class.getName());
    }
    filterConfig.put(DelegationTokenAuthenticationHandler.TOKEN_KIND,
        TimelineDelegationTokenIdentifier.KIND_NAME.toString());

    container.addGlobalFilter("Timeline Authentication Filter",
        TimelineAuthenticationFilter.class.getName(),
        filterConfig);
  }
}
