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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

@Unstable
public class RMAuthenticationFilterInitializer extends FilterInitializer {

  String configPrefix;
  String kerberosPrincipalProperty;
  String cookiePath;

  public RMAuthenticationFilterInitializer() {
    this.configPrefix = "hadoop.http.authentication.";
    this.kerberosPrincipalProperty = KerberosAuthenticationHandler.PRINCIPAL;
    this.cookiePath = "/";
  }

  protected Map<String, String> createFilterConfig(Configuration conf) {
    Map<String, String> filterConfig = new HashMap<String, String>();

    // setting the cookie path to root '/' so it is used for all resources.
    filterConfig.put(AuthenticationFilter.COOKIE_PATH, cookiePath);

    // Before conf object is passed in, RM has already processed it and used RM
    // specific configs to overwrite hadoop common ones. Hence we just need to
    // source hadoop.proxyuser configs here.
    for (Map.Entry<String, String> entry : conf) {
      String propName = entry.getKey();
      if (propName.startsWith(configPrefix)) {
        String value = conf.get(propName);
        String name = propName.substring(configPrefix.length());
        filterConfig.put(name, value);
      } else if (propName.startsWith(ProxyUsers.CONF_HADOOP_PROXYUSER)) {
        String value = conf.get(propName);
        String name = propName.substring("hadoop.".length());
        filterConfig.put(name, value);
      }
    }

    // Resolve _HOST into bind address
    String bindAddress = conf.get(HttpServer2.BIND_ADDRESS);
    String principal = filterConfig.get(kerberosPrincipalProperty);
    if (principal != null) {
      try {
        principal = SecurityUtil.getServerPrincipal(principal, bindAddress);
      } catch (IOException ex) {
        throw new RuntimeException(
          "Could not resolve Kerberos principal name: " + ex.toString(), ex);
      }
      filterConfig.put(KerberosAuthenticationHandler.PRINCIPAL, principal);
    }

    filterConfig.put(DelegationTokenAuthenticationHandler.TOKEN_KIND,
        RMDelegationTokenIdentifier.KIND_NAME.toString());

    return filterConfig;
  }

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {

    Map<String, String> filterConfig = createFilterConfig(conf);
    container.addFilter("RMAuthenticationFilter",
      RMAuthenticationFilter.class.getName(), filterConfig);
  }

}
