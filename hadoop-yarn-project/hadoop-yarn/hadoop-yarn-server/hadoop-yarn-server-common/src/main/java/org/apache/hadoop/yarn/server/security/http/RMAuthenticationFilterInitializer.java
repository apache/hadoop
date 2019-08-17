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

import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

@Unstable
public class RMAuthenticationFilterInitializer extends FilterInitializer {

  String configPrefix;

  public RMAuthenticationFilterInitializer() {
    this.configPrefix = "hadoop.http.authentication.";
  }

  protected Map<String, String> createFilterConfig(Configuration conf) {
    Map<String, String> filterConfig = AuthenticationFilterInitializer
        .getFilterConfigMap(conf, configPrefix);

    // Before conf object is passed in, RM has already processed it and used RM
    // specific configs to overwrite hadoop common ones. Hence we just need to
    // source hadoop.proxyuser configs here.

    //Add proxy user configs
    for (Map.Entry<String, String> entry : conf.
        getPropsWithPrefix(ProxyUsers.CONF_HADOOP_PROXYUSER).entrySet()) {
      filterConfig.put("proxyuser" + entry.getKey(), entry.getValue());
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
