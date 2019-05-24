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

package org.apache.hadoop.security.authentication.server;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authorize.ProxyUsers;

/**
 * Filter initializer to initialize
 * {@link ProxyUserAuthenticationFilter} which adds support
 * to perform operations using end user instead of proxy user.
 */
public class ProxyUserAuthenticationFilterInitializer
    extends FilterInitializer {

  private String configPrefix;

  public ProxyUserAuthenticationFilterInitializer() {
    this.configPrefix = "hadoop.http.authentication.";
  }

  protected Map<String, String> createFilterConfig(Configuration conf) {
    Map<String, String> filterConfig = AuthenticationFilterInitializer
        .getFilterConfigMap(conf, configPrefix);
    //Add proxy user configs
    for (Map.Entry<String, String> entry : conf.getPropsWithPrefix(
        ProxyUsers.CONF_HADOOP_PROXYUSER).entrySet()) {
      filterConfig.put("proxyuser" + entry.getKey(), entry.getValue());
    }
    return filterConfig;
  }

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> filterConfig = createFilterConfig(conf);
    container.addFilter("ProxyUserAuthenticationFilter",
        ProxyUserAuthenticationFilter.class.getName(), filterConfig);
  }

}