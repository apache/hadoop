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

package org.apache.slider.server.appmaster.web.rest;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsecureAmFilterInitializer extends FilterInitializer {
  private static final String FILTER_NAME = "AM_PROXY_FILTER";
  private static final String FILTER_CLASS =
      InsecureAmFilter.class.getCanonicalName();
  private static final String HTTPS_PREFIX = "https://";
  private static final String HTTP_PREFIX = "http://";

  static final String PROXY_HOSTS = "PROXY_HOSTS";
  static final String PROXY_HOSTS_DELIMITER = ",";
  static final String PROXY_URI_BASES = "PROXY_URI_BASES";
  static final String PROXY_URI_BASES_DELIMITER = ",";

  private Configuration configuration;

  public static final String NAME =
      "org.apache.slider.server.appmaster.web.rest.InsecureAmFilterInitializer";

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    configuration = conf;
    Map<String, String> params = new HashMap<String, String>();
    String proxy = WebAppUtils.getProxyHostAndPort(conf);
    String[] parts = proxy.split(":");
    params.put(InsecureAmFilter.PROXY_HOST, parts[0]);
    // todo:  eventually call WebAppUtils.getHttpSchemePrefix
    params.put(InsecureAmFilter.PROXY_URI_BASE, getHttpSchemePrefix()
                                                + proxy +
                                                getApplicationWebProxyBase());
    params.put(InsecureAmFilter.WS_CONTEXT_ROOT, RestPaths.WS_CONTEXT_ROOT);
    container.addFilter(FILTER_NAME, FILTER_CLASS, params);
  }

  private void classicAmFilterInitializerInit(FilterContainer container,
      Configuration conf) {
    Map<String, String> params = new HashMap<String, String>();
    List<String> proxies = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf);
    StringBuilder sb = new StringBuilder();
    for (String proxy : proxies) {
      sb.append(proxy.split(":")[0]).append(PROXY_HOSTS_DELIMITER);
    }
    sb.setLength(sb.length() - 1);
    params.put(PROXY_HOSTS, sb.toString());

    String prefix = WebAppUtils.getHttpSchemePrefix(conf);
    String proxyBase = getApplicationWebProxyBase();
    sb = new StringBuilder();
    for (String proxy : proxies) {
      sb.append(prefix).append(proxy).append(proxyBase)
        .append(PROXY_HOSTS_DELIMITER);
    }
    sb.setLength(sb.length() - 1);
    params.put(PROXY_URI_BASES, sb.toString());

  }

  @VisibleForTesting
  protected String getApplicationWebProxyBase() {
    return System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV);
  }

  private String getHttpSchemePrefix() {
    return HttpConfig.Policy.HTTPS_ONLY ==
           HttpConfig.Policy.fromString(configuration
               .get(
                   YarnConfiguration.YARN_HTTP_POLICY_KEY,
                   YarnConfiguration.YARN_HTTP_POLICY_DEFAULT))
           ? HTTPS_PREFIX : HTTP_PREFIX;
  }
}
