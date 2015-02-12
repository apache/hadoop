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

package org.apache.hadoop.yarn.server.webproxy.amfilter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

public class AmFilterInitializer extends FilterInitializer {
  private static final String FILTER_NAME = "AM_PROXY_FILTER";
  private static final String FILTER_CLASS = AmIpFilter.class.getCanonicalName();
  
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> params = new HashMap<>();
    List<String> proxies = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf);
    StringBuilder sb = new StringBuilder();
    for (String proxy : proxies) {
      sb.append(proxy.split(":")[0]).append(AmIpFilter.PROXY_HOSTS_DELIMITER);
    }
    sb.setLength(sb.length() - 1);
    params.put(AmIpFilter.PROXY_HOSTS, sb.toString());

    String prefix = WebAppUtils.getHttpSchemePrefix(conf);
    String proxyBase = getApplicationWebProxyBase();
    sb = new StringBuilder();
    for (String proxy : proxies) {
      sb.append(prefix).append(proxy).append(proxyBase)
          .append(AmIpFilter.PROXY_HOSTS_DELIMITER);
    }
    sb.setLength(sb.length() - 1);
    params.put(AmIpFilter.PROXY_URI_BASES, sb.toString());
    container.addFilter(FILTER_NAME, FILTER_CLASS, params);
  }

  @VisibleForTesting
  protected String getApplicationWebProxyBase() {
    return System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV);
  }
}
