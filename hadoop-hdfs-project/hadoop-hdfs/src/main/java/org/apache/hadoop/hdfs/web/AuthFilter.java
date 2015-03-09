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
package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.util.StringUtils;

/**
 * Subclass of {@link AuthenticationFilter} that
 * obtains Hadoop-Auth configuration for webhdfs.
 */
public class AuthFilter extends AuthenticationFilter {
  private static final String CONF_PREFIX = "dfs.web.authentication.";

  /**
   * Returns the filter configuration properties,
   * including the ones prefixed with {@link #CONF_PREFIX}.
   * The prefix is removed from the returned property names.
   *
   * @param prefix parameter not used.
   * @param config parameter contains the initialization values.
   * @return Hadoop-Auth configuration properties.
   * @throws ServletException 
   */
  @Override
  protected Properties getConfiguration(String prefix, FilterConfig config)
      throws ServletException {
    final Properties p = super.getConfiguration(CONF_PREFIX, config);
    // set authentication type
    p.setProperty(AUTH_TYPE, UserGroupInformation.isSecurityEnabled()?
        KerberosAuthenticationHandler.TYPE: PseudoAuthenticationHandler.TYPE);
    // if not set, enable anonymous for pseudo authentication
    if (p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED) == null) {
      p.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
    }
    //set cookie path
    p.setProperty(COOKIE_PATH, "/");
    return p;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    final HttpServletRequest httpRequest = toLowerCase((HttpServletRequest)request);
    final String tokenString = httpRequest.getParameter(DelegationParam.NAME);
    if (tokenString != null) {
      //Token is present in the url, therefore token will be used for
      //authentication, bypass kerberos authentication.
      filterChain.doFilter(httpRequest, response);
      return;
    }
    super.doFilter(httpRequest, response, filterChain);
  }

  private static HttpServletRequest toLowerCase(final HttpServletRequest request) {
    @SuppressWarnings("unchecked")
    final Map<String, String[]> original = (Map<String, String[]>)request.getParameterMap();
    if (!ParamFilter.containsUpperCase(original.keySet())) {
      return request;
    }

    final Map<String, List<String>> m = new HashMap<String, List<String>>();
    for(Map.Entry<String, String[]> entry : original.entrySet()) {
      final String key = StringUtils.toLowerCase(entry.getKey());
      List<String> strings = m.get(key);
      if (strings == null) {
        strings = new ArrayList<String>();
        m.put(key, strings);
      }
      for(String v : entry.getValue()) {
        strings.add(v);
      }
    }

    return new HttpServletRequestWrapper(request) {
      private Map<String, String[]> parameters = null;

      @Override
      public Map<String, String[]> getParameterMap() {
        if (parameters == null) {
          parameters = new HashMap<String, String[]>();
          for(Map.Entry<String, List<String>> entry : m.entrySet()) {
            final List<String> a = entry.getValue();
            parameters.put(entry.getKey(), a.toArray(new String[a.size()]));
          }
        }
       return parameters;
      }

      @Override
      public String getParameter(String name) {
        final List<String> a = m.get(name);
        return a == null? null: a.get(0);
      }
      
      @Override
      public String[] getParameterValues(String name) {
        return getParameterMap().get(name);
      }

      @Override
      public Enumeration<String> getParameterNames() {
        final Iterator<String> i = m.keySet().iterator();
        return new Enumeration<String>() {
          @Override
          public boolean hasMoreElements() {
            return i.hasNext();
          }
          @Override
          public String nextElement() {
            return i.next();
          }
        };
      }
    };
  }
}