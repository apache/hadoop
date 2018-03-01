/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Extend the function of {@link AuthenticationFilter} to
 * support authorizing proxy user. If the query string
 * contains doAs parameter, then check the proxy user,
 * otherwise do the next filter.
 */
public class AuthenticationWithProxyUserFilter extends AuthenticationFilter {

  /**
   * Constant used in URL's query string to perform a proxy user request, the
   * value of the <code>DO_AS</code> parameter is the user the request will be
   * done on behalf of.
   */
  private static final String DO_AS = "doAs";

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");


  /**
   * This method provide the ability to do pre/post tasks
   * in filter chain. Override this method to authorize
   * proxy user between AuthenticationFilter and next filter.
   * @param filterChain the filter chain object.
   * @param request the request object.
   * @param response the response object.
   *
   * @throws IOException
   * @throws ServletException
   */
  @Override
  protected void doFilter(FilterChain filterChain, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {

    // authorize proxy user before calling next filter.
    String proxyUser = getDoAs(request);
    if (proxyUser != null) {
      UserGroupInformation realUser =
          UserGroupInformation.createRemoteUser(request.getRemoteUser());
      UserGroupInformation proxyUserInfo =
          UserGroupInformation.createProxyUser(proxyUser, realUser);

      try {
        ProxyUsers.authorize(proxyUserInfo, request.getRemoteAddr());
      } catch (AuthorizationException ex) {
        HttpExceptionUtils.createServletExceptionResponse(response,
            HttpServletResponse.SC_FORBIDDEN, ex);
        // stop filter chain if there is an Authorization Exception.
        return;
      }

      final UserGroupInformation finalProxyUser = proxyUserInfo;
      // Change the remote user after proxy user is authorized.
      request = new HttpServletRequestWrapper(request) {
        @Override
        public String getRemoteUser() {
          return finalProxyUser.getUserName();
        }
      };

    }
    filterChain.doFilter(request, response);
  }

  /**
   * Get proxy user from query string.
   * @param request the request object
   * @return proxy user
   */
  public static String getDoAs(HttpServletRequest request) {
    String queryString = request.getQueryString();
    if (queryString == null) {
      return null;
    }
    List<NameValuePair> list = URLEncodedUtils.parse(queryString, UTF8_CHARSET);
    if (list != null) {
      for (NameValuePair nv : list) {
        if (DO_AS.equalsIgnoreCase(nv.getName())) {
          return nv.getValue();
        }
      }
    }
    return null;
  }
}
