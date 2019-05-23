/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;
import java.util.Enumeration;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

/**
 * AuthenticationFilter which adds support to perform operations
 * using end user instead of proxy user. Fetches the end user from
 * doAs Query Parameter.
 */
public class ProxyUserAuthenticationFilter extends AuthenticationFilter {

  private static final Logger LOG = LoggerFactory.getLogger(
      ProxyUserAuthenticationFilter.class);

  private static final String DO_AS = "doAs";
  public static final String PROXYUSER_PREFIX = "proxyuser";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    Configuration conf = getProxyuserConfiguration(filterConfig);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf, PROXYUSER_PREFIX);
    super.init(filterConfig);
  }

  @Override
  protected void doFilter(FilterChain filterChain, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {

    String doAsUser = request.getParameter(DO_AS);
    if (doAsUser != null && !doAsUser.equals(request.getRemoteUser())) {
      LOG.debug("doAsUser = {}, RemoteUser = {} , RemoteAddress = {} ",
          doAsUser, request.getRemoteUser(), request.getRemoteAddr());
      UserGroupInformation requestUgi = (request.getUserPrincipal() != null) ?
          UserGroupInformation.createRemoteUser(request.getRemoteUser())
          : null;
      if (requestUgi != null) {
        requestUgi = UserGroupInformation.createProxyUser(doAsUser,
            requestUgi);
        try {
          ProxyUsers.authorize(requestUgi, request.getRemoteAddr());

          final UserGroupInformation ugiF = requestUgi;
          request = new HttpServletRequestWrapper(request) {
            @Override
            public String getRemoteUser() {
              return ugiF.getShortUserName();
            }

            @Override
            public Principal getUserPrincipal() {
              return new Principal() {
                @Override
                public String getName() {
                  return ugiF.getUserName();
                }
              };
            }
          };
          LOG.debug("Proxy user Authentication successful");
        } catch (AuthorizationException ex) {
          HttpExceptionUtils.createServletExceptionResponse(response,
              HttpServletResponse.SC_FORBIDDEN, ex);
          LOG.warn("Proxy user Authentication exception", ex);
          return;
        }
      }
    }
    super.doFilter(filterChain, request, response);
  }

  protected Configuration getProxyuserConfiguration(FilterConfig filterConfig)
      throws ServletException {
    Configuration conf = new Configuration(false);
    Enumeration<?> names = filterConfig.getInitParameterNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      if (name.startsWith(PROXYUSER_PREFIX + ".")) {
        String value = filterConfig.getInitParameter(name);
        conf.set(name, value);
      }
    }
    return conf;
  }

}

