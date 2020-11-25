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
package org.apache.hadoop.security.token.delegation.web;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.Principal;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 *  The <code>DelegationTokenAuthenticationFilter</code> filter is a
 *  {@link AuthenticationFilter} with Hadoop Delegation Token support.
 *  <p>
 *  By default it uses it own instance of the {@link
 *  AbstractDelegationTokenSecretManager}. For situations where an external
 *  <code>AbstractDelegationTokenSecretManager</code> is required (i.e. one that
 *  shares the secret with <code>AbstractDelegationTokenSecretManager</code>
 *  instance running in other services), the external
 *  <code>AbstractDelegationTokenSecretManager</code> must be set as an
 *  attribute in the {@link ServletContext} of the web application using the
 *  {@link #DELEGATION_TOKEN_SECRET_MANAGER_ATTR} attribute name (
 *  'hadoop.http.delegation-token-secret-manager').
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DelegationTokenAuthenticationFilter
    extends AuthenticationFilter {

  private static final String APPLICATION_JSON_MIME = "application/json";
  private static final String ERROR_EXCEPTION_JSON = "exception";
  private static final String ERROR_MESSAGE_JSON = "message";

  private static final Logger LOG = LoggerFactory.getLogger(
          DelegationTokenAuthenticationFilter.class);

  /**
   * Sets an external <code>DelegationTokenSecretManager</code> instance to
   * manage creation and verification of Delegation Tokens.
   * <p>
   * This is useful for use cases where secrets must be shared across multiple
   * services.
   */

  public static final String DELEGATION_TOKEN_SECRET_MANAGER_ATTR =
      "hadoop.http.delegation-token-secret-manager";

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  private static final ThreadLocal<UserGroupInformation> UGI_TL =
      new ThreadLocal<UserGroupInformation>();
  public static final String PROXYUSER_PREFIX = "proxyuser";

  private SaslRpcServer.AuthMethod handlerAuthMethod;

  /**
   * It delegates to
   * {@link AuthenticationFilter#getConfiguration(String, FilterConfig)} and
   * then overrides the {@link AuthenticationHandler} to use if authentication
   * type is set to <code>simple</code> or <code>kerberos</code> in order to use
   * the corresponding implementation with delegation token support.
   *
   * @param configPrefix parameter not used.
   * @param filterConfig parameter not used.
   * @return hadoop-auth de-prefixed configuration for the filter and handler.
   */
  @Override
  protected Properties getConfiguration(String configPrefix,
      FilterConfig filterConfig) throws ServletException {
    Properties props = super.getConfiguration(configPrefix, filterConfig);
    setAuthHandlerClass(props);
    return props;
  }

  /**
   * Set AUTH_TYPE property to the name of the corresponding authentication
   * handler class based on the input properties.
   * @param props input properties.
   */
  protected void setAuthHandlerClass(Properties props)
      throws ServletException {
    String authType = props.getProperty(AUTH_TYPE);
    if (authType == null) {
      throw new ServletException("Config property "
          + AUTH_TYPE + " doesn't exist");
    }
    if (authType.equals(PseudoAuthenticationHandler.TYPE)) {
      props.setProperty(AUTH_TYPE,
          PseudoDelegationTokenAuthenticationHandler.class.getName());
    } else if (authType.equals(KerberosAuthenticationHandler.TYPE)) {
      props.setProperty(AUTH_TYPE,
          KerberosDelegationTokenAuthenticationHandler.class.getName());
    } else if (authType.equals(MultiSchemeAuthenticationHandler.TYPE)) {
      props.setProperty(AUTH_TYPE,
          MultiSchemeDelegationTokenAuthenticationHandler.class.getName());
    }
  }

  /**
   * Returns the proxyuser configuration. All returned properties must start
   * with <code>proxyuser.</code>'
   * <p>
   * Subclasses may override this method if the proxyuser configuration is 
   * read from other place than the filter init parameters.
   *
   * @param filterConfig filter configuration object
   * @return the proxyuser configuration properties.
   * @throws ServletException thrown if the configuration could not be created.
   */
  protected Configuration getProxyuserConfiguration(FilterConfig filterConfig)
      throws ServletException {
    // this filter class gets the configuration from the filter configs, we are
    // creating an empty configuration and injecting the proxyuser settings in
    // it. In the initialization of the filter, the returned configuration is
    // passed to the ProxyUsers which only looks for 'proxyusers.' properties.
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


  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    super.init(filterConfig);
    AuthenticationHandler handler = getAuthenticationHandler();
    AbstractDelegationTokenSecretManager dtSecretManager =
        (AbstractDelegationTokenSecretManager) filterConfig.getServletContext().
            getAttribute(DELEGATION_TOKEN_SECRET_MANAGER_ATTR);
    if (dtSecretManager != null && handler
        instanceof DelegationTokenAuthenticationHandler) {
      DelegationTokenAuthenticationHandler dtHandler =
          (DelegationTokenAuthenticationHandler) getAuthenticationHandler();
      dtHandler.setExternalDelegationTokenSecretManager(dtSecretManager);
    }
    if (handler instanceof PseudoAuthenticationHandler ||
        handler instanceof PseudoDelegationTokenAuthenticationHandler) {
      setHandlerAuthMethod(SaslRpcServer.AuthMethod.SIMPLE);
    }
    if (handler instanceof KerberosAuthenticationHandler ||
        handler instanceof KerberosDelegationTokenAuthenticationHandler) {
      setHandlerAuthMethod(SaslRpcServer.AuthMethod.KERBEROS);
    }

    // proxyuser configuration
    Configuration conf = getProxyuserConfiguration(filterConfig);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf, PROXYUSER_PREFIX);
  }

  @Override
  protected void initializeAuthHandler(String authHandlerClassName,
      FilterConfig filterConfig) throws ServletException {
    // A single CuratorFramework should be used for a ZK cluster.
    // If the ZKSignerSecretProvider has already created it, it has to
    // be set here... to be used by the ZKDelegationTokenSecretManager
    ZKDelegationTokenSecretManager.setCurator((CuratorFramework)
        filterConfig.getServletContext().getAttribute(ZKSignerSecretProvider.
            ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE));
    super.initializeAuthHandler(authHandlerClassName, filterConfig);
    ZKDelegationTokenSecretManager.setCurator(null);
  }

  protected void setHandlerAuthMethod(SaslRpcServer.AuthMethod authMethod) {
    this.handlerAuthMethod = authMethod;
  }

  @VisibleForTesting
  static String getDoAs(HttpServletRequest request) {
    String queryString = request.getQueryString();
    if (queryString == null) {
      return null;
    }
    List<NameValuePair> list = URLEncodedUtils.parse(queryString, UTF8_CHARSET);
    if (list != null) {
      for (NameValuePair nv : list) {
        if (DelegationTokenAuthenticatedURL.DO_AS.
            equalsIgnoreCase(nv.getName())) {
          return nv.getValue();
        }
      }
    }
    return null;
  }

  static UserGroupInformation getHttpUserGroupInformationInContext() {
    return UGI_TL.get();
  }

  @Override
  protected void doFilter(FilterChain filterChain, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    boolean requestCompleted = false;
    UserGroupInformation ugi = null;
    AuthenticationToken authToken = (AuthenticationToken)
        request.getUserPrincipal();
    if (authToken != null && authToken != AuthenticationToken.ANONYMOUS) {
      // if the request was authenticated because of a delegation token,
      // then we ignore proxyuser (this is the same as the RPC behavior).
      ugi = (UserGroupInformation) request.getAttribute(
          DelegationTokenAuthenticationHandler.DELEGATION_TOKEN_UGI_ATTRIBUTE);
      if (ugi == null) {
        String realUser = request.getUserPrincipal().getName();
        ugi = UserGroupInformation.createRemoteUser(realUser,
            handlerAuthMethod);
        String doAsUser = getDoAs(request);
        if (doAsUser != null) {
          ugi = UserGroupInformation.createProxyUser(doAsUser, ugi);
          try {
            ProxyUsers.authorize(ugi, request.getRemoteAddr());
          } catch (AuthorizationException ex) {
            HttpExceptionUtils.createServletExceptionResponse(response,
                HttpServletResponse.SC_FORBIDDEN, ex);
            requestCompleted = true;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Authentication exception: " + ex.getMessage(), ex);
            } else {
              LOG.warn("Authentication exception: " + ex.getMessage());
            }
          }
        }
      }
      UGI_TL.set(ugi);
    }
    if (!requestCompleted) {
      final UserGroupInformation ugiF = ugi;
      try {
        request = new HttpServletRequestWrapper(request) {

          @Override
          public String getAuthType() {
            return (ugiF != null) ? handlerAuthMethod.toString() : null;
          }

          @Override
          public String getRemoteUser() {
            return (ugiF != null) ? ugiF.getShortUserName() : null;
          }

          @Override
          public Principal getUserPrincipal() {
            return (ugiF != null) ? new Principal() {
              @Override
              public String getName() {
                return ugiF.getUserName();
              }
            } : null;
          }
        };
        super.doFilter(filterChain, request, response);
      } finally {
        UGI_TL.remove();
      }
    }
  }
}
