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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.util.Properties;

/**
 *  The <code>DelegationTokenAuthenticationFilter</code> filter is a
 *  {@link AuthenticationFilter} with Hadoop Delegation Token support.
 *  <p/>
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

  /**
   * Sets an external <code>DelegationTokenSecretManager</code> instance to
   * manage creation and verification of Delegation Tokens.
   * <p/>
   * This is useful for use cases where secrets must be shared across multiple
   * services.
   */

  public static final String DELEGATION_TOKEN_SECRET_MANAGER_ATTR =
      "hadoop.http.delegation-token-secret-manager";

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
    String authType = props.getProperty(AUTH_TYPE);
    if (authType.equals(PseudoAuthenticationHandler.TYPE)) {
      props.setProperty(AUTH_TYPE,
          PseudoDelegationTokenAuthenticationHandler.class.getName());
    } else if (authType.equals(KerberosAuthenticationHandler.TYPE)) {
      props.setProperty(AUTH_TYPE,
          KerberosDelegationTokenAuthenticationHandler.class.getName());
    }
    return props;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    super.init(filterConfig);
    AbstractDelegationTokenSecretManager dtSecretManager =
        (AbstractDelegationTokenSecretManager) filterConfig.getServletContext().
            getAttribute(DELEGATION_TOKEN_SECRET_MANAGER_ATTR);
    if (dtSecretManager != null && getAuthenticationHandler()
        instanceof DelegationTokenAuthenticationHandler) {
      DelegationTokenAuthenticationHandler handler =
          (DelegationTokenAuthenticationHandler) getAuthenticationHandler();
      handler.setExternalDelegationTokenSecretManager(dtSecretManager);
    }
  }
}
