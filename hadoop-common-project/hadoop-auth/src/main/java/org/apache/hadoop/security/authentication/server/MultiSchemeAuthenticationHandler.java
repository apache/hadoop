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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

/**
 * The {@link MultiSchemeAuthenticationHandler} supports configuring multiple
 * authentication mechanisms simultaneously. e.g. server can support multiple
 * authentication mechanisms such as Kerberos (SPENGO) and LDAP. During the
 * authentication phase, server will specify all possible authentication schemes
 * and let client choose the appropriate scheme. Please refer to RFC-2616 and
 * HADOOP-12082 for more details.
 * <p>
 * The supported configuration properties are:
 * <ul>
 * <li>multi-scheme-auth-handler.schemes: A comma separated list of HTTP
 * authentication mechanisms supported by this handler. It does not have a
 * default value. e.g. multi-scheme-auth-handler.schemes=basic,negotiate
 * <li>multi-scheme-auth-handler.schemes.${scheme-name}.handler: The
 * authentication handler implementation to be used for the specified
 * authentication scheme. It does not have a default value. e.g.
 * multi-scheme-auth-handler.schemes.negotiate.handler=kerberos
 * </ul>
 *
 * It expected that for every authentication scheme specified in
 * multi-scheme-auth-handler.schemes property, a handler needs to be configured.
 * Note that while scheme values in 'multi-scheme-auth-handler.schemes' property
 * are case-insensitive, the scheme value in the handler configuration property
 * name must be lower case. i.e. property name such as
 * multi-scheme-auth-handler.schemes.Negotiate.handler is invalid.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MultiSchemeAuthenticationHandler implements
    CompositeAuthenticationHandler {
  private static Logger logger = LoggerFactory
      .getLogger(MultiSchemeAuthenticationHandler.class);
  public static final String SCHEMES_PROPERTY =
      "multi-scheme-auth-handler.schemes";
  public static final String AUTH_HANDLER_PROPERTY =
      "multi-scheme-auth-handler.schemes.%s.handler";
  private static final Splitter STR_SPLITTER = Splitter.on(',').trimResults()
      .omitEmptyStrings();

  private final Map<String, AuthenticationHandler> schemeToAuthHandlerMapping =
      new HashMap<>();
  private final Collection<String> types = new HashSet<>();
  private final String authType;

  /**
   * Constant that identifies the authentication mechanism.
   */
  public static final String TYPE = "multi-scheme";

  public MultiSchemeAuthenticationHandler() {
    this(TYPE);
  }

  public MultiSchemeAuthenticationHandler(String authType) {
    this.authType = authType;
  }

  @Override
  public String getType() {
    return authType;
  }

  /**
   * This method returns the token types supported by this authentication
   * handler.
   *
   * @return the token types supported by this authentication handler.
   */
  @Override
  public Collection<String> getTokenTypes() {
    return types;
  }

  @Override
  public void init(Properties config) throws ServletException {
    // Useful for debugging purpose.
    for (Map.Entry prop : config.entrySet()) {
      logger.info("{} : {}", prop.getKey(), prop.getValue());
    }

    this.types.clear();

    String schemesProperty =
        Preconditions.checkNotNull(config.getProperty(SCHEMES_PROPERTY),
            "%s system property is not specified.", SCHEMES_PROPERTY);
    for (String scheme : STR_SPLITTER.split(schemesProperty)) {
      scheme = AuthenticationHandlerUtil.checkAuthScheme(scheme);
      if (schemeToAuthHandlerMapping.containsKey(scheme)) {
        throw new IllegalArgumentException("Handler is already specified for "
            + scheme + " authentication scheme.");
      }

      String authHandlerPropName =
          String.format(AUTH_HANDLER_PROPERTY, scheme).toLowerCase();
      String authHandlerName = config.getProperty(authHandlerPropName);
      Preconditions.checkNotNull(authHandlerName,
          "No auth handler configured for scheme %s.", scheme);

      String authHandlerClassName =
          AuthenticationHandlerUtil
              .getAuthenticationHandlerClassName(authHandlerName);
      AuthenticationHandler handler =
          initializeAuthHandler(authHandlerClassName, config);
      schemeToAuthHandlerMapping.put(scheme, handler);
      types.add(handler.getType());
    }
    logger.info("Successfully initialized MultiSchemeAuthenticationHandler");
  }

  protected AuthenticationHandler initializeAuthHandler(
      String authHandlerClassName, Properties config) throws ServletException {
    try {
      Preconditions.checkNotNull(authHandlerClassName);
      logger.debug("Initializing Authentication handler of type "
          + authHandlerClassName);
      Class<?> klass =
          Thread.currentThread().getContextClassLoader()
              .loadClass(authHandlerClassName);
      AuthenticationHandler authHandler =
          (AuthenticationHandler) klass.newInstance();
      authHandler.init(config);
      logger.info("Successfully initialized Authentication handler of type "
          + authHandlerClassName);
      return authHandler;
    } catch (ClassNotFoundException | InstantiationException
        | IllegalAccessException ex) {
      logger.error("Failed to initialize authentication handler "
          + authHandlerClassName, ex);
      throw new ServletException(ex);
    }
  }

  @Override
  public void destroy() {
    for (AuthenticationHandler handler : schemeToAuthHandlerMapping.values()) {
      handler.destroy();
    }
  }

  @Override
  public boolean managementOperation(AuthenticationToken token,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    return true;
  }

  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
      HttpServletResponse response)
          throws IOException, AuthenticationException {
    String authorization =
        request.getHeader(HttpConstants.AUTHORIZATION_HEADER);
    if (authorization != null) {
      for (Map.Entry<String, AuthenticationHandler> entry :
          schemeToAuthHandlerMapping.entrySet()) {
        if (AuthenticationHandlerUtil.matchAuthScheme(
            entry.getKey(), authorization)) {
          AuthenticationToken token =
              entry.getValue().authenticate(request, response);
          logger.trace("Token generated with type {}", token.getType());
          return token;
        }
      }
    }

    // Handle the case when (authorization == null) or an invalid authorization
    // header (e.g. a header value without the scheme name).
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    for (String scheme : schemeToAuthHandlerMapping.keySet()) {
      response.addHeader(HttpConstants.WWW_AUTHENTICATE_HEADER, scheme);
    }

    return null;
  }
}