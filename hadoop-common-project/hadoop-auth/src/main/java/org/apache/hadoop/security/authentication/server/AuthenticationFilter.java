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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerException;
import org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.security.authentication.util.StringSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * The {@link AuthenticationFilter} enables protecting web application resources with different (pluggable)
 * authentication mechanisms and signer secret providers.
 * <p/>
 * Out of the box it provides 2 authentication mechanisms: Pseudo and Kerberos SPNEGO.
 * <p/>
 * Additional authentication mechanisms are supported via the {@link AuthenticationHandler} interface.
 * <p/>
 * This filter delegates to the configured authentication handler for authentication and once it obtains an
 * {@link AuthenticationToken} from it, sets a signed HTTP cookie with the token. For client requests
 * that provide the signed HTTP cookie, it verifies the validity of the cookie, extracts the user information
 * and lets the request proceed to the target resource.
 * <p/>
 * The supported configuration properties are:
 * <ul>
 * <li>config.prefix: indicates the prefix to be used by all other configuration properties, the default value
 * is no prefix. See below for details on how/why this prefix is used.</li>
 * <li>[#PREFIX#.]type: simple|kerberos|#CLASS#, 'simple' is short for the
 * {@link PseudoAuthenticationHandler}, 'kerberos' is short for {@link KerberosAuthenticationHandler}, otherwise
 * the full class name of the {@link AuthenticationHandler} must be specified.</li>
 * <li>[#PREFIX#.]signature.secret: when signer.secret.provider is set to
 * "string" or not specified, this is the value for the secret used to sign the
 * HTTP cookie.</li>
 * <li>[#PREFIX#.]token.validity: time -in seconds- that the generated token is
 * valid before a new authentication is triggered, default value is
 * <code>3600</code> seconds. This is also used for the rollover interval for
 * the "random" and "zookeeper" SignerSecretProviders.</li>
 * <li>[#PREFIX#.]cookie.domain: domain to use for the HTTP cookie that stores the authentication token.</li>
 * <li>[#PREFIX#.]cookie.path: path to use for the HTTP cookie that stores the authentication token.</li>
 * </ul>
 * <p/>
 * The rest of the configuration properties are specific to the {@link AuthenticationHandler} implementation and the
 * {@link AuthenticationFilter} will take all the properties that start with the prefix #PREFIX#, it will remove
 * the prefix from it and it will pass them to the the authentication handler for initialization. Properties that do
 * not start with the prefix will not be passed to the authentication handler initialization.
 * <p/>
 * Out of the box it provides 3 signer secret provider implementations:
 * "string", "random", and "zookeeper"
 * <p/>
 * Additional signer secret providers are supported via the
 * {@link SignerSecretProvider} class.
 * <p/>
 * For the HTTP cookies mentioned above, the SignerSecretProvider is used to
 * determine the secret to use for signing the cookies. Different
 * implementations can have different behaviors.  The "string" implementation
 * simply uses the string set in the [#PREFIX#.]signature.secret property
 * mentioned above.  The "random" implementation uses a randomly generated
 * secret that rolls over at the interval specified by the
 * [#PREFIX#.]token.validity mentioned above.  The "zookeeper" implementation
 * is like the "random" one, except that it synchronizes the random secret
 * and rollovers between multiple servers; it's meant for HA services.
 * <p/>
 * The relevant configuration properties are:
 * <ul>
 * <li>signer.secret.provider: indicates the name of the SignerSecretProvider
 * class to use. Possible values are: "string", "random", "zookeeper", or a
 * classname. If not specified, the "string" implementation will be used with
 * [#PREFIX#.]signature.secret; and if that's not specified, the "random"
 * implementation will be used.</li>
 * <li>[#PREFIX#.]signature.secret: When the "string" implementation is
 * specified, this value is used as the secret.</li>
 * <li>[#PREFIX#.]token.validity: When the "random" or "zookeeper"
 * implementations are specified, this value is used as the rollover
 * interval.</li>
 * </ul>
 * <p/>
 * The "zookeeper" implementation has additional configuration properties that
 * must be specified; see {@link ZKSignerSecretProvider} for details.
 * <p/>
 * For subclasses of AuthenticationFilter that want additional control over the
 * SignerSecretProvider, they can use the following attribute set in the
 * ServletContext:
 * <ul>
 * <li>signer.secret.provider.object: A SignerSecretProvider implementation can
 * be passed here that will be used instead of the signer.secret.provider
 * configuration property. Note that the class should already be
 * initialized.</li>
 * </ul>
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AuthenticationFilter implements Filter {

  private static Logger LOG = LoggerFactory.getLogger(AuthenticationFilter.class);

  /**
   * Constant for the property that specifies the configuration prefix.
   */
  public static final String CONFIG_PREFIX = "config.prefix";

  /**
   * Constant for the property that specifies the authentication handler to use.
   */
  public static final String AUTH_TYPE = "type";

  /**
   * Constant for the property that specifies the secret to use for signing the HTTP Cookies.
   */
  public static final String SIGNATURE_SECRET = "signature.secret";

  /**
   * Constant for the configuration property that indicates the validity of the generated token.
   */
  public static final String AUTH_TOKEN_VALIDITY = "token.validity";

  /**
   * Constant for the configuration property that indicates the domain to use in the HTTP cookie.
   */
  public static final String COOKIE_DOMAIN = "cookie.domain";

  /**
   * Constant for the configuration property that indicates the path to use in the HTTP cookie.
   */
  public static final String COOKIE_PATH = "cookie.path";

  /**
   * Constant for the configuration property that indicates the name of the
   * SignerSecretProvider class to use.
   * Possible values are: "string", "random", "zookeeper", or a classname.
   * If not specified, the "string" implementation will be used with
   * SIGNATURE_SECRET; and if that's not specified, the "random" implementation
   * will be used.
   */
  public static final String SIGNER_SECRET_PROVIDER =
          "signer.secret.provider";

  /**
   * Constant for the ServletContext attribute that can be used for providing a
   * custom implementation of the SignerSecretProvider. Note that the class
   * should already be initialized. If not specified, SIGNER_SECRET_PROVIDER
   * will be used.
   */
  public static final String SIGNER_SECRET_PROVIDER_ATTRIBUTE =
      "signer.secret.provider.object";

  private Properties config;
  private Signer signer;
  private SignerSecretProvider secretProvider;
  private AuthenticationHandler authHandler;
  private boolean randomSecret;
  private boolean customSecretProvider;
  private long validity;
  private String cookieDomain;
  private String cookiePath;

  /**
   * Initializes the authentication filter and signer secret provider.
   * <p/>
   * It instantiates and initializes the specified {@link AuthenticationHandler}.
   * <p/>
   *
   * @param filterConfig filter configuration.
   *
   * @throws ServletException thrown if the filter or the authentication handler could not be initialized properly.
   */
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String configPrefix = filterConfig.getInitParameter(CONFIG_PREFIX);
    configPrefix = (configPrefix != null) ? configPrefix + "." : "";
    config = getConfiguration(configPrefix, filterConfig);
    String authHandlerName = config.getProperty(AUTH_TYPE, null);
    String authHandlerClassName;
    if (authHandlerName == null) {
      throw new ServletException("Authentication type must be specified: " +
          PseudoAuthenticationHandler.TYPE + "|" + 
          KerberosAuthenticationHandler.TYPE + "|<class>");
    }
    if (authHandlerName.toLowerCase(Locale.ENGLISH).equals(
        PseudoAuthenticationHandler.TYPE)) {
      authHandlerClassName = PseudoAuthenticationHandler.class.getName();
    } else if (authHandlerName.toLowerCase(Locale.ENGLISH).equals(
        KerberosAuthenticationHandler.TYPE)) {
      authHandlerClassName = KerberosAuthenticationHandler.class.getName();
    } else {
      authHandlerClassName = authHandlerName;
    }

    validity = Long.parseLong(config.getProperty(AUTH_TOKEN_VALIDITY, "36000"))
        * 1000; //10 hours
    initializeSecretProvider(filterConfig);

    initializeAuthHandler(authHandlerClassName, filterConfig);


    cookieDomain = config.getProperty(COOKIE_DOMAIN, null);
    cookiePath = config.getProperty(COOKIE_PATH, null);
  }

  protected void initializeAuthHandler(String authHandlerClassName, FilterConfig filterConfig)
      throws ServletException {
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(authHandlerClassName);
      authHandler = (AuthenticationHandler) klass.newInstance();
      authHandler.init(config);
    } catch (ClassNotFoundException ex) {
      throw new ServletException(ex);
    } catch (InstantiationException ex) {
      throw new ServletException(ex);
    } catch (IllegalAccessException ex) {
      throw new ServletException(ex);
    }
  }

  protected void initializeSecretProvider(FilterConfig filterConfig)
      throws ServletException {
    secretProvider = (SignerSecretProvider) filterConfig.getServletContext().
        getAttribute(SIGNER_SECRET_PROVIDER_ATTRIBUTE);
    if (secretProvider == null) {
      Class<? extends SignerSecretProvider> providerClass
              = getProviderClass(config);
      try {
        secretProvider = providerClass.newInstance();
      } catch (InstantiationException ex) {
        throw new ServletException(ex);
      } catch (IllegalAccessException ex) {
        throw new ServletException(ex);
      }
      try {
        secretProvider.init(config, filterConfig.getServletContext(), validity);
      } catch (Exception ex) {
        throw new ServletException(ex);
      }
    } else {
      customSecretProvider = true;
    }
    signer = new Signer(secretProvider);
  }

  @SuppressWarnings("unchecked")
  private Class<? extends SignerSecretProvider> getProviderClass(Properties config)
          throws ServletException {
    String providerClassName;
    String signerSecretProviderName
            = config.getProperty(SIGNER_SECRET_PROVIDER, null);
    // fallback to old behavior
    if (signerSecretProviderName == null) {
      String signatureSecret = config.getProperty(SIGNATURE_SECRET, null);
      if (signatureSecret != null) {
        providerClassName = StringSignerSecretProvider.class.getName();
      } else {
        providerClassName = RandomSignerSecretProvider.class.getName();
        randomSecret = true;
      }
    } else {
      if ("random".equals(signerSecretProviderName)) {
        providerClassName = RandomSignerSecretProvider.class.getName();
        randomSecret = true;
      } else if ("string".equals(signerSecretProviderName)) {
        providerClassName = StringSignerSecretProvider.class.getName();
      } else if ("zookeeper".equals(signerSecretProviderName)) {
        providerClassName = ZKSignerSecretProvider.class.getName();
      } else {
        providerClassName = signerSecretProviderName;
        customSecretProvider = true;
      }
    }
    try {
      return (Class<? extends SignerSecretProvider>) Thread.currentThread().
              getContextClassLoader().loadClass(providerClassName);
    } catch (ClassNotFoundException ex) {
      throw new ServletException(ex);
    }
  }

  /**
   * Returns the configuration properties of the {@link AuthenticationFilter}
   * without the prefix. The returned properties are the same that the
   * {@link #getConfiguration(String, FilterConfig)} method returned.
   *
   * @return the configuration properties.
   */
  protected Properties getConfiguration() {
    return config;
  }

  /**
   * Returns the authentication handler being used.
   *
   * @return the authentication handler being used.
   */
  protected AuthenticationHandler getAuthenticationHandler() {
    return authHandler;
  }

  /**
   * Returns if a random secret is being used.
   *
   * @return if a random secret is being used.
   */
  protected boolean isRandomSecret() {
    return randomSecret;
  }

  /**
   * Returns if a custom implementation of a SignerSecretProvider is being used.
   *
   * @return if a custom implementation of a SignerSecretProvider is being used.
   */
  protected boolean isCustomSignerSecretProvider() {
    return customSecretProvider;
  }

  /**
   * Returns the validity time of the generated tokens.
   *
   * @return the validity time of the generated tokens, in seconds.
   */
  protected long getValidity() {
    return validity / 1000;
  }

  /**
   * Returns the cookie domain to use for the HTTP cookie.
   *
   * @return the cookie domain to use for the HTTP cookie.
   */
  protected String getCookieDomain() {
    return cookieDomain;
  }

  /**
   * Returns the cookie path to use for the HTTP cookie.
   *
   * @return the cookie path to use for the HTTP cookie.
   */
  protected String getCookiePath() {
    return cookiePath;
  }

  /**
   * Destroys the filter.
   * <p/>
   * It invokes the {@link AuthenticationHandler#destroy()} method to release any resources it may hold.
   */
  @Override
  public void destroy() {
    if (authHandler != null) {
      authHandler.destroy();
      authHandler = null;
    }
    if (secretProvider != null) {
      secretProvider.destroy();
    }
  }

  /**
   * Returns the filtered configuration (only properties starting with the specified prefix). The property keys
   * are also trimmed from the prefix. The returned {@link Properties} object is used to initialized the
   * {@link AuthenticationHandler}.
   * <p/>
   * This method can be overriden by subclasses to obtain the configuration from other configuration source than
   * the web.xml file.
   *
   * @param configPrefix configuration prefix to use for extracting configuration properties.
   * @param filterConfig filter configuration object
   *
   * @return the configuration to be used with the {@link AuthenticationHandler} instance.
   *
   * @throws ServletException thrown if the configuration could not be created.
   */
  protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
    Properties props = new Properties();
    Enumeration<?> names = filterConfig.getInitParameterNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      if (name.startsWith(configPrefix)) {
        String value = filterConfig.getInitParameter(name);
        props.put(name.substring(configPrefix.length()), value);
      }
    }
    return props;
  }

  /**
   * Returns the full URL of the request including the query string.
   * <p/>
   * Used as a convenience method for logging purposes.
   *
   * @param request the request object.
   *
   * @return the full URL of the request including the query string.
   */
  protected String getRequestURL(HttpServletRequest request) {
    StringBuffer sb = request.getRequestURL();
    if (request.getQueryString() != null) {
      sb.append("?").append(request.getQueryString());
    }
    return sb.toString();
  }

  /**
   * Returns the {@link AuthenticationToken} for the request.
   * <p/>
   * It looks at the received HTTP cookies and extracts the value of the {@link AuthenticatedURL#AUTH_COOKIE}
   * if present. It verifies the signature and if correct it creates the {@link AuthenticationToken} and returns
   * it.
   * <p/>
   * If this method returns <code>null</code> the filter will invoke the configured {@link AuthenticationHandler}
   * to perform user authentication.
   *
   * @param request request object.
   *
   * @return the Authentication token if the request is authenticated, <code>null</code> otherwise.
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws AuthenticationException thrown if the token is invalid or if it has expired.
   */
  protected AuthenticationToken getToken(HttpServletRequest request) throws IOException, AuthenticationException {
    AuthenticationToken token = null;
    String tokenStr = null;
    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookie.getName().equals(AuthenticatedURL.AUTH_COOKIE)) {
          tokenStr = cookie.getValue();
          try {
            tokenStr = signer.verifyAndExtract(tokenStr);
          } catch (SignerException ex) {
            throw new AuthenticationException(ex);
          }
          break;
        }
      }
    }
    if (tokenStr != null) {
      token = AuthenticationToken.parse(tokenStr);
      if (!token.getType().equals(authHandler.getType())) {
        throw new AuthenticationException("Invalid AuthenticationToken type");
      }
      if (token.isExpired()) {
        throw new AuthenticationException("AuthenticationToken expired");
      }
    }
    return token;
  }

  /**
   * If the request has a valid authentication token it allows the request to continue to the target resource,
   * otherwise it triggers an authentication sequence using the configured {@link AuthenticationHandler}.
   *
   * @param request the request object.
   * @param response the response object.
   * @param filterChain the filter chain object.
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws ServletException thrown if a processing error occurred.
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
      throws IOException, ServletException {
    boolean unauthorizedResponse = true;
    int errCode = HttpServletResponse.SC_UNAUTHORIZED;
    AuthenticationException authenticationEx = null;
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    boolean isHttps = "https".equals(httpRequest.getScheme());
    try {
      boolean newToken = false;
      AuthenticationToken token;
      try {
        token = getToken(httpRequest);
      }
      catch (AuthenticationException ex) {
        LOG.warn("AuthenticationToken ignored: " + ex.getMessage());
        // will be sent back in a 401 unless filter authenticates
        authenticationEx = ex;
        token = null;
      }
      if (authHandler.managementOperation(token, httpRequest, httpResponse)) {
        if (token == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Request [{}] triggering authentication", getRequestURL(httpRequest));
          }
          token = authHandler.authenticate(httpRequest, httpResponse);
          if (token != null && token.getExpires() != 0 &&
              token != AuthenticationToken.ANONYMOUS) {
            token.setExpires(System.currentTimeMillis() + getValidity() * 1000);
          }
          newToken = true;
        }
        if (token != null) {
          unauthorizedResponse = false;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Request [{}] user [{}] authenticated", getRequestURL(httpRequest), token.getUserName());
          }
          final AuthenticationToken authToken = token;
          httpRequest = new HttpServletRequestWrapper(httpRequest) {

            @Override
            public String getAuthType() {
              return authToken.getType();
            }

            @Override
            public String getRemoteUser() {
              return authToken.getUserName();
            }

            @Override
            public Principal getUserPrincipal() {
              return (authToken != AuthenticationToken.ANONYMOUS) ? authToken : null;
            }
          };
          if (newToken && !token.isExpired() && token != AuthenticationToken.ANONYMOUS) {
            String signedToken = signer.sign(token.toString());
            createAuthCookie(httpResponse, signedToken, getCookieDomain(),
                    getCookiePath(), token.getExpires(), isHttps);
          }
          doFilter(filterChain, httpRequest, httpResponse);
        }
      } else {
        unauthorizedResponse = false;
      }
    } catch (AuthenticationException ex) {
      // exception from the filter itself is fatal
      errCode = HttpServletResponse.SC_FORBIDDEN;
      authenticationEx = ex;
      LOG.warn("Authentication exception: " + ex.getMessage(), ex);
    }
    if (unauthorizedResponse) {
      if (!httpResponse.isCommitted()) {
        createAuthCookie(httpResponse, "", getCookieDomain(),
                getCookiePath(), 0, isHttps);
        // If response code is 401. Then WWW-Authenticate Header should be
        // present.. reset to 403 if not found..
        if ((errCode == HttpServletResponse.SC_UNAUTHORIZED)
            && (!httpResponse.containsHeader(
                KerberosAuthenticator.WWW_AUTHENTICATE))) {
          errCode = HttpServletResponse.SC_FORBIDDEN;
        }
        if (authenticationEx == null) {
          httpResponse.sendError(errCode, "Authentication required");
        } else {
          httpResponse.sendError(errCode, authenticationEx.getMessage());
        }
      }
    }
  }

  /**
   * Delegates call to the servlet filter chain. Sub-classes my override this
   * method to perform pre and post tasks.
   */
  protected void doFilter(FilterChain filterChain, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    filterChain.doFilter(request, response);
  }

  /**
   * Creates the Hadoop authentication HTTP cookie.
   *
   * @param token authentication token for the cookie.
   * @param expires UNIX timestamp that indicates the expire date of the
   *                cookie. It has no effect if its value < 0.
   *
   * XXX the following code duplicate some logic in Jetty / Servlet API,
   * because of the fact that Hadoop is stuck at servlet 2.5 and jetty 6
   * right now.
   */
  public static void createAuthCookie(HttpServletResponse resp, String token,
                                      String domain, String path, long expires,
                                      boolean isSecure) {
    StringBuilder sb = new StringBuilder(AuthenticatedURL.AUTH_COOKIE)
                           .append("=");
    if (token != null && token.length() > 0) {
      sb.append("\"").append(token).append("\"");
    }

    if (path != null) {
      sb.append("; Path=").append(path);
    }

    if (domain != null) {
      sb.append("; Domain=").append(domain);
    }

    if (expires >= 0) {
      Date date = new Date(expires);
      SimpleDateFormat df = new SimpleDateFormat("EEE, " +
              "dd-MMM-yyyy HH:mm:ss zzz");
      df.setTimeZone(TimeZone.getTimeZone("GMT"));
      sb.append("; Expires=").append(df.format(date));
    }

    if (isSecure) {
      sb.append("; Secure");
    }

    sb.append("; HttpOnly");
    resp.addHeader("Set-Cookie", sb.toString());
  }
}
