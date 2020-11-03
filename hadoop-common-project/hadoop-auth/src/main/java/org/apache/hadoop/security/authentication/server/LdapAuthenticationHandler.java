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
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * The {@link LdapAuthenticationHandler} implements the BASIC authentication
 * mechanism for HTTP using LDAP back-end.
 *
 * The supported configuration properties are:
 * <ul>
 * <li>ldap.providerurl: The url of the LDAP server. It does not have a default
 * value.</li>
 * <li>ldap.basedn: the base distinguished name (DN) to be used with the LDAP
 * server. This value is appended to the provided user id for authentication
 * purpose. It does not have a default value.</li>
 * <li>ldap.binddomain: the LDAP bind domain value to be used with the LDAP
 * server. This property is optional and useful only in case of Active
 * Directory server.
 * <li>ldap.enablestarttls: A boolean value used to define if the LDAP server
 * supports 'StartTLS' extension.</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LdapAuthenticationHandler implements AuthenticationHandler {
  private static Logger logger = LoggerFactory
      .getLogger(LdapAuthenticationHandler.class);

  /**
   * Constant that identifies the authentication mechanism.
   */
  public static final String TYPE = "ldap";

  /**
   * Constant that identifies the authentication mechanism to be used with the
   * LDAP server.
   */
  public static final String SECURITY_AUTHENTICATION = "simple";

  /**
   * Constant for the configuration property that indicates the url of the LDAP
   * server.
   */
  public static final String PROVIDER_URL = TYPE + ".providerurl";

  /**
   * Constant for the configuration property that indicates the base
   * distinguished name (DN) to be used with the LDAP server. This value is
   * appended to the provided user id for authentication purpose.
   */
  public static final String BASE_DN = TYPE + ".basedn";

  /**
   * Constant for the configuration property that indicates the LDAP bind
   * domain value to be used with the LDAP server.
   */
  public static final String LDAP_BIND_DOMAIN = TYPE + ".binddomain";

  /**
   * Constant for the configuration property that indicates whether
   * the LDAP server supports 'StartTLS' extension.
   */
  public static final String ENABLE_START_TLS = TYPE + ".enablestarttls";

  private String ldapDomain;
  private String baseDN;
  private String providerUrl;
  private Boolean enableStartTls;
  private Boolean disableHostNameVerification;

  /**
   * Configure StartTLS LDAP extension for this handler.
   *
   * @param enableStartTls true If the StartTLS LDAP extension is to be enabled
   *          false otherwise
   */
  @VisibleForTesting
  public void setEnableStartTls(Boolean enableStartTls) {
    this.enableStartTls = enableStartTls;
  }

  /**
   * Configure the Host name verification for this handler. This method is
   * introduced only for unit testing and should never be used in production.
   *
   * @param disableHostNameVerification true to disable host-name verification
   *          false otherwise
   */
  @VisibleForTesting
  public void setDisableHostNameVerification(
      Boolean disableHostNameVerification) {
    this.disableHostNameVerification = disableHostNameVerification;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public void init(Properties config) throws ServletException {
    this.baseDN = config.getProperty(BASE_DN);
    this.providerUrl = config.getProperty(PROVIDER_URL);
    this.ldapDomain = config.getProperty(LDAP_BIND_DOMAIN);
    this.enableStartTls =
        Boolean.valueOf(config.getProperty(ENABLE_START_TLS, "false"));

    Preconditions
        .checkNotNull(this.providerUrl, "The LDAP URI can not be null");
    Preconditions.checkArgument((this.baseDN == null)
        ^ (this.ldapDomain == null),
        "Either LDAP base DN or LDAP domain value needs to be specified");
    if (this.enableStartTls) {
      String tmp = this.providerUrl.toLowerCase();
      Preconditions.checkArgument(!tmp.startsWith("ldaps"),
          "Can not use ldaps and StartTLS option at the same time");
    }
  }

  @Override
  public void destroy() {
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
    AuthenticationToken token = null;
    String authorization =
        request.getHeader(HttpConstants.AUTHORIZATION_HEADER);

    if (authorization == null
        || !AuthenticationHandlerUtil.matchAuthScheme(HttpConstants.BASIC,
            authorization)) {
      response.setHeader(WWW_AUTHENTICATE, HttpConstants.BASIC);
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      if (authorization == null) {
        logger.trace("Basic auth starting");
      } else {
        logger.warn("'" + HttpConstants.AUTHORIZATION_HEADER
            + "' does not start with '" + HttpConstants.BASIC + "' :  {}",
            authorization);
      }
    } else {
      authorization =
          authorization.substring(HttpConstants.BASIC.length()).trim();
      final Base64 base64 = new Base64(0);
      // As per RFC7617, UTF-8 charset should be used for decoding.
      String[] credentials = new String(base64.decode(authorization),
          StandardCharsets.UTF_8).split(":", 2);
      if (credentials.length == 2) {
        token = authenticateUser(credentials[0], credentials[1]);
        response.setStatus(HttpServletResponse.SC_OK);
      }
    }
    return token;
  }

  private AuthenticationToken authenticateUser(String userName,
      String password) throws AuthenticationException {
    if (userName == null || userName.isEmpty()) {
      throw new AuthenticationException("Error validating LDAP user:"
          + " a null or blank username has been provided");
    }

    // If the domain is available in the config, then append it unless domain
    // is already part of the username. LDAP providers like Active Directory
    // use a fully qualified user name like foo@bar.com.
    if (!hasDomain(userName) && ldapDomain != null) {
      userName = userName + "@" + ldapDomain;
    }

    if (password == null || password.isEmpty() ||
        password.getBytes(StandardCharsets.UTF_8)[0] == 0) {
      throw new AuthenticationException("Error validating LDAP user:"
          + " a null or blank password has been provided");
    }

    // setup the security principal
    String bindDN;
    if (baseDN == null) {
      bindDN = userName;
    } else {
      bindDN = "uid=" + userName + "," + baseDN;
    }

    if (this.enableStartTls) {
      authenticateWithTlsExtension(bindDN, password);
    } else {
      authenticateWithoutTlsExtension(bindDN, password);
    }

    return new AuthenticationToken(userName, userName, TYPE);
  }

  private void authenticateWithTlsExtension(String userDN, String password)
      throws AuthenticationException {
    LdapContext ctx = null;
    Hashtable<String, Object> env = new Hashtable<String, Object>();
    env.put(Context.INITIAL_CONTEXT_FACTORY,
        "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, providerUrl);

    try {
      // Create initial context
      ctx = new InitialLdapContext(env, null);
      // Establish TLS session
      StartTlsResponse tls =
          (StartTlsResponse) ctx.extendedOperation(new StartTlsRequest());

      if (disableHostNameVerification) {
        tls.setHostnameVerifier(new HostnameVerifier() {
          @Override
          public boolean verify(String hostname, SSLSession session) {
            return true;
          }
        });
      }

      tls.negotiate();

      // Initialize security credentials & perform read operation for
      // verification.
      ctx.addToEnvironment(Context.SECURITY_AUTHENTICATION,
          SECURITY_AUTHENTICATION);
      ctx.addToEnvironment(Context.SECURITY_PRINCIPAL, userDN);
      ctx.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
      ctx.lookup(userDN);
      logger.debug("Authentication successful for {}", userDN);

    } catch (NamingException | IOException ex) {
      throw new AuthenticationException("Error validating LDAP user", ex);
    } finally {
      if (ctx != null) {
        try {
          ctx.close();
        } catch (NamingException e) { /* Ignore. */
        }
      }
    }
  }

  private void authenticateWithoutTlsExtension(String userDN, String password)
      throws AuthenticationException {
    Hashtable<String, Object> env = new Hashtable<String, Object>();
    env.put(Context.INITIAL_CONTEXT_FACTORY,
        "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, providerUrl);
    env.put(Context.SECURITY_AUTHENTICATION, SECURITY_AUTHENTICATION);
    env.put(Context.SECURITY_PRINCIPAL, userDN);
    env.put(Context.SECURITY_CREDENTIALS, password);

    try {
      // Create initial context
      Context ctx = new InitialDirContext(env);
      ctx.close();
      logger.debug("Authentication successful for {}", userDN);

    } catch (NamingException e) {
      throw new AuthenticationException("Error validating LDAP user", e);
    }
  }

  private static boolean hasDomain(String userName) {
    return (indexOfDomainMatch(userName) > 0);
  }

  /*
   * Get the index separating the user name from domain name (the user's name
   * up to the first '/' or '@').
   *
   * @param userName full user name.
   *
   * @return index of domain match or -1 if not found
   */
  private static int indexOfDomainMatch(String userName) {
    if (userName == null) {
      return -1;
    }

    int idx = userName.indexOf('/');
    int idx2 = userName.indexOf('@');
    int endIdx = Math.min(idx, idx2); // Use the earlier match.
    // Unless at least one of '/' or '@' was not found, in
    // which case, user the latter match.
    if (endIdx == -1) {
      endIdx = Math.max(idx, idx2);
    }
    return endIdx;
  }

}
