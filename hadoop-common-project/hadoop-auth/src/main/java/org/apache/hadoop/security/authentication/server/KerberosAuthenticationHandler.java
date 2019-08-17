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

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The {@link KerberosAuthenticationHandler} implements the Kerberos SPNEGO
 * authentication mechanism for HTTP.
 * <p>
 * The supported configuration properties are:
 * <ul>
 * <li>kerberos.principal: the Kerberos principal to used by the server. As
 * stated by the Kerberos SPNEGO specification, it should be
 * <code>HTTP/${HOSTNAME}@{REALM}</code>. The realm can be omitted from the
 * principal as the JDK GSS libraries will use the realm name of the configured
 * default realm.
 * It does not have a default value.</li>
 * <li>kerberos.keytab: the keytab file containing the credentials for the
 * Kerberos principal.
 * It does not have a default value.</li>
 * <li>kerberos.name.rules: kerberos names rules to resolve principal names, see
 * {@link KerberosName#setRules(String)}</li>
 * </ul>
 */
public class KerberosAuthenticationHandler implements AuthenticationHandler {
  public static final Logger LOG = LoggerFactory.getLogger(
      KerberosAuthenticationHandler.class);

  /**
   * Constant that identifies the authentication mechanism.
   */
  public static final String TYPE = "kerberos";

  /**
   * Constant for the configuration property that indicates the kerberos
   * principal.
   */
  public static final String PRINCIPAL = TYPE + ".principal";

  /**
   * Constant for the configuration property that indicates the keytab
   * file path.
   */
  public static final String KEYTAB = TYPE + ".keytab";

  /**
   * Constant for the configuration property that indicates the Kerberos name
   * rules for the Kerberos principals.
   */
  public static final String NAME_RULES = TYPE + ".name.rules";

  /**
   * Constant for the configuration property that indicates how auth_to_local
   * rules are evaluated.
   */
  public static final String RULE_MECHANISM = TYPE + ".name.rules.mechanism";

  private String type;
  private String keytab;
  private GSSManager gssManager;
  private Subject serverSubject = new Subject();

  /**
   * Creates a Kerberos SPNEGO authentication handler with the default
   * auth-token type, <code>kerberos</code>.
   */
  public KerberosAuthenticationHandler() {
    this(TYPE);
  }

  /**
   * Creates a Kerberos SPNEGO authentication handler with a custom auth-token
   * type.
   *
   * @param type auth-token type.
   */
  public KerberosAuthenticationHandler(String type) {
    this.type = type;
  }

  /**
   * Initializes the authentication handler instance.
   * <p>
   * It creates a Kerberos context using the principal and keytab specified in
   * the configuration.
   * <p>
   * This method is invoked by the {@link AuthenticationFilter#init} method.
   *
   * @param config configuration properties to initialize the handler.
   *
   * @throws ServletException thrown if the handler could not be initialized.
   */
  @Override
  public void init(Properties config) throws ServletException {
    try {
      String principal = config.getProperty(PRINCIPAL);
      if (principal == null || principal.trim().length() == 0) {
        throw new ServletException("Principal not defined in configuration");
      }
      keytab = config.getProperty(KEYTAB, keytab);
      if (keytab == null || keytab.trim().length() == 0) {
        throw new ServletException("Keytab not defined in configuration");
      }
      File keytabFile = new File(keytab);
      if (!keytabFile.exists()) {
        throw new ServletException("Keytab does not exist: " + keytab);
      }
      
      // use all SPNEGO principals in the keytab if a principal isn't
      // specifically configured
      final String[] spnegoPrincipals;
      if (principal.equals("*")) {
        spnegoPrincipals = KerberosUtil.getPrincipalNames(
            keytab, Pattern.compile("HTTP/.*"));
        if (spnegoPrincipals.length == 0) {
          throw new ServletException("Principals do not exist in the keytab");
        }
      } else {
        spnegoPrincipals = new String[]{principal};
      }
      KeyTab keytabInstance = KeyTab.getInstance(keytabFile);
      serverSubject.getPrivateCredentials().add(keytabInstance);
      for (String spnegoPrincipal : spnegoPrincipals) {
        Principal krbPrincipal = new KerberosPrincipal(spnegoPrincipal);
        LOG.info("Using keytab {}, for principal {}",
            keytab, krbPrincipal);
        serverSubject.getPrincipals().add(krbPrincipal);
      }
      String nameRules = config.getProperty(NAME_RULES, null);
      if (nameRules != null) {
        KerberosName.setRules(nameRules);
      }
      String ruleMechanism = config.getProperty(RULE_MECHANISM, null);
      if (ruleMechanism != null) {
        KerberosName.setRuleMechanism(ruleMechanism);
      }
      try {
        gssManager = Subject.doAs(serverSubject,
            new PrivilegedExceptionAction<GSSManager>() {
              @Override
              public GSSManager run() throws Exception {
                return GSSManager.getInstance();
              }
            });
      } catch (PrivilegedActionException ex) {
        throw ex.getException();
      }
    } catch (Exception ex) {
      throw new ServletException(ex);
    }
  }

  /**
   * Releases any resources initialized by the authentication handler.
   * <p>
   * It destroys the Kerberos context.
   */
  @Override
  public void destroy() {
    keytab = null;
    serverSubject = null;
  }

  /**
   * Returns the authentication type of the authentication handler, 'kerberos'.
   * <p>
   *
   * @return the authentication type of the authentication handler, 'kerberos'.
   */
  @Override
  public String getType() {
    return type;
  }

  /**
   * Returns the Kerberos principals used by the authentication handler.
   *
   * @return the Kerberos principals used by the authentication handler.
   */
  protected Set<KerberosPrincipal> getPrincipals() {
    return serverSubject.getPrincipals(KerberosPrincipal.class);
  }

  /**
   * Returns the keytab used by the authentication handler.
   *
   * @return the keytab used by the authentication handler.
   */
  protected String getKeytab() {
    return keytab;
  }

  /**
   * This is an empty implementation, it always returns <code>TRUE</code>.
   *
   *
   *
   * @param token the authentication token if any, otherwise <code>NULL</code>.
   * @param request the HTTP client request.
   * @param response the HTTP client response.
   *
   * @return <code>TRUE</code>
   * @throws IOException it is never thrown.
   * @throws AuthenticationException it is never thrown.
   */
  @Override
  public boolean managementOperation(AuthenticationToken token,
                                     HttpServletRequest request,
                                     HttpServletResponse response)
    throws IOException, AuthenticationException {
    return true;
  }

  /**
   * It enforces the the Kerberos SPNEGO authentication sequence returning an
   * {@link AuthenticationToken} only after the Kerberos SPNEGO sequence has
   * completed successfully.
   *
   * @param request the HTTP client request.
   * @param response the HTTP client response.
   *
   * @return an authentication token if the Kerberos SPNEGO sequence is complete
   * and valid, <code>null</code> if it is in progress (in this case the handler
   * handles the response to the client).
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws AuthenticationException thrown if Kerberos SPNEGO sequence failed.
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
      final HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken token = null;
    String authorization = request.getHeader(
        KerberosAuthenticator.AUTHORIZATION);

    if (authorization == null
        || !authorization.startsWith(KerberosAuthenticator.NEGOTIATE)) {
      response.setHeader(WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      if (authorization == null) {
        LOG.trace("SPNEGO starting for url: {}", request.getRequestURL());
      } else {
        LOG.warn("'" + KerberosAuthenticator.AUTHORIZATION +
            "' does not start with '" +
            KerberosAuthenticator.NEGOTIATE + "' :  {}", authorization);
      }
    } else {
      authorization = authorization.substring(
          KerberosAuthenticator.NEGOTIATE.length()).trim();
      final Base64 base64 = new Base64(0);
      final byte[] clientToken = base64.decode(authorization);
      try {
        final String serverPrincipal =
            KerberosUtil.getTokenServerName(clientToken);
        if (!serverPrincipal.startsWith("HTTP/")) {
          throw new IllegalArgumentException(
              "Invalid server principal " + serverPrincipal +
              "decoded from client request");
        }
        token = Subject.doAs(serverSubject,
            new PrivilegedExceptionAction<AuthenticationToken>() {
              @Override
              public AuthenticationToken run() throws Exception {
                return runWithPrincipal(serverPrincipal, clientToken,
                      base64, response);
              }
            });
      } catch (PrivilegedActionException ex) {
        if (ex.getException() instanceof IOException) {
          throw (IOException) ex.getException();
        } else {
          throw new AuthenticationException(ex.getException());
        }
      } catch (Exception ex) {
        throw new AuthenticationException(ex);
      }
    }
    return token;
  }

  private AuthenticationToken runWithPrincipal(String serverPrincipal,
      byte[] clientToken, Base64 base64, HttpServletResponse response) throws
      IOException, GSSException {
    GSSContext gssContext = null;
    GSSCredential gssCreds = null;
    AuthenticationToken token = null;
    try {
      LOG.trace("SPNEGO initiated with server principal [{}]", serverPrincipal);
      gssCreds = this.gssManager.createCredential(
          this.gssManager.createName(serverPrincipal,
              KerberosUtil.NT_GSS_KRB5_PRINCIPAL_OID),
          GSSCredential.INDEFINITE_LIFETIME,
          new Oid[]{
              KerberosUtil.GSS_SPNEGO_MECH_OID,
              KerberosUtil.GSS_KRB5_MECH_OID },
          GSSCredential.ACCEPT_ONLY);
      gssContext = this.gssManager.createContext(gssCreds);
      byte[] serverToken = gssContext.acceptSecContext(clientToken, 0,
          clientToken.length);
      if (serverToken != null && serverToken.length > 0) {
        String authenticate = base64.encodeToString(serverToken);
        response.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE,
                           KerberosAuthenticator.NEGOTIATE + " " +
                           authenticate);
      }
      if (!gssContext.isEstablished()) {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        LOG.trace("SPNEGO in progress");
      } else {
        String clientPrincipal = gssContext.getSrcName().toString();
        KerberosName kerberosName = new KerberosName(clientPrincipal);
        String userName = kerberosName.getShortName();
        token = new AuthenticationToken(userName, clientPrincipal, getType());
        response.setStatus(HttpServletResponse.SC_OK);
        LOG.trace("SPNEGO completed for client principal [{}]",
            clientPrincipal);
      }
    } finally {
      if (gssContext != null) {
        gssContext.dispose();
      }
      if (gssCreds != null) {
        gssCreds.dispose();
      }
    }
    return token;
  }
}
