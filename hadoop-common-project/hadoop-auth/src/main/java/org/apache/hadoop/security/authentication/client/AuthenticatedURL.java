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
package org.apache.hadoop.security.authentication.client;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.CookieHandler;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link AuthenticatedURL} class enables the use of the JDK {@link URL} class
 * against HTTP endpoints protected with the {@link AuthenticationFilter}.
 * <p>
 * The authentication mechanisms supported by default are Hadoop Simple  authentication
 * (also known as pseudo authentication) and Kerberos SPNEGO authentication.
 * <p>
 * Additional authentication mechanisms can be supported via {@link Authenticator} implementations.
 * <p>
 * The default {@link Authenticator} is the {@link KerberosAuthenticator} class which supports
 * automatic fallback from Kerberos SPNEGO to Hadoop Simple authentication.
 * <p>
 * <code>AuthenticatedURL</code> instances are not thread-safe.
 * <p>
 * The usage pattern of the {@link AuthenticatedURL} is:
 * <pre>
 *
 * // establishing an initial connection
 *
 * URL url = new URL("http://foo:8080/bar");
 * AuthenticatedURL.Token token = new AuthenticatedURL.Token();
 * AuthenticatedURL aUrl = new AuthenticatedURL();
 * HttpURLConnection conn = new AuthenticatedURL().openConnection(url, token);
 * ....
 * // use the 'conn' instance
 * ....
 *
 * // establishing a follow up connection using a token from the previous connection
 *
 * HttpURLConnection conn = new AuthenticatedURL().openConnection(url, token);
 * ....
 * // use the 'conn' instance
 * ....
 *
 * </pre>
 */
public class AuthenticatedURL {
  private static final Logger LOG =
      LoggerFactory.getLogger(AuthenticatedURL.class);

  /**
   * Name of the HTTP cookie used for the authentication token between the client and the server.
   */
  public static final String AUTH_COOKIE = "hadoop.auth";

  // a lightweight cookie handler that will be attached to url connections.
  // client code is not required to extract or inject auth cookies.
  private static class AuthCookieHandler extends CookieHandler {
    private HttpCookie authCookie;
    private Map<String, List<String>> cookieHeaders = Collections.emptyMap();

    @Override
    public synchronized Map<String, List<String>> get(URI uri,
        Map<String, List<String>> requestHeaders) throws IOException {
      // call getter so it will reset headers if token is expiring.
      getAuthCookie();
      return cookieHeaders;
    }

    @Override
    public void put(URI uri, Map<String, List<String>> responseHeaders) {
      List<String> headers = responseHeaders.get("Set-Cookie");
      if (headers != null) {
        for (String header : headers) {
          List<HttpCookie> cookies;
          try {
            cookies = HttpCookie.parse(header);
          } catch (IllegalArgumentException iae) {
            // don't care. just skip malformed cookie headers.
            LOG.debug("Cannot parse cookie header: " + header, iae);
            continue;
          }
          for (HttpCookie cookie : cookies) {
            if (AUTH_COOKIE.equals(cookie.getName())) {
              setAuthCookie(cookie);
            }
          }
        }
      }
    }

    // return the auth cookie if still valid.
    private synchronized HttpCookie getAuthCookie() {
      if (authCookie != null && authCookie.hasExpired()) {
        setAuthCookie(null);
      }
      return authCookie;
    }

    private synchronized void setAuthCookie(HttpCookie cookie) {
      final HttpCookie oldCookie = authCookie;
      // will redefine if new cookie is valid.
      authCookie = null;
      cookieHeaders = Collections.emptyMap();
      boolean valid = cookie != null && !cookie.getValue().isEmpty() &&
          !cookie.hasExpired();
      if (valid) {
        // decrease lifetime to avoid using a cookie soon to expire.
        // allows authenticators to pre-emptively reauthenticate to
        // prevent clients unnecessarily receiving a 401.
        long maxAge = cookie.getMaxAge();
        if (maxAge != -1) {
          cookie.setMaxAge(maxAge * 9/10);
          valid = !cookie.hasExpired();
        }
      }
      if (valid) {
        // v0 cookies value aren't quoted by default but tomcat demands
        // quoting.
        if (cookie.getVersion() == 0) {
          String value = cookie.getValue();
          if (!value.startsWith("\"")) {
            value = "\"" + value + "\"";
            cookie.setValue(value);
          }
        }
        authCookie = cookie;
        cookieHeaders = new HashMap<>();
        cookieHeaders.put("Cookie", Arrays.asList(cookie.toString()));
      }
      LOG.trace("Setting token value to {} ({})", authCookie, oldCookie);
    }

    private void setAuthCookieValue(String value) {
      HttpCookie c = null;
      if (value != null) {
        c = new HttpCookie(AUTH_COOKIE, value);
      }
      setAuthCookie(c);
    }
  }

  /**
   * Client side authentication token.
   */
  public static class Token {

    private final AuthCookieHandler cookieHandler = new AuthCookieHandler();

    /**
     * Creates a token.
     */
    public Token() {
    }

    /**
     * Creates a token using an existing string representation of the token.
     *
     * @param tokenStr string representation of the tokenStr.
     */
    public Token(String tokenStr) {
      if (tokenStr == null) {
        throw new IllegalArgumentException("tokenStr cannot be null");
      }
      set(tokenStr);
    }

    /**
     * Returns if a token from the server has been set.
     *
     * @return if a token from the server has been set.
     */
    public boolean isSet() {
      return cookieHandler.getAuthCookie() != null;
    }

    /**
     * Sets a token.
     *
     * @param tokenStr string representation of the tokenStr.
     */
    void set(String tokenStr) {
      cookieHandler.setAuthCookieValue(tokenStr);
    }

    /**
     * Installs a cookie handler for the http request to manage session
     * cookies.
     * @param url
     * @return HttpUrlConnection
     * @throws IOException
     */
    HttpURLConnection openConnection(URL url,
        ConnectionConfigurator connConfigurator) throws IOException {
      // the cookie handler is unfortunately a global static.  it's a
      // synchronized class method so we can safely swap the handler while
      // instantiating the connection object to prevent it leaking into
      // other connections.
      final HttpURLConnection conn;
      synchronized(CookieHandler.class) {
        CookieHandler current = CookieHandler.getDefault();
        CookieHandler.setDefault(cookieHandler);
        try {
          conn = (HttpURLConnection)url.openConnection();
        } finally {
          CookieHandler.setDefault(current);
        }
      }
      if (connConfigurator != null) {
        connConfigurator.configure(conn);
      }
      return conn;
    }

    /**
     * Returns the string representation of the token.
     *
     * @return the string representation of the token.
     */
    @Override
    public String toString() {
      String value = "";
      HttpCookie authCookie = cookieHandler.getAuthCookie();
      if (authCookie != null) {
        value = authCookie.getValue();
        if (value.startsWith("\"")) { // tests don't want the quotes.
          value = value.substring(1, value.length()-1);
        }
      }
      return value;
    }

  }

  private static Class<? extends Authenticator> DEFAULT_AUTHENTICATOR = KerberosAuthenticator.class;

  /**
   * Sets the default {@link Authenticator} class to use when an {@link AuthenticatedURL} instance
   * is created without specifying an authenticator.
   *
   * @param authenticator the authenticator class to use as default.
   */
  public static void setDefaultAuthenticator(Class<? extends Authenticator> authenticator) {
    DEFAULT_AUTHENTICATOR = authenticator;
  }

  /**
   * Returns the default {@link Authenticator} class to use when an {@link AuthenticatedURL} instance
   * is created without specifying an authenticator.
   *
   * @return the authenticator class to use as default.
   */
  public static Class<? extends Authenticator> getDefaultAuthenticator() {
    return DEFAULT_AUTHENTICATOR;
  }

  private Authenticator authenticator;
  private ConnectionConfigurator connConfigurator;

  /**
   * Creates an {@link AuthenticatedURL}.
   */
  public AuthenticatedURL() {
    this(null);
  }

  /**
   * Creates an <code>AuthenticatedURL</code>.
   *
   * @param authenticator the {@link Authenticator} instance to use, if <code>null</code> a {@link
   * KerberosAuthenticator} is used.
   */
  public AuthenticatedURL(Authenticator authenticator) {
    this(authenticator, null);
  }

  /**
   * Creates an <code>AuthenticatedURL</code>.
   *
   * @param authenticator the {@link Authenticator} instance to use, if <code>null</code> a {@link
   * KerberosAuthenticator} is used.
   * @param connConfigurator a connection configurator.
   */
  public AuthenticatedURL(Authenticator authenticator,
                          ConnectionConfigurator connConfigurator) {
    try {
      this.authenticator = (authenticator != null) ? authenticator : DEFAULT_AUTHENTICATOR.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    this.connConfigurator = connConfigurator;
    this.authenticator.setConnectionConfigurator(connConfigurator);
  }

  /**
   * Returns the {@link Authenticator} instance used by the
   * <code>AuthenticatedURL</code>.
   *
   * @return the {@link Authenticator} instance
   */
  protected Authenticator getAuthenticator() {
    return authenticator;
  }

  /**
   * Returns an authenticated {@link HttpURLConnection}.
   *
   * @param url the URL to connect to. Only HTTP/S URLs are supported.
   * @param token the authentication token being used for the user.
   *
   * @return an authenticated {@link HttpURLConnection}.
   *
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public HttpURLConnection openConnection(URL url, Token token) throws IOException, AuthenticationException {
    if (url == null) {
      throw new IllegalArgumentException("url cannot be NULL");
    }
    if (!url.getProtocol().equalsIgnoreCase("http") && !url.getProtocol().equalsIgnoreCase("https")) {
      throw new IllegalArgumentException("url must be for a HTTP or HTTPS resource");
    }
    if (token == null) {
      throw new IllegalArgumentException("token cannot be NULL");
    }
    authenticator.authenticate(url, token);

    // allow the token to create the connection with a cookie handler for
    // managing session cookies.
    return token.openConnection(url, connConfigurator);
  }

  /**
   * Helper method that injects an authentication token to send with a
   * connection. Callers should prefer using
   * {@link Token#openConnection(URL, ConnectionConfigurator)} which
   * automatically manages authentication tokens.
   *
   * @param conn connection to inject the authentication token into.
   * @param token authentication token to inject.
   */
  public static void injectToken(HttpURLConnection conn, Token token) {
    HttpCookie authCookie = token.cookieHandler.getAuthCookie();
    if (authCookie != null) {
      conn.addRequestProperty("Cookie", authCookie.toString());
    }
  }

  /**
   * Helper method that extracts an authentication token received from a connection.
   * <p>
   * This method is used by {@link Authenticator} implementations.
   *
   * @param conn connection to extract the authentication token from.
   * @param token the authentication token.
   *
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public static void extractToken(HttpURLConnection conn, Token token) throws IOException, AuthenticationException {
    int respCode = conn.getResponseCode();
    if (respCode == HttpURLConnection.HTTP_OK
        || respCode == HttpURLConnection.HTTP_CREATED
        || respCode == HttpURLConnection.HTTP_ACCEPTED) {
      // cookie handler should have already extracted the token.  try again
      // for backwards compatibility if this method is called on a connection
      // not opened via this instance.
      token.cookieHandler.put(null, conn.getHeaderFields());
    } else if (respCode == HttpURLConnection.HTTP_NOT_FOUND) {
      LOG.trace("Setting token value to null ({}), resp={}", token, respCode);
      token.set(null);
      throw new FileNotFoundException(conn.getURL().toString());
    } else {
      LOG.trace("Setting token value to null ({}), resp={}", token, respCode);
      token.set(null);
      throw new AuthenticationException("Authentication failed" +
          ", URL: " + conn.getURL() +
          ", status: " + conn.getResponseCode() +
          ", message: " + conn.getResponseMessage());
    }
  }

}
