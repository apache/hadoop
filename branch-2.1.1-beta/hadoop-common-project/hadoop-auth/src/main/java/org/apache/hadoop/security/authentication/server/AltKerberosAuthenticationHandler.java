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
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

 /**
 * The {@link AltKerberosAuthenticationHandler} behaves exactly the same way as
 * the {@link KerberosAuthenticationHandler}, except that it allows for an
 * alternative form of authentication for browsers while still using Kerberos
 * for Java access.  This is an abstract class that should be subclassed
 * to allow a developer to implement their own custom authentication for browser
 * access.  The alternateAuthenticate method will be called whenever a request
 * comes from a browser.
 * <p/>
 */
public abstract class AltKerberosAuthenticationHandler
                        extends KerberosAuthenticationHandler {

  /**
   * Constant that identifies the authentication mechanism.
   */
  public static final String TYPE = "alt-kerberos";

  /**
   * Constant for the configuration property that indicates which user agents
   * are not considered browsers (comma separated)
   */
  public static final String NON_BROWSER_USER_AGENTS =
          TYPE + ".non-browser.user-agents";
  private static final String NON_BROWSER_USER_AGENTS_DEFAULT =
          "java,curl,wget,perl";

  private String[] nonBrowserUserAgents;

  /**
   * Returns the authentication type of the authentication handler,
   * 'alt-kerberos'.
   * <p/>
   *
   * @return the authentication type of the authentication handler,
   * 'alt-kerberos'.
   */
  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public void init(Properties config) throws ServletException {
    super.init(config);

    nonBrowserUserAgents = config.getProperty(
            NON_BROWSER_USER_AGENTS, NON_BROWSER_USER_AGENTS_DEFAULT)
            .split("\\W*,\\W*");
    for (int i = 0; i < nonBrowserUserAgents.length; i++) {
        nonBrowserUserAgents[i] = nonBrowserUserAgents[i].toLowerCase();
    }
  }

  /**
   * It enforces the the Kerberos SPNEGO authentication sequence returning an
   * {@link AuthenticationToken} only after the Kerberos SPNEGO sequence has
   * completed successfully (in the case of Java access) and only after the
   * custom authentication implemented by the subclass in alternateAuthenticate
   * has completed successfully (in the case of browser access).
   * <p/>
   *
   * @param request the HTTP client request.
   * @param response the HTTP client response.
   *
   * @return an authentication token if the request is authorized or null
   *
   * @throws IOException thrown if an IO error occurred
   * @throws AuthenticationException thrown if an authentication error occurred
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
      HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken token;
    if (isBrowser(request.getHeader("User-Agent"))) {
      token = alternateAuthenticate(request, response);
    }
    else {
      token = super.authenticate(request, response);
    }
    return token;
  }

  /**
   * This method parses the User-Agent String and returns whether or not it
   * refers to a browser.  If its not a browser, then Kerberos authentication
   * will be used; if it is a browser, alternateAuthenticate from the subclass
   * will be used.
   * <p/>
   * A User-Agent String is considered to be a browser if it does not contain
   * any of the values from alt-kerberos.non-browser.user-agents; the default
   * behavior is to consider everything a browser unless it contains one of:
   * "java", "curl", "wget", or "perl".  Subclasses can optionally override
   * this method to use different behavior.
   *
   * @param userAgent The User-Agent String, or null if there isn't one
   * @return true if the User-Agent String refers to a browser, false if not
   */
  protected boolean isBrowser(String userAgent) {
    if (userAgent == null) {
      return false;
    }
    userAgent = userAgent.toLowerCase();
    boolean isBrowser = true;
    for (String nonBrowserUserAgent : nonBrowserUserAgents) {
        if (userAgent.contains(nonBrowserUserAgent)) {
            isBrowser = false;
            break;
        }
    }
    return isBrowser;
  }

  /**
   * Subclasses should implement this method to provide the custom
   * authentication to be used for browsers.
   *
   * @param request the HTTP client request.
   * @param response the HTTP client response.
   * @return an authentication token if the request is authorized, or null
   * @throws IOException thrown if an IO error occurs
   * @throws AuthenticationException thrown if an authentication error occurs
   */
  public abstract AuthenticationToken alternateAuthenticate(
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException;
}
