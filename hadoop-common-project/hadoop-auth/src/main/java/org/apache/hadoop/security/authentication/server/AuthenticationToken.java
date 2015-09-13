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
import org.apache.hadoop.security.authentication.util.AuthToken;

import java.security.Principal;

import javax.servlet.http.HttpServletRequest;

/**
 * The {@link AuthenticationToken} contains information about an authenticated
 * HTTP client and doubles as the {@link Principal} to be returned by
 * authenticated {@link HttpServletRequest}s
 * <p>
 * The token can be serialized/deserialized to and from a string as it is sent
 * and received in HTTP client responses and requests as a HTTP cookie (this is
 * done by the {@link AuthenticationFilter}).
 */
public class AuthenticationToken extends AuthToken {

  /**
   * Constant that identifies an anonymous request.
   */
  public static final AuthenticationToken ANONYMOUS = new AuthenticationToken();

  private AuthenticationToken() {
    super();
  }

  private AuthenticationToken(AuthToken token) {
    super(token.getUserName(), token.getName(), token.getType());
    setMaxInactives(token.getMaxInactives());
    setExpires(token.getExpires());
  }

  /**
   * Creates an authentication token.
   *
   * @param userName user name.
   * @param principal principal (commonly matches the user name, with Kerberos is the full/long principal
   * name while the userName is the short name).
   * @param type the authentication mechanism name.
   * (<code>System.currentTimeMillis() + validityPeriod</code>).
   */
  public AuthenticationToken(String userName, String principal, String type) {
    super(userName, principal, type);
  }

  /**
   * Sets the max inactive time of the token.
   *
   * @param maxInactives inactive time of the token in milliseconds
   *                     since the epoch.
   */
  public void setMaxInactives(long maxInactives) {
    if (this != AuthenticationToken.ANONYMOUS) {
      super.setMaxInactives(maxInactives);
    }
  }

  /**
   * Sets the expiration of the token.
   *
   * @param expires expiration time of the token in milliseconds since the epoch.
   */
  public void setExpires(long expires) {
    if (this != AuthenticationToken.ANONYMOUS) {
      super.setExpires(expires);
    }
  }

  /**
   * Returns true if the token has expired.
   *
   * @return true if the token has expired.
   */
  public boolean isExpired() {
    return super.isExpired();
  }

  /**
   * Parses a string into an authentication token.
   *
   * @param tokenStr string representation of a token.
   *
   * @return the parsed authentication token.
   *
   * @throws AuthenticationException thrown if the string representation could not be parsed into
   * an authentication token.
   */
  public static AuthenticationToken parse(String tokenStr) throws AuthenticationException {
    return new AuthenticationToken(AuthToken.parse(tokenStr));
  }
}
