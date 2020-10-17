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

import static org.apache.hadoop.security.authentication.server.HttpConstants.NEGOTIATE;
import static org.apache.hadoop.security.authentication.server.HttpConstants.BASIC;
import static org.apache.hadoop.security.authentication.server.HttpConstants.DIGEST;

import java.util.Locale;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * This is a utility class designed to provide functionality related to
 * {@link AuthenticationHandler}.
 */
public final class AuthenticationHandlerUtil {

  /**
   * This class should only contain the static utility methods. Hence it is not
   * intended to be instantiated.
   */
  private AuthenticationHandlerUtil() {
  }

  /**
   * This method provides an instance of {@link AuthenticationHandler} based on
   * specified <code>authHandlerName</code>.
   *
   * @param authHandler The short-name (or fully qualified class name) of the
   *          authentication handler.
   * @return an instance of AuthenticationHandler implementation.
   */
  public static String getAuthenticationHandlerClassName(String authHandler) {
    String handlerName =
        Preconditions.checkNotNull(authHandler).toLowerCase(Locale.ENGLISH);

    String authHandlerClassName = null;

    if (handlerName.equals(PseudoAuthenticationHandler.TYPE)) {
      authHandlerClassName = PseudoAuthenticationHandler.class.getName();
    } else if (handlerName.equals(KerberosAuthenticationHandler.TYPE)) {
      authHandlerClassName = KerberosAuthenticationHandler.class.getName();
    } else if (handlerName.equals(LdapAuthenticationHandler.TYPE)) {
      authHandlerClassName = LdapAuthenticationHandler.class.getName();
    } else if (handlerName.equals(MultiSchemeAuthenticationHandler.TYPE)) {
      authHandlerClassName = MultiSchemeAuthenticationHandler.class.getName();
    } else {
      authHandlerClassName = authHandler;
    }

    return authHandlerClassName;
  }

  /**
   * This method checks if the specified HTTP authentication <code>scheme</code>
   * value is valid.
   *
   * @param scheme HTTP authentication scheme to be checked
   * @return Canonical representation of HTTP authentication scheme
   * @throws IllegalArgumentException In case the specified value is not a valid
   *           HTTP authentication scheme.
   */
  public static String checkAuthScheme(String scheme) {
    if (BASIC.equalsIgnoreCase(scheme)) {
      return BASIC;
    } else if (NEGOTIATE.equalsIgnoreCase(scheme)) {
      return NEGOTIATE;
    } else if (DIGEST.equalsIgnoreCase(scheme)) {
      return DIGEST;
    }
    throw new IllegalArgumentException(String.format(
        "Unsupported HTTP authentication scheme %s ."
            + " Supported schemes are [%s, %s, %s]", scheme, BASIC, NEGOTIATE,
        DIGEST));
  }

  /**
   * This method checks if the specified <code>authToken</code> belongs to the
   * specified HTTP authentication <code>scheme</code>.
   *
   * @param scheme HTTP authentication scheme to be checked
   * @param auth Authentication header value which is to be compared with the
   *          authentication scheme.
   * @return true If the authentication header value corresponds to the
   *         specified authentication scheme false Otherwise.
   */
  public static boolean matchAuthScheme(String scheme, String auth) {
    scheme = Preconditions.checkNotNull(scheme).trim();
    auth = Preconditions.checkNotNull(auth).trim();
    return auth.regionMatches(true, 0, scheme, 0, scheme.length());
  }
}
