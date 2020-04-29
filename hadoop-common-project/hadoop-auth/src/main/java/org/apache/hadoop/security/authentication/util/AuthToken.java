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
package org.apache.hadoop.security.authentication.util;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 */
public class AuthToken implements Principal {

  /**
   * Constant that identifies an anonymous request.
   */

  private static final String ATTR_SEPARATOR = "&";
  private static final String USER_NAME = "u";
  private static final String PRINCIPAL = "p";
  private static final String MAX_INACTIVES = "i";
  private static final String EXPIRES = "e";
  private static final String TYPE = "t";

  private final static Set<String> ATTRIBUTES =
      new HashSet<>(Arrays.asList(USER_NAME, PRINCIPAL, EXPIRES, TYPE));

  private String userName;
  private String principal;
  private String type;
  private long maxInactives;
  private long expires;
  private String tokenStr;

  protected AuthToken() {
    userName = null;
    principal = null;
    type = null;
    maxInactives = -1;
    expires = -1;
    tokenStr = "ANONYMOUS";
    generateToken();
  }

  private static final String ILLEGAL_ARG_MSG = " is NULL, empty or contains a '" + ATTR_SEPARATOR + "'";

  /**
   * Creates an authentication token.
   *
   * @param userName user name.
   * @param principal principal (commonly matches the user name, with Kerberos is the full/long principal
   * name while the userName is the short name).
   * @param type the authentication mechanism name.
   * (<code>System.currentTimeMillis() + validityPeriod</code>).
   */
  public AuthToken(String userName, String principal, String type) {
    checkForIllegalArgument(userName, "userName");
    checkForIllegalArgument(principal, "principal");
    checkForIllegalArgument(type, "type");
    this.userName = userName;
    this.principal = principal;
    this.type = type;
    this.maxInactives = -1;
    this.expires = -1;
  }
  
  /**
   * Check if the provided value is invalid. Throw an error if it is invalid, NOP otherwise.
   * 
   * @param value the value to check.
   * @param name the parameter name to use in an error message if the value is invalid.
   */
  protected static void checkForIllegalArgument(String value, String name) {
    if (value == null || value.length() == 0 || value.contains(ATTR_SEPARATOR)) {
      throw new IllegalArgumentException(name + ILLEGAL_ARG_MSG);
    }
  }

  /**
   * Sets the max inactive interval of the token.
   *
   * @param interval max inactive interval of the token in milliseconds since
   *                 the epoch.
   */
  public void setMaxInactives(long interval) {
    this.maxInactives = interval;
  }

  /**
   * Sets the expiration of the token.
   *
   * @param expires expiration time of the token in milliseconds since the epoch.
   */
  public void setExpires(long expires) {
    this.expires = expires;
      generateToken();
  }

  /**
   * Returns true if the token has expired.
   *
   * @return true if the token has expired.
   */
  public boolean isExpired() {
    return (getMaxInactives() != -1 &&
        System.currentTimeMillis() > getMaxInactives())
        || (getExpires() != -1 &&
        System.currentTimeMillis() > getExpires());
  }

  /**
   * Generates the token.
   */
  private void generateToken() {
    StringBuilder sb = new StringBuilder();
    sb.append(USER_NAME).append("=").append(getUserName()).append(ATTR_SEPARATOR);
    sb.append(PRINCIPAL).append("=").append(getName()).append(ATTR_SEPARATOR);
    sb.append(TYPE).append("=").append(getType()).append(ATTR_SEPARATOR);
    if (getMaxInactives() != -1) {
      sb.append(MAX_INACTIVES).append("=")
      .append(getMaxInactives()).append(ATTR_SEPARATOR);
    }
    sb.append(EXPIRES).append("=").append(getExpires());
    tokenStr = sb.toString();
  }

  /**
   * Returns the user name.
   *
   * @return the user name.
   */
  public String getUserName() {
    return userName;
  }

  /**
   * Returns the principal name (this method name comes from the JDK {@link Principal} interface).
   *
   * @return the principal name.
   */
  @Override
  public String getName() {
    return principal;
  }

  /**
   * Returns the authentication mechanism of the token.
   *
   * @return the authentication mechanism of the token.
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the max inactive time of the token.
   *
   * @return the max inactive time of the token, in milliseconds since Epoc.
   */
  public long getMaxInactives() {
    return maxInactives;
  }

  /**
   * Returns the expiration time of the token.
   *
   * @return the expiration time of the token, in milliseconds since Epoc.
   */
  public long getExpires() {
    return expires;
  }

  /**
   * Returns the string representation of the token.
   * <p>
   * This string representation is parseable by the {@link #parse} method.
   *
   * @return the string representation of the token.
   */
  @Override
  public String toString() {
    return tokenStr;
  }

  public static AuthToken parse(String tokenStr) throws AuthenticationException {
    if (tokenStr.length() >= 2) {
      // strip the \" at the two ends of the tokenStr
      if (tokenStr.charAt(0) == '\"' &&
          tokenStr.charAt(tokenStr.length()-1) == '\"') {
        tokenStr = tokenStr.substring(1, tokenStr.length()-1);
      }
    } 
    Map<String, String> map = split(tokenStr);
    // remove the signature part, since client doesn't care about it
    map.remove("s");

    if (!map.keySet().containsAll(ATTRIBUTES)) {
      throw new AuthenticationException("Invalid token string, missing attributes");
    }
    long expires = Long.parseLong(map.get(EXPIRES));
    AuthToken token = new AuthToken(map.get(USER_NAME), map.get(PRINCIPAL), map.get(TYPE));
    //process optional attributes
    if (map.containsKey(MAX_INACTIVES)) {
      long maxInactives = Long.parseLong(map.get(MAX_INACTIVES));
      token.setMaxInactives(maxInactives);
    }
    token.setExpires(expires);
    return token;
  }

  /**
   * Splits the string representation of a token into attributes pairs.
   *
   * @param tokenStr string representation of a token.
   *
   * @return a map with the attribute pairs of the token.
   *
   * @throws AuthenticationException thrown if the string representation of the token could not be broken into
   * attribute pairs.
   */
  private static Map<String, String> split(String tokenStr) throws AuthenticationException {
    Map<String, String> map = new HashMap<String, String>();
    StringTokenizer st = new StringTokenizer(tokenStr, ATTR_SEPARATOR);
    while (st.hasMoreTokens()) {
      String part = st.nextToken();
      int separator = part.indexOf('=');
      if (separator == -1) {
        throw new AuthenticationException("Invalid authentication token");
      }
      String key = part.substring(0, separator);
      String value = part.substring(separator + 1);
      map.put(key, value);
    }
    return map;
  }

}
