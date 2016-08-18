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

package org.apache.hadoop.fs.aliyun.oss;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Objects;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility methods for Aliyun OSS code.
 */
final public class AliyunOSSUtils {
  private AliyunOSSUtils() {
  }

  /**
   * User information includes user name and password.
   */
  static public class UserInfo {
    private final String user;
    private final String password;

    public static final UserInfo EMPTY = new UserInfo("", "");

    public UserInfo(String user, String password) {
      this.user = user;
      this.password = password;
    }

    /**
     * Predicate to verify user information is set.
     * @return true if the username is defined (not null, not empty).
     */
    public boolean hasLogin() {
      return StringUtils.isNotEmpty(user);
    }

    /**
     * Equality test matches user and password.
     * @param o other object
     * @return true if the objects are considered equivalent.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UserInfo that = (UserInfo) o;
      return Objects.equals(user, that.user) &&
          Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
      return Objects.hash(user, password);
    }

    public String getUser() {
      return user;
    }

    public String getPassword() {
      return password;
    }
  }

  /**
   * Used to get password from configuration, if default value is not available.
   * @param conf configuration that contains password information
   * @param key the key of the password
   * @param val the default value of the key
   * @return the value for the key
   * @throws IOException if failed to get password from configuration
   */
  static public String getPassword(Configuration conf, String key, String val)
      throws IOException {
    if (StringUtils.isEmpty(val)) {
      try {
        final char[] pass = conf.getPassword(key);
        if (pass != null) {
          return (new String(pass)).trim();
        } else {
          return "";
        }
      } catch (IOException ioe) {
        throw new IOException("Cannot find password option " + key, ioe);
      }
    } else {
      return val;
    }
  }

  /**
   * Extract the user information details from a URI.
   * @param name URI of the filesystem.
   * @return a login tuple, possibly empty.
   */
  public static UserInfo extractLoginDetails(URI name) {
    try {
      String authority = name.getAuthority();
      if (authority == null) {
        return UserInfo.EMPTY;
      }
      int loginIndex = authority.indexOf('@');
      if (loginIndex < 0) {
        // No user information
        return UserInfo.EMPTY;
      }
      String login = authority.substring(0, loginIndex);
      int loginSplit = login.indexOf(':');
      if (loginSplit > 0) {
        String user = login.substring(0, loginSplit);
        String password = URLDecoder.decode(login.substring(loginSplit + 1),
            "UTF-8");
        return new UserInfo(user, password);
      } else if (loginSplit == 0) {
        // There is no user, just a password.
        return UserInfo.EMPTY;
      } else {
        return new UserInfo(login, "");
      }
    } catch (UnsupportedEncodingException e) {
      // This should never happen; translate it if it does.
      throw new RuntimeException(e);
    }
  }

  /**
   * Skips the requested number of bytes or fail if there are not enough left.
   * This allows for the possibility that {@link InputStream#skip(long)} may not
   * skip as many bytes as requested (most likely because of reaching EOF).
   * @param is the input stream to skip.
   * @param n the number of bytes to skip.
   * @throws IOException thrown when skipped less number of bytes.
   */
  public static void skipFully(InputStream is, long n) throws IOException {
    long total = 0;
    long cur = 0;

    do {
      cur = is.skip(n - total);
      total += cur;
    } while((total < n) && (cur > 0));

    if (total < n) {
      throw new IOException("Failed to skip " + n + " bytes, possibly due " +
              "to EOF.");
    }
  }
}
