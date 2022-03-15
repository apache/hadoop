/*
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

package org.apache.hadoop.fs.obs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

/**
 * Helper for OBS login.
 */
final class OBSLoginHelper {
  /**
   * login warning.
   */
  public static final String LOGIN_WARNING =
      "The Filesystem URI contains login details."
          + " This is insecure and may be unsupported in future.";

  /**
   * plus warning.
   */
  public static final String PLUS_WARNING =
      "Secret key contains a special character that should be URL encoded! "
          + "Attempting to resolve...";

  /**
   * defined plus unencoded char.
   */
  public static final String PLUS_UNENCODED = "+";

  /**
   * defined plus encoded char.
   */
  public static final String PLUS_ENCODED = "%2B";

  /**
   * Class logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      OBSLoginHelper.class);

  private OBSLoginHelper() {
  }

  /**
   * Build the filesystem URI. This can include stripping down of part of the
   * URI.
   *
   * @param uri filesystem uri
   * @return the URI to use as the basis for FS operation and qualifying paths.
   * @throws IllegalArgumentException if the URI is in some way invalid.
   */
  public static URI buildFSURI(final URI uri) {
    Objects.requireNonNull(uri, "null uri");
    Objects.requireNonNull(uri.getScheme(), "null uri.getScheme()");
    if (uri.getHost() == null && uri.getAuthority() != null) {
      Objects.requireNonNull(
          uri.getHost(),
          "null uri host."
              + " This can be caused by unencoded / in the "
              + "password string");
    }
    Objects.requireNonNull(uri.getHost(), "null uri host.");
    return URI.create(uri.getScheme() + "://" + uri.getHost());
  }

  /**
   * Create a stripped down string value for error messages.
   *
   * @param pathUri URI
   * @return a shortened schema://host/path value
   */
  public static String toString(final URI pathUri) {
    return pathUri != null
        ? String.format("%s://%s/%s", pathUri.getScheme(),
        pathUri.getHost(), pathUri.getPath())
        : "(null URI)";
  }

  /**
   * Extract the login details from a URI, logging a warning if the URI contains
   * these.
   *
   * @param name URI of the filesystem
   * @return a login tuple, possibly empty.
   */
  public static Login extractLoginDetailsWithWarnings(final URI name) {
    Login login = extractLoginDetails(name);
    if (login.hasLogin()) {
      LOG.warn(LOGIN_WARNING);
    }
    return login;
  }

  /**
   * Extract the login details from a URI.
   *
   * @param name URI of the filesystem
   * @return a login tuple, possibly empty.
   */
  public static Login extractLoginDetails(final URI name) {
    try {
      String authority = name.getAuthority();
      if (authority == null) {
        return Login.EMPTY;
      }
      int loginIndex = authority.indexOf('@');
      if (loginIndex < 0) {
        // no login
        return Login.EMPTY;
      }
      String login = authority.substring(0, loginIndex);
      int loginSplit = login.indexOf(':');
      if (loginSplit > 0) {
        String user = login.substring(0, loginSplit);
        String encodedPassword = login.substring(loginSplit + 1);
        if (encodedPassword.contains(PLUS_UNENCODED)) {
          LOG.warn(PLUS_WARNING);
          encodedPassword = encodedPassword.replaceAll(
              "\\" + PLUS_UNENCODED, PLUS_ENCODED);
        }
        String password = URLDecoder.decode(encodedPassword, "UTF-8");
        return new Login(user, password);
      } else if (loginSplit == 0) {
        // there is no user, just a password. In this case,
        // there's no login
        return Login.EMPTY;
      } else {
        return new Login(login, "");
      }
    } catch (UnsupportedEncodingException e) {
      // this should never happen; translate it if it does.
      throw new RuntimeException(e);
    }
  }

  /**
   * Canonicalize the given URI.
   *
   * <p>This strips out login information.
   *
   * @param uri         the URI to canonicalize
   * @param defaultPort default port to use in canonicalized URI if the input
   *                    URI has no port and this value is greater than 0
   * @return a new, canonicalized URI.
   */
  public static URI canonicalizeUri(final URI uri, final int defaultPort) {
    URI newUri = uri;
    if (uri.getPort() == -1 && defaultPort > 0) {
      // reconstruct the uri with the default port set
      try {
        newUri =
            new URI(
                newUri.getScheme(),
                null,
                newUri.getHost(),
                defaultPort,
                newUri.getPath(),
                newUri.getQuery(),
                newUri.getFragment());
      } catch (URISyntaxException e) {
        // Should never happen!
        throw new AssertionError(
            "Valid URI became unparseable: " + newUri);
      }
    }

    return newUri;
  }

  /**
   * Check the path, ignoring authentication details. See {@link
   * OBSFileSystem#checkPath(Path)} for the operation of this.
   *
   * <p>Essentially
   *
   * <ol>
   * <li>The URI is canonicalized.
   * <li>If the schemas match, the hosts are compared.
   * <li>If there is a mismatch between null/non-null host,
   * the default FS values are used to patch in the host.
   * </ol>
   * <p>
   * That all originates in the core FS; the sole change here being to use
   * {@link URI#getHost()}over {@link URI#getAuthority()}. Some of that code
   * looks a relic of the code anti-pattern of using "hdfs:file.txt" to define
   * the path without declaring the hostname. It's retained for compatibility.
   *
   * @param conf        FS configuration
   * @param fsUri       the FS URI
   * @param path        path to check
   * @param defaultPort default port of FS
   */
  public static void checkPath(final Configuration conf, final URI fsUri,
      final Path path, final int defaultPort) {
    URI pathUri = path.toUri();
    String thatScheme = pathUri.getScheme();
    if (thatScheme == null) {
      // fs is relative
      return;
    }
    URI thisUri = canonicalizeUri(fsUri, defaultPort);
    String thisScheme = thisUri.getScheme();
    // hostname and scheme are not case sensitive in these checks
    if (equalsIgnoreCase(thisScheme, thatScheme)) { // schemes match
      String thisHost = thisUri.getHost();
      String thatHost = pathUri.getHost();
      if (thatHost == null
          && // path's host is null
          thisHost != null) { // fs has a host
        URI defaultUri = FileSystem.getDefaultUri(conf);
        if (equalsIgnoreCase(thisScheme, defaultUri.getScheme())) {
          pathUri
              = defaultUri; // schemes match, so use this uri instead
        } else {
          pathUri = null; // can't determine auth of the path
        }
      }
      if (pathUri != null) {
        // canonicalize uri before comparing with this fs
        pathUri = canonicalizeUri(pathUri, defaultPort);
        thatHost = pathUri.getHost();
        if (equalsIgnoreCase(thisHost, thatHost)) {
          return;
        }
      }
    }
    // make sure the exception strips out any auth details
    throw new IllegalArgumentException(
        "Wrong FS " + OBSLoginHelper.toString(pathUri) + " -expected "
            + fsUri);
  }

  /**
   * Simple tuple of login details.
   */
  public static class Login {
    /**
     * Defined empty login instance.
     */
    public static final Login EMPTY = new Login();

    /**
     * Defined user name.
     */
    private final String user;

    /**
     * Defined password.
     */
    private final String password;

    /**
     * Login token.
     */
    private final String token;

    /**
     * Create an instance with no login details. Calls to {@link #hasLogin()}
     * return false.
     */
    Login() {
      this("", "");
    }

    Login(final String userName, final String passwd) {
      this(userName, passwd, null);
    }

    Login(final String userName, final String passwd,
        final String sessionToken) {
      this.user = userName;
      this.password = passwd;
      this.token = sessionToken;
    }

    /**
     * Predicate to verify login details are defined.
     *
     * @return true if the username is defined (not null, not empty).
     */
    public boolean hasLogin() {
      return StringUtils.isNotEmpty(user);
    }

    /**
     * Equality test matches user and password.
     *
     * @param o other object
     * @return true if the objects are considered equivalent.
     */
    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Login that = (Login) o;
      return Objects.equals(user, that.user) && Objects.equals(password,
          that.password);
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

    public String getToken() {
      return token;
    }
  }
}
