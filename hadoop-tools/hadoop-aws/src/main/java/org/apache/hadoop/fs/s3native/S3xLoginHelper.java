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

package org.apache.hadoop.fs.s3native;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

/**
 * Class to aid logging in to S3 endpoints.
 * It is in S3N so that it can be used across all S3 filesystems.
 *
 * The core function of this class was the extraction and decoding of user:secret
 * information from filesystems URIs. As this is no longer supported,
 * its role has been reduced to checking for secrets in the URI and rejecting
 * them where found.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3xLoginHelper {

  private S3xLoginHelper() {
  }

  public static final String LOGIN_WARNING =
      "The Filesystem URI contains login details."
      +" This authentication mechanism is no longer supported.";

  /**
   * Build the filesystem URI.
   * @param uri filesystem uri
   * @return the URI to use as the basis for FS operation and qualifying paths.
   * @throws IllegalArgumentException if the URI is in some way invalid.
   */
  public static URI buildFSURI(URI uri) {
    // look for login secrets and fail if they are present.
    rejectSecretsInURIs(uri);
    Objects.requireNonNull(uri, "null uri");
    Objects.requireNonNull(uri.getScheme(), "null uri.getScheme()");
    if (uri.getHost() == null && uri.getAuthority() != null) {
      Objects.requireNonNull(uri.getHost(), "null uri host.");
    }
    Objects.requireNonNull(uri.getHost(), "null uri host.");
    return URI.create(uri.getScheme() + "://" + uri.getHost());
  }

  /**
   * Create a stripped down string value for error messages.
   * @param pathUri URI
   * @return a shortened schema://host/path value
   */
  public static String toString(URI pathUri) {
    return pathUri != null
        ? String.format("%s://%s/%s",
        pathUri.getScheme(), pathUri.getHost(), pathUri.getPath())
        : "(null URI)";
  }

  /**
   * Extract the login details from a URI, raising an exception if
   * the URI contains them.
   * @param name URI of the filesystem, can be null
   * @throws IllegalArgumentException if there is a secret in the URI.
   */
  public static void rejectSecretsInURIs(URI name) {
    Login login = extractLoginDetails(name);
    Preconditions.checkArgument(!login.hasLogin(), LOGIN_WARNING);
  }

  /**
   * Extract the login details from a URI.
   * @param name URI of the filesystem, may be null
   * @return a login tuple, possibly empty.
   */
  @VisibleForTesting
  static Login extractLoginDetails(URI name) {
    if (name == null) {
      return Login.EMPTY;
    }

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
      return new Login(user, encodedPassword.isEmpty()? "": "password removed");
    } else if (loginSplit == 0) {
      // there is no user, just a password. In this case, there's no login
      return Login.EMPTY;
    } else {
      // loginSplit < 0: there is no ":".
      // return a login with a null password
      return new Login(login, "");
    }
  }

  /**
   * Canonicalize the given URI.
   *
   * This strips out login information.
   *
   * @param uri the URI to canonicalize
   * @param defaultPort default port to use in canonicalized URI if the input
   *     URI has no port and this value is greater than 0
   * @return a new, canonicalized URI.
   */
  public static URI canonicalizeUri(URI uri, int defaultPort) {
    if (uri.getPort() == -1 && defaultPort > 0) {
      // reconstruct the uri with the default port set
      try {
        uri = new URI(uri.getScheme(),
            uri.getUserInfo(),
            uri.getHost(),
            defaultPort,
            uri.getPath(),
            uri.getQuery(),
            uri.getFragment());
      } catch (URISyntaxException e) {
        // Should never happen!
        throw new AssertionError("Valid URI became unparseable: " +
            uri);
      }
    }

    return uri;
  }

  /**
   * Check the path, ignoring authentication details.
   * See {@link FileSystem#checkPath(Path)} for the operation of this.
   *
   * Essentially
   * <ol>
   *   <li>The URI is canonicalized.</li>
   *   <li>If the schemas match, the hosts are compared.</li>
   *   <li>If there is a mismatch between null/non-null host, the default FS
   *   values are used to patch in the host.</li>
   * </ol>
   * That all originates in the core FS; the sole change here being to use
   * {@link URI#getHost()} over {@link URI#getAuthority()}. Some of that
   * code looks a relic of the code anti-pattern of using "hdfs:file.txt"
   * to define the path without declaring the hostname. It's retained
   * for compatibility.
   * @param conf FS configuration
   * @param fsUri the FS URI
   * @param path path to check
   * @param defaultPort default port of FS
   */
  public static void checkPath(Configuration conf,
      URI fsUri,
      Path path,
      int defaultPort) {
    URI pathUri = path.toUri();
    String thatScheme = pathUri.getScheme();
    if (thatScheme == null) {
      // fs is relative
      return;
    }
    URI thisUri = canonicalizeUri(fsUri, defaultPort);
    String thisScheme = thisUri.getScheme();
    //hostname and scheme are not case sensitive in these checks
    if (equalsIgnoreCase(thisScheme, thatScheme)) {// schemes match
      String thisHost = thisUri.getHost();
      String thatHost = pathUri.getHost();
      if (thatHost == null &&                // path's host is null
          thisHost != null) {                // fs has a host
        URI defaultUri = FileSystem.getDefaultUri(conf);
        if (equalsIgnoreCase(thisScheme, defaultUri.getScheme())) {
          pathUri = defaultUri; // schemes match, so use this uri instead
        } else {
          pathUri = null; // can't determine auth of the path
        }
      }
      if (pathUri != null) {
        // canonicalize uri before comparing with this fs
        pathUri = canonicalizeUri(pathUri, defaultPort);
        thatHost = pathUri.getHost();
        if (thisHost == thatHost ||       // hosts match
            (thisHost != null &&
                 equalsIgnoreCase(thisHost, thatHost))) {
          return;
        }
      }
    }
    // make sure the exception strips out any auth details
    throw new IllegalArgumentException(
        "Wrong FS " + S3xLoginHelper.toString(pathUri)
            + " -expected " + fsUri);
  }

  /**
   * Simple tuple of login details.
   */
  public static class Login {
    private final String user;
    private final String password;

    public static final Login EMPTY = new Login();

    /**
     * Create an instance with no login details.
     * Calls to {@link #hasLogin()} return false.
     */
    public Login() {
      this("", "");
    }

    public Login(String user, String password) {
      this.user = user;
      this.password = password;
    }

    /**
     * Predicate to verify login details are defined.
     * @return true if the instance contains login information.
     */
    public boolean hasLogin() {
      return StringUtils.isNotEmpty(password) || StringUtils.isNotEmpty(user);
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
      Login that = (Login) o;
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

}
