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

package org.apache.hadoop.security;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.security.alias.LocalJavaKeyStoreProvider;

/**
 * Utility methods for both key and credential provider APIs.
 *
 */
public final class ProviderUtils {
  @VisibleForTesting
  public static final String NO_PASSWORD_WARN =
      "WARNING: You have accepted the use of the default provider password\n" +
      "by not configuring a password in one of the two following locations:\n";
  @VisibleForTesting
  public static final String NO_PASSWORD_ERROR =
      "ERROR: The provider cannot find a password in the expected " +
      "locations.\nPlease supply a password using one of the " +
      "following two mechanisms:\n";
  @VisibleForTesting
  public static final String NO_PASSWORD_CONT =
      "Continuing with the default provider password.\n";
  @VisibleForTesting
  public static final String NO_PASSWORD_INSTRUCTIONS_DOC =
      "Please review the documentation regarding provider passwords in\n" +
      "the keystore passwords section of the Credential Provider API\n";

  private static final Log LOG = LogFactory.getLog(ProviderUtils.class);

  /**
   * Hidden ctor to ensure that this utility class isn't
   * instantiated explicitly.
   */
  private ProviderUtils() {
    // hide ctor for checkstyle compliance
  }

  /**
   * Convert a nested URI to decode the underlying path. The translation takes
   * the authority and parses it into the underlying scheme and authority.
   * For example, "myscheme://hdfs@nn/my/path" is converted to
   * "hdfs://nn/my/path".
   * @param nestedUri the URI from the nested URI
   * @return the unnested path
   */
  public static Path unnestUri(URI nestedUri) {
    StringBuilder result = new StringBuilder();
    String authority = nestedUri.getAuthority();
    if (authority != null) {
      String[] parts = nestedUri.getAuthority().split("@", 2);
      result.append(parts[0]);
      result.append("://");
      if (parts.length == 2) {
        result.append(parts[1]);
      }
    }
    result.append(nestedUri.getPath());
    if (nestedUri.getQuery() != null) {
      result.append("?");
      result.append(nestedUri.getQuery());
    }
    if (nestedUri.getFragment() != null) {
      result.append("#");
      result.append(nestedUri.getFragment());
    }
    return new Path(result.toString());
  }

  /**
   * Mangle given local java keystore file URI to allow use as a
   * LocalJavaKeyStoreProvider.
   * @param localFile absolute URI with file scheme and no authority component.
   *                  i.e. return of File.toURI,
   *                  e.g. file:///home/larry/creds.jceks
   * @return URI of the form localjceks://file/home/larry/creds.jceks
   * @throws IllegalArgumentException if localFile isn't not a file uri or if it
   *                                  has an authority component.
   * @throws URISyntaxException if the wrapping process violates RFC 2396
   */
  public static URI nestURIForLocalJavaKeyStoreProvider(final URI localFile)
      throws URISyntaxException {
    if (!("file".equals(localFile.getScheme()))) {
      throw new IllegalArgumentException("passed URI had a scheme other than " +
          "file.");
    }
    if (localFile.getAuthority() != null) {
      throw new IllegalArgumentException("passed URI must not have an " +
          "authority component. For non-local keystores, please use " +
          JavaKeyStoreProvider.class.getName());
    }
    return new URI(LocalJavaKeyStoreProvider.SCHEME_NAME,
        "//file" + localFile.getSchemeSpecificPart(), localFile.getFragment());
  }

  /**
   * There are certain integrations of the credential provider API in
   * which a recursive dependency between the provider and the hadoop
   * filesystem abstraction causes a problem. These integration points
   * need to leverage this utility method to remove problematic provider
   * types from the existing provider path within the configuration.
   *
   * @param config the existing configuration with provider path
   * @param fileSystemClass the class which providers must be compatible
   * @return Configuration clone with new provider path
   */
  public static Configuration excludeIncompatibleCredentialProviders(
      Configuration config, Class<? extends FileSystem> fileSystemClass)
          throws IOException {

    String providerPath = config.get(
        CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);

    if (providerPath == null) {
      return config;
    }
    StringBuffer newProviderPath = new StringBuffer();
    String[] providers = providerPath.split(",");
    Path path = null;
    for (String provider: providers) {
      try {
        path = unnestUri(new URI(provider));
        Class<? extends FileSystem> clazz = null;
        try {
          String scheme = path.toUri().getScheme();
          clazz = FileSystem.getFileSystemClass(scheme, config);
        } catch (IOException ioe) {
          // not all providers are filesystem based
          // for instance user:/// will not be able to
          // have a filesystem class associated with it.
          if (newProviderPath.length() > 0) {
            newProviderPath.append(",");
          }
          newProviderPath.append(provider);
        }
        if (clazz != null) {
          if (fileSystemClass.isAssignableFrom(clazz)) {
            LOG.debug("Filesystem based provider" +
                " excluded from provider path due to recursive dependency: "
                + provider);
          } else {
            if (newProviderPath.length() > 0) {
              newProviderPath.append(",");
            }
            newProviderPath.append(provider);
          }
        }
      } catch (URISyntaxException e) {
        LOG.warn("Credential Provider URI is invalid." + provider);
      }
    }

    String effectivePath = newProviderPath.toString();
    if (effectivePath.equals(providerPath)) {
      return config;
    }

    Configuration conf = new Configuration(config);
    if (effectivePath.equals("")) {
      conf.unset(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
    } else {
      conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
          effectivePath);
    }
    return conf;
  }

  /**
   * The password is either found in the environment or in a file. This
   * routine implements the logic for locating the password in these
   * locations.
   *
   * @param envWithPass  The name of the environment variable that might
   *                     contain the password. Must not be null.
   * @param fileWithPass The name of a file that could contain the password.
   *                     Can be null.
   * @return The password as a char []; null if not found.
   * @throws IOException If fileWithPass is non-null and points to a
   * nonexistent file or a file that fails to open and be read properly.
   */
  public static char[] locatePassword(String envWithPass, String fileWithPass)
      throws IOException {
    char[] pass = null;
    if (System.getenv().containsKey(envWithPass)) {
      pass = System.getenv(envWithPass).toCharArray();
    }
    if (pass == null) {
      if (fileWithPass != null) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL pwdFile = cl.getResource(fileWithPass);
        if (pwdFile == null) {
          // Provided Password file does not exist
          throw new IOException("Password file does not exist");
        }
        try (InputStream is = pwdFile.openStream()) {
          pass = IOUtils.toString(is).trim().toCharArray();
        }
      }
    }
    return pass;
  }

  private static String noPasswordInstruction(String envKey, String fileKey) {
    return
        "    * In the environment variable " + envKey + "\n" +
        "    * In a file referred to by the configuration entry\n" +
        "      " + fileKey + ".\n" +
        NO_PASSWORD_INSTRUCTIONS_DOC;
  }

  public static String noPasswordWarning(String envKey, String fileKey) {
    return NO_PASSWORD_WARN + noPasswordInstruction(envKey, fileKey) +
        NO_PASSWORD_CONT;
  }

  public static String noPasswordError(String envKey, String fileKey) {
    return NO_PASSWORD_ERROR + noPasswordInstruction(envKey, fileKey);
  }
}
