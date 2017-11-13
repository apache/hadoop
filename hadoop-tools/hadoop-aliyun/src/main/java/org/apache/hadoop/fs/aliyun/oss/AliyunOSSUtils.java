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

import com.aliyun.oss.common.auth.CredentialsProvider;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ProviderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * Utility methods for Aliyun OSS code.
 */
final public class AliyunOSSUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(AliyunOSSUtils.class);

  private AliyunOSSUtils() {
  }

  /**
   * Used to get password from configuration.
   *
   * @param conf configuration that contains password information
   * @param key the key of the password
   * @return the value for the key
   * @throws IOException if failed to get password from configuration
   */
  public static String getValueWithKey(Configuration conf, String key)
      throws IOException {
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
  }

  /**
   * Skip the requested number of bytes or fail if there are no enough bytes
   * left. This allows for the possibility that {@link InputStream#skip(long)}
   * may not skip as many bytes as requested (most likely because of reaching
   * EOF).
   *
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

  /**
   * Calculate a proper size of multipart piece. If <code>minPartSize</code>
   * is too small, the number of multipart pieces may exceed the limit of
   * {@link Constants#MULTIPART_UPLOAD_PART_NUM_LIMIT}.
   *
   * @param contentLength the size of file.
   * @param minPartSize the minimum size of multipart piece.
   * @return a revisional size of multipart piece.
   */
  public static long calculatePartSize(long contentLength, long minPartSize) {
    long tmpPartSize = contentLength / MULTIPART_UPLOAD_PART_NUM_LIMIT + 1;
    return Math.max(minPartSize, tmpPartSize);
  }

  /**
   * Create credential provider specified by configuration, or create default
   * credential provider if not specified.
   *
   * @param conf configuration
   * @return a credential provider
   * @throws IOException on any problem. Class construction issues may be
   * nested inside the IOE.
   */
  public static CredentialsProvider getCredentialsProvider(Configuration conf)
      throws IOException {
    CredentialsProvider credentials;

    String className = conf.getTrimmed(ALIYUN_OSS_CREDENTIALS_PROVIDER_KEY);
    if (StringUtils.isEmpty(className)) {
      Configuration newConf =
          ProviderUtils.excludeIncompatibleCredentialProviders(conf,
              AliyunOSSFileSystem.class);
      credentials = new AliyunCredentialsProvider(newConf);
    } else {
      try {
        LOG.debug("Credential provider class is:" + className);
        Class<?> credClass = Class.forName(className);
        try {
          credentials =
              (CredentialsProvider)credClass.getDeclaredConstructor(
                  Configuration.class).newInstance(conf);
        } catch (NoSuchMethodException | SecurityException e) {
          credentials =
              (CredentialsProvider)credClass.getDeclaredConstructor()
              .newInstance();
        }
      } catch (ClassNotFoundException e) {
        throw new IOException(className + " not found.", e);
      } catch (NoSuchMethodException | SecurityException e) {
        throw new IOException(String.format("%s constructor exception.  A " +
            "class specified in %s must provide an accessible constructor " +
            "accepting URI and Configuration, or an accessible default " +
            "constructor.", className, ALIYUN_OSS_CREDENTIALS_PROVIDER_KEY),
            e);
      } catch (ReflectiveOperationException | IllegalArgumentException e) {
        throw new IOException(className + " instantiation exception.", e);
      }
    }

    return credentials;
  }

  /**
   * Turns a path (relative or otherwise) into an OSS key, adding a trailing
   * "/" if the path is not the root <i>and</i> does not already have a "/"
   * at the end.
   *
   * @param key OSS key or ""
   * @return the with a trailing "/", or, if it is the root key, "".
   */
  public static String maybeAddTrailingSlash(String key) {
    if (StringUtils.isNotEmpty(key) && !key.endsWith("/")) {
      return key + '/';
    } else {
      return key;
    }
  }
}
