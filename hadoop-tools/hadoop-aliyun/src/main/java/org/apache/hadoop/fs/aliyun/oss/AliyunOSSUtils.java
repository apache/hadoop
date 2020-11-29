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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.aliyun.oss.common.auth.CredentialsProvider;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
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
  private static LocalDirAllocator directoryAllocator;

  private AliyunOSSUtils() {
  }

  public static int intPositiveOption(
      Configuration conf, String key, int defVal) {
    int v = conf.getInt(key, defVal);
    if (v <= 0) {
      LOG.warn(key + " is configured to " + v
          + ", will use default value: " + defVal);
      v = defVal;
    }

    return v;
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
   * @param uri uri passed by caller
   * @param conf configuration
   * @return a credential provider
   * @throws IOException on any problem. Class construction issues may be
   * nested inside the IOE.
   */
  public static CredentialsProvider getCredentialsProvider(
      URI uri, Configuration conf) throws IOException {
    CredentialsProvider credentials;

    String className = conf.getTrimmed(CREDENTIALS_PROVIDER_KEY);
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
                  URI.class, Configuration.class).newInstance(uri, conf);
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
            "constructor.", className, CREDENTIALS_PROVIDER_KEY),
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

  /**
   * Check if OSS object represents a directory.
   *
   * @param name object key
   * @param size object content length
   * @return true if object represents a directory
   */
  public static boolean objectRepresentsDirectory(final String name,
      final long size) {
    return StringUtils.isNotEmpty(name) && name.endsWith("/") && size == 0L;
  }

  /**
   * Demand create the directory allocator, then create a temporary file.
   *  @param path prefix for the temporary file
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @return a unique temporary file
   *  @throws IOException IO problems
   */
  public static File createTmpFileForWrite(String path, long size,
      Configuration conf) throws IOException {
    if (conf.get(BUFFER_DIR_KEY) == null) {
      conf.set(BUFFER_DIR_KEY, conf.get("hadoop.tmp.dir") + "/oss");
    }
    if (directoryAllocator == null) {
      directoryAllocator = new LocalDirAllocator(BUFFER_DIR_KEY);
    }
    return directoryAllocator.createTmpFileForWrite(path, size, conf);
  }

  /**
   * Get a integer option >= the minimum allowed value.
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static int intOption(Configuration conf, String key, int defVal, int min) {
    int v = conf.getInt(key, defVal);
    Preconditions.checkArgument(v >= min,
        String.format("Value of %s: %d is below the minimum value %d",
            key, v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a long option >= the minimum allowed value.
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static long longOption(Configuration conf, String key, long defVal,
      long min) {
    long v = conf.getLong(key, defVal);
    Preconditions.checkArgument(v >= min,
        String.format("Value of %s: %d is below the minimum value %d",
            key, v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a size property from the configuration: this property must
   * be at least equal to {@link Constants#MULTIPART_MIN_SIZE}.
   * If it is too small, it is rounded up to that minimum, and a warning
   * printed.
   * @param conf configuration
   * @param property property name
   * @param defVal default value
   * @return the value, guaranteed to be above the minimum size
   */
  public static long getMultipartSizeProperty(Configuration conf,
      String property, long defVal) {
    long partSize = conf.getLong(property, defVal);
    if (partSize < MULTIPART_MIN_SIZE) {
      LOG.warn("{} must be at least 100 KB; configured value is {}",
          property, partSize);
      partSize = MULTIPART_MIN_SIZE;
    } else if (partSize > Integer.MAX_VALUE) {
      LOG.warn("oss: {} capped to ~2.14GB(maximum allowed size with " +
          "current output mechanism)", MULTIPART_UPLOAD_PART_SIZE_KEY);
      partSize = Integer.MAX_VALUE;
    }
    return partSize;
  }
}
