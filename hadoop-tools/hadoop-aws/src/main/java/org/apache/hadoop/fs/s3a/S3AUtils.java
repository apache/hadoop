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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.security.ProviderUtils;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

/**
 * Utility methods for S3A code.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3AUtils {

  private S3AUtils() {
  }

  /**
   * Translate an exception raised in an operation into an IOException.
   * The specific type of IOException depends on the class of
   * {@link AmazonClientException} passed in, and any status codes included
   * in the operation. That is: HTTP error codes are examined and can be
   * used to build a more specific response.
   * @param operation operation
   * @param path path operated on (must not be null)
   * @param exception amazon exception raised
   * @return an IOE which wraps the caught exception.
   */
  public static IOException translateException(String operation,
      Path path,
      AmazonClientException exception) {
    return translateException(operation, path.toString(), exception);
  }

  /**
   * Translate an exception raised in an operation into an IOException.
   * The specific type of IOException depends on the class of
   * {@link AmazonClientException} passed in, and any status codes included
   * in the operation. That is: HTTP error codes are examined and can be
   * used to build a more specific response.
   * @param operation operation
   * @param path path operated on (may be null)
   * @param exception amazon exception raised
   * @return an IOE which wraps the caught exception.
   */
  @SuppressWarnings("ThrowableInstanceNeverThrown")
  public static IOException translateException(String operation,
      String path,
      AmazonClientException exception) {
    String message = String.format("%s%s: %s",
        operation,
        path != null ? (" on " + path) : "",
        exception);
    if (!(exception instanceof AmazonServiceException)) {
      return new AWSClientIOException(message, exception);
    } else {

      IOException ioe;
      AmazonServiceException ase = (AmazonServiceException) exception;
      // this exception is non-null if the service exception is an s3 one
      AmazonS3Exception s3Exception = ase instanceof AmazonS3Exception
          ? (AmazonS3Exception) ase
          : null;
      int status = ase.getStatusCode();
      switch (status) {

      // permissions
      case 401:
      case 403:
        ioe = new AccessDeniedException(path, null, message);
        ioe.initCause(ase);
        break;

      // the object isn't there
      case 404:
      case 410:
        ioe = new FileNotFoundException(message);
        ioe.initCause(ase);
        break;

      // out of range. This may happen if an object is overwritten with
      // a shorter one while it is being read.
      case 416:
        ioe = new EOFException(message);
        break;

      default:
        // no specific exit code. Choose an IOE subclass based on the class
        // of the caught exception
        ioe = s3Exception != null
            ? new AWSS3IOException(message, s3Exception)
            : new AWSServiceIOException(message, ase);
        break;
      }
      return ioe;
    }
  }

  /**
   * Extract an exception from a failed future, and convert to an IOE.
   * @param operation operation which failed
   * @param path path operated on (may be null)
   * @param ee execution exception
   * @return an IOE which can be thrown
   */
  public static IOException extractException(String operation,
      String path,
      ExecutionException ee) {
    IOException ioe;
    Throwable cause = ee.getCause();
    if (cause instanceof AmazonClientException) {
      ioe = translateException(operation, path, (AmazonClientException) cause);
    } else if (cause instanceof IOException) {
      ioe = (IOException) cause;
    } else {
      ioe = new IOException(operation + " failed: " + cause, cause);
    }
    return ioe;
  }

  /**
   * Get low level details of an amazon exception for logging; multi-line.
   * @param e exception
   * @return string details
   */
  public static String stringify(AmazonServiceException e) {
    StringBuilder builder = new StringBuilder(
        String.format("%s: %s error %d: %s; %s%s%n",
            e.getErrorType(),
            e.getServiceName(),
            e.getStatusCode(),
            e.getErrorCode(),
            e.getErrorMessage(),
            (e.isRetryable() ? " (retryable)": "")
        ));
    String rawResponseContent = e.getRawResponseContent();
    if (rawResponseContent != null) {
      builder.append(rawResponseContent);
    }
    return builder.toString();
  }

  /**
   * Get low level details of an amazon exception for logging; multi-line.
   * @param e exception
   * @return string details
   */
  public static String stringify(AmazonS3Exception e) {
    // get the low level details of an exception,
    StringBuilder builder = new StringBuilder(
        stringify((AmazonServiceException) e));
    Map<String, String> details = e.getAdditionalDetails();
    if (details != null) {
      builder.append('\n');
      for (Map.Entry<String, String> d : details.entrySet()) {
        builder.append(d.getKey()).append('=')
            .append(d.getValue()).append('\n');
      }
    }
    return builder.toString();
  }

  /**
   * Create a files status instance from a listing.
   * @param keyPath path to entry
   * @param summary summary from AWS
   * @param blockSize block size to declare.
   * @return a status entry
   */
  public static S3AFileStatus createFileStatus(Path keyPath,
      S3ObjectSummary summary,
      long blockSize) {
    if (objectRepresentsDirectory(summary.getKey(), summary.getSize())) {
      return new S3AFileStatus(true, true, keyPath);
    } else {
      return new S3AFileStatus(summary.getSize(),
          dateToLong(summary.getLastModified()), keyPath,
          blockSize);
    }
  }

  /**
   * Predicate: does the object represent a directory?.
   * @param name object name
   * @param size object size
   * @return true if it meets the criteria for being an object
   */
  public static boolean objectRepresentsDirectory(final String name,
      final long size) {
    return !name.isEmpty()
        && name.charAt(name.length() - 1) == '/'
        && size == 0L;
  }

  /**
   * Date to long conversion.
   * Handles null Dates that can be returned by AWS by returning 0
   * @param date date from AWS query
   * @return timestamp of the object
   */
  public static long dateToLong(final Date date) {
    if (date == null) {
      return 0L;
    }

    return date.getTime();
  }

  /**
   * Return the access key and secret for S3 API use.
   * Credentials may exist in configuration, within credential providers
   * or indicated in the UserInfo of the name URI param.
   * @param name the URI for which we need the access keys.
   * @param conf the Configuration object to interrogate for keys.
   * @return AWSAccessKeys
   * @throws IOException problems retrieving passwords from KMS.
   */
  public static S3xLoginHelper.Login getAWSAccessKeys(URI name,
      Configuration conf) throws IOException {
    S3xLoginHelper.Login login =
        S3xLoginHelper.extractLoginDetailsWithWarnings(name);
    Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
        conf, S3AFileSystem.class);
    String accessKey = getPassword(c, ACCESS_KEY, login.getUser());
    String secretKey = getPassword(c, SECRET_KEY, login.getPassword());
    return new S3xLoginHelper.Login(accessKey, secretKey);
  }

  private static String getPassword(Configuration conf, String key, String val)
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
}
