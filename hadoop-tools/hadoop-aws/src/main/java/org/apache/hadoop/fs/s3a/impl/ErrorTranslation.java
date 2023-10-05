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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_404_NOT_FOUND;

/**
 * Translate from AWS SDK-wrapped exceptions into IOExceptions with
 * as much information as possible.
 * The core of the translation logic is in S3AUtils, in
 * {@code translateException} and nearby; that has grown to be
 * a large a complex piece of logic, as it ties in with retry/recovery
 * policies, throttling, etc.
 *
 * This class is where future expansion of that code should go so that we have
 * an isolated place for all the changes..
 * The existing code las been left in S3AUtils it is to avoid cherry-picking
 * problems on backports.
 */
public class ErrorTranslation {

  /**
   * Private constructor for utility class.
   */
  private ErrorTranslation() {
  }

  /**
   * Does this exception indicate that the AWS Bucket was unknown.
   * @param e exception.
   * @return true if the status code and error code mean that the
   * remote bucket is unknown.
   */
  public static boolean isUnknownBucket(AwsServiceException e) {
    return e.statusCode() == SC_404_NOT_FOUND
        && AwsErrorCodes.E_NO_SUCH_BUCKET.equals(e.awsErrorDetails().errorCode());
  }

  /**
   * Does this exception indicate that a reference to an object
   * returned a 404. Unknown bucket errors do not match this
   * predicate.
   * @param e exception.
   * @return true if the status code and error code mean that the
   * HEAD request returned 404 but the bucket was there.
   */
  public static boolean isObjectNotFound(AwsServiceException e) {
    return e.statusCode() == SC_404_NOT_FOUND && !isUnknownBucket(e);
  }

  /**
   * Translate an exception if it or its inner exception is an
   * IOException.
   * If this condition is not met, null is returned.
   * @param path path of operation.
   * @param thrown exception
   * @return a translated exception or null.
   */
  public static IOException maybeExtractNetworkException(String path, Throwable thrown) {

    if (thrown == null) {
      return null;
    }

    // look inside
    Throwable cause = thrown.getCause();
    while(cause != null && cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (!(cause instanceof IOException)) {
      return null;
    }

    // the cause can be extracted to an IOE.
    // rather than just return it, we try to preserve the stack trace
    // of the outer exception.
    IOException ioe = (IOException) cause;
    // now examine any IOE
    if (ioe instanceof UnknownHostException) {
      return (IOException) new UnknownHostException(thrown + ": " + ioe.getMessage())
          .initCause(thrown);
    }
    if (ioe instanceof NoRouteToHostException) {
      return (IOException) new NoRouteToHostException(thrown + ": " + ioe.getMessage())
          .initCause(thrown);
    }
    if (ioe instanceof ConnectException) {
      return (IOException) new ConnectException(thrown + ": " + ioe.getMessage())
          .initCause(thrown);
    }

    // currently not attempting to translate any other exception types.
    return null;

  }

  /**
   * AWS error codes explicitly recognized and processes specially;
   * kept in their own class for isolation.
   */
  public static final class AwsErrorCodes {

    /**
     * The AWS S3 error code used to recognize when a 404 means the bucket is
     * unknown.
     */
    public static final String E_NO_SUCH_BUCKET = "NoSuchBucket";

    /** private constructor. */
    private AwsErrorCodes() {
    }
  }
}
