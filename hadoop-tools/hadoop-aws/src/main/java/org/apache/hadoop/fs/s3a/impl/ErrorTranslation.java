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
import java.lang.reflect.Constructor;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.s3a.HttpChannelEOFException;
import org.apache.hadoop.fs.PathIOException;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
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
public final class ErrorTranslation {

  /**
   * OpenSSL stream closed error: {@value}.
   * See HADOOP-19027.
   */
  public static final String OPENSSL_STREAM_CLOSED = "WFOPENSSL0035";

  /**
   * Classname of unshaded Http Client exception: {@value}.
   */
  private static final String RAW_NO_HTTP_RESPONSE_EXCEPTION =
      "org.apache.http.NoHttpResponseException";

  /**
   * Classname of shaded Http Client exception: {@value}.
   */
  private static final String SHADED_NO_HTTP_RESPONSE_EXCEPTION =
      "software.amazon.awssdk.thirdparty.org.apache.http.NoHttpResponseException";

  /**
   * S3 encryption client exception class name: {@value}.
   */
  private static final String S3_ENCRYPTION_CLIENT_EXCEPTION =
      "software.amazon.encryption.s3.S3EncryptionClientException";

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
   * Tail recursive extraction of the innermost throwable.
   * @param thrown next thrown in chain.
   * @param outer outermost.
   * @return the last non-null throwable in the chain.
   */
  private static Throwable getInnermostThrowable(Throwable thrown, Throwable outer) {
    if (thrown == null) {
      return outer;
    }
    return getInnermostThrowable(thrown.getCause(), thrown);
  }

  /**
   * Attempts to extract the underlying SdkException from an S3 encryption client exception.
   *
   * <p>This method is designed to handle exceptions that may be wrapped within
   * S3EncryptionClientExceptions. It performs the following steps:
   * <ol>
   *   <li>Checks if the input exception is null.</li>
   *   <li>Verifies if the exception contains the S3EncryptionClientException signature.</li>
   *   <li>Examines the cause chain to find the most relevant SdkException.</li>
   * </ol>
   *
   * <p>The method aims to unwrap nested exceptions to provide more meaningful
   * error information, particularly in the context of S3 encryption operations.
   *
   * @param exception The SdkException to analyze. This may be a wrapper exception
   *                  containing a more specific underlying cause.
   * @return The extracted SdkException if found within the exception chain,
   *         or the original exception if no relevant nested exception is found.
   *         Returns null if the input exception is null.
   *
   * @see SdkException
   * @see AwsServiceException
   */
  public static SdkException maybeProcessEncryptionClientException(
      SdkException exception) {
    if (exception == null) {
      return null;
    }

    // check if the exception contains S3EncryptionClientException
    if (!exception.toString().contains(S3_ENCRYPTION_CLIENT_EXCEPTION)) {
      return exception;
    }

    Throwable cause = exception.getCause();
    if (!(cause instanceof SdkException)) {
      return exception;
    }

    // get the actual sdk exception.
    SdkException sdkCause = (SdkException) cause;
    if (sdkCause.getCause() instanceof AwsServiceException) {
      return (SdkException) sdkCause.getCause();
    }

    return sdkCause;
  }

  /**
   * Translate an exception if it or its inner exception is an
   * IOException.
   * This also contains the logic to extract an AWS HTTP channel exception,
   * which may or may not be an IOE, depending on the underlying SSL implementation
   * in use.
   * If an IOException cannot be extracted, null is returned.
   * @param path path of operation.
   * @param thrown exception
   * @param message message generated by the caller.
   * @return a translated exception or null.
   */
  public static IOException maybeExtractIOException(
      String path,
      Throwable thrown,
      String message) {

    if (thrown == null) {
      return null;
    }

    // walk down the chain of exceptions to find the innermost.
    Throwable cause = getInnermostThrowable(thrown.getCause(), thrown);

    // see if this is an http channel exception
    HttpChannelEOFException channelException =
        maybeExtractChannelException(path, message, cause);
    if (channelException != null) {
      return channelException;
    }

    // not a channel exception, not an IOE.
    if (!(cause instanceof IOException)) {
      return null;
    }

    // the cause can be extracted to an IOE.
    // rather than just return it, we try to preserve the stack trace
    // of the outer exception.
    // as a new instance is created through reflection, the
    // class of the returned instance will be that of the innermost,
    // unless no suitable constructor is available.
    final IOException ioe = (IOException) cause;

    return wrapWithInnerIOE(path, message, thrown, ioe);
  }

  /**
   * Given an outer and an inner exception, create a new IOE
   * of the inner type, with the outer exception as the cause.
   * The message is derived from both.
   * This only works if the inner exception has a constructor which
   * takes a string; if not a PathIOException is created.
   * <p>
   * See {@code NetUtils}.
   * @param <T> type of inner exception.
   * @param path path of the failure.
   * @param message message generated by the caller.
   * @param outer outermost exception.
   * @param inner inner exception.
   * @return the new exception.
   */
  @SuppressWarnings("unchecked")
  private static <T extends IOException> IOException wrapWithInnerIOE(
      String path,
      String message,
      Throwable outer,
      T inner) {
    String msg = (isNotEmpty(message) ? (message  + ":"
        + "    ") : "")
        + outer.toString() + ": " + inner.getMessage();
    Class<? extends Throwable> clazz = inner.getClass();
    try {
      Constructor<? extends Throwable> ctor = clazz.getConstructor(String.class);
      Throwable t = ctor.newInstance(msg);
      return (T) (t.initCause(outer));
    } catch (Throwable e) {
      return new PathIOException(path, msg, outer);
    }
  }

  /**
   * Extract an AWS HTTP channel exception if the inner exception is considered
   * an HttpClient {@code NoHttpResponseException} or an OpenSSL channel exception.
   * This is based on string matching, which is inelegant and brittle.
   * @param path path of the failure.
   * @param message message generated by the caller.
   * @param thrown inner exception.
   * @return the new exception.
   */
  @VisibleForTesting
  public static HttpChannelEOFException maybeExtractChannelException(
      String path,
      String message,
      Throwable thrown) {
    final String classname = thrown.getClass().getName();
    if (thrown instanceof IOException
        && (classname.equals(RAW_NO_HTTP_RESPONSE_EXCEPTION)
        || classname.equals(SHADED_NO_HTTP_RESPONSE_EXCEPTION))) {
      // shaded or unshaded http client exception class
      return new HttpChannelEOFException(path, message, thrown);
    }
    // there's ambiguity about what exception class this is
    // so rather than use its type, we look for an OpenSSL string in the message
    if (thrown.getMessage().contains(OPENSSL_STREAM_CLOSED)) {
      return new HttpChannelEOFException(path, message, thrown);
    }
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
