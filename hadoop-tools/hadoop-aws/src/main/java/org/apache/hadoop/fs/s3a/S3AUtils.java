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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryUtils;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.impl.S3AEncryption;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteException;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.fs.s3a.AWSCredentialProviderList.maybeTranslateCredentialException;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.audit.AuditIntegration.maybeTranslateAuditException;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.isUnknownBucket;
import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.instantiationException;
import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.isAbstract;
import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.isNotInstanceOf;
import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.unsupportedConstructor;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.*;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.maybeExtractIOException;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.util.functional.RemoteIterators.filteringRemoteIterator;

/**
 * Utility methods for S3A code.
 * Some methods are marked LimitedPrivate since they are being used in an
 * external project.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3AUtils {

  private static final Logger LOG = LoggerFactory.getLogger(S3AUtils.class);

  static final String ENDPOINT_KEY = "Endpoint";

  /** Filesystem is closed; kept here to keep the errors close. */
  static final String E_FS_CLOSED = "FileSystem is closed!";

  /**
   * Core property for provider path. Duplicated here for consistent
   * code across Hadoop version: {@value}.
   */
  static final String CREDENTIAL_PROVIDER_PATH =
      "hadoop.security.credential.provider.path";

  /**
   * Encryption SSE-C used but the config lacks an encryption key.
   */
  public static final String SSE_C_NO_KEY_ERROR =
      S3AEncryptionMethods.SSE_C.getMethod()
          + " is enabled but no encryption key was declared in "
          + Constants.S3_ENCRYPTION_KEY;
  /**
   * Encryption SSE-S3 is used but the caller also set an encryption key.
   */
  public static final String SSE_S3_WITH_KEY_ERROR =
      S3AEncryptionMethods.SSE_S3.getMethod()
          + " is enabled but an encryption key was set in "
          + Constants.S3_ENCRYPTION_KEY;
  public static final String EOF_MESSAGE_IN_XML_PARSER
      = "Failed to sanitize XML document destined for handler class";

  public static final String EOF_READ_DIFFERENT_LENGTH
      = "Data read has a different length than the expected";

  private static final String BUCKET_PATTERN = FS_S3A_BUCKET_PREFIX + "%s.%s";

  private S3AUtils() {
  }

  /**
   * Translate an exception raised in an operation into an IOException.
   * The specific type of IOException depends on the class of
   * {@link SdkException} passed in, and any status codes included
   * in the operation. That is: HTTP error codes are examined and can be
   * used to build a more specific response.
   *
   * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">S3 Error responses</a>
   * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/ErrorBestPractices.html">Amazon S3 Error Best Practices</a>
   * @param operation operation
   * @param path path operated on (must not be null)
   * @param exception amazon exception raised
   * @return an IOE which wraps the caught exception.
   */
  public static IOException translateException(String operation,
      Path path,
      SdkException exception) {
    return translateException(operation, path.toString(), exception);
  }

  /**
   * Translate an exception raised in an operation into an IOException.
   * The specific type of IOException depends on the class of
   * {@link SdkException} passed in, and any status codes included
   * in the operation. That is: HTTP error codes are examined and can be
   * used to build a more specific response.
   * @param operation operation
   * @param path path operated on (may be null)
   * @param exception amazon exception raised
   * @return an IOE which wraps the caught exception.
   */
  @SuppressWarnings("ThrowableInstanceNeverThrown")
  public static IOException translateException(@Nullable String operation,
      @Nullable String path,
      SdkException exception) {
    String message = String.format("%s%s: %s",
        operation,
        StringUtils.isNotEmpty(path)? (" on " + path) : "",
        exception);

    if (path == null || path.isEmpty()) {
      // handle null path by giving it a stub value.
      // not ideal/informative, but ensures that the path is never null in
      // exceptions constructed.
      path = "/";
    }

    if (!(exception instanceof AwsServiceException)) {
      // exceptions raised client-side: connectivity, auth, network problems...
      Exception innerCause = containsInterruptedException(exception);
      if (innerCause != null) {
        // interrupted IO, or a socket exception underneath that class
        return translateInterruptedException(exception, innerCause, message);
      }
      if (isMessageTranslatableToEOF(exception)) {
        // call considered an sign of connectivity failure
        return (EOFException)new EOFException(message).initCause(exception);
      }
      // if the exception came from the auditor, hand off translation
      // to it.
      IOException ioe = maybeTranslateAuditException(path, exception);
      if (ioe != null) {
        return ioe;
      }
      ioe = maybeTranslateCredentialException(path, exception);
      if (ioe != null) {
        return ioe;
      }
      // network problems covered by an IOE inside the exception chain.
      ioe = maybeExtractIOException(path, exception, message);
      if (ioe != null) {
        return ioe;
      }
      // timeout issues
      // ApiCallAttemptTimeoutException: a single HTTP request attempt failed.
      // ApiCallTimeoutException: a request with any configured retries failed.
      // The ApiCallTimeoutException exception should be the only one seen in
      // the S3A code, but for due diligence both are handled and mapped to
      // our own AWSApiCallTimeoutException.
      if (exception instanceof ApiCallTimeoutException
          || exception instanceof ApiCallAttemptTimeoutException) {
        // An API call to an AWS service timed out.
        // This is a subclass of ConnectTimeoutException so
        // all retry logic for that exception is handled without
        // having to look down the stack for a
        return new AWSApiCallTimeoutException(message, exception);
      }
      // no custom handling.
      return new AWSClientIOException(message, exception);
    } else {
      // "error response returned by an S3 or other service."
      // These contain more details and should be translated based
      // on the HTTP status code and other details.
      IOException ioe;
      AwsServiceException ase = (AwsServiceException) exception;
      // this exception is non-null if the service exception is an s3 one
      S3Exception s3Exception = ase instanceof S3Exception
          ? (S3Exception) ase
          : null;
      int status = ase.statusCode();
      if (ase.awsErrorDetails() != null) {
        message = message + ":" + ase.awsErrorDetails().errorCode();
      }

      // big switch on the HTTP status code.
      switch (status) {

      case SC_301_MOVED_PERMANENTLY:
      case SC_307_TEMPORARY_REDIRECT:
        if (s3Exception != null) {
          message = String.format("Received permanent redirect response to "
                  + "region %s.  This likely indicates that the S3 region "
                  + "configured in %s does not match the AWS region containing " + "the bucket.",
              s3Exception.awsErrorDetails().sdkHttpResponse().headers().get(BUCKET_REGION_HEADER),
              AWS_REGION);
          ioe = new AWSRedirectException(message, s3Exception);
        } else {
          ioe = new AWSRedirectException(message, ase);
        }
        break;

      case SC_400_BAD_REQUEST:
        ioe = new AWSBadRequestException(message, ase);
        break;

      // permissions
      case SC_401_UNAUTHORIZED:
      case SC_403_FORBIDDEN:
        ioe = new AccessDeniedException(path, null, message);
        ioe.initCause(ase);
        break;

      // the object isn't there
      case SC_404_NOT_FOUND:
        if (isUnknownBucket(ase)) {
          // this is a missing bucket
          ioe = new UnknownStoreException(path, message, ase);
        } else {
          // a normal unknown object.
          // Can also be raised by third-party stores when aborting an unknown multipart upload
          ioe = new FileNotFoundException(message);
          ioe.initCause(ase);
        }
        break;

      // Caused by duplicate create bucket call.
      case SC_409_CONFLICT:
        ioe = new AWSBadRequestException(message, ase);
        break;

      // this also surfaces sometimes and is considered to
      // be ~ a not found exception.
      case SC_410_GONE:
        ioe = new FileNotFoundException(message);
        ioe.initCause(ase);
        break;

      // errors which stores can return from requests which
      // the store does not support.
      case SC_405_METHOD_NOT_ALLOWED:
      case SC_415_UNSUPPORTED_MEDIA_TYPE:
      case SC_501_NOT_IMPLEMENTED:
        ioe = new AWSUnsupportedFeatureException(message, ase);
        break;

      // precondition failure: the object is there, but the precondition
      // (e.g. etag) didn't match. Assume remote file change during
      // rename or status passed in to openfile had an etag which didn't match.
      case SC_412_PRECONDITION_FAILED:
        ioe = new RemoteFileChangedException(path, message, "", ase);
        break;

      // out of range. This may happen if an object is overwritten with
      // a shorter one while it is being read or openFile() was invoked
      // passing a FileStatus or file length less than that of the object.
      // although the HTTP specification says that the response should
      // include a range header specifying the actual range available,
      // this isn't picked up here.
      case SC_416_RANGE_NOT_SATISFIABLE:
        ioe = new RangeNotSatisfiableEOFException(message, ase);
        break;

      // this has surfaced as a "no response from server" message.
      // so rare we haven't replicated it.
      // Treating as an idempotent proxy error.
      case SC_443_NO_RESPONSE:
      case SC_444_NO_RESPONSE:
        ioe = new AWSNoResponseException(message, ase);
        break;

      // throttling
      case SC_429_TOO_MANY_REQUESTS_GCS:    // google cloud through this connector
      case SC_503_SERVICE_UNAVAILABLE:      // AWS
        ioe = new AWSServiceThrottledException(message, ase);
        break;

      // gateway timeout
      case SC_504_GATEWAY_TIMEOUT:
        ioe = new AWSApiCallTimeoutException(message, ase);
        break;

      // internal error
      case SC_500_INTERNAL_SERVER_ERROR:
        ioe = new AWSStatus500Exception(message, ase);
        break;

      case SC_200_OK:
        if (exception instanceof MultiObjectDeleteException) {
          // failure during a bulk delete
          return ((MultiObjectDeleteException) exception)
              .translateException(message);
        }
        // other 200: FALL THROUGH

      default:
        // no specifically handled exit code.

        // convert all unknown 500+ errors to a 500 exception
        if (status > SC_500_INTERNAL_SERVER_ERROR) {
          ioe = new AWSStatus500Exception(message, ase);
          break;
        }

        // Choose an IOE subclass based on the class of the caught exception
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
    return convertExceptionCause(operation, path, ee.getCause());
  }

  /**
   * Extract an exception from a failed future, and convert to an IOE.
   * @param operation operation which failed
   * @param path path operated on (may be null)
   * @param ce completion exception
   * @return an IOE which can be thrown
   */
  public static IOException extractException(String operation,
      String path,
      CompletionException ce) {
    return convertExceptionCause(operation, path, ce.getCause());
  }

  /**
   * Convert the cause of a concurrent exception to an IOE.
   * @param operation operation which failed
   * @param path path operated on (may be null)
   * @param cause cause of a concurrent exception
   * @return an IOE which can be thrown
   */
  private static IOException convertExceptionCause(String operation,
      String path,
      Throwable cause) {
    IOException ioe;
    if (cause instanceof SdkException) {
      ioe = translateException(operation, path, (SdkException) cause);
    } else if (cause instanceof IOException) {
      ioe = (IOException) cause;
    } else {
      ioe = new IOException(operation + " failed: " + cause, cause);
    }
    return ioe;
  }

  /**
   * Recurse down the exception loop looking for any inner details about
   * an interrupted exception.
   * @param thrown exception thrown
   * @return the actual exception if the operation was an interrupt
   */
  static Exception containsInterruptedException(Throwable thrown) {
    if (thrown == null) {
      return null;
    }
    if (thrown instanceof InterruptedException ||
        thrown instanceof InterruptedIOException ||
        thrown instanceof AbortedException) {
      return (Exception)thrown;
    }
    // tail recurse
    return containsInterruptedException(thrown.getCause());
  }

  /**
   * Handles translation of interrupted exception. This includes
   * preserving the class of the fault for better retry logic
   * @param exception outer exception
   * @param innerCause inner cause (which is guaranteed to be some form
   * of interrupted exception
   * @param message message for the new exception.
   * @return an IOE which can be rethrown
   */
  private static InterruptedIOException translateInterruptedException(
      SdkException exception,
      final Exception innerCause,
      String message) {
    InterruptedIOException ioe;
    if (innerCause instanceof SocketTimeoutException) {
      ioe = new SocketTimeoutException(message);
    } else {
      String name = innerCause.getClass().getName();
      if (name.endsWith(".ConnectTimeoutException")
          || name.endsWith(".ConnectionPoolTimeoutException")
          || name.endsWith("$ConnectTimeoutException")) {
        // TODO: review in v2
        // TCP connection http timeout from the shaded or unshaded filenames
        // com.amazonaws.thirdparty.apache.http.conn.ConnectTimeoutException
        ioe = new ConnectTimeoutException(message);
      } else {
        // any other exception
        ioe = new InterruptedIOException(message);
      }
    }
    ioe.initCause(exception);
    return ioe;
  }

  /**
   * Is the exception an instance of a throttling exception. That
   * is an AmazonServiceException with a 503 response, an
   * {@link AWSServiceThrottledException},
   * or anything which the AWS SDK's RetryUtils considers to be
   * a throttling exception.
   * @param ex exception to examine
   * @return true if it is considered a throttling exception
   */
  public static boolean isThrottleException(Exception ex) {
    return ex instanceof AWSServiceThrottledException
        || (ex instanceof AwsServiceException
            && 503  == ((AwsServiceException)ex).statusCode())
        || (ex instanceof SdkException
            && RetryUtils.isThrottlingException((SdkException) ex));
  }

  /**
   * Cue that an AWS exception is likely to be an EOF Exception based
   * on the message coming back from the client. This is likely to be
   * brittle, so only a hint.
   * @param ex exception
   * @return true if this is believed to be a sign the connection was broken.
   */
  public static boolean isMessageTranslatableToEOF(SdkException ex) {
    // TODO: review in v2
    return ex.toString().contains(EOF_MESSAGE_IN_XML_PARSER) ||
            ex.toString().contains(EOF_READ_DIFFERENT_LENGTH);
  }

  /**
   * Get low level details of an amazon exception for logging; multi-line.
   * @param e exception
   * @return string details
   */
  public static String stringify(AwsServiceException e) {
    StringBuilder builder = new StringBuilder(
        String.format("%s error %d: %s; %s%s%n",
            e.awsErrorDetails().serviceName(),
            e.statusCode(),
            e.awsErrorDetails().errorCode(),
            e.awsErrorDetails().errorMessage(),
            (e.retryable() ? " (retryable)": "")
        ));
    String rawResponseContent = e.awsErrorDetails().rawResponse().asUtf8String();
    if (rawResponseContent != null) {
      builder.append(rawResponseContent);
    }
    return builder.toString();
  }

  /**
   * Create a files status instance from a listing.
   * @param keyPath path to entry
   * @param s3Object s3Object entry
   * @param blockSize block size to declare.
   * @param owner owner of the file
   * @param eTag S3 object eTag or null if unavailable
   * @param versionId S3 object versionId or null if unavailable
   * @param size s3 object size
   * @return a status entry
   */
  public static S3AFileStatus createFileStatus(Path keyPath,
      S3Object s3Object,
      long blockSize,
      String owner,
      String eTag,
      String versionId,
      long size) {
    return createFileStatus(keyPath,
        objectRepresentsDirectory(s3Object.key()),
        size, Date.from(s3Object.lastModified()), blockSize, owner, eTag, versionId);
  }

  /**
   * Create a file status for object we just uploaded.  For files, we use
   * current time as modification time, since s3a uses S3's service-based
   * modification time, which will not be available until we do a
   * getFileStatus() later on.
   * @param keyPath path for created object
   * @param isDir true iff directory
   * @param size file length
   * @param blockSize block size for file status
   * @param owner Hadoop username
   * @param eTag S3 object eTag or null if unavailable
   * @param versionId S3 object versionId or null if unavailable
   * @return a status entry
   */
  public static S3AFileStatus createUploadFileStatus(Path keyPath,
      boolean isDir, long size, long blockSize, String owner,
      String eTag, String versionId) {
    Date date = isDir ? null : new Date();
    return createFileStatus(keyPath, isDir, size, date, blockSize, owner,
        eTag, versionId);
  }

  /* Date 'modified' is ignored when isDir is true. */
  private static S3AFileStatus createFileStatus(Path keyPath, boolean isDir,
      long size, Date modified, long blockSize, String owner,
      String eTag, String versionId) {
    if (isDir) {
      return new S3AFileStatus(Tristate.UNKNOWN, keyPath, owner);
    } else {
      return new S3AFileStatus(size, dateToLong(modified), keyPath, blockSize,
          owner, eTag, versionId);
    }
  }

  /**
   * Predicate: does the object represent a directory?.
   * @param name object name
   * @return true if it meets the criteria for being an object
   */
  public static boolean objectRepresentsDirectory(final String name) {
    return !name.isEmpty()
        && name.charAt(name.length() - 1) == '/';
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
   * Creates an instance of a class using reflection. The
   * class must implement one of the following means of construction, which are
   * attempted in order:
   *
   * <ol>
   * <li>a public constructor accepting java.net.URI and
   *     org.apache.hadoop.conf.Configuration</li>
   * <li>a public constructor accepting
   *    org.apache.hadoop.conf.Configuration</li>
   * <li>a public static method named as per methodName, that accepts no
   *    arguments and returns an instance of
   *    specified type, or</li>
   * <li>a public default constructor.</li>
   * </ol>
   *
   * @param className name of class for which instance is to be created
   * @param conf configuration
   * @param uri URI of the FS
   * @param interfaceImplemented interface that this class implements
   * @param methodName name of factory method to be invoked
   * @param configKey config key under which this class is specified
   * @param <InstanceT> Instance of class
   * @return instance of the specified class
   * @throws IOException on any problem
   */
  @SuppressWarnings("unchecked")
  public static <InstanceT> InstanceT getInstanceFromReflection(String className,
      Configuration conf,
      @Nullable URI uri,
      Class<? extends InstanceT> interfaceImplemented,
      String methodName,
      String configKey) throws IOException {
    try {
      Class<?> instanceClass = S3AUtils.class.getClassLoader().loadClass(className);
      if (Modifier.isAbstract(instanceClass.getModifiers())) {
        throw isAbstract(uri, className, configKey);
      }
      if (!interfaceImplemented.isAssignableFrom(instanceClass)) {
        throw isNotInstanceOf(uri, className, interfaceImplemented.getName(), configKey);

      }
      Constructor cons;
      if (conf != null) {
        // new X(uri, conf)
        cons = getConstructor(instanceClass, URI.class, Configuration.class);

        if (cons != null) {
          return (InstanceT) cons.newInstance(uri, conf);
        }
        // new X(conf)
        cons = getConstructor(instanceClass, Configuration.class);
        if (cons != null) {
          return (InstanceT) cons.newInstance(conf);
        }
      }

      // X.methodName()
      Method factory = getFactoryMethod(instanceClass, interfaceImplemented, methodName);
      if (factory != null) {
        return (InstanceT) factory.invoke(null);
      }

      // new X()
      cons = getConstructor(instanceClass);
      if (cons != null) {
        return (InstanceT) cons.newInstance();
      }

      // no supported constructor or factory method found
      throw unsupportedConstructor(uri, className, configKey);
    } catch (InvocationTargetException e) {
      Throwable targetException = e.getTargetException();
      if (targetException == null) {
        targetException = e;
      }
      if (targetException instanceof IOException) {
        throw (IOException) targetException;
      } else if (targetException instanceof SdkException) {
        throw translateException("Instantiate " + className, "/", (SdkException) targetException);
      } else {
        // supported constructor or factory method found, but the call failed
        throw instantiationException(uri, className, configKey, targetException);
      }
    } catch (ReflectiveOperationException | IllegalArgumentException e) {
      // supported constructor or factory method found, but the call failed
      throw instantiationException(uri, className, configKey, e);
    }
  }


  /**
   * Set a key if the value is non-empty.
   * @param config config to patch
   * @param key key to set
   * @param val value to probe and set
   * @param origin origin
   * @return true if the property was set
   */
  public static boolean setIfDefined(Configuration config, String key,
      String val, String origin) {
    if (StringUtils.isNotEmpty(val)) {
      config.set(key, val, origin);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Return the access key and secret for S3 API use.
   * or indicated in the UserInfo of the name URI param.
   * @param name the URI for which we need the access keys; may be null
   * @param conf the Configuration object to interrogate for keys.
   * @return AWSAccessKeys
   * @throws IOException problems retrieving passwords from KMS.
   */
  public static S3xLoginHelper.Login getAWSAccessKeys(URI name,
      Configuration conf) throws IOException {
    S3xLoginHelper.rejectSecretsInURIs(name);
    Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
        conf, S3AFileSystem.class);
    String bucket = name != null ? name.getHost() : "";

    // get the secrets from the configuration

    // get the access key
    String accessKey = lookupPassword(bucket, c, ACCESS_KEY);

    // and the secret
    String secretKey = lookupPassword(bucket, c, SECRET_KEY);

    return new S3xLoginHelper.Login(accessKey, secretKey);
  }

  /**
   * Get a password from a configuration, including JCEKS files, handling both
   * the absolute key and bucket override.
   * @param bucket bucket or "" if none known
   * @param conf configuration
   * @param baseKey base key to look up, e.g "fs.s3a.secret.key"
   * @param overrideVal override value: if non empty this is used instead of
   * querying the configuration.
   * @return a password or "".
   * @throws IOException on any IO problem
   * @throws IllegalArgumentException bad arguments
   */
  @Deprecated
  public static String lookupPassword(
      String bucket,
      Configuration conf,
      String baseKey,
      String overrideVal)
      throws IOException {
    return lookupPassword(bucket, conf, baseKey, overrideVal, "");
  }

  /**
   * Get a password from a configuration, including JCEKS files, handling both
   * the absolute key and bucket override.
   * @param bucket bucket or "" if none known
   * @param conf configuration
   * @param baseKey base key to look up, e.g "fs.s3a.secret.key"
   * @return a password or "".
   * @throws IOException on any IO problem
   * @throws IllegalArgumentException bad arguments
   */
  public static String lookupPassword(
      String bucket,
      Configuration conf,
      String baseKey)
      throws IOException {
    return lookupPassword(bucket, conf, baseKey, null, "");
  }

  /**
   * Get a password from a configuration, including JCEKS files, handling both
   * the absolute key and bucket override.
   * <br>
   * <i>Note:</i> LimitedPrivate for ranger repository to get secrets.
   * @param bucket bucket or "" if none known
   * @param conf configuration
   * @param baseKey base key to look up, e.g "fs.s3a.secret.key"
   * @param overrideVal override value: if non empty this is used instead of
   * querying the configuration.
   * @param defVal value to return if there is no password
   * @return a password or the value of defVal.
   * @throws IOException on any IO problem
   * @throws IllegalArgumentException bad arguments
   */
  @InterfaceAudience.LimitedPrivate("Ranger")
  public static String lookupPassword(
      String bucket,
      Configuration conf,
      String baseKey,
      String overrideVal,
      String defVal)
      throws IOException {
    String initialVal;
    Preconditions.checkArgument(baseKey.startsWith(FS_S3A_PREFIX),
        "%s does not start with $%s", baseKey, FS_S3A_PREFIX);
    // if there's a bucket, work with it
    if (StringUtils.isNotEmpty(bucket)) {
      String subkey = baseKey.substring(FS_S3A_PREFIX.length());
      String shortBucketKey = String.format(
          BUCKET_PATTERN, bucket, subkey);
      String longBucketKey = String.format(
          BUCKET_PATTERN, bucket, baseKey);

      // set from the long key unless overidden.
      initialVal = getPassword(conf, longBucketKey, overrideVal);
      // then override from the short one if it is set
      initialVal = getPassword(conf, shortBucketKey, initialVal);
    } else {
      // no bucket, make the initial value the override value
      initialVal = overrideVal;
    }
    return getPassword(conf, baseKey, initialVal, defVal);
  }

  /**
   * Get a password from a configuration, or, if a value is passed in,
   * pick that up instead.
   * @param conf configuration
   * @param key key to look up
   * @param val current value: if non empty this is used instead of
   * querying the configuration.
   * @return a password or "".
   * @throws IOException on any problem
   */
  private static String getPassword(Configuration conf, String key, String val)
      throws IOException {
    return getPassword(conf, key, val, "");
  }

  /**
   * Get a password from a configuration, or, if a value is passed in,
   * pick that up instead.
   * @param conf configuration
   * @param key key to look up
   * @param val current value: if non empty this is used instead of
   * querying the configuration.
   * @param defVal default value if nothing is set
   * @return a password or "".
   * @throws IOException on any problem
   */
  private static String getPassword(Configuration conf,
      String key,
      String val,
      String defVal) throws IOException {
    return isEmpty(val)
        ? lookupPassword(conf, key, defVal)
        : val;
  }

  /**
   * Get a password from a configuration/configured credential providers.
   * @param conf configuration
   * @param key key to look up
   * @param defVal value to return if there is no password
   * @return a password or the value in {@code defVal}
   * @throws IOException on any problem
   */
  static String lookupPassword(Configuration conf, String key, String defVal)
      throws IOException {
    try {
      final char[] pass = conf.getPassword(key);
      return pass != null ?
          new String(pass).trim()
          : defVal;
    } catch (IOException ioe) {
      throw new IOException("Cannot find password option " + key, ioe);
    }
  }

  /**
   * String information about a summary entry for debug messages.
   * @param s3Object s3Object entry
   * @return string value
   */
  public static String stringify(S3Object s3Object) {
    StringBuilder builder = new StringBuilder(s3Object.key().length() + 100);
    builder.append("\"").append(s3Object.key()).append("\" ");
    builder.append("size=").append(s3Object.size());
    return builder.toString();
  }

  /**
   * Get a integer option &gt;= the minimum allowed value.
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  public static int intOption(Configuration conf, String key, int defVal, int min) {
    int v = conf.getInt(key, defVal);
    Preconditions.checkArgument(v >= min,
        String.format("Value of %s: %d is below the minimum value %d",
            key, v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a long option &gt;= the minimum allowed value.
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  public static long longOption(Configuration conf,
      String key,
      long defVal,
      long min) {
    long v = conf.getLong(key, defVal);
    Preconditions.checkArgument(v >= min,
        String.format("Value of %s: %d is below the minimum value %d",
            key, v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a long option &gt;= the minimum allowed value, supporting memory
   * prefixes K,M,G,T,P.
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  public static long longBytesOption(Configuration conf,
                             String key,
                             long defVal,
                             long min) {
    long v = conf.getLongBytes(key, defVal);
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
    long partSize = conf.getLongBytes(property, defVal);
    if (partSize < MULTIPART_MIN_SIZE) {
      LOG.warn("{} must be at least 5 MB; configured value is {}",
          property, partSize);
      partSize = MULTIPART_MIN_SIZE;
    }
    return partSize;
  }

  /**
   * Validates the output stream configuration.
   * @param path path: for error messages
   * @param conf : configuration object for the given context
   * @throws PathIOException Unsupported configuration.
   */
  public static void validateOutputStreamConfiguration(final Path path,
      Configuration conf) throws PathIOException {
    if(!checkDiskBuffer(conf)){
      throw new PathIOException(path.toString(),
          "Unable to create OutputStream with the given"
          + " multipart upload and buffer configuration.");
    }
  }

  /**
   * Check whether the configuration for S3ABlockOutputStream is
   * consistent or not. Multipart uploads allow all kinds of fast buffers to
   * be supported. When the option is disabled only disk buffers are allowed to
   * be used as the file size might be bigger than the buffer size that can be
   * allocated.
   * @param conf : configuration object for the given context
   * @return true if the disk buffer and the multipart settings are supported
   */
  public static boolean checkDiskBuffer(Configuration conf) {
    boolean isMultipartUploadEnabled = conf.getBoolean(MULTIPART_UPLOADS_ENABLED,
        DEFAULT_MULTIPART_UPLOAD_ENABLED);
    return isMultipartUploadEnabled
        || FAST_UPLOAD_BUFFER_DISK.equals(
            conf.get(FAST_UPLOAD_BUFFER, DEFAULT_FAST_UPLOAD_BUFFER));
  }

  /**
   * Ensure that the long value is in the range of an integer.
   * @param name property name for error messages
   * @param size original size
   * @return the size, guaranteed to be less than or equal to the max
   * value of an integer.
   */
  public static int ensureOutputParameterInRange(String name, long size) {
    if (size > Integer.MAX_VALUE) {
      LOG.warn("s3a: {} capped to ~2.14GB" +
          " (maximum allowed size with current output mechanism)", name);
      return Integer.MAX_VALUE;
    } else {
      return (int)size;
    }
  }

  /**
   * Returns the public constructor of {@code cl} specified by the list of
   * {@code args} or {@code null} if {@code cl} has no public constructor that
   * matches that specification.
   * @param cl class
   * @param args constructor argument types
   * @return constructor or null
   */
  private static Constructor<?> getConstructor(Class<?> cl, Class<?>... args) {
    try {
      Constructor cons = cl.getDeclaredConstructor(args);
      return Modifier.isPublic(cons.getModifiers()) ? cons : null;
    } catch (NoSuchMethodException | SecurityException e) {
      return null;
    }
  }

  /**
   * Returns the public static method of {@code cl} that accepts no arguments
   * and returns {@code returnType} specified by {@code methodName} or
   * {@code null} if {@code cl} has no public static method that matches that
   * specification.
   * @param cl class
   * @param returnType return type
   * @param methodName method name
   * @return method or null
   */
  private static Method getFactoryMethod(Class<?> cl, Class<?> returnType,
      String methodName) {
    try {
      Method m = cl.getDeclaredMethod(methodName);
      if (Modifier.isPublic(m.getModifiers()) &&
          Modifier.isStatic(m.getModifiers()) &&
          returnType.isAssignableFrom(m.getReturnType())) {
        return m;
      } else {
        return null;
      }
    } catch (NoSuchMethodException | SecurityException e) {
      return null;
    }
  }

  /**
   * Propagates bucket-specific settings into generic S3A configuration keys.
   * This is done by propagating the values of the form
   * {@code fs.s3a.bucket.${bucket}.key} to
   * {@code fs.s3a.key}, for all values of "key" other than a small set
   * of unmodifiable values.
   *
   * The source of the updated property is set to the key name of the bucket
   * property, to aid in diagnostics of where things came from.
   *
   * Returns a new configuration. Why the clone?
   * You can use the same conf for different filesystems, and the original
   * values are not updated.
   *
   * The {@code fs.s3a.impl} property cannot be set, nor can
   * any with the prefix {@code fs.s3a.bucket}.
   *
   * This method does not propagate security provider path information from
   * the S3A property into the Hadoop common provider: callers must call
   * {@link #patchSecurityCredentialProviders(Configuration)} explicitly.
   *
   * <br>
   * <i>Note:</i> LimitedPrivate for ranger repository to set up
   * per-bucket configurations.
   * @param source Source Configuration object.
   * @param bucket bucket name. Must not be empty.
   * @return a (potentially) patched clone of the original.
   */
  @InterfaceAudience.LimitedPrivate("Ranger")
  public static Configuration propagateBucketOptions(Configuration source,
      String bucket) {

    Preconditions.checkArgument(StringUtils.isNotEmpty(bucket), "bucket is null/empty");
    final String bucketPrefix = FS_S3A_BUCKET_PREFIX + bucket +'.';
    LOG.debug("Propagating entries under {}", bucketPrefix);
    final Configuration dest = new Configuration(source);
    for (Map.Entry<String, String> entry : source) {
      final String key = entry.getKey();
      // get the (unexpanded) value.
      final String value = entry.getValue();
      if (!key.startsWith(bucketPrefix) || bucketPrefix.equals(key)) {
        continue;
      }
      // there's a bucket prefix, so strip it
      final String stripped = key.substring(bucketPrefix.length());
      if (stripped.startsWith("bucket.") || "impl".equals(stripped)) {
        //tell user off
        LOG.debug("Ignoring bucket option {}", key);
      }  else {
        // propagate the value, building a new origin field.
        // to track overwrites, the generic key is overwritten even if
        // already matches the new one.
        String origin = "[" + StringUtils.join(
            source.getPropertySources(key), ", ") +"]";
        final String generic = FS_S3A_PREFIX + stripped;
        LOG.debug("Updating {} from {}", generic, origin);
        dest.set(generic, value, key + " via " + origin);
      }
    }
    return dest;
  }


  /**
   * Delete a path quietly: failures are logged at DEBUG.
   * @param fs filesystem
   * @param path path
   * @param recursive recursive?
   */
  public static void deleteQuietly(FileSystem fs,
      Path path,
      boolean recursive) {
    try {
      fs.delete(path, recursive);
    } catch (IOException e) {
      LOG.debug("Failed to delete {}", path, e);
    }
  }

  /**
   * Delete a path: failures are logged at WARN.
   * @param fs filesystem
   * @param path path
   * @param recursive recursive?
   */
  public static void deleteWithWarning(FileSystem fs,
      Path path,
      boolean recursive) {
    try {
      fs.delete(path, recursive);
    } catch (IOException e) {
      LOG.warn("Failed to delete {}", path, e);
    }
  }

  /**
   * Convert the data of an iterator of {@link S3AFileStatus} to
   * an array.
   * @param iterator a non-null iterator
   * @return a possibly-empty array of file status entries
   * @throws IOException failure
   */
  public static S3AFileStatus[] iteratorToStatuses(
      RemoteIterator<S3AFileStatus> iterator)
      throws IOException {
    S3AFileStatus[] statuses = RemoteIterators
        .toArray(iterator, new S3AFileStatus[0]);
    return statuses;
  }

  /**
   * Get the length of the PUT, verifying that the length is known.
   * @param putObjectRequest a request bound to a file or a stream.
   * @return the request length
   * @throws IllegalArgumentException if the length is negative
   */
  public static long getPutRequestLength(PutObjectRequest putObjectRequest) {
    long len = putObjectRequest.contentLength();

    Preconditions.checkState(len >= 0, "Cannot PUT object of unknown length");
    return len;
  }

  /**
   * An interface for use in lambda-expressions working with
   * directory tree listings.
   */
  @FunctionalInterface
  public interface CallOnLocatedFileStatus {
    void call(LocatedFileStatus status) throws IOException;
  }

  /**
   * An interface for use in lambda-expressions working with
   * directory tree listings.
   */
  @FunctionalInterface
  public interface LocatedFileStatusMap<T> {
    T call(LocatedFileStatus status) throws IOException;
  }

  /**
   * Apply an operation to every {@link LocatedFileStatus} in a remote
   * iterator.
   * @param iterator iterator from a list
   * @param eval closure to evaluate
   * @return the number of files processed
   * @throws IOException anything in the closure, or iteration logic.
   */
  public static long applyLocatedFiles(
      RemoteIterator<? extends LocatedFileStatus> iterator,
      CallOnLocatedFileStatus eval) throws IOException {
    return RemoteIterators.foreach(iterator, eval::call);
  }

  /**
   * Map an operation to every {@link LocatedFileStatus} in a remote
   * iterator, returning a list of the results.
   * @param <T> return type of map
   * @param iterator iterator from a list
   * @param eval closure to evaluate
   * @return the list of mapped results.
   * @throws IOException anything in the closure, or iteration logic.
   */
  public static <T> List<T> mapLocatedFiles(
      RemoteIterator<? extends LocatedFileStatus> iterator,
      LocatedFileStatusMap<T> eval) throws IOException {
    final List<T> results = new ArrayList<>();
    applyLocatedFiles(iterator,
        (s) -> results.add(eval.call(s)));
    return results;
  }

  /**
   * Map an operation to every {@link LocatedFileStatus} in a remote
   * iterator, returning a list of the all results which were not empty.
   * @param <T> return type of map
   * @param iterator iterator from a list
   * @param eval closure to evaluate
   * @return the flattened list of mapped results.
   * @throws IOException anything in the closure, or iteration logic.
   */
  public static <T> List<T> flatmapLocatedFiles(
      RemoteIterator<LocatedFileStatus> iterator,
      LocatedFileStatusMap<Optional<T>> eval) throws IOException {
    final List<T> results = new ArrayList<>();
    applyLocatedFiles(iterator,
        (s) -> eval.call(s).map(r -> results.add(r)));
    return results;
  }

  /**
   * List located files and filter them as a classic listFiles(path, filter)
   * would do.
   * This will be incremental, fetching pages async.
   * While it is rare for job to have many thousands of files, jobs
   * against versioned buckets may return earlier if there are many
   * non-visible objects.
   * @param fileSystem filesystem
   * @param path path to list
   * @param recursive recursive listing?
   * @param filter filter for the filename
   * @return interator over the entries.
   * @throws IOException IO failure.
   */
  public static RemoteIterator<LocatedFileStatus> listAndFilter(FileSystem fileSystem,
      Path path, boolean recursive, PathFilter filter) throws IOException {
    return filteringRemoteIterator(
        fileSystem.listFiles(path, recursive),
        status -> filter.accept(status.getPath()));
  }

  /**
   * Convert a value into a non-empty Optional instance if
   * the value of {@code include} is true.
   * @param include flag to indicate the value is to be included.
   * @param value value to return
   * @param <T> type of option.
   * @return if include is false, Optional.empty. Otherwise, the value.
   */
  public static <T> Optional<T> maybe(boolean include, T value) {
    return include ? Optional.of(value) : Optional.empty();
  }

  /**
   * Patch the security credential provider information in
   * {@link #CREDENTIAL_PROVIDER_PATH}
   * with the providers listed in
   * {@link Constants#S3A_SECURITY_CREDENTIAL_PROVIDER_PATH}.
   *
   * This allows different buckets to use different credential files.
   * @param conf configuration to patch
   */
  static void patchSecurityCredentialProviders(Configuration conf) {
    Collection<String> customCredentials = conf.getStringCollection(
        S3A_SECURITY_CREDENTIAL_PROVIDER_PATH);
    Collection<String> hadoopCredentials = conf.getStringCollection(
        CREDENTIAL_PROVIDER_PATH);
    if (!customCredentials.isEmpty()) {
      List<String> all = Lists.newArrayList(customCredentials);
      all.addAll(hadoopCredentials);
      String joined = StringUtils.join(all, ',');
      LOG.debug("Setting {} to {}", CREDENTIAL_PROVIDER_PATH,
          joined);
      conf.set(CREDENTIAL_PROVIDER_PATH, joined,
          "patch of " + S3A_SECURITY_CREDENTIAL_PROVIDER_PATH);
    }
  }

  /**
   * Lookup a per-bucket-secret from a configuration including JCEKS files.
   * No attempt is made to look for the global configuration.
   * @param bucket bucket or "" if none known
   * @param conf configuration
   * @param baseKey base key to look up, e.g "fs.s3a.secret.key"
   * @return the secret or null.
   * @throws IOException on any IO problem
   * @throws IllegalArgumentException bad arguments
   */
  public static String lookupBucketSecret(
      String bucket,
      Configuration conf,
      String baseKey)
      throws IOException {

    Preconditions.checkArgument(!isEmpty(bucket), "null/empty bucket argument");
    Preconditions.checkArgument(baseKey.startsWith(FS_S3A_PREFIX),
        "%s does not start with $%s", baseKey, FS_S3A_PREFIX);
    String subkey = baseKey.substring(FS_S3A_PREFIX.length());

    // set from the long key unless overidden.
    String longBucketKey = String.format(
        BUCKET_PATTERN, bucket, baseKey);
    String initialVal = getPassword(conf, longBucketKey, null, null);
    // then override from the short one if it is set
    String shortBucketKey = String.format(
        BUCKET_PATTERN, bucket, subkey);
    return getPassword(conf, shortBucketKey, initialVal, null);
  }

  /**
   * Get any S3 encryption key, without propagating exceptions from
   * JCEKs files.
   * @param bucket bucket to query for
   * @param conf configuration to examine
   * @return the encryption key or ""
   * @throws IllegalArgumentException bad arguments.
   */
  public static String getS3EncryptionKey(
      String bucket,
      Configuration conf) {
    try {
      return getS3EncryptionKey(bucket, conf, false);
    } catch (IOException e) {
      // never going to happen, but to make sure, covert to
      // runtime exception
      throw new UncheckedIOException(e);
    }
  }

    /**
     * Get any SSE/CSE key from a configuration/credential provider.
     * This operation handles the case where the option has been
     * set in the provider or configuration to the option
     * {@code SERVER_SIDE_ENCRYPTION_KEY}.
     * IOExceptions raised during retrieval are swallowed.
     * @param bucket bucket to query for
     * @param conf configuration to examine
     * @param propagateExceptions should IO exceptions be rethrown?
     * @return the encryption key or ""
     * @throws IllegalArgumentException bad arguments.
     * @throws IOException if propagateExceptions==true and reading a JCEKS file raised an IOE
     */
  @SuppressWarnings("deprecation")
  public static String getS3EncryptionKey(
      String bucket,
      Configuration conf,
      boolean propagateExceptions) throws IOException {
    try {
      // look up the per-bucket value of the new key,
      // which implicitly includes the deprecation remapping
      String key = lookupBucketSecret(bucket, conf, S3_ENCRYPTION_KEY);
      if (key == null) {
        // old key in bucket, jceks
        key = lookupBucketSecret(bucket, conf, SERVER_SIDE_ENCRYPTION_KEY);
      }
      if (key == null) {
        // new key, global; implicit translation of old key in XML files.
        key = lookupPassword(null, conf, S3_ENCRYPTION_KEY);
      }
      if (key == null) {
        // old key, JCEKS
        key = lookupPassword(null, conf, SERVER_SIDE_ENCRYPTION_KEY);
      }
      if (key == null) {
        // no key, return ""
        key = "";
      }
      return key;
    } catch (IOException e) {
      if (propagateExceptions) {
        throw e;
      }
      LOG.warn("Cannot retrieve {} for bucket {}",
          S3_ENCRYPTION_KEY, bucket, e);
      return "";
    }
  }

  /**
   * Get the server-side encryption or client side encryption algorithm.
   * This includes validation of the configuration, checking the state of
   * the encryption key given the chosen algorithm.
   *
   * @param bucket bucket to query for
   * @param conf configuration to scan
   * @return the encryption mechanism (which will be {@code NONE} unless
   * one is set.
   * @throws IOException on JCKES lookup or invalid method/key configuration.
   */
  public static S3AEncryptionMethods getEncryptionAlgorithm(String bucket,
      Configuration conf) throws IOException {
    return buildEncryptionSecrets(bucket, conf).getEncryptionMethod();
  }

  /**
   * Get the server-side encryption or client side encryption algorithm.
   * This includes validation of the configuration, checking the state of
   * the encryption key given the chosen algorithm.
   *
   * @param bucket bucket to query for
   * @param conf configuration to scan
   * @return the encryption mechanism (which will be {@code NONE} unless
   * one is set and secrets.
   * @throws IOException on JCKES lookup or invalid method/key configuration.
   */
  @SuppressWarnings("deprecation")
  public static EncryptionSecrets buildEncryptionSecrets(String bucket,
      Configuration conf) throws IOException {

    // new key, per-bucket
    // this will include fixup of the old key in config XML entries
    String algorithm = lookupBucketSecret(bucket, conf, S3_ENCRYPTION_ALGORITHM);
    if (algorithm == null) {
      // try the old key, per-bucket setting, which will find JCEKS values
      algorithm = lookupBucketSecret(bucket, conf, SERVER_SIDE_ENCRYPTION_ALGORITHM);
    }
    if (algorithm == null) {
      // new key, global setting
      // this will include fixup of the old key in config XML entries
      algorithm = lookupPassword(null, conf, S3_ENCRYPTION_ALGORITHM);
    }
    if (algorithm == null) {
      // old key, global setting, for JCEKS entries.
      algorithm = lookupPassword(null, conf, SERVER_SIDE_ENCRYPTION_ALGORITHM);
    }
    // now determine the algorithm
    final S3AEncryptionMethods encryptionMethod = S3AEncryptionMethods.getMethod(algorithm);

    // look up the encryption key
    String encryptionKey = getS3EncryptionKey(bucket, conf,
        encryptionMethod.requiresSecret());
    int encryptionKeyLen =
        StringUtils.isBlank(encryptionKey) ? 0 : encryptionKey.length();
    String diagnostics = passwordDiagnostics(encryptionKey, "key");
    String encryptionContext = S3AEncryption.getS3EncryptionContextBase64Encoded(bucket, conf,
        encryptionMethod.requiresSecret());
    switch (encryptionMethod) {
    case SSE_C:
      LOG.debug("Using SSE-C with {}", diagnostics);
      if (encryptionKeyLen == 0) {
        throw new IOException(SSE_C_NO_KEY_ERROR);
      }
      break;

    case SSE_S3:
      if (encryptionKeyLen != 0) {
        throw new IOException(SSE_S3_WITH_KEY_ERROR
            + " (" + diagnostics + ")");
      }
      break;

    case SSE_KMS:
      LOG.debug("Using SSE-KMS with {}",
          diagnostics);
      break;

    case CSE_KMS:
      LOG.debug("Using CSE-KMS with {}",
          diagnostics);
      break;

    case DSSE_KMS:
      LOG.debug("Using DSSE-KMS with {}",
          diagnostics);
      break;

    case NONE:
    default:
      LOG.debug("Data is unencrypted");
      break;
    }
    return new EncryptionSecrets(encryptionMethod, encryptionKey, encryptionContext);
  }

  /**
   * Provide a password diagnostics string.
   * This aims to help diagnostics without revealing significant password details
   * @param pass password
   * @param description description for text, e.g "key" or "password"
   * @return text for use in messages.
   */
  private static String passwordDiagnostics(String pass, String description) {
    if (pass == null) {
      return "null " + description;
    }
    int len = pass.length();
    switch (len) {
    case 0:
      return "empty " + description;
    case 1:
      return description + " of length 1";

    default:
      return description + " of length " + len + " ending with "
          + pass.charAt(len - 1);
    }
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any Exception or
   * null pointers.
   * This is obsolete: use
   * {@link org.apache.hadoop.io.IOUtils#cleanupWithLogger(Logger, Closeable...)}
   * @param log the log to log at debug level. Can be null.
   * @param closeables the objects to close
   */
  @Deprecated
  public static void closeAll(Logger log,
      Closeable... closeables) {
    cleanupWithLogger(log, closeables);
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any Exception or
   * null pointers.
   * (This is the SLF4J equivalent of that in {@code IOUtils}).
   * @param log the log to log at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void closeAutocloseables(Logger log,
      AutoCloseable... closeables) {
    if (log == null) {
      log = LOG;
    }
    for (AutoCloseable c : closeables) {
      if (c != null) {
        try {
          log.debug("Closing {}", c);
          c.close();
        } catch (Exception e) {
          log.debug("Exception in closing {}", c, e);
        }
      }
    }
  }

  /**
   * Set a bucket-specific property to a particular value.
   * If the generic key passed in has an {@code fs.s3a. prefix},
   * that's stripped off, so that when the the bucket properties are propagated
   * down to the generic values, that value gets copied down.
   * @param conf configuration to set
   * @param bucket bucket name
   * @param genericKey key; can start with "fs.s3a."
   * @param value value to set
   */
  public static void setBucketOption(Configuration conf, String bucket,
      String genericKey, String value) {
    final String baseKey = genericKey.startsWith(FS_S3A_PREFIX) ?
        genericKey.substring(FS_S3A_PREFIX.length())
        : genericKey;
    conf.set(FS_S3A_BUCKET_PREFIX + bucket + '.' + baseKey, value, "S3AUtils");
  }

  /**
   * Clear a bucket-specific property.
   * If the generic key passed in has an {@code fs.s3a. prefix},
   * that's stripped off, so that when the the bucket properties are propagated
   * down to the generic values, that value gets copied down.
   * @param conf configuration to set
   * @param bucket bucket name
   * @param genericKey key; can start with "fs.s3a."
   */
  public static void clearBucketOption(Configuration conf, String bucket,
      String genericKey) {
    final String baseKey = genericKey.startsWith(FS_S3A_PREFIX) ?
        genericKey.substring(FS_S3A_PREFIX.length())
        : genericKey;
    String k = FS_S3A_BUCKET_PREFIX + bucket + '.' + baseKey;
    LOG.debug("Unset {}", k);
    conf.unset(k);
  }

  /**
   * Get a bucket-specific property.
   * If the generic key passed in has an {@code fs.s3a. prefix},
   * that's stripped off.
   * @param conf configuration to set
   * @param bucket bucket name
   * @param genericKey key; can start with "fs.s3a."
   * @return the bucket option, null if there is none
   */
  public static String getBucketOption(Configuration conf, String bucket,
      String genericKey) {
    final String baseKey = genericKey.startsWith(FS_S3A_PREFIX) ?
        genericKey.substring(FS_S3A_PREFIX.length())
        : genericKey;
    return conf.get(FS_S3A_BUCKET_PREFIX + bucket + '.' + baseKey);
  }

  /**
   * Turns a path (relative or otherwise) into an S3 key, adding a trailing
   * "/" if the path is not the root <i>and</i> does not already have a "/"
   * at the end.
   *
   * @param key s3 key or ""
   * @return the with a trailing "/", or, if it is the root key, "",
   */
  public static String maybeAddTrailingSlash(String key) {
    if (!key.isEmpty() && !key.endsWith("/")) {
      return key + '/';
    } else {
      return key;
    }
  }

  /**
   * Path filter which ignores any file which starts with . or _.
   */
  public static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }

    @Override
    public String toString() {
      return "HIDDEN_FILE_FILTER";
    }
  };

  /**
   * A Path filter which accepts all filenames.
   */
  public static final PathFilter ACCEPT_ALL = new PathFilter() {
    @Override
    public boolean accept(Path file) {
      return true;
    }

    @Override
    public String toString() {
      return "ACCEPT_ALL";
    }
  };

  /**
   * Format a byte range for a request header.
   * See https://www.rfc-editor.org/rfc/rfc9110.html#section-14.1.2
   *
   * @param rangeStart the start byte offset
   * @param rangeEnd the end byte offset (inclusive)
   * @return a formatted byte range
   */
  public static String formatRange(long rangeStart, long rangeEnd) {
    return String.format("bytes=%d-%d", rangeStart, rangeEnd);
  }

  /**
   * Get the equal op (=) delimited key-value pairs of the <code>name</code> property as
   * a collection of pair of <code>String</code>s, trimmed of the leading and trailing whitespace
   * after delimiting the <code>name</code> by comma and new line separator.
   * If no such property is specified then empty <code>Map</code> is returned.
   *
   * @param configuration the configuration object.
   * @param name property name.
   * @return property value as a <code>Map</code> of <code>String</code>s, or empty
   * <code>Map</code>.
   */
  public static Map<String, String> getTrimmedStringCollectionSplitByEquals(
      final Configuration configuration,
      final String name) {
    String valueString = configuration.get(name);
    return getTrimmedStringCollectionSplitByEquals(valueString);
  }

  /**
   * Get the equal op (=) delimited key-value pairs of the <code>name</code> property as
   * a collection of pair of <code>String</code>s, trimmed of the leading and trailing whitespace
   * after delimiting the <code>name</code> by comma and new line separator.
   * If no such property is specified then empty <code>Map</code> is returned.
   *
   * @param valueString the string containing the key-value pairs.
   * @return property value as a <code>Map</code> of <code>String</code>s, or empty
   * <code>Map</code>.
   */
  public static Map<String, String> getTrimmedStringCollectionSplitByEquals(
      final String valueString) {
    if (null == valueString) {
      return new HashMap<>();
    }
    return org.apache.hadoop.util.StringUtils
        .getTrimmedStringCollectionSplitByEquals(valueString);
  }


  /**
   * If classloader isolation is {@code true}
   * (through {@link Constants#AWS_S3_CLASSLOADER_ISOLATION}) or not
   * explicitly set, then the classLoader of the input configuration object
   * will be set to the input classloader, otherwise nothing will happen.
   * @param conf configuration object.
   * @param classLoader isolated classLoader.
   */
  static void maybeIsolateClassloader(Configuration conf, ClassLoader classLoader) {
    if (conf.getBoolean(Constants.AWS_S3_CLASSLOADER_ISOLATION,
            Constants.DEFAULT_AWS_S3_CLASSLOADER_ISOLATION)) {
      LOG.debug("Configuration classloader set to S3AFileSystem classloader: {}", classLoader);
      conf.setClassLoader(classLoader);
    } else {
      LOG.debug("Configuration classloader not changed, support classes needed will be loaded " +
                      "from the classloader that instantiated the Configuration object: {}",
              conf.getClassLoader());
    }
  }

}
