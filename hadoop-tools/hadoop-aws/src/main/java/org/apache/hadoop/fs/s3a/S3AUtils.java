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

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.retry.RetryUtils;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;
import org.apache.hadoop.fs.s3a.impl.NetworkBinding;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.util.VersionInfo;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.isUnknownBucket;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.translateDeleteException;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Utility methods for S3A code.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3AUtils {

  private static final Logger LOG = LoggerFactory.getLogger(S3AUtils.class);
  static final String CONSTRUCTOR_EXCEPTION = "constructor exception";
  static final String INSTANTIATION_EXCEPTION
      = "instantiation exception";
  static final String NOT_AWS_PROVIDER =
      "does not implement AWSCredentialsProvider";
  static final String ABSTRACT_PROVIDER =
      "is abstract and therefore cannot be created";
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
          + SERVER_SIDE_ENCRYPTION_KEY;
  /**
   * Encryption SSE-S3 is used but the caller also set an encryption key.
   */
  public static final String SSE_S3_WITH_KEY_ERROR =
      S3AEncryptionMethods.SSE_S3.getMethod()
          + " is enabled but an encryption key was set in "
          + SERVER_SIDE_ENCRYPTION_KEY;
  private static final String EOF_MESSAGE_IN_XML_PARSER
      = "Failed to sanitize XML document destined for handler class";

  private static final String BUCKET_PATTERN = FS_S3A_BUCKET_PREFIX + "%s.%s";

  /**
   * Error message when the AWS provider list built up contains a forbidden
   * entry.
   */
  @VisibleForTesting
  public static final String E_FORBIDDEN_AWS_PROVIDER
      = "AWS provider class cannot be used";

  private S3AUtils() {
  }

  /**
   * Translate an exception raised in an operation into an IOException.
   * The specific type of IOException depends on the class of
   * {@link AmazonClientException} passed in, and any status codes included
   * in the operation. That is: HTTP error codes are examined and can be
   * used to build a more specific response.
   *
   * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">S3 Error responses</a>
   * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/ErrorBestPractices.html">Amazon S3 Error Best Practices</a>
   * @see <a href="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html">Dynamo DB Commmon errors</a>
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
  public static IOException translateException(@Nullable String operation,
      String path,
      SdkBaseException exception) {
    String message = String.format("%s%s: %s",
        operation,
        StringUtils.isNotEmpty(path)? (" on " + path) : "",
        exception);
    if (!(exception instanceof AmazonServiceException)) {
      Exception innerCause = containsInterruptedException(exception);
      if (innerCause != null) {
        // interrupted IO, or a socket exception underneath that class
        return translateInterruptedException(exception, innerCause, message);
      }
      if (signifiesConnectionBroken(exception)) {
        // call considered an sign of connectivity failure
        return (EOFException)new EOFException(message).initCause(exception);
      }
      if (exception instanceof CredentialInitializationException) {
        // the exception raised by AWSCredentialProvider list if the
        // credentials were not accepted.
        return (AccessDeniedException)new AccessDeniedException(path, null,
            exception.toString()).initCause(exception);
      }
      return new AWSClientIOException(message, exception);
    } else {
      if (exception instanceof AmazonDynamoDBException) {
        // special handling for dynamo DB exceptions
        return translateDynamoDBException(path, message,
            (AmazonDynamoDBException)exception);
      }
      IOException ioe;
      AmazonServiceException ase = (AmazonServiceException) exception;
      // this exception is non-null if the service exception is an s3 one
      AmazonS3Exception s3Exception = ase instanceof AmazonS3Exception
          ? (AmazonS3Exception) ase
          : null;
      int status = ase.getStatusCode();
      message = message + ":" + ase.getErrorCode();
      switch (status) {

      case 301:
      case 307:
        if (s3Exception != null) {
          if (s3Exception.getAdditionalDetails() != null &&
              s3Exception.getAdditionalDetails().containsKey(ENDPOINT_KEY)) {
            message = String.format("Received permanent redirect response to "
                + "endpoint %s.  This likely indicates that the S3 endpoint "
                + "configured in %s does not match the AWS region containing "
                + "the bucket.",
                s3Exception.getAdditionalDetails().get(ENDPOINT_KEY), ENDPOINT);
          }
          ioe = new AWSRedirectException(message, s3Exception);
        } else {
          ioe = new AWSRedirectException(message, ase);
        }
        break;

      case 400:
        ioe = new AWSBadRequestException(message, ase);
        break;

      // permissions
      case 401:
      case 403:
        ioe = new AccessDeniedException(path, null, message);
        ioe.initCause(ase);
        break;

      // the object isn't there
      case 404:
        if (isUnknownBucket(ase)) {
          // this is a missing bucket
          ioe = new UnknownStoreException(path, ase);
        } else {
          // a normal unknown object
          ioe = new FileNotFoundException(message);
          ioe.initCause(ase);
        }
        break;

      // this also surfaces sometimes and is considered to
      // be ~ a not found exception.
      case 410:
        ioe = new FileNotFoundException(message);
        ioe.initCause(ase);
        break;

      // method not allowed; seen on S3 Select.
      // treated as a bad request
      case 405:
        ioe = new AWSBadRequestException(message, s3Exception);
        break;

      // out of range. This may happen if an object is overwritten with
      // a shorter one while it is being read.
      case 416:
        ioe = new EOFException(message);
        ioe.initCause(ase);
        break;

      // this has surfaced as a "no response from server" message.
      // so rare we haven't replicated it.
      // Treating as an idempotent proxy error.
      case 443:
      case 444:
        ioe = new AWSNoResponseException(message, ase);
        break;

      // throttling
      case 503:
        ioe = new AWSServiceThrottledException(message, ase);
        break;

      // internal error
      case 500:
        ioe = new AWSStatus500Exception(message, ase);
        break;

      case 200:
        if (exception instanceof MultiObjectDeleteException) {
          // failure during a bulk delete
          return translateDeleteException(message,
              (MultiObjectDeleteException) exception);
        }
        // other 200: FALL THROUGH

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
      SdkBaseException exception,
      final Exception innerCause,
      String message) {
    InterruptedIOException ioe;
    if (innerCause instanceof SocketTimeoutException) {
      ioe = new SocketTimeoutException(message);
    } else {
      String name = innerCause.getClass().getName();
      if (name.endsWith(".ConnectTimeoutException")
          || name.endsWith("$ConnectTimeoutException")) {
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
   * is an AmazonServiceException with a 503 response, any
   * exception from DynamoDB for limits exceeded, an
   * {@link AWSServiceThrottledException},
   * or anything which the AWS SDK's RetryUtils considers to be
   * a throttling exception.
   * @param ex exception to examine
   * @return true if it is considered a throttling exception
   */
  public static boolean isThrottleException(Exception ex) {
    return ex instanceof AWSServiceThrottledException
        || ex instanceof ProvisionedThroughputExceededException
        || ex instanceof LimitExceededException
        || (ex instanceof AmazonServiceException
            && 503  == ((AmazonServiceException)ex).getStatusCode())
        || (ex instanceof SdkBaseException
            && RetryUtils.isThrottlingException((SdkBaseException) ex));
  }

  /**
   * Cue that an AWS exception is likely to be an EOF Exception based
   * on the message coming back from an XML/JSON parser. This is likely
   * to be brittle, so only a hint.
   * @param ex exception
   * @return true if this is believed to be a sign the connection was broken.
   */
  public static boolean signifiesConnectionBroken(SdkBaseException ex) {
    return ex.toString().contains(EOF_MESSAGE_IN_XML_PARSER);
  }

  /**
   * Translate a DynamoDB exception into an IOException.
   *
   * @param path path in the DDB
   * @param message preformatted message for the exception
   * @param ddbException exception
   * @return an exception to throw.
   */
  public static IOException translateDynamoDBException(final String path,
      final String message,
      final AmazonDynamoDBException ddbException) {
    if (isThrottleException(ddbException)) {
      return new AWSServiceThrottledException(message, ddbException);
    }
    if (ddbException instanceof ResourceNotFoundException) {
      return (FileNotFoundException) new FileNotFoundException(message)
          .initCause(ddbException);
    }
    final int statusCode = ddbException.getStatusCode();
    final String errorCode = ddbException.getErrorCode();
    IOException result = null;
    // 400 gets used a lot by DDB
    if (statusCode == 400) {
      switch (errorCode) {
      case "AccessDeniedException":
        result = (IOException) new AccessDeniedException(
            path,
            null,
            ddbException.toString())
            .initCause(ddbException);
        break;

      default:
        result = new AWSBadRequestException(message, ddbException);
      }

    }
    if (result ==  null) {
      result = new AWSServiceIOException(message, ddbException);
    }
    return result;
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
   * @param owner owner of the file
   * @param eTag S3 object eTag or null if unavailable
   * @param versionId S3 object versionId or null if unavailable
   * @return a status entry
   */
  public static S3AFileStatus createFileStatus(Path keyPath,
      S3ObjectSummary summary,
      long blockSize,
      String owner,
      String eTag,
      String versionId) {
    long size = summary.getSize();
    return createFileStatus(keyPath,
        objectRepresentsDirectory(summary.getKey(), size),
        size, summary.getLastModified(), blockSize, owner, eTag, versionId);
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
   * The standard AWS provider list for AWS connections.
   */
  public static final List<Class<?>>
      STANDARD_AWS_PROVIDERS = Collections.unmodifiableList(
      Arrays.asList(
          TemporaryAWSCredentialsProvider.class,
          SimpleAWSCredentialsProvider.class,
          EnvironmentVariableCredentialsProvider.class,
          IAMInstanceCredentialsProvider.class));

  /**
   * Create the AWS credentials from the providers, the URI and
   * the key {@link Constants#AWS_CREDENTIALS_PROVIDER} in the configuration.
   * @param binding Binding URI -may be null
   * @param conf filesystem configuration
   * @return a credentials provider list
   * @throws IOException Problems loading the providers (including reading
   * secrets from credential files).
   */
  public static AWSCredentialProviderList createAWSCredentialProviderSet(
      @Nullable URI binding,
      Configuration conf) throws IOException {
    // this will reject any user:secret entries in the URI
    S3xLoginHelper.rejectSecretsInURIs(binding);
    AWSCredentialProviderList credentials =
        buildAWSProviderList(binding,
            conf,
            AWS_CREDENTIALS_PROVIDER,
            STANDARD_AWS_PROVIDERS,
            new HashSet<>());
    // make sure the logging message strips out any auth details
    LOG.debug("For URI {}, using credentials {}",
        binding, credentials);
    return credentials;
  }

  /**
   * Load list of AWS credential provider/credential provider factory classes.
   * @param conf configuration
   * @param key key
   * @param defaultValue list of default values
   * @return the list of classes, possibly empty
   * @throws IOException on a failure to load the list.
   */
  public static List<Class<?>> loadAWSProviderClasses(Configuration conf,
      String key,
      Class<?>... defaultValue) throws IOException {
    try {
      return Arrays.asList(conf.getClasses(key, defaultValue));
    } catch (RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException("From option " + key + ' ' + c, c);
    }
  }

  /**
   * Load list of AWS credential provider/credential provider factory classes;
   * support a forbidden list to prevent loops, mandate full secrets, etc.
   * @param binding Binding URI -may be null
   * @param conf configuration
   * @param key key
   * @param forbidden a possibly empty set of forbidden classes.
   * @param defaultValues list of default providers.
   * @return the list of classes, possibly empty
   * @throws IOException on a failure to load the list.
   */
  public static AWSCredentialProviderList buildAWSProviderList(
      @Nullable final URI binding,
      final Configuration conf,
      final String key,
      final List<Class<?>> defaultValues,
      final Set<Class<?>> forbidden) throws IOException {

    // build up the base provider
    List<Class<?>> awsClasses = loadAWSProviderClasses(conf,
        key,
        defaultValues.toArray(new Class[defaultValues.size()]));
    // and if the list is empty, switch back to the defaults.
    // this is to address the issue that configuration.getClasses()
    // doesn't return the default if the config value is just whitespace.
    if (awsClasses.isEmpty()) {
      awsClasses = defaultValues;
    }
    // iterate through, checking for blacklists and then instantiating
    // each provider
    AWSCredentialProviderList providers = new AWSCredentialProviderList();
    for (Class<?> aClass : awsClasses) {

      if (forbidden.contains(aClass)) {
        throw new IOException(E_FORBIDDEN_AWS_PROVIDER
            + " in option " + key + ": " + aClass);
      }
      providers.add(createAWSCredentialProvider(conf,
          aClass, binding));
    }
    return providers;
  }

  /**
   * Create an AWS credential provider from its class by using reflection.  The
   * class must implement one of the following means of construction, which are
   * attempted in order:
   *
   * <ol>
   * <li>a public constructor accepting java.net.URI and
   *     org.apache.hadoop.conf.Configuration</li>
   * <li>a public constructor accepting
   *    org.apache.hadoop.conf.Configuration</li>
   * <li>a public static method named getInstance that accepts no
   *    arguments and returns an instance of
   *    com.amazonaws.auth.AWSCredentialsProvider, or</li>
   * <li>a public default constructor.</li>
   * </ol>
   *
   * @param conf configuration
   * @param credClass credential class
   * @param uri URI of the FS
   * @return the instantiated class
   * @throws IOException on any instantiation failure.
   */
  private static AWSCredentialsProvider createAWSCredentialProvider(
      Configuration conf,
      Class<?> credClass,
      @Nullable URI uri) throws IOException {
    AWSCredentialsProvider credentials = null;
    String className = credClass.getName();
    if (!AWSCredentialsProvider.class.isAssignableFrom(credClass)) {
      throw new IOException("Class " + credClass + " " + NOT_AWS_PROVIDER);
    }
    if (Modifier.isAbstract(credClass.getModifiers())) {
      throw new IOException("Class " + credClass + " " + ABSTRACT_PROVIDER);
    }
    LOG.debug("Credential provider class is {}", className);

    try {
      // new X(uri, conf)
      Constructor cons = getConstructor(credClass, URI.class,
          Configuration.class);
      if (cons != null) {
        credentials = (AWSCredentialsProvider)cons.newInstance(uri, conf);
        return credentials;
      }
      // new X(conf)
      cons = getConstructor(credClass, Configuration.class);
      if (cons != null) {
        credentials = (AWSCredentialsProvider)cons.newInstance(conf);
        return credentials;
      }

      // X.getInstance()
      Method factory = getFactoryMethod(credClass, AWSCredentialsProvider.class,
          "getInstance");
      if (factory != null) {
        credentials = (AWSCredentialsProvider)factory.invoke(null);
        return credentials;
      }

      // new X()
      cons = getConstructor(credClass);
      if (cons != null) {
        credentials = (AWSCredentialsProvider)cons.newInstance();
        return credentials;
      }

      // no supported constructor or factory method found
      throw new IOException(String.format("%s " + CONSTRUCTOR_EXCEPTION
          + ".  A class specified in %s must provide a public constructor "
          + "of a supported signature, or a public factory method named "
          + "getInstance that accepts no arguments.",
          className, AWS_CREDENTIALS_PROVIDER));
    } catch (InvocationTargetException e) {
      Throwable targetException = e.getTargetException();
      if (targetException == null) {
        targetException =  e;
      }
      if (targetException instanceof IOException) {
        throw (IOException) targetException;
      } else if (targetException instanceof SdkBaseException) {
        throw translateException("Instantiate " + className, "",
            (SdkBaseException) targetException);
      } else {
        // supported constructor or factory method found, but the call failed
        throw new IOException(className + " " + INSTANTIATION_EXCEPTION
            + ": " + targetException,
            targetException);
      }
    } catch (ReflectiveOperationException | IllegalArgumentException e) {
      // supported constructor or factory method found, but the call failed
      throw new IOException(className + " " + INSTANTIATION_EXCEPTION
          + ": " + e,
          e);
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
   * @param summary summary object
   * @return string value
   */
  public static String stringify(S3ObjectSummary summary) {
    StringBuilder builder = new StringBuilder(summary.getKey().length() + 100);
    builder.append(summary.getKey()).append(' ');
    builder.append("size=").append(summary.getSize());
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
   * @param source Source Configuration object.
   * @param bucket bucket name. Must not be empty.
   * @return a (potentially) patched clone of the original.
   */
  public static Configuration propagateBucketOptions(Configuration source,
      String bucket) {

    Preconditions.checkArgument(StringUtils.isNotEmpty(bucket), "bucket");
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
   * Create a new AWS {@code ClientConfiguration}.
   * All clients to AWS services <i>MUST</i> use this for consistent setup
   * of connectivity, UA, proxy settings.
   * @param conf The Hadoop configuration
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @return new AWS client configuration
   * @throws IOException problem creating AWS client configuration
   *
   * @deprecated use {@link #createAwsConf(Configuration, String, String)}
   */
  @Deprecated
  public static ClientConfiguration createAwsConf(Configuration conf,
      String bucket)
      throws IOException {
    return createAwsConf(conf, bucket, null);
  }

  /**
   * Create a new AWS {@code ClientConfiguration}. All clients to AWS services
   * <i>MUST</i> use this or the equivalents for the specific service for
   * consistent setup of connectivity, UA, proxy settings.
   *
   * @param conf The Hadoop configuration
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param awsServiceIdentifier a string representing the AWS service (S3,
   * DDB, etc) for which the ClientConfiguration is being created.
   * @return new AWS client configuration
   * @throws IOException problem creating AWS client configuration
   */
  public static ClientConfiguration createAwsConf(Configuration conf,
      String bucket, String awsServiceIdentifier)
      throws IOException {
    final ClientConfiguration awsConf = new ClientConfiguration();
    initConnectionSettings(conf, awsConf);
    initProxySupport(conf, bucket, awsConf);
    initUserAgent(conf, awsConf);
    if (StringUtils.isNotEmpty(awsServiceIdentifier)) {
      String configKey = null;
      switch (awsServiceIdentifier) {
      case AWS_SERVICE_IDENTIFIER_S3:
        configKey = SIGNING_ALGORITHM_S3;
        break;
      case AWS_SERVICE_IDENTIFIER_DDB:
        configKey = SIGNING_ALGORITHM_DDB;
        break;
      case AWS_SERVICE_IDENTIFIER_STS:
        configKey = SIGNING_ALGORITHM_STS;
        break;
      default:
        // Nothing to do. The original signer override is already setup
      }
      if (configKey != null) {
        String signerOverride = conf.getTrimmed(configKey, "");
        if (!signerOverride.isEmpty()) {
          LOG.debug("Signer override for {}} = {}", awsServiceIdentifier,
              signerOverride);
          awsConf.setSignerOverride(signerOverride);
        }
      }
    }
    return awsConf;
  }

  /**
   * Initializes all AWS SDK settings related to connection management.
   *
   * @param conf Hadoop configuration
   * @param awsConf AWS SDK configuration
   *
   * @throws IOException if there was an error initializing the protocol
   *                     settings
   */
  public static void initConnectionSettings(Configuration conf,
      ClientConfiguration awsConf) throws IOException {
    awsConf.setMaxConnections(intOption(conf, MAXIMUM_CONNECTIONS,
        DEFAULT_MAXIMUM_CONNECTIONS, 1));
    initProtocolSettings(conf, awsConf);
    awsConf.setMaxErrorRetry(intOption(conf, MAX_ERROR_RETRIES,
        DEFAULT_MAX_ERROR_RETRIES, 0));
    awsConf.setConnectionTimeout(intOption(conf, ESTABLISH_TIMEOUT,
        DEFAULT_ESTABLISH_TIMEOUT, 0));
    awsConf.setSocketTimeout(intOption(conf, SOCKET_TIMEOUT,
        DEFAULT_SOCKET_TIMEOUT, 0));
    int sockSendBuffer = intOption(conf, SOCKET_SEND_BUFFER,
        DEFAULT_SOCKET_SEND_BUFFER, 2048);
    int sockRecvBuffer = intOption(conf, SOCKET_RECV_BUFFER,
        DEFAULT_SOCKET_RECV_BUFFER, 2048);
    long requestTimeoutMillis = conf.getTimeDuration(REQUEST_TIMEOUT,
        DEFAULT_REQUEST_TIMEOUT, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

    if (requestTimeoutMillis > Integer.MAX_VALUE) {
      LOG.debug("Request timeout is too high({} ms). Setting to {} ms instead",
          requestTimeoutMillis, Integer.MAX_VALUE);
      requestTimeoutMillis = Integer.MAX_VALUE;
    }
    awsConf.setRequestTimeout((int) requestTimeoutMillis);
    awsConf.setSocketBufferSizeHints(sockSendBuffer, sockRecvBuffer);
    String signerOverride = conf.getTrimmed(SIGNING_ALGORITHM, "");
    if (!signerOverride.isEmpty()) {
     LOG.debug("Signer override = {}", signerOverride);
      awsConf.setSignerOverride(signerOverride);
    }
  }

  /**
   * Initializes the connection protocol settings when connecting to S3 (e.g.
   * either HTTP or HTTPS). If secure connections are enabled, this method
   * will load the configured SSL providers.
   *
   * @param conf Hadoop configuration
   * @param awsConf AWS SDK configuration
   *
   * @throws IOException if there is an error initializing the configured
   *                     {@link javax.net.ssl.SSLSocketFactory}
   */
  private static void initProtocolSettings(Configuration conf,
      ClientConfiguration awsConf) throws IOException {
    boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS,
        DEFAULT_SECURE_CONNECTIONS);
    awsConf.setProtocol(secureConnections ?  Protocol.HTTPS : Protocol.HTTP);
    if (secureConnections) {
      NetworkBinding.bindSSLChannelMode(conf, awsConf);
    }
  }

  /**
   * Initializes AWS SDK proxy support in the AWS client configuration
   * if the S3A settings enable it.
   *
   * @param conf Hadoop configuration
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param awsConf AWS SDK configuration to update
   * @throws IllegalArgumentException if misconfigured
   * @throws IOException problem getting username/secret from password source.
   */
  public static void initProxySupport(Configuration conf,
      String bucket,
      ClientConfiguration awsConf) throws IllegalArgumentException,
      IOException {
    String proxyHost = conf.getTrimmed(PROXY_HOST, "");
    int proxyPort = conf.getInt(PROXY_PORT, -1);
    if (!proxyHost.isEmpty()) {
      awsConf.setProxyHost(proxyHost);
      if (proxyPort >= 0) {
        awsConf.setProxyPort(proxyPort);
      } else {
        if (conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS)) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          awsConf.setProxyPort(443);
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          awsConf.setProxyPort(80);
        }
      }
      final String proxyUsername = lookupPassword(bucket, conf, PROXY_USERNAME,
          null, null);
      final String proxyPassword = lookupPassword(bucket, conf, PROXY_PASSWORD,
          null, null);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME + " or " +
            PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      awsConf.setProxyUsername(proxyUsername);
      awsConf.setProxyPassword(proxyPassword);
      awsConf.setProxyDomain(conf.getTrimmed(PROXY_DOMAIN));
      awsConf.setProxyWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using proxy server {}:{} as user {} with password {} on " +
                "domain {} as workstation {}", awsConf.getProxyHost(),
            awsConf.getProxyPort(),
            String.valueOf(awsConf.getProxyUsername()),
            awsConf.getProxyPassword(), awsConf.getProxyDomain(),
            awsConf.getProxyWorkstation());
      }
    } else if (proxyPort >= 0) {
      String msg =
          "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Initializes the User-Agent header to send in HTTP requests to AWS
   * services.  We always include the Hadoop version number.  The user also
   * may set an optional custom prefix to put in front of the Hadoop version
   * number.  The AWS SDK internally appends its own information, which seems
   * to include the AWS SDK version, OS and JVM version.
   *
   * @param conf Hadoop configuration
   * @param awsConf AWS SDK configuration to update
   */
  private static void initUserAgent(Configuration conf,
      ClientConfiguration awsConf) {
    String userAgent = "Hadoop " + VersionInfo.getVersion();
    String userAgentPrefix = conf.getTrimmed(USER_AGENT_PREFIX, "");
    if (!userAgentPrefix.isEmpty()) {
      userAgent = userAgentPrefix + ", " + userAgent;
    }
    LOG.debug("Using User-Agent: {}", userAgent);
    awsConf.setUserAgentPrefix(userAgent);
  }

  /**
   * Convert the data of an iterator of {@link S3AFileStatus} to
   * an array. Given tombstones are filtered out. If the iterator
   * does return any item, an empty array is returned.
   * @param iterator a non-null iterator
   * @param tombstones
   * @return a possibly-empty array of file status entries
   * @throws IOException
   */
  public static S3AFileStatus[] iteratorToStatuses(
      RemoteIterator<S3AFileStatus> iterator, Set<Path> tombstones)
      throws IOException {
    List<FileStatus> statuses = new ArrayList<>();

    while (iterator.hasNext()) {
      S3AFileStatus status = iterator.next();
      if (!tombstones.contains(status.getPath())) {
        statuses.add(status);
      }
    }

    return statuses.toArray(new S3AFileStatus[0]);
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
   * @param fileSystem filesystem
   * @param path path to list
   * @param recursive recursive listing?
   * @param filter filter for the filename
   * @return the filtered list of entries
   * @throws IOException IO failure.
   */
  public static List<LocatedFileStatus> listAndFilter(FileSystem fileSystem,
      Path path, boolean recursive, PathFilter filter) throws IOException {
    return flatmapLocatedFiles(fileSystem.listFiles(path, recursive),
        status -> maybe(filter.accept(status.getPath()), status));
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
   * Get any SSE key from a configuration/credential provider.
   * This operation handles the case where the option has been
   * set in the provider or configuration to the option
   * {@code OLD_S3A_SERVER_SIDE_ENCRYPTION_KEY}.
   * IOExceptions raised during retrieval are swallowed.
   * @param bucket bucket to query for
   * @param conf configuration to examine
   * @return the encryption key or ""
   * @throws IllegalArgumentException bad arguments.
   */
  public static String getServerSideEncryptionKey(String bucket,
      Configuration conf) {
    try {
      return lookupPassword(bucket, conf, SERVER_SIDE_ENCRYPTION_KEY);
    } catch (IOException e) {
      LOG.error("Cannot retrieve " + SERVER_SIDE_ENCRYPTION_KEY, e);
      return "";
    }
  }

  /**
   * Get the server-side encryption algorithm.
   * This includes validation of the configuration, checking the state of
   * the encryption key given the chosen algorithm.
   *
   * @param bucket bucket to query for
   * @param conf configuration to scan
   * @return the encryption mechanism (which will be {@code NONE} unless
   * one is set.
   * @throws IOException on any validation problem.
   */
  public static S3AEncryptionMethods getEncryptionAlgorithm(String bucket,
      Configuration conf) throws IOException {
    S3AEncryptionMethods sse = S3AEncryptionMethods.getMethod(
        lookupPassword(bucket, conf,
            SERVER_SIDE_ENCRYPTION_ALGORITHM));
    String sseKey = getServerSideEncryptionKey(bucket, conf);
    int sseKeyLen = StringUtils.isBlank(sseKey) ? 0 : sseKey.length();
    String diagnostics = passwordDiagnostics(sseKey, "key");
    switch (sse) {
    case SSE_C:
      LOG.debug("Using SSE-C with {}", diagnostics);
      if (sseKeyLen == 0) {
        throw new IOException(SSE_C_NO_KEY_ERROR);
      }
      break;

    case SSE_S3:
      if (sseKeyLen != 0) {
        throw new IOException(SSE_S3_WITH_KEY_ERROR
            + " (" + diagnostics + ")");
      }
      break;

    case SSE_KMS:
      LOG.debug("Using SSE-KMS with {}",
          diagnostics);
      break;

    case NONE:
    default:
      LOG.debug("Data is unencrypted");
      break;
    }
    return sse;
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

}
