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
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.security.ProviderUtils;

import com.google.common.collect.Lists;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Utility methods for S3A code.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3AUtils {

  /** Reuse the S3AFileSystem log. */
  private static final Logger LOG = S3AFileSystem.LOG;
  static final String CONSTRUCTOR_EXCEPTION = "constructor exception";
  static final String INSTANTIATION_EXCEPTION
      = "instantiation exception";
  static final String NOT_AWS_PROVIDER =
      "does not implement AWSCredentialsProvider";
  static final String ABSTRACT_PROVIDER =
      "is abstract and therefore cannot be created";
  static final String ENDPOINT_KEY = "Endpoint";

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
      if (containsInterruptedException(exception)) {
        return (IOException)new InterruptedIOException(message)
            .initCause(exception);
      }
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

      case 301:
        if (s3Exception != null) {
          if (s3Exception.getAdditionalDetails() != null &&
              s3Exception.getAdditionalDetails().containsKey(ENDPOINT_KEY)) {
            message = String.format("Received permanent redirect response to "
                + "endpoint %s.  This likely indicates that the S3 endpoint "
                + "configured in %s does not match the AWS region containing "
                + "the bucket.",
                s3Exception.getAdditionalDetails().get(ENDPOINT_KEY), ENDPOINT);
          }
          ioe = new AWSS3IOException(message, s3Exception);
        } else {
          ioe = new AWSServiceIOException(message, ase);
        }
        break;
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
   * Recurse down the exception loop looking for any inner details about
   * an interrupted exception.
   * @param thrown exception thrown
   * @return true if down the execution chain the operation was an interrupt
   */
  static boolean containsInterruptedException(Throwable thrown) {
    if (thrown == null) {
      return false;
    }
    if (thrown instanceof InterruptedException ||
        thrown instanceof InterruptedIOException) {
      return true;
    }
    // tail recurse
    return containsInterruptedException(thrown.getCause());
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
   * @return a status entry
   */
  public static S3AFileStatus createFileStatus(Path keyPath,
      S3ObjectSummary summary,
      long blockSize,
      String owner) {
    long size = summary.getSize();
    return createFileStatus(keyPath,
        objectRepresentsDirectory(summary.getKey(), size),
        size, summary.getLastModified(), blockSize, owner);
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
   * @return a status entry
   */
  public static S3AFileStatus createUploadFileStatus(Path keyPath,
      boolean isDir, long size, long blockSize, String owner) {
    Date date = isDir ? null : new Date();
    return createFileStatus(keyPath, isDir, size, date, blockSize, owner);
  }

  /* Date 'modified' is ignored when isDir is true. */
  private static S3AFileStatus createFileStatus(Path keyPath, boolean isDir,
      long size, Date modified, long blockSize, String owner) {
    if (isDir) {
      return new S3AFileStatus(Tristate.UNKNOWN, keyPath, owner);
    } else {
      return new S3AFileStatus(size, dateToLong(modified), keyPath, blockSize,
          owner);
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
   * Create the AWS credentials from the providers and the URI.
   * @param binding Binding URI, may contain user:pass login details
   * @param conf filesystem configuration
   * @return a credentials provider list
   * @throws IOException Problems loading the providers (including reading
   * secrets from credential files).
   */
  public static AWSCredentialProviderList createAWSCredentialProviderSet(
      URI binding, Configuration conf) throws IOException {
    AWSCredentialProviderList credentials = new AWSCredentialProviderList();

    Class<?>[] awsClasses;
    try {
      awsClasses = conf.getClasses(AWS_CREDENTIALS_PROVIDER);
    } catch (RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException("From option " + AWS_CREDENTIALS_PROVIDER +
          ' ' + c, c);
    }
    if (awsClasses.length == 0) {
      S3xLoginHelper.Login creds = getAWSAccessKeys(binding, conf);
      credentials.add(new BasicAWSCredentialsProvider(
              creds.getUser(), creds.getPassword()));
      credentials.add(new EnvironmentVariableCredentialsProvider());
      credentials.add(InstanceProfileCredentialsProvider.getInstance());
    } else {
      for (Class<?> aClass : awsClasses) {
        credentials.add(createAWSCredentialProvider(conf, aClass));
      }
    }
    // make sure the logging message strips out any auth details
    LOG.debug("For URI {}, using credentials {}",
        S3xLoginHelper.toString(binding), credentials);
    return credentials;
  }

  /**
   * Create an AWS credential provider from its class by using reflection.  The
   * class must implement one of the following means of construction, which are
   * attempted in order:
   *
   * <ol>
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
   * @return the instantiated class
   * @throws IOException on any instantiation failure.
   */
  static AWSCredentialsProvider createAWSCredentialProvider(
      Configuration conf, Class<?> credClass) throws IOException {
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
      // new X(conf)
      Constructor cons = getConstructor(credClass, Configuration.class);
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
          + "accepting Configuration, or a public factory method named "
          + "getInstance that accepts no arguments, or a public default "
          + "constructor.", className, AWS_CREDENTIALS_PROVIDER));
    } catch (ReflectiveOperationException | IllegalArgumentException e) {
      // supported constructor or factory method found, but the call failed
      throw new IOException(className + " " + INSTANTIATION_EXCEPTION +".", e);
    }
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
  static String getPassword(Configuration conf, String key, String val)
      throws IOException {
    String defVal = "";
    return getPassword(conf, key, val, defVal);
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
    return StringUtils.isEmpty(val)
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
  static long longOption(Configuration conf,
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
   * Get a long option >= the minimum allowed value, supporting memory
   * prefixes K,M,G,T,P.
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static long longBytesOption(Configuration conf,
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
   * @param conf configuration to examine
   * @return the encryption key or null
   */
  static String getServerSideEncryptionKey(Configuration conf) {
    try {
      return lookupPassword(conf, SERVER_SIDE_ENCRYPTION_KEY,
          getPassword(conf, OLD_S3A_SERVER_SIDE_ENCRYPTION_KEY,
              null, null));
    } catch (IOException e) {
      LOG.error("Cannot retrieve SERVER_SIDE_ENCRYPTION_KEY", e);
      return "";
    }
  }

  /**
   * Get the server-side encryption algorithm.
   * This includes validation of the configuration, checking the state of
   * the encryption key given the chosen algorithm.
   * @param conf configuration to scan
   * @return the encryption mechanism (which will be {@code NONE} unless
   * one is set.
   * @throws IOException on any validation problem.
   */
  static S3AEncryptionMethods getEncryptionAlgorithm(Configuration conf)
      throws IOException {
    S3AEncryptionMethods sse = S3AEncryptionMethods.getMethod(
        conf.getTrimmed(SERVER_SIDE_ENCRYPTION_ALGORITHM));
    String sseKey = getServerSideEncryptionKey(conf);
    int sseKeyLen = StringUtils.isBlank(sseKey) ? 0 : sseKey.length();
    String diagnostics = passwordDiagnostics(sseKey, "key");
    switch (sse) {
    case SSE_C:
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
    LOG.debug("Using SSE-C with {}", diagnostics);
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
   * (This is the SLF4J equivalent of that in {@code IOUtils}).
   * @param log the log to log at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void closeAll(Logger log,
      java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          if (log != null) {
            log.debug("Closing {}", c);
          }
          c.close();
        } catch (Exception e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing {}", c, e);
          }
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

}
