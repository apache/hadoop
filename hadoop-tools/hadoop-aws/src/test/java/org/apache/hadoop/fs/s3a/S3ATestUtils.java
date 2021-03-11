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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;

import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ContextAccessors;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.impl.StoreContextBuilder;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStoreCapabilities;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.apache.hadoop.fs.s3a.test.OperationTrackingStore;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.impl.FutureIOSupport.awaitFuture;
import static org.apache.hadoop.fs.s3a.FailureInjectionPolicy.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.*;

/**
 * Utilities for the S3A tests.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class S3ATestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(
      S3ATestUtils.class);

  /**
   * Value to set a system property to (in maven) to declare that
   * a property has been unset.
   */
  public static final String UNSET_PROPERTY = "unset";
  public static final int PURGE_DELAY_SECONDS = 60 * 60;

  public static final int TIMESTAMP_SLEEP = 2000;
  public static final int STABILIZATION_TIME = 20_000;
  public static final int PROBE_INTERVAL_MILLIS = 500;

  /** Add any deprecated keys. */
  @SuppressWarnings("deprecation")
  private static void addDeprecatedKeys() {
    // STS endpoint configuration option
    Configuration.DeprecationDelta[] deltas = {
        // STS endpoint configuration option
        new Configuration.DeprecationDelta(
            S3ATestConstants.TEST_STS_ENDPOINT,
            ASSUMED_ROLE_STS_ENDPOINT)
    };

    if (deltas.length > 0) {
      Configuration.addDeprecations(deltas);
      Configuration.reloadExistingConfigurations();
    }
  }

  static {
    addDeprecatedKeys();
  }

  /**
   * Get S3A FS name.
   * @param conf configuration.
   * @return S3A fs name.
   */
  public static String getFsName(Configuration conf) {
    return conf.getTrimmed(TEST_FS_S3A_NAME, "");
  }

  /**
   * Create the test filesystem.
   *
   * If the test.fs.s3a.name property is not set, this will
   * trigger a JUnit failure.
   *
   * Multipart purging is enabled.
   * @param conf configuration
   * @return the FS
   * @throws IOException IO Problems
   * @throws AssumptionViolatedException if the FS is not named
   */
  public static S3AFileSystem createTestFileSystem(Configuration conf)
      throws IOException {
    return createTestFileSystem(conf, false);
  }

  /**
   * Create the test filesystem with or without multipart purging
   *
   * If the test.fs.s3a.name property is not set, this will
   * trigger a JUnit failure.
   * @param conf configuration
   * @param purge flag to enable Multipart purging
   * @return the FS
   * @throws IOException IO Problems
   */
  public static S3AFileSystem createTestFileSystem(Configuration conf,
      boolean purge)
      throws IOException {

    String fsname = conf.getTrimmed(TEST_FS_S3A_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(Constants.FS_S3A);
    }
    // This doesn't work with our JUnit 3 style test cases, so instead we'll
    // make this whole class not run by default
    Assume.assumeTrue("No test filesystem in " + TEST_FS_S3A_NAME,
        liveTest);
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    S3AFileSystem fs1 = new S3AFileSystem();
    //enable purging in tests
    if (purge) {
      // purge with but a delay so that parallel multipart tests don't
      // suddenly start timing out
      enableMultipartPurge(conf, PURGE_DELAY_SECONDS);
    }
    fs1.initialize(testURI, conf);
    return fs1;
  }

  public static void enableMultipartPurge(Configuration conf, int seconds) {
    conf.setBoolean(PURGE_EXISTING_MULTIPART, true);
    conf.setInt(PURGE_EXISTING_MULTIPART_AGE, seconds);
  }

  /**
   * Create a file context for tests.
   *
   * If the test.fs.s3a.name property is not set, this will
   * trigger a JUnit failure.
   *
   * Multipart purging is enabled.
   * @param conf configuration
   * @return the FS
   * @throws IOException IO Problems
   */
  public static FileContext createTestFileContext(Configuration conf)
      throws IOException {
    String fsname = conf.getTrimmed(TEST_FS_S3A_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(Constants.FS_S3A);
    }
    // This doesn't work with our JUnit 3 style test cases, so instead we'll
    // make this whole class not run by default
    Assume.assumeTrue("No test filesystem in " + TEST_FS_S3A_NAME,
        liveTest);
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    FileContext fc = FileContext.getFileContext(testURI, conf);
    return fc;
  }

  /**
   * Get a long test property.
   * <ol>
   *   <li>Look up configuration value (which can pick up core-default.xml),
   *       using {@code defVal} as the default value (if conf != null).
   *   </li>
   *   <li>Fetch the system property.</li>
   *   <li>If the system property is not empty or "(unset)":
   *   it overrides the conf value.
   *   </li>
   * </ol>
   * This puts the build properties in charge of everything. It's not a
   * perfect design; having maven set properties based on a file, as ant let
   * you do, is better for customization.
   *
   * As to why there's a special (unset) value, see
   * {@link http://stackoverflow.com/questions/7773134/null-versus-empty-arguments-in-maven}
   * @param conf config: may be null
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */
  public static long getTestPropertyLong(Configuration conf,
      String key, long defVal) {
    return Long.valueOf(
        getTestProperty(conf, key, Long.toString(defVal)));
  }
  /**
   * Get a test property value in bytes, using k, m, g, t, p, e suffixes.
   * {@link org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix#string2long(String)}
   * <ol>
   *   <li>Look up configuration value (which can pick up core-default.xml),
   *       using {@code defVal} as the default value (if conf != null).
   *   </li>
   *   <li>Fetch the system property.</li>
   *   <li>If the system property is not empty or "(unset)":
   *   it overrides the conf value.
   *   </li>
   * </ol>
   * This puts the build properties in charge of everything. It's not a
   * perfect design; having maven set properties based on a file, as ant let
   * you do, is better for customization.
   *
   * As to why there's a special (unset) value, see
   * {@link http://stackoverflow.com/questions/7773134/null-versus-empty-arguments-in-maven}
   * @param conf config: may be null
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */
  public static long getTestPropertyBytes(Configuration conf,
      String key, String defVal) {
    return org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
        .string2long(getTestProperty(conf, key, defVal));
  }

  /**
   * Get an integer test property; algorithm described in
   * {@link #getTestPropertyLong(Configuration, String, long)}.
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */
  public static int getTestPropertyInt(Configuration conf,
      String key, int defVal) {
    return (int) getTestPropertyLong(conf, key, defVal);
  }

  /**
   * Get a boolean test property; algorithm described in
   * {@link #getTestPropertyLong(Configuration, String, long)}.
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */
  public static boolean getTestPropertyBool(Configuration conf,
      String key,
      boolean defVal) {
    return Boolean.valueOf(
        getTestProperty(conf, key, Boolean.toString(defVal)));
  }

  /**
   * Get a string test property.
   * <ol>
   *   <li>Look up configuration value (which can pick up core-default.xml),
   *       using {@code defVal} as the default value (if conf != null).
   *   </li>
   *   <li>Fetch the system property.</li>
   *   <li>If the system property is not empty or "(unset)":
   *   it overrides the conf value.
   *   </li>
   * </ol>
   * This puts the build properties in charge of everything. It's not a
   * perfect design; having maven set properties based on a file, as ant let
   * you do, is better for customization.
   *
   * As to why there's a special (unset) value, see
   * @see <a href="http://stackoverflow.com/questions/7773134/null-versus-empty-arguments-in-maven">
   *   Stack Overflow</a>
   * @param conf config: may be null
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */

  public static String getTestProperty(Configuration conf,
      String key,
      String defVal) {
    String confVal = conf != null ? conf.getTrimmed(key, defVal) : defVal;
    String propval = System.getProperty(key);
    return isNotEmpty(propval) && !UNSET_PROPERTY.equals(propval)
        ? propval : confVal;
  }

  /**
   * Get the test CSV file; assume() that it is not empty.
   * @param conf test configuration
   * @return test file.
   */
  public static String getCSVTestFile(Configuration conf) {
    String csvFile = conf
        .getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE);
    Assume.assumeTrue("CSV test file is not the default",
        isNotEmpty(csvFile));
    return csvFile;
  }

  /**
   * Get the test CSV path; assume() that it is not empty.
   * @param conf test configuration
   * @return test file as a path.
   */
  public static Path getCSVTestPath(Configuration conf) {
    return new Path(getCSVTestFile(conf));
  }

  /**
   * Get the test CSV file; assume() that it is not modified (i.e. we haven't
   * switched to a new storage infrastructure where the bucket is no longer
   * read only).
   * @return test file.
   * @param conf test configuration
   */
  public static String getLandsatCSVFile(Configuration conf) {
    String csvFile = getCSVTestFile(conf);
    Assume.assumeTrue("CSV test file is not the default",
        DEFAULT_CSVTEST_FILE.equals(csvFile));
    return csvFile;
  }
  /**
   * Get the test CSV file; assume() that it is not modified (i.e. we haven't
   * switched to a new storage infrastructure where the bucket is no longer
   * read only).
   * @param conf test configuration
   * @return test file as a path.
   */
  public static Path getLandsatCSVPath(Configuration conf) {
    return new Path(getLandsatCSVFile(conf));
  }

  /**
   * Verify the class of an exception. If it is not as expected, rethrow it.
   * Comparison is on the exact class, not subclass-of inference as
   * offered by {@code instanceof}.
   * @param clazz the expected exception class
   * @param ex the exception caught
   * @return the exception, if it is of the expected class
   * @throws Exception the exception passed in.
   */
  public static <E extends Throwable> E verifyExceptionClass(Class<E> clazz,
      Exception ex)
      throws Exception {
    if (!(ex.getClass().equals(clazz))) {
      throw ex;
    }
    return (E)ex;
  }

  /**
   * Turn off FS Caching: use if a filesystem with different options from
   * the default is required.
   * @param conf configuration to patch
   */
  public static void disableFilesystemCaching(Configuration conf) {
    conf.setBoolean(FS_S3A_IMPL_DISABLE_CACHE, true);
  }

  /**
   * Skip a test if encryption tests are disabled.
   * @param configuration configuration to probe
   */
  public static void skipIfEncryptionTestsDisabled(
      Configuration configuration) {
    if (!configuration.getBoolean(KEY_ENCRYPTION_TESTS, true)) {
      skip("Skipping encryption tests");
    }
  }

  /**
   * Create a test path, using the value of
   * {@link S3ATestConstants#TEST_UNIQUE_FORK_ID} if it is set.
   * @param defVal default value
   * @return a path
   */
  public static Path createTestPath(Path defVal) {
    String testUniqueForkId =
        System.getProperty(S3ATestConstants.TEST_UNIQUE_FORK_ID);
    return testUniqueForkId == null ? defVal :
        new Path("/" + testUniqueForkId, "test");
  }

  /**
   * Test assumption that S3Guard is/is not enabled.
   * @param shouldBeEnabled should S3Guard be enabled?
   * @param originalConf configuration to check
   * @throws URISyntaxException
   */
  public static void assumeS3GuardState(boolean shouldBeEnabled,
      Configuration originalConf) throws URISyntaxException {
    boolean isEnabled = isS3GuardTestPropertySet(originalConf);
    Assume.assumeThat("Unexpected S3Guard test state:"
            + " shouldBeEnabled=" + shouldBeEnabled
            + " and isEnabled=" + isEnabled,
        shouldBeEnabled, Is.is(isEnabled));

    final String fsname = originalConf.getTrimmed(TEST_FS_S3A_NAME);
    Assume.assumeNotNull(fsname);
    final String bucket = new URI(fsname).getHost();
    final Configuration conf = propagateBucketOptions(originalConf, bucket);
    boolean usingNullImpl = S3GUARD_METASTORE_NULL.equals(
        conf.getTrimmed(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL));
    Assume.assumeThat("Unexpected S3Guard test state:"
            + " shouldBeEnabled=" + shouldBeEnabled
            + " but usingNullImpl=" + usingNullImpl,
        shouldBeEnabled, Is.is(!usingNullImpl));
  }

  /**
   * Is the test option for S3Guard set?
   * @param conf configuration to examine.
   * @return true if the config or system property turns s3guard tests on
   */
  public static boolean isS3GuardTestPropertySet(final Configuration conf) {
    return getTestPropertyBool(conf, TEST_S3GUARD_ENABLED,
        conf.getBoolean(TEST_S3GUARD_ENABLED, false));
  }

  /**
   * Conditionally set the S3Guard options from test properties.
   * @param conf configuration
   */
  public static void maybeEnableS3Guard(Configuration conf) {
    if (isS3GuardTestPropertySet(conf)) {
      // S3Guard is enabled.
      boolean authoritative = getTestPropertyBool(conf,
          TEST_S3GUARD_AUTHORITATIVE,
          conf.getBoolean(TEST_S3GUARD_AUTHORITATIVE, true));
      String impl = getTestProperty(conf, TEST_S3GUARD_IMPLEMENTATION,
          conf.get(TEST_S3GUARD_IMPLEMENTATION,
              TEST_S3GUARD_IMPLEMENTATION_LOCAL));
      String implClass = "";
      switch (impl) {
      case TEST_S3GUARD_IMPLEMENTATION_LOCAL:
        implClass = S3GUARD_METASTORE_LOCAL;
        break;
      case TEST_S3GUARD_IMPLEMENTATION_DYNAMO:
        implClass = S3GUARD_METASTORE_DYNAMO;
        break;
      case TEST_S3GUARD_IMPLEMENTATION_NONE:
        implClass = S3GUARD_METASTORE_NULL;
        break;
      default:
        fail("Unknown s3guard back end: \"" + impl + "\"");
      }
      LOG.debug("Enabling S3Guard, authoritative={}, implementation={}",
          authoritative, implClass);
      conf.setBoolean(METADATASTORE_AUTHORITATIVE, authoritative);
      conf.set(AUTHORITATIVE_PATH, "");
      conf.set(S3_METADATA_STORE_IMPL, implClass);
      conf.setBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, true);
    }
  }

  /**
   * Is there a MetadataStore configured for s3a with authoritative enabled?
   * @param conf Configuration to test.
   * @return true iff there is a MetadataStore configured, and it is
   * configured allow authoritative results.  This can result in reducing
   * round trips to S3 service for cached results, which may affect FS/FC
   * statistics.
   */
  public static boolean isMetadataStoreAuthoritative(Configuration conf) {
    if (conf == null) {
      return Constants.DEFAULT_METADATASTORE_AUTHORITATIVE;
    }
    return conf.getBoolean(
        Constants.METADATASTORE_AUTHORITATIVE,
        Constants.DEFAULT_METADATASTORE_AUTHORITATIVE);
  }

  /**
   * Require a filesystem to have a metadata store; skip test
   * if not.
   * @param fs filesystem to check
   */
  public static void assumeFilesystemHasMetadatastore(S3AFileSystem fs) {
    assume("Filesystem does not have a metastore",
        fs.hasMetadataStore());
  }

  /**
   * Reset all metrics in a list.
   * @param metrics metrics to reset
   */
  public static void reset(S3ATestUtils.MetricDiff... metrics) {
    for (S3ATestUtils.MetricDiff metric : metrics) {
      metric.reset();
    }
  }

  /**
   * Print all metrics in a list.
   * @param log log to print the metrics to.
   * @param metrics metrics to process
   */
  public static void print(Logger log, S3ATestUtils.MetricDiff... metrics) {
    for (S3ATestUtils.MetricDiff metric : metrics) {
      log.info(metric.toString());
    }
  }

  /**
   * Print all metrics in a list, then reset them.
   * @param log log to print the metrics to.
   * @param metrics metrics to process
   */
  public static void printThenReset(Logger log,
      S3ATestUtils.MetricDiff... metrics) {
    print(log, metrics);
    reset(metrics);
  }

  /**
   * Variant of {@code LambdaTestUtils#intercept() which closes the Closeable
   * returned by the invoked operation, and using its toString() value
   * for exception messages.
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param eval expression to eval
   * @param <T> return type of expression
   * @param <E> exception class
   * @return the caught exception if it was of the expected type and contents
   */
  public static <E extends Throwable, T extends Closeable> E interceptClosing(
      Class<E> clazz,
      String contained,
      Callable<T> eval)
      throws Exception {

    return intercept(clazz, contained,
        () -> {
          try (Closeable c = eval.call()) {
            return c.toString();
          }
        });
  }

  /**
   * Patch a configuration for testing.
   * This includes possibly enabling s3guard, setting up the local
   * FS temp dir and anything else needed for test runs.
   * @param conf configuration to patch
   * @return the now-patched configuration
   */
  public static Configuration prepareTestConfiguration(final Configuration conf) {
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    // set hadoop temp dir to a default value
    String testUniqueForkId =
        System.getProperty(TEST_UNIQUE_FORK_ID);
    String tmpDir = conf.get(HADOOP_TMP_DIR, "target/build/test");
    if (testUniqueForkId != null) {
      // patch temp dir for the specific branch
      tmpDir = tmpDir + File.separator + testUniqueForkId;
      conf.set(HADOOP_TMP_DIR, tmpDir);
    }
    conf.set(BUFFER_DIR, tmpDir);

    // directory marker policy
    String directoryRetention = getTestProperty(
        conf,
        DIRECTORY_MARKER_POLICY,
        DEFAULT_DIRECTORY_MARKER_POLICY);
    conf.set(DIRECTORY_MARKER_POLICY, directoryRetention);

    return conf;
  }

  /**
   * Clear any Hadoop credential provider path.
   * This is needed if people's test setups switch to credential providers,
   * and the test case is altering FS login details: changes made in the
   * config will not be picked up.
   * @param conf configuration to update
   */
  public static void unsetHadoopCredentialProviders(final Configuration conf) {
    conf.unset(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH);
  }

  /**
   * Build AWS credentials to talk to the STS. Also where checks for the
   * session tests being disabled are implemented.
   * @return a set of credentials
   * @throws IOException on a failure
   */
  public static AWSCredentialsProvider buildAwsCredentialsProvider(
      final Configuration conf)
      throws IOException {
    assumeSessionTestsEnabled(conf);

    S3xLoginHelper.Login login = S3AUtils.getAWSAccessKeys(
        URI.create("s3a://foobar"), conf);
    if (!login.hasLogin()) {
      skip("testSTS disabled because AWS credentials not configured");
    }
    return new SimpleAWSCredentialsProvider(login);
  }

  /**
   * Skip the current test if STS tess are not enabled.
   * @param conf configuration to examine
   */
  public static void assumeSessionTestsEnabled(final Configuration conf) {
    if (!conf.getBoolean(TEST_STS_ENABLED, true)) {
      skip("STS functional tests disabled");
    }
  }

  /**
   * Request session credentials for the default time (900s).
   * @param conf configuration to use for login
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @return the credentials
   * @throws IOException on a failure
   */
  public static MarshalledCredentials requestSessionCredentials(
      final Configuration conf,
      final String bucket)
      throws IOException {
    return requestSessionCredentials(conf, bucket,
        TEST_SESSION_TOKEN_DURATION_SECONDS);
  }

  /**
   * Request session credentials.
   * @param conf The Hadoop configuration
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param duration duration in seconds.
   * @return the credentials
   * @throws IOException on a failure
   */
  public static MarshalledCredentials requestSessionCredentials(
      final Configuration conf,
      final String bucket,
      final int duration)
      throws IOException {
    assumeSessionTestsEnabled(conf);
    MarshalledCredentials sc = MarshalledCredentialBinding
        .requestSessionCredentials(
          buildAwsCredentialsProvider(conf),
          S3AUtils.createAwsConf(conf, bucket, AWS_SERVICE_IDENTIFIER_STS),
          conf.getTrimmed(ASSUMED_ROLE_STS_ENDPOINT,
              DEFAULT_ASSUMED_ROLE_STS_ENDPOINT),
          conf.getTrimmed(ASSUMED_ROLE_STS_ENDPOINT_REGION,
              ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT),
          duration,
          new Invoker(new S3ARetryPolicy(conf), Invoker.LOG_EVENT));
    sc.validate("requested session credentials: ",
        MarshalledCredentials.CredentialTypeRequired.SessionOnly);
    return sc;
  }

  /**
   * Round trip a writable to a new instance.
   * @param source source object
   * @param conf configuration
   * @param <T> type
   * @return an unmarshalled instance of the type
   * @throws Exception on any failure.
   */
  @SuppressWarnings("unchecked")
  public static <T extends Writable> T roundTrip(
      final T source,
      final Configuration conf)
      throws Exception {
    DataOutputBuffer dob = new DataOutputBuffer();
    source.write(dob);

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), dob.getLength());

    T after = ReflectionUtils.newInstance((Class<T>) source.getClass(), conf);
    after.readFields(dib);
    return after;
  }

  /**
   * Get the name of the test bucket.
   * @param conf configuration to scan.
   * @return the bucket name from the config.
   * @throws NullPointerException: no test bucket
   */
  public static String getTestBucketName(final Configuration conf) {
    String bucket = checkNotNull(conf.get(TEST_FS_S3A_NAME),
        "No test bucket");
    return URI.create(bucket).getHost();
  }

  /**
   * Get the prefix for DynamoDB table names used in tests.
   * @param conf configuration to scan.
   * @return the table name prefix
   */
  public static String getTestDynamoTablePrefix(final Configuration conf) {
    return getTestProperty(conf, TEST_S3GUARD_DYNAMO_TABLE_PREFIX,
        TEST_S3GUARD_DYNAMO_TABLE_PREFIX_DEFAULT);
  }

  /**
   * Remove any values from a bucket.
   * @param bucket bucket whose overrides are to be removed. Can be null/empty
   * @param conf config
   * @param options list of fs.s3a options to remove
   */
  public static void removeBucketOverrides(final String bucket,
      final Configuration conf,
      final String... options) {

    if (StringUtils.isEmpty(bucket)) {
      return;
    }
    final String bucketPrefix = FS_S3A_BUCKET_PREFIX + bucket + '.';
    for (String option : options) {
      final String stripped = option.substring("fs.s3a.".length());
      String target = bucketPrefix + stripped;
      String v = conf.get(target);
      if (v != null) {
        LOG.debug("Removing option {}; was {}", target, v);
        conf.unset(target);
      }
      String extended = bucketPrefix + option;
      if (conf.get(extended) != null) {
        LOG.debug("Removing option {}", extended);
        conf.unset(extended);
      }
    }
  }

  /**
   * Remove any values from a bucket and the base values too.
   * @param bucket bucket whose overrides are to be removed. Can be null/empty.
   * @param conf config
   * @param options list of fs.s3a options to remove
   */
  public static void removeBaseAndBucketOverrides(final String bucket,
      final Configuration conf,
      final String... options) {
    for (String option : options) {
      conf.unset(option);
    }
    removeBucketOverrides(bucket, conf, options);
  }

  /**
   * Remove any values from the test bucket and the base values too.
   * @param conf config
   * @param options list of fs.s3a options to remove
   */
  public static void removeBaseAndBucketOverrides(
      final Configuration conf,
      final String... options) {
    for (String option : options) {
      conf.unset(option);
    }
    removeBaseAndBucketOverrides(getTestBucketName(conf), conf, options);
  }

  /**
   * Disable S3Guard from the test bucket in a configuration.
   * @param conf configuration.
   */
  public static void disableS3GuardInTestBucket(Configuration conf) {
    removeBaseAndBucketOverrides(getTestBucketName(conf), conf,
        S3_METADATA_STORE_IMPL,
        DIRECTORY_MARKER_POLICY);
    conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);
  }
  /**
   * Call a function; any exception raised is logged at info.
   * This is for test teardowns.
   * @param log log to use.
   * @param operation operation to invoke
   * @param <T> type of operation.
   */
  public static <T> void callQuietly(final Logger log,
      final CallableRaisingIOE<T> operation) {
    try {
      operation.apply();
    } catch (Exception e) {
      log.info(e.toString(), e);
    }
  }

  /**
   * Call a void operation; any exception raised is logged at info.
   * This is for test teardowns.
   * @param log log to use.
   * @param operation operation to invoke
   */
  public static void callQuietly(final Logger log,
      final Invoker.VoidOperation operation) {
    try {
      operation.execute();
    } catch (Exception e) {
      log.info(e.toString(), e);
    }
  }

  /**
   * Deploy a hadoop service: init and start it.
   * @param conf configuration to use
   * @param service service to configure
   * @param <T> type of service
   * @return the started service
   */
  public static <T extends Service> T deployService(
      final Configuration conf,
      final T service) {
    service.init(conf);
    service.start();
    return service;
  }

  /**
   * Terminate a service, returning {@code null} cast at compile-time
   * to the type of the service, for ease of setting fields to null.
   * @param service service.
   * @param <T> type of the service
   * @return null, always
   */
  @SuppressWarnings("ThrowableNotThrown")
  public static <T extends Service> T terminateService(final T service) {
    ServiceOperations.stopQuietly(LOG, service);
    return null;
  }

  /**
   * Get a file status from S3A with the {@code needEmptyDirectoryFlag}
   * state probed.
   * This accesses a package-private method in the
   * S3A filesystem.
   * @param fs filesystem
   * @param dir directory
   * @return a status
   * @throws IOException
   */
  public static S3AFileStatus getStatusWithEmptyDirFlag(
      final S3AFileSystem fs,
      final Path dir) throws IOException {
    return fs.innerGetFileStatus(dir, true,
        StatusProbeEnum.ALL);
  }

  /**
   * Create mock implementation of store context.
   * @param multiDelete
   * @param store
   * @param accessors
   * @return
   * @throws URISyntaxException
   * @throws IOException
   */
  public static StoreContext createMockStoreContext(
          boolean multiDelete,
          OperationTrackingStore store,
          ContextAccessors accessors)
          throws URISyntaxException, IOException {
    URI name = new URI("s3a://bucket");
    Configuration conf = new Configuration();
    return new StoreContextBuilder().setFsURI(name)
        .setBucket("bucket")
        .setConfiguration(conf)
        .setUsername("alice")
        .setOwner(UserGroupInformation.getCurrentUser())
        .setExecutor(BlockingThreadPoolExecutorService.newInstance(
            4,
            4,
            10, TimeUnit.SECONDS,
            "s3a-transfer-shared"))
        .setExecutorCapacity(DEFAULT_EXECUTOR_CAPACITY)
        .setInvoker(
            new Invoker(RetryPolicies.TRY_ONCE_THEN_FAIL, Invoker.LOG_EVENT))
        .setInstrumentation(new EmptyS3AStatisticsContext())
        .setStorageStatistics(new S3AStorageStatistics())
        .setInputPolicy(S3AInputPolicy.Normal)
        .setChangeDetectionPolicy(
            ChangeDetectionPolicy.createPolicy(ChangeDetectionPolicy.Mode.None,
                ChangeDetectionPolicy.Source.ETag, false))
        .setMultiObjectDeleteEnabled(multiDelete)
        .setMetadataStore(store)
        .setUseListV1(false)
        .setContextAccessors(accessors)
        .setTimeProvider(new S3Guard.TtlTimeProvider(conf))
        .build();
  }

  /**
   * Helper class to do diffs of metrics.
   */
  public static final class MetricDiff {
    private final S3AFileSystem fs;
    private final Statistic statistic;
    private long startingValue;

    /**
     * Constructor.
     * Invokes {@link #reset()} so it is immediately capable of measuring the
     * difference in metric values.
     *
     * @param fs the filesystem to monitor
     * @param statistic the statistic to monitor.
     */
    public MetricDiff(S3AFileSystem fs, Statistic statistic) {
      this.fs = fs;
      this.statistic = statistic;
      reset();
    }

    /**
     * Reset the starting value to the current value.
     * Diffs will be against this new value.
     */
    public void reset() {
      startingValue = currentValue();
    }

    /**
     * Get the current value of the metric.
     * @return the latest value.
     */
    public long currentValue() {
      return fs.getInstrumentation().getCounterValue(statistic);
    }

    /**
     * Get the difference between the the current value and
     * {@link #startingValue}.
     * @return the difference.
     */
    public long diff() {
      return currentValue() - startingValue;
    }

    @Override
    public String toString() {
      long c = currentValue();
      final StringBuilder sb = new StringBuilder(statistic.getSymbol());
      sb.append(" starting=").append(startingValue);
      sb.append(" current=").append(c);
      sb.append(" diff=").append(c - startingValue);
      return sb.toString();
    }

    /**
     * Assert that the value of {@link #diff()} matches that expected.
     * @param message message to print; metric name is appended
     * @param expected expected value.
     */
    public void assertDiffEquals(String message, long expected) {
      String text = message + ": " + statistic.getSymbol();
      long diff = diff();
      if (expected != diff) {
        // Log in error ensures that the details appear in the test output
        LOG.error(text + " expected {}, actual {}", expected, diff);
      }
      Assert.assertEquals(text,
          expected, diff);
    }

    /**
     * Assert that the value of {@link #diff()} matches that expected.
     * @param expected expected value.
     */
    public void assertDiffEquals(long expected) {
      assertDiffEquals("Count of " + this, expected);
    }

    /**
     * Assert that the value of {@link #diff()} matches that of another
     * instance.
     * @param that the other metric diff instance.
     */
    public void assertDiffEquals(MetricDiff that) {
      Assert.assertEquals(this.toString() + " != " + that,
          this.diff(), that.diff());
    }

    /**
     * Comparator for assertions.
     * @param that other metric diff
     * @return true if the value is {@code ==} the other's
     */
    public boolean diffEquals(MetricDiff that) {
      return this.diff() == that.diff();
    }

    /**
     * Comparator for assertions.
     * @param that other metric diff
     * @return true if the value is {@code <} the other's
     */
    public boolean diffLessThan(MetricDiff that) {
      return this.diff() < that.diff();
    }

    /**
     * Comparator for assertions.
     * @param that other metric diff
     * @return true if the value is {@code <=} the other's
     */
    public boolean diffLessThanOrEquals(MetricDiff that) {
      return this.diff() <= that.diff();
    }

    /**
     * Get the statistic.
     * @return the statistic
     */
    public Statistic getStatistic() {
      return statistic;
    }

    /**
     * Get the starting value; that set in the last {@link #reset()}.
     * @return the starting value for diffs.
     */
    public long getStartingValue() {
      return startingValue;
    }
  }

  /**
   * Asserts that {@code obj} is an instance of {@code expectedClass} using a
   * descriptive assertion message.
   * @param expectedClass class
   * @param obj object to check
   */
  public static void assertInstanceOf(Class<?> expectedClass, Object obj) {
    Assert.assertTrue(String.format("Expected instance of class %s, but is %s.",
        expectedClass, obj.getClass()),
        expectedClass.isAssignableFrom(obj.getClass()));
  }

  /**
   * Builds a comma-separated list of class names.
   * @param classes list of classes
   * @return comma-separated list of class names
   */
  public static <T extends Class<?>> String buildClassListString(
      List<T> classes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < classes.size(); ++i) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(classes.get(i).getName());
    }
    return sb.toString();
  }

  /**
   * This class should not be instantiated.
   */
  private S3ATestUtils() {
  }

  /**
   * Verify the core size, block size and timestamp values of a file.
   * @param status status entry to check
   * @param size file size
   * @param blockSize block size
   * @param modTime modified time
   */
  public static void verifyFileStatus(FileStatus status, long size,
      long blockSize, long modTime) {
    verifyFileStatus(status, size, 0, modTime, 0, blockSize, null, null, null);
  }

  /**
   * Verify the status entry of a file matches that expected.
   * @param status status entry to check
   * @param size file size
   * @param replication replication factor (may be 0)
   * @param modTime modified time
   * @param accessTime access time (may be 0)
   * @param blockSize block size
   * @param owner owner (may be null)
   * @param group user group (may be null)
   * @param permission permission (may be null)
   */
  public static void verifyFileStatus(FileStatus status,
      long size,
      int replication,
      long modTime,
      long accessTime,
      long blockSize,
      String owner,
      String group,
      FsPermission permission) {
    String details = status.toString();
    assertFalse("Not a dir: " + details, status.isDirectory());
    assertEquals("Mod time: " + details, modTime, status.getModificationTime());
    assertEquals("File size: " + details, size, status.getLen());
    assertEquals("Block size: " + details, blockSize, status.getBlockSize());
    if (replication > 0) {
      assertEquals("Replication value: " + details, replication,
          status.getReplication());
    }
    if (accessTime != 0) {
      assertEquals("Access time: " + details, accessTime,
          status.getAccessTime());
    }
    if (owner != null) {
      assertEquals("Owner: " + details, owner, status.getOwner());
    }
    if (group != null) {
      assertEquals("Group: " + details, group, status.getGroup());
    }
    if (permission != null) {
      assertEquals("Permission: " + details, permission,
          status.getPermission());
    }
  }

  /**
   * Verify the status entry of a directory matches that expected.
   * @param status status entry to check
   * @param replication replication factor
   * @param owner owner
   */
  public static void verifyDirStatus(S3AFileStatus status,
      int replication,
      String owner) {
    String details = status.toString();
    assertTrue("Is a dir: " + details, status.isDirectory());
    assertEquals("zero length: " + details, 0, status.getLen());
    // S3AFileStatus always assigns modTime = System.currentTimeMillis()
    assertTrue("Mod time: " + details, status.getModificationTime() > 0);
    assertEquals("Replication value: " + details, replication,
        status.getReplication());
    assertEquals("Access time: " + details, 0, status.getAccessTime());
    assertEquals("Owner: " + details, owner, status.getOwner());
    // S3AFileStatus always assigns group=owner
    assertEquals("Group: " + details, owner, status.getGroup());
    // S3AFileStatus always assigns permission = default
    assertEquals("Permission: " + details,
        FsPermission.getDefault(), status.getPermission());
  }

  /**
   * Assert that a configuration option matches the expected value.
   * @param conf configuration
   * @param key option key
   * @param expected expected value
   */
  public static void assertOptionEquals(Configuration conf,
      String key,
      String expected) {
    assertEquals("Value of " + key, expected, conf.get(key));
  }

  /**
   * Assume that a condition is met. If not: log at WARN and
   * then throw an {@link AssumptionViolatedException}.
   * @param message
   * @param condition
   */
  public static void assume(String message, boolean condition) {
    if (!condition) {
      LOG.warn(message);
    }
    Assume.assumeTrue(message, condition);
  }

  /**
   * Get the statistics from a wrapped block output stream.
   * @param out output stream
   * @return the (active) stats of the write
   */
  public static BlockOutputStreamStatistics
      getOutputStreamStatistics(FSDataOutputStream out) {
    S3ABlockOutputStream blockOutputStream
        = (S3ABlockOutputStream) out.getWrappedStream();
    return blockOutputStream.getStatistics();
  }

  /**
   * Read in a file and convert to an ascii string.
   * @param fs filesystem
   * @param path path to read
   * @return the bytes read and converted to a string
   * @throws IOException IO problems
   */
  public static String read(FileSystem fs,
      Path path) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    try (FSDataInputStream in = fs.open(path)) {
      byte[] buf = new byte[(int)status.getLen()];
      in.readFully(0, buf);
      return new String(buf);
    }
  }

  /**
   * Read in a file and convert to an ascii string, using the openFile
   * builder API and the file status.
   * If the status is an S3A FileStatus, any etag or versionId used
   * will be picked up.
   * @param fs filesystem
   * @param status file status, including path
   * @return the bytes read and converted to a string
   * @throws IOException IO problems
   */
  public static String readWithStatus(
      final FileSystem fs,
      final FileStatus status) throws IOException {
    final CompletableFuture<FSDataInputStream> future =
        fs.openFile(status.getPath())
            .withFileStatus(status)
            .build();

    try (FSDataInputStream in = awaitFuture(future)) {
      byte[] buf = new byte[(int) status.getLen()];
      in.readFully(0, buf);
      return new String(buf);
    }
  }

  /**
   * List a directory/directory tree.
   * @param fileSystem FS
   * @param path path
   * @param recursive do a recursive listing?
   * @return the number of files found.
   * @throws IOException failure.
   */
  public static long lsR(FileSystem fileSystem, Path path, boolean recursive)
      throws Exception {
    if (path == null) {
      // surfaces when someone calls getParent() on something at the top
      // of the path
      LOG.info("Empty path");
      return 0;
    }
    return S3AUtils.applyLocatedFiles(fileSystem.listFiles(path, recursive),
        (status) -> LOG.info("{}", status));
  }

  /**
   * Turn on the inconsistent S3A FS client in a configuration,
   * with 100% probability of inconsistency, default delays.
   * For this to go live, the paths must include the element
   * {@link FailureInjectionPolicy#DEFAULT_DELAY_KEY_SUBSTRING}.
   * @param conf configuration to patch
   * @param delay delay in millis
   */
  public static void enableInconsistentS3Client(Configuration conf,
      long delay) {
    LOG.info("Enabling inconsistent S3 client");
    conf.setClass(S3_CLIENT_FACTORY_IMPL, InconsistentS3ClientFactory.class,
        S3ClientFactory.class);
    conf.set(FAIL_INJECT_INCONSISTENCY_KEY, DEFAULT_DELAY_KEY_SUBSTRING);
    conf.setLong(FAIL_INJECT_INCONSISTENCY_MSEC, delay);
    conf.setFloat(FAIL_INJECT_INCONSISTENCY_PROBABILITY, 0.0f);
    conf.setFloat(FAIL_INJECT_THROTTLE_PROBABILITY, 0.0f);
  }

  /**
   * Is the filesystem using the inconsistent/throttling/unreliable client?
   * @param fs filesystem
   * @return true if the filesystem's client is the inconsistent one.
   */
  public static boolean isFaultInjecting(S3AFileSystem fs) {
    return fs.getAmazonS3Client() instanceof InconsistentAmazonS3Client;
  }

  /**
   * Skip a test because the client is using fault injection.
   * This should only be done for those tests which are measuring the cost
   * of operations or otherwise cannot handle retries.
   * @param fs filesystem to check
   */
  public static void skipDuringFaultInjection(S3AFileSystem fs) {
    Assume.assumeFalse("Skipping as filesystem has fault injection",
        isFaultInjecting(fs));
  }

  /**
   * Date format used for mapping upload initiation time to human string.
   */
  public static final DateFormat LISTING_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");

  /**
   * Skip a test if the FS isn't marked as supporting magic commits.
   * @param fs filesystem
   */
  public static void assumeMagicCommitEnabled(S3AFileSystem fs)
      throws IOException {
    assume("Magic commit option disabled on " + fs,
        fs.hasPathCapability(
            fs.getWorkingDirectory(),
            CommitConstants.STORE_CAPABILITY_MAGIC_COMMITTER));
  }

  /**
   * Probe for the configuration containing a specific credential provider.
   * If the list is empty, there will be no match, even if the named provider
   * is on the default list.
   *
   * @param conf configuration
   * @param providerClassname provider class
   * @return true if the configuration contains that classname.
   */
  public static boolean authenticationContains(Configuration conf,
      String providerClassname) {
    return conf.getTrimmedStringCollection(AWS_CREDENTIALS_PROVIDER)
        .contains(providerClassname);
  }

  public static boolean metadataStorePersistsAuthoritativeBit(MetadataStore ms)
      throws IOException {
    Map<String, String> diags = ms.getDiagnostics();
    String persists =
        diags.get(MetadataStoreCapabilities.PERSISTS_AUTHORITATIVE_BIT);
    if(persists == null){
      return false;
    }
    return Boolean.valueOf(persists);
  }

  /**
   * Set the metadata store of a filesystem instance to the given
   * store, via a package-private setter method.
   * @param fs filesystem.
   * @param ms metastore
   */
  public static void setMetadataStore(S3AFileSystem fs, MetadataStore ms) {
    fs.setMetadataStore(ms);
}

  public static void checkListingDoesNotContainPath(S3AFileSystem fs, Path filePath)
      throws IOException {
    final RemoteIterator<LocatedFileStatus> listIter =
        fs.listFiles(filePath.getParent(), false);
    while (listIter.hasNext()) {
      final LocatedFileStatus lfs = listIter.next();
      assertNotEquals("Listing was not supposed to include " + filePath,
            filePath, lfs.getPath());
    }
    LOG.info("{}; file omitted from listFiles listing as expected.", filePath);

    final FileStatus[] fileStatuses = fs.listStatus(filePath.getParent());
    for (FileStatus fileStatus : fileStatuses) {
      assertNotEquals("Listing was not supposed to include " + filePath,
            filePath, fileStatus.getPath());
    }
    LOG.info("{}; file omitted from listStatus as expected.", filePath);
  }

  public static void checkListingContainsPath(S3AFileSystem fs, Path filePath)
      throws IOException {

    boolean listFilesHasIt = false;
    boolean listStatusHasIt = false;

    final RemoteIterator<LocatedFileStatus> listIter =
        fs.listFiles(filePath.getParent(), false);


    while (listIter.hasNext()) {
      final LocatedFileStatus lfs = listIter.next();
      if (filePath.equals(lfs.getPath())) {
        listFilesHasIt = true;
      }
    }

    final FileStatus[] fileStatuses = fs.listStatus(filePath.getParent());
    for (FileStatus fileStatus : fileStatuses) {
      if (filePath.equals(fileStatus.getPath())) {
        listStatusHasIt = true;
      }
    }
    assertTrue("fs.listFiles didn't include " + filePath,
          listFilesHasIt);
    assertTrue("fs.listStatus didn't include " + filePath,
          listStatusHasIt);
  }

  /**
   * Wait for a deleted file to no longer be visible.
   * @param fs filesystem
   * @param testFilePath path to query
   * @throws Exception failure
   */
  public static void awaitDeletedFileDisappearance(final S3AFileSystem fs,
      final Path testFilePath) throws Exception {
    eventually(
        STABILIZATION_TIME, PROBE_INTERVAL_MILLIS,
        () -> intercept(FileNotFoundException.class,
            () -> fs.getFileStatus(testFilePath)));
  }

  /**
   * Wait for a file to be visible.
   * @param fs filesystem
   * @param testFilePath path to query
   * @return the file status.
   * @throws Exception failure
   */
  public static S3AFileStatus awaitFileStatus(S3AFileSystem fs,
      final Path testFilePath)
      throws Exception {
    return (S3AFileStatus) eventually(
        STABILIZATION_TIME, PROBE_INTERVAL_MILLIS,
        () -> fs.getFileStatus(testFilePath));
  }

  /**
   * This creates a set containing all current threads and some well-known
   * thread names whose existence should not fail test runs.
   * They are generally static cleaner threads created by various classes
   * on instantiation.
   * @return a set of threads to use in later assertions.
   */
  public static Set<String> listInitialThreadsForLifecycleChecks() {
    Set<String> threadSet = getCurrentThreadNames();
    // static filesystem statistics cleaner
    threadSet.add(
        "org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner");
    // AWS progress callbacks
    threadSet.add("java-sdk-progress-listener-callback-thread");
    // another AWS thread
    threadSet.add("java-sdk-http-connection-reaper");
    // java.lang.UNIXProcess. maybe if chmod is called?
    threadSet.add("process reaper");
    // once a quantile has been scheduled, the mutable quantile thread pool
    // is initialized; it has a minimum thread size of 1.
    threadSet.add("MutableQuantiles-0");
    // IDE?
    threadSet.add("Attach Listener");
    return threadSet;
  }

  /**
   * Get a set containing the names of all active threads,
   * stripping out all test runner threads.
   * @return the current set of threads.
   */
  public static Set<String> getCurrentThreadNames() {
    TreeSet<String> threads = Thread.getAllStackTraces().keySet()
        .stream()
        .map(Thread::getName)
        .filter(n -> n.startsWith("JUnit"))
        .filter(n -> n.startsWith("surefire"))
        .collect(Collectors.toCollection(TreeSet::new));
    return threads;
  }

  /**
   * Call the package-private {@code innerGetFileStatus()} method
   * on the passed in FS.
   * @param fs filesystem
   * @param path path
   * @param needEmptyDirectoryFlag look for empty directory
   * @param probes file status probes to perform
   * @return the status
   * @throws IOException
   */
  public static S3AFileStatus innerGetFileStatus(
      S3AFileSystem fs,
      Path path,
      boolean needEmptyDirectoryFlag,
      Set<StatusProbeEnum> probes) throws IOException {

    return fs.innerGetFileStatus(
        path,
        needEmptyDirectoryFlag,
        probes);
  }

}
