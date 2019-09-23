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

package org.apache.hadoop.fs.azure.integration;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;

import static org.junit.Assume.assumeTrue;

import static org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount.WASB_ACCOUNT_NAME_DOMAIN_SUFFIX_REGEX;
import static org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount.WASB_TEST_ACCOUNT_NAME_WITH_DOMAIN;
import static org.apache.hadoop.fs.azure.integration.AzureTestConstants.*;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Utilities for the Azure tests. Based on {@code S3ATestUtils}, so
 * (initially) has unused method.
 */
public final class AzureTestUtils extends Assert {
  private static final Logger LOG = LoggerFactory.getLogger(
      AzureTestUtils.class);

  /**
   * Value to set a system property to (in maven) to declare that
   * a property has been unset.
   */
  public static final String UNSET_PROPERTY = "unset";

  /**
   * Create the test filesystem.
   *
   * If the test.fs.wasb.name property is not set, this will
   * raise a JUnit assumption exception
   *
   * @param conf configuration
   * @return the FS
   * @throws IOException IO Problems
   * @throws AssumptionViolatedException if the FS is not named
   */
  public static NativeAzureFileSystem createTestFileSystem(Configuration conf)
      throws IOException {

    String fsname = conf.getTrimmed(TEST_FS_WASB_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(WASB_SCHEME);
    }
    if (!liveTest) {
      // Skip the test
      throw new AssumptionViolatedException(
          "No test filesystem in " + TEST_FS_WASB_NAME);
    }
    NativeAzureFileSystem fs1 = new NativeAzureFileSystem();
    fs1.initialize(testURI, conf);
    return fs1;
  }

  /**
   * Create a file context for tests.
   *
   * If the test.fs.wasb.name property is not set, this will
   * trigger a JUnit failure.
   *
   * Multipart purging is enabled.
   * @param conf configuration
   * @return the FS
   * @throws IOException IO Problems
   * @throws AssumptionViolatedException if the FS is not named
   */
  public static FileContext createTestFileContext(Configuration conf)
      throws IOException {
    String fsname = conf.getTrimmed(TEST_FS_WASB_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(WASB_SCHEME);
    }
    if (!liveTest) {
      // This doesn't work with our JUnit 3 style test cases, so instead we'll
      // make this whole class not run by default
      throw new AssumptionViolatedException("No test filesystem in "
          + TEST_FS_WASB_NAME);
    }
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
    String confVal = conf != null
        ? conf.getTrimmed(key, defVal)
        : defVal;
    String propval = System.getProperty(key);
    return StringUtils.isNotEmpty(propval) && !UNSET_PROPERTY.equals(propval)
        ? propval : confVal;
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
  public static Exception verifyExceptionClass(Class clazz,
      Exception ex)
      throws Exception {
    if (!(ex.getClass().equals(clazz))) {
      throw ex;
    }
    return ex;
  }

  /**
   * Turn off FS Caching: use if a filesystem with different options from
   * the default is required.
   * @param conf configuration to patch
   */
  public static void disableFilesystemCaching(Configuration conf) {
    conf.setBoolean("fs.wasb.impl.disable.cache", true);
  }

  /**
   * Create a test path, using the value of
   * {@link AzureTestUtils#TEST_UNIQUE_FORK_ID} if it is set.
   * @param defVal default value
   * @return a path
   */
  public static Path createTestPath(Path defVal) {
    String testUniqueForkId = System.getProperty(
        AzureTestConstants.TEST_UNIQUE_FORK_ID);
    return testUniqueForkId == null
        ? defVal
        : new Path("/" + testUniqueForkId, "test");
  }

  /**
   * Create a test page blob path using the value of
   * {@link AzureTestConstants#TEST_UNIQUE_FORK_ID} if it is set.
   * @param filename filename at the end of the path
   * @return an absolute path
   */
  public static Path blobPathForTests(FileSystem fs, String filename) {
    String testUniqueForkId = System.getProperty(
        AzureTestConstants.TEST_UNIQUE_FORK_ID);
    return fs.makeQualified(new Path(PAGE_BLOB_DIR,
        testUniqueForkId == null
            ? filename
            : (testUniqueForkId + "/" + filename)));
  }

  /**
   * Create a test path using the value of
   * {@link AzureTestConstants#TEST_UNIQUE_FORK_ID} if it is set.
   * @param filename filename at the end of the path
   * @return an absolute path
   */
  public static Path pathForTests(FileSystem fs, String filename) {
    String testUniqueForkId = System.getProperty(
        AzureTestConstants.TEST_UNIQUE_FORK_ID);
    return fs.makeQualified(new Path(
        testUniqueForkId == null
            ? ("/test/" + filename)
            : (testUniqueForkId + "/" + filename)));
  }

  /**
   * Get a unique fork ID.
   * Returns a default value for non-parallel tests.
   * @return a string unique for all test VMs running in this maven build.
   */
  public static String getForkID() {
    return System.getProperty(
        AzureTestConstants.TEST_UNIQUE_FORK_ID, "fork-1");
  }

  /**
   * Flag to indicate that this test is being executed in parallel.
   * This is used by some of the scale tests to validate test time expectations.
   * @return true if the build indicates this test is being run in parallel.
   */
  public static boolean isParallelExecution() {
    return Boolean.getBoolean(KEY_PARALLEL_TEST_EXECUTION);
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
  private AzureTestUtils() {
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
   * @param message message in an assumption
   * @param condition condition to probe
   */
  public static void assume(String message, boolean condition) {
    if (!condition) {
      LOG.warn(message);
    }
    Assume.assumeTrue(message, condition);
  }

  /**
   * Gets the current value of the given gauge.
   * @param fs filesystem
   * @param gaugeName gauge name
   * @return the gauge value
   */
  public static long getLongGaugeValue(NativeAzureFileSystem fs,
      String gaugeName) {
    return getLongGauge(gaugeName, getMetrics(fs.getInstrumentation()));
  }

  /**
   * Gets the current value of the given counter.
   * @param fs filesystem
   * @param counterName counter name
   * @return the counter value
   */
  public static long getLongCounterValue(NativeAzureFileSystem fs,
      String counterName) {
    return getLongCounter(counterName, getMetrics(fs.getInstrumentation()));
  }


  /**
   * Delete a path, catching any exception and downgrading to a log message.
   * @param fs filesystem
   * @param path path to delete
   * @param recursive recursive delete?
   * @throws IOException IO failure.
   */
  public static void deleteQuietly(FileSystem fs,
      Path path,
      boolean recursive) throws IOException {
    if (fs != null && path != null) {
      try {
        fs.delete(path, recursive);
      } catch (IOException e) {
        LOG.warn("When deleting {}", path, e);
      }
    }
  }


  /**
   * Clean up the test account if non-null; return null to put in the
   * field.
   * @param testAccount test account to clean up
   * @return null
   * @throws Execption cleanup problems
   */
  public static AzureBlobStorageTestAccount cleanup(
      AzureBlobStorageTestAccount testAccount) throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
    return null;
  }


  /**
   * Clean up the test account; any thrown exceptions are caught and
   * logged.
   * @param testAccount test account
   * @return null, so that any fields can be reset.
   */
  public static AzureBlobStorageTestAccount cleanupTestAccount(
      AzureBlobStorageTestAccount testAccount) {
    if (testAccount != null) {
      try {
        testAccount.cleanup();
      } catch (Exception e) {
        LOG.error("While cleaning up test account: ", e);
      }
    }
    return null;
  }

  /**
   * Assume that the scale tests are enabled by the relevant system property.
   */
  public static void assumeScaleTestsEnabled(Configuration conf) {
    boolean enabled = getTestPropertyBool(
        conf,
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    assume("Scale test disabled: to enable set property "
            + KEY_SCALE_TESTS_ENABLED,
        enabled);
  }

  /**
   * Check the account name for WASB tests is set correctly and return.
   */
  public static String verifyWasbAccountNameInConfig(Configuration conf) {
    String accountName = conf.get(ACCOUNT_NAME_PROPERTY_NAME);
    if (accountName == null) {
      accountName = conf.get(WASB_TEST_ACCOUNT_NAME_WITH_DOMAIN);
    }
    assumeTrue("Account for WASB is missing or it is not in correct format",
            accountName != null && !accountName.endsWith(WASB_ACCOUNT_NAME_DOMAIN_SUFFIX_REGEX));
    return accountName;
  }

  /**
   * Write string into a file.
   */
  public static void writeStringToFile(FileSystem fs, Path path, String value)
          throws IOException {
    FSDataOutputStream outputStream = fs.create(path, true);
    writeStringToStream(outputStream, value);
  }

  /**
   * Write string into a file.
   */
  public static void writeStringToStream(FSDataOutputStream outputStream, String value)
          throws IOException {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
            outputStream));
    writer.write(value);
    writer.close();
  }

  /**
   * Read string from a file.
   */
  public static String readStringFromFile(FileSystem fs, Path testFile) throws IOException {
    FSDataInputStream inputStream = fs.open(testFile);
    String ret = readStringFromStream(inputStream);
    inputStream.close();
    return ret;
  }

  /**
   * Read string from stream.
   */
  public static String readStringFromStream(FSDataInputStream inputStream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
            inputStream));
    final int BUFFER_SIZE = 1024;
    char[] buffer = new char[BUFFER_SIZE];
    int count = reader.read(buffer, 0, BUFFER_SIZE);
    if (count > BUFFER_SIZE) {
      throw new IOException("Exceeded buffer size");
    }
    inputStream.close();
    return new String(buffer, 0, count);
  }

  /**
   * Assume hierarchical namespace is disabled for test account.
   */
  public static void assumeNamespaceDisabled(Configuration conf) {
    Assume.assumeFalse("Hierarchical namespace is enabled for test account.",
        conf.getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false));
  }
}
