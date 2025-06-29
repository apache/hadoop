/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.s3a;

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.s3a.S3ATestConstants.KEY_PERFORMANCE_TESTS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_EXPECT_CONTINUE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPerformanceFlags;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfAnalyticsAcceleratorEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotEnabled;

/**
 * S3A contract tests creating files.
 * Parameterized on the create performance flag as all overwrite
 * tests are required to fail in create performance mode.
 */
public class ITestS3AContractCreate extends AbstractContractCreateTest {

  /**
   * This test suite is parameterized for the different create file
   * options.
   * @return a list of test parameters.
   */
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {false, false},
        {true, true}
    });
  }

  /**
   * Is this test run in create performance mode?
   */
  private boolean createPerformance;

  /**
   * Expect a 100-continue response?
   */
  private boolean expectContinue;

  public void initITestS3AContractCreate(final boolean pCreatePerformance,
      final boolean pExpectContinue) {
    this.createPerformance = pCreatePerformance;
    this.expectContinue = pExpectContinue;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = setPerformanceFlags(
        super.createConfiguration(),
        createPerformance ? "create" : "");
    removeBaseAndBucketOverrides(
        conf,
        CONNECTION_EXPECT_CONTINUE);
    conf.setBoolean(CONNECTION_EXPECT_CONTINUE, expectContinue);
    if (createPerformance) {
      skipIfNotEnabled(conf, KEY_PERFORMANCE_TESTS_ENABLED, "Skipping tests running in performance mode");
    }
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testOverwriteNonEmptyDirectory(boolean pCreatePerformance,
      boolean pExpectContinue) throws Throwable {
    initITestS3AContractCreate(pCreatePerformance, pExpectContinue);
    try {
       // Currently analytics accelerator does not support reading of files that have been overwritten.
       // This is because the analytics accelerator library caches metadata, and when a file is
       // overwritten, the old metadata continues to be used, until it is removed from the cache over
       // time. This will be fixed in https://github.com/awslabs/analytics-accelerator-s3/issues/218.
       skipIfAnalyticsAcceleratorEnabled(getContract().getConf(),
           "Analytics Accelerator currently does not support reading of over written files");

       super.testOverwriteNonEmptyDirectory();
       failWithCreatePerformance();
    } catch (AssertionError e) {
      swallowWithCreatePerformance(e);
    }
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testOverwriteEmptyDirectory(boolean pCreatePerformance,
      boolean pExpectContinue) throws Throwable {
    initITestS3AContractCreate(pCreatePerformance, pExpectContinue);
    try {
      super.testOverwriteEmptyDirectory();
      failWithCreatePerformance();
    } catch (AssertionError e) {
      swallowWithCreatePerformance(e);
    }
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testCreateFileOverExistingFileNoOverwrite(boolean pCreatePerformance,
      boolean pExpectContinue) throws Throwable {
    initITestS3AContractCreate(pCreatePerformance, pExpectContinue);
    try {
      super.testCreateFileOverExistingFileNoOverwrite();
      failWithCreatePerformance();
    } catch (AssertionError e) {
      swallowWithCreatePerformance(e);
    }
  }

  private void failWithCreatePerformance() {
    if (createPerformance) {
      fail("expected an assertion error in create performance mode");
    }
  }

  /**
   * Swallow an assertion error if the create performance flag is set.
   * @param e assertion error
   */
  private void swallowWithCreatePerformance(final AssertionError e) {
    // this is expected in create performance modea
    if (!createPerformance) {
      // but if the create performance flag is set, then it is supported
      // and the assertion error is unexpected
      throw e;
    }
  }
}
