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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * S3A contract tests creating files.
 * Parameterized on the create performance flag as all overwrite
 * tests are required to fail in create performance mode.
 */
@RunWith(Parameterized.class)
public class ITestS3AContractCreate extends AbstractContractCreateTest {

  /**
   * This test suite is parameterized for the different create file
   * options.
   * @return a list of test parameters.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {false},
        {true}
    });
  }

  /**
   * Is this test run in create performance mode?
   */
  private final boolean createPerformance;

  public ITestS3AContractCreate(final boolean createPerformance) {
    this.createPerformance = createPerformance;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        FS_S3A_CREATE_PERFORMANCE);
    conf.setBoolean(FS_S3A_CREATE_PERFORMANCE, createPerformance);
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  @Override
  public void testOverwriteNonEmptyDirectory() throws Throwable {
    try {
      super.testOverwriteNonEmptyDirectory();
      failWithCreatePerformance();
    } catch (AssertionError e) {
      swallowWithCreatePerformance(e);
    }
  }

  @Override
  public void testOverwriteEmptyDirectory() throws Throwable {
    try {
      super.testOverwriteEmptyDirectory();
      failWithCreatePerformance();
    } catch (AssertionError e) {
      swallowWithCreatePerformance(e);
    }
  }

  @Override
  public void testCreateFileOverExistingFileNoOverwrite() throws Throwable {
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
