/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCopyTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.apache.hadoop.fs.CommonPathCapabilities.FS_NATIVE_COPY;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.S3A_TEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * S3a contract test covering copy tests.
 */
public class ITestS3AContractCopy extends AbstractContractCopyTest {

  public static final Logger LOG = LoggerFactory.getLogger(
      ITestS3AContractRename.class);

  @Override
  public void setup() throws Exception {
    super.setup();
  }

  @Override
  protected int getTestTimeoutMillis() {
    return S3A_TEST_TIMEOUT;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   *
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    maybeEnableS3Guard(conf);
    return conf;
  }

  @Test
  public void testNativeCopyCapability() throws Throwable {
    describe("Test native copy capability");

    // Check for native copy support
    boolean nativeCopySupported = fs.hasPathCapability(srcFile, FS_NATIVE_COPY);
    assertTrue("s3a has to support native file copy",
        nativeCopySupported);
  }

  @Test
  public void testCopyAcrossSchemes() throws Throwable {
    describe("Test native file copy with different schemes");

    URI src = URI.create("s3a://testbucket/sample.txt");
    URI dst = URI.create("hdfs://testhdfs/sample.txt");

    handleExpectedException(intercept(IllegalArgumentException.class,
        () -> fs.copyFile(src, dst).get()));
  }

}
