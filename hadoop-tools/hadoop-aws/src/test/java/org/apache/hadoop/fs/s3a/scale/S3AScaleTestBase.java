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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Base class for scale tests; here is where the common scale configuration
 * keys are defined.
 */
public class S3AScaleTestBase extends Assert implements S3ATestConstants {

  @Rule
  public TestName methodName = new TestName();

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @BeforeClass
  public static void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * The number of operations to perform: {@value}.
   */
  public static final String KEY_OPERATION_COUNT =
      SCALE_TEST + "operation.count";

  /**
   * The number of directory operations to perform: {@value}.
   */
  public static final String KEY_DIRECTORY_COUNT =
      SCALE_TEST + "directory.count";

  /**
   * The readahead buffer: {@value}.
   */
  public static final String KEY_READ_BUFFER_SIZE =
      S3A_SCALE_TEST + "read.buffer.size";

  public static final int DEFAULT_READ_BUFFER_SIZE = 16384;

  /**
   * Key for a multi MB test file: {@value}.
   */
  public static final String KEY_CSVTEST_FILE =
      S3A_SCALE_TEST + "csvfile";

  /**
   * Default path for the multi MB test file: {@value}.
   */
  public static final String DEFAULT_CSVTEST_FILE
      = "s3a://landsat-pds/scene_list.gz";

  /**
   * The default number of operations to perform: {@value}.
   */
  public static final long DEFAULT_OPERATION_COUNT = 2005;

  /**
   * Default number of directories to create when performing
   * directory performance/scale tests.
   */
  public static final int DEFAULT_DIRECTORY_COUNT = 2;

  protected S3AFileSystem fs;

  protected static final Logger LOG =
      LoggerFactory.getLogger(S3AScaleTestBase.class);

  private Configuration conf;

  /**
   * Configuration generator. May be overridden to inject
   * some custom options.
   * @return a configuration with which to create FS instances
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Get the configuration used to set up the FS.
   * @return the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    conf = createConfiguration();
    LOG.debug("Scale test operation count = {}", getOperationCount());
    fs = S3ATestUtils.createTestFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
    ContractTestUtils.rm(fs, getTestPath(), true, true);
  }

  protected Path getTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null ? new Path("/tests3a") :
        new Path("/" + testUniqueForkId, "tests3a");
  }

  protected long getOperationCount() {
    return getConf().getLong(KEY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
  }

  /**
   * Describe a test in the logs
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        methodName.getMethodName(),
        String.format(text, args));
  }

  /**
   * Get the input stream statistics of an input stream.
   * Raises an exception if the inner stream is not an S3A input stream
   * @param in wrapper
   * @return the statistics for the inner stream
   */
  protected S3AInstrumentation.InputStreamStatistics getInputStreamStatistics(
      FSDataInputStream in) {
    InputStream inner = in.getWrappedStream();
    if (inner instanceof S3AInputStream) {
      S3AInputStream s3a = (S3AInputStream) inner;
      return s3a.getS3AStreamStatistics();
    } else {
      Assert.fail("Not an S3AInputStream: " + inner);
      // never reached
      return null;
    }
  }

}
