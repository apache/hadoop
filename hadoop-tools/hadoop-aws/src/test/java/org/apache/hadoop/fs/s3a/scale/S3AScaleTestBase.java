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
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assume.assumeTrue;

/**
 * Base class for scale tests; here is where the common scale configuration
 * keys are defined
 */
public class S3AScaleTestBase {

  public static final String SCALE_TEST = "scale.test.";
  public static final String KEY_OPERATION_COUNT =
      SCALE_TEST + "operation.count";
  public static final long DEFAULT_OPERATION_COUNT = 2005;

  protected S3AFileSystem fs;
  private static final Logger LOG =
      LoggerFactory.getLogger(S3AScaleTestBase.class);

  private Configuration conf;

  /**
   * Configuration generator. May be overridden to inject
   * some custom options
   * @return a configuration with which to create FS instances
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Get the configuration used to set up the FS
   * @return the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    conf = createConfiguration();
    fs = S3ATestUtils.createTestFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
    ContractTestUtils.rm(fs, getTestPath(), true, true);
  }

  protected Path getTestPath() {
    return new Path("/tests3a");
  }

  protected long getOperationCount() {
    return getConf().getLong(KEY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
  }
}
