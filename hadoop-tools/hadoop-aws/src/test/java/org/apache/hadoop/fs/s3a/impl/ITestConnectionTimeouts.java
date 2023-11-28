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

package org.apache.hadoop.fs.s3a.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.ConnectTimeoutException;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_ACQUISITION_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_IDLE_TIME;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_TTL;
import static org.apache.hadoop.fs.s3a.Constants.ESTABLISH_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.ConfigurationHelper.setDurationAsMillis;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test for timeout generation/handling, especially those related to connection
 * pools.
 * This test has proven a bit brittle to parallel test runs, so test cases should
 * create their own FS instances.
 * The likely cause is actually -Dprefetch test runs as these return connections to
 * the pool.
 * However, it is also important to have a non-brittle FS for creating the test file
 * and teardow, again, this makes for a flaky test..
 */
public class ITestConnectionTimeouts extends AbstractS3ATestBase {

  /**
   * How big a file to create?
   */
  public static final int FILE_SIZE = 1024;

  /**
   * Create a configuration for an FS which has timeouts set to very low values
   * and no retries.
   * @return a configuration to use for the brittle FS.
   */
  private Configuration timingOutConfiguration() {
    Configuration conf = new Configuration(getConfiguration());
    removeBaseAndBucketOverrides(conf,
        CONNECTION_TTL,
        CONNECTION_ACQUISITION_TIMEOUT,
        CONNECTION_IDLE_TIME,
        ESTABLISH_TIMEOUT,
        MAX_ERROR_RETRIES,
        MAXIMUM_CONNECTIONS,
        PREFETCH_ENABLED_KEY,
        REQUEST_TIMEOUT,
        SOCKET_TIMEOUT,
        FS_S3A_CREATE_PERFORMANCE
    );

    // only one connection is allowed, and the establish timeout is low
    conf.setInt(MAXIMUM_CONNECTIONS, 1);
    conf.setInt(MAX_ERROR_RETRIES, 0);
    // needed to ensure that streams are kept open.
    // without this the tests is unreliable in batch runs.
    conf.setBoolean(PREFETCH_ENABLED_KEY, false);
    conf.setInt(RETRY_LIMIT, 0);
    conf.setBoolean(FS_S3A_CREATE_PERFORMANCE, true);
    final Duration ms10 = Duration.ofMillis(10);
    setDurationAsMillis(conf, CONNECTION_ACQUISITION_TIMEOUT, ms10);
    setDurationAsMillis(conf, ESTABLISH_TIMEOUT, ms10);
    return conf;
  }

  @Override
  public void teardown() throws Exception {
    AWSClientConfig.resetMinimumOperationDuration();
    super.teardown();
  }

  /**
   * Use up the connection pool and expect the failure to be handled.
   */
  @Test
  public void testGeneratePoolTimeouts() throws Throwable {
    byte[] data = dataset(FILE_SIZE, '0', 10);
    AWSClientConfig.setMinimumOperationDuration(Duration.ZERO);
    Configuration conf = timingOutConfiguration();
    Path path = methodPath();
    int streamsToCreate = 100;
    List<FSDataInputStream> streams = new ArrayList<>(streamsToCreate);
    final S3AFileSystem fs = getFileSystem();
    // create the test file using the good fs, to avoid connection timeouts
    // during setup.
    ContractTestUtils.createFile(fs, path, true, data);
    final FileStatus st = fs.getFileStatus(path);
    try (FileSystem brittleFS = FileSystem.newInstance(fs.getUri(), conf)) {
      intercept(ConnectTimeoutException.class, () -> {
        for (int i = 0; i < streamsToCreate; i++) {
          FutureDataInputStreamBuilder b = brittleFS.openFile(path);
          b.withFileStatus(st);
          b.opt(FS_OPTION_OPENFILE_READ_POLICY, FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE);

          final FSDataInputStream in = b.build().get();
          streams.add(in);
          // kick off the read so forcing a GET.
          in.read();
        }
      });
    } finally {
      streams.forEach(s -> {
        IOUtils.cleanupWithLogger(LOG, s);
      });
    }
  }
}
